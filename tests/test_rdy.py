from __future__ import annotations

import asyncio

import pytest

from ansq import create_reader, create_writer, open_connection
from ansq.tcp.reader import Reader


def test_compute_rdy_distribution_even_split():
    assert Reader._compute_rdy_distribution(4, ["a", "b"]) == {"a": 2, "b": 2}


def test_compute_rdy_distribution_with_remainder():
    assert Reader._compute_rdy_distribution(5, ["a", "b"]) == {"a": 3, "b": 2}


def test_compute_rdy_distribution_fewer_than_connections():
    assert Reader._compute_rdy_distribution(1, ["a", "b", "c"]) == {
        "a": 1,
        "b": 0,
        "c": 0,
    }


def test_compute_rdy_distribution_zero_connections():
    assert Reader._compute_rdy_distribution(4, []) == {}


def test_compute_rdy_distribution_zero_total():
    assert Reader._compute_rdy_distribution(0, ["a", "b"]) == {"a": 0, "b": 0}


@pytest.fixture
async def nsqd2(tmp_path, create_nsqd):
    async with create_nsqd() as nsqd:
        yield nsqd


async def test_default_behavior_each_connection_gets_full_rdy(nsqd, nsqd2, wait_for):
    """Before set_max_in_flight is ever called, distribution stays off: every
    connection independently subscribes with RDY=max_in_flight (0.3.x parity).
    """
    reader = await create_reader(
        topic="rdy1",
        channel="c1",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
        max_in_flight=3,
    )
    await wait_for(lambda: len(reader.connections) == 2)
    for conn in reader.connections:
        assert conn.rdy_messages_count == 3

    await reader.close()


async def test_set_max_in_flight_distributes_across_connections(nsqd, nsqd2, wait_for):
    reader = await create_reader(
        topic="rdy2",
        channel="c2",
        nsqd_tcp_addresses=[nsqd.tcp_address, nsqd2.tcp_address],
    )
    await wait_for(lambda: len(reader.connections) == 2)

    await reader.set_max_in_flight(4)
    for conn in reader.connections:
        assert conn.rdy_messages_count == 2

    await reader.set_max_in_flight(5)
    counts = sorted(conn.rdy_messages_count for conn in reader.connections)
    assert counts == [2, 3]

    await reader.close()


async def test_distributed_rdy_is_a_standing_window(nsqd, wait_for):
    """RDY N sustains N concurrent in-flight messages with no manual
    replenishment: FIN reopens the window for exactly one more message."""
    reader = await create_reader(
        topic="rdy3", channel="c3", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await reader.set_max_in_flight(3)

    producer = await open_connection(nsqd.host, nsqd.port)
    for i in range(6):
        await producer.pub("rdy3", f"msg-{i}")
    await producer.close()

    held = [await reader.wait_for_message() for _ in range(3)]

    conn = reader.connections[0]
    await wait_for(lambda: conn.in_flight == 3)

    # Window is full: no 4th message should arrive while 3 are held.
    await asyncio.sleep(0.2)
    assert reader.message_queue.qsize() == 0

    await held[0].fin()
    fourth = await asyncio.wait_for(reader.wait_for_message(), timeout=2)
    assert fourth is not None

    for message in held[1:]:
        await message.fin()
    await fourth.fin()

    await reader.close()


async def test_rdy_preserved_across_reconnect(nsqd):
    reader = await create_reader(
        topic="rdy4", channel="c4", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await reader.set_max_in_flight(5)
    conn = reader.connections[0]
    assert conn.rdy_messages_count == 5

    await conn.reconnect()

    assert conn.rdy_messages_count == 5
    assert conn.is_subscribed

    await reader.close()


async def test_lookupd_add_triggers_redistribution(
    create_nsqlookupd, create_nsqd, wait_for
):
    async with create_nsqlookupd() as lookupd:
        async with create_nsqd(lookupd_tcp_addresses=[lookupd.tcp_address]) as nsqd1:
            reader = await create_reader(
                topic="rdy5",
                channel="c5",
                lookupd_http_addresses=[lookupd.http_address],
                lookupd_poll_interval=100,
            )
            await reader.set_max_in_flight(4)

            writer1 = await create_writer(nsqd_tcp_addresses=[nsqd1.tcp_address])
            await writer1.pub(topic="rdy5", message="seed")
            await writer1.close()
            await wait_for(lambda: len(reader.connections) == 1)
            assert reader.connections[0].rdy_messages_count == 4

            async with create_nsqd(
                lookupd_tcp_addresses=[lookupd.tcp_address]
            ) as nsqd2:
                writer2 = await create_writer(nsqd_tcp_addresses=[nsqd2.tcp_address])
                await writer2.pub(topic="rdy5", message="seed2")
                await writer2.close()
                await wait_for(lambda: len(reader.connections) == 2)
                await wait_for(
                    lambda: all(c.rdy_messages_count == 2 for c in reader.connections)
                )

            await reader.close()
