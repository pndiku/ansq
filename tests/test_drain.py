from __future__ import annotations

import asyncio

from ansq import ConnectionFeatures, ConnectionOptions, create_reader, open_connection


async def test_drain_completes_when_in_flight_finishes(nsqd):
    reader = await create_reader(
        topic="drain1", channel="c1", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await reader.set_max_in_flight(3)

    producer = await open_connection(nsqd.host, nsqd.port)
    for i in range(3):
        await producer.pub("drain1", f"msg-{i}")
    await producer.close()

    held = [await reader.wait_for_message() for _ in range(3)]

    drain_task = asyncio.create_task(reader.drain(timeout=5))
    await asyncio.sleep(0.1)
    for message in held:
        await message.fin()

    assert await drain_task is True


async def test_drain_timeout_redelivers_unfinished_message(nsqd):
    reader = await create_reader(
        topic="drain2",
        channel="c2",
        nsqd_tcp_addresses=[nsqd.tcp_address],
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=1000)
        ),
    )
    await reader.set_max_in_flight(1)

    producer = await open_connection(nsqd.host, nsqd.port)
    await producer.pub("drain2", "never-finned")
    await producer.close()

    await reader.wait_for_message()  # pulled but deliberately never finned

    assert await reader.drain(timeout=0.3) is False

    # nsqd's in-flight sweep is ticked, not instant: give it a generous
    # margin past msg_timeout rather than asserting near-immediate delivery.
    nsq2 = await open_connection(nsqd.host, nsqd.port)
    await nsq2.subscribe("drain2", "c2", 1)
    redelivered = await asyncio.wait_for(nsq2.wait_for_message(), timeout=10)
    assert redelivered.body == b"never-finned"
    assert redelivered.attempts == 2
    await redelivered.fin()
    await nsq2.close()


async def test_drain_disables_auto_reconnect(nsqd):
    reader = await create_reader(
        topic="drain3", channel="c3", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    conn = reader.connections[0]
    assert conn.options.auto_reconnect is True

    await reader.drain(timeout=2)

    assert conn._auto_reconnect is False


async def test_drain_sends_rdy_zero(nsqd, monkeypatch):
    reader = await create_reader(
        topic="drain4", channel="c4", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    conn = reader.connections[0]

    sent = []
    original_rdy = conn.rdy

    async def spy_rdy(count):
        sent.append(count)
        await original_rdy(count)

    monkeypatch.setattr(conn, "rdy", spy_rdy)

    await reader.drain(timeout=2)

    assert 0 in sent
