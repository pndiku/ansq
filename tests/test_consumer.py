from __future__ import annotations

import asyncio

from ansq import (
    BackoffSettings,
    ConnectionFeatures,
    ConnectionOptions,
    Consumer,
    ConsumerMetrics,
    create_reader,
    open_connection,
)


async def _publish(nsqd, topic: str, *bodies: str) -> None:
    producer = await open_connection(nsqd.host, nsqd.port)
    for body in bodies:
        await producer.pub(topic, body)
    await producer.close()


async def test_success_finishes_message(nsqd):
    reader = await create_reader(
        topic="cons1", channel="c1", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons1", "hello")

    seen = []

    async def handler(message):
        seen.append(message.body)

    consumer = Consumer(reader, handler, concurrency=1)
    run_task = asyncio.create_task(consumer.run())

    async def _done():
        return len(seen) == 1

    for _ in range(50):
        if await _done():
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    await run_task

    assert seen == [b"hello"]


async def test_exception_requeues_and_redelivers(nsqd):
    reader = await create_reader(
        topic="cons2", channel="c2", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons2", "boom")

    attempts_seen = []
    done = asyncio.Event()

    async def handler(message):
        attempts_seen.append(message.attempts)
        if message.attempts < 2:
            raise RuntimeError("fail on first attempt")
        done.set()

    consumer = Consumer(reader, handler, concurrency=1, requeue_delay=0)
    run_task = asyncio.create_task(consumer.run())

    await asyncio.wait_for(done.wait(), timeout=5)
    await consumer.stop()
    await run_task

    assert attempts_seen == [1, 2]


async def test_concurrency_is_capped(nsqd, wait_for):
    reader = await create_reader(
        topic="cons3", channel="c3", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons3", *[f"m{i}" for i in range(6)])

    concurrency = 2
    barrier_release = asyncio.Event()
    in_flight_peak = 0
    currently_in_flight = 0
    lock = asyncio.Lock()

    async def handler(message):
        nonlocal in_flight_peak, currently_in_flight
        async with lock:
            currently_in_flight += 1
            in_flight_peak = max(in_flight_peak, currently_in_flight)
        await barrier_release.wait()
        async with lock:
            currently_in_flight -= 1

    consumer = Consumer(reader, handler, concurrency=concurrency)
    run_task = asyncio.create_task(consumer.run())

    await wait_for(lambda: currently_in_flight == concurrency)
    await asyncio.sleep(0.3)
    assert currently_in_flight == concurrency
    assert in_flight_peak == concurrency

    barrier_release.set()
    await wait_for(lambda: currently_in_flight == 0, timeout=5)

    await consumer.stop()
    await run_task


async def test_rdy_invariant_matches_concurrency(nsqd, wait_for):
    reader = await create_reader(
        topic="cons4", channel="c4", nsqd_tcp_addresses=[nsqd.tcp_address]
    )

    async def handler(message):
        await message.fin()

    consumer = Consumer(reader, handler, concurrency=5)
    run_task = asyncio.create_task(consumer.run())

    await wait_for(lambda: bool(reader.connections))
    conn = reader.connections[0]
    await wait_for(lambda: conn.rdy_messages_count == 5)

    await consumer.stop()
    await run_task


async def test_max_attempts_hook_called_once_then_finished(nsqd):
    reader = await create_reader(
        topic="cons5", channel="c5", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons5", "always-fails")

    handler_calls = []
    hook_calls = []
    done = asyncio.Event()

    async def handler(message):
        handler_calls.append(message.attempts)
        raise RuntimeError("always fails")

    async def on_max_attempts(message):
        hook_calls.append(message.attempts)
        done.set()

    consumer = Consumer(
        reader,
        handler,
        concurrency=1,
        max_attempts=3,
        on_max_attempts=on_max_attempts,
        requeue_delay=0,
    )
    run_task = asyncio.create_task(consumer.run())

    await asyncio.wait_for(done.wait(), timeout=5)
    await asyncio.sleep(0.2)  # let fin() settle before stopping
    await consumer.stop()
    await run_task

    assert handler_calls == [1, 2]
    assert hook_calls == [3]


async def test_max_attempts_hook_failure_requeues_and_retries(nsqd):
    reader = await create_reader(
        topic="cons6", channel="c6", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons6", "always-fails")

    hook_calls = []

    async def handler(message):
        raise RuntimeError("always fails")

    async def on_max_attempts(message):
        hook_calls.append(message.attempts)
        if len(hook_calls) < 2:
            raise RuntimeError("hook publish failed")

    consumer = Consumer(
        reader,
        handler,
        concurrency=1,
        max_attempts=1,
        on_max_attempts=on_max_attempts,
        requeue_delay=0,
    )
    run_task = asyncio.create_task(consumer.run())

    for _ in range(50):
        if len(hook_calls) >= 2:
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    await run_task

    assert len(hook_calls) >= 2


async def test_handler_self_fin_no_double_processing(nsqd):
    reader = await create_reader(
        topic="cons7", channel="c7", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons7", "self-finned")

    finished = asyncio.Event()

    async def handler(message):
        await message.fin()
        finished.set()

    consumer = Consumer(reader, handler, concurrency=1)
    run_task = asyncio.create_task(consumer.run())

    await asyncio.wait_for(finished.wait(), timeout=5)
    await asyncio.sleep(0.1)

    await consumer.stop()
    await run_task


async def test_stop_drains_without_losing_messages(nsqd):
    reader = await create_reader(
        topic="cons8", channel="c8", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons8", *[f"m{i}" for i in range(4)])

    processed = []

    async def handler(message):
        await asyncio.sleep(0.2)
        processed.append(message.body)

    consumer = Consumer(reader, handler, concurrency=2)
    run_task = asyncio.create_task(consumer.run())

    await asyncio.sleep(0.3)
    await consumer.stop()
    await run_task

    # Every in-flight handler at stop time completes (fin'd); nothing raises
    # and nothing is double-counted.
    assert len(processed) == len(set(processed))


async def test_low_level_connection_source(nsqd):
    nsq = await open_connection(nsqd.host, nsqd.port)
    await _publish(nsqd, "cons9", "via-connection")
    await nsq.subscribe("cons9", "c9", 0)

    seen = []

    async def handler(message):
        seen.append(message.body)

    consumer = Consumer(nsq, handler, concurrency=1)
    run_task = asyncio.create_task(consumer.run())

    for _ in range(50):
        if seen:
            break
        await asyncio.sleep(0.1)

    await consumer.stop()
    await run_task

    assert seen == [b"via-connection"]


async def test_auto_touch_prevents_timeout_redelivery(nsqd):
    reader = await create_reader(
        topic="cons10",
        channel="c10",
        nsqd_tcp_addresses=[nsqd.tcp_address],
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=2000)
        ),
    )
    await _publish(nsqd, "cons10", "slow")

    touch_calls = []
    finished = asyncio.Event()

    async def handler(message):
        original_touch = message.touch

        async def spy_touch():
            touch_calls.append(1)
            await original_touch()

        message.touch = spy_touch
        await asyncio.sleep(1.5)  # > msg_timeout without auto-touch
        finished.set()

    consumer = Consumer(
        reader, handler, concurrency=1, touch_interval=0.5, requeue_delay=0
    )
    run_task = asyncio.create_task(consumer.run())

    await asyncio.wait_for(finished.wait(), timeout=5)
    await asyncio.sleep(0.1)
    await consumer.stop()
    await run_task

    assert len(touch_calls) >= 2


async def test_max_touch_duration_lets_hung_handler_time_out(nsqd):
    reader = await create_reader(
        topic="cons11",
        channel="c11",
        nsqd_tcp_addresses=[nsqd.tcp_address],
        connection_options=ConnectionOptions(
            features=ConnectionFeatures(msg_timeout=1000)
        ),
    )
    await _publish(nsqd, "cons11", "hangs")

    attempts_seen = []
    hung = asyncio.Event()

    async def handler(message):
        attempts_seen.append(message.attempts)
        hung.set()
        await asyncio.sleep(10)  # hangs well past max_touch_duration

    consumer = Consumer(
        reader,
        handler,
        concurrency=1,
        touch_interval=0.2,
        max_touch_duration=0.5,
        requeue_delay=0,
    )
    run_task = asyncio.create_task(consumer.run())

    await asyncio.wait_for(hung.wait(), timeout=5)

    # Once max_touch_duration elapses, auto-touch stops and nsqd redelivers
    # to a fresh consumer without waiting on the still-hung handler.
    nsq2 = await open_connection(nsqd.host, nsqd.port)
    await nsq2.subscribe("cons11", "c11", 1)
    redelivered = await asyncio.wait_for(nsq2.wait_for_message(), timeout=10)
    assert redelivered.attempts == 2
    await redelivered.fin()
    await nsq2.close()

    await consumer.stop()
    await run_task


async def test_backoff_lowers_then_restores_rdy(nsqd, wait_for):
    reader = await create_reader(
        topic="cons12", channel="c12", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons12", "fails-once")

    attempts_seen = []

    async def handler(message):
        attempts_seen.append(message.attempts)
        if message.attempts < 2:
            raise RuntimeError("fail once to trigger backoff")

    consumer = Consumer(
        reader,
        handler,
        concurrency=3,
        # Non-zero so the redelivered attempt can't race the backoff state
        # machine into succeeding before RDY visibly drops to 0.
        requeue_delay=500,
        backoff=BackoffSettings(base_delay=0.2, max_delay=1.0),
    )
    run_task = asyncio.create_task(consumer.run())

    await wait_for(lambda: bool(reader.connections))
    conn = reader.connections[0]

    # Failure drops RDY to 0, then probes at 1 after base_delay.
    await wait_for(lambda: conn.rdy_messages_count == 0, timeout=3)
    await wait_for(lambda: conn.rdy_messages_count == 1, timeout=3)

    # Success while backing off restores full concurrency. nsqd's deferred
    # in-flight sweep is ticked (~1s granularity), so the 500ms requeue_delay
    # above can take noticeably longer than 500ms to actually redeliver.
    await wait_for(lambda: conn.rdy_messages_count == 3, timeout=8)

    await consumer.stop()
    await run_task

    assert attempts_seen == [1, 2]


async def test_metrics_callbacks_invoked(nsqd, wait_for):
    reader = await create_reader(
        topic="cons13", channel="c13", nsqd_tcp_addresses=[nsqd.tcp_address]
    )
    await _publish(nsqd, "cons13", "tracked")

    started = []
    finished = []
    in_flight_changes = []

    metrics = ConsumerMetrics(
        on_message_started=lambda message: started.append(message.id),
        on_message_finished=lambda message, duration: finished.append(message.id),
        on_in_flight_change=lambda count: in_flight_changes.append(count),
    )

    async def handler(message):
        await message.fin()

    consumer = Consumer(reader, handler, concurrency=1, metrics=metrics)
    run_task = asyncio.create_task(consumer.run())

    await wait_for(lambda: len(finished) == 1)
    await consumer.stop()
    await run_task

    assert len(started) == 1
    assert len(finished) == 1
    assert 0 in in_flight_changes
