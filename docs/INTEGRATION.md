# Integration guide: consuming NSQ with ansq

Consumer patterns for ansq 0.4.0. Application-specific retry and dead-letter policy remains in the application.

## Canonical consumer skeleton

```python
import asyncio
import signal

from ansq import ConnectionOptions, Consumer, create_reader


async def handle(message):
    # Raise to requeue; call message.fin()/req() yourself for advanced
    # cases (e.g. skip retry on a non-retryable error).
    await process(message.body)


async def publish_to_dlq(message):
    # Runs once attempts is exhausted, INSTEAD of dispatching to `handle`.
    # This is where service-specific DLQ policy lives, not in the library.
    await report_dead_letter(topic=reader.topic, body=message.body)


async def main():
    reader = await create_reader(
        topic="accounting",
        channel="default",
        nsqd_tcp_addresses=[settings.NSQD_HOST_PORT],
        connection_options=ConnectionOptions(
            connect_attempts=30,               # replaces a tenacity wrapper
            connect_retry_initial_delay=1.0,
            connect_retry_max_delay=10.0,
        ),
    )

    consumer = Consumer(
        reader,
        handle,
        concurrency=settings.NSQD_MAX_IN_FLIGHT,  # RDY == concurrency, enforced
        max_attempts=settings.NSQD_MAX_TRIES,
        on_max_attempts=publish_to_dlq,
        touch_interval=20.0,       # optional: for handlers that can run long
        max_touch_duration=90.0,   # hard cap so a hung handler still times out
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda: asyncio.ensure_future(reader.drain(timeout=30))
        )

    await consumer.run()  # blocks until reader.drain() stops it


asyncio.run(main())
```

`reader.drain(timeout)` stops the attached `Consumer` first, then drains and
closes every connection — one call from a signal handler does the whole
graceful shutdown (RDY 0 → wait for in-flight → CLS → close).

## Low-level connection variant

Workers that need per-connection control can hand a raw
`NSQConnection` to `Consumer` instead of a `Reader`:

```python
from ansq import ConnectionOptions, Consumer, open_connection

nsq = await open_connection(
    settings.NSQD_HOST,
    settings.NSQD_PORT,
    connection_options=ConnectionOptions(connect_attempts=30),
)
await nsq.subscribe(settings.NSQ_TOPIC, "default", messages_count=0)

consumer = Consumer(nsq, handle, concurrency=settings.NSQD_MAX_IN_FLIGHT)
await consumer.run()

# shutdown:
await consumer.stop()
await nsq.drain(timeout=30)
```

## Producer guidance

`Writer.pub()`/`dpub()`/`mpub()` now raise `ConnectionClosedError` (instead
of an opaque `IndexError`) when there is no open connection to publish on.
There is no built-in pub-retry — if a service needs one, wrap the `pub()`
call the same way the old connect-retry wrapper worked. Outbox/dedup/accounting
semantics stay entirely in the service.

## Migration map

| Old hand-rolled pattern | New API |
|---|---|
| `tenacity`-wrapped `open_connection()`/`connect()` | `ConnectionOptions(connect_attempts=..., connect_retry_initial_delay=..., connect_retry_max_delay=...)` |
| `async for m in nsq.messages(): try: ... fin()/req() except: ...` loop | `Consumer(source, handler, concurrency=...)` |
| `if message.attempts >= MAX_TRIES: publish_dlq(); await message.fin()` | `max_attempts=` + `on_max_attempts=` |
| Manual RDY bookkeeping / semaphore to bound concurrency | `concurrency=` (RDY is kept in lockstep automatically) |
| Ad-hoc or missing SIGTERM handling | `await consumer.stop(); await reader.drain(timeout)` (or just `reader.drain(timeout)` if a `Consumer` is attached) |
| No `touch()` on long provider calls | `touch_interval=` / `max_touch_duration=` |

## Semantics reference

- **RDY is a standing window, not a consumable credit.** `RDY N` sustains N
  concurrent in-flight messages indefinitely; `FIN`/`REQ` each reopen the
  window by one. No periodic "topping up" is needed — `Consumer` relies on
  this and only touches RDY on start, stop, backoff, and `max_in_flight`
  changes.
- **`msg_timeout` vs `touch()`**: `msg_timeout` (set via `ConnectionFeatures`)
  is how long nsqd waits before redeliving an unacked message. `touch()`
  resets that clock for one message without finishing it — use
  `touch_interval` for handlers whose runtime can approach `msg_timeout`.
- **`attempts` grows on every redelivery**, including consumer-side `req()`
  and nsqd-side timeout-driven redelivery. Pair `max_attempts` with a
  `requeue_delay` that gives transient failures (a flaky downstream call)
  a real chance to succeed before giving up.
- **RDY distribution is opt-in.** A `Reader` that never calls
  `set_max_in_flight()` keeps giving every connection its own full
  `max_in_flight` RDY (unchanged from 0.3.x). Calling `set_max_in_flight()`
  — which `Consumer` does automatically — switches the reader into
  distributing a single total RDY budget across all connections. This only
  matters for multi-nsqd readers (via `lookupd`).
- **Real nsqd's in-flight/deferred sweep is ticked**, not instant. A
  `msg_timeout` or `req(delay)` of, say, 500ms can take noticeably longer
  in practice to actually redeliver. Don't assume near-immediate redelivery
  when tuning these values or writing tests against real nsqd.

## What does not belong in this library

Outbox writes, dedup markers, DLQ message *payload* shape, and accounting/
money-event semantics are service policy, not transport concerns — they
stay in each service.
