from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from ansq.hooks import BackoffSettings, ConsumerMetrics
from ansq.tcp.consts import DEFAULT_REQ_TIMEOUT
from ansq.tcp.exceptions import ConnectionClosedError
from ansq.utils import get_logger

if TYPE_CHECKING:
    from ansq.tcp.connection import NSQConnection
    from ansq.tcp.reader import Reader
    from ansq.tcp.types import NSQMessage

__all__ = ["Consumer"]

_logger = get_logger(unique_name="consumer")

HandlerT = Callable[["NSQMessage"], Awaitable[None]]
OnMaxAttemptsT = Callable[["NSQMessage"], Awaitable[None]]


class Consumer:
    """Bounded-concurrency consumer runner.

    Drives messages from a `Reader` or a low-level `NSQConnection`, running
    up to `concurrency` handlers at once, auto `fin()`-ing on success and
    `req()`-ing on handler exceptions. Enforces the invariant that total RDY
    equals `concurrency` — the local semaphore and nsqd's RDY window stay in
    lockstep because both key off handler completion (fin/req).

    Policy that differs per service (accounting, dedup, DLQ payloads, outbox)
    does not belong here: `on_max_attempts` is a hook, not a DLQ implementation.
    """

    def __init__(
        self,
        source: Reader | NSQConnection,
        handler: HandlerT,
        *,
        concurrency: int = 1,
        max_attempts: int | None = None,
        on_max_attempts: OnMaxAttemptsT | None = None,
        requeue_delay: int = DEFAULT_REQ_TIMEOUT,
        touch_interval: float | None = None,
        max_touch_duration: float | None = None,
        backoff: BackoffSettings | None = None,
        metrics: ConsumerMetrics | None = None,
        drain_timeout: float = 30.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")

        self._source = source
        self._handler = handler
        self._concurrency = concurrency
        self._max_attempts = max_attempts
        self._on_max_attempts = on_max_attempts
        self._requeue_delay = requeue_delay
        self._touch_interval = touch_interval
        self._max_touch_duration = max_touch_duration
        self._backoff = backoff
        self._metrics = metrics or ConsumerMetrics()
        self._drain_timeout = drain_timeout

        self._semaphore = asyncio.Semaphore(concurrency)
        self._tasks: set[asyncio.Task] = set()
        self._stopping = asyncio.Event()
        self._stopped = asyncio.Event()
        self._run_task: asyncio.Task | None = None
        self._backoff_level = 0
        self._backoff_task: asyncio.Task | None = None

    @property
    def in_flight(self) -> int:
        """Number of handler tasks currently running."""
        return len(self._tasks)

    async def run(self) -> None:
        """Consume messages until `stop()` is called. Blocks until stopped."""
        self._run_task = asyncio.current_task()

        if self._stopping.is_set():
            self._stopped.set()
            return

        await self._set_total_rdy(self._concurrency)
        try:
            async for message in self._source.messages():
                if self._stopping.is_set():
                    await self._safe_req(message, self._requeue_delay)
                    break

                await self._semaphore.acquire()
                if self._stopping.is_set():
                    self._semaphore.release()
                    await self._safe_req(message, self._requeue_delay)
                    break

                task = asyncio.create_task(self._handle(message))
                self._tasks.add(task)
                task.add_done_callback(self._on_task_done)
        except asyncio.CancelledError:
            pass
        finally:
            self._stopped.set()

    async def stop(self, drain: bool = True) -> None:
        """Stop consuming: RDY 0, let in-flight handlers finish, requeue the rest.

        Idempotent. `drain` is accepted for API symmetry with
        `Reader.drain`/`NSQConnection.drain`; the runner always requeues
        undelivered local messages and waits (bounded by `drain_timeout`)
        for in-flight handlers regardless.
        """
        if self._stopping.is_set():
            await self._stopped.wait()
            return

        self._stopping.set()
        await self._set_total_rdy(0)
        self._cancel_backoff_task()

        if self._run_task is not None and not self._run_task.done():
            self._run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._run_task
        self._stopped.set()

        # Requeue any message already pulled off the wire but not yet
        # dispatched to a handler, so it redelivers immediately rather than
        # waiting out msg_timeout.
        queue = self._source.message_queue
        while True:
            try:
                leftover = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if leftover is None:
                continue
            await self._safe_req(leftover, 0)

        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=self._drain_timeout)
            for task in pending:
                task.cancel()

    async def _set_total_rdy(self, count: int) -> None:
        if hasattr(self._source, "set_max_in_flight"):
            await self._source.set_max_in_flight(count)
        else:
            await self._source.rdy(count)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        self._semaphore.release()
        self._safe_metric(self._metrics.on_in_flight_change, len(self._tasks))

        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _logger.exception("Consumer handler task failed unexpectedly", exc_info=exc)

    async def _handle(self, message: NSQMessage) -> None:
        if self._max_attempts is not None and message.attempts >= self._max_attempts:
            await self._handle_max_attempts(message)
            return

        started = time.monotonic()
        touch_task: asyncio.Task | None = None
        if self._touch_interval:
            touch_task = asyncio.create_task(self._auto_touch(message))

        try:
            self._safe_metric(self._metrics.on_message_started, message)
            await self._handler(message)
            if not message.is_processed:
                await message.fin()
            await self._on_success()
            self._safe_metric(
                self._metrics.on_message_finished, message, time.monotonic() - started
            )
        except Exception as exc:
            _logger.exception("Handler raised for message id=%s", message.id)
            await self._safe_req(message, self._requeue_delay)
            await self._on_failure()
            self._safe_metric(self._metrics.on_message_requeued, message, exc)
        finally:
            if touch_task is not None:
                touch_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await touch_task

    async def _handle_max_attempts(self, message: NSQMessage) -> None:
        try:
            if self._on_max_attempts is not None:
                await self._on_max_attempts(message)
            if not message.is_processed:
                await message.fin()
            self._safe_metric(self._metrics.on_max_attempts, message)
        except Exception:
            # Never fin a message whose max-attempts hook (e.g. a DLQ
            # publish) failed to run — that would silently drop it.
            _logger.exception(
                "on_max_attempts hook failed for message id=%s", message.id
            )
            await self._safe_req(message, self._requeue_delay)

    async def _auto_touch(self, message: NSQMessage) -> None:
        assert self._touch_interval is not None
        started = time.monotonic()
        while True:
            await asyncio.sleep(self._touch_interval)
            if message.is_processed:
                return
            if (
                self._max_touch_duration is not None
                and time.monotonic() - started >= self._max_touch_duration
            ):
                _logger.warning(
                    "max_touch_duration exceeded for message id=%s; "
                    "letting it time out",
                    message.id,
                )
                return
            try:
                await message.touch()
            except (RuntimeWarning, ConnectionClosedError):
                return

    async def _safe_req(self, message: NSQMessage, timeout: int) -> None:
        if message.is_processed:
            return
        try:
            await message.req(timeout)
        except (RuntimeWarning, ConnectionClosedError):
            pass

    def _safe_metric(self, fn: Callable | None, *args: Any) -> None:
        if fn is None:
            return
        try:
            fn(*args)
        except Exception:
            _logger.exception("Consumer metrics callback raised")

    async def _on_failure(self) -> None:
        if self._backoff is None or not self._backoff.enabled:
            return

        self._backoff_level += 1
        self._safe_metric(self._metrics.on_backoff_change, self._backoff_level)
        await self._set_total_rdy(0)

        delay = min(
            self._backoff.base_delay * (2 ** (self._backoff_level - 1)),
            self._backoff.max_delay,
        )
        self._cancel_backoff_task()
        self._backoff_task = asyncio.create_task(self._resume_after_backoff(delay))

    async def _on_success(self) -> None:
        if self._backoff_level == 0:
            return

        self._backoff_level -= 1
        self._safe_metric(self._metrics.on_backoff_change, self._backoff_level)
        self._cancel_backoff_task()
        if self._backoff_level == 0:
            await self._set_total_rdy(self._concurrency)
        else:
            await self._set_total_rdy(1)

    async def _resume_after_backoff(self, delay: float) -> None:
        await asyncio.sleep(delay)
        if self._stopping.is_set():
            return
        await self._set_total_rdy(1)

    def _cancel_backoff_task(self) -> None:
        if self._backoff_task is not None and not self._backoff_task.done():
            self._backoff_task.cancel()
        self._backoff_task = None
