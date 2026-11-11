from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import attr

from ansq.utils import get_logger

if TYPE_CHECKING:
    from ansq.tcp.types import NSQMessage

_logger = get_logger(unique_name="hooks")

__all__ = ["BackoffSettings", "ConsumerMetrics"]


@attr.define(kw_only=True)
class ConsumerMetrics:
    """Optional observability callbacks for `Consumer`.

    All callbacks are plain, synchronous callables. Each invocation is
    wrapped in a try/except by the consumer: a broken metrics callback must
    never interrupt message processing.
    """

    on_message_started: Callable[[NSQMessage], None] | None = None
    on_message_finished: Callable[[NSQMessage, float], None] | None = None
    on_message_requeued: Callable[[NSQMessage, BaseException | None], None] | None = (
        None
    )
    on_max_attempts: Callable[[NSQMessage], None] | None = None
    on_in_flight_change: Callable[[int], None] | None = None
    on_backoff_change: Callable[[int], None] | None = None

    def _call(self, fn: Callable | None, *args: object) -> None:
        if fn is None:
            return
        try:
            fn(*args)
        except Exception:
            _logger.exception("Consumer metrics callback raised")


@attr.define(frozen=True, kw_only=True)
class BackoffSettings:
    """Failure backoff policy for `Consumer`.

    On consecutive handler failures the consumer lowers total RDY to 0, then
    ramps back up: RDY 1 to test the waters, then the full `concurrency`
    once a message succeeds. Mirrors go-nsq's per-connection backoff, but
    applied to the consumer's total RDY budget.
    """

    enabled: bool = True
    base_delay: float = 1.0
    max_delay: float = 120.0
