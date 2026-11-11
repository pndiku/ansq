# Changelog

## 0.4.1

- Build wheel and source distributions in GitHub Actions.

## 0.4.0

Transport-level consumer features. See `docs/INTEGRATION.md` for usage.

### Added

- `Consumer` (`ansq.Consumer`): bounded-concurrency consumer runner for both
  `Reader` and low-level `NSQConnection` sources. Runs up to `concurrency`
  handlers at once, `fin()`s on success, `req()`s on exception. Supports:
  - `max_attempts` + `on_max_attempts`: a hook invoked once a message's
    attempt count is exhausted, instead of dispatching to the handler again.
    The hook is a mechanism, not a policy — DLQ publishing stays in the
    service.
  - `touch_interval` / `max_touch_duration`: opt-in auto-touch for
    long-running handlers, bounded so a hung handler can't be touched
    forever.
  - `backoff` (`BackoffSettings`): on consecutive handler failures, lowers
    RDY to 0, then probes at 1, then restores full concurrency on success.
  - `metrics` (`ConsumerMetrics`): optional observability callbacks
    (message started/finished/requeued, max-attempts, in-flight, backoff
    level, connection state).
  - `Reader.consume(handler, concurrency=...)` convenience wrapper.
- `Reader.set_max_in_flight()` is now implemented (previously always raised
  `NotImplementedError`). Distributes the total RDY budget across all
  connections (go-nsq style: as evenly as possible, remainder to the
  earliest connections), and keeps redistributing as connections are
  added/removed (e.g. via lookupd).
- `NSQConnection.drain()` / `Client.drain()` / `Reader.drain()`: graceful
  shutdown — RDY 0, wait for in-flight messages to be acked (bounded by
  `timeout`), send `CLS`, then close. Disables auto-reconnect for the
  duration so a reconnect can't reopen the RDY window mid-drain.
  `Reader.drain()` stops an attached `Consumer` first, so a single call
  from a SIGTERM handler does the full shutdown.
- Initial-connect retry: `ConnectionOptions.connect_attempts` (default `1`,
  no behavior change), `connect_retry_initial_delay`, `connect_retry_max_delay`.
  Applies to `Client.connect_to_nsqd()` and the module-level `open_connection()`.
- `ConnectionOptions.on_status_change` callback for connection lifecycle
  observability.

### Fixed

- `ConnectionOptions._evolve()` silently dropped every kwarg past the first
  when passed 2+ deprecated keyword arguments.
- `Writer._get_random_open_connection()` raised an unhelpful `IndexError`
  when publishing with no open connections; now raises `ConnectionClosedError`
  with a clear message.
- Stale `Reader.max_in_flight` docstring.

### Behavior notes (not breaking for existing single-nsqd consumers)

- RDY distribution across connections is **opt-in**: it activates the first
  time `set_max_in_flight()` is called. Readers that never call it keep the
  exact 0.3.x behavior — every connection independently subscribes with
  `RDY = max_in_flight`. Once distribution is enabled, a multi-connection
  reader's *total* in-flight budget changes from `k × max_in_flight` to
  `max_in_flight`. This matters only when explicitly enabled.

### Changed

- Python 3.12 or newer is required; CI tests Python 3.12–3.14.
