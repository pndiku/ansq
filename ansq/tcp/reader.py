from __future__ import annotations

import asyncio
import contextlib
import random
from asyncio import AbstractEventLoop
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn

import attr

from ansq.http import NsqLookupd
from ansq.tcp.types import Client, ConnectionOptions
from ansq.utils import get_logger

if TYPE_CHECKING:
    from ansq.consumer import Consumer
    from ansq.tcp.connection import NSQConnection
    from ansq.tcp.types import NSQMessage, TCPConnection


class Reader(Client):
    """A consumer that provides an interface for reading messages from nsqd."""

    def __init__(
        self,
        topic: str,
        channel: str,
        nsqd_tcp_addresses: Sequence[str] | None = None,
        lookupd_http_addresses: Sequence[str] | None = None,
        lookupd_poll_interval: float = 60000,
        lookupd_poll_jitter: float = 0.3,
        max_in_flight: int = 1,
        connection_options: ConnectionOptions = ConnectionOptions(),
        loop: AbstractEventLoop | None = None,
    ):
        if nsqd_tcp_addresses is None:
            nsqd_tcp_addresses = []

        super().__init__(
            nsqd_tcp_addresses=nsqd_tcp_addresses or [],
            connection_options=connection_options,
        )

        if not any((self._nsqd_tcp_addresses, lookupd_http_addresses)):
            self._nsqd_tcp_addresses = ["127.0.0.1:4150"]

        self._topic = topic
        self._channel = channel
        self._max_in_flight = max_in_flight
        self._loop = loop or asyncio.get_event_loop()
        self._lookupd: Lookupd | None = None
        self._logger = get_logger(connection_options.debug, "reader")

        # RDY distribution state (see `set_max_in_flight`/`_redistribute_rdy`).
        # Distribution is opt-in: disabled until `set_max_in_flight` is first
        # called, so readers that never call it keep 0.3.x behavior (every
        # connection independently gets RDY=max_in_flight).
        self._rdy_distribution_enabled = False
        self._rdy_lock = asyncio.Lock()
        self._is_draining = False
        self._background_tasks: set[asyncio.Task] = set()

        # Set by `Consumer`/`consume()` so `drain()` can stop it first.
        self._consumer: Consumer | None = None

        # Common message queue for all connections
        self._message_queue: asyncio.Queue[NSQMessage | None] = asyncio.Queue()
        self.connection_options = attr.evolve(
            self.connection_options, message_queue=self._message_queue
        )

        # Init lookupd
        if lookupd_http_addresses:
            self._lookupd = Lookupd(
                reader=self,
                http_addresses=lookupd_http_addresses,
                poll_interval=lookupd_poll_interval,
                poll_jitter=lookupd_poll_jitter,
                loop=self._loop,
                debug=self.connection_options.debug,
            )

    async def connect(self) -> None:
        """Connect to nsqd addresses.

        Queries lookupd if specified.
        """
        await super().connect()

        if self._lookupd:
            # Do first lookup manually
            await self._lookupd.query_lookup()
            await self._lookupd.start_polling()

    async def messages(self) -> AsyncIterator[NSQMessage]:
        """Return messages from message queue."""
        while True:
            message = await self.wait_for_message()

            # One of connection is closed
            if message is None:
                # Keep the generator alive if lookupd is enabled as the reader
                # would restore or discover new connections
                if self._lookupd is not None:
                    continue

                # Keep the generator alive if auto-reconnect is enabled, as the
                # connection would be restored later by itself
                if self._is_auto_reconnect_enabled:
                    continue

                # Otherwise close the generator
                break

            yield message

    async def wait_for_message(self) -> NSQMessage | None:
        """Return a message from message queue."""
        return await self.message_queue.get()

    @property
    def topic(self) -> str:
        """Return a subscribed topic."""
        return self._topic

    @property
    def channel(self) -> str:
        """Return a subscribed channel."""
        return self._channel

    @property
    def message_queue(self) -> asyncio.Queue[NSQMessage | None]:
        """Return a message queue."""
        return self._message_queue

    @property
    def max_in_flight(self) -> int:
        """Return 'max_in_flight' number.

        Before `set_max_in_flight` is ever called, each connection
        independently subscribes with RDY=`max_in_flight` (0.3.x behavior).
        After `set_max_in_flight` is called, this becomes the *total* RDY
        budget distributed across all current connections.
        """
        return self._max_in_flight

    async def set_max_in_flight(self, count: int) -> None:
        """Update 'max_in_flight' number and redistribute RDY across connections.

        The max_in_flight is the total number of messages the reader can
        receive before nsqd expects a response, distributed across all
        current connections (go-nsq-style: as evenly as possible, with any
        remainder going to the earliest connections; connections beyond
        ``count`` get RDY 0 if there are more connections than budget). For
        more detail see the doc:
        https://nsq.io/clients/building_client_libraries.html#rdy-state

        Calling this method opts the reader into RDY-distribution mode: until
        it is called, each connection independently subscribes with RDY
        equal to the reader's `max_in_flight` (unchanged 0.3.x behavior, so
        existing consumers are unaffected).
        """
        self._max_in_flight = count
        self._rdy_distribution_enabled = True
        await self._redistribute_rdy()

    @staticmethod
    def _compute_rdy_distribution(
        total: int, connection_ids: Sequence[str]
    ) -> dict[str, int]:
        """Pure helper: split `total` RDY budget across `connection_ids`.

        Connections are consumed in the given order, so callers should pass
        them in a deterministic order (e.g. sorted by connection id).
        """
        k = len(connection_ids)
        if k == 0 or total <= 0:
            return {conn_id: 0 for conn_id in connection_ids}

        if total >= k:
            base, remainder = divmod(total, k)
            return {
                conn_id: base + (1 if i < remainder else 0)
                for i, conn_id in enumerate(connection_ids)
            }

        # total < k: only the first `total` connections get RDY 1.
        return {
            conn_id: 1 if i < total else 0 for i, conn_id in enumerate(connection_ids)
        }

    async def _redistribute_rdy(self) -> None:
        """Recompute and send the RDY distribution to all connections.

        No-op while draining, so a connection add/remove event during
        shutdown can't reopen the RDY window.
        """
        async with self._rdy_lock:
            if self._is_draining:
                return

            connections = sorted(self.connections, key=lambda conn: conn.id)
            distribution = self._compute_rdy_distribution(
                self._max_in_flight, [conn.id for conn in connections]
            )
            for connection in connections:
                if not (connection.is_connected and connection.is_subscribed):
                    continue
                share = distribution[connection.id]
                if share != connection.rdy_messages_count:
                    await connection.rdy(share)

    def _schedule_redistribute_rdy(self) -> None:
        """Fire-and-forget `_redistribute_rdy()`, e.g. from a sync callback."""
        task = self._loop.create_task(self._redistribute_rdy())
        self._background_tasks.add(task)
        task.add_done_callback(self._on_redistribute_task_done)

    def _on_redistribute_task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._logger.exception("RDY redistribution failed", exc_info=exc)

    async def connect_to_nsqd(self, host: str, port: int) -> NSQConnection:
        """Connect, identify and subscribe to nsqd by given host and port."""
        connection = await super().connect_to_nsqd(host=host, port=port)
        if not connection.is_subscribed:
            if self._rdy_distribution_enabled:
                await connection.subscribe(
                    topic=self._topic,
                    channel=self._channel,
                    messages_count=0,
                )
                await self._redistribute_rdy()
            else:
                # 0.3.x behavior: each connection independently gets the
                # full `max_in_flight` RDY budget.
                await connection.subscribe(
                    topic=self._topic,
                    channel=self._channel,
                    messages_count=self._max_in_flight,
                )
        return connection

    def remove_connection(self, connection: TCPConnection) -> None:
        """Remove connection from the pool and redistribute remaining RDY."""
        super().remove_connection(connection)
        if self._rdy_distribution_enabled:
            self._schedule_redistribute_rdy()

    @property
    def _is_auto_reconnect_enabled(self) -> bool:
        return self.connection_options.auto_reconnect

    async def close(self) -> None:
        """Close all connections."""
        if self._lookupd is not None:
            await self._lookupd.close()

        await super().close()

    async def drain(self, timeout: float | None = None) -> bool:
        """Gracefully stop consuming, drain in-flight messages, and close.

        Stops the attached `Consumer` (if any) first, then stops lookupd
        polling (so it can't re-add connections mid-drain), then drains and
        closes every connection.
        """
        self._is_draining = True

        if self._consumer is not None:
            await self._consumer.stop()

        if self._lookupd is not None:
            await self._lookupd.stop_polling()

        return await super().drain(timeout)

    async def consume(
        self,
        handler: Any,
        *,
        concurrency: int = 1,
        **consumer_kwargs: Any,
    ) -> None:
        """Build a `Consumer` bound to this reader and run it until stopped.

        The consumer is stored on the reader so `drain()` (e.g. from a
        service's SIGTERM handler) stops it before draining connections.
        """
        from ansq.consumer import Consumer

        self._consumer = Consumer(
            self, handler, concurrency=concurrency, **consumer_kwargs
        )
        await self._consumer.run()


class Lookupd:
    """Lookupd wrapper helps to connect to found producers via lookupd services."""

    def __init__(
        self,
        reader: Reader,
        http_addresses: Sequence[str],
        poll_interval: float,
        poll_jitter: float,
        loop: AbstractEventLoop | None = None,
        debug: bool = False,
    ):
        self._reader = reader
        self._poll_interval = poll_interval / 1000
        self._poll_jitter = poll_jitter
        self._loop = loop or asyncio.get_event_loop()
        self._query_lookupd_attempts = 0
        self._logger = get_logger(debug, "lookupd")
        self._debug = debug
        self._poll_lookup_task: asyncio.Task | None = None

        # Keep original on close callback to call it in `self._on_close_connection`
        self._orig_on_close_callback = self._reader.connection_options.on_close

        # Configure connections specific for lookupd
        self._reader.connection_options = attr.evolve(
            self._reader.connection_options,
            # Lookupd adds and removes connections itself,
            # so disable auto-reconnect for each connections.
            auto_reconnect=False,
            # When a connection is closed it should be removed from the reader.
            # Lookupd would add it later if the producer is up.
            on_close=self._on_close_connection,
        )

        # Create lookupd connections
        self._lookupd_connections = [
            NsqLookupd.from_address(address) for address in http_addresses
        ]

    async def query_lookup(self) -> None:
        """Query lookupd for topic producers and connect to them."""
        # Get lookupd connection in a round robin fashion way
        lookupd_connection = self._get_lookupd_connection()

        # Try to query lookupd connection
        try:
            producer_addresses = await self._do_query_lookup(lookupd_connection)
        except Exception as exc:
            self._logger.error(
                "Failed to query lookupd %s due to: %s",
                lookupd_connection,
                exc,
                exc_info=exc if self._debug else False,
            )
            return

        # Connect to all producers addresses
        for address in producer_addresses:
            await self._reader.connect_to_nsqd(address.host, address.port)

    async def poll_lookup(self) -> NoReturn:
        """Poll ``query_lookup()`` infinitely."""
        # Add a delay to poll which helps to distribute evenly requests
        # even if multiple readers restart at the same time.
        delay = self._poll_interval * self._poll_jitter
        await asyncio.sleep(random.random() * delay)

        # Poll infinitely lookup
        while True:
            await asyncio.sleep(self._poll_interval)
            await self.query_lookup()

    async def start_polling(self) -> None:
        """Start polling lookupd."""
        # Polling is already started
        if self._poll_lookup_task is not None and not self._poll_lookup_task.done():
            return

        # Start polling task
        self._poll_lookup_task = self._loop.create_task(self.poll_lookup())

    async def stop_polling(self) -> None:
        """Stop polling lookupd."""
        if self._poll_lookup_task is None:
            return

        self._poll_lookup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._poll_lookup_task

    async def close(self) -> None:
        """Close all lookupd connections and stop poll lookup task."""
        for lookupd_connection in self._lookupd_connections:
            await lookupd_connection.close()

        await self.stop_polling()

    def _get_lookupd_connection(self) -> NsqLookupd:
        """Return lookupd connection in a round robin fashion way."""
        index = self._query_lookupd_attempts % len(self._lookupd_connections)
        lookupd_connection = self._lookupd_connections[index]
        self._query_lookupd_attempts += 1
        return lookupd_connection

    async def _do_query_lookup(self, lookupd_connection: NsqLookupd) -> list[Address]:
        """Query lookup with a given connection and return producer addresses."""
        # Lookup for the reader's topic
        self._logger.debug("Query %s", lookupd_connection)
        lookup_response = await lookupd_connection.lookup(self._reader.topic)
        if not isinstance(lookup_response, dict):
            raise ValueError(f"lookup response must be a dict: {lookup_response}")

        # Get producers from the result response
        return self._get_producer_addresses(lookup_response)

    @staticmethod
    def _get_producer_addresses(response: dict[str, Any]) -> list[Address]:
        """Return a list of producers addresses from parsed lookup response."""
        producers = response.get("producers")
        if producers is None:
            raise ValueError("producers not found in response data")

        if not isinstance(producers, list):
            raise ValueError(f"producers must be a list: {producers}")

        addresses: list[Address] = []

        for producer in producers:
            if not isinstance(producer, dict):
                raise ValueError(f"producer must be a dict: {producer}")

            host = producer["broadcast_address"]
            port = int(producer["tcp_port"])
            addresses.append(Address(host, port))

        return addresses

    def _on_close_connection(self, connection: TCPConnection) -> None:
        """A callback to be called after a connection being closed."""
        # Remove the connection from the reader so that lookupd could add it later
        self._reader.remove_connection(connection)

        # Call an original on_close callback if specified
        if self._orig_on_close_callback is not None:
            self._orig_on_close_callback(connection)


class Address(NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


async def create_reader(
    topic: str,
    channel: str,
    nsqd_tcp_addresses: Sequence[str] | None = None,
    lookupd_http_addresses: Sequence[str] | None = None,
    lookupd_poll_interval: float = 60000,
    lookupd_poll_jitter: float = 0.3,
    max_in_flight: int = 1,
    connection_options: ConnectionOptions = ConnectionOptions(),
) -> Reader:
    """Return created and connected reader."""
    reader = Reader(
        topic=topic,
        channel=channel,
        nsqd_tcp_addresses=nsqd_tcp_addresses,
        lookupd_http_addresses=lookupd_http_addresses,
        lookupd_poll_interval=lookupd_poll_interval,
        lookupd_poll_jitter=lookupd_poll_jitter,
        max_in_flight=max_in_flight,
        connection_options=connection_options,
    )
    await reader.connect()
    return reader
