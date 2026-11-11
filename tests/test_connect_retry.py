from __future__ import annotations

import asyncio
import contextlib
import socket
import time

import pytest

from ansq import ConnectionOptions, open_connection


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def test_connect_attempts_default_fails_fast():
    """connect_attempts=1 (default) preserves 0.3.x fail-fast behavior."""
    port = _free_port()
    start = time.monotonic()
    with pytest.raises(OSError):
        await open_connection("127.0.0.1", port)
    assert time.monotonic() - start < 1.0


async def test_connect_attempts_retries_then_gives_up():
    port = _free_port()
    start = time.monotonic()
    with pytest.raises(OSError):
        await open_connection(
            "127.0.0.1",
            port,
            connection_options=ConnectionOptions(
                connect_attempts=3,
                connect_retry_initial_delay=0.1,
                connect_retry_max_delay=0.2,
            ),
        )
    # 2 retries: sleeps ~0.1s then ~0.2s between the 3 attempts.
    assert time.monotonic() - start >= 0.25


async def test_connect_attempts_succeeds_once_nsqd_comes_up(create_nsqd):
    port = _free_port()

    async def start_nsqd_late():
        await asyncio.sleep(0.4)
        async with create_nsqd(port=port):
            await asyncio.sleep(5)

    task = asyncio.create_task(start_nsqd_late())
    try:
        nsq = await open_connection(
            "127.0.0.1",
            port,
            connection_options=ConnectionOptions(
                connect_attempts=10,
                connect_retry_initial_delay=0.1,
                connect_retry_max_delay=0.3,
            ),
        )
        assert nsq.status.is_connected
        await nsq.close()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
