"""Signal-aware lifecycle helpers for CLI programs and async daemons."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from types import FrameType
from typing import Any

from pyev.integrations.asgi import LifecycleBroker


@asynccontextmanager
async def shutdown_signals(
    signals: Iterable[signal.Signals] = (signal.SIGINT, signal.SIGTERM),
) -> AsyncIterator[asyncio.Event]:
    """Yield an event set by SIGINT/SIGTERM and restore prior handlers safely."""

    loop = asyncio.get_running_loop()
    event = asyncio.Event()
    installed: list[signal.Signals] = []
    previous: dict[signal.Signals, Any] = {}

    def set_event(_number: int | None = None, _frame: FrameType | None = None) -> None:
        loop.call_soon_threadsafe(event.set)

    try:
        for selected in tuple(dict.fromkeys(signals)):
            try:
                loop.add_signal_handler(selected, event.set)
            except (NotImplementedError, RuntimeError):
                previous[selected] = signal.getsignal(selected)
                signal.signal(selected, set_event)
            installed.append(selected)
        yield event
    finally:
        for selected in installed:
            if selected in previous:
                signal.signal(selected, previous[selected])
            else:
                loop.remove_signal_handler(selected)


@asynccontextmanager
async def broker_daemon(
    broker: LifecycleBroker,
    *,
    signals: Iterable[signal.Signals] = (signal.SIGINT, signal.SIGTERM),
) -> AsyncIterator[asyncio.Event]:
    """Start a broker, expose a shutdown event, then close it deterministically."""

    async with shutdown_signals(signals) as stop:
        await broker.startup()
        try:
            yield stop
        finally:
            await broker.shutdown()


async def serve_until_stopped(
    broker: LifecycleBroker,
    *,
    stop: asyncio.Event | None = None,
) -> None:
    """Run a broker until an injected event or an OS termination signal fires."""

    if stop is not None:
        await broker.startup()
        try:
            await stop.wait()
        finally:
            await broker.shutdown()
        return
    async with broker_daemon(broker) as signal_stop:
        await signal_stop.wait()


__all__ = ["broker_daemon", "serve_until_stopped", "shutdown_signals"]
