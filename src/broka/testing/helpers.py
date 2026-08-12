"""Async assertions and scoped broker overrides for application tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token

_BROKER_OVERRIDE: ContextVar[object | None] = ContextVar("pyev_testing_broker", default=None)


@asynccontextmanager
async def broker_override[T](broker: T) -> AsyncIterator[T]:
    """Temporarily expose a broker override in the current async context."""

    token: Token[object | None] = _BROKER_OVERRIDE.set(broker)
    try:
        yield broker
    finally:
        _BROKER_OVERRIDE.reset(token)


def get_broker_override() -> object | None:
    """Return the scoped testing broker, if one is active."""

    return _BROKER_OVERRIDE.get()


async def eventually(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 1.0,
    interval: float = 0.01,
) -> None:
    """Wait until a predicate becomes true or raise an assertion timeout."""

    if timeout <= 0 or interval < 0:
        raise ValueError("timeout must be positive and interval non-negative")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if loop.time() >= deadline:
            raise AssertionError(f"predicate did not become true within {timeout:g}s")
        await asyncio.sleep(interval)


__all__ = ["broker_override", "eventually", "get_broker_override"]
