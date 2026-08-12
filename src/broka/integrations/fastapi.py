"""FastAPI helpers that avoid importing FastAPI in the core distribution."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from broka.integrations.asgi import LifecycleBroker


def lifespan(broker: LifecycleBroker) -> Callable[[Any], AbstractAsyncContextManager[None]]:
    """Build a FastAPI-compatible lifespan callable for ``broker``."""

    @asynccontextmanager
    async def lifespan_context(_app: Any) -> AsyncIterator[None]:
        await broker.startup()
        try:
            yield
        finally:
            await broker.shutdown()

    return lifespan_context


def dependency(broker: LifecycleBroker) -> Callable[[], LifecycleBroker]:
    """Build a dependency provider returning the process-local broker."""

    def provide_broker() -> LifecycleBroker:
        return broker

    return provide_broker
