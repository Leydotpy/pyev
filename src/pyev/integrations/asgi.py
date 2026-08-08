"""Framework-neutral ASGI lifecycle integration."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from contextlib import asynccontextmanager
from typing import Any, Protocol


class LifecycleBroker(Protocol):
    """Minimal broker surface required by lifecycle integrations."""

    async def startup(self) -> None: ...

    async def shutdown(self) -> None: ...


ASGIScope = MutableMapping[str, Any]
ASGIMessage = MutableMapping[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]
ASGIApp = Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]


@asynccontextmanager
async def broker_lifespan(broker: LifecycleBroker) -> AsyncIterator[LifecycleBroker]:
    """Start and stop ``broker`` around an ASGI lifespan context.

    This helper is directly compatible with Starlette, FastAPI, Litestar, and
    any framework accepting an async lifespan context manager.
    """

    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()


class ASGIBrokerMiddleware:
    """Own a broker for the duration of an application's ASGI lifespan scope.

    The wrapped application's own lifespan protocol remains intact. The broker
    starts before the application begins processing lifespan messages and is
    always shut down if application startup or shutdown fails.
    """

    def __init__(self, app: ASGIApp, broker: LifecycleBroker) -> None:
        self.app = app
        self.broker = broker

    async def __call__(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        if scope.get("type") != "lifespan":
            await self.app(scope, receive, send)
            return

        await self.broker.startup()
        try:
            await self.app(scope, receive, send)
        finally:
            await self.broker.shutdown()
