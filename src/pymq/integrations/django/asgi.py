"""Explicit, process-local Django ASGI lifecycle integration."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pymq.integrations.asgi import (
    ASGIApp,
    ASGIReceive,
    ASGIScope,
    ASGISend,
    LifecycleBroker,
)
from pymq.integrations.django import drain_publish_tasks, get_broker


@asynccontextmanager
async def django_lifespan(
    broker: LifecycleBroker | None = None,
) -> AsyncIterator[LifecycleBroker]:
    """Own one broker around an explicitly managed Django ASGI lifespan."""

    selected = broker or get_broker()
    await selected.startup()
    try:
        yield selected
    finally:
        await drain_publish_tasks()
        await selected.shutdown()


class DjangoBrokerASGI:
    """Add ASGI lifespan support without starting work from ``AppConfig.ready``.

    Django's HTTP application remains untouched.  Lifespan messages are handled
    by this wrapper because Django itself does not promise to implement the ASGI
    lifespan protocol.  Each ASGI worker therefore owns exactly one broker.
    """

    def __init__(self, app: ASGIApp, broker: LifecycleBroker | None = None) -> None:
        self.app = app
        self._broker = broker

    @property
    def broker(self) -> LifecycleBroker:
        """Return the explicitly supplied or lazily configured broker."""

        if self._broker is None:
            self._broker = get_broker()
        return self._broker

    async def __call__(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        if scope.get("type") != "lifespan":
            await self.app(scope, receive, send)
            return

        started = False
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "lifespan.startup":
                try:
                    await self.broker.startup()
                except Exception as error:
                    await send(
                        {
                            "type": "lifespan.startup.failed",
                            "message": f"pyev startup failed: {type(error).__name__}",
                        }
                    )
                    return
                started = True
                await send({"type": "lifespan.startup.complete"})
                continue
            if message_type == "lifespan.shutdown":
                try:
                    await drain_publish_tasks()
                    if started:
                        await self.broker.shutdown()
                except Exception as error:
                    await send(
                        {
                            "type": "lifespan.shutdown.failed",
                            "message": f"pyev shutdown failed: {type(error).__name__}",
                        }
                    )
                    return
                await send({"type": "lifespan.shutdown.complete"})
                return
            raise RuntimeError(f"unsupported ASGI lifespan message {message_type!r}")


def with_broker_lifespan(
    application: ASGIApp,
    broker: LifecycleBroker | None = None,
) -> DjangoBrokerASGI:
    """Wrap Django's ``get_asgi_application()`` result with broker lifecycle."""

    return DjangoBrokerASGI(application, broker)


__all__ = ["DjangoBrokerASGI", "django_lifespan", "with_broker_lifespan"]
