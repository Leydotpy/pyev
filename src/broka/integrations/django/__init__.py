"""Optional Django integration.

The Django dependency is loaded only when one of these helpers is called.
Each OS process owns its own configured broker instance.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from broka.integrations.django.config import DjangoSettingsProvider, load_django_config

if TYPE_CHECKING:
    from broka.broker import Broker
    from broka.options import PublishOptions
    from broka.results import PublishResult
    from broka.routing import Route

_broker: Broker | None = None
_background_tasks: set[asyncio.Task[Any]] = set()
_logger = logging.getLogger(__name__)


def configure(broker: Broker) -> Broker:
    """Set the broker owned by the current Django process."""

    global _broker
    _broker = broker
    return broker


def get_broker() -> Broker:
    """Return or lazily construct the current process's configured broker."""

    global _broker
    if _broker is None:
        _broker = DjangoSettingsProvider().create_broker()
    return _broker


def clear_broker() -> None:
    """Forget a stopped process-local broker, primarily for Django tests."""

    global _broker
    if _broker is not None and _broker.ready:
        raise RuntimeError("shut down the configured pyev broker before clearing it")
    _broker = None


async def shutdown_broker(*, clear: bool = False) -> None:
    """Drain deferred publishes and stop this process's configured broker."""

    global _broker
    await drain_publish_tasks()
    if _broker is not None:
        await _broker.shutdown()
    if clear:
        _broker = None


def _task_done(task: asyncio.Task[Any]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        _logger.error(
            "pyev publish scheduled by transaction.on_commit failed",
            exc_info=(type(error), error, error.__traceback__),
        )


async def drain_publish_tasks() -> None:
    """Wait for publishes scheduled by completed async Django transactions."""

    if _background_tasks:
        await asyncio.gather(*tuple(_background_tasks))


def publish_on_commit(
    message: object,
    *,
    broker: Broker | None = None,
    route: str | Route | None = None,
    headers: Mapping[str, str] | None = None,
    options: PublishOptions | None = None,
    using: str | None = None,
    robust: bool = False,
) -> None:
    """Publish only after the selected Django database transaction commits.

    In a synchronous Django request the callback blocks until publication has
    finished.  In an async request it creates a tracked task; ASGI shutdown
    should call :func:`shutdown_broker` so those tasks are drained.
    """

    try:
        from django.db import transaction
    except ImportError as error:  # pragma: no cover - optional dependency
        raise RuntimeError("Django integration requires the 'django' extra") from error

    selected = broker or get_broker()

    def publish() -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                from asgiref.sync import async_to_sync
            except ImportError as error:  # pragma: no cover
                raise RuntimeError("Django's asgiref dependency is required") from error
            async_to_sync(selected.publish)(
                message,
                route=route,
                headers=headers,
                options=options,
            )
        else:
            task = loop.create_task(
                selected.publish(message, route=route, headers=headers, options=options),
                name="pyev-django-on-commit",
            )
            _background_tasks.add(task)
            task.add_done_callback(_task_done)

    transaction.on_commit(publish, using=using, robust=robust)


async def publish_immediately(
    message: object,
    *,
    broker: Broker | None = None,
    route: str | Route | None = None,
    headers: Mapping[str, str] | None = None,
    options: PublishOptions | None = None,
) -> PublishResult:
    """Publish immediately, bypassing transaction-on-commit semantics."""

    return await (broker or get_broker()).publish(
        message,
        route=route,
        headers=headers,
        options=options,
    )


__all__ = [
    "DjangoSettingsProvider",
    "clear_broker",
    "configure",
    "drain_publish_tasks",
    "get_broker",
    "load_django_config",
    "publish_immediately",
    "publish_on_commit",
    "shutdown_broker",
]
