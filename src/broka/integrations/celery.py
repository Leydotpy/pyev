"""Optional Celery worker lifecycle hooks.

Celery and pyev remain separate messaging runtimes: installing these hooks does
not make them share a transport, connection, retry policy, or worker pool.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

from broka.integrations.asgi import LifecycleBroker

_background_tasks: set[asyncio.Task[Any]] = set()
_installed: dict[int, CeleryHookHandle] = {}
_logger = logging.getLogger(__name__)


def _completed(task: asyncio.Task[Any]) -> None:
    _background_tasks.discard(task)
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        _logger.error(
            "pyev Celery lifecycle hook failed",
            exc_info=(type(error), error, error.__traceback__),
        )


def _run(coroutine: Coroutine[Any, Any, object]) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coroutine)
    else:
        task = loop.create_task(coroutine, name="pyev-celery-lifecycle")
        _background_tasks.add(task)
        task.add_done_callback(_completed)


@dataclass(frozen=True, slots=True)
class CeleryHookHandle:
    """Signal receiver handle returned by :func:`install_worker_hooks`."""

    broker: LifecycleBroker
    ready_signal: Any
    shutdown_signal: Any
    startup_receiver: Any
    shutdown_receiver: Any

    def disconnect(self) -> None:
        """Remove both receivers idempotently."""

        self.ready_signal.disconnect(self.startup_receiver)
        self.shutdown_signal.disconnect(self.shutdown_receiver)
        _installed.pop(id(self.broker), None)


def install_worker_hooks(broker: LifecycleBroker) -> CeleryHookHandle:
    """Connect a process-local broker to Celery worker lifecycle signals.

    Celery is imported lazily, so importing :mod:`pyev` never installs Celery or
    mutates signal state. Repeated installation for one broker is idempotent.
    """

    existing = _installed.get(id(broker))
    if existing is not None:
        return existing
    try:
        from celery import signals
    except ImportError as error:  # pragma: no cover - depends on optional extra
        raise RuntimeError("Celery integration requires the 'celery' package") from error

    def startup_receiver(**_kwargs: object) -> None:
        _run(broker.startup())

    def shutdown_receiver(**_kwargs: object) -> None:
        _run(broker.shutdown())

    signals.worker_ready.connect(startup_receiver, weak=False)
    signals.worker_shutdown.connect(shutdown_receiver, weak=False)
    handle = CeleryHookHandle(
        broker,
        signals.worker_ready,
        signals.worker_shutdown,
        startup_receiver,
        shutdown_receiver,
    )
    _installed[id(broker)] = handle
    return handle


def uninstall_worker_hooks(broker: LifecycleBroker) -> bool:
    """Disconnect previously installed receivers for ``broker``."""

    handle = _installed.get(id(broker))
    if handle is None:
        return False
    handle.disconnect()
    return True


__all__ = ["CeleryHookHandle", "install_worker_hooks", "uninstall_worker_hooks"]
