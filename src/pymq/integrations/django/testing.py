"""Django test helpers with explicit broker ownership."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager

from pymq.broker import Broker
from pymq.integrations.django import clear_broker, configure


@contextmanager
def override_pyev_settings(config: Mapping[str, object]) -> Iterator[None]:
    """Temporarily replace ``settings.PYEV`` and reset the lazy broker cache.

    The helper intentionally refuses to clear a running broker.  Tests should
    use :func:`django_broker_context` when lifecycle management is required.
    """

    from django.test import override_settings

    clear_broker()
    with override_settings(PYEV=dict(config)):
        try:
            yield
        finally:
            clear_broker()


@asynccontextmanager
async def django_broker_context(
    config: Mapping[str, object] | None = None,
    *,
    broker: Broker | None = None,
) -> AsyncIterator[Broker]:
    """Start an isolated broker and expose it through ``get_broker`` in a test."""

    selected = broker or Broker.from_config(config or {"engine": "memory"})
    clear_broker()
    configure(selected)
    await selected.startup()
    try:
        yield selected
    finally:
        await selected.shutdown()
        clear_broker()


__all__ = ["django_broker_context", "override_pyev_settings"]
