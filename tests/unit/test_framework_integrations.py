from __future__ import annotations

import asyncio

import pytest

from broka.integrations.asgi import broker_lifespan
from broka.integrations.cli import serve_until_stopped


class LifecycleProbe:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def startup(self) -> None:
        self.calls.append("startup")

    async def shutdown(self) -> None:
        self.calls.append("shutdown")


@pytest.mark.asyncio
async def test_generic_lifespan_shuts_down_after_failure() -> None:
    probe = LifecycleProbe()

    with pytest.raises(RuntimeError):
        async with broker_lifespan(probe):
            raise RuntimeError("application failed")

    assert probe.calls == ["startup", "shutdown"]


@pytest.mark.asyncio
async def test_cli_helper_accepts_injected_stop_event() -> None:
    probe = LifecycleProbe()
    stop = asyncio.Event()
    stop.set()

    await serve_until_stopped(probe, stop=stop)

    assert probe.calls == ["startup", "shutdown"]


def test_celery_hook_installation_is_idempotent() -> None:
    pytest.importorskip("celery")
    from broka.integrations.celery import install_worker_hooks, uninstall_worker_hooks

    probe = LifecycleProbe()
    first = install_worker_hooks(probe)
    second = install_worker_hooks(probe)

    assert first is second
    assert uninstall_worker_hooks(probe) is True
    assert uninstall_worker_hooks(probe) is False
