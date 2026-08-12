from __future__ import annotations

import asyncio

import pytest

from pymq.connection import ConnectionManager, ConnectionState
from pymq.exceptions import BrokerConnectionError
from pymq.reliability import FixedBackoff, RetryManager, RetryPolicy
from pymq.testing import DeterministicRetryScheduler, FakeEngine


def connection_manager(engine: FakeEngine) -> ConnectionManager:
    scheduler = DeterministicRetryScheduler()
    policy = RetryPolicy(max_attempts=4, backoff=FixedBackoff(0), name="connection-test")
    return ConnectionManager(
        engine,
        retry_manager=RetryManager(policy, sleep=scheduler.sleep, clock=scheduler.clock),
        retry_policy=policy,
        heartbeat_interval=None,
    )


@pytest.mark.asyncio
async def test_startup_retries_and_restores_topology() -> None:
    engine = FakeEngine()
    engine.failures.fail_next("connect", OSError("offline"), count=2)
    manager = connection_manager(engine)
    restored: list[str] = []

    async def restore() -> None:
        restored.append("subscriptions")

    manager.register_restore_callback(restore)
    await manager.startup()

    assert manager.state is ConnectionState.CONNECTED
    assert engine.connected
    assert restored == ["subscriptions"]
    await manager.shutdown()
    assert manager.state is ConnectionState.CLOSED


@pytest.mark.asyncio
async def test_failed_topology_restore_unwinds_connected_transport() -> None:
    engine = FakeEngine()
    manager = connection_manager(engine)

    async def broken_restore() -> None:
        raise RuntimeError("subscription declaration failed")

    manager.register_restore_callback(broken_restore)
    with pytest.raises(BrokerConnectionError, match="restoration"):
        await manager.startup()

    assert manager.state is ConnectionState.FAILED
    assert not engine.connected


@pytest.mark.asyncio
async def test_shutdown_drains_an_inflight_lease() -> None:
    engine = FakeEngine()
    manager = connection_manager(engine)
    await manager.startup()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_lease() -> None:
        async with manager.lease():
            entered.set()
            await release.wait()

    operation = asyncio.create_task(hold_lease())
    await entered.wait()
    shutdown = asyncio.create_task(manager.shutdown(grace_period=1))
    await asyncio.sleep(0)
    assert manager.state is ConnectionState.DRAINING
    release.set()
    await operation
    await shutdown
    assert manager.active_leases == 0
    assert not engine.connected


@pytest.mark.asyncio
async def test_connection_error_triggers_reconnect_but_not_implicit_replay() -> None:
    engine = FakeEngine()
    manager = connection_manager(engine)
    await manager.startup()
    calls = 0

    async def operation(selected: object) -> None:
        nonlocal calls
        calls += 1
        raise OSError("socket closed")

    with pytest.raises(OSError, match="socket closed"):
        await manager.run(operation)
    assert calls == 1
    assert manager.state is ConnectionState.CONNECTED
    assert manager.snapshot().reconnect_count == 1
    await manager.shutdown()


@pytest.mark.asyncio
async def test_connection_health_merges_engine_state() -> None:
    engine = FakeEngine()
    manager = connection_manager(engine)
    await manager.startup()
    health = await manager.health()
    assert health.status.value == "healthy"
    assert health.details["engine"] == "fake"
    await manager.shutdown()
