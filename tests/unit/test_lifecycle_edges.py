from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest

from pyev.connection import ConnectionManager, ConnectionState
from pyev.engines.base import EngineHealth
from pyev.events.internal import (
    InternalEventEmitter,
    OperationalEvent,
    OperationalEventName,
)
from pyev.exceptions import BrokerConnectionError, LifecycleError
from pyev.lifecycle import (
    LifecycleManager,
    LifecycleState,
    RestartPolicy,
    SupervisedTaskState,
    TaskFailure,
    TaskSupervisor,
)
from pyev.reliability import FixedBackoff, RetryManager, RetryPolicy
from pyev.testing import DeterministicRetryScheduler, FakeEngine


class RecordingEmitter:
    def __init__(self, *, asynchronous: bool = True, fail: bool = False) -> None:
        self.asynchronous = asynchronous
        self.fail = fail
        self.events: list[tuple[str, dict[str, object]]] = []

    def emit(self, name: str, **details: object) -> Awaitable[None] | None:
        self.events.append((name, details))
        if self.fail:
            raise RuntimeError("observer unavailable")
        if not self.asynchronous:
            return None

        async def complete() -> None:
            return None

        return complete()


class ProbeComponent:
    def __init__(
        self,
        name: str,
        log: list[str],
        *,
        fail_start: bool = False,
        fail_stop: bool = False,
        drain_hook: Callable[[], object] | None = None,
    ) -> None:
        self.name = name
        self.log = log
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.drain_hook = drain_hook

    async def startup(self) -> None:
        self.log.append(f"start:{self.name}")
        if self.fail_start:
            raise RuntimeError(f"start failed: {self.name}")

    async def shutdown(self) -> None:
        self.log.append(f"stop:{self.name}")
        if self.fail_stop:
            raise RuntimeError(f"stop failed: {self.name}")

    def drain(self) -> object:
        self.log.append(f"drain:{self.name}")
        return self.drain_hook() if self.drain_hook is not None else None


def make_connection_manager(
    engine: FakeEngine,
    *,
    emitter: object | None = None,
) -> ConnectionManager:
    scheduler = DeterministicRetryScheduler()
    policy = RetryPolicy(max_attempts=2, backoff=FixedBackoff(0), name="edges")
    return ConnectionManager(
        engine,
        retry_manager=RetryManager(policy, sleep=scheduler.sleep, clock=scheduler.clock),
        retry_policy=policy,
        heartbeat_interval=None,
        event_emitter=emitter,
        clock=scheduler.clock,
        sleep=scheduler.sleep,
    )


@pytest.mark.asyncio
async def test_supervisor_validates_registration_and_unknown_tasks() -> None:
    supervisor = TaskSupervisor()

    async def complete() -> None:
        return None

    pending: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    with pytest.raises(ValueError, match="must not be empty"):
        supervisor.start_soon(complete, name="")
    with pytest.raises(ValueError, match="non-negative"):
        supervisor.start_soon(complete, name="negative", max_restarts=-1)
    with pytest.raises(ValueError, match="coroutine factories"):
        supervisor.start_soon(
            pending,
            name="not-a-factory",
            restart_policy=RestartPolicy.ALWAYS,
        )
    with pytest.raises(KeyError, match="unknown supervised task"):
        supervisor.snapshot("missing")
    with pytest.raises(KeyError, match="unknown supervised task"):
        await supervisor.wait("missing")
    with pytest.raises(ValueError, match="non-negative"):
        await supervisor.shutdown(-1)

    supervisor.start_soon(complete, name="unique")
    with pytest.raises(ValueError, match="already running"):
        supervisor.start_soon(complete, name="unique")
    await supervisor.wait("unique")
    assert not await supervisor.cancel("unique")
    assert not await supervisor.cancel("missing")

    await supervisor.shutdown()
    with pytest.raises(LifecycleError, match="shutting down"):
        supervisor.start_soon(complete, name="too-late")


@pytest.mark.asyncio
async def test_supervisor_exposes_restart_snapshot_and_failure_details() -> None:
    restart_waiting = asyncio.Event()
    permit_restart = asyncio.Event()
    delays: list[float] = []
    handled: list[TaskFailure] = []
    emitter = RecordingEmitter()
    calls = 0

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        restart_waiting.set()
        await permit_restart.wait()

    def handle_failure(failure: TaskFailure) -> None:
        handled.append(failure)

    async def worker() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("password=super-secret")

    supervisor = TaskSupervisor(
        sleep=fake_sleep,
        clock=lambda: 12.5,
        failure_handler=handle_failure,
        event_emitter=emitter,
    )
    supervisor.start_soon(
        worker,
        name="restartable",
        restart_policy=RestartPolicy.ON_FAILURE,
        max_restarts=1,
        backoff=FixedBackoff(3),
    )

    await restart_waiting.wait()
    restarting = supervisor.snapshot("restartable")
    assert restarting.state is SupervisedTaskState.RESTARTING
    assert restarting.restart_count == 1
    assert restarting.last_error_type == "RuntimeError"
    assert restarting.last_error_message == "password=[REDACTED]"
    assert not restarting.done
    assert delays == [3]
    assert handled == list(supervisor.failures)
    assert handled[0].timestamp == 12.5

    permit_restart.set()
    completed = await supervisor.wait("restartable")
    assert completed.state is SupervisedTaskState.COMPLETED
    assert completed.done
    assert calls == 2
    assert [name for name, _ in emitter.events] == ["task_failed", "task_restarted"]


@pytest.mark.asyncio
async def test_supervisor_terminal_failure_async_handler_and_broken_emitter() -> None:
    handled: list[str] = []

    async def handle_failure(failure: TaskFailure) -> None:
        handled.append(type(failure.error).__name__)

    async def fail() -> None:
        raise LookupError("gone")

    supervisor = TaskSupervisor(
        failure_handler=handle_failure,
        event_emitter=RecordingEmitter(fail=True),
    )
    supervisor.start_soon(fail, name="failing")
    snapshot = await supervisor.wait("failing")

    assert snapshot.state is SupervisedTaskState.FAILED
    assert snapshot.restart_count == 0
    assert handled == ["LookupError"]
    assert supervisor.active_count == 0


@pytest.mark.asyncio
async def test_supervisor_always_restarts_successful_factory_to_bound() -> None:
    calls = 0
    delays: list[float] = []

    async def immediate_sleep(delay: float) -> None:
        delays.append(delay)

    async def worker() -> None:
        nonlocal calls
        calls += 1

    supervisor = TaskSupervisor(sleep=immediate_sleep)
    supervisor.start_soon(
        worker,
        name="periodic",
        restart_policy=RestartPolicy.ALWAYS,
        max_restarts=2,
        backoff=FixedBackoff(0.25),
    )
    snapshot = await supervisor.wait("periodic")

    assert calls == 3
    assert delays == [0.25, 0.25]
    assert snapshot.state is SupervisedTaskState.COMPLETED
    assert snapshot.restart_count == 2
    assert not supervisor.failures


@pytest.mark.asyncio
async def test_supervisor_accepts_single_awaitable_and_sorts_snapshots() -> None:
    async def result() -> str:
        return "done"

    supervisor = TaskSupervisor()
    supervisor.start_soon(result(), name="z-last")
    supervisor.start_soon(result, name="a-first")
    await supervisor.wait("z-last")
    await supervisor.wait("a-first")

    assert [snapshot.name for snapshot in supervisor.snapshots()] == ["a-first", "z-last"]
    assert all(
        snapshot.state is SupervisedTaskState.COMPLETED for snapshot in supervisor.snapshots()
    )


@pytest.mark.asyncio
async def test_supervisor_cancel_and_context_manager_cancel_owned_tasks() -> None:
    entered = asyncio.Event()

    async def blocked() -> None:
        entered.set()
        await asyncio.Event().wait()

    supervisor = TaskSupervisor()
    supervisor.start_soon(blocked, name="cancel-me")
    await entered.wait()
    assert await supervisor.cancel("cancel-me")
    assert supervisor.snapshot("cancel-me").state is SupervisedTaskState.CANCELLED

    context_entered = asyncio.Event()

    async def context_worker() -> None:
        context_entered.set()
        await asyncio.Event().wait()

    managed = TaskSupervisor()
    async with managed as returned:
        assert returned is managed
        managed.start_soon(context_worker, name="context-worker")
        await context_entered.wait()
    assert managed.snapshot("context-worker").state is SupervisedTaskState.CANCELLED


@pytest.mark.asyncio
async def test_supervisor_graceful_and_repeated_shutdown_paths() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def worker() -> None:
        entered.set()
        await release.wait()

    supervisor = TaskSupervisor()
    supervisor.start_soon(worker, name="graceful")
    await entered.wait()
    release.set()
    await supervisor.shutdown(grace_period=1)
    assert supervisor.snapshot("graceful").state is SupervisedTaskState.COMPLETED

    # A repeated or concurrent shutdown waits for known records and stays safe.
    await supervisor.shutdown()


def test_lifecycle_registration_validation() -> None:
    log: list[str] = []
    component = ProbeComponent("one", log)
    manager = LifecycleManager()

    with pytest.raises(ValueError, match="must not be empty"):
        manager.register("", component)
    manager.register("one", component)
    with pytest.raises(ValueError, match="already registered"):
        manager.register("one", component)


@pytest.mark.asyncio
async def test_lifecycle_success_idempotence_drain_and_reverse_shutdown() -> None:
    log: list[str] = []
    emitter = RecordingEmitter()
    manager = LifecycleManager(event_emitter=emitter)
    manager.register("first", ProbeComponent("first", log))
    manager.register("second", ProbeComponent("second", log))

    await manager.startup()
    await manager.startup()
    assert manager.state is LifecycleState.RUNNING
    with pytest.raises(LifecycleError, match="only be registered while stopped"):
        manager.register("late", ProbeComponent("late", log))

    await manager.drain()
    assert manager.state is LifecycleState.DRAINING
    with pytest.raises(LifecycleError, match="cannot drain"):
        await manager.drain()
    with pytest.raises(LifecycleError, match="cannot start"):
        await manager.startup()

    await manager.shutdown()
    await manager.shutdown()
    assert manager.state is LifecycleState.STOPPED
    assert log == [
        "start:first",
        "start:second",
        "drain:first",
        "drain:second",
        "stop:second",
        "stop:first",
    ]
    assert [name for name, _ in emitter.events] == [
        "startup_started",
        "startup_completed",
        "drain_started",
        "drain_completed",
        "shutdown_started",
        "shutdown_completed",
    ]


@pytest.mark.asyncio
async def test_lifecycle_startup_failure_context_and_best_effort_unwind() -> None:
    log: list[str] = []
    manager = LifecycleManager()
    manager.register("first", ProbeComponent("first", log, fail_stop=True))
    manager.register("second", ProbeComponent("second", log))
    manager.register("broken", ProbeComponent("broken", log, fail_start=True))

    with pytest.raises(LifecycleError, match="startup failed") as raised:
        await manager.startup()

    assert manager.state is LifecycleState.FAILED
    assert raised.value.context == {
        "component": "broken",
        "error_type": "RuntimeError",
    }
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert log == [
        "start:first",
        "start:second",
        "start:broken",
        "stop:second",
        "stop:first",
    ]


@pytest.mark.asyncio
async def test_lifecycle_drain_awaits_async_hooks_and_emits_for_sync_emitter() -> None:
    log: list[str] = []
    drained = asyncio.Event()

    async def async_drain() -> None:
        drained.set()

    emitter = RecordingEmitter(asynchronous=False)
    manager = LifecycleManager(event_emitter=emitter)
    manager.register(
        "component",
        ProbeComponent("component", log, drain_hook=async_drain),
    )
    await manager.startup()
    await manager.drain(timeout=1)
    assert drained.is_set()
    await manager.shutdown()


@pytest.mark.asyncio
async def test_lifecycle_drain_validation_noop_timeout_and_hook_error() -> None:
    manager = LifecycleManager()
    with pytest.raises(ValueError, match="non-negative"):
        await manager.drain(-1)
    await manager.drain()

    waiting = asyncio.Event()

    async def never_finishes() -> None:
        await waiting.wait()

    manager.register(
        "blocked",
        ProbeComponent("blocked", [], drain_hook=never_finishes),
    )
    await manager.startup()
    with pytest.raises(LifecycleError, match="timed out") as raised:
        await manager.drain(timeout=0)
    assert raised.value.context == {"timeout": 0}
    await manager.shutdown(grace_period=0)

    def broken_drain() -> None:
        raise LookupError("drain hook failed")

    second = LifecycleManager()
    second.register(
        "broken",
        ProbeComponent("broken", [], drain_hook=broken_drain),
    )
    await second.startup()
    with pytest.raises(LookupError, match="drain hook failed"):
        await second.drain()
    await second.shutdown()


class FailingSupervisor(TaskSupervisor):
    async def shutdown(self, grace_period: float = 0.0) -> None:
        del grace_period
        raise RuntimeError("supervisor shutdown failed")


@pytest.mark.asyncio
async def test_lifecycle_shutdown_aggregates_supervisor_and_component_errors() -> None:
    log: list[str] = []
    emitter = RecordingEmitter()
    manager = LifecycleManager(supervisor=FailingSupervisor(), event_emitter=emitter)
    manager.register("good", ProbeComponent("good", log))
    manager.register("bad", ProbeComponent("bad", log, fail_stop=True))
    await manager.startup()

    with pytest.raises(LifecycleError, match="failed during shutdown") as raised:
        await manager.shutdown(grace_period=0)

    assert manager.state is LifecycleState.STOPPED
    assert raised.value.context == {"errors": ("RuntimeError", "RuntimeError")}
    assert isinstance(raised.value.__cause__, ExceptionGroup)
    assert log == ["start:good", "start:bad", "stop:bad", "stop:good"]
    assert emitter.events[-1] == ("shutdown_completed", {"forced": True})


@pytest.mark.asyncio
async def test_lifecycle_shutdown_validation_new_and_stopped_registration() -> None:
    manager = LifecycleManager()
    with pytest.raises(ValueError, match="non-negative"):
        await manager.shutdown(-1)
    await manager.shutdown()
    assert manager.state is LifecycleState.STOPPED

    log: list[str] = []
    manager.register("after-stop", ProbeComponent("after-stop", log))
    await manager.startup()
    await manager.shutdown()
    assert log == ["start:after-stop", "stop:after-stop"]


@pytest.mark.asyncio
async def test_lifecycle_context_manager_starts_and_stops_on_body_error() -> None:
    log: list[str] = []
    manager = LifecycleManager()
    manager.register("context", ProbeComponent("context", log))

    with pytest.raises(RuntimeError, match="body failed"):
        async with manager as entered:
            assert entered is manager
            assert manager.state is LifecycleState.RUNNING
            raise RuntimeError("body failed")

    assert manager.state is LifecycleState.STOPPED
    assert log == ["start:context", "stop:context"]


def test_operational_event_validation_namespace_and_immutable_details() -> None:
    event = OperationalEvent(
        "published",
        {"route": "orders"},
        timestamp=datetime(2026, 8, 6, tzinfo=UTC),
        id="fixed",
    )
    assert event.qualified_name == "pyev.internal.published"
    assert event.details == {"route": "orders"}
    with pytest.raises(TypeError):
        event.details["route"] = "changed"  # type: ignore[index]
    with pytest.raises(ValueError, match="must not be empty"):
        OperationalEvent("")
    with pytest.raises(ValueError, match="timezone-aware"):
        OperationalEvent("published", timestamp=datetime(2026, 8, 6))


def test_internal_event_emitter_validates_configuration() -> None:
    with pytest.raises(ValueError, match="positive"):
        InternalEventEmitter(listener_timeout=0)
    with pytest.raises(ValueError, match="non-negative"):
        InternalEventEmitter(failure_history_limit=-1)


@pytest.mark.asyncio
async def test_internal_event_exact_wildcard_unsubscribe_and_clear() -> None:
    emitter = InternalEventEmitter()
    received: list[tuple[str, str]] = []

    async def exact(event: OperationalEvent) -> None:
        received.append(("exact", event.name))

    async def wildcard(event: OperationalEvent) -> None:
        received.append(("wildcard", event.name))

    exact_registration = emitter.on(OperationalEventName.PUBLISHED, exact)
    wildcard_registration = emitter.subscribe("*", wildcard, name="all-events")
    assert emitter.listeners("published") == (exact_registration,)
    assert len(emitter.listeners()) == 2

    result = await emitter.emit("published", route="orders")
    assert result.listeners_called == 2
    assert result.successful
    assert received == [("exact", "published"), ("wildcard", "published")]

    assert emitter.unsubscribe(exact_registration)
    assert emitter.unsubscribe(wildcard_registration.id)
    assert not emitter.unsubscribe("missing")
    empty = await emitter.emit("published")
    assert empty.listeners_called == 0
    emitter.clear()
    assert not emitter.listeners()
    assert not emitter.failures()


@pytest.mark.asyncio
async def test_internal_event_instance_details_contract_and_failure_history_bound() -> None:
    emitter = InternalEventEmitter(failure_history_limit=1)

    async def broken(event: OperationalEvent) -> None:
        raise RuntimeError(event.name)

    emitter.subscribe("*", broken, name="broken")
    event = OperationalEvent("first")
    with pytest.raises(TypeError, match="details cannot be supplied"):
        await emitter.emit(event, extra=True)

    first = await emitter.emit(event)
    second = await emitter.emit("second")
    assert not first.successful
    assert not second.successful
    assert first.failures[0].event_name == "first"
    assert [failure.event_name for failure in emitter.failures()] == ["second"]


@pytest.mark.asyncio
async def test_connection_validation_restore_registry_and_context() -> None:
    engine = FakeEngine()
    with pytest.raises(ValueError, match="heartbeat_interval"):
        ConnectionManager(engine, heartbeat_interval=0)
    with pytest.raises(ValueError, match="heartbeat_timeout"):
        ConnectionManager(engine, heartbeat_timeout=0)

    manager = make_connection_manager(engine)

    async def restore() -> None:
        return None

    manager.register_restore_callback(restore, name="topology")
    with pytest.raises(ValueError, match="already registered"):
        manager.register_restore_callback(restore, name="topology")
    assert manager.unregister_restore_callback("topology")
    assert not manager.unregister_restore_callback("topology")

    async with manager as entered:
        assert entered is manager
        assert manager.connected
        await manager.ensure_connected()
        async with manager.lease() as selected:
            assert selected is engine
            assert manager.active_leases == 1
    assert manager.state is ConnectionState.CLOSED
    assert manager.active_leases == 0


@pytest.mark.asyncio
async def test_connection_drain_timeout_health_failure_and_disconnect_error() -> None:
    emitter = RecordingEmitter(fail=True)
    engine = FakeEngine()
    manager = make_connection_manager(engine, emitter=emitter)
    assert await manager.drain()
    with pytest.raises(ValueError, match="non-negative"):
        await manager.drain(-1)
    with pytest.raises(ValueError, match="non-negative"):
        await manager.shutdown(-1)

    await manager.startup()
    lease_entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_lease() -> None:
        async with manager.lease():
            lease_entered.set()
            await release.wait()

    holder = asyncio.create_task(hold_lease())
    await lease_entered.wait()
    assert not await manager.drain(timeout=0)
    release.set()
    await holder

    engine.failures.fail_next("healthcheck", OSError("password=health-secret"))
    health = await manager.health()
    assert health.status.value == "degraded"
    assert "OSError" in (health.message or "")

    # A disconnect failure is translated and leaves an inspectable failed state.
    engine.failures.fail_next("disconnect", OSError("password=disconnect-secret"))
    with pytest.raises(BrokerConnectionError, match="disconnect failed"):
        await manager.shutdown()
    snapshot = manager.snapshot()
    assert snapshot.state is ConnectionState.FAILED
    assert snapshot.last_error_type == "OSError"
    assert snapshot.last_error_message == "password=[REDACTED]"


@pytest.mark.asyncio
async def test_connection_run_non_connection_failure_and_reconnect_guards() -> None:
    engine = FakeEngine()
    manager = make_connection_manager(engine)
    await manager.startup()

    async def bad_operation(selected: object) -> None:
        del selected
        raise LookupError("application failure")

    with pytest.raises(LookupError, match="application failure"):
        await manager.run(bad_operation)
    assert manager.state is ConnectionState.CONNECTED
    assert manager.snapshot().reconnect_count == 0

    await manager.drain()
    with pytest.raises(BrokerConnectionError, match="cannot connect"):
        await manager.startup()
    with pytest.raises(BrokerConnectionError, match="cannot reconnect"):
        await manager.reconnect()
    await manager.shutdown()


@pytest.mark.asyncio
async def test_connection_health_degraded_and_snapshot_without_heartbeat() -> None:
    engine = FakeEngine()
    manager = make_connection_manager(engine)
    disconnected = await manager.health()
    assert disconnected.status.value == "unhealthy"

    await manager.startup()
    engine.health_override = EngineHealth(
        engine="fake",
        connected=False,
        healthy=False,
        latency_ms=4.5,
        details={"reason": "maintenance"},
    )
    degraded = await manager.health()
    assert degraded.status.value == "degraded"
    assert degraded.details["reason"] == "maintenance"
    assert degraded.details["engine_latency_ms"] == 4.5
    assert not manager.snapshot().heartbeat_running
    await manager.shutdown()


@pytest.mark.asyncio
async def test_connection_failed_restore_records_snapshot_and_event() -> None:
    engine = FakeEngine()
    emitter = RecordingEmitter()
    manager = make_connection_manager(engine, emitter=emitter)

    async def broken_restore() -> None:
        raise RuntimeError("token=restore-secret")

    manager.register_restore_callback(broken_restore, name="subscriptions")
    with pytest.raises(BrokerConnectionError, match="restoration"):
        await manager.startup()

    snapshot = manager.snapshot()
    assert snapshot.state is ConnectionState.FAILED
    assert snapshot.last_error_type == "ConnectionError"
    assert not engine.connected
    assert [name for name, _ in emitter.events] == ["connection_failed"]


@pytest.mark.asyncio
async def test_connection_reconnect_tolerates_stale_disconnect_failure() -> None:
    engine = FakeEngine()
    manager = make_connection_manager(engine)
    await manager.startup()
    engine.failures.fail_next("disconnect", OSError("already closed"))

    await manager.reconnect()

    snapshot = manager.snapshot()
    assert snapshot.state is ConnectionState.CONNECTED
    assert snapshot.reconnect_count == 1
    await manager.shutdown()


@pytest.mark.asyncio
async def test_connection_cancellation_is_not_translated() -> None:
    engine = FakeEngine()
    manager = make_connection_manager(engine)
    await manager.startup()

    async def cancelled(selected: object) -> None:
        del selected
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await manager.run(cancelled)
    assert manager.state is ConnectionState.CONNECTED
    await manager.shutdown()
