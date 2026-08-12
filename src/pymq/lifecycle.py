"""Deterministic component lifecycle and background task supervision."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from pymq.exceptions import LifecycleError
from pymq.observability.redaction import redact_text
from pymq.reliability.backoff import BackoffStrategy, FixedBackoff, calculate_backoff


class LifecycleState(StrEnum):
    """State of an explicitly managed runtime service."""

    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class RestartPolicy(StrEnum):
    """Bounded restart behaviour for a supervised task."""

    NEVER = "never"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"


class SupervisedTaskState(StrEnum):
    """Observable state of one supervised task."""

    RUNNING = "running"
    RESTARTING = "restarting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class TaskFailure:
    """Captured background task failure."""

    name: str
    error: BaseException
    restart_count: int
    timestamp: float


@dataclass(frozen=True, slots=True)
class SupervisedTaskSnapshot:
    """Immutable task state suitable for health reporting."""

    name: str
    state: SupervisedTaskState
    restart_count: int
    last_error_type: str | None
    last_error_message: str | None
    done: bool


type TaskFactory = Callable[[], Awaitable[object]]
type TaskFailureHandler = Callable[[TaskFailure], Awaitable[None] | None]


@dataclass(slots=True)
class _TaskRecord:
    name: str
    factory: TaskFactory
    restart_policy: RestartPolicy
    max_restarts: int
    backoff: BackoffStrategy
    task: asyncio.Task[None]
    state: SupervisedTaskState = SupervisedTaskState.RUNNING
    restart_count: int = 0
    last_error: BaseException | None = None


class TaskSupervisor:
    """Own background tasks and guarantee bounded, deterministic shutdown."""

    def __init__(
        self,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        failure_handler: TaskFailureHandler | None = None,
        event_emitter: object | None = None,
    ) -> None:
        self._sleep = sleep
        self._clock = clock
        self._failure_handler = failure_handler
        self._events = event_emitter
        self._records: dict[str, _TaskRecord] = {}
        self._failures: list[TaskFailure] = []
        self._stopping = False

    def start_soon(
        self,
        task: TaskFactory | Awaitable[object],
        *,
        name: str,
        restart_policy: RestartPolicy = RestartPolicy.NEVER,
        max_restarts: int = 0,
        backoff: BackoffStrategy | None = None,
    ) -> asyncio.Task[None]:
        """Start and own a task.

        A restartable task must be supplied as a factory because a coroutine
        object can only be awaited once.
        """

        if self._stopping:
            raise LifecycleError("task supervisor is shutting down")
        if not name:
            raise ValueError("task name must not be empty")
        if name in self._records and not self._records[name].task.done():
            raise ValueError(f"task {name!r} is already running")
        if max_restarts < 0:
            raise ValueError("max_restarts must be non-negative")
        if not callable(task) and restart_policy is not RestartPolicy.NEVER:
            raise ValueError("restartable tasks must be supplied as coroutine factories")

        if callable(task):
            factory = task
        else:
            awaitable = task
            used = False

            def factory() -> Awaitable[object]:
                nonlocal used
                if used:
                    raise RuntimeError("coroutine object cannot be restarted")
                used = True
                return awaitable

        # Install the record before the task gets a chance to run by creating a
        # gate. This keeps snapshots deterministic even with eager task factories.
        gate = asyncio.Event()

        async def gated_runner() -> None:
            await gate.wait()
            await self._run(name)

        asyncio_task = asyncio.create_task(gated_runner(), name=f"pyev:{name}")
        record = _TaskRecord(
            name=name,
            factory=factory,
            restart_policy=restart_policy,
            max_restarts=max_restarts,
            backoff=backoff or FixedBackoff(1.0),
            task=asyncio_task,
        )
        self._records[name] = record
        gate.set()
        return asyncio_task

    async def _run(self, name: str) -> None:
        record = self._records[name]
        while not self._stopping:
            failure: BaseException | None = None
            try:
                await record.factory()
            except asyncio.CancelledError:
                record.state = SupervisedTaskState.CANCELLED
                raise
            except Exception as error:
                failure = error
                record.last_error = error
                captured = TaskFailure(name, error, record.restart_count, self._clock())
                self._failures.append(captured)
                await self._report_failure(captured)

            should_restart = record.restart_policy is RestartPolicy.ALWAYS or (
                record.restart_policy is RestartPolicy.ON_FAILURE and failure is not None
            )
            if not should_restart or record.restart_count >= record.max_restarts:
                record.state = (
                    SupervisedTaskState.FAILED
                    if failure is not None
                    else SupervisedTaskState.COMPLETED
                )
                return
            record.restart_count += 1
            record.state = SupervisedTaskState.RESTARTING
            await self._emit("task_restarted", task=name, restart_count=record.restart_count)
            delay = calculate_backoff(record.backoff, record.restart_count)
            await self._sleep(delay)
            record.state = SupervisedTaskState.RUNNING

    async def _report_failure(self, failure: TaskFailure) -> None:
        if self._failure_handler is not None:
            result = self._failure_handler(failure)
            if inspect.isawaitable(result):
                await result
        await self._emit(
            "task_failed",
            task=failure.name,
            error_type=type(failure.error).__name__,
            restart_count=failure.restart_count,
        )

    async def cancel(self, name: str) -> bool:
        """Cancel one task and wait for its cancellation to settle."""

        record = self._records.get(name)
        if record is None or record.task.done():
            return False
        record.task.cancel()
        await asyncio.gather(record.task, return_exceptions=True)
        return True

    async def shutdown(self, grace_period: float = 0.0) -> None:
        """Wait briefly for natural completion, then cancel every owned task."""

        if grace_period < 0:
            raise ValueError("grace_period must be non-negative")
        if self._stopping:
            # A concurrent caller can still deterministically await all records.
            await asyncio.gather(
                *(item.task for item in self._records.values()), return_exceptions=True
            )
            return
        self._stopping = True
        pending = [item.task for item in self._records.values() if not item.task.done()]
        if grace_period and pending:
            _, still_pending = await asyncio.wait(pending, timeout=grace_period)
        else:
            still_pending = set(pending)
        for task in still_pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def wait(self, name: str) -> SupervisedTaskSnapshot:
        """Wait for one task to finish and return its final snapshot."""

        try:
            record = self._records[name]
        except KeyError as error:
            raise KeyError(f"unknown supervised task {name!r}") from error
        await asyncio.gather(record.task, return_exceptions=True)
        return self.snapshot(name)

    def snapshot(self, name: str) -> SupervisedTaskSnapshot:
        """Return a current snapshot of one task."""

        try:
            record = self._records[name]
        except KeyError as error:
            raise KeyError(f"unknown supervised task {name!r}") from error
        return SupervisedTaskSnapshot(
            name=record.name,
            state=record.state,
            restart_count=record.restart_count,
            last_error_type=(type(record.last_error).__name__ if record.last_error else None),
            last_error_message=(redact_text(str(record.last_error)) if record.last_error else None),
            done=record.task.done(),
        )

    def snapshots(self) -> tuple[SupervisedTaskSnapshot, ...]:
        """Return every task snapshot in deterministic name order."""

        return tuple(self.snapshot(name) for name in sorted(self._records))

    @property
    def failures(self) -> tuple[TaskFailure, ...]:
        """Return captured failures in occurrence order."""

        return tuple(self._failures)

    @property
    def active_count(self) -> int:
        """Return the number of unfinished owned tasks."""

        return sum(not record.task.done() for record in self._records.values())

    async def _emit(self, event_name: str, **details: object) -> None:
        emitter = self._events
        emit = getattr(emitter, "emit", None) if emitter is not None else None
        if emit is None:
            return
        try:
            result = emit(event_name, **details)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # Supervision must not recurse because an observability listener
            # itself failed. InternalEventEmitter retains the listener failure.
            return

    async def __aenter__(self) -> TaskSupervisor:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.shutdown()


@runtime_checkable
class LifecycleComponent(Protocol):
    """Explicitly started and stopped service."""

    async def startup(self) -> None:
        """Acquire runtime resources."""

    async def shutdown(self) -> None:
        """Release runtime resources."""


@dataclass(frozen=True, slots=True)
class RegisteredComponent:
    """Named component in startup order."""

    name: str
    component: LifecycleComponent


class LifecycleManager:
    """Start components transactionally and stop them in reverse order."""

    def __init__(
        self,
        components: Sequence[RegisteredComponent] = (),
        *,
        supervisor: TaskSupervisor | None = None,
        event_emitter: object | None = None,
    ) -> None:
        self._components: list[RegisteredComponent] = []
        self._started: list[RegisteredComponent] = []
        self.supervisor = supervisor or TaskSupervisor(event_emitter=event_emitter)
        self._events = event_emitter
        self._state = LifecycleState.NEW
        self._lock = asyncio.Lock()
        for item in components:
            self.register(item.name, item.component)

    @property
    def state(self) -> LifecycleState:
        """Return lifecycle state."""

        return self._state

    def register(self, name: str, component: LifecycleComponent) -> None:
        """Append a component before startup."""

        if self._state not in (LifecycleState.NEW, LifecycleState.STOPPED):
            raise LifecycleError("components can only be registered while stopped")
        if not name:
            raise ValueError("component name must not be empty")
        if any(item.name == name for item in self._components):
            raise ValueError(f"lifecycle component {name!r} is already registered")
        self._components.append(RegisteredComponent(name, component))

    async def startup(self) -> None:
        """Start all components, unwinding earlier ones after any failure."""

        async with self._lock:
            if self._state is LifecycleState.RUNNING:
                return
            if self._state in (
                LifecycleState.STARTING,
                LifecycleState.DRAINING,
                LifecycleState.STOPPING,
            ):
                raise LifecycleError(f"cannot start while lifecycle is {self._state.value}")
            self._state = LifecycleState.STARTING
            await self._emit("startup_started")
            self._started.clear()
            try:
                for registration in self._components:
                    await registration.component.startup()
                    self._started.append(registration)
            except Exception as error:
                await self._unwind()
                self._state = LifecycleState.FAILED
                raise LifecycleError(
                    "lifecycle startup failed",
                    context={
                        "component": registration.name,
                        "error_type": type(error).__name__,
                    },
                ) from error
            self._state = LifecycleState.RUNNING
            await self._emit("startup_completed")

    async def drain(self, timeout: float = 30.0) -> None:
        """Invoke optional drain hooks in startup order."""

        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        async with self._lock:
            if self._state in (LifecycleState.NEW, LifecycleState.STOPPED):
                return
            if self._state is not LifecycleState.RUNNING:
                raise LifecycleError(f"cannot drain while lifecycle is {self._state.value}")
            self._state = LifecycleState.DRAINING
            await self._emit("drain_started")
            try:
                async with asyncio.timeout(timeout):
                    for registration in self._started:
                        drain = getattr(registration.component, "drain", None)
                        if drain is not None:
                            result = drain()
                            if inspect.isawaitable(result):
                                await result
            except TimeoutError as error:
                raise LifecycleError(
                    "lifecycle drain timed out",
                    context={"timeout": timeout},
                ) from error
            finally:
                await self._emit("drain_completed")

    async def shutdown(self, grace_period: float = 30.0) -> None:
        """Stop tasks and components without leaving orphaned resources."""

        if grace_period < 0:
            raise ValueError("grace_period must be non-negative")
        async with self._lock:
            if self._state in (LifecycleState.NEW, LifecycleState.STOPPED):
                self._state = LifecycleState.STOPPED
                return
            if self._state is LifecycleState.STOPPING:
                return
            self._state = LifecycleState.STOPPING
            await self._emit("shutdown_started")
            errors: list[Exception] = []
            started = asyncio.get_running_loop().time()
            remaining = grace_period
            try:
                await self.supervisor.shutdown(remaining)
            except Exception as error:
                errors.append(error)
            remaining = max(0.0, grace_period - (asyncio.get_running_loop().time() - started))
            for registration in reversed(self._started):
                try:
                    if remaining == 0:
                        async with asyncio.timeout(0.001):
                            await registration.component.shutdown()
                    else:
                        async with asyncio.timeout(remaining):
                            await registration.component.shutdown()
                except Exception as error:
                    errors.append(error)
                remaining = max(0.0, grace_period - (asyncio.get_running_loop().time() - started))
            self._started.clear()
            self._state = LifecycleState.STOPPED
            await self._emit("shutdown_completed", forced=bool(errors))
            if errors:
                raise LifecycleError(
                    "one or more lifecycle components failed during shutdown",
                    context={"errors": tuple(type(error).__name__ for error in errors)},
                ) from ExceptionGroup("lifecycle shutdown failures", errors)

    async def _unwind(self) -> None:
        for registration in reversed(self._started):
            try:
                await registration.component.shutdown()
            except Exception:
                continue
        self._started.clear()

    async def _emit(self, event_name: str, **details: object) -> None:
        emitter = self._events
        emit = getattr(emitter, "emit", None) if emitter is not None else None
        if emit is not None:
            result = emit(event_name, **details)
            if inspect.isawaitable(result):
                await result

    async def __aenter__(self) -> LifecycleManager:
        await self.startup()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.shutdown()


__all__ = [
    "LifecycleComponent",
    "LifecycleManager",
    "LifecycleState",
    "RegisteredComponent",
    "RestartPolicy",
    "SupervisedTaskSnapshot",
    "SupervisedTaskState",
    "TaskFailure",
    "TaskSupervisor",
]
