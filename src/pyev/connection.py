"""Central connection, heartbeat, reconnect, lease, and drain policy."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from pyev.engines.base import BaseEngine, EngineHealth
from pyev.exceptions import BrokerConnectionError
from pyev.lifecycle import RestartPolicy, TaskSupervisor
from pyev.observability.health import ComponentHealth, HealthStatus
from pyev.observability.redaction import redact_text
from pyev.reliability.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from pyev.reliability.retry import RetryContext, RetryManager, RetryPolicy

T = TypeVar("T")


class ConnectionState(StrEnum):
    """Framework-level connection lifecycle."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DRAINING = "draining"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ConnectionSnapshot:
    """Immutable connection state for health and diagnostics."""

    engine: str
    state: ConnectionState
    active_leases: int
    reconnect_count: int
    connected_since: float | None
    last_connected_at: float | None
    last_disconnected_at: float | None
    last_error_type: str | None
    last_error_message: str | None
    heartbeat_running: bool


type RestoreCallback = Callable[[], Awaitable[None]]
type ConnectionErrorClassifier = Callable[[BaseException], bool]
type EngineOperation[T] = Callable[[BaseEngine], Awaitable[T]]


class ConnectionManager:
    """Own framework connection policy around one transport engine.

    The engine supplies only transport primitives. Retry timing, circuit state,
    heartbeat, subscription restoration, draining, and shutdown order remain in
    this manager.
    """

    def __init__(
        self,
        engine: BaseEngine,
        *,
        retry_manager: RetryManager | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        event_emitter: object | None = None,
        supervisor: TaskSupervisor | None = None,
        heartbeat_interval: float | None = 30.0,
        heartbeat_timeout: float = 5.0,
        reconnect_on_unhealthy: bool = True,
        connection_error_classifier: ConnectionErrorClassifier | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if heartbeat_interval is not None and heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")
        self.engine = engine
        self.retry_policy = (
            retry_policy
            if retry_policy is not None
            else RetryPolicy(max_attempts=5, name="connection")
        )
        self.retry_manager = (
            retry_manager
            if retry_manager is not None
            else RetryManager(self.retry_policy, sleep=sleep, clock=clock)
        )
        self._events = event_emitter
        self.circuit_breaker = (
            circuit_breaker
            if circuit_breaker is not None
            else CircuitBreaker(
                f"connection:{engine.name}",
                CircuitBreakerConfig(failure_threshold=5, recovery_timeout=30.0),
                clock=clock,
                event_emitter=event_emitter,
            )
        )
        self._supervisor = (
            supervisor
            if supervisor is not None
            else TaskSupervisor(
                sleep=sleep,
                clock=clock,
                event_emitter=event_emitter,
            )
        )
        self._owns_supervisor = supervisor is None
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.reconnect_on_unhealthy = reconnect_on_unhealthy
        self._classify_connection_error = connection_error_classifier or (
            lambda error: isinstance(error, (BrokerConnectionError, OSError))
        )
        self._clock = clock
        self._sleep = sleep
        self._state = ConnectionState.DISCONNECTED
        self._operation_lock = asyncio.Lock()
        self._lease_condition = asyncio.Condition()
        self._active_leases = 0
        self._restore_callbacks: list[tuple[str, RestoreCallback]] = []
        self._reconnect_count = 0
        self._connected_since: float | None = None
        self._last_connected_at: float | None = None
        self._last_disconnected_at: float | None = None
        self._last_error: BaseException | None = None
        self._heartbeat_task_name = f"connection:{engine.name}:heartbeat"

    @property
    def state(self) -> ConnectionState:
        """Return the connection lifecycle state."""

        return self._state

    @property
    def connected(self) -> bool:
        """Return whether new operation leases may be acquired."""

        return self._state is ConnectionState.CONNECTED

    @property
    def active_leases(self) -> int:
        """Return the number of in-flight operations."""

        return self._active_leases

    def register_restore_callback(
        self,
        callback: RestoreCallback,
        *,
        name: str | None = None,
    ) -> None:
        """Register topology or subscription recovery in deterministic order."""

        inferred_name = name or getattr(callback, "__qualname__", None)
        callback_name = inferred_name if isinstance(inferred_name, str) else "restore"
        if any(existing == callback_name for existing, _ in self._restore_callbacks):
            raise ValueError(f"restore callback {callback_name!r} is already registered")
        self._restore_callbacks.append((callback_name, callback))

    def unregister_restore_callback(self, name: str) -> bool:
        """Remove a topology-restoration callback by name."""

        for index, (existing, _callback) in enumerate(self._restore_callbacks):
            if existing == name:
                del self._restore_callbacks[index]
                return True
        return False

    async def startup(self) -> None:
        """Connect idempotently and start the optional heartbeat service."""

        async with self._operation_lock:
            if self._state is ConnectionState.CONNECTED:
                return
            if self._state in (ConnectionState.DRAINING, ConnectionState.CLOSING):
                raise BrokerConnectionError(
                    f"cannot connect while manager is {self._state.value}",
                    retryable=False,
                )
            self._state = ConnectionState.CONNECTING
            await self._connect_locked(reconnecting=False)
            self._start_heartbeat_if_needed()

    connect = startup

    async def ensure_connected(self) -> None:
        """Ensure a usable connection exists."""

        if self._state is ConnectionState.CONNECTED:
            return
        await self.startup()

    async def reconnect(self) -> None:
        """Reconnect once at a time and restore registered topology."""

        async with self._operation_lock:
            if self._state in (
                ConnectionState.DRAINING,
                ConnectionState.CLOSING,
                ConnectionState.CLOSED,
            ):
                raise BrokerConnectionError(
                    f"cannot reconnect while manager is {self._state.value}",
                    retryable=False,
                )
            self._state = ConnectionState.RECONNECTING
            try:
                await self.engine.disconnect()
            except Exception:
                # A stale/broken connection may not support a clean disconnect;
                # the subsequent connect remains authoritative.
                pass
            self._connected_since = None
            self._last_disconnected_at = self._clock()
            await self._emit("disconnected", engine=self.engine.name, reconnecting=True)
            self._reconnect_count += 1
            await self._connect_locked(reconnecting=True)

    async def _connect_locked(self, *, reconnecting: bool) -> None:
        async def connect_operation() -> None:
            await self.circuit_breaker.call(self.engine.connect)

        transport_connected = False
        try:
            await self.retry_manager.run(
                connect_operation,
                self.retry_policy,
                context=RetryContext(
                    operation="reconnect" if reconnecting else "connect",
                    metadata={"engine": self.engine.name},
                ),
            )
            transport_connected = True
            for callback_name, callback in self._restore_callbacks:
                try:
                    await callback()
                except Exception as error:
                    raise BrokerConnectionError(
                        "connection topology restoration failed",
                        retryable=True,
                        context={
                            "engine": self.engine.name,
                            "callback": callback_name,
                            "error_type": type(error).__name__,
                        },
                    ) from error
        except BaseException as error:
            if transport_connected:
                try:
                    await self.engine.disconnect()
                except Exception:
                    pass
            self._last_error = error
            self._state = ConnectionState.FAILED
            await self._emit(
                "connection_failed",
                engine=self.engine.name,
                error_type=type(error).__name__,
                reconnecting=reconnecting,
            )
            raise
        now = self._clock()
        self._connected_since = now
        self._last_connected_at = now
        self._last_error = None
        self._state = ConnectionState.CONNECTED
        await self._emit(
            "connected",
            engine=self.engine.name,
            reconnecting=reconnecting,
            reconnect_count=self._reconnect_count,
        )

    def _start_heartbeat_if_needed(self) -> None:
        if self.heartbeat_interval is None:
            return
        existing = {item.name: item for item in self._supervisor.snapshots()}
        if self._heartbeat_task_name in existing and not existing[self._heartbeat_task_name].done:
            return
        self._supervisor.start_soon(
            self._heartbeat_loop,
            name=self._heartbeat_task_name,
            restart_policy=RestartPolicy.ON_FAILURE,
            max_restarts=3,
        )

    async def _heartbeat_loop(self) -> None:
        assert self.heartbeat_interval is not None
        while self._state not in (
            ConnectionState.DRAINING,
            ConnectionState.CLOSING,
            ConnectionState.CLOSED,
        ):
            await self._sleep(self.heartbeat_interval)
            if self._state is not ConnectionState.CONNECTED:
                if self.reconnect_on_unhealthy and self._state is ConnectionState.FAILED:
                    await self.reconnect()
                continue
            try:
                async with asyncio.timeout(self.heartbeat_timeout):
                    engine_health = await self.engine.healthcheck()
                if not engine_health.healthy or not engine_health.connected:
                    raise BrokerConnectionError(
                        "engine heartbeat reported an unhealthy connection",
                        retryable=True,
                        context={"engine": self.engine.name},
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._last_error = error
                self._state = ConnectionState.FAILED
                await self._emit(
                    "connection_failed",
                    engine=self.engine.name,
                    error_type=type(error).__name__,
                    heartbeat=True,
                )
                if self.reconnect_on_unhealthy:
                    await self.reconnect()

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[BaseEngine]:
        """Lease the connected engine for one in-flight operation."""

        await self.ensure_connected()
        async with self._lease_condition:
            if self._state is not ConnectionState.CONNECTED:
                raise BrokerConnectionError(
                    f"connection is not available ({self._state.value})",
                    retryable=self._state is ConnectionState.FAILED,
                    context={"engine": self.engine.name, "state": self._state.value},
                )
            self._active_leases += 1
        try:
            yield self.engine
        finally:
            async with self._lease_condition:
                self._active_leases -= 1
                self._lease_condition.notify_all()

    async def run(self, operation: EngineOperation[T]) -> T:
        """Run an engine operation under a lease and the connection circuit."""

        try:
            async with self.lease() as engine:
                return await self.circuit_breaker.call(operation, engine)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._classify_connection_error(error):
                self._last_error = error
                self._state = ConnectionState.FAILED
                # Reconnect policy is centralized here, but the failed operation
                # is not replayed implicitly; its caller's retry policy decides.
                await self.reconnect()
            raise

    async def drain(self, timeout: float = 30.0) -> bool:
        """Reject new leases and wait for in-flight operations to finish."""

        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        if self._state in (ConnectionState.CLOSED, ConnectionState.DISCONNECTED):
            return True
        self._state = ConnectionState.DRAINING
        await self._emit("drain_started", engine=self.engine.name, inflight=self._active_leases)
        try:
            async with asyncio.timeout(timeout):
                async with self._lease_condition:
                    await self._lease_condition.wait_for(lambda: self._active_leases == 0)
        except TimeoutError:
            return False
        await self._emit("drain_completed", engine=self.engine.name)
        return True

    async def shutdown(self, grace_period: float = 30.0) -> None:
        """Drain, stop heartbeat, and disconnect idempotently."""

        if grace_period < 0:
            raise ValueError("grace_period must be non-negative")
        async with self._operation_lock:
            if self._state is ConnectionState.CLOSED:
                return
            await self._supervisor.cancel(self._heartbeat_task_name)
            drained = await self.drain(grace_period)
            self._state = ConnectionState.CLOSING
            try:
                await self.engine.disconnect()
            except Exception as error:
                self._last_error = error
                self._state = ConnectionState.FAILED
                raise BrokerConnectionError(
                    "engine disconnect failed",
                    retryable=False,
                    context={"engine": self.engine.name, "error_type": type(error).__name__},
                ) from error
            finally:
                self._connected_since = None
                self._last_disconnected_at = self._clock()
            self._state = ConnectionState.CLOSED
            await self._emit("disconnected", engine=self.engine.name, forced=not drained)
            if self._owns_supervisor:
                await self._supervisor.shutdown()

    disconnect = shutdown

    async def health(self) -> ComponentHealth:
        """Merge manager and engine state into a redacted component result."""

        started = self._clock()
        engine_health: EngineHealth | None = None
        error: BaseException | None = None
        try:
            async with asyncio.timeout(self.heartbeat_timeout):
                engine_health = await self.engine.healthcheck()
        except Exception as caught:
            error = caught
        latency = max(0.0, self._clock() - started)
        if self._state is ConnectionState.CONNECTED and engine_health is not None:
            status = (
                HealthStatus.HEALTHY
                if engine_health.healthy and engine_health.connected
                else HealthStatus.DEGRADED
            )
        elif self._state in (
            ConnectionState.CONNECTING,
            ConnectionState.RECONNECTING,
            ConnectionState.DRAINING,
        ):
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.UNHEALTHY
        details: dict[str, object] = {
            "engine": self.engine.name,
            "state": self._state.value,
            "active_leases": self._active_leases,
            "reconnect_count": self._reconnect_count,
            "circuit_state": self.circuit_breaker.state.value,
        }
        if engine_health is not None:
            details.update(engine_health.details)
            details["engine_latency_ms"] = engine_health.latency_ms
        return ComponentHealth(
            name=f"connection:{self.engine.name}",
            status=status,
            message=(f"{type(error).__name__}: {error}" if error is not None else None),
            latency=latency,
            details=details,
        )

    def snapshot(self) -> ConnectionSnapshot:
        """Return a low-cost synchronous connection snapshot."""

        heartbeat_running = any(
            item.name == self._heartbeat_task_name and not item.done
            for item in self._supervisor.snapshots()
        )
        return ConnectionSnapshot(
            engine=self.engine.name,
            state=self._state,
            active_leases=self._active_leases,
            reconnect_count=self._reconnect_count,
            connected_since=self._connected_since,
            last_connected_at=self._last_connected_at,
            last_disconnected_at=self._last_disconnected_at,
            last_error_type=(type(self._last_error).__name__ if self._last_error else None),
            last_error_message=(redact_text(str(self._last_error)) if self._last_error else None),
            heartbeat_running=heartbeat_running,
        )

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
            return

    async def __aenter__(self) -> ConnectionManager:
        await self.startup()
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.shutdown()


__all__ = [
    "ConnectionErrorClassifier",
    "ConnectionManager",
    "ConnectionSnapshot",
    "ConnectionState",
    "EngineOperation",
    "RestoreCallback",
]
