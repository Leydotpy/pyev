"""Isolated asynchronous operational events.

Internal events describe framework activity and intentionally do not share the
domain-event registry. Listener failures are collected and isolated unless a
listener is explicitly registered as critical.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from uuid import uuid4


class OperationalEventName(StrEnum):
    """Names emitted by the built-in runtime services."""

    STARTUP_STARTED = "startup_started"
    STARTUP_COMPLETED = "startup_completed"
    SHUTDOWN_STARTED = "shutdown_started"
    SHUTDOWN_COMPLETED = "shutdown_completed"
    ENGINE_SELECTED = "engine_selected"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTION_FAILED = "connection_failed"
    PUBLISHED = "published"
    PUBLISH_FAILED = "publish_failed"
    RECEIVED = "received"
    HANDLER_STARTED = "handler_started"
    HANDLER_COMPLETED = "handler_completed"
    HANDLER_FAILED = "handler_failed"
    ACKNOWLEDGED = "acknowledged"
    NACKED = "nacked"
    DEFERRED = "deferred"
    RETRIED = "retried"
    BACKOFF_STARTED = "backoff_started"
    DEAD_LETTERED = "dead_lettered"
    REPLAYED = "replayed"
    HEALTH_CHANGED = "health_changed"
    CIRCUIT_OPENED = "circuit_opened"
    CIRCUIT_HALF_OPENED = "circuit_half_opened"
    CIRCUIT_CLOSED = "circuit_closed"
    TASK_FAILED = "task_failed"
    TASK_RESTARTED = "task_restarted"
    DRAIN_STARTED = "drain_started"
    DRAIN_COMPLETED = "drain_completed"


@dataclass(frozen=True, slots=True)
class OperationalEvent:
    """Transport-independent framework event."""

    name: str
    details: Mapping[str, object] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    id: str = field(default_factory=lambda: str(uuid4()))

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("event name must not be empty")
        if self.timestamp.tzinfo is None:
            raise ValueError("event timestamp must be timezone-aware")
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))

    @property
    def qualified_name(self) -> str:
        """Return the isolated public namespace for this operational event."""

        return f"pyev.internal.{self.name}"


InternalEventListener = Callable[[OperationalEvent], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ListenerRegistration:
    """A named listener registration that can later be removed."""

    id: str
    event_name: str
    listener: InternalEventListener
    critical: bool = False
    name: str = "listener"


@dataclass(frozen=True, slots=True)
class ListenerFailure:
    """A captured listener failure."""

    registration_id: str
    listener_name: str
    event_name: str
    error: Exception
    critical: bool


@dataclass(frozen=True, slots=True)
class EmitResult:
    """Outcome of an event emission."""

    event: OperationalEvent
    listeners_called: int
    failures: tuple[ListenerFailure, ...] = ()

    @property
    def successful(self) -> bool:
        """Return ``True`` if every listener completed successfully."""

        return not self.failures


class CriticalListenerError(RuntimeError):
    """Raised after one or more explicitly critical listeners fail."""

    def __init__(self, result: EmitResult) -> None:
        self.result = result
        names = ", ".join(failure.listener_name for failure in result.failures if failure.critical)
        super().__init__(f"critical listener failure while emitting {result.event.name!r}: {names}")


class InternalEventEmitter:
    """Concurrent, bounded and isolated internal event dispatcher."""

    def __init__(
        self,
        *,
        listener_timeout: float | None = None,
        failure_history_limit: int = 100,
    ) -> None:
        if listener_timeout is not None and listener_timeout <= 0:
            raise ValueError("listener_timeout must be positive")
        if failure_history_limit < 0:
            raise ValueError("failure_history_limit must be non-negative")
        self.listener_timeout = listener_timeout
        self._listeners: dict[str, list[ListenerRegistration]] = {}
        self._lock = threading.RLock()
        self._failures: deque[ListenerFailure] = deque(maxlen=failure_history_limit)

    def subscribe(
        self,
        event_name: str | OperationalEventName,
        listener: InternalEventListener,
        *,
        critical: bool = False,
        name: str | None = None,
    ) -> ListenerRegistration:
        """Register an async listener for one name or ``"*"``."""

        normalized = str(event_name)
        if not normalized:
            raise ValueError("event_name must not be empty")
        inferred_name = name or getattr(listener, "__qualname__", None)
        listener_name = inferred_name if isinstance(inferred_name, str) else "listener"
        registration = ListenerRegistration(
            id=str(uuid4()),
            event_name=normalized,
            listener=listener,
            critical=critical,
            name=listener_name,
        )
        with self._lock:
            self._listeners.setdefault(normalized, []).append(registration)
        return registration

    on = subscribe

    def unsubscribe(self, registration: ListenerRegistration | str) -> bool:
        """Remove a registration by object or identifier."""

        registration_id = (
            registration.id if isinstance(registration, ListenerRegistration) else registration
        )
        with self._lock:
            for event_name, listeners in tuple(self._listeners.items()):
                remaining = [item for item in listeners if item.id != registration_id]
                if len(remaining) != len(listeners):
                    if remaining:
                        self._listeners[event_name] = remaining
                    else:
                        del self._listeners[event_name]
                    return True
        return False

    async def emit(
        self,
        event: OperationalEvent | str | OperationalEventName,
        **details: object,
    ) -> EmitResult:
        """Emit an event and wait for all matching listeners to settle."""

        if isinstance(event, OperationalEvent):
            if details:
                raise TypeError("details cannot be supplied with an OperationalEvent instance")
            operational_event = event
        else:
            operational_event = OperationalEvent(str(event), details)

        with self._lock:
            exact = self._listeners.get(operational_event.name, ())
            wildcard = () if operational_event.name == "*" else self._listeners.get("*", ())
            listeners = (*exact, *wildcard)
        if not listeners:
            return EmitResult(operational_event, 0)

        results = await asyncio.gather(
            *(self._invoke(item, operational_event) for item in listeners),
        )
        failures = tuple(failure for failure in results if failure is not None)
        if failures:
            with self._lock:
                self._failures.extend(failures)
        outcome = EmitResult(operational_event, len(listeners), failures)
        if any(failure.critical for failure in failures):
            raise CriticalListenerError(outcome)
        return outcome

    async def _invoke(
        self,
        registration: ListenerRegistration,
        event: OperationalEvent,
    ) -> ListenerFailure | None:
        try:
            if self.listener_timeout is None:
                await registration.listener(event)
            else:
                async with asyncio.timeout(self.listener_timeout):
                    await registration.listener(event)
        except Exception as error:
            return ListenerFailure(
                registration_id=registration.id,
                listener_name=registration.name,
                event_name=event.name,
                error=error,
                critical=registration.critical,
            )
        return None

    def listeners(self, event_name: str | None = None) -> tuple[ListenerRegistration, ...]:
        """Inspect registrations without exposing mutable internal state."""

        with self._lock:
            if event_name is not None:
                return tuple(self._listeners.get(event_name, ()))
            return tuple(item for values in self._listeners.values() for item in values)

    def failures(self) -> tuple[ListenerFailure, ...]:
        """Return the bounded listener-failure history."""

        with self._lock:
            return tuple(self._failures)

    def clear(self) -> None:
        """Remove all listeners and captured failures."""

        with self._lock:
            self._listeners.clear()
            self._failures.clear()


# Compatibility spelling for callers that prefer the shorter concept name.
EventEmitter = InternalEventEmitter
InternalEvent = OperationalEvent


__all__ = [
    "CriticalListenerError",
    "EmitResult",
    "EventEmitter",
    "InternalEvent",
    "InternalEventEmitter",
    "InternalEventListener",
    "ListenerFailure",
    "ListenerRegistration",
    "OperationalEvent",
    "OperationalEventName",
]
