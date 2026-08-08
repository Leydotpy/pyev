"""Framework-internal operational events, isolated from domain events."""

from .internal import (
    CriticalListenerError,
    EmitResult,
    EventEmitter,
    InternalEvent,
    InternalEventEmitter,
    InternalEventListener,
    ListenerFailure,
    ListenerRegistration,
    OperationalEvent,
    OperationalEventName,
)

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
