"""Typed exception hierarchy for :mod:`pyev`.

The exceptions in this module deliberately carry small, structured context
dictionaries.  They do not inspect exception text to decide whether an
operation is retryable, and callers can safely use :attr:`PyevError.retryable`
for that decision.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType


class PyevError(Exception):
    """Base class for all errors raised by pyev.

    Args:
        message: A concise, human-readable explanation.
        retryable: Whether retry policy may consider the failure transient.
        context: Non-sensitive structured values useful to logs and callers.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        context: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.context: Mapping[str, object] = MappingProxyType(dict(context or {}))


class ConfigurationError(PyevError):
    """Raised when broker configuration is invalid or incomplete."""


class LifecycleError(PyevError):
    """Raised when a lifecycle operation is invalid or fails."""


class RegistryError(PyevError):
    """Base class for registry and plugin discovery errors."""


class DuplicateRegistrationError(RegistryError):
    """Raised when a registry key is already owned by another object."""


class PluginLoadError(RegistryError):
    """Raised when an installed plugin cannot be loaded safely."""


class EventRegistrationError(RegistryError):
    """Raised when an event declaration or upcaster is invalid."""


class UnknownEventError(RegistryError):
    """Raised when no event class is registered for a name and version."""


class EngineError(PyevError):
    """Base class for failures reported by a transport engine."""


class EngineUnavailableError(EngineError):
    """Raised when a selected engine cannot be used in this environment."""


class ConnectionError(EngineError):
    """Raised when an engine connection operation fails."""


# A more explicit spelling for users that want to avoid shadowing the built-in.
BrokerConnectionError = ConnectionError


class PublishError(EngineError):
    """Raised when an engine cannot publish an envelope."""


class ConsumeError(EngineError):
    """Raised when an engine cannot receive or manage a delivery."""


class UnsupportedCapabilityError(EngineError):
    """Raised when an engine cannot satisfy requested semantics.

    Args:
        capability: The missing capability or a readable collection of them.
        operation: Optional operation for which the capability was requested.
        message: Optional replacement for the generated error message.
    """

    def __init__(
        self,
        capability: object,
        *,
        operation: str | None = None,
        message: str | None = None,
        context: Mapping[str, object] | None = None,
    ) -> None:
        capability_text = _describe_capability(capability)
        detail = f" for {operation!r}" if operation else ""
        merged_context = dict(context or {})
        merged_context.setdefault("capability", capability_text)
        if operation is not None:
            merged_context.setdefault("operation", operation)
        super().__init__(
            message or f"Unsupported capability {capability_text}{detail}",
            context=merged_context,
        )
        self.capability = capability
        self.operation = operation


class SerializationError(PyevError):
    """Raised when data cannot be encoded or decoded safely."""


class UnsafeSerializationError(SerializationError):
    """Raised when an unsafe serializer is used without explicit consent."""


class MessageValidationError(PyevError):
    """Raised when a message or envelope violates the public data contract."""


class RoutingError(PyevError):
    """Raised when a logical route cannot be resolved or is malformed."""


class MiddlewareError(PyevError):
    """Raised when middleware registration or execution fails."""


class AcknowledgementError(PyevError):
    """Raised when an acknowledgement adapter operation fails."""


class InvalidStateTransitionError(AcknowledgementError):
    """Raised when a delivery or subscription transition is not permitted."""


class RetryExhaustedError(PyevError):
    """Raised after a retry policy reaches its configured terminal limit."""


class CircuitOpenError(PyevError):
    """Raised when a circuit breaker rejects an operation while open."""


class DeadLetterError(PyevError):
    """Raised when dead-letter persistence or replay fails."""


class RequestTimeoutError(PyevError):
    """Raised when a request/reply operation exceeds its deadline."""


def _describe_capability(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (set, frozenset, list, tuple)):
        return ", ".join(sorted(str(item) for item in value))
    return str(value)


__all__ = [
    "AcknowledgementError",
    "BrokerConnectionError",
    "CircuitOpenError",
    "ConfigurationError",
    "ConnectionError",
    "ConsumeError",
    "DeadLetterError",
    "DuplicateRegistrationError",
    "EngineError",
    "EngineUnavailableError",
    "EventRegistrationError",
    "InvalidStateTransitionError",
    "LifecycleError",
    "MessageValidationError",
    "MiddlewareError",
    "PluginLoadError",
    "PublishError",
    "PyevError",
    "RegistryError",
    "RequestTimeoutError",
    "RetryExhaustedError",
    "RoutingError",
    "SerializationError",
    "UnknownEventError",
    "UnsafeSerializationError",
    "UnsupportedCapabilityError",
]
