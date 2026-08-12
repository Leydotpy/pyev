"""Framework delivery object and validated message lifecycle state machine."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Generic, TypeVar, cast

from .acknowledgements import (
    AcknowledgementAdapter,
    AcknowledgementMode,
    NoOpAcknowledgementAdapter,
)
from .envelope import Envelope
from .exceptions import (
    AcknowledgementError,
    InvalidStateTransitionError,
    PyevError,
    UnsupportedCapabilityError,
)

TMessage = TypeVar("TMessage")
_LOGGER = logging.getLogger(__name__)


class DeliveryState(StrEnum):
    """Transport-independent lifecycle states for a message delivery."""

    CREATED = "created"
    PUBLISHED = "published"
    QUEUED = "queued"
    DELIVERED = "delivered"
    PROCESSING = "processing"
    ACKNOWLEDGED = "acknowledged"
    NACKED = "nacked"
    DEFERRED = "deferred"
    REQUEUED = "requeued"
    RETRY_SCHEDULED = "retry_scheduled"
    REJECTED = "rejected"
    DEAD_LETTERED = "dead_lettered"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


TERMINAL_DELIVERY_STATES = frozenset(
    {
        DeliveryState.ACKNOWLEDGED,
        DeliveryState.DEAD_LETTERED,
        DeliveryState.EXPIRED,
        DeliveryState.CANCELLED,
    }
)


_ALLOWED_TRANSITIONS: Mapping[DeliveryState, frozenset[DeliveryState]] = {
    DeliveryState.CREATED: frozenset(
        {
            DeliveryState.PUBLISHED,
            DeliveryState.QUEUED,
            DeliveryState.DELIVERED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.PUBLISHED: frozenset(
        {DeliveryState.QUEUED, DeliveryState.DELIVERED, DeliveryState.CANCELLED}
    ),
    DeliveryState.QUEUED: frozenset(
        {DeliveryState.DELIVERED, DeliveryState.EXPIRED, DeliveryState.CANCELLED}
    ),
    DeliveryState.DELIVERED: frozenset(
        {
            DeliveryState.PROCESSING,
            DeliveryState.ACKNOWLEDGED,
            DeliveryState.NACKED,
            DeliveryState.REQUEUED,
            DeliveryState.DEFERRED,
            DeliveryState.RETRY_SCHEDULED,
            DeliveryState.REJECTED,
            DeliveryState.DEAD_LETTERED,
            DeliveryState.EXPIRED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.PROCESSING: frozenset(
        {
            DeliveryState.ACKNOWLEDGED,
            DeliveryState.NACKED,
            DeliveryState.REQUEUED,
            DeliveryState.DEFERRED,
            DeliveryState.RETRY_SCHEDULED,
            DeliveryState.REJECTED,
            DeliveryState.DEAD_LETTERED,
            DeliveryState.EXPIRED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.NACKED: frozenset(
        {
            DeliveryState.REQUEUED,
            DeliveryState.RETRY_SCHEDULED,
            DeliveryState.REJECTED,
            DeliveryState.DEAD_LETTERED,
        }
    ),
    DeliveryState.DEFERRED: frozenset(
        {
            DeliveryState.QUEUED,
            DeliveryState.DELIVERED,
            DeliveryState.RETRY_SCHEDULED,
            DeliveryState.EXPIRED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.REQUEUED: frozenset(
        {
            DeliveryState.QUEUED,
            DeliveryState.DELIVERED,
            DeliveryState.EXPIRED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.RETRY_SCHEDULED: frozenset(
        {
            DeliveryState.QUEUED,
            DeliveryState.DELIVERED,
            DeliveryState.DEAD_LETTERED,
            DeliveryState.EXPIRED,
            DeliveryState.CANCELLED,
        }
    ),
    DeliveryState.REJECTED: frozenset({DeliveryState.DEAD_LETTERED}),
    DeliveryState.ACKNOWLEDGED: frozenset(),
    DeliveryState.DEAD_LETTERED: frozenset(),
    DeliveryState.EXPIRED: frozenset(),
    DeliveryState.CANCELLED: frozenset(),
}


@dataclass(frozen=True, slots=True)
class DeliveryTransition:
    """Immutable description of a completed delivery state change."""

    delivery: Delivery[Any]
    from_state: DeliveryState
    to_state: DeliveryState
    at: datetime
    reason: str | None = None


TransitionObserver = Callable[[DeliveryTransition], Awaitable[None] | None]


class Delivery(Generic[TMessage]):
    """A decoded message plus mutable processing and acknowledgement state."""

    __slots__ = (
        "_acknowledgement",
        "_lock",
        "_observer",
        "_state",
        "_transport_metadata",
        "attempt",
        "consumer_id",
        "deadline",
        "delivered_at",
        "envelope",
        "message",
        "mode",
        "route",
        "subscription_id",
    )

    def __init__(
        self,
        message: TMessage,
        envelope: Envelope,
        *,
        route: str | None = None,
        subscription_id: str | None = None,
        attempt: int = 1,
        delivered_at: datetime | None = None,
        deadline: datetime | None = None,
        consumer_id: str | None = None,
        acknowledgement: AcknowledgementAdapter | None = None,
        mode: AcknowledgementMode = AcknowledgementMode.MANUAL,
        state: DeliveryState = DeliveryState.DELIVERED,
        transport_metadata: Mapping[str, object] | None = None,
        on_transition: TransitionObserver | None = None,
    ) -> None:
        if not isinstance(envelope, Envelope):
            raise TypeError("envelope must be an Envelope")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        delivered = _aware_utc("delivered_at", delivered_at or datetime.now(UTC))
        normalized_deadline = None if deadline is None else _aware_utc("deadline", deadline)
        if normalized_deadline is not None and normalized_deadline < delivered:
            raise ValueError("deadline cannot be earlier than delivered_at")
        self.message = message
        self.envelope = envelope
        self.route = route
        self.subscription_id = subscription_id
        self.attempt = attempt
        self.delivered_at = delivered
        self.deadline = normalized_deadline
        self.consumer_id = consumer_id
        self.mode = AcknowledgementMode(mode)
        self._acknowledgement = acknowledgement or NoOpAcknowledgementAdapter()
        self._state = DeliveryState(state)
        self._transport_metadata = MappingProxyType(dict(transport_metadata or {}))
        self._observer = on_transition
        self._lock = asyncio.Lock()

    @property
    def state(self) -> DeliveryState:
        """Return the current delivery state."""

        return self._state

    @property
    def is_terminal(self) -> bool:
        """Return whether no later lifecycle transition is permitted."""

        return self._state in TERMINAL_DELIVERY_STATES

    @property
    def acknowledged(self) -> bool:
        """Return whether this delivery completed successfully."""

        return self._state is DeliveryState.ACKNOWLEDGED

    @property
    def message_id(self) -> str:
        """Return the immutable envelope message identifier."""

        return self.envelope.id

    @property
    def transport_metadata(self) -> Mapping[str, object]:
        """Return explicit, read-only metadata supplied by the transport.

        The mapping contains normalized diagnostics only; application handlers
        do not receive a transport-native message object through this property.
        """

        return self._transport_metadata

    def native_metadata(self) -> Mapping[str, object]:
        """Return the explicit, read-only transport metadata escape hatch."""

        return self._transport_metadata

    async def transition(
        self, target: DeliveryState, *, reason: str | None = None
    ) -> DeliveryTransition | None:
        """Move to an allowed state and notify the transition observer.

        Repeating the current state is idempotent and returns ``None``.
        """

        normalized = DeliveryState(target)
        async with self._lock:
            transition = self._transition_locked(normalized, reason=reason)
        await self._notify(transition)
        return transition

    async def start_processing(self) -> None:
        """Mark a delivered message as being handled."""

        await self.transition(DeliveryState.PROCESSING)

    async def ack(self) -> None:
        """Acknowledge successful processing exactly once."""

        await self._perform(
            DeliveryState.ACKNOWLEDGED,
            "ack",
            self._acknowledgement.ack,
        )

    async def nack(self, *, requeue: bool = True) -> None:
        """Negatively acknowledge processing, optionally requesting redelivery."""

        target = DeliveryState.REQUEUED if requeue else DeliveryState.NACKED
        await self._perform(
            target,
            "nack",
            lambda: self._acknowledgement.nack(requeue=requeue),
        )

    async def reject(self) -> None:
        """Reject this delivery without implicit redelivery."""

        await self._perform(
            DeliveryState.REJECTED,
            "reject",
            self._acknowledgement.reject,
        )

    async def requeue(self) -> None:
        """Explicitly request redelivery through the acknowledgement adapter."""

        await self._perform(
            DeliveryState.REQUEUED,
            "requeue",
            self._acknowledgement.requeue,
        )

    async def defer(self, *, delay: float) -> None:
        """Defer processing for a positive number of seconds."""

        seconds = _positive_seconds("delay", delay)
        await self._perform(
            DeliveryState.DEFERRED,
            "defer",
            lambda: self._acknowledgement.defer(seconds),
        )

    async def touch(self, *, extension: float) -> None:
        """Extend a delivery's lease without changing its lifecycle state."""

        seconds = _positive_seconds("extension", extension)
        if self.mode is AcknowledgementMode.NONE:
            raise UnsupportedCapabilityError("acknowledgements", operation="touch")
        async with self._lock:
            if self._state not in {
                DeliveryState.DELIVERED,
                DeliveryState.PROCESSING,
                DeliveryState.DEFERRED,
            }:
                raise InvalidStateTransitionError(
                    f"Cannot touch a delivery in state {self._state.value!r}",
                    context={"state": self._state.value, "operation": "touch"},
                )
            try:
                await self._acknowledgement.touch(seconds)
            except (UnsupportedCapabilityError, AcknowledgementError):
                raise
            except Exception as exc:
                raise AcknowledgementError(
                    "Transport acknowledgement operation 'touch' failed",
                    retryable=isinstance(exc, PyevError) and exc.retryable,
                    context={"operation": "touch", "message_id": self.message_id},
                ) from exc

    async def schedule_retry(self, *, reason: str | None = None) -> None:
        """Move the delivery into the framework retry-scheduled state."""

        await self.transition(DeliveryState.RETRY_SCHEDULED, reason=reason)

    async def mark_dead_lettered(self, *, reason: str | None = None) -> None:
        """Move the delivery into the terminal dead-letter state."""

        await self.transition(DeliveryState.DEAD_LETTERED, reason=reason)

    async def mark_expired(self, *, reason: str | None = None) -> None:
        """Move the delivery into the terminal expired state."""

        await self.transition(DeliveryState.EXPIRED, reason=reason)

    async def cancel(self, *, reason: str | None = None) -> None:
        """Move the delivery into the terminal cancelled state."""

        await self.transition(DeliveryState.CANCELLED, reason=reason)

    async def _perform(
        self,
        target: DeliveryState,
        operation: str,
        invoke: Callable[[], Awaitable[None]],
    ) -> None:
        if self.mode is AcknowledgementMode.NONE:
            raise UnsupportedCapabilityError("acknowledgements", operation=operation)
        async with self._lock:
            if self._state is target:
                return
            self._validate_transition(target)
            try:
                await invoke()
            except (UnsupportedCapabilityError, AcknowledgementError):
                raise
            except Exception as exc:
                raise AcknowledgementError(
                    f"Transport acknowledgement operation {operation!r} failed",
                    retryable=isinstance(exc, PyevError) and exc.retryable,
                    context={"operation": operation, "message_id": self.message_id},
                ) from exc
            transition = self._transition_locked(target, reason=operation)
        await self._notify(transition)

    def _transition_locked(
        self, target: DeliveryState, *, reason: str | None
    ) -> DeliveryTransition | None:
        if target is self._state:
            return None
        self._validate_transition(target)
        previous = self._state
        self._state = target
        return DeliveryTransition(
            delivery=cast(Delivery[Any], self),
            from_state=previous,
            to_state=target,
            at=datetime.now(UTC),
            reason=reason,
        )

    def _validate_transition(self, target: DeliveryState) -> None:
        if target not in _ALLOWED_TRANSITIONS[self._state]:
            raise InvalidStateTransitionError(
                f"Delivery cannot transition from {self._state.value!r} to {target.value!r}",
                context={
                    "from_state": self._state.value,
                    "to_state": target.value,
                    "message_id": self.message_id,
                },
            )

    async def _notify(self, transition: DeliveryTransition | None) -> None:
        if transition is None or self._observer is None:
            return
        try:
            result = self._observer(transition)
            if inspect.isawaitable(result):
                await result
        except Exception:
            _LOGGER.exception(
                "Delivery transition observer failed",
                extra={
                    "message_id": self.message_id,
                    "from_state": transition.from_state.value,
                    "to_state": transition.to_state.value,
                },
            )

    def __repr__(self) -> str:
        return (
            f"Delivery(message_id={self.message_id!r}, route={self.route!r}, "
            f"state={self._state.value!r}, attempt={self.attempt})"
        )


def _positive_seconds(name: str, value: float) -> float:
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive number of seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")
    return seconds


def _aware_utc(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(UTC)


__all__ = [
    "TERMINAL_DELIVERY_STATES",
    "Delivery",
    "DeliveryState",
    "DeliveryTransition",
    "TransitionObserver",
]
