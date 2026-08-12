"""Transport-independent subscription options and lifecycle handle."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable
from uuid import uuid4

from .acknowledgements import AcknowledgementMode
from .exceptions import InvalidStateTransitionError, LifecycleError
from .typing import MessageHandler

if TYPE_CHECKING:
    from .routing import Route


TMessage = TypeVar("TMessage")


@dataclass(frozen=True, slots=True)
class SubscriptionOptions:
    """Portable consumer options validated before engine subscription."""

    acknowledgement_mode: AcknowledgementMode = AcknowledgementMode.AUTO
    durable: bool = False
    consumer_group: str | None = None
    consumer_id: str | None = None
    concurrency: int = 1
    capacity: int = 100
    prefetch: int | None = None
    max_in_flight: int | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "acknowledgement_mode", AcknowledgementMode(self.acknowledgement_mode)
        )
        _positive_int("concurrency", self.concurrency)
        _positive_int("capacity", self.capacity)
        if self.prefetch is not None:
            _positive_int("prefetch", self.prefetch)
        if self.max_in_flight is not None:
            _positive_int("max_in_flight", self.max_in_flight)
        for name in ("consumer_group", "consumer_id"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be a non-empty string when supplied")
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


class SubscriptionState(StrEnum):
    """Lifecycle state of an application subscription."""

    CREATED = "created"
    ACTIVE = "active"
    PAUSED = "paused"
    CLOSED = "closed"
    FAILED = "failed"


@runtime_checkable
class SubscriptionController(Protocol):
    """Engine/broker callbacks used by a subscription lifecycle handle."""

    async def pause(self, subscription: Subscription[Any]) -> None:
        """Pause the underlying consumer."""

    async def resume(self, subscription: Subscription[Any]) -> None:
        """Resume the underlying consumer."""

    async def close(self, subscription: Subscription[Any]) -> None:
        """Close the underlying consumer and release its resources."""


class Subscription(Generic[TMessage]):
    """A stable, awaitable lifecycle handle returned by ``Broker.subscribe``."""

    __slots__ = (
        "_controller",
        "_lock",
        "_state",
        "created_at",
        "handler",
        "id",
        "options",
        "route",
    )

    def __init__(
        self,
        route: str | Route,
        handler: MessageHandler[TMessage],
        *,
        options: SubscriptionOptions | None = None,
        id: str | None = None,
        controller: SubscriptionController | None = None,
        state: SubscriptionState = SubscriptionState.CREATED,
        created_at: datetime | None = None,
    ) -> None:
        if not callable(handler):
            raise TypeError("handler must be callable")
        identifier = str(uuid4()) if id is None else id
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValueError("subscription id must be a non-empty string")
        timestamp = created_at or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        self.id = identifier
        self.route = route
        self.handler = handler
        self.options = options or SubscriptionOptions()
        self.created_at = timestamp.astimezone(UTC)
        self._controller = controller
        self._state = SubscriptionState(state)
        self._lock = asyncio.Lock()

    @property
    def state(self) -> SubscriptionState:
        """Return the current lifecycle state."""

        return self._state

    @property
    def active(self) -> bool:
        """Return whether the subscription currently accepts deliveries."""

        return self._state is SubscriptionState.ACTIVE

    @property
    def closed(self) -> bool:
        """Return whether the subscription was permanently closed."""

        return self._state is SubscriptionState.CLOSED

    async def activate(self) -> None:
        """Mark a newly-created subscription active."""

        async with self._lock:
            if self._state is SubscriptionState.ACTIVE:
                return
            if self._state is not SubscriptionState.CREATED:
                self._invalid(SubscriptionState.ACTIVE)
            self._state = SubscriptionState.ACTIVE

    async def pause(self) -> None:
        """Pause an active underlying consumer idempotently."""

        async with self._lock:
            if self._state is SubscriptionState.PAUSED:
                return
            if self._state is not SubscriptionState.ACTIVE:
                self._invalid(SubscriptionState.PAUSED)
            if self._controller is None:
                raise LifecycleError(
                    "Subscription has no controller capable of pausing its consumer",
                    context={"subscription_id": self.id},
                )
            await self._controller.pause(self)
            self._state = SubscriptionState.PAUSED

    async def resume(self) -> None:
        """Resume a paused underlying consumer idempotently."""

        async with self._lock:
            if self._state is SubscriptionState.ACTIVE:
                return
            if self._state is not SubscriptionState.PAUSED:
                self._invalid(SubscriptionState.ACTIVE)
            if self._controller is None:
                raise LifecycleError(
                    "Subscription has no controller capable of resuming its consumer",
                    context={"subscription_id": self.id},
                )
            await self._controller.resume(self)
            self._state = SubscriptionState.ACTIVE

    async def close(self) -> None:
        """Close the subscription and its consumer exactly once."""

        async with self._lock:
            if self._state is SubscriptionState.CLOSED:
                return
            if self._controller is not None:
                await self._controller.close(self)
            self._state = SubscriptionState.CLOSED

    async def fail(self) -> None:
        """Mark a non-closed subscription failed."""

        async with self._lock:
            if self._state is SubscriptionState.CLOSED:
                self._invalid(SubscriptionState.FAILED)
            self._state = SubscriptionState.FAILED

    async def __aenter__(self) -> Subscription[TMessage]:
        if self._state is SubscriptionState.CREATED:
            await self.activate()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.close()

    def _invalid(self, target: SubscriptionState) -> None:
        raise InvalidStateTransitionError(
            f"Subscription cannot transition from {self._state.value!r} to {target.value!r}",
            context={
                "subscription_id": self.id,
                "from_state": self._state.value,
                "to_state": target.value,
            },
        )

    def __repr__(self) -> str:
        return f"Subscription(id={self.id!r}, route={self.route!r}, state={self._state.value!r})"


def _positive_int(name: str, value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


__all__ = [
    "Subscription",
    "SubscriptionController",
    "SubscriptionOptions",
    "SubscriptionState",
]
