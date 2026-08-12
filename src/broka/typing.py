"""Shared public typing aliases and protocols for pyev applications."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Protocol, TypeAlias, TypeVar

if TYPE_CHECKING:
    from .delivery import Delivery
    from .options import PublishOptions
    from .results import PublishResult
    from .routing import Route
    from .subscription import Subscription, SubscriptionOptions


JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
Headers: TypeAlias = Mapping[str, str]
TraceContext: TypeAlias = Mapping[str, JSONValue]
MessageLike: TypeAlias = object

TMessage = TypeVar("TMessage")
TMessage_co = TypeVar("TMessage_co", covariant=True)
TMessage_contra = TypeVar("TMessage_contra", contravariant=True)


MessageHandler: TypeAlias = Callable[["Delivery[TMessage]"], Awaitable[None]]


TransitionCallback: TypeAlias = Callable[[object], Awaitable[None] | None]


class Publisher(Protocol):
    """Restricted structural view for components that may only publish."""

    async def publish(
        self,
        message: MessageLike,
        *,
        route: str | Route | None = None,
        headers: Headers | None = None,
        options: PublishOptions | None = None,
    ) -> PublishResult: ...


class Subscriber(Protocol):
    """Restricted structural view for components that may only subscribe."""

    async def subscribe(
        self,
        route: str | Route,
        handler: MessageHandler[object],
        *,
        options: SubscriptionOptions | None = None,
    ) -> Subscription[object]: ...


__all__ = [
    "Headers",
    "JSONScalar",
    "JSONValue",
    "MessageHandler",
    "MessageLike",
    "Publisher",
    "Subscriber",
    "TMessage",
    "TMessage_co",
    "TMessage_contra",
    "TraceContext",
    "TransitionCallback",
]
