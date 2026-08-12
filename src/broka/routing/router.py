"""Deterministic registration and dispatch for local message handlers."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from broka.exceptions import RoutingError

from .matching import route_matches
from .models import Destination, HandlerPattern, PatternLike, RouteLike, as_pattern

DeliveryT = TypeVar("DeliveryT", contravariant=True)
ResultT = TypeVar("ResultT", covariant=True)


class MessageHandler(Protocol[DeliveryT, ResultT]):
    """Structural protocol for an asynchronous application handler."""

    def __call__(self, delivery: DeliveryT) -> Awaitable[ResultT]: ...


type Handler = Callable[[Any], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class HandlerRegistration:
    """An immutable, introspectable handler registration."""

    name: str
    pattern: HandlerPattern
    handler: Handler
    priority: int
    sequence: int


@dataclass(frozen=True, slots=True)
class DestinationRule:
    """Maps a logical handler-style pattern to a physical destination."""

    pattern: HandlerPattern
    destination: Destination
    priority: int
    sequence: int


def _is_async_callable(value: object) -> bool:
    if inspect.iscoroutinefunction(value):
        return True
    return callable(value) and inspect.iscoroutinefunction(value.__call__)


def _handler_name(handler: Handler) -> str:
    module = getattr(handler, "__module__", handler.__class__.__module__)
    qualname = getattr(handler, "__qualname__", handler.__class__.__qualname__)
    return f"{module}.{qualname}"


class Router:
    """A transport-independent handler and destination router.

    Higher priority registrations run first; ties preserve registration order.
    Dispatch is sequential by default, making side effects and tests
    deterministic. Engines remain free to dispatch distinct deliveries with
    configured consumer concurrency.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerRegistration] = {}
        self._destination_rules: list[DestinationRule] = []
        self._sequence = 0

    @property
    def registrations(self) -> tuple[HandlerRegistration, ...]:
        """Return handlers in deterministic invocation order."""

        return tuple(
            sorted(
                self._handlers.values(),
                key=lambda item: (-item.priority, item.sequence),
            )
        )

    @property
    def destination_rules(self) -> tuple[DestinationRule, ...]:
        """Return destination rules in deterministic resolution order."""

        return tuple(
            sorted(
                self._destination_rules,
                key=lambda item: (-item.priority, item.sequence),
            )
        )

    def register(
        self,
        pattern: PatternLike,
        handler: Handler,
        *,
        name: str | None = None,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
        priority: int = 0,
    ) -> HandlerRegistration:
        """Register an async handler and return its registration record."""

        if not _is_async_callable(handler):
            raise RoutingError("message handlers must be asynchronous callables")
        self._sequence += 1
        generated_name = _handler_name(handler)
        registration_name = name or generated_name
        if registration_name in self._handlers:
            if name is not None:
                raise RoutingError(f"handler registration {registration_name!r} already exists")
            registration_name = f"{generated_name}:{self._sequence}"
        registration = HandlerRegistration(
            name=registration_name,
            pattern=as_pattern(pattern, headers=headers, version=version),
            handler=handler,
            priority=priority,
            sequence=self._sequence,
        )
        self._handlers[registration_name] = registration
        return registration

    def on(
        self,
        pattern: PatternLike,
        *,
        name: str | None = None,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
        priority: int = 0,
    ) -> Callable[[Handler], Handler]:
        """Decorate and register an async handler without runtime side effects."""

        def decorator(handler: Handler) -> Handler:
            self.register(
                pattern,
                handler,
                name=name,
                headers=headers,
                version=version,
                priority=priority,
            )
            return handler

        return decorator

    def unregister(self, registration: HandlerRegistration | str) -> HandlerRegistration:
        """Remove a handler registration by value or name."""

        name = registration.name if isinstance(registration, HandlerRegistration) else registration
        try:
            return self._handlers.pop(name)
        except KeyError as error:
            raise RoutingError(f"unknown handler registration {name!r}") from error

    def map_destination(
        self,
        pattern: PatternLike,
        destination: Destination,
        *,
        priority: int = 0,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
    ) -> DestinationRule:
        """Map a logical route pattern to a physical destination."""

        self._sequence += 1
        rule = DestinationRule(
            pattern=as_pattern(pattern, headers=headers, version=version),
            destination=destination,
            priority=priority,
            sequence=self._sequence,
        )
        self._destination_rules.append(rule)
        return rule

    def unmap_destination(self, rule: DestinationRule) -> None:
        """Remove an exact destination rule."""

        try:
            self._destination_rules.remove(rule)
        except ValueError as error:
            raise RoutingError("unknown destination rule") from error

    def match(
        self,
        route: RouteLike,
        *,
        message: object | None = None,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
    ) -> tuple[HandlerRegistration, ...]:
        """Resolve all matching local handlers."""

        return tuple(
            registration
            for registration in self.registrations
            if route_matches(
                registration.pattern,
                route,
                headers,
                message=message,
                version=version,
            )
        )

    resolve = match

    def destinations(
        self,
        route: RouteLike,
        *,
        message: object | None = None,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
    ) -> tuple[Destination, ...]:
        """Resolve physical destinations, de-duplicated in rule order."""

        resolved: list[Destination] = []
        for rule in self.destination_rules:
            if route_matches(rule.pattern, route, headers, message=message, version=version):
                if rule.destination not in resolved:
                    resolved.append(rule.destination)
        return tuple(resolved)

    async def dispatch(
        self,
        delivery: object,
        *,
        route: RouteLike | None = None,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
    ) -> tuple[object, ...]:
        """Invoke matching handlers sequentially and return their results."""

        message = getattr(delivery, "message", delivery)
        envelope = getattr(delivery, "envelope", None)
        logical_route = route or getattr(delivery, "route", None)
        if logical_route is None and envelope is not None:
            logical_route = getattr(envelope, "type", None) or getattr(
                envelope, "message_type", None
            )
        if logical_route is None:
            raise RoutingError("dispatch requires a logical route")
        effective_headers = headers
        if effective_headers is None and envelope is not None:
            effective_headers = getattr(envelope, "headers", None)
        effective_version = version
        if effective_version is None and envelope is not None:
            effective_version = getattr(envelope, "version", None)

        results: list[object] = []
        for registration in self.match(
            logical_route,
            message=message,
            headers=effective_headers,
            version=effective_version,
        ):
            result = registration.handler(delivery)
            if not inspect.isawaitable(result):
                raise RoutingError(f"handler {registration.name!r} returned a non-awaitable result")
            results.append(await result)
        return tuple(results)

    def clear(self) -> None:
        """Remove all handlers and destination rules (primarily for tests)."""

        self._handlers.clear()
        self._destination_rules.clear()


EventRouter = Router
