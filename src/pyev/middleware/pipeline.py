"""ASGI-style asynchronous middleware pipelines.

Middleware is called as ``await middleware(context, call_next)``. The first
registered item is the outermost layer: it observes the request first and the
result last. A middleware may short-circuit intentionally by returning without
calling ``call_next``.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, TypeVar, cast

from pyev.exceptions import MiddlewareError
from pyev.routing import HandlerPattern, PatternLike, Route, as_pattern, route_matches

ContextT = TypeVar("ContextT")
ResultT = TypeVar("ResultT")

type NextCallable[ContextT, ResultT] = Callable[[ContextT], Awaitable[ResultT]]


class Middleware(Protocol[ContextT, ResultT]):
    """Structural protocol implemented by middleware callables."""

    def __call__(
        self,
        context: ContextT,
        call_next: NextCallable[ContextT, ResultT],
    ) -> Awaitable[ResultT]: ...


class MiddlewareDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"


@dataclass(frozen=True, slots=True)
class MiddlewareRegistration[ContextT, ResultT]:
    """Immutable metadata for one configured middleware layer."""

    name: str
    middleware: Middleware[ContextT, ResultT]
    order: int
    sequence: int
    routes: tuple[HandlerPattern, ...]


def _is_async_callable(value: object) -> bool:
    if inspect.iscoroutinefunction(value):
        return True
    return callable(value) and inspect.iscoroutinefunction(value.__call__)


def _callable_name(value: object) -> str:
    module = getattr(value, "__module__", value.__class__.__module__)
    qualname = getattr(value, "__qualname__", value.__class__.__qualname__)
    return f"{module}.{qualname}"


def _normalise_routes(
    routes: PatternLike | Iterable[PatternLike] | None,
) -> tuple[HandlerPattern, ...]:
    if routes is None:
        return ()
    if isinstance(routes, (str, type, Route, HandlerPattern)):
        return (as_pattern(routes),)
    return tuple(as_pattern(route) for route in routes)


class MiddlewarePipeline[ContextT, ResultT]:
    """A deterministic, named, inspectable async middleware pipeline."""

    def __init__(self, *, direction: MiddlewareDirection | str) -> None:
        self._direction = MiddlewareDirection(direction)
        self._registrations: dict[str, MiddlewareRegistration[ContextT, ResultT]] = {}
        self._sequence = 0

    @property
    def direction(self) -> MiddlewareDirection:
        return self._direction

    @property
    def registrations(self) -> tuple[MiddlewareRegistration[ContextT, ResultT], ...]:
        """Return middleware in invocation order."""

        return tuple(
            sorted(
                self._registrations.values(),
                key=lambda registration: (registration.order, registration.sequence),
            )
        )

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(registration.name for registration in self.registrations)

    def register(
        self,
        middleware: Middleware[ContextT, ResultT],
        *,
        name: str | None = None,
        order: int = 0,
        routes: PatternLike | Iterable[PatternLike] | None = None,
    ) -> MiddlewareRegistration[ContextT, ResultT]:
        """Add an async middleware layer.

        Lower ``order`` values execute first. Registration sequence breaks ties,
        which makes independently constructed pipelines deterministic.
        """

        if not _is_async_callable(middleware):
            raise MiddlewareError("middleware must be an asynchronous callable")
        self._sequence += 1
        registration_name = name or _callable_name(middleware)
        if registration_name in self._registrations:
            raise MiddlewareError(f"middleware {registration_name!r} is already registered")
        registration = MiddlewareRegistration(
            name=registration_name,
            middleware=middleware,
            order=order,
            sequence=self._sequence,
            routes=_normalise_routes(routes),
        )
        self._registrations[registration_name] = registration
        return registration

    add = register

    def use(
        self,
        *,
        name: str | None = None,
        order: int = 0,
        routes: PatternLike | Iterable[PatternLike] | None = None,
    ) -> Callable[[Middleware[ContextT, ResultT]], Middleware[ContextT, ResultT]]:
        """Decorator form of :meth:`register`."""

        def decorator(
            middleware: Middleware[ContextT, ResultT],
        ) -> Middleware[ContextT, ResultT]:
            self.register(middleware, name=name, order=order, routes=routes)
            return middleware

        return decorator

    def unregister(self, name: str) -> MiddlewareRegistration[ContextT, ResultT]:
        """Remove a middleware layer by name."""

        try:
            return self._registrations.pop(name)
        except KeyError as error:
            raise MiddlewareError(f"unknown middleware {name!r}") from error

    remove = unregister

    def get(self, name: str) -> MiddlewareRegistration[ContextT, ResultT]:
        try:
            return self._registrations[name]
        except KeyError as error:
            raise MiddlewareError(f"unknown middleware {name!r}") from error

    def clear(self) -> None:
        self._registrations.clear()

    def applicable(
        self,
        *,
        route: str | Route | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> tuple[MiddlewareRegistration[ContextT, ResultT], ...]:
        """Inspect layers applicable to a particular logical route."""

        selected: list[MiddlewareRegistration[ContextT, ResultT]] = []
        for registration in self.registrations:
            if not registration.routes:
                selected.append(registration)
            elif route is not None and any(
                route_matches(pattern, route, headers) for pattern in registration.routes
            ):
                selected.append(registration)
        return tuple(selected)

    async def run(
        self,
        context: ContextT,
        terminal: NextCallable[ContextT, ResultT],
        *,
        route: str | Route | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> ResultT:
        """Execute applicable middleware around a terminal callable."""

        selected = self.applicable(route=route, headers=headers)
        next_callable: NextCallable[ContextT, ResultT] = terminal

        for registration in reversed(selected):
            following = next_callable

            async def layer(
                current_context: ContextT,
                *,
                _registration: MiddlewareRegistration[ContextT, ResultT] = registration,
                _following: NextCallable[ContextT, ResultT] = following,
            ) -> ResultT:
                result = cast(object, _registration.middleware(current_context, _following))
                if not inspect.isawaitable(result):
                    raise MiddlewareError(
                        f"middleware {_registration.name!r} returned a non-awaitable result"
                    )
                return await cast(Awaitable[ResultT], result)

            next_callable = layer

        result = cast(object, next_callable(context))
        if not inspect.isawaitable(result):
            raise MiddlewareError("terminal middleware callable returned a non-awaitable result")
        return await cast(Awaitable[ResultT], result)

    __call__ = run


class InboundMiddlewarePipeline(MiddlewarePipeline[ContextT, ResultT]):
    """A pipeline fixed to the inbound processing direction."""

    def __init__(self) -> None:
        super().__init__(direction=MiddlewareDirection.INBOUND)


class OutboundMiddlewarePipeline(MiddlewarePipeline[ContextT, ResultT]):
    """A pipeline fixed to the outbound processing direction."""

    def __init__(self) -> None:
        super().__init__(direction=MiddlewareDirection.OUTBOUND)


DEFAULT_OUTBOUND_STAGES: tuple[str, ...] = (
    "validation",
    "metadata",
    "tracing",
    "authorization",
    "serialization",
    "compression",
    "encryption",
    "observability",
    "reliability",
    "engine",
)

DEFAULT_INBOUND_STAGES: tuple[str, ...] = (
    "transport_normalization",
    "decryption",
    "decompression",
    "deserialization",
    "validation",
    "tracing",
    "observability",
    "idempotency",
    "handler",
    "acknowledgement",
    "failure_handling",
)
