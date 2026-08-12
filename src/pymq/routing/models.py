"""Transport-independent routing value objects.

The three values in this module deliberately model different concerns:

``Route``
    The logical address used by application code.
``Destination``
    The physical address understood by a transport engine.
``HandlerPattern``
    A local subscription pattern used to select handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from pymq.exceptions import RoutingError


class RouteKind(StrEnum):
    """Portable logical routing modes."""

    TOPIC = "topic"
    DIRECT = "direct"
    FANOUT = "fanout"
    BROADCAST = "broadcast"
    REQUEST = "request"
    REPLY = "reply"


class DestinationKind(StrEnum):
    """Common physical destination shapes exposed to engine adapters."""

    QUEUE = "queue"
    TOPIC = "topic"
    EXCHANGE = "exchange"
    SUBJECT = "subject"
    STREAM = "stream"
    LOCAL = "local"
    REPLY = "reply"


class PatternKind(StrEnum):
    """The matching strategy of a :class:`HandlerPattern`."""

    EXACT = "exact"
    WILDCARD = "wildcard"
    NAMESPACE = "namespace"
    TYPE = "type"
    HEADERS = "headers"


type HeaderItems = tuple[tuple[str, str], ...]


def _validate_name(value: str, *, label: str, allow_wildcards: bool = False) -> str:
    value = value.strip()
    if not value:
        raise RoutingError(f"{label} must not be empty")
    if len(value) > 512:
        raise RoutingError(f"{label} must not exceed 512 characters")
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise RoutingError(f"{label} must not contain whitespace or control characters")
    if not allow_wildcards and any(character in value for character in "*?["):
        raise RoutingError(f"{label} must not contain wildcard characters")
    return value


def _normalise_headers(headers: Mapping[str, str] | None) -> HeaderItems:
    if not headers:
        return ()
    items: list[tuple[str, str]] = []
    for key, value in headers.items():
        normalised_key = str(key).strip()
        if not normalised_key:
            raise RoutingError("header names must not be empty")
        if any(ord(character) < 32 for character in normalised_key):
            raise RoutingError("header names must not contain control characters")
        items.append((normalised_key, str(value)))
    return tuple(sorted(items))


@dataclass(frozen=True, slots=True, init=False)
class Route:
    """An immutable logical application route.

    A route never contains an engine-specific queue, topic, or exchange object.
    That translation belongs in a destination rule or an engine adapter.
    """

    name: str
    kind: RouteKind
    _headers: HeaderItems = field(repr=False)

    def __init__(
        self,
        name: str,
        kind: RouteKind | str = RouteKind.TOPIC,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        object.__setattr__(self, "name", _validate_name(name, label="route"))
        object.__setattr__(self, "kind", RouteKind(kind))
        object.__setattr__(self, "_headers", _normalise_headers(headers))

    @property
    def headers(self) -> Mapping[str, str]:
        """Return immutable route headers."""

        return MappingProxyType(dict(self._headers))

    @property
    def logical_name(self) -> str:
        """An explicit alias useful where routes and destinations coexist."""

        return self.name

    @classmethod
    def parse(cls, value: str | Route) -> Route:
        """Normalise a route-like value without changing an existing route."""

        if isinstance(value, Route):
            return value
        return cls(value)


@dataclass(frozen=True, slots=True, init=False)
class Destination:
    """An immutable physical destination selected for a transport engine."""

    name: str
    kind: DestinationKind
    engine: str | None
    _options: tuple[tuple[str, Any], ...] = field(repr=False, hash=False)

    def __init__(
        self,
        name: str,
        kind: DestinationKind | str = DestinationKind.TOPIC,
        *,
        engine: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> None:
        object.__setattr__(self, "name", _validate_name(name, label="destination"))
        object.__setattr__(self, "kind", DestinationKind(kind))
        object.__setattr__(self, "engine", engine)
        object.__setattr__(
            self,
            "_options",
            tuple(sorted((str(key), value) for key, value in (options or {}).items())),
        )

    @property
    def options(self) -> Mapping[str, Any]:
        """Return a read-only view of engine-specific destination options."""

        return MappingProxyType(dict(self._options))


@dataclass(frozen=True, slots=True, init=False)
class HandlerPattern:
    """A local handler-matching rule.

    Header requirements are conjunctive and act in addition to the route or type
    criterion. A version, when supplied, must match exactly.
    """

    value: str | type[object]
    kind: PatternKind
    _headers: HeaderItems = field(repr=False)
    version: int | None

    def __init__(
        self,
        value: str | type[object] = "*",
        kind: PatternKind | str | None = None,
        *,
        headers: Mapping[str, str] | None = None,
        version: int | None = None,
    ) -> None:
        if version is not None and version < 1:
            raise RoutingError("handler pattern version must be at least 1")

        if isinstance(value, type):
            inferred_kind = PatternKind.TYPE
            normalised_value: str | type[object] = value
        else:
            normalised_value = _validate_name(
                value,
                label="handler pattern",
                allow_wildcards=True,
            )
            if kind is None:
                if normalised_value.endswith(".*") and normalised_value.count("*") == 1:
                    inferred_kind = PatternKind.NAMESPACE
                elif any(character in normalised_value for character in "*?["):
                    inferred_kind = PatternKind.WILDCARD
                else:
                    inferred_kind = PatternKind.EXACT
            else:
                inferred_kind = PatternKind(kind)

        if kind is not None:
            inferred_kind = PatternKind(kind)
        if isinstance(normalised_value, type) and inferred_kind is not PatternKind.TYPE:
            raise RoutingError("Python types require a type handler pattern")
        if isinstance(normalised_value, str) and inferred_kind is PatternKind.TYPE:
            raise RoutingError("type handler patterns require a Python type")
        if inferred_kind is PatternKind.HEADERS and not headers:
            raise RoutingError("headers handler patterns require at least one header")

        object.__setattr__(self, "value", normalised_value)
        object.__setattr__(self, "kind", inferred_kind)
        object.__setattr__(self, "_headers", _normalise_headers(headers))
        object.__setattr__(self, "version", version)

    @property
    def headers(self) -> Mapping[str, str]:
        """Return immutable required headers."""

        return MappingProxyType(dict(self._headers))

    @classmethod
    def exact(cls, route: str, *, version: int | None = None) -> HandlerPattern:
        return cls(route, PatternKind.EXACT, version=version)

    @classmethod
    def namespace(cls, namespace: str, *, version: int | None = None) -> HandlerPattern:
        namespace = namespace.removesuffix(".*")
        return cls(f"{namespace}.*", PatternKind.NAMESPACE, version=version)

    @classmethod
    def wildcard(cls, pattern: str = "*", *, version: int | None = None) -> HandlerPattern:
        return cls(pattern, PatternKind.WILDCARD, version=version)

    @classmethod
    def for_type(
        cls,
        message_type: type[object],
        *,
        version: int | None = None,
    ) -> HandlerPattern:
        return cls(message_type, PatternKind.TYPE, version=version)

    @classmethod
    def with_headers(
        cls,
        headers: Mapping[str, str],
        *,
        route: str = "*",
        version: int | None = None,
    ) -> HandlerPattern:
        kind = PatternKind.HEADERS if route == "*" else None
        return cls(route, kind, headers=headers, version=version)


type RouteLike = str | Route
type PatternLike = str | type[object] | Route | HandlerPattern


def as_pattern(
    value: PatternLike,
    *,
    headers: Mapping[str, str] | None = None,
    version: int | None = None,
) -> HandlerPattern:
    """Convert a public pattern value to a :class:`HandlerPattern`."""

    if isinstance(value, HandlerPattern):
        if headers is not None or version is not None:
            return HandlerPattern(
                value.value,
                value.kind,
                headers=headers if headers is not None else value.headers,
                version=version if version is not None else value.version,
            )
        return value
    if isinstance(value, Route):
        return HandlerPattern.exact(value.name, version=version)
    return HandlerPattern(value, headers=headers, version=version)
