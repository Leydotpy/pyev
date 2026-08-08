"""Portable route matching independent of transport capabilities."""

from __future__ import annotations

from collections.abc import Mapping
from fnmatch import fnmatchcase

from .models import PatternKind, PatternLike, Route, as_pattern


def route_matches(
    pattern: PatternLike,
    logical_route: str | Route,
    headers: Mapping[str, str] | None = None,
    *,
    message: object | None = None,
    version: int | None = None,
) -> bool:
    """Return whether a local handler pattern matches a logical route.

    Matching is deliberately performed in the framework so wildcard,
    namespace, and header subscriptions behave consistently on every engine.
    Required headers are an exact subset of the supplied headers.
    """

    candidate = as_pattern(pattern)
    route = Route.parse(logical_route)

    if candidate.version is not None and candidate.version != version:
        return False

    supplied_headers = dict(route.headers)
    supplied_headers.update(headers or {})
    if any(supplied_headers.get(key) != expected for key, expected in candidate.headers.items()):
        return False

    if candidate.kind is PatternKind.TYPE:
        message_type = candidate.value
        assert isinstance(message_type, type)
        return message is not None and isinstance(message, message_type)

    assert isinstance(candidate.value, str)
    if candidate.kind is PatternKind.EXACT:
        return route.name == candidate.value
    if candidate.kind is PatternKind.NAMESPACE:
        namespace = candidate.value.removesuffix(".*")
        return route.name.startswith(f"{namespace}.")
    if candidate.kind is PatternKind.HEADERS:
        return candidate.value == "*" or fnmatchcase(route.name, candidate.value)
    return fnmatchcase(route.name, candidate.value)
