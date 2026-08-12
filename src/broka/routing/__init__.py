"""Public transport-independent routing API."""

from .matching import route_matches
from .models import (
    Destination,
    DestinationKind,
    HandlerPattern,
    PatternKind,
    PatternLike,
    Route,
    RouteKind,
    RouteLike,
    as_pattern,
)
from .router import DestinationRule, EventRouter, HandlerRegistration, MessageHandler, Router

__all__ = [
    "Destination",
    "DestinationKind",
    "DestinationRule",
    "EventRouter",
    "HandlerPattern",
    "HandlerRegistration",
    "MessageHandler",
    "PatternKind",
    "PatternLike",
    "Route",
    "RouteKind",
    "RouteLike",
    "Router",
    "as_pattern",
    "route_matches",
]
