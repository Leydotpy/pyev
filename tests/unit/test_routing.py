from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from broka.exceptions import RoutingError
from broka.routing import (
    Destination,
    DestinationKind,
    HandlerPattern,
    Route,
    Router,
    route_matches,
)


@dataclass(frozen=True)
class Joined:
    room: int


def test_route_matching_is_portable_and_header_aware() -> None:
    route = Route("video.publisher.joined", headers={"tenant": "acme"})

    assert route_matches("video.publisher.joined", route)
    assert route_matches("video.*", route)
    assert route_matches("*.publisher.*", route)
    assert route_matches(HandlerPattern.with_headers({"tenant": "acme"}), route)
    assert not route_matches(HandlerPattern.with_headers({"tenant": "other"}), route)
    assert not route_matches("session.*", route)


def test_type_and_version_matching() -> None:
    message = Joined(room=42)

    assert route_matches(Joined, "video.joined", message=message)
    assert route_matches(
        HandlerPattern.exact("video.joined", version=2),
        "video.joined",
        message=message,
        version=2,
    )
    assert not route_matches(
        HandlerPattern.exact("video.joined", version=1),
        "video.joined",
        message=message,
        version=2,
    )


def test_router_registration_decorator_and_priority_are_deterministic() -> None:
    router = Router()
    calls: list[str] = []

    @router.on("video.*", priority=0)
    async def wildcard(delivery: object) -> str:
        del delivery
        calls.append("wildcard")
        return "wildcard"

    @router.on("video.publisher.joined", priority=10)
    async def exact(delivery: object) -> str:
        del delivery
        calls.append("exact")
        return "exact"

    results = asyncio.run(router.dispatch(Joined(42), route="video.publisher.joined"))

    assert results == ("exact", "wildcard")
    assert calls == ["exact", "wildcard"]
    assert router.registrations[0].handler is exact
    assert router.registrations[1].handler is wildcard


def test_logical_routes_and_physical_destinations_are_distinct() -> None:
    router = Router()
    destination = Destination(
        "events-v1",
        DestinationKind.EXCHANGE,
        engine="rabbitmq",
        options={"durable": True},
    )
    router.map_destination("video.*", destination)

    assert router.destinations(Route("video.publisher.joined")) == (destination,)
    assert router.destinations(Route("session.created")) == ()
    with pytest.raises(TypeError):
        destination.options["durable"] = False  # type: ignore[index]


def test_router_rejects_synchronous_handlers() -> None:
    router = Router()

    def handler(delivery: object) -> None:
        del delivery

    with pytest.raises(RoutingError, match="asynchronous"):
        router.register("*", handler)  # type: ignore[arg-type]
