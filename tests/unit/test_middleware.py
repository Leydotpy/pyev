from __future__ import annotations

import asyncio

import pytest

from pymq.exceptions import MiddlewareError
from pymq.middleware import InboundMiddlewarePipeline, OutboundMiddlewarePipeline


def test_middleware_order_and_unwind_are_deterministic() -> None:
    pipeline: OutboundMiddlewarePipeline[str, str] = OutboundMiddlewarePipeline()
    calls: list[str] = []

    async def second(context: str, call_next):  # type: ignore[no-untyped-def]
        calls.append(f"second-in:{context}")
        result = await call_next(context)
        calls.append("second-out")
        return result

    async def first(context: str, call_next):  # type: ignore[no-untyped-def]
        calls.append(f"first-in:{context}")
        result = await call_next(context)
        calls.append("first-out")
        return result

    async def terminal(context: str) -> str:
        calls.append(f"terminal:{context}")
        return "done"

    pipeline.register(second, name="second", order=20)
    pipeline.register(first, name="first", order=10)

    assert asyncio.run(pipeline.run("value", terminal)) == "done"
    assert pipeline.names == ("first", "second")
    assert calls == [
        "first-in:value",
        "second-in:value",
        "terminal:value",
        "second-out",
        "first-out",
    ]


def test_middleware_can_short_circuit_and_be_scoped_by_route() -> None:
    pipeline: InboundMiddlewarePipeline[str, str] = InboundMiddlewarePipeline()
    calls: list[str] = []

    async def scoped(context: str, call_next):  # type: ignore[no-untyped-def]
        del call_next
        calls.append(context)
        return "short"

    async def terminal(context: str) -> str:
        return f"terminal:{context}"

    pipeline.register(scoped, name="auth", routes="private.*")

    assert (
        asyncio.run(pipeline.run("request", terminal, route="public.event")) == "terminal:request"
    )
    assert asyncio.run(pipeline.run("request", terminal, route="private.event")) == "short"
    assert calls == ["request"]


def test_named_middleware_registration_is_introspectable_and_unique() -> None:
    pipeline: InboundMiddlewarePipeline[object, object] = InboundMiddlewarePipeline()

    async def layer(context: object, call_next):  # type: ignore[no-untyped-def]
        return await call_next(context)

    registration = pipeline.register(layer, name="trace", routes=("a.*", "b.*"))

    assert registration.name == "trace"
    assert len(registration.routes) == 2
    assert pipeline.get("trace") is registration
    with pytest.raises(MiddlewareError, match="already registered"):
        pipeline.register(layer, name="trace")
