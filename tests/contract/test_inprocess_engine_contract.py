"""Reusable minimum-engine conformance exercised by built-in engines."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from pymq.capabilities import Capability
from pymq.engines.base import (
    BaseEngine,
    EngineIncomingMessage,
    EnginePublishContext,
    EngineSubscription,
)
from pymq.engines.local import LocalEngine
from pymq.engines.memory import MemoryEngine


@pytest.fixture(params=[LocalEngine, MemoryEngine], ids=["local", "memory"])
def engine_factory(request: pytest.FixtureRequest) -> Callable[[], BaseEngine]:
    engine_type = request.param
    return engine_type


async def test_minimum_engine_contract(engine_factory: Callable[[], BaseEngine]) -> None:
    engine = engine_factory()
    assert engine.is_available()
    assert engine.capabilities.supports(Capability.PUBLISH_SUBSCRIBE)
    assert not (await engine.healthcheck()).connected

    await engine.connect()
    delivered = asyncio.Event()
    payloads: list[bytes] = []

    async def callback(message: EngineIncomingMessage) -> None:
        payloads.append(message.payload)
        await message.acknowledgement.ack()
        delivered.set()

    consumer = await engine.create_consumer(
        EngineSubscription(
            id=f"{engine.name}-contract",
            pattern="contract.message",
            destination="contract.message",
            capacity=2,
        ),
        callback,
    )
    result = await engine.publish(
        "contract.message",
        b"payload",
        EnginePublishContext(message_id="contract-1"),
    )
    async with asyncio.timeout(1):
        await delivered.wait()

    assert result.accepted
    assert payloads == [b"payload"]
    assert (await engine.healthcheck()).healthy
    await consumer.pause()
    await consumer.resume()
    await consumer.close()
    await consumer.close()
    await engine.disconnect()
    await engine.disconnect()
    assert not (await engine.healthcheck()).connected
