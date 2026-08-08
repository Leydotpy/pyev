"""Contract-oriented tests for pyev's built-in local and memory engines."""

from __future__ import annotations

import asyncio

import pytest

from pyev.capabilities import Capability
from pyev.engines.base import (
    EngineIncomingMessage,
    EnginePublishContext,
    EngineSubscription,
)
from pyev.engines.local import LocalEngine
from pyev.engines.memory import MemoryBackpressureError, MemoryEngine


def _context(message_id: str) -> EnginePublishContext:
    return EnginePublishContext(message_id=message_id)


async def _wait_for(event: asyncio.Event, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        await event.wait()


async def _wait_for_queue_depth(
    engine: MemoryEngine,
    expected: int,
    *,
    timeout: float = 1.0,
) -> None:
    async with asyncio.timeout(timeout):
        while True:
            health = await engine.healthcheck()
            if health.details.get("queue_depth") == expected:
                return
            await asyncio.sleep(0)


async def test_local_engine_lifecycle_health_wildcard_and_requeue_ack() -> None:
    engine = LocalEngine()
    before = await engine.healthcheck()
    assert not before.connected
    with pytest.raises(RuntimeError, match="not connected"):
        await engine.publish("tests.engine.one", b"payload", _context("before"))

    await engine.connect()
    await engine.connect()
    attempts: list[int] = []

    async def callback(incoming: EngineIncomingMessage) -> None:
        attempts.append(incoming.attempt)
        if incoming.attempt == 1:
            await incoming.acknowledgement.nack(requeue=True)
        else:
            await incoming.acknowledgement.ack()

    consumer = await engine.create_consumer(
        EngineSubscription(
            id="local-wildcard",
            pattern="tests.engine.*",
            destination="tests.engine.*",
        ),
        callback,
    )
    result = await engine.publish("tests.engine.one", b"payload", _context("local-message"))

    assert result.accepted
    assert result.transport_id == "local-message"
    assert attempts == [1, 2]
    assert engine.capabilities.supports(Capability.PUBLISH_SUBSCRIBE)
    assert engine.capabilities.supports(Capability.WILDCARD_SUBSCRIPTIONS)

    health = await engine.healthcheck()
    assert health.connected and health.healthy
    assert health.details["active_consumers"] == 1
    await consumer.close()
    assert (await engine.healthcheck()).details["active_consumers"] == 0
    await engine.disconnect()
    await engine.disconnect()
    assert not (await engine.healthcheck()).connected


async def test_local_engine_isolates_nonmatching_routes_and_rejects_duplicates() -> None:
    engine = LocalEngine()
    await engine.connect()
    called = asyncio.Event()

    async def callback(incoming: EngineIncomingMessage) -> None:
        called.set()
        await incoming.acknowledgement.ack()

    subscription = EngineSubscription(
        id="unique-local",
        pattern="tests.matching.*",
        destination="tests.matching.*",
    )
    consumer = await engine.create_consumer(subscription, callback)
    with pytest.raises(ValueError, match="already registered"):
        await engine.create_consumer(subscription, callback)

    await engine.publish("tests.other.route", b"ignored", _context("ignored"))
    await asyncio.sleep(0)
    assert not called.is_set()

    await consumer.close()
    await engine.disconnect()


async def test_memory_engine_delivers_asynchronously_and_reports_queue_health() -> None:
    engine = MemoryEngine()
    await engine.connect()
    delivered = asyncio.Event()
    received: list[EngineIncomingMessage] = []

    async def callback(incoming: EngineIncomingMessage) -> None:
        received.append(incoming)
        await incoming.acknowledgement.ack()
        delivered.set()

    consumer = await engine.create_consumer(
        EngineSubscription(
            id="memory-delivery",
            pattern="tests.memory.*",
            destination="tests.memory.*",
            capacity=2,
        ),
        callback,
    )
    result = await engine.publish("tests.memory.one", b"payload", _context("memory-message"))
    await _wait_for(delivered)
    await _wait_for_queue_depth(engine, 0)

    assert result.accepted
    assert received[0].payload == b"payload"
    assert received[0].attempt == 1
    assert received[0].transport_metadata["engine"] == "memory"
    health = await engine.healthcheck()
    assert health.connected and health.healthy
    assert health.details["active_consumers"] == 1
    assert health.details["queue_depth"] == 0

    await consumer.close()
    await engine.disconnect()
    await engine.disconnect()


async def test_memory_engine_applies_bounded_reject_backpressure() -> None:
    engine = MemoryEngine({"overflow_policy": "reject", "drain_timeout": 1.0})
    await engine.connect()
    delivered = asyncio.Event()
    seen: list[str] = []

    async def callback(incoming: EngineIncomingMessage) -> None:
        seen.append(incoming.payload.decode("ascii"))
        await incoming.acknowledgement.ack()
        if len(seen) == 2:
            delivered.set()

    consumer = await engine.create_consumer(
        EngineSubscription(
            id="memory-bounded",
            pattern="tests.pressure",
            destination="tests.pressure",
            capacity=1,
        ),
        callback,
    )
    await consumer.pause()
    await engine.publish("tests.pressure", b"first", _context("first"))
    # Wait until the single worker owns the first item and is paused before its
    # callback. The queue now has exactly one free bounded slot.
    await _wait_for_queue_depth(engine, 0)
    await engine.publish("tests.pressure", b"second", _context("second"))
    assert (await engine.healthcheck()).details["queue_depth"] == 1

    with pytest.raises(MemoryBackpressureError, match="queue is full"):
        await engine.publish("tests.pressure", b"third", _context("third"))

    await consumer.resume()
    await _wait_for(delivered)
    await _wait_for_queue_depth(engine, 0)
    assert seen == ["first", "second"]

    await consumer.close()
    await engine.disconnect()


async def test_memory_engine_drop_newest_reports_unaccepted_without_unbounded_growth() -> None:
    engine = MemoryEngine({"overflow_policy": "drop-newest", "drain_timeout": 1.0})
    await engine.connect()
    delivered = asyncio.Event()

    async def callback(incoming: EngineIncomingMessage) -> None:
        await incoming.acknowledgement.ack()
        delivered.set()

    consumer = await engine.create_consumer(
        EngineSubscription(
            id="memory-drop",
            pattern="tests.drop",
            destination="tests.drop",
            capacity=1,
        ),
        callback,
    )
    await consumer.pause()
    await engine.publish("tests.drop", b"first", _context("drop-first"))
    await _wait_for_queue_depth(engine, 0)
    accepted = await engine.publish("tests.drop", b"second", _context("drop-second"))
    dropped = await engine.publish("tests.drop", b"third", _context("drop-third"))

    assert accepted.accepted
    assert not dropped.accepted
    health = await engine.healthcheck()
    assert health.details["queue_depth"] == 1
    assert health.details["dropped"] == 1

    await consumer.resume()
    await _wait_for(delivered)
    await consumer.close()
    await engine.disconnect()
