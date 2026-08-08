from __future__ import annotations

import asyncio

import pytest

from pyev.engines.base import EnginePublishContext, EngineSubscription
from pyev.testing import (
    DeterministicClock,
    FakeEngine,
    MockPublisher,
    assert_published,
    broker_override,
    eventually,
    get_broker_override,
)


@pytest.mark.asyncio
async def test_deterministic_clock_can_manually_wake_sleepers() -> None:
    clock = DeterministicClock(auto_advance=False)
    sleeping = asyncio.create_task(clock.sleep(5))
    await asyncio.sleep(0)
    assert clock.pending_sleeps == (5,)
    clock.advance(5)
    await sleeping
    assert clock.now == 5


@pytest.mark.asyncio
async def test_fake_engine_captures_publish_and_delivers_to_consumer() -> None:
    engine = FakeEngine()
    await engine.connect()
    received: list[bytes] = []

    async def callback(message: object) -> None:
        received.append(message.payload)  # type: ignore[attr-defined]

    consumer = await engine.create_consumer(
        EngineSubscription("sub", "events.*", "events.*"),
        callback,
    )
    context = EnginePublishContext("message-1")
    await engine.publish("events.created", b"payload", context)
    await engine.emit("events.created", b"incoming")

    assert consumer.id == "sub"
    assert received == [b"incoming"]
    assert_published(engine, destination="events.created")


@pytest.mark.asyncio
async def test_mock_publisher_assertion_and_scoped_override() -> None:
    publisher = MockPublisher()
    await publisher.publish("message", route="events")
    assert_published(publisher, route="events")

    assert get_broker_override() is None
    async with broker_override(publisher):
        assert get_broker_override() is publisher
    assert get_broker_override() is None


@pytest.mark.asyncio
async def test_eventually_waits_for_async_state_change() -> None:
    ready = False

    async def set_ready() -> None:
        nonlocal ready
        await asyncio.sleep(0)
        ready = True

    task = asyncio.create_task(set_ready())
    await eventually(lambda: ready, timeout=0.2, interval=0)
    await task
