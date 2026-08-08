"""End-to-end broker tests against the built-in process-local engines."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from pyev.acknowledgements import AcknowledgementMode
from pyev.broker import Broker, BrokerState
from pyev.delivery import Delivery, DeliveryState
from pyev.engines.local import LocalEngine
from pyev.engines.memory import MemoryEngine
from pyev.event import EventRegistry, event
from pyev.exceptions import RequestTimeoutError
from pyev.observability.health import HealthStatus
from pyev.options import BatchPublishOptions, RequestOptions
from pyev.subscription import SubscriptionOptions


@event("tests.broker.item.created", register=False)
@dataclass(frozen=True, slots=True)
class ItemCreated:
    item_id: int
    label: str


@event("tests.broker.rpc.calculate", register=False)
@dataclass(frozen=True, slots=True)
class Calculate:
    left: int
    right: int


@event("tests.broker.rpc.calculated", register=False)
@dataclass(frozen=True, slots=True)
class Calculated:
    total: int


def _event_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(ItemCreated)
    registry.register(Calculate)
    registry.register(Calculated)
    return registry


def _broker(
    engine: LocalEngine | MemoryEngine,
    *,
    reliability: Mapping[str, object] | None = None,
) -> Broker:
    return Broker(
        {
            "engine": engine.name,
            "source": "pyev-tests",
            "reliability": reliability or {},
        },
        engine=engine,
        event_registry=_event_registry(),
    )


@asynccontextmanager
async def _running(broker: Broker) -> AsyncIterator[Broker]:
    await broker.startup()
    try:
        yield broker
    finally:
        await broker.shutdown()


async def _wait_for(event: asyncio.Event, timeout: float = 1.0) -> None:
    async with asyncio.timeout(timeout):
        await event.wait()


async def test_broker_startup_shutdown_and_health_are_idempotent() -> None:
    broker = _broker(LocalEngine())
    await broker.startup()
    await broker.startup()

    assert broker.state is BrokerState.RUNNING
    assert broker.ready
    assert broker.engine is not None
    assert broker.engine.name == "local"

    health = await broker.health()
    assert health.status is HealthStatus.HEALTHY
    assert health.live and health.ready
    assert health.lifecycle_state == BrokerState.RUNNING.value
    assert health.selected_engine == "local"
    assert health.connection_state == "connected"

    await broker.shutdown()
    await broker.shutdown()
    assert broker.state is BrokerState.STOPPED
    assert not broker.ready

    stopped_health = await broker.health()
    assert not stopped_health.live
    assert not stopped_health.ready
    assert stopped_health.connection_state == "disconnected"


async def test_typed_publish_subscribe_wildcard_and_auto_acknowledgement() -> None:
    typed_deliveries: list[Delivery[object]] = []
    wildcard_deliveries: list[Delivery[object]] = []

    async def typed_handler(delivery: Delivery[object]) -> None:
        typed_deliveries.append(delivery)

    async def wildcard_handler(delivery: Delivery[object]) -> None:
        wildcard_deliveries.append(delivery)

    async with _running(_broker(LocalEngine())) as broker:
        typed = await broker.subscribe(ItemCreated, typed_handler)
        wildcard = await broker.subscribe("tests.broker.*", wildcard_handler)
        result = await broker.publish(ItemCreated(7, "typed"))

        assert result.accepted
        assert result.route == "tests.broker.item.created"
        assert result.engine == "local"
        assert typed.active and wildcard.active
        assert len(typed_deliveries) == len(wildcard_deliveries) == 1
        assert typed_deliveries[0].message == ItemCreated(7, "typed")
        assert typed_deliveries[0].envelope.source == "pyev-tests"
        assert typed_deliveries[0].state is DeliveryState.ACKNOWLEDGED
        assert typed_deliveries[0].transport_metadata["engine"] == "local"
        assert wildcard_deliveries[0].state is DeliveryState.ACKNOWLEDGED

        await broker.unsubscribe(typed)
        await broker.unsubscribe(typed)
        assert typed.closed


async def test_manual_acknowledgement_is_not_repeated_by_broker() -> None:
    captured: list[Delivery[object]] = []

    async def handler(delivery: Delivery[object]) -> None:
        captured.append(delivery)
        await delivery.ack()

    async with _running(_broker(LocalEngine())) as broker:
        await broker.subscribe(
            ItemCreated,
            handler,
            options=SubscriptionOptions(acknowledgement_mode=AcknowledgementMode.MANUAL),
        )
        await broker.publish(ItemCreated(1, "manual"))

    assert len(captured) == 1
    assert captured[0].state is DeliveryState.ACKNOWLEDGED


async def test_batch_publish_returns_ordered_successes_and_delivers_every_item() -> None:
    delivered: list[int] = []

    async def handler(delivery: Delivery[object]) -> None:
        assert isinstance(delivery.message, ItemCreated)
        delivered.append(delivery.message.item_id)

    async with _running(_broker(LocalEngine())) as broker:
        await broker.subscribe(ItemCreated, handler)
        result = await broker.publish_batch(
            [ItemCreated(index, f"item-{index}") for index in range(6)],
            options=BatchPublishOptions(concurrency=3),
        )

        assert result.ok
        assert result.successful == 6
        assert result.failed == 0
        assert len(result.results) == 6
        assert {item.route for item in result.results} == {"tests.broker.item.created"}
        assert sorted(delivered) == list(range(6))


async def test_request_reply_uses_shared_dispatcher_and_preserves_typed_response() -> None:
    requests: list[Delivery[object]] = []

    async with _running(_broker(LocalEngine())) as broker:

        async def calculate(delivery: Delivery[object]) -> None:
            requests.append(delivery)
            assert isinstance(delivery.message, Calculate)
            await broker.reply(
                delivery,
                Calculated(delivery.message.left + delivery.message.right),
            )

        await broker.subscribe(Calculate, calculate)
        response = await broker.request(Calculate(20, 22), timeout=0.5)

        assert response == Calculated(42)
        assert len(requests) == 1
        assert requests[0].envelope.reply_to is not None
        assert requests[0].envelope.correlation_id is not None
        assert requests[0].state is DeliveryState.ACKNOWLEDGED

        with pytest.raises(RequestTimeoutError):
            await broker.request(
                {"value": "nobody-listens"},
                timeout=0.01,
                options=RequestOptions(route="tests.broker.rpc.unhandled"),
            )


async def test_handler_retries_then_dead_letters_and_rejects_delivery() -> None:
    attempts: list[Delivery[object]] = []
    reliability = {
        "handler_retry": {
            "max_attempts": 2,
            "backoff": {"strategy": "fixed", "seconds": 0},
        }
    }

    async def failing_handler(delivery: Delivery[object]) -> None:
        attempts.append(delivery)
        raise RuntimeError("handler remains unavailable")

    async with _running(_broker(LocalEngine(), reliability=reliability)) as broker:
        await broker.subscribe(ItemCreated, failing_handler)
        result = await broker.publish(ItemCreated(99, "poison"))
        records = await broker.dead_letters.filter()
        health = await broker.health()

        assert result.accepted
        assert len(attempts) == 2
        assert attempts[0] is attempts[1]
        assert attempts[-1].state is DeliveryState.DEAD_LETTERED
        assert len(records) == 1
        assert records[0].event_type == "tests.broker.item.created"
        assert records[0].schema_version == 1
        assert records[0].engine == "local"
        assert records[0].error_type == "RetryExhaustedError"
        assert records[0].envelope_bytes is not None
        assert health.retry_count == 1
        assert health.dead_letter_count == 1


async def test_memory_broker_retries_transient_publish_and_reports_health() -> None:
    delivered = asyncio.Event()
    engine = MemoryEngine({"fail_publish_count": 1})
    reliability = {
        "publish_retry": {
            "max_attempts": 2,
            "backoff": {"strategy": "fixed", "seconds": 0},
        }
    }

    async def handler(delivery: Delivery[object]) -> None:
        assert delivery.message == ItemCreated(5, "retried")
        delivered.set()

    async with _running(_broker(engine, reliability=reliability)) as broker:
        await broker.subscribe(ItemCreated, handler)
        result = await broker.publish(ItemCreated(5, "retried"))
        await _wait_for(delivered)
        health = await broker.health()

        assert result.accepted
        assert result.engine == "memory"
        assert health.status is HealthStatus.HEALTHY
        assert health.retry_count == 1
        assert health.publish_failures == 0
        assert health.active_consumers == 1
        assert health.queue_depth == 0
