"""Service-free regression tests for the optional transport adapters."""

from __future__ import annotations

import asyncio
import importlib.util as importlib_util
import os
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from pymq.capabilities import Capability
from pymq.engines.base import (
    Availability,
    EngineIncomingMessage,
    EnginePublishContext,
)
from pymq.engines.kafka import (
    KafkaEngine,
    _KafkaAcknowledgement,
    _KafkaConsumer,
    _topic_pattern,
)
from pymq.engines.rabbitmq import (
    RabbitMQEngine,
    _amqp_pattern,
    _rabbit_confirmation_accepted,
    _RabbitAcknowledgement,
    _RabbitConsumer,
)
from pymq.engines.redis import (
    RedisEngine,
    _decode_stream_entry,
    _parse_xautoclaim,
    _PubSubAcknowledgement,
    _StreamsAcknowledgement,
)
from pymq.exceptions import PublishError, UnsupportedCapabilityError


def test_optional_engine_modules_do_not_import_client_libraries() -> None:
    root = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(root / "src"), environment.get("PYTHONPATH", ""))
    )
    code = """
import sys
import pyev.engines.redis
import pyev.engines.rabbitmq
import pyev.engines.kafka
assert 'redis' not in sys.modules
assert 'aio_pika' not in sys.modules
assert 'aiokafka' not in sys.modules
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    "engine_type",
    (RedisEngine, RabbitMQEngine, KafkaEngine),
)
def test_availability_probes_are_side_effect_free_when_import_lookup_fails(
    engine_type: Any,
) -> None:
    with patch.object(
        importlib_util,
        "find_spec",
        side_effect=ModuleNotFoundError("optional dependency is absent"),
    ):
        availability = engine_type.is_available({})

    assert not availability
    assert availability.reason is not None


def test_availability_accepts_defaults_but_rejects_explicit_empty_endpoints() -> None:
    with patch.object(importlib_util, "find_spec", return_value=object()):
        assert RedisEngine.is_available({})
        assert not RedisEngine.is_available({"url": ""})
    with patch.object(importlib_util, "find_spec", return_value=object()):
        assert RabbitMQEngine.is_available({})
        assert not RabbitMQEngine.is_available({"url": ""})
    with patch.object(importlib_util, "find_spec", return_value=object()):
        assert KafkaEngine.is_available({})
        assert not KafkaEngine.is_available({"bootstrap_servers": ""})


@pytest.mark.parametrize(
    "factory",
    (
        lambda: RedisEngine({"url": None}),
        lambda: RedisEngine({"max_length": 0}),
        lambda: RedisEngine({"claim_idle_ms": 0}),
        lambda: RabbitMQEngine({"exchange": ""}),
        lambda: RabbitMQEngine({"url": None}),
        lambda: KafkaEngine({"bootstrap_servers": ""}),
    ),
)
def test_invalid_optional_engine_configuration_is_rejected(
    factory: Callable[[], object],
) -> None:
    with pytest.raises(ValueError):
        factory()


def test_redis_capabilities_distinguish_pubsub_from_streams() -> None:
    pubsub = RedisEngine({"mode": "pubsub"}).capabilities
    assert pubsub.supports(Capability.PUBLISH_SUBSCRIBE)
    assert pubsub.supports(Capability.WILDCARD_SUBSCRIPTIONS)
    assert pubsub.supports(Capability.AT_MOST_ONCE)
    assert not pubsub.supports(Capability.AT_LEAST_ONCE)

    streams = RedisEngine({"mode": "streams"}).capabilities
    assert streams.supports(Capability.DURABLE_SUBSCRIPTIONS)
    assert streams.supports(Capability.CONSUMER_GROUPS)
    assert streams.supports(Capability.AT_LEAST_ONCE)
    assert streams.attribute(Capability.AT_LEAST_ONCE, "pending_recovery") == "xautoclaim"
    assert not streams.supports(Capability.WILDCARD_SUBSCRIPTIONS)
    assert not streams.supports(Capability.QUEUE_DEPTH)


def test_rabbitmq_capabilities_follow_configured_topology() -> None:
    defaults = RabbitMQEngine().capabilities
    assert defaults.supports(Capability.DURABLE_SUBSCRIPTIONS)
    assert defaults.supports(Capability.PUBLISHER_CONFIRMS)
    assert not defaults.supports(Capability.NATIVE_DEAD_LETTER)
    assert not defaults.supports(Capability.MESSAGE_PRIORITIES)

    transient = RabbitMQEngine({"durable": False, "publisher_confirms": False}).capabilities
    assert not transient.supports(Capability.DURABLE_SUBSCRIPTIONS)
    assert not transient.supports(Capability.PUBLISHER_CONFIRMS)

    dead_lettered = RabbitMQEngine({"dead_letter_exchange": "failed"}).capabilities
    assert dead_lettered.supports(Capability.NATIVE_DEAD_LETTER)


def test_kafka_capabilities_do_not_claim_unimplemented_extensions() -> None:
    engine = KafkaEngine({"transactional_id": "configured-but-not-exposed"})
    capabilities = engine.capabilities
    assert capabilities.supports(Capability.DURABLE_SUBSCRIPTIONS)
    assert capabilities.supports(Capability.CONSUMER_GROUPS)
    assert capabilities.supports(Capability.PARTITION_ORDERING)
    assert capabilities.supports(Capability.PUBLISHER_CONFIRMS)
    assert capabilities.attribute(Capability.PUBLISHER_CONFIRMS, "idempotent") is True
    assert not capabilities.supports(Capability.TRANSACTIONS)
    assert not capabilities.supports(Capability.BATCH_PUBLISHING)
    assert not capabilities.supports(Capability.CONSUMER_LAG)

    non_idempotent = KafkaEngine({"enable_idempotence": False}).capabilities
    assert non_idempotent.supports(Capability.PUBLISHER_CONFIRMS)
    assert non_idempotent.attribute(Capability.PUBLISHER_CONFIRMS, "acknowledgement") == "leader"


def test_transport_pattern_translation_is_explicit() -> None:
    assert _amqp_pattern("*") == "#"
    assert _amqp_pattern("orders.*") == "orders.*"

    pattern = _topic_pattern("orders.*")
    assert pattern.fullmatch("orders.created")
    assert pattern.fullmatch("orders.created.eu")
    assert not pattern.fullmatch("payments.created")


def test_redis_stream_response_helpers_validate_and_decode_wire_values() -> None:
    cursor, entries = _parse_xautoclaim([b"2-0", [[b"1-0", {b"payload": b"value"}]], []])
    assert cursor == "2-0"
    assert entries == ((b"1-0", {b"payload": b"value"}),)

    payload, headers, entry_id = _decode_stream_entry(
        b"1-0",
        {b"payload": b"value", b"headers": b'{"tenant":"acme"}'},
    )
    assert payload == b"value"
    assert headers == {"tenant": "acme"}
    assert entry_id == "1-0"

    with pytest.raises(RuntimeError, match="invalid response"):
        _parse_xautoclaim(object())
    with pytest.raises(RuntimeError, match="headers"):
        _decode_stream_entry("1-0", {"headers": "[]"})


class _RedisStreamClient:
    def __init__(self) -> None:
        self.added: list[tuple[str, Mapping[str, object], Mapping[str, object]]] = []
        self.acked: list[tuple[str, str, str]] = []

    async def xadd(
        self,
        stream: str,
        fields: Mapping[str, object],
        **kwargs: object,
    ) -> bytes:
        self.added.append((stream, fields, dict(kwargs)))
        return b"2-0"

    async def xack(self, stream: str, group: str, entry_id: str) -> int:
        self.acked.append((stream, group, entry_id))
        return 1


async def test_redis_acknowledgements_preserve_delivery_semantics() -> None:
    pubsub = _PubSubAcknowledgement()
    await pubsub.ack()
    await pubsub.nack(requeue=False)
    with pytest.raises(UnsupportedCapabilityError):
        await pubsub.requeue()
    with pytest.raises(UnsupportedCapabilityError):
        await pubsub.defer(0)
    with pytest.raises(UnsupportedCapabilityError):
        await pubsub.touch(1)

    client = _RedisStreamClient()
    streams = _StreamsAcknowledgement(
        client,
        stream="events",
        group="workers",
        entry_id="1-0",
        payload=b"payload",
        headers={"tenant": "acme"},
    )
    await streams.requeue()
    await streams.requeue()
    assert len(client.added) == 1
    assert client.acked == [("events", "workers", "1-0")]
    with pytest.raises(UnsupportedCapabilityError):
        await streams.touch(1)


class _RabbitInboundMessage:
    def __init__(self) -> None:
        self.body = b"payload"
        self.headers = {"tenant": "acme"}
        self.content_type = "application/json"
        self.content_encoding = "utf-8"
        self.correlation_id = "correlation"
        self.message_id = "message"
        self.delivery_mode = "persistent"
        self.priority = 4
        self.reply_to = "replies"
        self.expiration = 30.0
        self.timestamp = None
        self.type = "event"
        self.user_id = None
        self.app_id = "tests"
        self.calls: list[tuple[str, bool | None]] = []

    async def ack(self) -> None:
        self.calls.append(("ack", None))

    async def nack(self, *, requeue: bool) -> None:
        self.calls.append(("nack", requeue))

    async def reject(self, *, requeue: bool) -> None:
        self.calls.append(("reject", requeue))


class _RabbitOutboundMessage:
    def __init__(self, **values: object) -> None:
        self.values = values


class _RabbitExchange:
    def __init__(self, confirmation: object = None) -> None:
        self.confirmation = confirmation
        self.published: list[tuple[object, str, bool | None]] = []

    async def publish(
        self,
        message: object,
        *,
        routing_key: str,
        mandatory: bool | None = None,
    ) -> object:
        self.published.append((message, routing_key, mandatory))
        return self.confirmation


async def test_rabbitmq_acknowledgements_are_idempotent_and_reschedulable() -> None:
    message = _RabbitInboundMessage()
    exchange = _RabbitExchange()
    aio_pika = SimpleNamespace(Message=_RabbitOutboundMessage)
    acknowledgement = _RabbitAcknowledgement(message, exchange, "events", aio_pika)

    await acknowledgement.defer(0)
    await acknowledgement.ack()
    assert len(exchange.published) == 1
    replacement = exchange.published[0][0]
    assert isinstance(replacement, _RabbitOutboundMessage)
    assert replacement.values["expiration"] == 30.0
    assert replacement.values["reply_to"] == "replies"
    assert message.calls == [("ack", None)]
    with pytest.raises(UnsupportedCapabilityError):
        await acknowledgement.touch(1)

    nacked_message = _RabbitInboundMessage()
    nacked = _RabbitAcknowledgement(nacked_message, exchange, "events", aio_pika)
    await nacked.nack(requeue=False)
    await nacked.requeue()
    assert nacked_message.calls == [("nack", False)]

    unconfirmed_message = _RabbitInboundMessage()
    unconfirmed = _RabbitAcknowledgement(
        unconfirmed_message,
        _RabbitExchange(Nack()),
        "events",
        aio_pika,
    )
    with pytest.raises(PublishError, match="not acknowledged"):
        await unconfirmed.defer(0)
    assert unconfirmed_message.calls == []


class _RabbitQueue:
    def __init__(self) -> None:
        self.cancelled: list[str] = []
        self.callbacks: list[object] = []

    async def cancel(self, tag: str) -> None:
        self.cancelled.append(tag)

    async def consume(self, callback: object, *, no_ack: bool) -> str:
        assert not no_ack
        self.callbacks.append(callback)
        return "resumed-tag"


class _RabbitChannel:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


async def test_rabbitmq_consumer_can_resume_after_pause_and_close_idempotently() -> None:
    queue = _RabbitQueue()
    channel = _RabbitChannel()

    async def callback(message: object) -> None:
        del message

    consumer = _RabbitConsumer("consumer", channel, queue, "initial-tag", callback)
    await consumer.pause()
    await consumer.resume()
    await consumer.close()
    await consumer.close()

    assert queue.cancelled == ["initial-tag", "resumed-tag"]
    assert queue.callbacks == [callback]
    assert channel.close_calls == 1
    with pytest.raises(RuntimeError, match="closed"):
        await consumer.resume()


@dataclass(frozen=True, slots=True)
class _KafkaPartition:
    topic: str
    partition: int


@dataclass(frozen=True, slots=True)
class _KafkaRecord:
    topic: str
    partition: int
    offset: int


class _KafkaAcknowledgementConsumer:
    def __init__(self, assignments: tuple[_KafkaPartition, ...]) -> None:
        self.assignments = assignments
        self.commits: list[Mapping[object, int]] = []
        self.seeks: list[tuple[object, int]] = []

    def assignment(self) -> tuple[_KafkaPartition, ...]:
        return self.assignments

    async def commit(self, offsets: Mapping[object, int]) -> None:
        self.commits.append(dict(offsets))

    def seek(self, partition: object, offset: int) -> None:
        self.seeks.append((partition, offset))


async def test_kafka_acknowledgement_targets_only_the_delivered_partition() -> None:
    selected = _KafkaPartition("events", 1)
    other = _KafkaPartition("events", 2)
    consumer = _KafkaAcknowledgementConsumer((selected, other))
    record = _KafkaRecord("events", 1, 41)

    acknowledged = _KafkaAcknowledgement(consumer, record)
    await acknowledged.ack()
    await acknowledged.ack()
    assert consumer.commits == [{selected: 42}]

    requeued = _KafkaAcknowledgement(consumer, record)
    await requeued.requeue()
    await requeued.requeue()
    assert consumer.seeks == [(selected, 41)]
    with pytest.raises(UnsupportedCapabilityError):
        await requeued.touch(1)

    unassigned = _KafkaAcknowledgement(_KafkaAcknowledgementConsumer(()), record)
    with pytest.raises(RuntimeError, match="no longer assigned"):
        await unassigned.ack()


class _KafkaNativeConsumer:
    def __init__(self, *, fail_start: bool = False) -> None:
        self.partition = _KafkaPartition("events", 0)
        self.fail_start = fail_start
        self.start_calls = 0
        self.stop_calls = 0
        self.paused: list[tuple[_KafkaPartition, ...]] = []
        self.resumed: list[tuple[_KafkaPartition, ...]] = []
        self._blocked = asyncio.Event()

    async def start(self) -> None:
        self.start_calls += 1
        if self.fail_start:
            raise RuntimeError("consumer startup failed")

    async def stop(self) -> None:
        self.stop_calls += 1

    def assignment(self) -> tuple[_KafkaPartition, ...]:
        return (self.partition,)

    def pause(self, *partitions: _KafkaPartition) -> None:
        self.paused.append(partitions)

    def resume(self, *partitions: _KafkaPartition) -> None:
        self.resumed.append(partitions)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        await self._blocked.wait()
        raise StopAsyncIteration


async def _ignore_delivery(message: EngineIncomingMessage) -> None:
    del message


async def test_kafka_consumer_start_pause_resume_and_close_are_coordinated() -> None:
    native = _KafkaNativeConsumer()
    consumer = _KafkaConsumer("consumer", native, _ignore_delivery)
    await consumer.start()
    await consumer.pause()
    await consumer.resume()
    await consumer.close()
    await consumer.close()

    assert native.start_calls == 1
    assert native.paused == [(native.partition,)]
    assert native.resumed == [(native.partition,)]
    assert native.stop_calls == 1
    with pytest.raises(RuntimeError, match="closed"):
        await consumer.resume()


async def test_kafka_consumer_start_failure_is_cleaned_up_before_returning() -> None:
    native = _KafkaNativeConsumer(fail_start=True)
    consumer = _KafkaConsumer("consumer", native, _ignore_delivery)

    with pytest.raises(RuntimeError, match="startup failed"):
        await consumer.start()
    assert native.stop_calls == 1


class _FailingRedisClient:
    def __init__(self) -> None:
        self.closed = False

    async def ping(self) -> None:
        raise RuntimeError("redis ping failed")

    async def aclose(self) -> None:
        self.closed = True


async def test_redis_connect_failure_closes_partial_client() -> None:
    client = _FailingRedisClient()

    class RedisFactory:
        @classmethod
        def from_url(cls, url: str, *, decode_responses: bool) -> _FailingRedisClient:
            del cls, url, decode_responses
            return client

    redis_asyncio = ModuleType("redis.asyncio")
    redis_asyncio.__dict__["Redis"] = RedisFactory
    redis_package = ModuleType("redis")
    redis_package.__dict__["__path__"] = []
    redis_package.__dict__["asyncio"] = redis_asyncio
    engine = RedisEngine()

    with (
        patch.dict(
            sys.modules,
            {"redis": redis_package, "redis.asyncio": redis_asyncio},
        ),
        patch.object(RedisEngine, "is_available", return_value=Availability(True)),
        pytest.raises(RuntimeError, match="ping failed"),
    ):
        await engine.connect()

    assert client.closed
    assert engine._client is None


class _FailingRabbitConnection:
    def __init__(self) -> None:
        self.closed = False

    async def channel(self, *, publisher_confirms: bool) -> object:
        del publisher_confirms
        raise RuntimeError("channel creation failed")

    async def close(self) -> None:
        self.closed = True


async def test_rabbitmq_connect_failure_closes_partial_connection() -> None:
    connection = _FailingRabbitConnection()

    async def connect(url: str) -> _FailingRabbitConnection:
        del url
        return connection

    aio_pika = ModuleType("aio_pika")
    aio_pika.__dict__["connect"] = connect
    engine = RabbitMQEngine()

    with (
        patch.dict(sys.modules, {"aio_pika": aio_pika}),
        patch.object(RabbitMQEngine, "is_available", return_value=Availability(True)),
        pytest.raises(RuntimeError, match="channel creation failed"),
    ):
        await engine.connect()

    assert connection.closed
    assert engine._connection is None
    assert engine._channel is None
    assert engine._exchange is None


class _FailingKafkaProducer:
    def __init__(self) -> None:
        self.kwargs: Mapping[str, object] = {}
        self.stop_calls = 0

    async def start(self) -> None:
        raise RuntimeError("producer startup failed")

    async def stop(self) -> None:
        self.stop_calls += 1


async def test_kafka_connect_failure_closes_partial_producer() -> None:
    producer = _FailingKafkaProducer()

    def producer_factory(**kwargs: object) -> _FailingKafkaProducer:
        producer.kwargs = dict(kwargs)
        return producer

    aiokafka = ModuleType("aiokafka")
    aiokafka.__dict__["AIOKafkaProducer"] = producer_factory
    engine = KafkaEngine()

    with (
        patch.dict(sys.modules, {"aiokafka": aiokafka}),
        patch.object(KafkaEngine, "is_available", return_value=Availability(True)),
        pytest.raises(RuntimeError, match="producer startup failed"),
    ):
        await engine.connect()

    assert producer.stop_calls == 1
    assert producer.kwargs["bootstrap_servers"] == "localhost:9092"
    assert engine._producer is None


class _RedisPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, object], Mapping[str, object]]] = []

    async def xadd(
        self,
        destination: str,
        fields: Mapping[str, object],
        **kwargs: object,
    ) -> bytes:
        self.calls.append((destination, fields, dict(kwargs)))
        return b"7-0"


async def test_redis_stream_publish_preserves_context_and_capacity_limit() -> None:
    engine = RedisEngine({"mode": "streams", "max_length": 10})
    client = _RedisPublisher()
    engine._client = client

    result = await engine.publish(
        "events",
        b"payload",
        EnginePublishContext("message", headers={"tenant": "acme"}),
    )

    assert result.transport_id == "7-0"
    destination, fields, kwargs = client.calls[0]
    assert destination == "events"
    assert fields["message_id"] == "message"
    assert fields["headers"] == '{"tenant":"acme"}'
    assert kwargs == {"maxlen": 10, "approximate": True}


class Ack:
    pass


class Nack:
    pass


def test_rabbitmq_confirmation_frames_are_normalized_truthfully() -> None:
    assert _rabbit_confirmation_accepted(Ack(), publisher_confirms=True)
    assert not _rabbit_confirmation_accepted(Nack(), publisher_confirms=True)
    assert not _rabbit_confirmation_accepted(None, publisher_confirms=True)
    assert _rabbit_confirmation_accepted(None, publisher_confirms=False)


async def test_rabbitmq_publish_maps_ttl_content_type_and_confirmation() -> None:
    engine = RabbitMQEngine()
    exchange = _RabbitExchange(Ack())
    engine._exchange = exchange
    engine._aio_pika = SimpleNamespace(
        DeliveryMode=SimpleNamespace(PERSISTENT="persistent", NOT_PERSISTENT="transient"),
        Message=_RabbitOutboundMessage,
    )

    result = await engine.publish(
        "events.created",
        b"payload",
        EnginePublishContext(
            "message",
            headers={
                "correlation_id": "correlation",
                "pyev-content-type": "application/msgpack",
            },
            ttl=1.25,
        ),
    )

    assert result.accepted
    outbound, route, mandatory = exchange.published[0]
    assert isinstance(outbound, _RabbitOutboundMessage)
    assert outbound.values["expiration"] == 1.25
    assert outbound.values["content_type"] == "application/msgpack"
    assert route == "events.created"
    assert mandatory is False

    with pytest.raises(ValueError, match="ttl"):
        await engine.publish(
            "events.created",
            b"payload",
            EnginePublishContext("message", ttl=float("inf")),
        )


@dataclass(frozen=True, slots=True)
class _KafkaMetadata:
    topic: str
    partition: int
    offset: int


class _KafkaProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, bytes | None, list[tuple[str, bytes]]]] = []

    async def send_and_wait(
        self,
        destination: str,
        payload: bytes,
        *,
        key: bytes | None,
        headers: list[tuple[str, bytes]],
    ) -> _KafkaMetadata:
        self.calls.append((destination, payload, key, headers))
        return _KafkaMetadata(destination, 2, 9)


async def test_kafka_publish_uses_ordering_key_and_returns_partition_identity() -> None:
    engine = KafkaEngine()
    producer = _KafkaProducer()
    engine._producer = producer

    result = await engine.publish(
        "events",
        b"payload",
        EnginePublishContext(
            "message",
            headers={"tenant": "acme"},
            ordering_key="account-42",
        ),
    )

    assert result.transport_id == "events:2:9"
    assert producer.calls == [("events", b"payload", b"account-42", [("tenant", b"acme")])]
