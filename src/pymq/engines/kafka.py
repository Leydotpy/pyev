"""Optional Apache Kafka transport powered by ``aiokafka``."""

from __future__ import annotations

import asyncio
import importlib.util
import re
import time
from collections.abc import Mapping
from contextlib import suppress
from types import MappingProxyType
from typing import Any

from pymq.capabilities import Capability, CapabilitySet
from pymq.engines.base import (
    Availability,
    BaseEngine,
    EngineConsumer,
    EngineDeliveryCallback,
    EngineHealth,
    EngineIncomingMessage,
    EnginePublishContext,
    EnginePublishResult,
    EngineSubscription,
)
from pymq.exceptions import UnsupportedCapabilityError


class _KafkaAcknowledgement:
    def __init__(self, consumer: Any, record: Any) -> None:
        self.consumer = consumer
        self.record = record
        self._finished = False

    async def ack(self) -> None:
        if not self._finished:
            partition = self._partition()
            await self.consumer.commit({partition: self.record.offset + 1})
            self._finished = True

    async def nack(self, requeue: bool = True) -> None:
        if requeue:
            await self.requeue()
        else:
            await self.reject()

    async def reject(self) -> None:
        await self.ack()

    async def requeue(self) -> None:
        if self._finished:
            return
        self.consumer.seek(self._partition(), self.record.offset)
        self._finished = True

    async def defer(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("defer delay cannot be negative")
        await asyncio.sleep(delay)
        await self.requeue()

    async def touch(self, extension: float) -> None:
        del extension
        raise UnsupportedCapabilityError(
            Capability.VISIBILITY_TIMEOUT,
            operation="Kafka touch",
        )

    def _partition(self) -> object:
        for partition in self.consumer.assignment():
            if (
                partition.topic == self.record.topic
                and partition.partition == self.record.partition
            ):
                return partition
        raise RuntimeError("Kafka record partition is no longer assigned to this consumer")


class _KafkaConsumer(EngineConsumer):
    def __init__(
        self,
        consumer_id: str,
        consumer: Any,
        callback: EngineDeliveryCallback,
    ) -> None:
        self._id = consumer_id
        self.consumer = consumer
        self.callback = callback
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._stopped = False
        self.last_error: str | None = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        """Start the native consumer before exposing the subscription handle."""

        if self._task is not None:
            return
        if self._closed:
            raise RuntimeError("cannot start a closed Kafka consumer")
        try:
            await self.consumer.start()
        except BaseException:
            with suppress(Exception):
                await self._stop()
            raise
        self._task = asyncio.create_task(
            self._run(),
            name=f"pyev-kafka-{self.id}",
        )

    async def pause(self) -> None:
        self._resumed.clear()
        partitions = tuple(self.consumer.assignment())
        if partitions:
            self.consumer.pause(*partitions)

    async def resume(self) -> None:
        if self._closed:
            raise RuntimeError("cannot resume a closed Kafka consumer")
        partitions = tuple(self.consumer.assignment())
        if partitions:
            self.consumer.resume(*partitions)
        self._resumed.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._resumed.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        await self._stop()

    async def _run(self) -> None:
        try:
            async for record in self.consumer:
                await self._resumed.wait()
                headers = {
                    key: (
                        value.decode(errors="replace")
                        if isinstance(value, bytes)
                        else ""
                        if value is None
                        else str(value)
                    )
                    for key, value in (record.headers or [])
                }
                value = record.value
                if value is None:
                    payload = b""
                elif isinstance(value, bytes):
                    payload = value
                else:
                    payload = bytes(value)
                acknowledgement = _KafkaAcknowledgement(self.consumer, record)
                await self.callback(
                    EngineIncomingMessage(
                        destination=record.topic,
                        payload=payload,
                        acknowledgement=acknowledgement,
                        headers=MappingProxyType(headers),
                        transport_metadata=MappingProxyType(
                            {
                                "topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "timestamp": record.timestamp,
                            }
                        ),
                    )
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.last_error = f"{type(error).__name__}: {error}"
            raise
        finally:
            with suppress(Exception):
                await self._stop()

    async def _stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        await self.consumer.stop()


def _topic_pattern(pattern: str) -> re.Pattern[str]:
    escaped = re.escape(pattern).replace(r"\*", ".*")
    return re.compile(f"^{escaped}$")


class KafkaEngine(BaseEngine):
    """Kafka topics/partitions engine with explicit offset acknowledgement semantics."""

    name = "kafka"
    priority = 30

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(config)
        servers = self.config.get("bootstrap_servers", "localhost:9092")
        if not servers:
            raise ValueError("Kafka bootstrap_servers must not be empty")
        self.bootstrap_servers = servers
        self._aiokafka: Any = None
        self._producer: Any = None
        self._consumers: dict[str, _KafkaConsumer] = {}

    @classmethod
    def is_available(cls, config: Mapping[str, object] | None = None) -> Availability:
        try:
            dependency = importlib.util.find_spec("aiokafka")
        except (ImportError, ModuleNotFoundError, ValueError):
            dependency = None
        if dependency is None:
            return Availability(False, "install pyev[kafka] to use Kafka")
        if (
            config is not None
            and "bootstrap_servers" in config
            and not config.get("bootstrap_servers")
        ):
            return Availability(False, "Kafka configuration requires 'bootstrap_servers'")
        return Availability(True)

    @property
    def capabilities(self) -> CapabilitySet:
        capabilities = {
            Capability.PUBLISH_SUBSCRIBE,
            Capability.WILDCARD_SUBSCRIPTIONS,
            Capability.DURABLE_SUBSCRIPTIONS,
            Capability.CONSUMER_GROUPS,
            Capability.COMPETING_CONSUMERS,
            Capability.PARTITION_ORDERING,
            Capability.AT_LEAST_ONCE,
            Capability.PUBLISHER_CONFIRMS,
            Capability.REQUEST_REPLY,
        }
        values: dict[Capability, Mapping[str, object] | None] = dict.fromkeys(capabilities)
        idempotent = bool(self.config.get("enable_idempotence", True))
        values[Capability.PARTITION_ORDERING] = {"scope": "partition"}
        values[Capability.AT_LEAST_ONCE] = {"acknowledgement": "offset-commit"}
        values[Capability.PUBLISHER_CONFIRMS] = {
            "acknowledgement": "all-in-sync-replicas" if idempotent else "leader",
            "idempotent": idempotent,
        }
        values[Capability.REQUEST_REPLY] = {"implementation": "portable"}
        return CapabilitySet(values)

    async def connect(self) -> None:
        if self._producer is not None:
            return
        if not self.is_available(self.config):
            raise RuntimeError("aiokafka unavailable; install pyev[kafka]")
        import aiokafka

        kwargs: dict[str, object] = {
            "bootstrap_servers": self.bootstrap_servers,
            "enable_idempotence": bool(self.config.get("enable_idempotence", True)),
        }
        if transactional_id := self.config.get("transactional_id"):
            kwargs["transactional_id"] = str(transactional_id)
        producer = aiokafka.AIOKafkaProducer(**kwargs)
        try:
            await producer.start()
        except BaseException:
            with suppress(Exception):
                await producer.stop()
            raise
        self._aiokafka = aiokafka
        self._producer = producer

    async def disconnect(self) -> None:
        consumers = tuple(self._consumers.values())
        self._consumers.clear()
        await asyncio.gather(
            *(consumer.close() for consumer in consumers),
            return_exceptions=True,
        )
        producer = self._producer
        self._producer = None
        if producer is not None:
            await producer.stop()

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        if self._producer is None:
            raise RuntimeError("Kafka engine is not connected")
        routing_key = context.partition_key or context.ordering_key
        key = routing_key.encode() if routing_key is not None else None
        headers = [(name, value.encode()) for name, value in context.headers.items()]
        metadata = await self._producer.send_and_wait(
            destination,
            payload,
            key=key,
            headers=headers,
        )
        transport_id = f"{metadata.topic}:{metadata.partition}:{metadata.offset}"
        return EnginePublishResult(transport_id=transport_id)

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if self._producer is None:
            raise RuntimeError("Kafka engine is not connected")
        existing = self._consumers.get(subscription.id)
        if existing is not None and not existing.closed:
            raise ValueError(f"consumer {subscription.id!r} is already registered")
        self._consumers.pop(subscription.id, None)
        group_id = str(self.config.get("group_id", f"pyev-{subscription.id}"))
        consumer = self._aiokafka.AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset=str(self.config.get("auto_offset_reset", "earliest")),
            max_poll_records=subscription.capacity,
        )
        if "*" in subscription.pattern:
            consumer.subscribe(pattern=_topic_pattern(subscription.pattern))
        else:
            consumer.subscribe(topics=[subscription.destination])
        handle = _KafkaConsumer(subscription.id, consumer, callback)
        await handle.start()
        self._consumers[subscription.id] = handle
        return handle

    async def healthcheck(self) -> EngineHealth:
        connected = self._producer is not None
        errors = {
            consumer.id: consumer.last_error
            for consumer in self._consumers.values()
            if consumer.last_error is not None
        }
        started = time.perf_counter()
        return EngineHealth(
            self.name,
            connected=connected,
            healthy=connected and not errors,
            latency_ms=(time.perf_counter() - started) * 1000 if connected else None,
            details=MappingProxyType(
                {
                    "active_consumers": sum(
                        not consumer.closed for consumer in self._consumers.values()
                    ),
                    "errors": errors,
                }
            ),
        )
