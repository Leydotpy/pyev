"""Optional AMQP 0-9-1 transport powered by ``aio-pika``."""

from __future__ import annotations

import asyncio
import importlib.util
import math
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
from pymq.exceptions import PublishError, UnsupportedCapabilityError


class _RabbitAcknowledgement:
    def __init__(self, message: Any, exchange: Any, routing_key: str, aio_pika: Any) -> None:
        self.message = message
        self.exchange = exchange
        self.routing_key = routing_key
        self.aio_pika = aio_pika
        self._finished = False

    async def ack(self) -> None:
        if not self._finished:
            await self.message.ack()
            self._finished = True

    async def nack(self, requeue: bool = True) -> None:
        if not self._finished:
            await self.message.nack(requeue=requeue)
            self._finished = True

    async def reject(self) -> None:
        if not self._finished:
            await self.message.reject(requeue=False)
            self._finished = True

    async def requeue(self) -> None:
        await self.nack(requeue=True)

    async def defer(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("defer delay cannot be negative")
        if self._finished:
            return
        await asyncio.sleep(delay)
        replacement = self.aio_pika.Message(
            body=self.message.body,
            headers=self.message.headers,
            content_type=self.message.content_type,
            content_encoding=self.message.content_encoding,
            correlation_id=self.message.correlation_id,
            message_id=self.message.message_id,
            delivery_mode=self.message.delivery_mode,
            priority=self.message.priority,
            reply_to=self.message.reply_to,
            expiration=self.message.expiration,
            timestamp=self.message.timestamp,
            type=self.message.type,
            user_id=self.message.user_id,
            app_id=self.message.app_id,
        )
        confirmation = await self.exchange.publish(
            replacement,
            routing_key=self.routing_key,
        )
        if confirmation is not None and not _rabbit_confirmation_accepted(
            confirmation,
            publisher_confirms=True,
        ):
            raise PublishError("RabbitMQ deferred replacement was not acknowledged")
        await self.ack()

    async def touch(self, extension: float) -> None:
        del extension
        raise UnsupportedCapabilityError(
            Capability.VISIBILITY_TIMEOUT,
            operation="RabbitMQ touch",
        )


class _RabbitConsumer(EngineConsumer):
    def __init__(
        self,
        consumer_id: str,
        channel: Any,
        queue: Any,
        consumer_tag: str,
        callback: Any,
    ) -> None:
        self._id = consumer_id
        self.channel = channel
        self.queue = queue
        self.consumer_tag = consumer_tag
        self.callback = callback
        self._closed = False

    @property
    def id(self) -> str:
        return self._id

    @property
    def closed(self) -> bool:
        return self._closed

    async def pause(self) -> None:
        if self.consumer_tag:
            await self.queue.cancel(self.consumer_tag)
            self.consumer_tag = ""

    async def resume(self) -> None:
        if self._closed:
            raise RuntimeError("cannot resume a closed RabbitMQ consumer")
        if not self.consumer_tag:
            self.consumer_tag = await self.queue.consume(self.callback, no_ack=False)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.consumer_tag:
                await self.queue.cancel(self.consumer_tag)
                self.consumer_tag = ""
        finally:
            await self.channel.close()


def _amqp_pattern(pattern: str) -> str:
    if pattern == "*":
        return "#"
    return pattern


class RabbitMQEngine(BaseEngine):
    """RabbitMQ topic-exchange engine with publisher confirms and manual acks."""

    name = "rabbitmq"
    priority = 40

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(config)
        raw_url = self.config.get("url", "amqp://guest:guest@localhost/")
        raw_exchange = self.config.get("exchange", "pyev")
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise ValueError("RabbitMQ url must not be empty")
        if not isinstance(raw_exchange, str) or not raw_exchange.strip():
            raise ValueError("RabbitMQ exchange must not be empty")
        self.url = raw_url
        self.exchange_name = raw_exchange
        self._aio_pika: Any = None
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None
        self._consumers: dict[str, _RabbitConsumer] = {}

    @classmethod
    def is_available(cls, config: Mapping[str, object] | None = None) -> Availability:
        try:
            dependency = importlib.util.find_spec("aio_pika")
        except (ImportError, ModuleNotFoundError, ValueError):
            dependency = None
        if dependency is None:
            return Availability(False, "install pyev[rabbitmq] to use RabbitMQ")
        if config is not None and "url" in config and not config.get("url"):
            return Availability(False, "RabbitMQ engine configuration requires 'url'")
        return Availability(True)

    @property
    def capabilities(self) -> CapabilitySet:
        values: dict[Capability, Mapping[str, object] | None] = {
            Capability.PUBLISH_SUBSCRIBE: {"protocol": "amqp-0-9-1"},
            Capability.WILDCARD_SUBSCRIPTIONS: {"syntax": "amqp-topic"},
            Capability.COMPETING_CONSUMERS: None,
            Capability.FANOUT: None,
            Capability.MESSAGE_ORDERING: {"scope": "queue"},
            Capability.AT_LEAST_ONCE: None,
            Capability.REQUEST_REPLY: {"implementation": "portable"},
        }
        if bool(self.config.get("durable", True)):
            values[Capability.DURABLE_SUBSCRIPTIONS] = None
        if bool(self.config.get("publisher_confirms", True)):
            values[Capability.PUBLISHER_CONFIRMS] = None
        if self.config.get("dead_letter_exchange"):
            values[Capability.NATIVE_DEAD_LETTER] = None
        return CapabilitySet(values)

    async def connect(self) -> None:
        if self._connection is not None:
            return
        if not self.is_available(self.config):
            raise RuntimeError("aio-pika unavailable; install pyev[rabbitmq]")
        import aio_pika

        connection = await aio_pika.connect(self.url)
        channel: Any = None
        try:
            channel = await connection.channel(
                publisher_confirms=bool(self.config.get("publisher_confirms", True))
            )
            exchange = await channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=bool(self.config.get("durable", True)),
            )
        except BaseException:
            if channel is not None:
                with suppress(Exception):
                    await channel.close()
            with suppress(Exception):
                await connection.close()
            raise
        self._aio_pika = aio_pika
        self._connection = connection
        self._channel = channel
        self._exchange = exchange

    async def disconnect(self) -> None:
        consumers = tuple(self._consumers.values())
        self._consumers.clear()
        await asyncio.gather(
            *(consumer.close() for consumer in consumers),
            return_exceptions=True,
        )
        channel = self._channel
        connection = self._connection
        self._channel = None
        self._exchange = None
        self._connection = None
        if channel is not None and not channel.is_closed:
            await channel.close()
        if connection is not None and not connection.is_closed:
            await connection.close()

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        if self._exchange is None:
            raise RuntimeError("RabbitMQ engine is not connected")
        publisher_confirms = bool(self.config.get("publisher_confirms", True))
        delivery_mode = (
            self._aio_pika.DeliveryMode.PERSISTENT
            if bool(self.config.get("durable", True))
            else self._aio_pika.DeliveryMode.NOT_PERSISTENT
        )
        if context.ttl is not None and (not math.isfinite(context.ttl) or context.ttl <= 0):
            raise ValueError("RabbitMQ message ttl must be finite and positive")
        # aio-pika accepts expiration in seconds and encodes AMQP's millisecond
        # string internally. Passing the wire representation here would fail its
        # singledispatch encoder when message properties are materialized.
        expiration = context.ttl
        message = self._aio_pika.Message(
            body=payload,
            headers=dict(context.headers),
            message_id=context.message_id,
            correlation_id=context.headers.get("correlation_id"),
            delivery_mode=delivery_mode,
            expiration=expiration,
            content_type=context.headers.get("pyev-content-type", "application/octet-stream"),
        )
        confirmation = await self._exchange.publish(
            message,
            routing_key=destination,
            mandatory=bool(self.config.get("mandatory", False)),
        )
        return EnginePublishResult(
            accepted=_rabbit_confirmation_accepted(
                confirmation,
                publisher_confirms=publisher_confirms,
            ),
            transport_id=context.message_id,
        )

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if self._connection is None or self._exchange is None:
            raise RuntimeError("RabbitMQ engine is not connected")
        existing = self._consumers.get(subscription.id)
        if existing is not None and not existing.closed:
            raise ValueError(f"consumer {subscription.id!r} is already registered")
        self._consumers.pop(subscription.id, None)
        channel = await self._connection.channel()
        try:
            await channel.set_qos(prefetch_count=subscription.capacity)
            queue_name = str(self.config.get("queue", f"pyev.{subscription.id}"))
            arguments: dict[str, object] = {}
            if dead_letter_exchange := self.config.get("dead_letter_exchange"):
                arguments["x-dead-letter-exchange"] = str(dead_letter_exchange)
            queue = await channel.declare_queue(
                queue_name,
                durable=bool(self.config.get("durable", True)),
                auto_delete=bool(self.config.get("auto_delete", False)),
                arguments=arguments,
            )
            await queue.bind(self._exchange, routing_key=_amqp_pattern(subscription.pattern))
        except BaseException:
            with suppress(Exception):
                await channel.close()
            raise

        async def on_message(message: Any) -> None:
            routing_key = str(message.routing_key or subscription.destination)
            acknowledgement = _RabbitAcknowledgement(
                message,
                self._exchange,
                routing_key,
                self._aio_pika,
            )
            headers = {str(key): str(value) for key, value in (message.headers or {}).items()}
            await callback(
                EngineIncomingMessage(
                    destination=routing_key,
                    payload=message.body,
                    acknowledgement=acknowledgement,
                    headers=MappingProxyType(headers),
                    transport_metadata=MappingProxyType(
                        {
                            "exchange": self.exchange_name,
                            "queue": queue_name,
                            "delivery_tag": message.delivery_tag,
                            "redelivered": message.redelivered,
                        }
                    ),
                )
            )

        try:
            consumer_tag = await queue.consume(on_message, no_ack=False)
        except BaseException:
            with suppress(Exception):
                await channel.close()
            raise
        consumer = _RabbitConsumer(
            subscription.id,
            channel,
            queue,
            consumer_tag,
            on_message,
        )
        self._consumers[subscription.id] = consumer
        return consumer

    async def healthcheck(self) -> EngineHealth:
        connected = self._connection is not None and not self._connection.is_closed
        if not connected or self._channel is None:
            return EngineHealth(self.name, connected=False, healthy=False)
        started = time.perf_counter()
        healthy = not self._channel.is_closed
        return EngineHealth(
            self.name,
            connected=True,
            healthy=healthy,
            latency_ms=(time.perf_counter() - started) * 1000,
            details=MappingProxyType(
                {
                    "exchange": self.exchange_name,
                    "active_consumers": sum(
                        not consumer.closed for consumer in self._consumers.values()
                    ),
                }
            ),
        )


def _rabbit_confirmation_accepted(
    confirmation: object,
    *,
    publisher_confirms: bool,
) -> bool:
    """Normalize aio-pika's ``Ack | Nack | Reject | None`` result.

    ``pamqp`` frame classes remain an implementation detail of the optional
    dependency, so the adapter deliberately avoids importing them at module
    import time.  aio-pika returns ``None`` when confirms are disabled and an
    ``Ack`` frame on a successful confirmed publish.
    """

    if not publisher_confirms:
        return True
    return confirmation is True or type(confirmation).__name__ == "Ack"
