"""Deterministic same-process transport engine."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from types import MappingProxyType

from pymq.capabilities import Capability, CapabilitySet
from pymq.engines._inprocess import (
    AcknowledgementAction,
    InProcessAcknowledgement,
    InProcessItem,
    subscription_matches,
)
from pymq.engines.base import (
    BaseEngine,
    EngineConsumer,
    EngineDeliveryCallback,
    EngineHealth,
    EngineIncomingMessage,
    EnginePublishContext,
    EnginePublishResult,
    EngineSubscription,
)


class _LocalConsumer(EngineConsumer):
    def __init__(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> None:
        self.subscription = subscription
        self.callback = callback
        self._closed = False
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._semaphore = asyncio.Semaphore(subscription.concurrency)

    @property
    def id(self) -> str:
        return self.subscription.id

    @property
    def closed(self) -> bool:
        return self._closed

    async def pause(self) -> None:
        self._resumed.clear()

    async def resume(self) -> None:
        self._resumed.set()

    async def close(self) -> None:
        self._closed = True
        self._resumed.set()

    async def deliver(self, item: InProcessItem) -> None:
        async with self._semaphore:
            current = item
            while not self._closed:
                await self._resumed.wait()
                acknowledgement = InProcessAcknowledgement()
                incoming = EngineIncomingMessage(
                    destination=current.destination,
                    payload=current.payload,
                    acknowledgement=acknowledgement,
                    headers=current.context.headers,
                    transport_metadata=MappingProxyType({"engine": "local"}),
                    attempt=current.attempt,
                )
                await self.callback(incoming)
                if acknowledgement.action is AcknowledgementAction.DEFER:
                    await asyncio.sleep(acknowledgement.delay)
                elif acknowledgement.action is not AcknowledgementAction.REQUEUE:
                    return
                current = InProcessItem(
                    destination=current.destination,
                    payload=current.payload,
                    context=current.context,
                    attempt=current.attempt + 1,
                )


class LocalEngine(BaseEngine):
    """Dispatch envelopes directly to matching consumers in the current process."""

    name = "local"
    priority = 10

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(config)
        self._connected = False
        self._consumers: dict[str, _LocalConsumer] = {}
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            {
                Capability.PUBLISH_SUBSCRIBE: {"process_scope": "local"},
                Capability.WILDCARD_SUBSCRIPTIONS: None,
                Capability.COMPETING_CONSUMERS: None,
                Capability.FANOUT: None,
                Capability.HEADERS_ROUTING: None,
                Capability.MESSAGE_ORDERING: {"scope": "publisher-call"},
                Capability.AT_MOST_ONCE: None,
                Capability.AT_LEAST_ONCE: None,
                Capability.REQUEST_REPLY: {"implementation": "portable"},
            }
        )

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        async with self._lock:
            consumers = tuple(self._consumers.values())
            self._consumers.clear()
            self._connected = False
        for consumer in consumers:
            await consumer.close()

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        if not self._connected:
            raise RuntimeError("local engine is not connected")
        item = InProcessItem(destination, payload, context)
        async with self._lock:
            consumers = tuple(self._consumers.values())
        for consumer in consumers:
            if not consumer.closed and subscription_matches(
                consumer.subscription, destination, context
            ):
                await consumer.deliver(item)
        return EnginePublishResult(transport_id=context.message_id)

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if not self._connected:
            raise RuntimeError("local engine is not connected")
        consumer = _LocalConsumer(subscription, callback)
        async with self._lock:
            if subscription.id in self._consumers:
                raise ValueError(f"consumer {subscription.id!r} is already registered")
            self._consumers[subscription.id] = consumer
        return consumer

    async def remove_consumer(self, consumer_id: str) -> None:
        """Remove and close a consumer by identifier."""

        async with self._lock:
            consumer = self._consumers.pop(consumer_id, None)
        if consumer is not None:
            await consumer.close()

    async def healthcheck(self) -> EngineHealth:
        active = sum(not consumer.closed for consumer in self._consumers.values())
        return EngineHealth(
            engine=self.name,
            connected=self._connected,
            healthy=self._connected,
            details=MappingProxyType({"active_consumers": active}),
        )
