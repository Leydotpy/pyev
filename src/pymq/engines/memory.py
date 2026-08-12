"""Bounded asynchronous in-memory queue transport engine."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from enum import StrEnum
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


class OverflowPolicy(StrEnum):
    """Behaviour when a memory consumer queue reaches capacity."""

    BLOCK = "block"
    REJECT = "reject"
    DROP_NEWEST = "drop-newest"


class MemoryBackpressureError(RuntimeError):
    """Raised when bounded queue capacity rejects a publish."""


class _MemoryConsumer(EngineConsumer):
    def __init__(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
        *,
        drain_timeout: float,
    ) -> None:
        self.subscription = subscription
        self.callback = callback
        self.drain_timeout = drain_timeout
        self.queue: asyncio.Queue[InProcessItem | None] = asyncio.Queue(
            maxsize=subscription.capacity
        )
        self._closed = False
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._tasks: list[asyncio.Task[None]] = []
        self.last_error: str | None = None

    @property
    def id(self) -> str:
        return self.subscription.id

    @property
    def closed(self) -> bool:
        return self._closed

    def start(self) -> None:
        for index in range(self.subscription.concurrency):
            task = asyncio.create_task(
                self._worker(),
                name=f"pyev-memory-{self.id}-{index}",
            )
            self._tasks.append(task)

    async def enqueue(self, item: InProcessItem, policy: OverflowPolicy) -> bool:
        if self._closed:
            return False
        if policy is OverflowPolicy.BLOCK:
            await self.queue.put(item)
            return True
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            if policy is OverflowPolicy.DROP_NEWEST:
                return False
            raise MemoryBackpressureError(
                f"memory consumer {self.id!r} queue is full ({self.queue.maxsize})"
            ) from None
        return True

    async def pause(self) -> None:
        self._resumed.clear()

    async def resume(self) -> None:
        self._resumed.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._resumed.set()
        try:
            async with asyncio.timeout(self.drain_timeout):
                await self.queue.join()
        except TimeoutError:
            for task in self._tasks:
                task.cancel()
        else:
            for _task in self._tasks:
                await self.queue.put(None)
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _worker(self) -> None:
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                return
            try:
                await self._resumed.wait()
                acknowledgement = InProcessAcknowledgement()
                incoming = EngineIncomingMessage(
                    destination=item.destination,
                    payload=item.payload,
                    acknowledgement=acknowledgement,
                    headers=item.context.headers,
                    transport_metadata=MappingProxyType({"engine": "memory"}),
                    attempt=item.attempt,
                )
                await self.callback(incoming)
                if acknowledgement.action is AcknowledgementAction.DEFER:
                    await asyncio.sleep(acknowledgement.delay)
                if (
                    acknowledgement.action
                    in {
                        AcknowledgementAction.DEFER,
                        AcknowledgementAction.REQUEUE,
                    }
                    and not self._closed
                ):
                    await self.queue.put(
                        InProcessItem(
                            item.destination,
                            item.payload,
                            item.context,
                            item.attempt + 1,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:  # callback boundary; surfaced in health
                self.last_error = f"{type(error).__name__}: {error}"
            finally:
                self.queue.task_done()


class MemoryEngine(BaseEngine):
    """Use bounded queues and owned workers to model asynchronous delivery."""

    name = "memory"
    priority = -100

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        super().__init__(config)
        raw_policy = str(self.config.get("overflow_policy", OverflowPolicy.BLOCK.value))
        self.overflow_policy = OverflowPolicy(raw_policy)
        self.drain_timeout = _number(self.config.get("drain_timeout", 10.0), "drain_timeout")
        self._fail_publishes = _integer(
            self.config.get("fail_publish_count", 0), "fail_publish_count"
        )
        self._connected = False
        self._consumers: dict[str, _MemoryConsumer] = {}
        self._lock = asyncio.Lock()
        self._dropped = 0

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            {
                Capability.PUBLISH_SUBSCRIBE: {"process_scope": "local"},
                Capability.WILDCARD_SUBSCRIPTIONS: None,
                Capability.COMPETING_CONSUMERS: None,
                Capability.FANOUT: None,
                Capability.HEADERS_ROUTING: None,
                Capability.PARTITION_ORDERING: {"scope": "consumer-worker"},
                Capability.AT_LEAST_ONCE: None,
                Capability.REQUEST_REPLY: {"implementation": "portable"},
                Capability.QUEUE_DEPTH: {"bounded": True},
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
            raise RuntimeError("memory engine is not connected")
        if self._fail_publishes > 0:
            self._fail_publishes -= 1
            raise RuntimeError("injected memory-engine publish failure")
        item = InProcessItem(destination, payload, context)
        async with self._lock:
            consumers = tuple(self._consumers.values())
        accepted = True
        for consumer in consumers:
            if not consumer.closed and subscription_matches(
                consumer.subscription, destination, context
            ):
                enqueued = await consumer.enqueue(item, self.overflow_policy)
                accepted = accepted and enqueued
                if not enqueued:
                    self._dropped += 1
        return EnginePublishResult(accepted=accepted, transport_id=context.message_id)

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if not self._connected:
            raise RuntimeError("memory engine is not connected")
        consumer = _MemoryConsumer(
            subscription,
            callback,
            drain_timeout=self.drain_timeout,
        )
        async with self._lock:
            if subscription.id in self._consumers:
                raise ValueError(f"consumer {subscription.id!r} is already registered")
            self._consumers[subscription.id] = consumer
        consumer.start()
        return consumer

    async def remove_consumer(self, consumer_id: str) -> None:
        """Remove and close a memory consumer."""

        async with self._lock:
            consumer = self._consumers.pop(consumer_id, None)
        if consumer is not None:
            await consumer.close()

    async def healthcheck(self) -> EngineHealth:
        queue_depth = sum(consumer.queue.qsize() for consumer in self._consumers.values())
        errors = {
            consumer.id: consumer.last_error
            for consumer in self._consumers.values()
            if consumer.last_error is not None
        }
        return EngineHealth(
            engine=self.name,
            connected=self._connected,
            healthy=self._connected and not errors,
            details=MappingProxyType(
                {
                    "active_consumers": len(self._consumers),
                    "queue_depth": queue_depth,
                    "dropped": self._dropped,
                    "consumer_errors": errors,
                }
            ),
        )


def _number(value: object, name: str) -> float:
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    return float(value)


def _integer(value: object, name: str) -> int:
    if not isinstance(value, (int, str)) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return int(value)
