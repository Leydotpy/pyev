"""Transport-conforming fakes and message capture helpers."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from fnmatch import fnmatchcase
from types import MappingProxyType

from pymq.capabilities import Capability, CapabilitySet
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


class FailureInjector:
    """Queue explicit failures by operation name."""

    def __init__(self) -> None:
        self._failures: dict[str, deque[BaseException]] = defaultdict(deque)

    def fail_next(self, operation: str, error: BaseException, *, count: int = 1) -> None:
        """Make the next ``count`` calls of an operation raise ``error``."""

        if not operation:
            raise ValueError("operation must not be empty")
        if count < 1:
            raise ValueError("count must be at least 1")
        for _ in range(count):
            # Reusing an exception object is deliberate: tests can assert exact
            # identity and tracebacks are replaced each time it is raised.
            self._failures[operation].append(error)

    def maybe_raise(self, operation: str) -> None:
        """Raise and consume the next configured failure."""

        failures = self._failures.get(operation)
        if failures:
            error = failures.popleft()
            if not failures:
                self._failures.pop(operation, None)
            raise error

    def remaining(self, operation: str) -> int:
        """Return the number of pending injected failures."""

        return len(self._failures.get(operation, ()))

    def clear(self) -> None:
        """Remove every pending failure."""

        self._failures.clear()


@dataclass(frozen=True, slots=True)
class CapturedEnginePublish:
    """One publish observed by :class:`FakeEngine`."""

    destination: str
    payload: bytes
    context: EnginePublishContext
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


class FakeAcknowledgementAdapter:
    """Acknowledgement adapter recording calls without transport side effects."""

    def __init__(self) -> None:
        self.actions: list[tuple[str, object | None]] = []

    async def ack(self) -> None:
        self.actions.append(("ack", None))

    async def nack(self, requeue: bool = True) -> None:
        self.actions.append(("nack", requeue))

    async def reject(self) -> None:
        self.actions.append(("reject", None))

    async def requeue(self) -> None:
        self.actions.append(("requeue", None))

    async def defer(self, delay: float) -> None:
        self.actions.append(("defer", delay))

    async def touch(self, extension: float) -> None:
        self.actions.append(("touch", extension))


class FakeConsumer(EngineConsumer):
    """Controllable fake consumer returned by :class:`FakeEngine`."""

    def __init__(self, subscription: EngineSubscription, callback: EngineDeliveryCallback) -> None:
        self.subscription = subscription
        self.callback = callback
        self.paused = False
        self.closed = False

    @property
    def id(self) -> str:
        return self.subscription.id

    async def pause(self) -> None:
        self.paused = True

    async def resume(self) -> None:
        if self.closed:
            raise RuntimeError("consumer is closed")
        self.paused = False

    async def close(self) -> None:
        self.closed = True

    async def deliver(
        self,
        payload: bytes,
        *,
        destination: str | None = None,
        headers: Mapping[str, str] | None = None,
        attempt: int = 1,
        acknowledgement: FakeAcknowledgementAdapter | None = None,
    ) -> FakeAcknowledgementAdapter:
        """Synchronously invoke the registered delivery callback."""

        if self.closed:
            raise RuntimeError("consumer is closed")
        if self.paused:
            raise RuntimeError("consumer is paused")
        adapter = acknowledgement or FakeAcknowledgementAdapter()
        await self.callback(
            EngineIncomingMessage(
                destination=destination or self.subscription.destination,
                payload=payload,
                acknowledgement=adapter,
                headers=MappingProxyType(dict(headers or {})),
                transport_metadata=MappingProxyType({"engine": "fake"}),
                attempt=attempt,
            )
        )
        return adapter


class FakeEngine(BaseEngine):
    """Fully controllable engine implementing the core conformance surface."""

    name = "fake"
    priority = -1_000

    def __init__(
        self,
        config: Mapping[str, object] | None = None,
        *,
        capabilities: CapabilitySet | None = None,
        failures: FailureInjector | None = None,
    ) -> None:
        super().__init__(config)
        self._capabilities = capabilities or CapabilitySet.of(
            Capability.PUBLISH_SUBSCRIBE,
            Capability.WILDCARD_SUBSCRIPTIONS,
            Capability.AT_LEAST_ONCE,
            Capability.PUBLISHER_CONFIRMS,
        )
        self.failures = failures or FailureInjector()
        self.connected = False
        self.connect_count = 0
        self.disconnect_count = 0
        self.published: list[CapturedEnginePublish] = []
        self.consumers: dict[str, FakeConsumer] = {}
        self.health_override: EngineHealth | None = None

    @property
    def capabilities(self) -> CapabilitySet:
        return self._capabilities

    async def connect(self) -> None:
        self.failures.maybe_raise("connect")
        self.connect_count += 1
        self.connected = True

    async def disconnect(self) -> None:
        self.failures.maybe_raise("disconnect")
        self.disconnect_count += 1
        self.connected = False
        for consumer in tuple(self.consumers.values()):
            await consumer.close()

    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        if not self.connected:
            raise RuntimeError("fake engine is not connected")
        self.failures.maybe_raise("publish")
        self.published.append(CapturedEnginePublish(destination, payload, context))
        return EnginePublishResult(transport_id=context.message_id)

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        if not self.connected:
            raise RuntimeError("fake engine is not connected")
        self.failures.maybe_raise("create_consumer")
        if subscription.id in self.consumers:
            raise ValueError(f"consumer {subscription.id!r} already exists")
        consumer = FakeConsumer(subscription, callback)
        self.consumers[subscription.id] = consumer
        return consumer

    async def healthcheck(self) -> EngineHealth:
        self.failures.maybe_raise("healthcheck")
        return self.health_override or EngineHealth(
            engine=self.name,
            connected=self.connected,
            healthy=self.connected,
            details=MappingProxyType(
                {
                    "published": len(self.published),
                    "active_consumers": sum(not item.closed for item in self.consumers.values()),
                }
            ),
        )

    async def emit(
        self,
        destination: str,
        payload: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> int:
        """Deliver raw bytes to matching fake consumers and return their count."""

        delivered = 0
        for consumer in tuple(self.consumers.values()):
            # fnmatch is sufficient for a transport fake; routing semantics are
            # covered independently by the core router contract tests.
            if (
                fnmatchcase(destination, consumer.subscription.pattern)
                or destination == consumer.subscription.destination
            ):
                await consumer.deliver(payload, destination=destination, headers=headers)
                delivered += 1
        return delivered

    def clear(self) -> None:
        """Clear captured publishes and pending failure injection."""

        self.published.clear()
        self.failures.clear()


@dataclass(frozen=True, slots=True)
class CapturedPublish:
    """Application-level arguments captured by :class:`MockPublisher`."""

    message: object
    route: object | None
    headers: Mapping[str, str] | None
    options: object | None


class MockPublisher:
    """Protocol-compatible publisher that captures application messages."""

    def __init__(
        self, *, result_factory: Callable[[CapturedPublish], object] | None = None
    ) -> None:
        self.messages: list[CapturedPublish] = []
        self._result_factory = result_factory

    async def publish(
        self,
        message: object,
        *,
        route: object | None = None,
        headers: Mapping[str, str] | None = None,
        options: object | None = None,
    ) -> object:
        captured = CapturedPublish(message, route, headers, options)
        self.messages.append(captured)
        return self._result_factory(captured) if self._result_factory else captured

    def clear(self) -> None:
        self.messages.clear()


def assert_published(
    publisher: MockPublisher | FakeEngine,
    *,
    count: int | None = None,
    message_type: type[object] | None = None,
    route: object | None = None,
    destination: str | None = None,
) -> None:
    """Assert captured publication properties with useful failure messages."""

    if isinstance(publisher, MockPublisher):
        application_items = list(publisher.messages)
        if message_type is not None:
            application_items = [
                item for item in application_items if isinstance(item.message, message_type)
            ]
        if route is not None:
            application_items = [item for item in application_items if item.route == route]
        matches = len(application_items)
    else:
        engine_items = list(publisher.published)
        if destination is not None:
            engine_items = [item for item in engine_items if item.destination == destination]
        if message_type is not None or route is not None:
            raise TypeError("message_type and route apply only to MockPublisher")
        matches = len(engine_items)
    expected = 1 if count is None else count
    if matches != expected:
        raise AssertionError(f"expected {expected} matching publish(es), found {matches}")


__all__ = [
    "CapturedEnginePublish",
    "CapturedPublish",
    "FailureInjector",
    "FakeAcknowledgementAdapter",
    "FakeConsumer",
    "FakeEngine",
    "MockPublisher",
    "assert_published",
]
