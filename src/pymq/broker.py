"""Canonical application-facing broker orchestration facade."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, cast
from uuid import uuid4

from pymq.acknowledgements import AcknowledgementMode
from pymq.capabilities import Capability, CapabilitySet
from pymq.config import BrokerConfig
from pymq.connection import ConnectionManager
from pymq.deadletter import DeadLetterContext, DeadLetterManager, MemoryDeadLetterStore
from pymq.delivery import Delivery, DeliveryState, DeliveryTransition
from pymq.discovery import discover_engines
from pymq.engines.base import (
    BaseEngine,
    EngineConsumer,
    EngineIncomingMessage,
    EnginePublishContext,
    EngineSubscription,
)
from pymq.envelope import Envelope
from pymq.event import EventRegistry, default_event_registry
from pymq.events import InternalEventEmitter
from pymq.exceptions import (
    ConfigurationError,
    EngineError,
    LifecycleError,
    MessageValidationError,
    PublishError,
    RequestTimeoutError,
    RetryExhaustedError,
    RoutingError,
    SerializationError,
    UnknownEventError,
)
from pymq.message import MessageLike, is_message, message_to_payload
from pymq.middleware import InboundMiddlewarePipeline, OutboundMiddlewarePipeline
from pymq.observability.health import ComponentHealth, HealthReport
from pymq.observability.metrics import (
    ACK_TOTAL,
    CONSUME_TOTAL,
    HANDLER_DURATION_SECONDS,
    HANDLER_FAILURES_TOTAL,
    INFLIGHT,
    NACK_TOTAL,
    PUBLISH_FAILURES_TOTAL,
    PUBLISH_LATENCY_SECONDS,
    PUBLISH_TOTAL,
    MetricsProvider,
    NoOpMetrics,
)
from pymq.options import (
    BatchPublishOptions,
    DeliveryMode,
    PublishOptions,
    ReplyOptions,
    RequestOptions,
)
from pymq.registry import EngineRegistry, create_default_registry
from pymq.reliability import (
    CircuitBreaker,
    CircuitBreakerConfig,
    DecorrelatedJitterBackoff,
    EqualJitterBackoff,
    ExponentialBackoff,
    ExponentialFullJitterBackoff,
    FixedBackoff,
    LinearBackoff,
    RetryContext,
    RetryManager,
    RetryNotification,
    RetryPolicy,
)
from pymq.results import BatchItemError, BatchPublishResult, PublishResult
from pymq.routing import Destination, Route, Router
from pymq.serialization import (
    DeserializationContext,
    SerializationContext,
    Serializer,
    SerializerRegistry,
)
from pymq.subscription import (
    Subscription,
    SubscriptionController,
    SubscriptionOptions,
)

_WIRE_MAGIC = b"PYEV\x01"
_SERIALIZER_HEADER = "pyev-serializer"
_CONTENT_TYPE_HEADER = "pyev-content-type"
_CORRELATION_HEADER = "correlation_id"


class BrokerState(StrEnum):
    """Lifecycle states of the broker facade."""

    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(slots=True)
class OutboundContext:
    """Mutable outbound middleware context for one destination."""

    message: object
    envelope: Envelope
    route: Route
    destination: Destination
    engine: BaseEngine
    options: PublishOptions
    payload: bytes | None = None


@dataclass(slots=True)
class InboundContext:
    """Inbound middleware context passed around handler invocation."""

    delivery: Delivery[object]
    subscription: Subscription[object]
    engine_message: EngineIncomingMessage


@dataclass(slots=True)
class _SubscriptionRecord:
    subscription: Subscription[object]
    consumer: EngineConsumer
    registration_name: str
    engine_name: str
    destination: Destination
    restore_name: str


class Broker(SubscriptionController):
    """Typed asynchronous broker facade shared by all application code.

    Object construction is side-effect free. Connections, consumers, plugin
    discovery, and background tasks are acquired only by :meth:`startup` or an
    operation performed while the broker is already running.
    """

    def __init__(
        self,
        config: BrokerConfig | Mapping[str, object] | None = None,
        *,
        engine: BaseEngine | None = None,
        registry: EngineRegistry | None = None,
        event_registry: EventRegistry | None = None,
        serializer_registry: SerializerRegistry | None = None,
        router: Router | None = None,
        inbound: InboundMiddlewarePipeline[InboundContext, object] | None = None,
        outbound: OutboundMiddlewarePipeline[OutboundContext, object] | None = None,
        retry_manager: RetryManager | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        dead_letter_manager: DeadLetterManager | None = None,
        metrics: MetricsProvider | None = None,
        event_emitter: InternalEventEmitter | None = None,
    ) -> None:
        self.config = BrokerConfig.from_mapping(config)
        self.registry = registry if registry is not None else create_default_registry()
        self.event_registry = (
            event_registry if event_registry is not None else default_event_registry
        )
        self.serializers = (
            serializer_registry
            if serializer_registry is not None
            else SerializerRegistry.with_defaults()
        )
        self.router = router if router is not None else Router()
        self.inbound_middleware = inbound if inbound is not None else InboundMiddlewarePipeline()
        self.outbound_middleware = (
            outbound if outbound is not None else OutboundMiddlewarePipeline()
        )
        self.metrics = metrics if metrics is not None else NoOpMetrics()
        self.events = event_emitter if event_emitter is not None else InternalEventEmitter()

        self._injected_engine = engine
        self._engines: dict[str, BaseEngine] = {}
        self._connections: dict[str, ConnectionManager] = {}
        self._engine_lock = asyncio.Lock()
        self._default_engine_name: str | None = engine.name if engine is not None else None
        self._state = BrokerState.NEW
        self._lifecycle_lock = asyncio.Lock()
        self._subscriptions: dict[str, _SubscriptionRecord] = {}
        self._subscriptions_lock = asyncio.Lock()

        self._publish_policy = _retry_policy(
            self.config.reliability.get("publish_retry"),
            name="publish",
            default_attempts=3,
        )
        self._handler_policy = _retry_policy(
            self.config.reliability.get("handler_retry"),
            name="handler",
            default_attempts=3,
        )
        self._publish_retry = (
            retry_manager
            if retry_manager is not None
            else RetryManager(
                self._publish_policy,
                on_retry=self._on_retry,
                event_emitter=self.events,
            )
        )
        self._handler_retry = (
            retry_manager
            if retry_manager is not None
            else RetryManager(
                self._handler_policy,
                on_retry=self._on_retry,
                event_emitter=self.events,
            )
        )
        self._injected_breaker = circuit_breaker
        self._breakers: dict[str, CircuitBreaker] = {}
        self._breaker_config = _circuit_config(self.config.reliability.get("circuit_breaker"))
        self.dead_letters = (
            dead_letter_manager
            if dead_letter_manager is not None
            else DeadLetterManager(
                MemoryDeadLetterStore(),
                metrics=self.metrics,
                event_emitter=self.events,
            )
        )

        self._instance_id = uuid4().hex
        self._reply_route = f"_pyev.reply.{self._instance_id}"
        self._reply_subscription: Subscription[object] | None = None
        self._reply_lock = asyncio.Lock()
        self._pending_replies: dict[str, asyncio.Future[object]] = {}
        self._max_pending_replies = _as_int(
            self.config.extra.get("rpc_max_pending", 1024),
            name="rpc_max_pending",
        )
        if self._max_pending_replies < 1:
            raise ConfigurationError("rpc_max_pending must be a positive integer")

        self._publish_failures = 0
        self._retry_count = 0
        self._dead_letter_count = 0
        self._inflight = 0

    @classmethod
    def from_config(
        cls,
        config: BrokerConfig | Mapping[str, object] | None = None,
        **dependencies: Any,
    ) -> Broker:
        """Construct a broker from validated settings without starting it."""

        return cls(config=config, **dependencies)

    @property
    def state(self) -> BrokerState:
        """Return the current broker lifecycle state."""

        return self._state

    @property
    def ready(self) -> bool:
        """Return whether new operations are accepted."""

        return self._state is BrokerState.RUNNING

    @property
    def engine(self) -> BaseEngine | None:
        """Return the selected default engine after startup."""

        if self._default_engine_name is None:
            return None
        return self._engines.get(self._default_engine_name)

    @property
    def capabilities(self) -> CapabilitySet:
        """Return truthful capabilities of the selected default engine."""

        if self.engine is not None:
            return self.engine.capabilities
        if self._injected_engine is not None:
            return self._injected_engine.capabilities
        engine_type = self.registry.select(self.config)
        selected = engine_type(self.config.engine_settings(engine_type.name))
        return selected.capabilities

    async def startup(self) -> None:
        """Discover plugins, connect the default engine, and become ready."""

        async with self._lifecycle_lock:
            if self._state is BrokerState.RUNNING:
                return
            if self._state in {BrokerState.STARTING, BrokerState.DRAINING}:
                raise LifecycleError(
                    f"broker cannot start while {self._state.value}",
                    context={"state": self._state.value},
                )
            self._state = BrokerState.STARTING
            await self.events.emit("startup_started")
            try:
                discover_engines(self.registry, ignore_registered=True)
                if self._injected_engine is not None:
                    selected = self._injected_engine
                    self._default_engine_name = selected.name
                    await self._adopt_engine(selected)
                else:
                    engine_type = self.registry.select(self.config)
                    self._default_engine_name = engine_type.name
                    await self._connect_engine(engine_type.name)
                assert self.engine is not None
                self.engine.capabilities.require(
                    Capability.PUBLISH_SUBSCRIBE,
                    operation="broker startup",
                )
            except Exception as error:
                self._state = BrokerState.FAILED
                await self._disconnect_all()
                await self.events.emit(
                    "connection_failed",
                    error_type=type(error).__name__,
                )
                raise LifecycleError(
                    "broker startup failed",
                    retryable=isinstance(error, EngineError) and error.retryable,
                    context={"error_type": type(error).__name__},
                ) from error
            self._state = BrokerState.RUNNING
            self.metrics.set_gauge("pyev_connections", 1, labels={"engine": self.engine.name})
            await self.events.emit("engine_selected", engine=self.engine.name)
            await self.events.emit("startup_completed", engine=self.engine.name)

    async def shutdown(self) -> None:
        """Drain subscriptions and disconnect every owned engine idempotently."""

        async with self._lifecycle_lock:
            if self._state in {BrokerState.NEW, BrokerState.STOPPED}:
                self._state = BrokerState.STOPPED
                return
            if self._state is BrokerState.DRAINING:
                return
            self._state = BrokerState.DRAINING
            await self.events.emit("shutdown_started")
            subscriptions = tuple(record.subscription for record in self._subscriptions.values())
            for subscription in subscriptions:
                try:
                    await subscription.close()
                except Exception:
                    # Continue deterministic cleanup; component health records failures.
                    continue
            self._reply_subscription = None
            failure = LifecycleError("broker shut down while request was pending")
            for future in tuple(self._pending_replies.values()):
                if not future.done():
                    future.set_exception(failure)
            self._pending_replies.clear()
            await self._disconnect_all()
            self._breakers.clear()
            self._state = BrokerState.STOPPED
            await self.events.emit("shutdown_completed")

    async def publish(
        self,
        message: MessageLike,
        *,
        route: str | Route | None = None,
        headers: Mapping[str, str] | None = None,
        options: PublishOptions | None = None,
    ) -> PublishResult:
        """Normalize and publish one typed message or low-level mapping."""

        self._require_running("publish")
        selected_options = options or PublishOptions()
        logical_route = _route_for_message(message, route)
        envelope = self._make_envelope(
            message,
            route=logical_route,
            headers=headers,
            options=selected_options,
        )
        return await self._publish_envelope(
            message,
            envelope,
            route=logical_route,
            options=selected_options,
        )

    async def publish_batch(
        self,
        messages: Sequence[MessageLike],
        *,
        options: BatchPublishOptions | None = None,
    ) -> BatchPublishResult:
        """Publish a sequence with bounded concurrency and indexed failures."""

        self._require_running("publish_batch")
        selected = options or BatchPublishOptions()
        semaphore = asyncio.Semaphore(selected.concurrency)
        successes: list[tuple[int, PublishResult]] = []
        failures: list[BatchItemError] = []
        stop = asyncio.Event()

        async def publish_one(index: int, item: MessageLike) -> None:
            if selected.stop_on_error and stop.is_set():
                return
            async with semaphore:
                try:
                    result = await self.publish(item, options=selected.publish)
                except Exception as error:
                    failures.append(BatchItemError(index=index, error=error))
                    stop.set()
                else:
                    successes.append((index, result))

        async with asyncio.TaskGroup() as group:
            for index, item in enumerate(messages):
                group.create_task(publish_one(index, item), name=f"pyev-batch-{index}")
        successes.sort(key=lambda item: item[0])
        failures.sort(key=lambda item: item.index)
        return BatchPublishResult(
            results=tuple(result for _index, result in successes),
            errors=tuple(failures),
        )

    async def subscribe(
        self,
        route: str | Route | type[object],
        handler: Callable[[Delivery[Any]], Awaitable[None]],
        *,
        options: SubscriptionOptions | None = None,
    ) -> Subscription[Any]:
        """Create a consumer and register one asynchronous application handler."""

        self._require_running("subscribe")
        selected_options = options or SubscriptionOptions()
        pattern_name = _normalize_subscription_pattern(route)
        route_value: str | Route = route if isinstance(route, (str, Route)) else pattern_name
        subscription: Subscription[Any] = Subscription(
            route_value,
            handler,
            options=selected_options,
            controller=self,
        )
        registration = self.router.register(
            route,
            cast(Callable[[Any], Awaitable[Any]], handler),
            name=f"subscription:{subscription.id}",
        )
        destinations = self.router.destinations(pattern_name)
        destination = (
            destinations[0]
            if destinations
            else Destination(_physical_subscription_destination(pattern_name))
        )
        selected_engine = await self._get_engine(destination.engine)
        _validate_subscription_options(selected_engine, selected_options)
        engine_subscription = EngineSubscription(
            id=subscription.id,
            pattern=pattern_name,
            destination=destination.name,
            concurrency=selected_options.concurrency,
            capacity=selected_options.capacity,
            headers=_subscription_headers(selected_options),
        )

        async def callback(incoming: EngineIncomingMessage) -> None:
            await self._consume(
                incoming,
                subscription=cast(Subscription[object], subscription),
                destination=destination,
                engine=selected_engine,
            )

        try:
            connection = self._connections[selected_engine.name]

            async def create(engine: BaseEngine) -> EngineConsumer:
                return await engine.create_consumer(engine_subscription, callback)

            consumer = await connection.run(create)
        except Exception:
            self.router.unregister(registration)
            raise
        record = _SubscriptionRecord(
            subscription=cast(Subscription[object], subscription),
            consumer=consumer,
            registration_name=registration.name,
            engine_name=selected_engine.name,
            destination=destination,
            restore_name=f"subscription:{subscription.id}",
        )
        async with self._subscriptions_lock:
            self._subscriptions[subscription.id] = record

        async def restore_consumer() -> None:
            replacement = await selected_engine.create_consumer(
                engine_subscription,
                callback,
            )
            record.consumer = replacement

        connection.register_restore_callback(
            restore_consumer,
            name=record.restore_name,
        )
        await subscription.activate()
        return subscription

    async def unsubscribe(self, subscription: Subscription[Any] | str) -> None:
        """Close and unregister a subscription idempotently."""

        identifier = subscription.id if isinstance(subscription, Subscription) else subscription
        record = self._subscriptions.get(identifier)
        if record is None:
            return
        await record.subscription.close()

    async def pause(self, subscription: Subscription[Any]) -> None:
        """Pause the underlying engine consumer for ``subscription``."""

        record = self._subscriptions.get(subscription.id)
        if record is None:
            raise LifecycleError(
                "unknown subscription",
                context={"subscription_id": subscription.id},
            )
        await record.consumer.pause()

    async def resume(self, subscription: Subscription[Any]) -> None:
        """Resume the underlying engine consumer for ``subscription``."""

        record = self._subscriptions.get(subscription.id)
        if record is None:
            raise LifecycleError(
                "unknown subscription",
                context={"subscription_id": subscription.id},
            )
        await record.consumer.resume()

    async def close(self, subscription: Subscription[Any]) -> None:
        """Subscription-controller callback that releases consumer resources."""

        async with self._subscriptions_lock:
            record = self._subscriptions.pop(subscription.id, None)
        if record is None:
            return
        connection = self._connections.get(record.engine_name)
        if connection is not None:
            connection.unregister_restore_callback(record.restore_name)
        await record.consumer.close()
        try:
            self.router.unregister(record.registration_name)
        except RoutingError:
            pass

    async def request(
        self,
        message: MessageLike,
        *,
        timeout: float | None = None,
        options: RequestOptions | None = None,
    ) -> object:
        """Publish a request and wait on the broker's shared reply dispatcher."""

        self._require_running("request")
        await self._ensure_reply_subscription()
        if len(self._pending_replies) >= self._max_pending_replies:
            raise LifecycleError("the bounded pending-request table is full")
        correlation_id = str(uuid4())
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self._pending_replies[correlation_id] = future
        selected = options or RequestOptions()
        logical_route = _route_for_message(message, selected.route)
        envelope = self._make_envelope(
            message,
            route=logical_route,
            headers=None,
            options=selected.publish,
            correlation_id=correlation_id,
            reply_to=self._reply_route,
        )
        try:
            await self._publish_envelope(
                message,
                envelope,
                route=logical_route,
                options=selected.publish,
            )
            request_timeout = timeout
            if request_timeout is None:
                request_timeout = _as_float(
                    self.config.extra.get("rpc_timeout", 30.0),
                    name="rpc_timeout",
                )
            async with asyncio.timeout(request_timeout):
                return await future
        except TimeoutError as error:
            raise RequestTimeoutError(
                f"request timed out after {request_timeout:g} seconds",
                context={"timeout": request_timeout},
            ) from error
        finally:
            self._pending_replies.pop(correlation_id, None)
            if not future.done():
                future.cancel()

    async def reply(
        self,
        request: Delivery[Any],
        response: MessageLike,
        *,
        options: ReplyOptions | None = None,
    ) -> PublishResult:
        """Publish ``response`` to a request's validated reply destination."""

        self._require_running("reply")
        reply_to = request.envelope.reply_to
        if reply_to is None:
            raise RoutingError("request envelope has no reply destination")
        selected = options or ReplyOptions()
        route = Route(reply_to)
        correlation_id = request.envelope.correlation_id or request.envelope.id
        envelope = self._make_envelope(
            response,
            route=route,
            headers=None,
            options=selected.publish,
            correlation_id=correlation_id,
            causation_id=request.envelope.id,
        )
        return await self._publish_envelope(
            response,
            envelope,
            route=route,
            options=selected.publish,
        )

    async def health(self) -> HealthReport:
        """Return a redacted aggregate health snapshot."""

        components: list[ComponentHealth] = []
        selected_engine = self.engine
        queue_depth: int | None = None
        if selected_engine is not None:
            try:
                connection = self._connections[selected_engine.name]
                connection_health = await connection.health()
            except Exception as error:
                components.append(
                    ComponentHealth.unhealthy(
                        f"engine:{selected_engine.name}",
                        f"healthcheck failed: {type(error).__name__}",
                    )
                )
            else:
                components.append(connection_health)
                raw_depth = connection_health.details.get("queue_depth")
                if isinstance(raw_depth, int):
                    queue_depth = raw_depth
        elif self._state is BrokerState.RUNNING:
            components.append(ComponentHealth.unhealthy("engine", "no engine selected"))

        breaker_states = {name: breaker.state.value for name, breaker in self._breakers.items()}
        return HealthReport.from_components(
            components,
            lifecycle_state=self._state.value,
            selected_engine=self._default_engine_name,
            connection_state=("connected" if selected_engine is not None else "disconnected"),
            active_consumers=len(self._subscriptions),
            queue_depth=queue_depth,
            publish_failures=self._publish_failures,
            retry_count=self._retry_count,
            dead_letter_count=self._dead_letter_count,
            circuit_breakers=breaker_states,
            details={"inflight": self._inflight},
        )

    async def _publish_envelope(
        self,
        message: object,
        envelope: Envelope,
        *,
        route: Route,
        options: PublishOptions,
    ) -> PublishResult:
        destinations = self.router.destinations(
            route,
            message=message,
            headers=envelope.headers,
            version=envelope.version,
        )
        if not destinations:
            destinations = (Destination(route.name, engine=options.engine),)

        results: list[PublishResult] = []
        for destination in destinations:
            engine_name = options.engine or destination.engine
            selected_engine = await self._get_engine(engine_name)
            _validate_publish_options(selected_engine, options)
            context = OutboundContext(
                message=message,
                envelope=envelope,
                route=route,
                destination=destination,
                engine=selected_engine,
                options=options,
            )

            async def terminal(current: OutboundContext) -> object:
                return await self._send(current)

            value = await self.outbound_middleware.run(
                context,
                terminal,
                route=route,
                headers=envelope.headers,
            )
            if not isinstance(value, PublishResult):
                raise PublishError("outbound middleware returned an invalid publish result")
            results.append(value)

        if len(results) == 1:
            return results[0]
        return PublishResult(
            message_id=envelope.id,
            route=route.name,
            destination=",".join(result.destination for result in results),
            engine=",".join(dict.fromkeys(result.engine for result in results)),
            published_at=max(result.published_at for result in results),
            accepted=all(result.accepted for result in results),
            transport_id=",".join(
                result.transport_id for result in results if result.transport_id is not None
            )
            or None,
        )

    async def _send(self, context: OutboundContext) -> PublishResult:
        serializer = self._serializer(context.options.serializer)
        envelope = context.envelope
        if (
            envelope.serializer != serializer.name
            or envelope.content_type != serializer.content_type
        ):
            envelope = Envelope.from_dict(
                {
                    **envelope.to_dict(),
                    "serializer": serializer.name,
                    "content_type": serializer.content_type,
                }
            )
            context.envelope = envelope
        payload = _encode_wire(envelope, serializer)
        context.payload = payload
        transport_headers = {
            **dict(envelope.headers),
            _SERIALIZER_HEADER: serializer.name,
            _CONTENT_TYPE_HEADER: serializer.content_type,
        }
        if envelope.correlation_id is not None:
            transport_headers[_CORRELATION_HEADER] = envelope.correlation_id
        engine_context = EnginePublishContext(
            message_id=envelope.id,
            headers=MappingProxyType(transport_headers),
            partition_key=context.options.partition_key or envelope.partition_key,
            ordering_key=context.options.ordering_key or envelope.ordering_key,
            timeout=context.options.timeout,
            ttl=context.options.ttl,
        )
        started = time.perf_counter()
        breaker = self._breaker(context.engine.name)
        connection = self._connections[context.engine.name]

        async def attempt() -> Any:
            async def publish_once() -> Any:
                async def through_connection(engine: BaseEngine) -> Any:
                    return await engine.publish(
                        context.destination.name,
                        payload,
                        engine_context,
                    )

                return await connection.run(through_connection)

            return await breaker.call(publish_once)

        try:
            engine_result = await self._publish_retry.run(
                attempt,
                self._publish_policy,
                context=RetryContext(
                    operation="publish",
                    metadata={"engine": context.engine.name},
                ),
            )
        except Exception as error:
            self._publish_failures += 1
            self.metrics.increment(
                PUBLISH_FAILURES_TOTAL,
                labels={"engine": context.engine.name},
            )
            await self.events.emit(
                "publish_failed",
                engine=context.engine.name,
                error_type=type(error).__name__,
            )
            if isinstance(error, (EngineError, RetryExhaustedError)):
                raise
            raise PublishError(
                "engine publish failed",
                retryable=True,
                context={"engine": context.engine.name},
            ) from error
        elapsed = time.perf_counter() - started
        self.metrics.increment(PUBLISH_TOTAL, labels={"engine": context.engine.name})
        self.metrics.observe(
            PUBLISH_LATENCY_SECONDS,
            elapsed,
            labels={"engine": context.engine.name},
        )
        await self.events.emit(
            "published",
            engine=context.engine.name,
            event_type=envelope.type,
        )
        return PublishResult(
            message_id=envelope.id,
            route=context.route.name,
            destination=context.destination.name,
            engine=context.engine.name,
            published_at=engine_result.published_at,
            accepted=engine_result.accepted,
            transport_id=engine_result.transport_id,
        )

    async def _consume(
        self,
        incoming: EngineIncomingMessage,
        *,
        subscription: Subscription[object],
        destination: Destination,
        engine: BaseEngine,
    ) -> None:
        self._inflight += 1
        self.metrics.set_gauge(INFLIGHT, self._inflight, labels={"engine": engine.name})
        self.metrics.increment(CONSUME_TOTAL, labels={"engine": engine.name})
        try:
            envelope = self._decode_wire(incoming.payload)
            try:
                message = envelope.to_message(registry=self.event_registry)
            except UnknownEventError:
                message = dict(envelope.payload)
            delivery = Delivery(
                message,
                envelope,
                route=incoming.destination,
                subscription_id=subscription.id,
                attempt=incoming.attempt,
                delivered_at=incoming.received_at,
                consumer_id=subscription.options.consumer_id,
                acknowledgement=incoming.acknowledgement,
                mode=subscription.options.acknowledgement_mode,
                transport_metadata=incoming.transport_metadata,
                on_transition=self._on_delivery_transition,
            )
            if envelope.is_expired():
                await incoming.acknowledgement.reject()
                await delivery.mark_expired(reason="ttl elapsed")
                return
            if subscription.options.acknowledgement_mode is AcknowledgementMode.NONE:
                await incoming.acknowledgement.ack()
            await delivery.start_processing()
            inbound = InboundContext(delivery, subscription, incoming)

            async def invoke_handler() -> object:
                async def terminal(current: InboundContext) -> object:
                    started = time.perf_counter()
                    await self.events.emit(
                        "handler_started",
                        event_type=current.delivery.envelope.type,
                    )
                    try:
                        await subscription.handler(current.delivery)
                        return None
                    finally:
                        self.metrics.observe(
                            HANDLER_DURATION_SECONDS,
                            time.perf_counter() - started,
                            labels={"engine": engine.name},
                        )

                return await self.inbound_middleware.run(
                    inbound,
                    terminal,
                    route=incoming.destination,
                    headers=envelope.headers,
                )

            try:
                await self._handler_retry.run(
                    invoke_handler,
                    self._handler_policy,
                    context=RetryContext(
                        operation="handler",
                        metadata={"engine": engine.name},
                    ),
                )
            except Exception as error:
                if delivery.state in {
                    DeliveryState.ACKNOWLEDGED,
                    DeliveryState.DEFERRED,
                    DeliveryState.REQUEUED,
                    DeliveryState.RETRY_SCHEDULED,
                }:
                    return
                self.metrics.increment(
                    HANDLER_FAILURES_TOTAL,
                    labels={"engine": engine.name},
                )
                await self.events.emit(
                    "handler_failed",
                    event_type=envelope.type,
                    error_type=type(error).__name__,
                )
                await self.dead_letters.dead_letter(
                    envelope,
                    error,
                    context=DeadLetterContext(
                        route=incoming.destination,
                        destination=destination.name,
                        headers=envelope.headers,
                        metadata=incoming.transport_metadata,
                        consumer=subscription.options.consumer_id,
                        subscription=subscription.id,
                        engine=engine.name,
                        decoded_payload=message,
                        original_envelope_bytes=incoming.payload,
                    ),
                )
                self._dead_letter_count += 1
                try:
                    await incoming.acknowledgement.reject()
                finally:
                    await delivery.mark_dead_lettered(reason=type(error).__name__)
                return

            await self.events.emit("handler_completed", event_type=envelope.type)
            if (
                subscription.options.acknowledgement_mode
                in {AcknowledgementMode.AUTO, AcknowledgementMode.BATCH}
                and delivery.state is DeliveryState.PROCESSING
            ):
                await delivery.ack()
        except (SerializationError, MessageValidationError) as error:
            await self.dead_letters.dead_letter(
                incoming.payload,
                error,
                context=DeadLetterContext(
                    route=incoming.destination,
                    destination=destination.name,
                    metadata=incoming.transport_metadata,
                    subscription=subscription.id,
                    engine=engine.name,
                    original_envelope_bytes=incoming.payload,
                    failure_classification="invalid-envelope",
                ),
            )
            self._dead_letter_count += 1
            await incoming.acknowledgement.reject()
        finally:
            self._inflight -= 1
            self.metrics.set_gauge(INFLIGHT, self._inflight, labels={"engine": engine.name})

    def _make_envelope(
        self,
        message: object,
        *,
        route: Route,
        headers: Mapping[str, str] | None,
        options: PublishOptions,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        reply_to: str | None = None,
    ) -> Envelope:
        serializer = self._serializer(options.serializer)
        if is_message(message):
            return Envelope.from_message(
                message,
                source=self.config.source,
                correlation_id=correlation_id,
                causation_id=causation_id,
                content_type=serializer.content_type,
                serializer=serializer.name,
                headers=headers,
                partition_key=options.partition_key,
                ordering_key=options.ordering_key,
                ttl=options.ttl,
                reply_to=reply_to,
            )
        # message_to_payload supports mappings, attrs, and Pydantic-compatible
        # models; an explicit route gives undeclared models a stable wire type.
        payload = message_to_payload(message)
        return Envelope.create(
            payload,
            type=route.name,
            version=1,
            source=self.config.source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            content_type=serializer.content_type,
            serializer=serializer.name,
            headers=headers,
            partition_key=options.partition_key,
            ordering_key=options.ordering_key,
            ttl=options.ttl,
            reply_to=reply_to,
        )

    def _serializer(self, requested: str | None) -> Serializer:
        name = requested or self.config.serialization.default
        if name not in self.config.serialization.allowed:
            raise SerializationError(
                f"serializer {name!r} is not in the configured allowlist",
                context={"serializer": name},
            )
        return self.serializers.get(name)

    def _decode_wire(self, payload: bytes) -> Envelope:
        max_size = _as_int(
            self.config.serialization.options.get("max_envelope_bytes", 16 * 1024 * 1024),
            name="max_envelope_bytes",
        )
        if len(payload) > max_size:
            raise SerializationError(
                "incoming envelope exceeds configured byte limit",
                context={"size": len(payload), "max_size": max_size},
            )
        if not payload.startswith(_WIRE_MAGIC):
            return Envelope.from_bytes(payload, max_size=max_size)
        offset = len(_WIRE_MAGIC)
        if len(payload) <= offset:
            raise SerializationError("truncated pyev wire frame")
        name_size = payload[offset]
        start = offset + 1
        end = start + name_size
        if name_size == 0 or end > len(payload):
            raise SerializationError("invalid serializer name in pyev wire frame")
        try:
            name = payload[start:end].decode("ascii")
        except UnicodeDecodeError as error:
            raise SerializationError("wire serializer name must be ASCII") from error
        if name not in self.config.serialization.allowed:
            raise SerializationError(
                f"incoming serializer {name!r} is not allowed",
                context={"serializer": name},
            )
        decoded = self.serializers.decode(
            payload[end:],
            name=name,
            context=DeserializationContext(max_size=max_size),
        )
        if not isinstance(decoded, Mapping):
            raise SerializationError("decoded envelope root must be a mapping")
        return Envelope.from_dict(cast(Mapping[str, object], decoded))

    async def _get_engine(self, name: str | None) -> BaseEngine:
        selected_name = name or self._default_engine_name
        if selected_name is None:
            raise LifecycleError("no default engine is selected")
        existing = self._engines.get(selected_name)
        if existing is not None:
            return existing
        return await self._connect_engine(selected_name)

    async def _connect_engine(self, name: str) -> BaseEngine:
        async with self._engine_lock:
            existing = self._engines.get(name)
            if existing is not None:
                return existing
            selected = self.registry.create(name, self.config)
            return await self._adopt_engine(selected)

    async def _adopt_engine(self, selected: BaseEngine) -> BaseEngine:
        """Create the central connection policy owner for one engine."""

        name = selected.name
        existing = self._engines.get(name)
        if existing is not None:
            return existing
        lifecycle = self.config.lifecycle
        raw_interval = lifecycle.get("heartbeat_interval", 30.0)
        heartbeat_interval = (
            None if raw_interval is None else _as_float(raw_interval, name="heartbeat_interval")
        )
        connection = ConnectionManager(
            selected,
            event_emitter=self.events,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=_as_float(
                lifecycle.get("heartbeat_timeout", 5.0),
                name="heartbeat_timeout",
            ),
            reconnect_on_unhealthy=bool(lifecycle.get("reconnect_on_unhealthy", True)),
        )
        try:
            await connection.startup()
        except Exception as error:
            try:
                await connection.shutdown(grace_period=0)
            except Exception:
                pass
            raise LifecycleError(
                f"failed to connect engine {name!r}",
                retryable=True,
                context={"engine": name, "error_type": type(error).__name__},
            ) from error
        self._engines[name] = selected
        self._connections[name] = connection
        return selected

    async def _disconnect_all(self) -> None:
        connections = tuple(reversed(tuple(self._connections.values())))
        self._connections.clear()
        self._engines.clear()
        grace_period = _as_float(
            self.config.lifecycle.get("shutdown_grace_period", 30.0),
            name="shutdown_grace_period",
        )
        for connection in connections:
            try:
                await connection.shutdown(grace_period=grace_period)
            except Exception:
                continue

    def _breaker(self, engine_name: str) -> CircuitBreaker:
        existing = self._breakers.get(engine_name)
        if existing is not None:
            return existing
        if self._injected_breaker is not None and not self._breakers:
            breaker = self._injected_breaker
        else:
            breaker = CircuitBreaker(
                f"engine:{engine_name}",
                self._breaker_config,
                event_emitter=self.events,
            )
        self._breakers[engine_name] = breaker
        return breaker

    async def _ensure_reply_subscription(self) -> None:
        async with self._reply_lock:
            if self._reply_subscription is not None and not self._reply_subscription.closed:
                return

            async def dispatch_reply(delivery: Delivery[object]) -> None:
                correlation_id = delivery.envelope.correlation_id
                if correlation_id is None:
                    return
                future = self._pending_replies.get(correlation_id)
                if future is not None and not future.done():
                    future.set_result(delivery.message)

            self._reply_subscription = await self.subscribe(
                self._reply_route,
                dispatch_reply,
                options=SubscriptionOptions(
                    acknowledgement_mode=AcknowledgementMode.AUTO,
                    concurrency=1,
                    capacity=self._max_pending_replies,
                    consumer_id=f"pyev-rpc-{self._instance_id}",
                ),
            )

    async def _on_retry(self, notification: RetryNotification) -> None:
        self._retry_count += 1
        self.metrics.increment(
            "pyev_retry_total",
            labels={"operation": notification.operation},
        )

    async def _on_delivery_transition(self, transition: DeliveryTransition) -> None:
        if transition.to_state is DeliveryState.ACKNOWLEDGED:
            self.metrics.increment(ACK_TOTAL)
            await self.events.emit("acknowledged", event_type=transition.delivery.envelope.type)
        elif transition.to_state in {DeliveryState.NACKED, DeliveryState.REQUEUED}:
            self.metrics.increment(NACK_TOTAL)
            await self.events.emit("nacked", event_type=transition.delivery.envelope.type)
        elif transition.to_state is DeliveryState.DEFERRED:
            await self.events.emit("deferred", event_type=transition.delivery.envelope.type)

    def _require_running(self, operation: str) -> None:
        if self._state is not BrokerState.RUNNING:
            raise LifecycleError(
                f"cannot {operation} while broker is {self._state.value}",
                context={"operation": operation, "state": self._state.value},
            )

    async def __aenter__(self) -> Broker:
        await self.startup()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        del exc_type, exc, traceback
        await self.shutdown()


def _encode_wire(envelope: Envelope, serializer: Serializer) -> bytes:
    name = serializer.name.encode("ascii")
    if not 1 <= len(name) <= 255:
        raise SerializationError("serializer name must contain 1-255 ASCII bytes")
    encoded = serializer.encode(
        envelope.to_dict(),
        SerializationContext(
            message_type=envelope.type,
            schema_version=envelope.version,
        ),
    )
    return _WIRE_MAGIC + bytes((len(name),)) + name + encoded


def _route_for_message(message: object, route: str | Route | None) -> Route:
    if route is not None:
        return Route.parse(route)
    if is_message(message):
        from pymq.message import message_name

        return Route(message_name(message))
    raise RoutingError("an explicit route is required for an undeclared message")


def _normalize_subscription_pattern(route: str | Route | type[object]) -> str:
    if isinstance(route, str):
        if not route.strip():
            raise RoutingError("subscription pattern must not be empty")
        return route.strip()
    if isinstance(route, Route):
        return route.name
    if isinstance(route, type):
        from pymq.message import message_name

        return message_name(route)
    raise RoutingError("subscription route must be a string, Route, or declared event type")


def _physical_subscription_destination(pattern: str) -> str:
    """Derive an exact fallback destination while retaining a separate pattern."""

    base = pattern.split("*", 1)[0].split("?", 1)[0].rstrip(".:-/")
    return base or "pyev"


def _subscription_headers(options: SubscriptionOptions) -> Mapping[str, str]:
    raw = options.metadata.get("headers", {})
    if not isinstance(raw, Mapping):
        raise ConfigurationError("subscription metadata.headers must be a mapping")
    return MappingProxyType({str(key): str(value) for key, value in raw.items()})


def _validate_publish_options(engine: BaseEngine, options: PublishOptions) -> None:
    if options.delivery_mode is DeliveryMode.AT_MOST_ONCE:
        engine.capabilities.require(Capability.AT_MOST_ONCE, operation="publish")
    elif options.delivery_mode is DeliveryMode.AT_LEAST_ONCE:
        engine.capabilities.require(Capability.AT_LEAST_ONCE, operation="publish")
    elif options.delivery_mode is DeliveryMode.EXACTLY_ONCE:
        engine.capabilities.require(Capability.EXACTLY_ONCE, operation="publish")
    if options.required_capabilities:
        engine.capabilities.require(
            *options.required_capabilities,
            operation="publish",
        )


def _validate_subscription_options(
    engine: BaseEngine,
    options: SubscriptionOptions,
) -> None:
    if options.durable:
        engine.capabilities.require(
            Capability.DURABLE_SUBSCRIPTIONS,
            operation="durable subscription",
        )
    if options.consumer_group is not None:
        engine.capabilities.require(
            Capability.CONSUMER_GROUPS,
            operation="consumer-group subscription",
        )
    if options.acknowledgement_mode is AcknowledgementMode.BATCH:
        # The core safely emulates batch intent with individual acks, while a
        # native batch adapter may optimize it. Semantics are not weakened.
        return


def _retry_policy(
    raw: object,
    *,
    name: str,
    default_attempts: int,
) -> RetryPolicy:
    values = cast(Mapping[str, object], raw) if isinstance(raw, Mapping) else {}
    backoff = _backoff(values.get("backoff"))
    return RetryPolicy(
        max_attempts=_as_int(
            values.get("max_attempts", default_attempts),
            name=f"{name}.max_attempts",
        ),
        max_elapsed_time=_optional_float(values.get("max_elapsed_time")),
        backoff=backoff,
        attempt_timeout=_optional_float(values.get("attempt_timeout")),
        name=str(values.get("name", name)),
    )


def _backoff(raw: object) -> Any:
    if raw is None:
        return ExponentialFullJitterBackoff()
    if isinstance(raw, (int, float)):
        return FixedBackoff(float(raw))
    if not isinstance(raw, Mapping):
        raise ConfigurationError("retry backoff must be a number or mapping")
    kind = str(raw.get("strategy", raw.get("kind", "full_jitter"))).lower().replace("-", "_")
    initial = float(raw.get("initial", 1.0))
    maximum_value = raw.get("maximum", 60.0)
    maximum = None if maximum_value is None else float(maximum_value)
    multiplier = float(raw.get("multiplier", 2.0))
    if kind == "fixed":
        return FixedBackoff(float(raw.get("seconds", initial)))
    if kind == "linear":
        return LinearBackoff(initial, float(raw.get("increment", 1.0)), maximum)
    if kind == "exponential":
        return ExponentialBackoff(initial, multiplier, maximum)
    if kind in {"full_jitter", "exponential_full_jitter"}:
        return ExponentialFullJitterBackoff(initial, multiplier, maximum)
    if kind == "equal_jitter":
        return EqualJitterBackoff(initial, multiplier, maximum)
    if kind == "decorrelated_jitter":
        if maximum is None:
            raise ConfigurationError("decorrelated jitter requires a finite maximum")
        return DecorrelatedJitterBackoff(initial, multiplier, maximum)
    raise ConfigurationError(f"unknown backoff strategy {kind!r}")


def _circuit_config(raw: object) -> CircuitBreakerConfig:
    values = cast(Mapping[str, object], raw) if isinstance(raw, Mapping) else {}
    return CircuitBreakerConfig(
        failure_threshold=_as_int(values.get("failure_threshold", 5), name="failure_threshold"),
        recovery_timeout=_as_float(values.get("recovery_timeout", 30.0), name="recovery_timeout"),
        half_open_max_calls=_as_int(
            values.get("half_open_max_calls", 1), name="half_open_max_calls"
        ),
        success_threshold=_as_int(values.get("success_threshold", 1), name="success_threshold"),
        window_size=_as_int(values.get("window_size", 20), name="window_size"),
        minimum_calls=_as_int(values.get("minimum_calls", 5), name="minimum_calls"),
        failure_rate_threshold=_optional_float(values.get("failure_rate_threshold")),
    )


def _optional_float(value: object) -> float | None:
    return None if value is None else _as_float(value, name="optional float")


def _as_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ConfigurationError(f"{name} must be an integer")
    try:
        return int(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error


def _as_float(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ConfigurationError(f"{name} must be a number")
    try:
        return float(value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error


__all__ = [
    "Broker",
    "BrokerState",
    "InboundContext",
    "OutboundContext",
]
