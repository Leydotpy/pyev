# API reference

This is a hand-curated map of the stable and extension-facing API in the current release. Signatures
are abbreviated where keyword value objects carry the detail. Import application-facing types from
`pyev`; import advanced service contracts from their documented submodules.

## Broker façade

```python
class Broker:
    @classmethod
    def from_config(cls, config, **dependencies) -> Broker: ...

    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...
    async def publish(self, message, *, route=None, headers=None, options=None) -> PublishResult: ...
    async def publish_batch(self, messages, *, options=None) -> BatchPublishResult: ...
    async def subscribe(self, route, handler, *, options=None) -> Subscription: ...
    async def unsubscribe(self, subscription) -> None: ...
    async def request(self, message, *, timeout=None, options=None) -> object: ...
    async def reply(self, request, response, *, options=None) -> PublishResult: ...
    async def health(self) -> HealthReport: ...
```

Properties: `state`, `ready`, `engine`, and `capabilities`. `Broker` is an async context manager.
`EventBus` is a compatibility alias to the same class, not a second runtime. `Publisher` and
`Subscriber` are restricted protocols for dependency injection.

Operation values:

- `PublishOptions`, `BatchPublishOptions`, `RequestOptions`, and `ReplyOptions`;
- `DeliveryMode`;
- `PublishResult`, `BatchPublishResult`, and `BatchItemError`; and
- `Subscription`, `SubscriptionOptions`, and `SubscriptionState`.

## Messages, events, and envelopes

`Message` and `MessageLike` describe accepted application values. `@event(name, version=...)`
registers metadata on a model. `EventRegistry` supports isolated registration, version lookup,
upcasters, and reconstruction. `EventKey`, `EventMetadata`, and `get_event_metadata()` support
introspection.

`Envelope` is a frozen, independently versioned wire value:

- `Envelope.create(payload, *, type, version=1, ...)`;
- `Envelope.from_message(message, **metadata)`;
- `to_dict()` / `from_dict()`;
- `to_bytes()` / `from_bytes()`;
- `to_message(registry=...)`; and
- expiry, correlation, causation, trace, serializer, partition, ordering, and reply metadata.

`CURRENT_ENVELOPE_VERSION` identifies the supported framework wire format.

`Delivery[T]` exposes `message`, `envelope`, route/subscription/consumer metadata, attempt count,
transport metadata, and state transitions. Its async operations are `ack`, `nack`, `reject`,
`requeue`, `defer`, and `touch`. `DeliveryState` and `DeliveryTransition` make the state machine
observable.

## Acknowledgements

Module: `pyev.acknowledgements`.

- `AcknowledgementMode`: `AUTO`, `MANUAL`, `BATCH`, `NONE`.
- `AcknowledgementAdapter`: transport translation protocol.
- `CallbackAcknowledgementAdapter`: compose adapters from async callbacks.

Applications normally call acknowledgement methods on `Delivery`, not an adapter directly.

## Routing and middleware

Module: `pyev.routing`.

- `Route`, `RouteKind`, `Destination`, `DestinationKind`, and `HandlerPattern`;
- `Router.register()`, `Router.on()`, destination mapping, unregistering, and introspection; and
- `route_matches()` for portable wildcard matching.

Module: `pyev.middleware`.

- generic `Middleware` protocol and `MiddlewarePipeline`;
- `InboundMiddlewarePipeline` and `OutboundMiddlewarePipeline`; and
- named, ordered, route/header-scoped registration with deterministic introspection.

## Serialization

Module: `pyev.serialization`.

- `Serializer` protocol;
- `SerializationContext` and `DeserializationContext`;
- isolated `SerializerRegistry`; and
- `JsonSerializer`, optional `MessagePackSerializer`, and opt-in trusted-only `PickleSerializer`.

JSON is the safe default. Pickle requires explicit unsafe consent and must not decode untrusted
bytes.

## Capabilities, engines, and discovery

`Capability`, `CapabilitySpec`, and immutable `CapabilitySet` expose `supports()`, `require()`,
`attributes_for()`, `attribute()`, `with_capability()`, `without()`, and `union()`.

Module: `pyev.engines.base`.

- `BaseEngine`, `Availability`, and `EngineHealth`;
- `EnginePublishContext` and `EnginePublishResult`;
- `EngineSubscription`, `EngineConsumer`, and `EngineIncomingMessage`;
- `EngineAcknowledgementAdapter` and `EngineDeliveryCallback`; and
- optional `BatchPublishEngine` and `NativeRequestReplyEngine` protocols.

`EngineRegistry` supports explicit/lazy registration, isolated instances, availability-aware
selection, unregistering for tests, entry-point discovery, and construction. Built-ins are
`LocalEngine`, `MemoryEngine`, `RedisEngine`, `RabbitMQEngine`, and `KafkaEngine`.

## Reliability

Module: `pyev.reliability`.

- Backoff: `FixedBackoff`, `LinearBackoff`, `ExponentialBackoff`,
  `ExponentialFullJitterBackoff`, `EqualJitterBackoff`, `DecorrelatedJitterBackoff`,
  `CallableBackoff`, and `BackoffStrategy`.
- Retry: `RetryPolicy`, `RetryManager`, `RetryContext`, `RetryNotification`, `RetryBudget`,
  `TypeExceptionClassifier`, `CallableExceptionClassifier`, `FailureDecision`, and
  `TerminalAction`.
- Circuit: `CircuitBreaker`, `CircuitBreakerConfig`, `CircuitBreakerSnapshot`,
  `CircuitBreakerRegistry`, and `CircuitState`.
- Idempotency: `IdempotencyStore`, `MemoryIdempotencyStore`, `IdempotencyRecord`, and
  `IdempotencyStatus`.
- Outbox: `OutboxStore`, `MemoryOutboxStore`, `OutboxMessage`, `OutboxStatus`, and
  `OutboxDispatcher`.

## Dead letters

Module: `pyev.deadletter`.

- `DeadLetterManager`, `DeadLetterPolicy`, `DeadLetterContext`, and `DeadLetterRecord`;
- `DeadLetterStore` and bounded `MemoryDeadLetterStore`;
- `DeadLetterFilter`, `DeadLetterStatus`, and `RetryHistoryEntry`; and
- `ReplayManager`, `ReplayResult`, `ReplayOutcome`, and `QuarantineManager`.

Replay publishing and durable persistence are injected contracts; the memory store is not a
production durability claim.

## Lifecycle and connections

Module: `pyev.lifecycle`.

- `LifecycleManager`, `LifecycleState`, `LifecycleComponent`, and `RegisteredComponent`;
- `TaskSupervisor`, `RestartPolicy`, `SupervisedTaskState`, `SupervisedTaskSnapshot`, and
  `TaskFailure`.

Module: `pyev.connection`.

- `ConnectionManager`, `ConnectionState`, and `ConnectionSnapshot`;
- connect/startup, ensure connected, reconnect, health, drain, shutdown/disconnect;
- operation `lease()` and `run()`; and
- named topology restoration callbacks.

## Observability and internal events

Module: `pyev.observability`.

- Health: `HealthStatus`, `ComponentHealth`, `HealthReport`, `HealthRegistry`, and
  `NoOpHealthCheck`.
- Metrics: `MetricsProvider`, `NoOpMetrics`, `InMemoryMetrics`, `MetricsSnapshot`,
  `HistogramSnapshot`, and `DurationTimer`.
- Tracing: `TraceContext`, `TracePropagator`, `Tracer`, `Span`, `SpanKind`, `SpanStatus`,
  `NoOpTracer`, and `InMemoryTracer`.
- Logging/redaction: `StructuredLogAdapter`, `get_logger()`, `redact_value()`, and
  `redact_mapping()`.

Module: `pyev.events`.

- `OperationalEvent`, `OperationalEventName`, and `InternalEventEmitter`;
- listener registration, wildcard subscription, failure history, and `EmitResult`; and
- `CriticalListenerError` for explicitly critical hooks.

These events are operational hooks, not routed domain messages.

## Configuration and integrations

`BrokerConfig`, `ConfigLoader`, `SecretValue`, `load_config()`, immutable config views, and
redacted mapping exports are documented in [Getting started](getting-started.md).

Modules under `pyev.integrations` provide generic ASGI middleware/lifespan, FastAPI/Starlette
lifespan and dependency factories, lazy Celery worker hooks, and optional Django configuration plus
transaction-on-commit publishing. See [Framework integrations](integrations.md) for lifecycle and
process caveats.

## Testing

Module: `pyev.testing`.

`DeterministicClock`, `DeterministicRetryScheduler`, `FailureInjector`, `FakeEngine`,
`FakeConsumer`, `FakeAcknowledgementAdapter`, `MockPublisher`, `assert_published()`,
`broker_override()`, and `eventually()`.

## Exceptions

All framework exceptions derive from `PyevError` and expose `retryable` plus a non-secret
`context` mapping. Major families are:

```text
PyevError
├── ConfigurationError
├── LifecycleError
├── RegistryError
├── EngineError
│   ├── EngineUnavailableError
│   ├── ConnectionError (BrokerConnectionError alias)
│   ├── PublishError
│   ├── ConsumeError
│   └── UnsupportedCapabilityError
├── SerializationError
├── MessageValidationError
├── RoutingError
├── MiddlewareError
├── AcknowledgementError
│   └── InvalidStateTransitionError
├── RetryExhaustedError
├── CircuitOpenError
├── DeadLetterError
└── RequestTimeoutError
```
