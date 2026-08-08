# AGENTS.md

# Project: `janus-events`

**Document type:** Unified architecture and implementation specification
**Version:** 2.0
**Status:** Authoritative build brief
**Target runtime:** Python 3.12+

---

# 1. Purpose of This Document

This document is the single source of truth for building `janus-events`.

It harmonizes the requirements for:

- a production-grade, asynchronous message-broker abstraction;
- a strongly typed domain-event framework;
- pluggable transport engines;
- publish/subscribe and request/reply messaging;
- retries, acknowledgements, dead-letter handling, routing, middleware, and observability;
- ASGI-native lifecycle management; and
- optional first-class Django integration.

Where older specifications use different names for the same concept, this document defines the canonical terminology and responsibility boundaries that the implementation must follow.

An implementation agent must treat this document as authoritative. It must not create competing abstractions for concepts unified here.

---

# 2. Project Overview

`janus-events` is a standalone, asynchronous, transport-agnostic event and messaging framework for Python applications.

It is initially intended to support the wider `janus-api` ecosystem, but the package must contain **no Janus-specific business logic**. It must be reusable by any Python application that needs event publication, subscription, routing, message handling, or broker-backed communication.

The framework is not intended to reimplement Redis, RabbitMQ, Kafka, NATS, or any other broker. It is a stable abstraction and orchestration layer over interchangeable messaging technologies.

The central design philosophy is:

> **Applications publish domain events and messages. Transport engines deliver them. Consumers react through handlers.**

Application code must not depend on transport-specific APIs.

The same application-facing code should work regardless of whether the configured engine is:

- local in-process dispatch;
- an in-memory queue;
- Redis;
- RabbitMQ;
- Kafka;
- NATS;
- MQTT;
- Apache Pulsar;
- Amazon SQS/SNS;
- Azure Service Bus;
- Google Pub/Sub; or
- a third-party engine installed later.

---

# 3. Product Vision

The project should combine the architectural quality and developer experience associated with mature frameworks such as:

- Celery;
- SQLAlchemy;
- Django's cache and storage backend systems;
- OpenTelemetry SDK;
- Symfony Messenger;
- MassTransit;
- Spring Messaging;
- Laravel Events; and
- NestJS messaging abstractions.

It should remain Pythonic, strongly typed, modular, asynchronous, testable, and usable at different scales, including:

- single-process Python applications;
- Django applications;
- ASGI applications;
- FastAPI, Starlette, Litestar, Quart, or similar services;
- Celery workers;
- background daemons;
- modular monoliths;
- distributed microservices; and
- larger event-driven systems.

The public API must remain stable when transport engines change.

---

# 4. Canonical Terminology

The implementation must use the following terms consistently.

## 4.1 `Broker`

`Broker` is the canonical public façade used by applications.

It coordinates publishing, subscribing, routing, middleware, reliability, lifecycle, and transport selection. It must not contain transport-specific implementation logic.

`EventBus` may be exposed as a compatibility alias or structural protocol for `Broker`, but it must not become a separate competing implementation.

## 4.2 Event

An `Event` is a strongly typed representation of something that happened in a domain.

Examples include:

- `PublisherJoined`;
- `SessionCreated`;
- `RecordingCompleted`; and
- `UserInvited`.

An event should normally be represented by a dataclass, attrs class, or Pydantic-compatible model rather than an arbitrary dictionary.

## 4.3 Message

A `Message` is the framework-level unit delivered through the broker. An event is one category of message.

The design should allow future command, notification, and RPC message types without weakening the event API.

## 4.4 Envelope

An `Envelope` is the transport-independent serialized representation of a message plus its metadata.

Every engine sends or receives envelopes. Engines must not receive arbitrary application objects directly.

## 4.5 Transport Engine

A `TransportEngine` is a backend adapter that implements broker-specific I/O primitives.

Examples are `RedisEngine`, `RabbitMQEngine`, and `KafkaEngine`.

“Transport,” “backend,” and “engine” refer to the same architectural layer. The implementation should use `Engine` as the canonical class suffix.

## 4.6 Subscriber and Consumer

A subscriber defines interest in one or more routes. A consumer is the running process or task that receives deliveries for a subscription.

## 4.7 Handler

A handler is application code invoked by the router for a matching message.

## 4.8 Router

The router maps message or event types, names, namespaces, topics, headers, or patterns to handlers and destinations.

## 4.9 Acknowledgement Adapter

An acknowledgement adapter translates framework acknowledgement operations into transport-specific operations.

## 4.10 Reliability Services

Retries, backoff, circuit breaking, dead-letter handling, idempotency, and acknowledgement orchestration are framework services. They must not be duplicated independently inside engines.

---

# 5. Primary Goals

The completed framework must provide:

- one consistent application-facing API;
- strongly typed events and messages;
- a standard transport-independent envelope;
- interchangeable transport engines;
- automatic engine registration and discovery;
- runtime plugin loading through Python entry points;
- local and memory engines for development and testing;
- Redis, RabbitMQ, and Kafka engines;
- exact, wildcard, namespace, topic, fanout, direct, headers, broadcast, and RPC routing abstractions;
- middleware for outbound and inbound processing;
- configurable serializers;
- a unified acknowledgement protocol;
- centralized retry and backoff policies;
- circuit-breaker protection;
- transport-independent dead-letter handling;
- capability discovery instead of engine-type checks;
- ASGI-native lifecycle management;
- graceful startup and shutdown;
- health monitoring, metrics, and distributed tracing;
- optional Django integration;
- comprehensive testing utilities;
- documentation and examples; and
- a stable, extensible public API.

A basic application should be able to use the framework as follows:

```python
from janus_events import Broker

broker = Broker()

await broker.startup()

await broker.publish(
    PublisherJoined(
        room=42,
        publisher_id=1234,
        display="Alice",
    )
)

await broker.subscribe("videoroom.*", handler=handle_videoroom_event)

await broker.shutdown()
```

The application must not need to know whether Redis, RabbitMQ, Kafka, or another engine is configured.

---

# 6. Non-Goals

The project must not:

- implement a new network message broker;
- reproduce every transport's complete native API;
- pretend that all transports provide identical delivery guarantees;
- place Janus-specific event models in the core package;
- require Django, FastAPI, Celery, or another application framework in the core distribution;
- open network connections during module import;
- rely on global mutable runtime state;
- silently downgrade requested reliability guarantees; or
- expose transport-native message objects to application handlers by default.

Transport-specific advanced features may be exposed through typed extension interfaces, but the stable core API must remain portable.

---

# 7. Core Design Principles

## 7.1 Transport Agnosticism

Business code must never branch on engine classes or import transport libraries.

Incorrect:

```python
if isinstance(engine, KafkaEngine):
    ...
```

Correct:

```python
if broker.capabilities.supports(Capability.CONSUMER_GROUPS):
    ...
```

## 7.2 Async First

The runtime must be based on `asyncio`.

All I/O-bound public APIs must be asynchronous. Synchronous convenience wrappers may be added only in a separate adapter and must not drive the core architecture.

## 7.3 Strong Typing

All public APIs must have complete type annotations and pass strict static analysis.

Prefer:

```python
PublisherJoined(room=42, publisher_id=1234, display="Alice")
```

over:

```python
{"room": 42, "publisher_id": 1234, "display": "Alice"}
```

The framework may accept mapping payloads at low-level boundaries, but typed message models should be the primary developer experience.

## 7.4 Composition and Dependency Inversion

The broker façade must compose replaceable interfaces for:

- engines;
- routing;
- serialization;
- middleware;
- retries;
- backoff;
- acknowledgements;
- dead-letter storage;
- metrics;
- tracing;
- configuration; and
- lifecycle services.

Concrete implementations must depend on protocols or abstract interfaces rather than on each other directly.

## 7.5 Capability-Based Behaviour

The framework must model transport differences explicitly.

It must not claim exactly-once delivery, transactions, delayed delivery, native dead-lettering, or ordering when an engine cannot provide them.

## 7.6 Framework-Independent Core

The core package must not import Django, FastAPI, Starlette, Celery, or other application frameworks.

Framework integrations must live in optional integration packages or namespaces and depend on the core, never the reverse.

## 7.7 No Import-Time Side Effects

Importing `janus_events` must not:

- open sockets;
- create event loops;
- start tasks;
- discover network services;
- read secrets unnecessarily; or
- mutate application framework state.

Runtime initialization must occur through explicit lifecycle calls or framework startup hooks.

## 7.8 Stable Public API

Public interfaces must follow semantic versioning. Internal implementation details should remain private unless intentionally promoted to the public API.

---

# 8. Unified Architecture

## 8.1 Outbound Flow

```text
Application
    │
    ▼
Broker façade
    │
    ▼
Message normalization and envelope creation
    │
    ▼
Routing and destination resolution
    │
    ▼
Outbound middleware pipeline
    │
    ▼
Reliability coordinator
    ├── Retry manager
    ├── Backoff policy
    ├── Circuit breaker
    └── Dead-letter manager
    │
    ▼
Connection manager
    │
    ▼
Selected transport engine
```

## 8.2 Inbound Flow

```text
Transport engine
    │
    ▼
Connection and consumer manager
    │
    ▼
Delivery and acknowledgement adapter
    │
    ▼
Envelope decoding and validation
    │
    ▼
Inbound middleware pipeline
    │
    ▼
Event router
    │
    ▼
Handler invocation
    │
    ├── Success → acknowledge
    ├── Deferred → defer/touch
    ├── Retryable failure → retry/requeue
    └── Terminal failure → reject/dead-letter
```

All built-in engines, including local and memory engines, must preserve these semantics even when an internal optimization avoids a physical network boundary.

## 8.3 Responsibility Boundary

The engine performs transport-specific I/O.

The framework core performs:

- envelope construction;
- route resolution;
- serialization selection;
- middleware execution;
- retry decisions;
- acknowledgement semantics;
- dead-letter policy;
- lifecycle coordination;
- observability; and
- capability validation.

An engine may expose transport-specific hooks required by these services, but must not independently reimplement the service policy.

---

# 9. Recommended Repository Layout

Use a `src` layout.

```text
janus-events/
├── AGENTS.md
├── README.md
├── CHANGELOG.md
├── LICENSE
├── pyproject.toml
├── uv.lock                      # when uv is selected
├── docs/
├── examples/
├── scripts/
├── src/
│   └── janus_events/
│       ├── __init__.py
│       ├── broker.py
│       ├── event.py
│       ├── message.py
│       ├── envelope.py
│       ├── delivery.py
│       ├── subscription.py
│       ├── capabilities.py
│       ├── exceptions.py
│       ├── typing.py
│       ├── config.py
│       ├── factory.py
│       ├── registry.py
│       ├── discovery.py
│       ├── plugins.py
│       ├── lifecycle.py
│       ├── connection.py
│       ├── acknowledgements.py
│       ├── routing/
│       ├── middleware/
│       ├── serialization/
│       ├── reliability/
│       │   ├── retry.py
│       │   ├── backoff.py
│       │   ├── circuit_breaker.py
│       │   ├── idempotency.py
│       │   └── outbox.py
│       ├── deadletter/
│       ├── events/
│       ├── engines/
│       │   ├── base.py
│       │   ├── local.py
│       │   └── memory.py
│       ├── observability/
│       │   ├── health.py
│       │   ├── metrics.py
│       │   └── tracing.py
│       ├── integrations/
│       │   ├── asgi.py
│       │   ├── django/
│       │   ├── fastapi.py
│       │   ├── starlette.py
│       │   └── celery.py
│       ├── testing/
│       └── utils/
├── packages/
│   ├── janus-events-redis/
│   ├── janus-events-rabbitmq/
│   └── janus-events-kafka/
└── tests/
    ├── unit/
    ├── contract/
    ├── integration/
    ├── performance/
    └── fixtures/
```

The implementation may initially keep optional engines in one repository, but packaging boundaries must ensure that installing the core package does not install Redis, RabbitMQ, or Kafka client libraries.

Recommended distribution model:

```text
janus-events
janus-events-redis
janus-events-rabbitmq
janus-events-kafka
```

Optional extras may also be provided for convenience:

```bash
pip install "janus-events[redis]"
pip install "janus-events[rabbitmq]"
pip install "janus-events[kafka]"
```

The dedicated distributions and extras must resolve to the same registered engine implementations.

---

# 10. Public API Design

## 10.1 `Broker`

`Broker` is the only high-level runtime façade.

It should expose at least:

```python
class Broker:
    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...
    async def health(self) -> HealthReport: ...

    async def publish(
        self,
        message: MessageLike,
        *,
        route: str | Route | None = None,
        headers: Mapping[str, str] | None = None,
        options: PublishOptions | None = None,
    ) -> PublishResult: ...

    async def publish_batch(
        self,
        messages: Sequence[MessageLike],
        *,
        options: BatchPublishOptions | None = None,
    ) -> BatchPublishResult: ...

    async def subscribe(
        self,
        route: str | Route,
        handler: MessageHandler,
        *,
        options: SubscriptionOptions | None = None,
    ) -> Subscription: ...

    async def unsubscribe(self, subscription: Subscription | str) -> None: ...

    async def request(
        self,
        message: MessageLike,
        *,
        timeout: float | None = None,
        options: RequestOptions | None = None,
    ) -> Message: ...

    async def reply(
        self,
        request: Delivery,
        response: MessageLike,
        *,
        options: ReplyOptions | None = None,
    ) -> PublishResult: ...
```

The exact signatures may evolve during implementation, but the separation of responsibilities must remain.

The broker must support async context management:

```python
async with Broker.from_config(config) as broker:
    await broker.publish(MyEvent(...))
```

`startup()` and `shutdown()` must be idempotent.

## 10.2 Publisher and Subscriber Views

For dependency minimization, the framework should expose restricted protocols such as:

```python
class Publisher(Protocol):
    async def publish(...) -> PublishResult: ...


class Subscriber(Protocol):
    async def subscribe(...) -> Subscription: ...
```

These should be views or protocols over the same `Broker`, not separate runtime systems.

## 10.3 Factory

Provide a factory for configuration-driven construction:

```python
broker = BrokerFactory.create(config)
```

or:

```python
broker = Broker.from_config(config)
```

Do not create multiple factories that solve the same problem.

---

# 11. Event and Message Model

## 11.1 Base Event Metadata

Every event must support the following metadata, either directly or through its envelope:

- unique message ID;
- event name or type;
- schema version;
- timestamp;
- source;
- correlation ID;
- causation ID;
- trace ID and trace context;
- content type;
- serializer identifier;
- headers;
- optional partition or ordering key;
- optional expiry or time-to-live; and
- payload.

## 11.2 Typed Event Declaration

The package should provide an ergonomic way to declare events.

Example:

```python
from dataclasses import dataclass
from janus_events import event


@event("videoroom.publisher.joined", version=1)
@dataclass(frozen=True, slots=True)
class PublisherJoined:
    room: int
    publisher_id: int
    display: str
```

Pydantic models should also be supported through an adapter without making Pydantic mandatory for the core.

## 11.3 Envelope

A canonical envelope should resemble:

```json
{
  "id": "01J...",
  "type": "videoroom.publisher.joined",
  "version": 1,
  "timestamp": "2026-08-06T00:00:00Z",
  "source": "janus-api",
  "correlation_id": "...",
  "causation_id": "...",
  "trace": {},
  "content_type": "application/json",
  "serializer": "json",
  "headers": {},
  "payload": {
    "room": 42,
    "publisher_id": 1234,
    "display": "Alice"
  }
}
```

The envelope format must be versioned independently from application event schemas.

## 11.4 Event Versioning

The registry and router must support multiple versions of the same event name.

The framework should provide extension points for:

- upcasting old event schemas;
- rejecting unsupported versions;
- routing versions independently; and
- maintaining compatibility during rolling deployments.

It must not mutate event versions silently.

## 11.5 Immutability

Envelope identity and core delivery metadata should be immutable after creation. Mutable processing state must live in a separate delivery context.

---

# 12. Delivery Model and State Machine

A received message must be represented by a framework `Delivery` object rather than by a transport-native message.

A delivery should expose:

- decoded message or event;
- envelope;
- route and subscription information;
- attempt count;
- delivery timestamp;
- deadline or visibility timeout where applicable;
- consumer identity;
- acknowledgement methods; and
- a controlled escape hatch to transport metadata when explicitly requested.

## 12.1 Lifecycle States

Implement and validate a state machine covering at least:

```text
CREATED
PUBLISHED
QUEUED
DELIVERED
PROCESSING
ACKNOWLEDGED
NACKED
DEFERRED
REQUEUED
RETRY_SCHEDULED
REJECTED
DEAD_LETTERED
EXPIRED
CANCELLED
```

Invalid transitions must raise a typed error.

State transitions should emit internal events and observability signals.

---

# 13. Transport Engine SPI

## 13.1 Minimum Engine Interface

Every engine must implement a small, transport-focused interface resembling:

```python
class BaseEngine(ABC):
    name: ClassVar[str]
    priority: ClassVar[int]

    @classmethod
    def is_available(cls, config: ConfigView) -> Availability: ...

    @property
    def capabilities(self) -> CapabilitySet: ...

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...

    async def publish(
        self,
        destination: Destination,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult: ...

    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer: ...

    async def healthcheck(self) -> EngineHealth: ...
```

The exact surface should stay as small as practical.

## 13.2 Optional Engine Protocols

Advanced capabilities should be represented through optional protocols or capability-specific adapters, such as:

- `TransactionalEngine`;
- `BatchPublishEngine`;
- `BatchAcknowledgeEngine`;
- `NativeDelayEngine`;
- `NativeDeadLetterEngine`;
- `NativeRequestReplyEngine`;
- `ConsumerGroupEngine`; and
- `ExactlyOnceEngine`.

The broker should provide portable fallbacks where semantically safe. Otherwise it must raise a clear `UnsupportedCapabilityError`.

## 13.3 Reconnection Responsibility

The central `ConnectionManager` owns reconnect policy, timing, circuit breaking, and lifecycle orchestration.

Each engine must provide the transport-specific primitives needed to reconnect, resubscribe, restore consumers, or detect stale connections. Engines must not create an independent conflicting retry policy.

## 13.4 Engine Isolation

The core distribution must not import optional client libraries.

An unavailable optional dependency should mark that engine unavailable and produce an actionable error only when the engine is selected explicitly.

---

# 14. Engine Registry and Plugin Discovery

## 14.1 Registry

Provide a typed engine registry.

The registry must support:

- explicit registration;
- unregistering for tests;
- duplicate-name detection;
- inspection of registered engines;
- lazy loading; and
- isolated registry instances.

Avoid a mutable process-wide singleton as the only registry. A default registry may exist for convenience, but `Broker` must accept an injected registry.

## 14.2 Entry Points

Use Python package entry points.

Recommended groups:

```text
janus_events.engines
janus_events.serializers
janus_events.middleware
janus_events.routers
janus_events.deadletter_stores
janus_events.config_providers
janus_events.metrics_exporters
janus_events.tracing_providers
```

Example engine registration:

```toml
[project.entry-points."janus_events.engines"]
redis = "janus_events_redis:RedisEngine"
```

Installing a package such as:

```bash
pip install janus-events-nats
```

must add NATS support without modifying the core framework.

## 14.3 Discovery Timing

Entry-point discovery must be lazy or occur during `startup()`. It must not open connections or start tasks during import.

## 14.4 Engine Selection

Resolve an engine in this order:

1. runtime override supplied to the broker operation or scope;
2. explicitly configured default engine;
3. highest-priority engine whose availability requirements are satisfied;
4. memory engine only when fallback is explicitly permitted by configuration.

Production deployments must not silently fall back from an explicitly requested external engine to memory.

Automatic detection may inspect configuration through engine-owned availability checks. The `Broker` itself must not contain engine-specific environment-variable logic.

---

# 15. Built-In Engines

## 15.1 Local Engine

The local engine dispatches messages within the current process.

It is intended for:

- desktop or CLI applications;
- simple single-process deployments;
- development;
- deterministic tests; and
- direct handler integration.

It should preserve middleware, routing, acknowledgement, and delivery semantics.

## 15.2 Memory Engine

The memory engine uses asynchronous in-memory queues and supports realistic producer/consumer behaviour.

It is intended for:

- unit tests;
- integration tests;
- benchmarks;
- development; and
- environments where process-local queue semantics are sufficient.

It should support configurable queue capacity, backpressure, consumer concurrency, deterministic timing, and failure injection.

## 15.3 Redis Engine

Use `redis.asyncio`.

The implementation should define supported modes clearly, such as Redis Pub/Sub and Redis Streams. Do not conflate their delivery guarantees.

Where Redis Streams are used, support as appropriate:

- consumer groups;
- pending entries;
- acknowledgements;
- replay or reclaim behaviour;
- pattern or route mapping; and
- reconnect restoration.

## 15.4 RabbitMQ Engine

Use `aio-pika` or another well-maintained asynchronous AMQP client selected deliberately.

Support where applicable:

- exchanges;
- queues;
- routing keys;
- durable topology;
- publisher confirmations;
- acknowledgements;
- prefetch and backpressure;
- dead-letter exchanges; and
- reconnect restoration.

## 15.5 Kafka Engine

Use an actively maintained asynchronous Kafka client selected during implementation.

Support where applicable:

- topics;
- partitions;
- consumer groups;
- offsets and commits;
- partition keys;
- rebalancing;
- producer idempotence;
- transactions when supported; and
- consumer lag reporting.

Do not describe Kafka acknowledgement semantics as equivalent to AMQP acknowledgements; translate them through the unified delivery API while preserving their actual behaviour.

---

# 16. Capability System

Every engine must advertise a structured `CapabilitySet`.

Capabilities should include, where relevant:

- publish/subscribe;
- wildcard subscriptions;
- durable subscriptions;
- consumer groups;
- competing consumers;
- fanout;
- headers routing;
- message ordering;
- partition ordering;
- transactions;
- publisher confirms;
- native dead-letter queues;
- native delayed delivery;
- scheduled delivery;
- batch publishing;
- batch acknowledgement;
- visibility timeout;
- at-most-once delivery;
- at-least-once delivery;
- exactly-once processing support;
- request/reply support;
- message priorities; and
- queue-depth or lag metrics.

Capabilities should support additional attributes, not only booleans. For example, ordering may be scoped per partition rather than global.

The broker must validate requested options before performing an operation and must return actionable capability errors.

---

# 17. Routing System

The routing subsystem must be transport-independent.

It should support:

- exact event names;
- Python event types;
- namespace patterns;
- wildcard patterns;
- topic routing;
- direct routing;
- fanout;
- headers-based routing;
- broadcast;
- request/reply; and
- pluggable custom strategies.

Examples:

```text
videoroom.publisher.joined
videoroom.*
session.*
recording.completed
*
```

The framework router must implement portable matching when the selected transport lacks native wildcard or headers routing.

Routing must distinguish:

- the logical route exposed to applications;
- the physical transport destination; and
- the registered local handler pattern.

These concepts must not be represented by one ambiguous string internally.

## 17.1 Router Registration

Support explicit and decorator-based registration:

```python
router.register(PublisherJoined, handle_publisher_joined)
```

```python
@router.on("videoroom.*")
async def handle_videoroom_event(delivery: Delivery[Event]) -> None:
    ...
```

Decorator use must not require import-time connection creation.

## 17.2 Handler Discovery

Automatic handler discovery may be offered, but explicit registration should remain available and testable. Discovery should avoid importing every installed module indiscriminately.

---

# 18. Middleware Pipeline

Provide separate outbound and inbound middleware pipelines.

Middleware should use an ASGI-like callable design and support dependency injection.

## 18.1 Outbound Middleware

Typical outbound stages include:

1. message validation;
2. metadata and correlation enrichment;
3. tracing;
4. authorization policy where configured;
5. serialization;
6. compression;
7. encryption;
8. metrics and logging;
9. reliability orchestration; and
10. engine invocation.

## 18.2 Inbound Middleware

Typical inbound stages include:

1. transport metadata normalization;
2. decryption and decompression;
3. deserialization;
4. envelope and schema validation;
5. tracing context extraction;
6. logging and metrics;
7. idempotency checking;
8. handler dispatch;
9. acknowledgement decision; and
10. retry or dead-letter handling.

The exact default order must be documented and tested.

## 18.3 Middleware Requirements

Middleware must support:

- asynchronous callables;
- deterministic ordering;
- named registration;
- per-broker and per-route configuration;
- short-circuiting where valid;
- error propagation;
- introspection; and
- third-party entry-point registration.

A middleware must not directly acknowledge a transport-native message. It should operate through the delivery context and acknowledgement manager.

---

# 19. Serialization

Provide a serializer protocol with at least:

```python
class Serializer(Protocol):
    name: str
    content_type: str

    def encode(self, value: object, context: SerializationContext) -> bytes: ...
    def decode(self, data: bytes, context: DeserializationContext) -> object: ...
```

Built-in or officially supported serializers should include:

- JSON;
- MessagePack;
- Protobuf;
- Avro; and
- Pickle for trusted internal/testing use only.

Pickle must never be the default and must carry a security warning because untrusted pickle payloads are unsafe.

Serializer selection should be driven by envelope metadata and configuration. Consumers must validate supported content types.

Application-defined serializers must be registerable without editing the core package.

---

# 20. Unified Acknowledgement Protocol

Applications must acknowledge through the framework `Delivery`, never directly through a transport object.

A delivery should expose:

```python
await delivery.ack()
await delivery.nack(requeue=True)
await delivery.reject()
await delivery.requeue()
await delivery.defer(delay=30)
await delivery.touch(extension=60)
```

Operations unsupported by an engine must either:

- be safely emulated by a documented framework service; or
- raise `UnsupportedCapabilityError`.

They must not silently become no-ops.

## 20.1 Modes

Support:

- `AUTO`;
- `MANUAL`;
- `BATCH`; and
- `NONE`.

`AUTO` should acknowledge only after successful handler completion unless explicitly configured otherwise.

## 20.2 Idempotence

Acknowledgement operations should be idempotent at the framework layer where possible. Conflicting terminal operations must raise a state error.

## 20.3 Acknowledgement Adapters

Each engine must provide a transport-specific acknowledgement adapter that translates framework operations into:

- AMQP acknowledgements or rejects;
- Kafka offset commits;
- Redis Stream acknowledgements or claims;
- visibility-timeout changes; or
- equivalent native operations.

---

# 21. Reliability and Delivery Semantics

## 21.1 Delivery Modes

Expose explicit reliability modes:

- fire-and-forget;
- at-most-once;
- at-least-once; and
- exactly-once or effectively-once only when the full configuration supports it.

The framework must distinguish transport delivery guarantees from end-to-end processing guarantees.

It must never promise exactly-once processing merely because a producer or broker supports transactions.

## 21.2 Retry Manager

Retries must be orchestrated centrally.

Support policies for:

- publishing;
- consuming and handler execution;
- RPC;
- heartbeat;
- reconnect;
- topology recovery; and
- dead-letter replay.

A retry policy should define:

- maximum attempts;
- elapsed-time limit;
- retryable exception predicates;
- backoff strategy;
- jitter;
- retry budget;
- per-attempt timeout;
- cancellation behaviour; and
- terminal action.

## 21.3 Backoff Strategies

Support:

- fixed;
- linear;
- exponential;
- exponential with full jitter;
- equal jitter;
- decorrelated jitter; and
- adaptive strategies through plugins.

Exponential full jitter should be the default for network retry policies unless a transport contract requires otherwise.

## 21.4 Retry Metadata

A retry must preserve or update:

- original message ID;
- correlation and causation IDs;
- attempt number;
- previous errors;
- first-seen timestamp;
- next-attempt timestamp;
- retry policy name; and
- originating consumer.

Whether a retry uses the same message ID or creates a new delivery ID must be explicit and consistent.

## 21.5 Circuit Breaker

Provide a circuit breaker with:

```text
CLOSED
OPEN
HALF_OPEN
```

Support:

- configurable failure thresholds;
- failure-rate windows;
- cooldown periods;
- limited half-open probes;
- automatic recovery;
- manual reset;
- health integration; and
- observability events.

Circuit breakers should be scoped sensibly, such as by engine, connection pool, destination, or operation class.

## 21.6 Backpressure

Backpressure is mandatory.

Provide controls for:

- bounded in-memory queues;
- maximum in-flight deliveries;
- consumer concurrency;
- prefetch;
- producer rate limiting;
- overflow policy;
- pause and resume; and
- graceful overload behaviour.

The framework must not create an unbounded task for every message.

---

# 22. Dead-Letter Subsystem

Dead-letter handling must be transport-independent even when native DLQ facilities are available.

## 22.1 Components

Provide:

- `DeadLetterManager`;
- `DeadLetterPolicy`;
- `DeadLetterStore` protocol;
- `ReplayManager`;
- `QuarantineManager`; and
- retention and archive policies.

## 22.2 Dead-Letter Record

Every dead-letter record should preserve:

- original envelope bytes when available;
- decoded payload where safe;
- route and destination;
- headers and metadata;
- exception type and message;
- sanitized traceback;
- retry history;
- timestamps;
- consumer and subscription identity;
- engine information;
- failure classification;
- schema and serializer information; and
- quarantine status.

Sensitive values must be redactable before persistence.

## 22.3 Storage Implementations

Design storage adapters for:

- memory;
- filesystem;
- SQLite;
- PostgreSQL;
- Redis;
- RabbitMQ native DLQ;
- Kafka topics;
- S3-compatible object storage; and
- MongoDB.

Only a practical subset must be implemented initially. The protocol and conformance tests must permit third-party stores.

## 22.4 Operations

Support:

- inspect;
- filter;
- replay one;
- replay selected;
- replay all with safeguards;
- archive;
- quarantine;
- release from quarantine;
- purge; and
- export.

Replay must avoid accidental infinite redelivery loops. It should support rate limits, dry runs, attempt caps, and destination overrides.

---

# 23. Connection Manager

All external engine connections must be coordinated through a `ConnectionManager`.

Responsibilities include:

- connection creation;
- pooling where appropriate;
- lease or channel management;
- reconnect orchestration;
- heartbeat coordination;
- idle timeout;
- health status;
- topology or subscription restoration;
- graceful draining;
- shutdown ordering; and
- connection event emission.

An engine remains responsible for transport-specific connection primitives. The manager remains responsible for policy and orchestration.

Connections must not be created during import or ordinary object construction unless explicitly documented. Prefer initialization during `startup()`.

---

# 24. Request/Reply and RPC

The broker must support request/reply without requiring every engine to provide native RPC.

A portable implementation may use:

- correlation IDs;
- reply destinations;
- temporary or shared reply subscriptions;
- timeout management; and
- response routing.

Where an engine provides a native or optimized request/reply mechanism, the broker may use it through a capability-specific adapter.

RPC must support:

- timeout;
- cancellation;
- correlation validation;
- error envelopes;
- tracing propagation;
- reply cleanup; and
- bounded pending-request state.

Do not create one temporary consumer per request when a shared response dispatcher is more efficient and correct.

---

# 25. Event System

The framework should emit internal asynchronous events for operational visibility and extension hooks.

Examples include:

```text
startup_started
startup_completed
shutdown_started
shutdown_completed
engine_selected
connected
disconnected
connection_failed
published
publish_failed
received
handler_started
handler_completed
handler_failed
acknowledged
nacked
deferred
retried
backoff_started
dead_lettered
replayed
health_changed
circuit_opened
circuit_half_opened
circuit_closed
```

Listeners must be asynchronous, isolated, and configurable.

An observability listener failure must not normally fail message processing. The policy for critical listeners should be explicit.

Internal events are not a substitute for domain events and should use a separate namespace or type hierarchy.

---

# 26. Configuration System

Provide a typed, composable configuration system.

Supported providers should include:

- runtime overrides;
- environment variables;
- Django settings;
- Pydantic settings adapter;
- dictionaries or mappings;
- TOML;
- YAML;
- JSON; and
- framework defaults.

Recommended precedence, highest first:

```text
Runtime operation override
    ↓
Broker construction override
    ↓
Environment variables
    ↓
Framework integration settings
    ↓
Configuration file
    ↓
Defaults
```

Configuration merging must be deterministic and documented.

## 26.1 Typed Settings

Use typed settings models and validate early during startup.

Validation errors should include:

- the setting path;
- invalid value category without leaking secrets;
- expected type or constraint; and
- remediation guidance.

## 26.2 Secret Handling

Credentials must be represented using secret-aware types where possible and redacted from logs, exceptions, metrics, health reports, and `repr` output.

## 26.3 Example

```python
config = {
    "engine": "redis",
    "engines": {
        "redis": {
            "url": "redis://localhost:6379/0",
            "mode": "streams",
        }
    },
    "serialization": {"default": "json"},
    "reliability": {
        "publish_retry": {"max_attempts": 5},
    },
}

broker = Broker.from_config(config)
```

---

# 27. ASGI-Native Lifecycle

ASGI-native operation is mandatory.

## 27.1 `ASGILifecycleManager`

Provide an `ASGILifecycleManager` responsible for:

- loading plugin registries;
- resolving configuration;
- discovering serializers, middleware, and engines;
- selecting and validating engines;
- initializing connection pools;
- starting consumers;
- starting background services;
- registering health checks;
- coordinating application readiness;
- draining in-flight work;
- flushing pending publishes;
- persisting retry and DLQ state where required;
- cancelling background tasks; and
- closing connections cleanly.

## 27.2 Background Task Supervision

Long-lived services include:

- connection heartbeats;
- consumer loops;
- retry workers;
- delayed-delivery workers;
- dead-letter replay workers;
- health monitors;
- metrics exporters;
- outbox dispatchers; and
- subscription recovery tasks.

Use structured concurrency, preferably `asyncio.TaskGroup` or a compatible supervision abstraction.

Background tasks must have:

- ownership;
- names;
- cancellation propagation;
- restart policy;
- failure reporting;
- bounded restart loops; and
- deterministic shutdown.

No orphaned task may remain after shutdown.

## 27.3 Startup Sequence

Recommended startup sequence:

1. validate configuration;
2. load registries and plugins;
3. build serializers, router, middleware, and reliability services;
4. select engines and validate capabilities;
5. open connections;
6. restore or declare transport topology;
7. start consumers and background services;
8. run readiness checks; and
9. mark the broker ready.

Startup should be transactional where practical. If a later step fails, earlier resources must be unwound.

## 27.4 Shutdown Sequence

On shutdown:

1. mark the broker as draining;
2. reject new subscriptions and optionally new publishes according to policy;
3. stop accepting new deliveries;
4. allow in-flight handlers to complete within a grace period;
5. acknowledge completed deliveries;
6. persist retry or dead-letter state;
7. flush producer buffers and outbox items according to policy;
8. cancel remaining background tasks;
9. close consumers, channels, and connections; and
10. mark the broker stopped.

Forced shutdown after the grace period must be observable and must not falsely acknowledge incomplete work.

## 27.5 ASGI Integration

Provide helpers for ASGI lifespan integration.

Examples should cover:

- raw ASGI lifespan;
- Starlette/FastAPI lifespan context;
- Django ASGI startup strategy;
- Litestar; and
- Quart where practical.

The broker must expose:

```python
await broker.startup()
await broker.shutdown()
await broker.health()
```

---

# 28. Django Integration

Django support must be first-class but optional.

The core package must not import Django. Django-specific code should live under:

```text
janus_events.integrations.django
```

or in a dedicated `janus-events-django` distribution if packaging later requires stronger isolation.

## 28.1 Required Django Components

Provide:

- `AppConfig`;
- a Django configuration provider;
- system checks;
- startup/shutdown hooks appropriate to ASGI deployments;
- transaction-aware publishing;
- signals where useful;
- management commands;
- optional admin models and views;
- testing helpers; and
- clear deployment guidance for process models.

## 28.2 Settings Namespace

Use one top-level namespace:

```python
JANUS_EVENTS = {
    "ENGINE": "redis",
    "ENGINES": {
        "redis": {
            "URL": "redis://localhost:6379/0",
        }
    },
}
```

A legacy `BROKER` namespace may be supported only through an explicit compatibility adapter if required. New documentation should use `JANUS_EVENTS` consistently.

Environment overrides should remain available.

## 28.3 Transaction Integration

Publishing from within a database transaction should default to `transaction.on_commit()`.

Example:

```python
from janus_events.integrations.django import publish_on_commit

publish_on_commit(UserCreated(...))
```

The default must avoid publishing an event for a transaction that later rolls back.

Provide an explicit immediate-publish option for advanced cases.

## 28.4 Outbox Pattern

Design for a transactional outbox.

An initial implementation should include or prepare for:

- `OutboxMessage` storage;
- atomic creation with application state changes;
- asynchronous dispatch;
- locking or leasing;
- retry and dead-letter integration;
- idempotent publishing; and
- cleanup or retention.

## 28.5 Management Commands

Provide commands equivalent to:

```text
janus_events_health
janus_events_ping
janus_events_consumers
janus_events_dlq
janus_events_replay
janus_events_flush
janus_events_outbox
```

Command names should avoid generic collisions such as `broker_ping` unless namespaced by the Django app.

## 28.6 Optional Django Models

Potential optional models include:

- `BrokerMessage`;
- `DeadLetter`;
- `RetryRecord`;
- `ConsumerHeartbeat`;
- `BrokerConnection`;
- `OutboxMessage`; and
- `InboxMessage` or idempotency record.

Do not require database persistence for users who do not enable these features.

## 28.7 Multi-Process Warning

Django, Celery, ASGI workers, and management commands may run in separate processes. Never assume a Python singleton or in-memory broker object is shared across processes.

Each process must own its own runtime broker instance and coordinate through the selected external transport where cross-process delivery is required.

---

# 29. Other Framework Integrations

## 29.1 FastAPI and Starlette

Provide lifespan helpers and dependency providers.

## 29.2 Celery

Provide worker startup and shutdown integration without coupling the broker core to Celery.

Clarify that Celery is an application framework integration and may itself use a separate transport configuration.

## 29.3 CLI and Daemon Applications

Provide async context-manager examples and signal-aware shutdown helpers.

---

# 30. Observability

## 30.1 Structured Logging

Use structured log fields rather than interpolated payload dumps.

Useful fields include:

- message ID;
- event type and version;
- route;
- engine;
- destination;
- subscription;
- consumer;
- attempt;
- correlation ID;
- trace ID;
- duration;
- outcome; and
- error category.

Payload logging must be disabled by default or redacted through policy.

## 30.2 Metrics

Provide Prometheus-friendly instruments such as:

```text
janus_events_publish_total
janus_events_publish_failures_total
janus_events_consume_total
janus_events_handler_failures_total
janus_events_retry_total
janus_events_dead_letters_total
janus_events_ack_total
janus_events_nack_total
janus_events_connections
janus_events_consumer_lag
janus_events_queue_depth
janus_events_inflight
janus_events_publish_latency_seconds
janus_events_handler_duration_seconds
janus_events_health_status
```

Avoid high-cardinality labels such as raw message IDs.

## 30.3 Distributed Tracing

Support OpenTelemetry through an optional integration.

Every published message should propagate compatible trace context where configured.

Create spans for:

- publish;
- transport send;
- receive;
- deserialize;
- route;
- handler execution;
- acknowledge;
- retry; and
- dead-letter operations.

Trace propagation must coexist with correlation and causation IDs rather than replacing them.

## 30.4 Health

Every major component should expose health information.

A health report should include:

- lifecycle state;
- readiness and liveness;
- selected engine;
- connection state;
- latency;
- active consumers;
- queue depth where available;
- consumer lag where available;
- publish failures;
- retry counts;
- dead-letter counts;
- circuit-breaker state; and
- degraded capabilities.

Health endpoints must redact configuration secrets.

---

# 31. Security Requirements

The implementation must address:

- TLS configuration;
- authentication and credential rotation hooks;
- secret redaction;
- payload size limits;
- decompression limits;
- serializer allowlists;
- safe handling of untrusted payloads;
- optional message signing;
- optional encryption middleware;
- header and route validation;
- dead-letter data sensitivity;
- log redaction; and
- denial-of-service protection through quotas and backpressure.

Never deserialize untrusted pickle data.

Error messages must not expose credentials or full connection URLs.

---

# 32. Error Model

Create a coherent typed exception hierarchy.

Example:

```text
JanusEventsError
├── ConfigurationError
├── LifecycleError
├── RegistryError
│   ├── DuplicateRegistrationError
│   └── PluginLoadError
├── EngineError
│   ├── EngineUnavailableError
│   ├── ConnectionError
│   ├── PublishError
│   ├── ConsumeError
│   └── UnsupportedCapabilityError
├── SerializationError
├── RoutingError
├── MiddlewareError
├── AcknowledgementError
├── InvalidStateTransitionError
├── RetryExhaustedError
├── DeadLetterError
└── RequestTimeoutError
```

Errors should carry structured context without leaking secrets.

Retryability must be classified explicitly rather than inferred only from exception text.

---

# 33. Concurrency and Performance

The implementation should prioritize:

- efficient `asyncio` usage;
- low allocation overhead;
- minimal lock contention;
- bounded queues;
- connection and channel reuse;
- batch operations where supported;
- backpressure awareness;
- graceful degradation under load; and
- deterministic cancellation.

Avoid:

- creating one untracked task per delivery;
- blocking calls in the event loop;
- unbounded caches;
- broad process-wide locks;
- synchronous serializer work for extremely large payloads without offloading; and
- copying payload bytes unnecessarily.

Provide configurable handler concurrency and preserve ordering only within the scope the selected engine can guarantee.

---

# 34. Testing Strategy

Testing is a first-class deliverable.

## 34.1 Unit Tests

Cover:

- envelope creation;
- event registration;
- route matching;
- middleware order;
- state transitions;
- serializer behaviour;
- configuration merging;
- engine selection;
- capability validation;
- retry calculation;
- circuit-breaker transitions;
- acknowledgement idempotence; and
- dead-letter policy.

## 34.2 Contract Tests

Create a reusable engine conformance suite.

Every engine must pass applicable contract tests for:

- connect/disconnect;
- publish;
- consume;
- error translation;
- graceful shutdown;
- capability accuracy;
- acknowledgement semantics;
- reconnection hooks; and
- health checks.

Capabilities should determine which tests are required or skipped.

## 34.3 Integration Tests

Use real broker services through containers where practical.

Cover:

- Redis;
- RabbitMQ;
- Kafka;
- reconnect behaviour;
- process restart;
- consumer-group behaviour;
- retries;
- dead-lettering;
- trace propagation; and
- graceful shutdown.

## 34.4 Testing Utilities

Provide:

- memory engine;
- local engine;
- fake engine;
- mock publisher;
- mock subscriber;
- deterministic clock;
- deterministic retry scheduler;
- failure injection;
- broker override context manager;
- captured-message assertions; and
- test fixtures.

## 34.5 Property and State-Machine Tests

Use property-based tests where valuable, particularly for:

- message state transitions;
- wildcard routing;
- retry timing;
- envelope round trips; and
- configuration precedence.

## 34.6 Performance Tests

Include reproducible benchmarks for:

- local dispatch;
- memory queue throughput;
- serialization;
- middleware overhead;
- publish latency;
- consumer throughput; and
- high-concurrency behaviour.

Performance targets should be recorded after baseline measurements rather than invented prematurely.

---

# 35. Documentation Deliverables

Documentation must include:

- project overview;
- installation;
- quick start;
- core concepts;
- event declaration;
- envelope specification;
- publishing and subscribing;
- routing;
- middleware authoring;
- serializer authoring;
- engine authoring;
- capability model;
- acknowledgements;
- retries and dead-letter handling;
- request/reply;
- ASGI lifecycle integration;
- Django integration;
- FastAPI/Starlette integration;
- Celery integration;
- event versioning;
- observability;
- security guidance;
- testing guide;
- deployment patterns;
- migration and compatibility guide;
- API reference; and
- troubleshooting.

Examples must be executable or tested in CI where practical.

---

# 36. Coding and Quality Standards

Use:

- Python 3.12+;
- full type annotations;
- strict `mypy` or equivalent static checking;
- Ruff for linting and formatting unless another tool is deliberately selected;
- modern packaging through `pyproject.toml`;
- `pytest` and async test support;
- comprehensive docstrings for public APIs;
- dataclasses, enums, `Protocol`, `ABC`, generics, and immutable value objects where appropriate;
- dependency injection;
- semantic versioning; and
- automated CI checks.

Avoid:

- circular imports;
- catch-all exceptions without re-raising or classification;
- silent error swallowing;
- mutable default arguments;
- global event-loop ownership;
- transport imports in core modules;
- hidden threads;
- unnecessary inheritance hierarchies; and
- duplicate implementations of the same concept.

Code should be organized around cohesive modules, not oversized “manager” classes that accumulate unrelated responsibilities.

---

# 37. Implementation Phases

The agent should build in vertical, testable increments.

## Phase 0: Repository and Quality Foundation

Deliver:

- project metadata and packaging;
- `src` layout;
- linting, formatting, typing, and test configuration;
- CI workflow;
- exception hierarchy;
- initial documentation structure; and
- architecture decision records for major choices.

## Phase 1: Core Domain and Public API

Deliver:

- `Broker` façade skeleton;
- event and message protocols;
- typed envelope;
- delivery context;
- lifecycle state model;
- route abstractions;
- serializer registry;
- engine registry; and
- configuration model.

Include unit tests before adding network engines.

## Phase 2: Local and Memory Runtime

Deliver:

- local engine;
- memory engine;
- router;
- outbound and inbound middleware;
- subscription management;
- basic acknowledgements;
- bounded consumer concurrency;
- startup and shutdown; and
- deterministic testing utilities.

At the end of this phase, the public API must be usable without an external broker.

## Phase 3: Reliability Services

Deliver:

- retry manager;
- backoff strategies;
- circuit breaker;
- dead-letter manager and an initial store;
- delivery state enforcement;
- handler failure classification;
- backpressure controls; and
- internal operational events.

## Phase 4: Redis Engine

Deliver:

- optional Redis package;
- engine registration through entry points;
- explicit Pub/Sub and/or Streams modes;
- connection and consumer recovery;
- capability declaration;
- contract tests; and
- container-backed integration tests.

## Phase 5: RabbitMQ and Kafka Engines

Deliver separate optional packages with accurate capability declarations and transport-specific integration tests.

Do not force a false common denominator. Preserve semantic differences through adapters and documented capabilities.

## Phase 6: Observability and ASGI Integration

Deliver:

- health model;
- metrics hooks;
- OpenTelemetry integration;
- structured logging middleware;
- ASGI lifecycle manager;
- structured task supervision; and
- graceful drain behaviour.

## Phase 7: Django Integration

Deliver:

- Django settings provider;
- `AppConfig` and system checks;
- transaction-on-commit publishing;
- management commands;
- optional persistence models;
- admin integration where enabled;
- outbox foundation; and
- Django test helpers.

## Phase 8: Request/Reply, Versioning, and Advanced Features

Deliver:

- portable request/reply;
- event version registry and upcasting extension points;
- batch operations;
- delayed delivery abstraction;
- inbox/idempotency store; and
- richer replay tooling.

## Phase 9: Documentation, Hardening, and Release

Deliver:

- complete documentation;
- examples;
- security review;
- benchmark report;
- compatibility matrix;
- release automation;
- changelog; and
- migration guidance.

---

# 38. Definition of Done

A feature is not complete until it has:

- a typed public or internal contract;
- implementation separated from unrelated concerns;
- unit tests;
- contract or integration tests when applicable;
- documentation;
- error handling;
- lifecycle handling;
- observability hooks;
- security consideration; and
- compatibility review.

An engine is not complete until:

- it is optional-dependency safe;
- it registers through the plugin mechanism;
- its capabilities are accurate;
- it passes the engine conformance suite;
- it has real-service integration tests;
- it shuts down without leaked tasks or connections; and
- its transport-specific limitations are documented.

---

# 39. Acceptance Criteria

The project is successful when all of the following are true:

1. Application code can publish and subscribe through `Broker` without importing a transport library.
2. The same core test application runs against local, memory, Redis, RabbitMQ, and Kafka configurations with only configuration changes.
3. Third parties can add an engine through an entry point without editing the core package.
4. Typed events are converted to a versioned envelope and reconstructed safely.
5. Wildcard and namespace routing work even when the transport lacks native support.
6. Middleware order is deterministic and separately configurable for inbound and outbound flows.
7. Acknowledgement operations use one framework API and translate accurately per engine.
8. Retries, backoff, circuit breaking, and dead-letter handling are centralized rather than duplicated in engines.
9. Requested unsupported guarantees fail explicitly instead of being silently ignored.
10. Startup and shutdown are idempotent and leave no orphaned tasks or open connections.
11. The framework integrates with ASGI lifespan events.
12. Django publishing can be deferred until database transaction commit.
13. Health, metrics, logs, and tracing expose useful operational information without leaking secrets.
14. Optional engine dependencies are not installed with the core package.
15. Unit, contract, integration, and lifecycle tests pass in CI.
16. Documentation is sufficient for users to build an application and for third parties to author an engine or middleware plugin.

---

# 40. Future Roadmap

Design for, but do not necessarily implement in the first release:

- NATS engine;
- MQTT engine;
- Apache Pulsar engine;
- Amazon SQS/SNS engine;
- Azure Service Bus engine;
- Google Pub/Sub engine;
- ZeroMQ engine;
- full transactional outbox and inbox patterns;
- scheduled and delayed messages;
- message priorities;
- schema registry integrations;
- message compression and encryption plugins;
- multi-engine routing;
- automatic failover with explicit consistency semantics;
- multi-region deployments;
- consumer autoscaling signals;
- web dashboard;
- administrative CLI;
- AsyncAPI generation;
- OpenAPI management endpoints;
- GraphQL management API; and
- hosted control-plane integrations.

Future features must preserve the responsibility boundaries and capability-based design defined in this document.

---

# 41. Agent Execution Rules

An implementation agent working from this file must follow these rules:

1. Inspect the existing repository before changing architecture or naming.
2. Preserve working public APIs unless an explicit migration is documented.
3. Do not create both `Broker` and `EventBus` as independent runtimes.
4. Use `Broker` as the canonical façade and treat `EventBus` only as an alias or protocol when needed.
5. Keep all Janus-specific events and handlers outside the reusable core.
6. Keep Django and other framework imports outside core modules.
7. Do not open connections or start tasks during import.
8. Do not hardcode engine mappings in the broker façade.
9. Do not let engines own independent retry, circuit-breaker, or dead-letter policy.
10. Do not claim unsupported reliability guarantees.
11. Prefer capability checks over engine-class checks.
12. Add or update tests with every behavioural change.
13. Use small, reviewable commits organized by coherent feature.
14. Document significant trade-offs through architecture decision records.
15. Avoid broad rewrites when an incremental, compatible change is possible.
16. Run formatting, linting, typing, unit tests, and applicable integration tests before considering work complete.
17. Report incomplete portions, assumptions, and unavailable external services honestly.
18. Never leave placeholder code presented as production-ready.
19. Never commit secrets, generated credentials, broker data, or local environment files.
20. Treat this `AGENTS.md` as the governing specification unless the project owner explicitly supersedes it.

---

# 42. Final Architectural Position

`janus-events` is one unified application and package ecosystem with three clearly separated layers:

1. **Domain event and message API** — typed events, envelopes, publishers, subscribers, handlers, and routing.
2. **Broker orchestration core** — middleware, serialization, lifecycle, acknowledgements, retries, dead letters, configuration, and observability.
3. **Transport engine plugins** — Redis, RabbitMQ, Kafka, local, memory, and future backends.

The `Broker` façade is the single entry point joining these layers.

This design must produce a framework that is easy to use in a small application, dependable in a production ASGI or Django deployment, and extensible enough for third-party transports and enterprise messaging requirements without modifying the core.
