# Core concepts and architecture

## Broker

`Broker` is the only high-level runtime. It composes registries, router, middleware, serializers,
reliability services, lifecycle services, observability, and one selected engine. `EventBus` is not
a second implementation.

Construction is side-effect-free. `startup()` performs discovery, validates the selected engine,
connects it, and restores subscriptions. `shutdown()` drains consumers and owns deterministic task
cancellation. Both calls are idempotent.

## Message, event, envelope, and delivery

- A **message** is a typed application value.
- An **event** is a message declared with a stable name and schema version via `@event`.
- An **envelope** is immutable metadata plus a JSON-safe payload. Every engine sends envelope bytes,
  never an arbitrary application object.
- A **delivery** is mutable processing state around a received immutable envelope. It exposes the
  reconstructed message and framework acknowledgement methods.

Envelope format version and event schema version are separate. Rolling application deployments can
therefore upcast an event without changing the transport contract, or evolve the transport contract
without silently changing a domain schema.

## Logical routes and physical destinations

The public route `orders.created` is not necessarily the physical Kafka topic, AMQP routing key, or
Redis stream. `Route`, `Destination`, and `HandlerPattern` are separate values. Router rules perform
explicit translation, and engines see only the resulting destination and bytes.

## Engine SPI

An engine owns transport I/O:

```python
class BaseEngine:
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def publish(self, destination, payload, context): ...
    async def create_consumer(self, subscription, callback): ...
    async def healthcheck(self): ...
```

It does not own retry policy, the circuit breaker, dead-letter policy, message reconstruction,
handler routing, or application middleware. Optional protocols add native batch or request/reply
optimizations without bloating the minimum SPI.

## Capability model

`CapabilitySet` contains feature names and optional attributes. Attributes describe scope—Kafka
ordering is per partition, for example—so a boolean cannot overstate semantics.

```python
from pymq import Capability

if broker.capabilities.supports(Capability.CONSUMER_GROUPS):
    ...

broker.capabilities.require(
    Capability.AT_LEAST_ONCE,
    operation="durable order processing",
)
```

Portable framework fallbacks are used only when they preserve the requested semantics. Otherwise,
`UnsupportedCapabilityError` identifies the missing feature and operation.

## Outbound flow

1. Normalize the typed message.
2. Create an immutable envelope.
3. Resolve its logical route and physical destination.
4. Run outbound middleware.
5. Validate requested capabilities.
6. Execute centrally configured retries through the circuit breaker.
7. Ask the selected engine to publish bytes.
8. Emit low-cardinality operational signals.

## Inbound flow

1. Normalize incoming transport metadata.
2. Decode and validate the envelope using its serializer metadata.
3. Reconstruct a registered event version (or expose the safe mapping for an unknown low-level
   message).
4. Create a `Delivery` with an engine acknowledgement adapter.
5. Run inbound middleware and the matched handler.
6. On success, auto-ack when configured; on failure, centrally retry and then reject/dead-letter.

Engines never expose their native message object to normal handlers. `Delivery.transport_metadata`
is the controlled diagnostic escape hatch.

