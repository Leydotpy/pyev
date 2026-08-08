# Acknowledgements, retries, and dead letters

Reliability policy belongs to `pyev`, not to an engine. Engines translate a small set of
transport primitives; the broker decides whether to retry, reject, requeue, or dead-letter a
delivery. A configured transport capability is still the upper bound: framework orchestration
cannot manufacture durability from Redis Pub/Sub or cross-process delivery from an in-memory
queue.

## Acknowledgement modes

Handlers acknowledge through `Delivery`, never through an AMQP message, Kafka record, or Redis
entry:

```python
await delivery.ack()
await delivery.nack(requeue=True)
await delivery.reject()
await delivery.requeue()
await delivery.defer(delay=30)
await delivery.touch(extension=60)
```

`AcknowledgementMode` controls who makes the terminal decision:

| Mode | Behavior |
| --- | --- |
| `AUTO` | The broker acknowledges after successful handler completion. |
| `MANUAL` | The handler must choose a delivery operation. |
| `BATCH` | Uses the same framework state rules and is available for engines that can safely support the configured batch behavior. |
| `NONE` | The transport is acknowledged before handler completion; application failure cannot restore the message. |

Acknowledgement methods are idempotent when the same terminal action is repeated. Conflicting
terminal transitions raise `InvalidStateTransitionError`. An operation such as `touch()` or native
delay that cannot be implemented safely raises `UnsupportedCapabilityError`; it never becomes a
silent no-op.

The adapters retain transport semantics:

- RabbitMQ maps ack, nack, reject, and requeue to AMQP delivery operations.
- Kafka ack commits the next offset and requeue seeks the assigned partition back to the record.
- Redis Streams acknowledges consumer-group entries; its requeue implementation writes a new
  entry before acknowledging the old one.
- Redis Pub/Sub cannot requeue a lost publication.
- Local and memory engines model the same delivery state in one process.

## Retry policy

`RetryManager` runs any zero-argument async operation under a `RetryPolicy`:

```python
from pyev.reliability import FixedBackoff, RetryContext, RetryManager, RetryPolicy

policy = RetryPolicy(
    name="billing-publish",
    max_attempts=5,
    max_elapsed_time=20,
    attempt_timeout=3,
    backoff=FixedBackoff(0.5),
)

result = await RetryManager().run(
    send_once,
    policy,
    context=RetryContext("publish", {"engine": "rabbitmq"}),
)
```

`max_attempts` includes the initial call. Cancellation propagates by default. Terminally classified
exceptions are re-raised unchanged; a retryable sequence that reaches an attempt, elapsed-time, or
budget limit raises `RetryExhaustedError` with sanitized structured context and the last error as
its cause.

Built-in backoff strategies are `FixedBackoff`, `LinearBackoff`, `ExponentialBackoff`,
`ExponentialFullJitterBackoff`, `EqualJitterBackoff`, and `DecorrelatedJitterBackoff`. Attempts are
one-based. Randomized strategies accept a `random_source`, allowing deterministic tests. Full
jitter is the default network policy.

Use `TypeExceptionClassifier` or `CallableExceptionClassifier` to classify by exception type or
explicit application logic. Do not classify by matching human-readable error text. `RetryBudget`
is a thread-safe sliding-window token budget shared by multiple operations.

Broker configuration accepts policy mappings under `reliability`:

```python
config = {
    "engine": "rabbitmq",
    "reliability": {
        "publish_retry": {
            "max_attempts": 5,
            "backoff": {
                "strategy": "full_jitter",
                "initial": 0.2,
                "maximum": 10,
            },
        },
        "handler_retry": {
            "max_attempts": 3,
            "backoff": {"strategy": "exponential"},
        },
    },
}
```

See `BrokerConfig` validation for accepted values in the installed version. Application handlers
should also be idempotent: at-least-once delivery always permits duplicates around crashes.

## Circuit breaker

`CircuitBreaker` has `CLOSED`, `OPEN`, and `HALF_OPEN` states. It supports consecutive failure
thresholds, an optional sliding failure-rate window, a cooldown, bounded half-open probes, a
success threshold, excluded exception types, manual `trip()`, and manual `reset()`.

```python
from pyev.reliability import CircuitBreaker, CircuitBreakerConfig

breaker = CircuitBreaker(
    "payments-api",
    CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=30,
        half_open_max_calls=2,
    ),
)

response = await breaker.call(call_payments)
```

An open circuit raises `CircuitOpenError` with a `retry_after` value in its structured context.
State transitions emit isolated internal operational events and appear in broker health.

## Dead-letter capture

`DeadLetterManager` accepts an `Envelope` (or original bytes), the terminal exception, and an
optional `DeadLetterContext`. Records preserve routing, retry, engine, consumer, serializer, and
schema metadata. Credential-like headers, URLs, error messages, and tracebacks are redacted before
persistence. Decoded payload persistence is disabled by default.

```python
from pyev.deadletter import DeadLetterContext, DeadLetterManager, MemoryDeadLetterStore

dead_letters = DeadLetterManager(MemoryDeadLetterStore())
record = await dead_letters.dead_letter(
    envelope,
    error,
    context=DeadLetterContext(
        route="orders.created",
        destination="company-orders",
        engine="kafka",
        consumer="billing-v2",
    ),
)
```

`MemoryDeadLetterStore` is bounded and process-local. It is appropriate for tests and local
development, not durable production retention. `DeadLetterStore` is the persistence protocol for
database, object-storage, or transport-native adapters. The core release does not include those
durable adapters.

Administrative operations include filter/inspect, quarantine/release, archive, export, retention,
and purge. Unfiltered or multi-record purge requires explicit confirmation through the manager.

## Safe replay

`ReplayManager` is separate from capture and takes an injected publisher callable. Replay supports
one record, an explicit selection, or a filtered active set. Safeguards include:

- dry runs;
- explicit confirmation for multiple records;
- a maximum replay batch;
- a per-record replay-attempt cap;
- optional rate limiting;
- destination override;
- quarantine after repeated replay failure; and
- skipping records already marked replayed.

```python
async def republish(envelope: object, destination: str | None) -> object:
    return await broker.publish(envelope, route=destination)

replay = ReplayManager(store, republish)
preview = await replay.replay_all(dry_run=True)
results = await replay.replay_selected(ids, confirm=True)
```

Adapt the publisher to the application API: a stored `Envelope` is already normalized, while
`Broker.publish()` normally accepts a message. Never automatically loop every failed replay back
into the same dead-letter queue without an attempt cap.

## Idempotency and outbox foundation

`IdempotencyStore` and `MemoryIdempotencyStore` provide atomic claim, complete, release, expiry,
and bounded capacity for inbox-style deduplication. `OutboxStore`, `MemoryOutboxStore`, and
`OutboxDispatcher` provide atomic leasing, ownership checks, retry availability, terminal failure,
and published-record cleanup.

These are composable services; the broker does not automatically make an application database
transactional. A production transactional outbox must persist `OutboxMessage` in the same database
transaction as application state and supply a durable `OutboxStore` implementation.

## What guarantees mean

- **At-most-once** can lose a message but does not intentionally redeliver it.
- **At-least-once** can redeliver; handlers need idempotency.
- **Publisher confirms** confirm the transport accepted a send, not that a handler committed its
  business transaction.
- **Producer idempotence** does not make end-to-end handler execution exactly once.
- **Exactly once** is not advertised by the built-in end-to-end pipeline. It requires coordinated
  application state, inbox/outbox records, transport support, and carefully defined boundaries.

Always validate `broker.capabilities` for the selected engine and operation rather than branching
on an engine class.
