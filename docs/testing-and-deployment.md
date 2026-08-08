# Testing and deployment

The fastest tests should exercise application behavior through a real `Broker` using `memory` or
`local`. Narrow unit tests can inject `MockPublisher` or `FakeEngine`. External engine behavior
belongs in opt-in integration and conformance suites backed by real services.

## Install the development toolchain

```bash
python -m pip install -e ".[dev]"
ruff format .
ruff check .
mypy src/pyev
pytest
```

This repository uses a `src` layout. An editable install is recommended. When running directly from
a source checkout without installing, set the platform-equivalent of `PYTHONPATH=src` for the test
process.

## Test through the broker

```python
import asyncio

import pytest

from pyev import Broker


@pytest.mark.asyncio
async def test_order_handler() -> None:
    received = asyncio.Event()

    async def handle(delivery) -> None:
        assert delivery.message.order_id == "A-42"
        received.set()

    async with Broker.from_config({"engine": "memory"}) as broker:
        await broker.subscribe("orders.*", handle)
        await broker.publish(OrderCreated("A-42"))
        await asyncio.wait_for(received.wait(), timeout=1)
```

Use `local` when handler execution should finish inside `publish()`. Use `memory` when a bounded
producer/consumer queue and owned workers are relevant. Neither validates inter-process behavior,
network reconnects, or a remote broker's real delivery semantics.

## Deterministic utilities

`pyev.testing` provides:

- `DeterministicClock`, with monotonic and UTC time plus auto-advance or manually woken sleeps;
- `DeterministicRetryScheduler`, which records requested delays;
- `FailureInjector`, which queues explicit failures by operation name;
- `FakeEngine` and `FakeConsumer`, conforming to the minimum engine SPI;
- `FakeAcknowledgementAdapter`, recording framework acknowledgement calls;
- `MockPublisher`, capturing application messages;
- `assert_published()`, with route, destination, type, and count matching;
- `broker_override()`, a context-local override rather than process-global mutation; and
- `eventually()`, a bounded async predicate assertion.

```python
from pyev.reliability import FixedBackoff, RetryManager, RetryPolicy
from pyev.testing import DeterministicRetryScheduler

scheduler = DeterministicRetryScheduler()
manager = RetryManager(sleep=scheduler.sleep, clock=scheduler.clock)

await manager.run(
    operation,
    RetryPolicy(max_attempts=3, backoff=FixedBackoff(2)),
)
assert scheduler.delays == [2, 2]
```

Randomized backoff strategies accept an injected `random_source`. Prefer that over seeding or
monkeypatching process-global random state.

## Engine conformance and integration tests

Every third-party engine should cover:

1. side-effect-free availability detection;
2. idempotent connect and disconnect;
3. publish before/after connection behavior;
4. consumer creation, pause, resume, close, and shutdown drain;
5. every advertised acknowledgement operation;
6. unsupported operations raising `UnsupportedCapabilityError`;
7. truthful capability attributes;
8. health without credentials;
9. reconnect and topology restoration; and
10. no remaining tasks or sockets after shutdown.

Use real Redis, RabbitMQ, and Kafka services for integration tests. Mocking a client call is not
evidence of broker semantics, pending-entry recovery, AMQP confirmation, or Kafka rebalancing.
External-service tests should be marked `integration` and skipped when their service/configuration
is absent. The core unit suite does not require a container runtime.

## Deployment topology

Choose the engine from process topology, not convenience:

| Topology | Suitable engine |
| --- | --- |
| One CLI/desktop process | `local` or `memory` |
| One ASGI worker and process-local consumers | `memory`, if losing queued data on exit is acceptable |
| Multiple web workers | Redis Streams, RabbitMQ, Kafka, or a third-party external engine |
| Separate web and worker services | External engine only |
| Durable replay or compliance retention | External engine plus a durable `DeadLetterStore` |
| Atomic database mutation plus publish | Application-database `OutboxStore` implementation |

Each process constructs and starts its own broker. A module-level broker reference is process-local.
If every web worker registers the same durable subscription, the selected engine may treat them as
competing consumers. Separate publisher-only and consumer process roles when that is clearer.

## Startup, readiness, and shutdown

Use the framework's lifespan integration or explicit context management. Startup validates
configuration, discovers plugins lazily, connects the engine, restores registered topology, and
only then makes the broker ready. Point orchestration readiness checks at `await broker.health()`.

During shutdown:

1. stop accepting new work;
2. stop the heartbeat and incoming deliveries;
3. wait for in-flight operation leases within the grace period;
4. cancel owned background tasks;
5. close consumers and transport resources; and
6. mark the broker stopped.

Set the process termination grace period longer than pyev's shutdown grace period. A forced exit can
leave an uncommitted delivery eligible for redelivery; do not mark that delivery acknowledged.

## Reliability deployment checklist

- Keep `allow_memory_fallback` false in production unless process-local degradation is explicitly
  acceptable.
- Align retry elapsed-time limits with request deadlines and orchestrator termination windows.
- Use retry budgets so a broad outage cannot multiply load without bound.
- Configure durable queues/streams/topics and consumer identities deliberately.
- Make at-least-once handlers idempotent and persist deduplication records durably when required.
- Use a durable dead-letter store; the memory store disappears with its process.
- Rate-limit administrative replay and preview it with `dry_run=True`.
- Monitor queue depth, lag, retries, dead letters, circuit state, and forced shutdown.
- Load-test bounded queue capacity and overflow behavior rather than increasing limits blindly.
- Verify TLS, authentication, authorization, credential rotation, and broker-side quotas.

## Performance tests

Record baselines for local dispatch, memory queue throughput, serialization, middleware overhead,
publish latency, and handler concurrency on controlled hardware. Do not turn an invented throughput
number into a release promise. Benchmark with realistic payload sizes and report queue capacity,
concurrency, serializer, Python version, and engine configuration alongside results.

The included publish-throughput baseline emits machine-readable JSON:

```bash
python scripts/benchmark.py --engine memory --messages 10000 --payload-bytes 256 --rounds 3
```

Run it from an editable development install. Keep results with the hardware and configuration
metadata it prints; comparisons across unlike machines are not meaningful.
