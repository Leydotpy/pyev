# Engine configuration and plugin authoring

An engine receives a physical destination, serialized envelope bytes, and a small publish context.
It owns transport I/O only. Routing, serializers, retry policy, circuit breaking, dead-letter
policy, handler invocation, and lifecycle orchestration stay in the core broker.

## Selection and optional dependencies

The default configuration selects `memory`. An injected engine or explicitly configured engine
takes precedence. When automatic selection is requested, the registry orders available engines by
priority. Memory fallback is disabled unless `allow_memory_fallback` is explicitly true; an
unavailable external engine never silently degrades to memory.

```python
broker = Broker.from_config(
    {
        "engine": "redis",
        "engines": {
            "redis": {
                "url": "redis://localhost:6379/0",
                "mode": "streams",
            }
        },
    }
)
```

Client imports are lazy. Importing `pyev` without `redis`, `aio-pika`, or `aiokafka` installed is
safe. Selecting that unavailable engine produces an actionable error during startup.

## Capability matrix

This table summarizes declarations in the current implementations. Configuration can add or remove
conditional capabilities; inspect `broker.capabilities` at runtime for the authoritative set.

| Engine | Process scope / delivery | Ordering | Durable / groups | Notable limitations |
| --- | --- | --- | --- | --- |
| `local` | In-process; advertises at-most-once and modeled at-least-once behavior | Publisher call | No cross-process durability | Direct dispatch only. |
| `memory` | In-process bounded queues; modeled at-least-once | Consumer worker | Competing consumers in one process | Lost when the process exits. |
| Redis `pubsub` | External, at-most-once | Not promised | No durable subscription | No requeue, pending recovery, or native delay. |
| Redis `streams` | External, at-least-once via consumer groups and pending reclaim | Stream entry ID | Durable streams, groups, competing consumers | `touch()` visibility extension is unsupported. |
| `rabbitmq` | External AMQP at-least-once | Per queue | Durable queues when configured; competing consumers | Native DLX only when `dead_letter_exchange` is set; no visibility-timeout `touch()`. |
| `kafka` | External at-least-once through manual offset commits | Per partition | Durable topics, groups, competing consumers | Ack means offset commit, not AMQP ack; no visibility-timeout `touch()`. |

All current engines expose portable request/reply through the broker. This is not a claim of a
native RPC primitive. Header and wildcard matching can also be performed by the framework when a
transport does not provide equivalent native routing.

## Local and memory

```python
{"engine": "local"}
```

`local` invokes matching consumers during the publish flow. It is useful for deterministic direct
integration, but slow handlers add publish latency.

```python
{
    "engine": "memory",
    "engines": {
        "memory": {
            "overflow_policy": "block",  # block, reject, or drop-newest
            "drain_timeout": 10,
        }
    },
}
```

`memory` uses bounded asyncio queues with subscription-level capacity and concurrency. Choose an
explicit overflow policy. Neither built-in engine crosses a process boundary.

## Redis

Install `pyev[redis]`. The two modes have deliberately different capabilities.

```python
{
    "engine": "redis",
    "engines": {
        "redis": {
            "url": "redis://localhost:6379/0",
            "mode": "streams",
            "group": "billing",
            "consumer_name": "billing-1",
            "max_length": 100000,
            "claim_idle_ms": 60000,
            "claim_interval": 30,
        }
    },
}
```

Streams uses `XREADGROUP`, `XACK`, and `XAUTOCLAIM`. A requeue writes a replacement entry then
acknowledges the original, so duplicates remain possible around failure boundaries. Pub/Sub uses
pattern subscriptions and cannot recover messages published while a consumer is absent.

## RabbitMQ

Install `pyev[rabbitmq]`.

```python
{
    "engine": "rabbitmq",
    "engines": {
        "rabbitmq": {
            "url": "amqps://user:password@rabbit.example/vhost",
            "exchange": "company-events",
            "queue": "billing-orders",
            "durable": True,
            "publisher_confirms": True,
            "mandatory": True,
            "dead_letter_exchange": "company-events.dlx",
        }
    },
}
```

The engine declares a topic exchange and binds a queue for each consumer. Subscription capacity is
used as AMQP prefetch. Publisher confirms and native dead-letter capability are advertised only
when enabled. The current adapter uses `aio-pika.connect`; applications requiring a specialized
robust connection strategy can provide a plugin engine behind the same SPI.

## Kafka

Install `pyev[kafka]`.

```python
{
    "engine": "kafka",
    "engines": {
        "kafka": {
            "bootstrap_servers": "kafka-1:9092,kafka-2:9092",
            "group_id": "billing",
            "auto_offset_reset": "earliest",
            "enable_idempotence": True,
        }
    },
}
```

Logical destinations become topics. `partition_key` or `ordering_key` becomes the producer key.
Wildcard subscriptions are converted to a Kafka topic regex. Consumers disable auto-commit; ack
commits `offset + 1`, while requeue seeks the currently assigned partition back to the record.
Producer idempotence and a `transactional_id` do not by themselves provide exactly-once business
processing, and the current broker does not expose a coordinated Kafka transaction API.

## Author an engine plugin

Implement the minimum `BaseEngine` surface and publish a lazy entry point:

```python
from pymq.capabilities import Capability, CapabilitySet
from pymq.engines.base import BaseEngine


class NatsEngine(BaseEngine):
    name = "nats"
    priority = 35

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet.of(Capability.PUBLISH_SUBSCRIBE)

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def publish(self, destination, payload, context): ...

    async def create_consumer(self, subscription, callback): ...

    async def healthcheck(self): ...
```

```toml
[project.entry-points."pyev.engines"]
nats = "pyev_nats:NatsEngine"
```

`is_available()` must be side-effect-free and must not open a socket. Keep optional client imports
inside runtime methods. A consumer callback receives `EngineIncomingMessage` and a framework
acknowledgement adapter, never a native object. Declare only capabilities the concrete mode and
configuration actually provide. New engines should run the same connect, publish, consume,
acknowledgement, health, and graceful-shutdown contract tests as built-ins.
