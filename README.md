# pyev

`pyev` is a typed, asynchronous, transport-agnostic message broker for Python 3.12+.
Applications publish domain messages through one `Broker` API; pluggable engines decide how the
serialized envelopes travel. The core package contains no application- or framework-specific
business logic.

```python
import asyncio
from dataclasses import dataclass

from pymq import Broker, Delivery, event


@event("orders.created", version=1)
@dataclass(frozen=True, slots=True)
class OrderCreated:
    order_id: str
    total: int


async def main() -> None:
    received = asyncio.Event()

    async def handle(delivery: Delivery[OrderCreated]) -> None:
        print(delivery.message.order_id)
        received.set()

    async with Broker.from_config({"engine": "memory"}) as broker:
        await broker.subscribe("orders.*", handle)
        await broker.publish(OrderCreated(order_id="A-100", total=4200))
        await asyncio.wait_for(received.wait(), timeout=1)


asyncio.run(main())
```

## Why pyev?

- One stable, fully asynchronous façade for publishing, subscriptions, batches, and request/reply.
- Immutable, independently versioned envelopes and strongly typed event reconstruction.
- Exact, namespace, wildcard, type, and header routing independent of a transport's native syntax.
- Deterministic inbound and outbound middleware pipelines.
- Framework-level acknowledgement state, retries, jittered backoff, circuit breaking, dead letters,
  idempotency, and bounded backpressure.
- Built-in deterministic `local` and queued `memory` engines.
- Optional Redis Pub/Sub/Streams, RabbitMQ, and Kafka engines with truthful capabilities.
- Lazy entry-point discovery under `pyev.engines`; imports never open a connection or start a task.
- ASGI lifespan, FastAPI/Starlette, Celery, and optional Django helpers.
- Structured health, metrics, tracing hooks, and deterministic testing utilities.

## Install

```bash
python -m pip install pyev
```

External clients are optional:

```bash
python -m pip install "pyev[redis]"
python -m pip install "pyev[rabbitmq]"
python -m pip install "pyev[kafka]"
python -m pip install "pyev[django,otel]"
```

Dedicated plugin distributions (`pyev-redis`, `pyev-rabbitmq`, and `pyev-kafka`) expose the same
engine implementations through the same entry-point group.

## Reliability is explicit

`pyev` does not pretend Redis Pub/Sub, Redis Streams, AMQP acknowledgements, and Kafka offset
commits are equivalent. Engines publish a structured capability set. An operation requesting an
unsupported guarantee raises `UnsupportedCapabilityError`; it is never silently weakened to a
less reliable mode.

The built-in engines are process-local. Use an external engine whenever independent processes must
communicate. Redis Streams, RabbitMQ, and Kafka require their respective services; integration
tests for those transports are opt-in.

## Documentation

- [Getting started](docs/getting-started.md)
- [Core concepts and architecture](docs/core-concepts.md)
- [Envelope specification and event versioning](docs/envelopes.md)
- [Routing and middleware](docs/routing-and-middleware.md)
- [Acknowledgements, retries, and dead letters](docs/reliability.md)
- [Engine configuration and plugin authoring](docs/engines.md)
- [ASGI, Django, FastAPI/Starlette, and Celery](docs/integrations.md)
- [Observability and security](docs/operations.md)
- [Testing and deployment](docs/testing-and-deployment.md)
- [API reference](docs/api-reference.md)
- [Troubleshooting and migration](docs/troubleshooting.md)

## Development

```bash
python -m pip install -e ".[dev]"
ruff format .
ruff check .
mypy src/pyev
pytest
```

The architecture decisions in [`docs/adr`](docs/adr/README.md) explain the important responsibility
boundaries. See [AGENTS.md](AGENTS.md) for the complete build contract; project-facing names in that
source brief are intentionally implemented as `pyev`.

## License

MIT
