# Getting started

## Requirements and installation

`pyev` supports Python 3.12 and later. The core has no runtime third-party dependency.

```bash
python -m pip install pyev
```

Choose an optional engine extra only when it is used:

```bash
python -m pip install "pyev[redis]"
```

## Declare an event

The `@event` decorator gives a model a stable wire name and application schema version. Dataclasses
are the smallest dependency-free option; attrs and Pydantic-compatible models are adapted too.

```python
from dataclasses import dataclass
from broka import event


@event("accounts.user.invited", version=1)
@dataclass(frozen=True, slots=True)
class UserInvited:
  user_id: str
  email: str
```

Names are routing identities. Renaming the Python class does not have to break consumers as long as
the event name and version remain stable.

## Subscribe and publish

Handlers are asynchronous and receive a framework `Delivery`, never a Redis, AMQP, or Kafka object.

```python
from broka import Broker, Delivery


async def send_invitation(delivery: Delivery[UserInvited]) -> None:
  event = delivery.message
  await invitation_service.send(event.email)


async with Broker.from_config({"engine": "memory"}) as broker:
  subscription = await broker.subscribe("accounts.user.*", send_invitation)
  result = await broker.publish(UserInvited("u-42", "person@example.com"))
```

The broker's context manager calls idempotent `startup()` and `shutdown()`. Explicit lifecycle is
equivalent:

```python
broker = Broker()
await broker.startup()
try:
    await broker.publish(...)
finally:
    await broker.shutdown()
```

## Configuration

Construction accepts a typed `BrokerConfig` or mapping:

```python
config = {
    "engine": "redis",
    "engines": {
        "redis": {
            "url": "redis://localhost:6379/0",
            "mode": "streams",
            "group": "billing",
        }
    },
    "serialization": {"default": "json"},
    "reliability": {"publish_retry": {"max_attempts": 5}},
    "source": "billing-api",
}

broker = Broker.from_config(config)
```

Configuration precedence is defaults, files, framework settings, `PYEV_` environment variables,
construction overrides, then operation overrides. Secrets are redacted in `repr`, errors, health,
and default mapping exports.

## Choosing the built-in engine

- `local`: handler invocation happens in the publisher call and is deterministic. Best for direct
  integration and small command-line programs.
- `memory`: bounded queues and owned consumer workers model asynchronous producer/consumer behavior.
  Best for normal tests and single-process services.

Neither crosses a process boundary. In a multi-worker deployment, select Redis Streams, RabbitMQ,
Kafka, or a third-party engine.

