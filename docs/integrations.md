# ASGI, Django, FastAPI/Starlette, and Celery

Framework integrations depend on the core, never the reverse. They are lifecycle adapters around
the same `Broker`; they do not create a competing event bus. Every operating-system process owns
its own broker instance. An in-memory Python singleton is not shared by ASGI workers, Celery
workers, Django management commands, or a separate web process.

## Framework-neutral ASGI lifespan

`broker_lifespan()` is an async context manager accepted by frameworks with a lifespan hook:

```python
from pymq import Broker
from pymq.integrations.asgi import broker_lifespan

broker = Broker.from_config({"engine": "redis"})


async def application_lifespan():
    async with broker_lifespan(broker):
        yield
```

For a raw ASGI application, wrap its lifespan scope:

```python
from pymq.integrations.asgi import ASGIBrokerMiddleware

application = ASGIBrokerMiddleware(application, broker)
```

Non-lifespan scopes pass through unchanged. The broker starts before the wrapped application handles
lifespan messages and shuts down in `finally` if startup or application shutdown raises.

Do not call `startup()` in module import code. Production ASGI servers can import the application
more than once, fork workers, or disable lifespan handling. Confirm that the chosen server enables
ASGI lifespan.

## FastAPI and Starlette

The helper modules do not import FastAPI or Starlette, so they do not make either framework a core
dependency.

```python
from fastapi import Depends, FastAPI

from pymq import Broker
from pymq.integrations.fastapi import dependency, lifespan

broker = Broker.from_config({"engine": "memory"})
app = FastAPI(lifespan=lifespan(broker))


@app.get("/broker/health")
async def broker_health(selected=Depends(dependency(broker))):
    return await selected.health()
```

`pyev.integrations.starlette` exports the same `lifespan` and `dependency` helpers. The dependency
returns the process-local broker; it does not start or stop it per request.

Litestar, Quart, and other ASGI frameworks can use `broker_lifespan()` directly. The current release
does not include dedicated Litestar or Quart modules.

## Django

Install the Django extra and add the optional app when system checks are desired:

```python
INSTALLED_APPS = [
    # ...
    "pyev.integrations.django",
]

PYEV = {
    "engine": "redis",
    "engines": {
        "redis": {
            "url": "redis://localhost:6379/0",
            "mode": "streams",
        }
    },
}
```

`get_broker()` lazily constructs one broker for the current process. `configure(broker)` injects an
explicit instance, which is preferable in tests and applications with a custom configuration
loader.

Publishing from a database transaction should normally wait for commit:

```python
from pymq.integrations.django import publish_on_commit


def create_order(request):
    with transaction.atomic():
        order = Order.objects.create(...)
        publish_on_commit(OrderCreated(order_id=str(order.pk)))
```

`publish_on_commit()` calls `transaction.on_commit()`. In an async context it creates a tracked
process-local task; in a synchronous context it uses Django's `asgiref` bridge. Use
`await publish_immediately(...)` only when publishing before commit is intentional.

The `PyevConfig` app registers a namespaced system check that verifies `PYEV` is a mapping. The
current core release does **not** ship Django database models, admin pages, or management commands
for dead letters/outbox records. It also does not start a broker automatically merely because
`AppConfig.ready()` ran; wire broker lifecycle to the ASGI process that uses it.

For durable transaction integration, implement `OutboxStore` in the same application database and
write the outbox row in the same transaction. `MemoryOutboxStore` is not transactional across
process failure.

## Celery workers

Install Celery separately, construct the broker with an external engine when other processes must
observe its messages, then register worker signals:

```python
from pymq import Broker
from pymq.integrations.celery import install_worker_hooks

broker = Broker.from_config({"engine": "rabbitmq", "engines": {...}})
install_worker_hooks(broker)
```

Celery is imported only by `install_worker_hooks()`. The helper connects `worker_ready` to
`broker.startup()` and `worker_shutdown` to `broker.shutdown()`. If a signal fires inside an active
event loop, lifecycle work runs as a tracked task; otherwise the helper uses `asyncio.run()`.

Celery's own task transport and the pyev engine are independent configurations. Do not assume they
share queues, acknowledgements, retries, or credentials merely because both use RabbitMQ or Redis.

## Process and shutdown guidance

- Construct one broker per OS process; never rely on a global instance crossing a fork.
- Run subscriptions only in process roles intended to consume them. Registering the same durable
  consumer in every web worker may create competing consumers.
- Readiness should become true only after `startup()` connects and restores consumers.
- On shutdown, stop accepting work, let handlers finish within the configured grace period, then
  close the broker. A forced timeout must be treated as potentially unacknowledged work.
- Use external Redis Streams, RabbitMQ, Kafka, or a third-party engine for inter-process delivery.
