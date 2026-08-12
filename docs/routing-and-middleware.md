# Routing and middleware

## Routing

The router matches:

- exact names such as `orders.created`;
- namespace patterns such as `orders.*`;
- full wildcards (`*`);
- Python event types;
- exact schema versions; and
- conjunctive header requirements.

```python
router.register("orders.*", handle_order)

@router.on("billing.*", headers={"region": "eu"}, priority=10)
async def handle_eu_billing(delivery):
    ...
```

Higher priority handlers run first, then registration order. Explicit names make registrations easy
to introspect and unregister. Decorators only register metadata; they never start a broker or open a
connection.

## Destination mapping

```python
from broka.routing import Destination, Router

router = Router()
router.map_destination(
    "audit.*",
    Destination("company-audit", engine="kafka"),
)
```

Without a mapping, the logical route name is used as the portable physical destination. When mapping
rules overlap, priority and registration order are deterministic. Do not put native channel objects
or credentials in a `Route`.

## Middleware contract

Middleware follows the small ASGI-like pattern:

```python
async def timing(context, call_next):
    started = time.perf_counter()
    try:
        return await call_next(context)
    finally:
        metrics.observe("pyev_handler_duration_seconds", time.perf_counter() - started)
```

The first registered layer is outermost. Lower `order` values run first, and registration sequence
breaks ties. A layer may return without calling `call_next` when intentional short-circuiting is
valid. Errors propagate to the broker's reliability coordinator.

```python
broker.outbound_middleware.register(auth, name="auth", order=100)
broker.inbound_middleware.register(idempotency, name="dedupe", order=200, routes="commands.*")
```

Inbound and outbound pipelines are separate. A middleware may enrich or validate framework context,
but must acknowledge through `Delivery`; it must never call a native transport message.

## Default conceptual order

Outbound: validation, metadata, tracing, authorization, serialization, compression, encryption,
observability, reliability, engine invocation.

Inbound: transport normalization, decryption, decompression, deserialization, envelope validation,
tracing, observability, idempotency, handler dispatch, acknowledgement, failure handling.

Only configured layers run. Introspection exposes the exact effective order for tests and operations.

