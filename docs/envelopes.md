# Envelope specification and event versioning

## Canonical fields

Envelope format version 1 contains:

```json
{
  "envelope_version": 1,
  "id": "5ac09c41-09f2-4a42-80e1-c9bc17383cab",
  "type": "orders.created",
  "version": 1,
  "timestamp": "2026-08-06T00:00:00Z",
  "source": "checkout-api",
  "correlation_id": "...",
  "causation_id": "...",
  "trace": {},
  "content_type": "application/json",
  "serializer": "json",
  "headers": {},
  "partition_key": "customer-42",
  "ordering_key": null,
  "expires_at": null,
  "reply_to": null,
  "payload": {"order_id": "A-100"}
}
```

`Envelope` defensively freezes payload, trace context, and headers. Identity and timestamps cannot
be mutated after construction. Processing state belongs to `Delivery`.

The decoder rejects oversized input before parsing, invalid UTF-8, non-object roots, unsupported
format versions, malformed timestamps, invalid metadata, non-string headers, non-finite numbers,
recursive payloads, and non-JSON-safe values.

## Event registration

```python
@event("orders.created", version=2)
@dataclass(frozen=True)
class OrderCreatedV2:
    order_id: str
    currency: str
```

An `EventRegistry` is isolated and injectable. The convenience default registry holds classes
decorated without an explicit registry, but each broker can receive its own registry for tests or
multi-tenant applications.

## Explicit upcasting

Old schemas are never silently mutated. Register each forward transformation and request a target
version explicitly:

```python
registry.register_upcaster(
    "orders.created",
    from_version=1,
    to_version=2,
    transform=lambda old: {**old, "currency": "USD"},
)

message = envelope.to_message(registry=registry, target_version=2)
```

Missing links, backward targets, duplicate registrations, and invalid transformed payloads raise a
typed registration or validation error. Keep upcasters pure, deterministic, and covered by fixtures
containing real historical envelope data.

## Correlation and causation

Use `correlation_id` to group a workflow and `causation_id` to identify the immediate triggering
message. Trace context is complementary: it propagates distributed tracing data but does not replace
domain correlation.

## Serializer safety

JSON is the default. MessagePack support is lazy and requires the optional dependency. Pickle is
available only for explicitly trusted use and rejects encode/decode unless its unsafe opt-in and a
trusted deserialization context are both supplied. Never accept pickle from an untrusted producer.

