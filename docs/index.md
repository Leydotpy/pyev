# pyev documentation

`pyev` separates an application's domain messages from the technology that transports them.
Application code deals with typed events, `Envelope`, `Delivery`, and `Broker`. Engines deal only
with destinations and bytes. Framework services own routing, middleware, acknowledgement policy,
retries, circuit breaking, dead letters, lifecycle, and observability.

The shortest route through these docs is:

1. [Getting started](getting-started.md)
2. [Core concepts](core-concepts.md)
3. [Routing and middleware](routing-and-middleware.md)
4. [Reliability](reliability.md)
5. [Engines and plugins](engines.md)

Reference and operations guides:

- [Envelope and schema versioning](envelopes.md)
- [Framework integrations](integrations.md)
- [Observability and security](operations.md)
- [Testing and deployment](testing-and-deployment.md)
- [API reference](api-reference.md)
- [Troubleshooting and migration](troubleshooting.md)
- [Architecture decisions](adr/README.md)

## Architectural boundary

```text
typed application message
        │
        ▼
Broker → router → outbound middleware → reliability coordinator
        │
        ▼
physical Destination + serialized Envelope bytes
        │
        ▼
transport Engine
        │
        ▼
Delivery → inbound middleware → handler → framework acknowledgement
```

No transport package is required to declare an event or test application behavior with the local
or memory engine. No connection is opened and no task is started at import time.

