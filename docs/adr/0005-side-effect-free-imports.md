# ADR 0005: Keep imports side-effect free and registries injectable

- Status: Accepted
- Date: 2026-08-06

## Context

Import-time network connections, background tasks, secret reads, and process-wide mutable registries
make tests order-dependent and conflict with ASGI, Django, Celery, and CLI lifecycle ownership.
Entry-point plugins still need convenient discovery and applications need useful defaults.

## Decision

Importing pyev performs no I/O and starts no tasks. Entry-point discovery is lazy or happens during
explicit startup. Registries are ordinary injectable instances; a default registry may exist for
convenience but is not the only runtime authority. Connections and consumers start and stop through
the broker or an integration lifecycle hook.

## Consequences

Applications control when secrets and network resources are used, tests can isolate registration,
and multiple brokers can coexist. A freshly imported plugin is not active until discovery occurs,
and framework integrations must clearly designate one lifecycle owner per process.
