# ADR 0002: Put envelopes and bytes at the transport boundary

- Status: Accepted
- Date: 2026-08-06

## Context

Transport clients use incompatible native message types and metadata. Passing application objects
or native transport objects across the boundary would couple handlers to a client library and make
serialization, tracing, schema evolution, and replay inconsistent.

## Decision

The framework normalizes each application message into an immutable, independently versioned
`Envelope`. After framework serialization, an engine receives only a physical `Destination`, bytes,
and an engine publish context. Inbound transport records become framework `Delivery` objects;
handlers do not receive native client messages by default.

## Consequences

Event identity, correlation, schema version, content type, and retry provenance are portable.
Engines stay focused on I/O. Serialization and envelope evolution require explicit compatibility
work, and advanced transport metadata is available only through a controlled escape hatch.
