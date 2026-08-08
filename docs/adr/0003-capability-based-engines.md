# ADR 0003: Model engines through capabilities

- Status: Accepted
- Date: 2026-08-06

## Context

Redis Pub/Sub, Redis Streams, RabbitMQ, Kafka, and future engines do not share identical ordering,
acknowledgement, durability, delay, transaction, or request/reply semantics. Engine-type checks in
application or broker code would grow without bound and encourage false equivalence.

## Decision

Every engine exposes a structured `CapabilitySet`, including scoped attributes where a boolean is
insufficient. The broker validates requested options against capabilities. It uses a portable
fallback only when semantics are safe and documented; otherwise it raises
`UnsupportedCapabilityError`. Optional behavior is expressed through focused engine protocols.

## Consequences

Third-party engines can integrate without edits to broker policy, and applications can make explicit
deployment decisions. Capability declarations become part of an engine's contract and require
conformance tests. pyev does not silently downgrade a requested delivery guarantee.
