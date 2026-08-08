# ADR 0004: Centralize reliability and lifecycle policy

- Status: Accepted
- Date: 2026-08-06

## Context

If each engine independently owns retries, backoff, circuit breaking, dead-letter decisions,
reconnection, and task shutdown, behavior differs by transport and nested retry loops can amplify an
outage. The same policies also need shared observability and deterministic tests.

## Decision

Framework services own retry classification and budgets, backoff, circuit state, acknowledgement
decisions, dead-letter handling, connection orchestration, topology restoration, task supervision,
and graceful draining. Engines expose only the transport primitives those services need and do not
start conflicting policy loops. The broker composes these services and owns their lifecycle.

## Consequences

Reliability behavior is testable and consistent while acknowledgements still preserve each
transport's real meaning. Engines remain smaller. Service configuration must be coordinated with
transport timeouts and deployment termination windows, and transport-native features are used only
through an explicit adapter or capability.
