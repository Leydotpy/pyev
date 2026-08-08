# ADR 0001: Use one broker façade

- Status: Accepted
- Date: 2026-08-06

## Context

Publishing, subscriptions, RPC, lifecycle, routing, and reliability need one coherent application
contract. Separate event bus, publisher, subscriber, and transport runtimes would duplicate policy
and make switching engines observable to business code.

## Decision

`Broker` is the only high-level runtime façade. `EventBus` is a compatibility alias and
`Publisher`/`Subscriber` are restricted protocols or views over a broker. Configuration-driven
construction uses `Broker.from_config()` or the single broker factory.

## Consequences

Applications have one lifecycle owner and one stable API when engines change. Smaller dependencies
can accept protocols without gaining a second runtime. New high-level behavior must compose into the
broker rather than introduce a competing façade.
