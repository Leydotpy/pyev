# Troubleshooting and migration

Start with the exception type and `await broker.health()`. pyev errors carry a `retryable` flag
and a deliberately non-secret `context` mapping; logs and traces should report those fields without
printing credentials or message payloads.

## The broker is not ready

`publish()` and `subscribe()` require a started broker. Prefer an async context manager:

```python
async with Broker.from_config(config) as broker:
    await broker.publish(message)
```

For a long-running service, start exactly once in its lifespan hook and shut down in the matching
exit hook. Do not call `asyncio.run()` from an already-running ASGI event loop. Startup and shutdown
are idempotent, but overlapping framework hooks can still make ownership unclear.

If startup fails, inspect each component in the health report. Typical causes are an unavailable
optional client, invalid engine configuration, failed credentials, an unreachable service, or a
topology restoration callback that raised. A failed connection attempt is unwound before retrying.

## `EngineUnavailableError`

An explicitly selected external engine never silently falls back to memory. Check that:

1. the corresponding extra is installed (`pyev[redis]`, `pyev[rabbitmq]`, or `pyev[kafka]`);
2. the configured URL or bootstrap servers are present and valid;
3. the broker service is reachable from this process; and
4. an entry-point plugin is installed in the same Python environment as the application.

Call the selected engine's availability check or inspect the registry to distinguish a missing
dependency from a missing configuration value. Availability checks are side-effect free and do not
prove that credentials or network access work; `startup()` performs the real connection.

## `UnsupportedCapabilityError`

The requested behavior is not supplied or safely emulated by the active engine. Inspect
`broker.capabilities` and change either the request or the engine. Common mismatches include:

- durable subscriptions or requeue on Redis Pub/Sub;
- global ordering when only per-partition ordering exists;
- native delayed delivery on an engine without a scheduler;
- publisher confirmation when it was disabled in transport configuration; and
- exactly-once processing without an atomic application-side transaction.

Do not catch this error and silently downgrade reliability. If degraded behavior is valid, make it
an explicit application policy and expose it through health or operational telemetry.

## A handler does not receive a message

Check, in order:

- the subscription was created after startup and has not been closed;
- the logical route resolved from the event name is the route you expect;
- the pattern matches (`orders.*` matches `orders.created`, while a Python class name is not
  automatically the declared event name);
- the outbound destination and inbound handler pattern have not been confused;
- the process is still running and the subscription object remains owned;
- the handler did not fail and move the delivery into retry or dead-letter processing; and
- a bounded memory queue did not apply the configured overflow behavior.

`local` and `memory` communicate only inside one process. Two web workers, a web process and a
worker, or separate containers require an external engine.

## Duplicate delivery or repeated handler execution

At-least-once delivery permits duplicates after consumer failure, acknowledgement loss, reclaim,
or offset replay. Make handlers idempotent and derive a stable key from the envelope message ID or
an application business key. Use a durable `IdempotencyStore` when process restarts matter; the
memory implementation is only process-local.

In `AUTO` acknowledgement mode, success is acknowledged after the handler completes. If a handler
performs a database mutation and then crashes before acknowledgement, the mutation may have
succeeded while the message is delivered again. Use a database transaction, idempotency record, or
inbox pattern appropriate to the application.

## A delivery was acknowledged too early or cannot be acknowledged

- `AUTO` owns the terminal operation; application code normally should not also call `ack()`.
- `MANUAL` requires the handler to choose a terminal operation.
- `NONE` represents a transport path with no acknowledgement.
- Conflicting terminal calls raise `InvalidStateTransitionError`; they do not become no-ops.
- `defer()` and `touch()` require either engine capability or a configured safe emulation.

Do not retain a `Delivery` and acknowledge it after its handler or visibility deadline has ended.
Transport-native metadata is an escape hatch, not an alternate acknowledgement API.

## Retry or circuit behavior looks surprising

`max_attempts` includes the first call. A policy with `max_attempts=3` schedules at most two retry
delays. Jitter intentionally makes those delays nondeterministic; inject a random source or use
`DeterministicRetryScheduler` in tests.

An open circuit fails fast until its cooldown expires. Half-open probes are deliberately limited,
so concurrent callers can continue to receive `CircuitOpenError` while one probe runs. Inspect the
circuit snapshot and operational events before manually resetting it. Increasing retry counts
during a broad outage usually increases load; use elapsed-time limits and retry budgets instead.

## Dead letters do not survive restart

`MemoryDeadLetterStore` is bounded and ephemeral. Inject a durable store for production retention.
Verify its retention policy, encryption, access controls, and replay audit trail independently of
the transport's native dead-letter queue.

Use filtered inspection and a dry run before replay. Replay preserves provenance, is rate limited,
and should quarantine records that repeatedly fail. Never replay an unbounded production set in a
single administrative request.

## Request/reply times out

Confirm that the engine advertises request/reply or that the portable broker fallback is enabled,
the responder is subscribed, and it calls `broker.reply()` with the received `Delivery`. The reply
must preserve the correlation metadata and target the generated reply destination. Set a timeout
long enough for queueing plus handler execution, but always keep it bounded.

On timeout, the caller stops waiting; it does not prove that the request was never processed.
Design request handlers with the same idempotency assumptions as other at-least-once consumers.

## Event decoding or version errors

The envelope version and application event schema version are separate. Confirm that:

- the declared event name and version are registered in the consumer;
- the envelope content type and serializer are supported;
- an explicit upcaster path exists for each older supported schema; and
- the producer did not mutate a version number without changing its schema contract.

pyev does not silently coerce an unsupported version. Add a tested upcaster or deploy compatible
consumers before producers start emitting the new schema.

## Plugin discovery does not find an engine

Engine plugins use the `pyev.engines` entry-point group. Confirm the distribution metadata:

```toml
[project.entry-points."pyev.engines"]
acme = "acme_pyev:AcmeEngine"
```

Reinstall the plugin after changing entry points, restart the process, and inspect the injected
`EngineRegistry`. Tests that use an isolated registry do not automatically inherit registrations
from another registry instance. Discovery loads code lazily; importing `pyev` alone does not run it.

## Shutdown hangs or reports forced cancellation

Handlers must cooperate with cancellation and must not block the event loop with synchronous I/O.
Ensure operation leases are released, consumer callbacks return, and tasks created by application
code are owned and cancelled by the application. pyev drains only work and tasks it owns.

Set the service manager's termination grace period longer than the broker's shutdown grace period.
If shutdown must cancel in-flight work, expect at-least-once transports to redeliver unacknowledged
messages after restart.

## Framework integration issues

- **ASGI/FastAPI/Starlette:** install one pyev lifespan owner. If composing lifespans, preserve both
  startup and shutdown ordering.
- **Django:** `configure()` creates process-local state; it does not start consumers in every web
  process. Use `publish_on_commit()` so a rolled-back transaction does not emit an event.
- **Celery:** worker hooks start one broker for that worker process. Avoid creating a new broker per
  task and do not assume the web process shares its in-memory engine.

## Migrating from an older `janus-events` prototype

The independent package and import namespace are `pyev`. Replace `janus_events` imports, package
requirements, configuration prefixes, plugin distribution names, and entry-point groups:

| Older prototype name | pyev name |
| --- | --- |
| `janus-events` | `pyev` |
| `janus_events` | `pyev` |
| `janus_events.engines` | `pyev.engines` |
| `janus-events-redis` | `pyev-redis` |
| `janus-events-rabbitmq` | `pyev-rabbitmq` |
| `janus-events-kafka` | `pyev-kafka` |

There is no Janus application dependency or Janus-specific domain model in pyev. Move those event
classes into the consuming application, keep their stable wire names where compatibility matters,
and register any schema upcasters there. Test a rolling deployment with both old and new consumers
before changing an externally visible event name.

If a problem remains, capture the minimal redacted configuration, engine and dependency versions,
health report, exception type/context, delivery state, and a reproducible message schema. Do not
attach credentials, full broker URLs, unredacted headers, or sensitive payloads.
