# Observability and security

Operational signals are dependency-injected protocols. The core works without an exporter and
ships no-op and in-memory implementations; production applications can adapt their metrics and
tracing stack without transport code importing it.

## Health and readiness

`await broker.health()` returns a redacted `HealthReport` with:

- aggregate status, liveness, and readiness;
- lifecycle and connection state;
- selected engine and component checks;
- active consumers, queue depth, and consumer lag where an engine reports them;
- publish failure, retry, and dead-letter counters;
- circuit-breaker states and degraded capabilities; and
- low-cardinality component details.

```python
report = await broker.health()
if not report.ready:
    raise ServiceUnavailable()
```

`HealthRegistry` runs registered checks concurrently under individual timeouts. A failed critical
check makes readiness false; non-critical checks are reported as degraded. Health details pass
through recursive credential and URL redaction. Do not add raw message payloads or secrets to a
custom health check.

Liveness answers whether the process and runtime can continue; readiness answers whether it should
receive new work. A dependency outage should usually fail readiness without asking an orchestrator
to restart a healthy process in a tight loop.

## Metrics

`MetricsProvider` has synchronous `increment()`, `set_gauge()`, `observe()`, and `snapshot()`
operations. `NoOpMetrics` is the default-safe disabled provider. `InMemoryMetrics` is thread-safe,
retains aggregate histogram statistics rather than raw observations, and is useful for local
operation and tests.

Built-in instrument names use the `pyev_` prefix:

```text
pyev_publish_total
pyev_publish_failures_total
pyev_consume_total
pyev_handler_failures_total
pyev_retry_total
pyev_dead_letters_total
pyev_ack_total
pyev_nack_total
pyev_connections
pyev_consumer_lag
pyev_queue_depth
pyev_inflight
pyev_publish_latency_seconds
pyev_handler_duration_seconds
pyev_health_status
```

Labels such as engine, outcome, operation class, or event namespace are appropriate. The in-memory
provider rejects `message_id`, correlation, causation, trace, span, and request IDs by default
because they create an unbounded time series. An exporter can translate the same protocol to
Prometheus, StatsD, or another system; the core release does not bundle a Prometheus HTTP endpoint
or exporter.

## Distributed tracing

`TraceContext` represents W3C trace IDs, span IDs, flags, trace state, and baggage.
`TracePropagator` injects/extracts `traceparent`, `tracestate`, and `baggage` string headers.
Correlation and causation IDs remain separate envelope fields; trace context does not replace
either.

```python
from pymq.observability import InMemoryTracer, SpanKind, TracePropagator

tracer = InMemoryTracer()
async with tracer.start_as_current_span("publish", kind=SpanKind.PRODUCER) as span:
    carrier: dict[str, str] = {}
    TracePropagator().inject(carrier, span.context)
```

`NoOpTracer` preserves the callable contract without export. `InMemoryTracer` records completed
spans for assertions. The provider protocol is intentionally compatible with an adapter around
OpenTelemetry, but this release does not automatically configure an OpenTelemetry provider or
exporter. Applications remain responsible for sampling, exporter shutdown, baggage allowlists,
and preventing sensitive span attributes.

## Structured logging and internal events

`StructuredLogAdapter` adds redacted fields to standard-library logging. Payload logging is
disabled by default. Prefer stable fields such as route, engine, destination, event type/version,
attempt, duration, and error category. Do not use raw message IDs as metric labels, though they can
be useful in access-controlled logs.

`InternalEventEmitter` publishes isolated operational events such as `connected`, `retried`,
`dead_lettered`, `health_changed`, and circuit transitions. Listeners are async and run
concurrently. Ordinary listener failures are captured in bounded history and do not fail message
processing. A listener registered with `critical=True` raises `CriticalListenerError` after all
listeners settle; reserve this for hooks whose failure must stop the calling operation.

Internal operational events are not domain events. They do not travel through the application
router or engine unless an application explicitly bridges them.

## Connection and task supervision

`ConnectionManager` centralizes connect/reconnect retry, circuit breaking, heartbeat checks,
topology restoration, operation leases, draining, and shutdown. `ConnectionSnapshot` and
`health()` expose sanitized state. A failed operation classified as a connection error triggers a
reconnect but is not silently replayed; the caller's retry policy decides whether repeating it is
safe.

`TaskSupervisor` owns named tasks, bounded restart policy, failure history, cancellation, and
deterministic shutdown. Restartable services must be supplied as coroutine factories because a
coroutine object cannot be awaited twice. No task created by a framework service should be left
unowned after broker shutdown.

## Security baseline

- Use TLS and broker authentication appropriate to the external engine.
- Load credentials through secret-aware configuration; never interpolate full connection URLs in
  errors or health data.
- Keep JSON as the default and allowlist accepted serializers. Pickle is only for explicit trusted
  data and must never process untrusted payloads.
- Set envelope and queue-size limits. Treat decompression, custom serializer, and schema validation
  as denial-of-service boundaries.
- Validate route and header names before forwarding them to a transport.
- Leave decoded dead-letter payload persistence disabled unless the store has suitable access,
  encryption, and retention controls.
- Redact or hash sensitive domain fields before logs, traces, dead letters, and metrics.
- Rotate credentials through process lifecycle or an engine-specific extension; do not mutate
  frozen configuration in place.
- Use bounded consumer concurrency, queue capacity, pending RPC state, retry budgets, and replay
  batches.

Redaction is defense in depth, not a substitute for avoiding sensitive values at operational
boundaries.
