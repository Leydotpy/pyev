from __future__ import annotations

import pytest

from pymq.events import CriticalListenerError, InternalEventEmitter
from pymq.observability import (
    REDACTED,
    ComponentHealth,
    HealthRegistry,
    HealthStatus,
    InMemoryMetrics,
    InMemoryTracer,
    SpanKind,
    TracePropagator,
)


@pytest.mark.asyncio
async def test_internal_event_listener_failures_are_isolated() -> None:
    emitter = InternalEventEmitter()
    received: list[str] = []

    async def healthy(event: object) -> None:
        received.append("healthy")

    async def broken(event: object) -> None:
        raise RuntimeError("observer failed")

    emitter.subscribe("published", healthy)
    emitter.subscribe("published", broken)
    result = await emitter.emit("published", route="orders")

    assert received == ["healthy"]
    assert len(result.failures) == 1
    assert len(emitter.failures()) == 1


@pytest.mark.asyncio
async def test_critical_internal_listener_failure_is_explicit() -> None:
    emitter = InternalEventEmitter()

    async def broken(event: object) -> None:
        raise RuntimeError("required hook failed")

    emitter.subscribe("startup_started", broken, critical=True)
    with pytest.raises(CriticalListenerError):
        await emitter.emit("startup_started")


def test_metrics_capture_aggregates_and_reject_high_cardinality_labels() -> None:
    metrics = InMemoryMetrics()
    metrics.increment("pyev_publish_total", labels={"engine": "memory"})
    metrics.increment("pyev_publish_total", 2, labels={"engine": "memory"})
    metrics.set_gauge("pyev_connections", 1)
    metrics.observe("pyev_publish_latency_seconds", 0.25)
    metrics.observe("pyev_publish_latency_seconds", 0.75)

    snapshot = metrics.snapshot()
    assert snapshot.counter("pyev_publish_total", {"engine": "memory"}) == 3
    assert snapshot.gauge("pyev_connections") == 1
    assert snapshot.histogram("pyev_publish_latency_seconds").mean == 0.5
    with pytest.raises(ValueError, match="high-cardinality"):
        metrics.increment("pyev_publish_total", labels={"message_id": "unique"})


@pytest.mark.asyncio
async def test_trace_context_propagates_and_child_span_is_recorded() -> None:
    tracer = InMemoryTracer()
    async with tracer.start_as_current_span("publish", kind=SpanKind.PRODUCER) as parent:
        async with tracer.start_as_current_span("transport.send") as child:
            child.set_attribute("engine", "memory")
        carrier: dict[str, str] = {}
        TracePropagator().inject(carrier, parent.context)

    spans = tracer.finished_spans()
    assert [span.name for span in spans] == ["transport.send", "publish"]
    assert spans[0].parent_span_id == parent.context.span_id
    extracted = TracePropagator().extract(carrier)
    assert extracted is not None
    assert extracted.trace_id == parent.context.trace_id


@pytest.mark.asyncio
async def test_health_registry_aggregates_and_redacts_details() -> None:
    registry = HealthRegistry()

    async def database() -> ComponentHealth:
        return ComponentHealth.healthy(
            "database",
            details={"url": "postgres://user:secret@localhost/db", "password": "secret"},
        )

    registry.register("database", database)
    report = await registry.check_all(selected_engine="memory")

    assert report.status is HealthStatus.HEALTHY
    assert report.ready and report.live
    assert report.components["database"].details["password"] == REDACTED
    assert "user:secret" not in str(report.components["database"].details["url"])
