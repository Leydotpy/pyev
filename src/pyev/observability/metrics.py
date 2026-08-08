"""Dependency-free metrics contracts and an in-memory reference provider."""

from __future__ import annotations

import math
import re
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final, Protocol, runtime_checkable

type LabelValue = str | int | float | bool
type Labels = Mapping[str, LabelValue]
type LabelKey = tuple[tuple[str, str], ...]
type MetricKey = tuple[str, LabelKey]

PUBLISH_TOTAL: Final = "pyev_publish_total"
PUBLISH_FAILURES_TOTAL: Final = "pyev_publish_failures_total"
CONSUME_TOTAL: Final = "pyev_consume_total"
HANDLER_FAILURES_TOTAL: Final = "pyev_handler_failures_total"
RETRY_TOTAL: Final = "pyev_retry_total"
DEAD_LETTERS_TOTAL: Final = "pyev_dead_letters_total"
ACK_TOTAL: Final = "pyev_ack_total"
NACK_TOTAL: Final = "pyev_nack_total"
CONNECTIONS: Final = "pyev_connections"
CONSUMER_LAG: Final = "pyev_consumer_lag"
QUEUE_DEPTH: Final = "pyev_queue_depth"
INFLIGHT: Final = "pyev_inflight"
PUBLISH_LATENCY_SECONDS: Final = "pyev_publish_latency_seconds"
HANDLER_DURATION_SECONDS: Final = "pyev_handler_duration_seconds"
HEALTH_STATUS: Final = "pyev_health_status"

_METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
_LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_HIGH_CARDINALITY_LABELS = frozenset(
    {"message_id", "correlation_id", "causation_id", "trace_id", "span_id", "request_id"}
)


@dataclass(frozen=True, slots=True)
class HistogramSnapshot:
    """Aggregate histogram state without retaining raw observations."""

    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None

    @property
    def mean(self) -> float | None:
        """Return the arithmetic mean, when observations exist."""

        return self.total / self.count if self.count else None


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Immutable snapshot of all metric series."""

    counters: Mapping[MetricKey, float] = field(default_factory=dict)
    gauges: Mapping[MetricKey, float] = field(default_factory=dict)
    histograms: Mapping[MetricKey, HistogramSnapshot] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "counters", MappingProxyType(dict(self.counters)))
        object.__setattr__(self, "gauges", MappingProxyType(dict(self.gauges)))
        object.__setattr__(self, "histograms", MappingProxyType(dict(self.histograms)))

    def counter(self, name: str, labels: Labels | None = None) -> float:
        """Read a counter value, returning zero for a missing series."""

        return self.counters.get((name, normalize_labels(labels)), 0.0)

    def gauge(self, name: str, labels: Labels | None = None) -> float | None:
        """Read a gauge value."""

        return self.gauges.get((name, normalize_labels(labels)))

    def histogram(self, name: str, labels: Labels | None = None) -> HistogramSnapshot:
        """Read histogram aggregates."""

        return self.histograms.get((name, normalize_labels(labels)), HistogramSnapshot())


@runtime_checkable
class MetricsProvider(Protocol):
    """Small provider-neutral metrics interface."""

    def increment(self, name: str, value: float = 1.0, *, labels: Labels | None = None) -> None:
        """Increase a monotonic counter."""

    def set_gauge(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        """Set a gauge."""

    def observe(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        """Record a histogram observation."""

    def snapshot(self) -> MetricsSnapshot:
        """Return current metric state."""


def normalize_labels(
    labels: Labels | None,
    *,
    allow_high_cardinality: bool = False,
) -> LabelKey:
    """Validate and canonicalize labels into a deterministic key."""

    if not labels:
        return ()
    normalized: list[tuple[str, str]] = []
    for name, value in labels.items():
        if not _LABEL_NAME.fullmatch(name):
            raise ValueError(f"invalid metric label name {name!r}")
        if not allow_high_cardinality and name.casefold() in _HIGH_CARDINALITY_LABELS:
            raise ValueError(f"high-cardinality metric label {name!r} is not allowed")
        normalized.append((name, str(value)))
    return tuple(sorted(normalized))


def _validate_metric(name: str, value: float) -> float:
    if not _METRIC_NAME.fullmatch(name):
        raise ValueError(f"invalid metric name {name!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("metric values must be finite")
    return number


class InMemoryMetrics:
    """Thread-safe metrics provider for local operation and assertions."""

    def __init__(self, *, allow_high_cardinality: bool = False) -> None:
        self.allow_high_cardinality = allow_high_cardinality
        self._counters: dict[MetricKey, float] = {}
        self._gauges: dict[MetricKey, float] = {}
        self._histograms: dict[MetricKey, HistogramSnapshot] = {}
        self._lock = threading.RLock()

    def _key(self, name: str, labels: Labels | None) -> MetricKey:
        _validate_metric(name, 0.0)
        return (
            name,
            normalize_labels(labels, allow_high_cardinality=self.allow_high_cardinality),
        )

    def increment(self, name: str, value: float = 1.0, *, labels: Labels | None = None) -> None:
        number = _validate_metric(name, value)
        if number < 0:
            raise ValueError("counter increments must be non-negative")
        key = self._key(name, labels)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + number

    increment_counter = increment

    def set_gauge(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        number = _validate_metric(name, value)
        key = self._key(name, labels)
        with self._lock:
            self._gauges[key] = number

    gauge = set_gauge

    def observe(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        number = _validate_metric(name, value)
        key = self._key(name, labels)
        with self._lock:
            current = self._histograms.get(key, HistogramSnapshot())
            self._histograms[key] = HistogramSnapshot(
                count=current.count + 1,
                total=current.total + number,
                minimum=number if current.minimum is None else min(current.minimum, number),
                maximum=number if current.maximum is None else max(current.maximum, number),
            )

    histogram = observe

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(self._counters, self._gauges, self._histograms)

    def reset(self) -> None:
        """Remove every recorded series."""

        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()

    def timer(self, name: str, *, labels: Labels | None = None) -> DurationTimer:
        """Return a context manager that records elapsed seconds."""

        return DurationTimer(self, name, labels=labels)


class NoOpMetrics:
    """Zero-overhead compatible provider used when metrics are disabled."""

    def increment(self, name: str, value: float = 1.0, *, labels: Labels | None = None) -> None:
        return None

    increment_counter = increment

    def set_gauge(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        return None

    gauge = set_gauge

    def observe(self, name: str, value: float, *, labels: Labels | None = None) -> None:
        return None

    histogram = observe

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot()

    def timer(self, name: str, *, labels: Labels | None = None) -> DurationTimer:
        return DurationTimer(self, name, labels=labels)


class DurationTimer(AbstractContextManager["DurationTimer"]):
    """Synchronous timer suitable around async calls as well as sync blocks."""

    def __init__(
        self,
        provider: MetricsProvider,
        name: str,
        *,
        labels: Labels | None = None,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self.provider = provider
        self.name = name
        self.labels = labels
        self._clock = clock
        self._started: float | None = None

    def __enter__(self) -> DurationTimer:
        self._started = float(self._clock())
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._started is None:
            raise RuntimeError("timer was not started")
        self.provider.observe(
            self.name,
            max(0.0, float(self._clock()) - self._started),
            labels=self.labels,
        )


__all__ = [
    "ACK_TOTAL",
    "CONNECTIONS",
    "CONSUMER_LAG",
    "CONSUME_TOTAL",
    "DEAD_LETTERS_TOTAL",
    "HANDLER_DURATION_SECONDS",
    "HANDLER_FAILURES_TOTAL",
    "HEALTH_STATUS",
    "INFLIGHT",
    "NACK_TOTAL",
    "PUBLISH_FAILURES_TOTAL",
    "PUBLISH_LATENCY_SECONDS",
    "PUBLISH_TOTAL",
    "QUEUE_DEPTH",
    "RETRY_TOTAL",
    "DurationTimer",
    "HistogramSnapshot",
    "InMemoryMetrics",
    "Labels",
    "MetricKey",
    "MetricsProvider",
    "MetricsSnapshot",
    "NoOpMetrics",
    "normalize_labels",
]
