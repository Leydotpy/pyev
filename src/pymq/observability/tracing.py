"""Provider-neutral tracing with W3C propagation and in-memory spans."""

from __future__ import annotations

import re
import secrets
import threading
import time
from collections.abc import Callable, Mapping, MutableMapping
from contextlib import AbstractAsyncContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable
from urllib.parse import quote, unquote

from .redaction import redact_mapping, redact_text

_TRACEPARENT = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace>[0-9a-f]{32})-(?P<span>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$"
)


def _new_nonzero_hex(byte_count: int) -> str:
    value = "0" * (byte_count * 2)
    while int(value, 16) == 0:
        value = secrets.token_hex(byte_count)
    return value


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Portable W3C trace context carried in an envelope."""

    trace_id: str
    span_id: str
    trace_flags: int = 1
    trace_state: str | None = None
    baggage: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{32}", self.trace_id) or int(self.trace_id, 16) == 0:
            raise ValueError("trace_id must be 32 lowercase hexadecimal characters and non-zero")
        if not re.fullmatch(r"[0-9a-f]{16}", self.span_id) or int(self.span_id, 16) == 0:
            raise ValueError("span_id must be 16 lowercase hexadecimal characters and non-zero")
        if not 0 <= self.trace_flags <= 255:
            raise ValueError("trace_flags must fit in one byte")
        object.__setattr__(self, "baggage", MappingProxyType(dict(self.baggage)))

    @classmethod
    def new(cls, *, sampled: bool = True) -> TraceContext:
        """Create a new root trace context."""

        return cls(_new_nonzero_hex(16), _new_nonzero_hex(8), int(sampled))

    def child(self) -> TraceContext:
        """Create a child span context in the same trace."""

        return TraceContext(
            trace_id=self.trace_id,
            span_id=_new_nonzero_hex(8),
            trace_flags=self.trace_flags,
            trace_state=self.trace_state,
            baggage=self.baggage,
        )

    @property
    def sampled(self) -> bool:
        """Return the W3C sampled bit."""

        return bool(self.trace_flags & 1)

    @property
    def traceparent(self) -> str:
        """Serialize the W3C ``traceparent`` header."""

        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags:02x}"

    @classmethod
    def parse(cls, traceparent: str, *, trace_state: str | None = None) -> TraceContext:
        """Parse and validate a W3C ``traceparent`` value."""

        match = _TRACEPARENT.fullmatch(traceparent.strip().casefold())
        if match is None or match.group("version") == "ff":
            raise ValueError("invalid traceparent")
        return cls(
            trace_id=match.group("trace"),
            span_id=match.group("span"),
            trace_flags=int(match.group("flags"), 16),
            trace_state=trace_state,
        )


class SpanKind(StrEnum):
    """Portable span roles."""

    INTERNAL = "internal"
    PRODUCER = "producer"
    CONSUMER = "consumer"
    CLIENT = "client"
    SERVER = "server"


class SpanStatus(StrEnum):
    """Completion status of a span."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class SpanEvent:
    """Timestamped annotation attached to a span."""

    name: str
    timestamp: datetime
    attributes: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", MappingProxyType(redact_mapping(self.attributes)))


@dataclass(frozen=True, slots=True)
class SpanData:
    """Immutable completed span exported by :class:`InMemoryTracer`."""

    name: str
    context: TraceContext
    parent_span_id: str | None
    kind: SpanKind
    status: SpanStatus
    status_description: str | None
    started_at: datetime
    ended_at: datetime
    duration: float
    attributes: Mapping[str, object]
    events: tuple[SpanEvent, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


@runtime_checkable
class Span(Protocol):
    """Minimal active-span contract."""

    name: str
    context: TraceContext

    def set_attribute(self, name: str, value: object) -> None:
        """Set a redacted span attribute."""

    def add_event(self, name: str, attributes: Mapping[str, object] | None = None) -> None:
        """Add a timestamped event."""

    def record_exception(self, error: BaseException) -> None:
        """Record sanitized exception metadata."""

    def set_status(self, status: SpanStatus, description: str | None = None) -> None:
        """Set completion status."""

    def end(self) -> None:
        """Finish the span idempotently."""


@runtime_checkable
class Tracer(Protocol):
    """Tracing provider used by broker middleware."""

    def start_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> Span:
        """Create an active span without changing current context."""

    def start_as_current_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> AbstractAsyncContextManager[Span]:
        """Create a span and make it current for an async block."""


_CURRENT_SPAN: ContextVar[Span | None] = ContextVar("pyev_current_span", default=None)


def get_current_span() -> Span | None:
    """Return the span active in the current async context."""

    return _CURRENT_SPAN.get()


class _BaseSpan:
    def __init__(
        self,
        name: str,
        context: TraceContext,
        *,
        parent_span_id: str | None,
        kind: SpanKind,
        attributes: Mapping[str, object] | None,
        clock: Callable[[], float],
        wall_clock: Callable[[], datetime],
        on_end: Callable[[SpanData], None] | None,
    ) -> None:
        if not name:
            raise ValueError("span name must not be empty")
        self.name = name
        self.context = context
        self.parent_span_id = parent_span_id
        self.kind = kind
        self._attributes = redact_mapping(attributes or {})
        self._events: list[SpanEvent] = []
        self._status = SpanStatus.UNSET
        self._status_description: str | None = None
        self._clock = clock
        self._wall_clock = wall_clock
        self._started_monotonic = float(clock())
        self._started_at = wall_clock()
        self._ended = False
        self._on_end = on_end

    def set_attribute(self, name: str, value: object) -> None:
        if not name:
            raise ValueError("attribute name must not be empty")
        if self._ended:
            return
        self._attributes.update(redact_mapping({name: value}))

    def add_event(self, name: str, attributes: Mapping[str, object] | None = None) -> None:
        if not name:
            raise ValueError("event name must not be empty")
        if self._ended:
            return
        self._events.append(SpanEvent(name, self._wall_clock(), attributes or {}))

    def record_exception(self, error: BaseException) -> None:
        self.add_event(
            "exception",
            {
                "exception.type": type(error).__name__,
                "exception.message": str(error),
            },
        )

    def set_status(self, status: SpanStatus, description: str | None = None) -> None:
        if self._ended:
            return
        self._status = status
        self._status_description = redact_text(description) if description is not None else None

    def end(self) -> None:
        if self._ended:
            return
        self._ended = True
        ended_at = self._wall_clock()
        data = SpanData(
            name=self.name,
            context=self.context,
            parent_span_id=self.parent_span_id,
            kind=self.kind,
            status=self._status,
            status_description=self._status_description,
            started_at=self._started_at,
            ended_at=ended_at,
            duration=max(0.0, float(self._clock()) - self._started_monotonic),
            attributes=self._attributes,
            events=tuple(self._events),
        )
        if self._on_end is not None:
            self._on_end(data)


class InMemorySpan(_BaseSpan):
    """Mutable active span recorded by :class:`InMemoryTracer`."""


class NoOpSpan(_BaseSpan):
    """Compatible span that discards its completed data."""


class _SpanScope:
    def __init__(self, span: Span) -> None:
        self.span = span
        self._token: Token[Span | None] | None = None

    async def __aenter__(self) -> Span:
        self._token = _CURRENT_SPAN.set(self.span)
        return self.span

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            if isinstance(exc, BaseException):
                self.span.record_exception(exc)
                self.span.set_status(SpanStatus.ERROR, str(exc))
            elif exc_type is None:
                self.span.set_status(SpanStatus.OK)
            self.span.end()
        finally:
            if self._token is not None:
                _CURRENT_SPAN.reset(self._token)


class InMemoryTracer:
    """Thread-safe tracer retaining completed spans for tests and local use."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.perf_counter,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._finished: list[SpanData] = []
        self._lock = threading.RLock()

    def start_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> InMemorySpan:
        current = get_current_span()
        parent = context or (current.context if current is not None else None)
        span_context = parent.child() if parent is not None else TraceContext.new()
        return InMemorySpan(
            name,
            span_context,
            parent_span_id=parent.span_id if parent is not None else None,
            kind=kind,
            attributes=attributes,
            clock=self._clock,
            wall_clock=self._wall_clock,
            on_end=self._finish,
        )

    def start_as_current_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> AbstractAsyncContextManager[Span]:
        return _SpanScope(self.start_span(name, context=context, kind=kind, attributes=attributes))

    def _finish(self, data: SpanData) -> None:
        with self._lock:
            self._finished.append(data)

    def finished_spans(self) -> tuple[SpanData, ...]:
        """Return completed spans in completion order."""

        with self._lock:
            return tuple(self._finished)

    def clear(self) -> None:
        """Discard recorded spans."""

        with self._lock:
            self._finished.clear()


class NoOpTracer:
    """Tracing provider used when export is disabled."""

    def start_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> NoOpSpan:
        current = get_current_span()
        parent = context or (current.context if current is not None else None)
        span_context = parent.child() if parent is not None else TraceContext.new(sampled=False)
        return NoOpSpan(
            name,
            span_context,
            parent_span_id=parent.span_id if parent is not None else None,
            kind=kind,
            attributes=attributes,
            clock=time.perf_counter,
            wall_clock=lambda: datetime.now(UTC),
            on_end=None,
        )

    def start_as_current_span(
        self,
        name: str,
        *,
        context: TraceContext | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Mapping[str, object] | None = None,
    ) -> AbstractAsyncContextManager[Span]:
        return _SpanScope(self.start_span(name, context=context, kind=kind, attributes=attributes))


class TracePropagator:
    """Inject and extract W3C trace context from string headers."""

    def inject(
        self,
        carrier: MutableMapping[str, str],
        context: TraceContext | None = None,
    ) -> None:
        selected = context
        if selected is None:
            current = get_current_span()
            selected = current.context if current is not None else None
        if selected is None:
            return
        carrier["traceparent"] = selected.traceparent
        if selected.trace_state:
            carrier["tracestate"] = selected.trace_state
        if selected.baggage:
            carrier["baggage"] = ",".join(
                f"{quote(key, safe='')}={quote(value, safe='')}"
                for key, value in sorted(selected.baggage.items())
            )

    def extract(self, carrier: Mapping[str, str]) -> TraceContext | None:
        normalized = {key.casefold(): value for key, value in carrier.items()}
        traceparent = normalized.get("traceparent")
        if traceparent is None:
            return None
        context = TraceContext.parse(traceparent, trace_state=normalized.get("tracestate"))
        baggage: dict[str, str] = {}
        for item in normalized.get("baggage", "").split(","):
            if not item.strip() or "=" not in item:
                continue
            key, value = item.strip().split("=", 1)
            baggage[unquote(key)] = unquote(value.split(";", 1)[0])
        if baggage:
            context = TraceContext(
                trace_id=context.trace_id,
                span_id=context.span_id,
                trace_flags=context.trace_flags,
                trace_state=context.trace_state,
                baggage=baggage,
            )
        return context


__all__ = [
    "InMemorySpan",
    "InMemoryTracer",
    "NoOpSpan",
    "NoOpTracer",
    "Span",
    "SpanData",
    "SpanEvent",
    "SpanKind",
    "SpanStatus",
    "TraceContext",
    "TracePropagator",
    "Tracer",
    "get_current_span",
]
