"""Composable readiness, liveness, and component health reporting."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from .redaction import redact_mapping


class HealthStatus(StrEnum):
    """Severity of a health result."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


_SEVERITY = {
    HealthStatus.HEALTHY: 0,
    HealthStatus.UNKNOWN: 1,
    HealthStatus.DEGRADED: 2,
    HealthStatus.UNHEALTHY: 3,
}


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    """Health of one engine or framework service."""

    name: str
    status: HealthStatus
    message: str | None = None
    latency: float | None = None
    details: Mapping[str, object] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("component name must not be empty")
        if self.latency is not None and self.latency < 0:
            raise ValueError("latency must be non-negative")
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must be timezone-aware")
        object.__setattr__(self, "details", MappingProxyType(redact_mapping(self.details)))

    @classmethod
    def healthy(
        cls,
        name: str,
        *,
        latency: float | None = None,
        details: Mapping[str, object] | None = None,
    ) -> ComponentHealth:
        """Construct a healthy result."""

        return cls(name, HealthStatus.HEALTHY, latency=latency, details=details or {})

    @classmethod
    def unhealthy(cls, name: str, message: str) -> ComponentHealth:
        """Construct an unhealthy result."""

        return cls(name, HealthStatus.UNHEALTHY, message=message)


@dataclass(frozen=True, slots=True)
class HealthReport:
    """Redacted aggregate broker health suitable for an endpoint."""

    status: HealthStatus
    live: bool
    ready: bool
    lifecycle_state: str = "unknown"
    selected_engine: str | None = None
    connection_state: str | None = None
    latency: float | None = None
    active_consumers: int = 0
    queue_depth: int | None = None
    consumer_lag: int | None = None
    publish_failures: int = 0
    retry_count: int = 0
    dead_letter_count: int = 0
    circuit_breakers: Mapping[str, str] = field(default_factory=dict)
    degraded_capabilities: tuple[str, ...] = ()
    components: Mapping[str, ComponentHealth] = field(default_factory=dict)
    details: Mapping[str, object] = field(default_factory=dict)
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        for name in ("active_consumers", "publish_failures", "retry_count", "dead_letter_count"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.checked_at.tzinfo is None:
            raise ValueError("checked_at must be timezone-aware")
        object.__setattr__(self, "circuit_breakers", MappingProxyType(dict(self.circuit_breakers)))
        object.__setattr__(self, "components", MappingProxyType(dict(self.components)))
        object.__setattr__(self, "details", MappingProxyType(redact_mapping(self.details)))

    @classmethod
    def from_components(
        cls,
        components: Sequence[ComponentHealth],
        *,
        lifecycle_state: str = "running",
        selected_engine: str | None = None,
        connection_state: str | None = None,
        critical_components: frozenset[str] | None = None,
        latency: float | None = None,
        active_consumers: int = 0,
        queue_depth: int | None = None,
        consumer_lag: int | None = None,
        publish_failures: int = 0,
        retry_count: int = 0,
        dead_letter_count: int = 0,
        circuit_breakers: Mapping[str, str] | None = None,
        degraded_capabilities: Sequence[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> HealthReport:
        """Build a report from component checks and optional broker counters."""

        by_name = {component.name: component for component in components}
        critical = frozenset(by_name) if critical_components is None else critical_components
        status = max(
            (component.status for component in components),
            key=lambda value: _SEVERITY[value],
            default=HealthStatus.UNKNOWN,
        )
        ready = all(
            component.status is HealthStatus.HEALTHY
            for name, component in by_name.items()
            if name in critical
        )
        live = lifecycle_state not in {"failed", "stopped"}
        return cls(
            status=status,
            live=live,
            ready=ready and live,
            lifecycle_state=lifecycle_state,
            selected_engine=selected_engine,
            connection_state=connection_state,
            components=by_name,
            latency=latency,
            active_consumers=active_consumers,
            queue_depth=queue_depth,
            consumer_lag=consumer_lag,
            publish_failures=publish_failures,
            retry_count=retry_count,
            dead_letter_count=dead_letter_count,
            circuit_breakers=circuit_breakers or {},
            degraded_capabilities=tuple(degraded_capabilities),
            details=details or {},
        )

    def with_component(self, component: ComponentHealth, *, critical: bool = True) -> HealthReport:
        """Return a new report merged with one component."""

        components = {**self.components, component.name: component}
        critical_names = frozenset(components if critical else self.components)
        return HealthReport.from_components(
            tuple(components.values()),
            lifecycle_state=self.lifecycle_state,
            selected_engine=self.selected_engine,
            connection_state=self.connection_state,
            critical_components=critical_names,
            latency=self.latency,
            active_consumers=self.active_consumers,
            queue_depth=self.queue_depth,
            consumer_lag=self.consumer_lag,
            publish_failures=self.publish_failures,
            retry_count=self.retry_count,
            dead_letter_count=self.dead_letter_count,
            circuit_breakers=self.circuit_breakers,
            degraded_capabilities=self.degraded_capabilities,
            details=self.details,
        )


@runtime_checkable
class HealthCheck(Protocol):
    """Async callable health check."""

    async def __call__(self) -> ComponentHealth:
        """Evaluate and return component health."""


@dataclass(frozen=True, slots=True)
class RegisteredHealthCheck:
    """Named health check configuration."""

    name: str
    check: Callable[[], Awaitable[ComponentHealth] | ComponentHealth]
    critical: bool = True
    timeout: float = 5.0


class HealthRegistry:
    """Isolated registry that evaluates checks concurrently with timeouts."""

    def __init__(self) -> None:
        self._checks: dict[str, RegisteredHealthCheck] = {}

    def register(
        self,
        name: str,
        check: Callable[[], Awaitable[ComponentHealth] | ComponentHealth],
        *,
        critical: bool = True,
        timeout: float = 5.0,
        replace: bool = False,
    ) -> None:
        """Register a component check."""

        if not name:
            raise ValueError("name must not be empty")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if name in self._checks and not replace:
            raise ValueError(f"health check {name!r} is already registered")
        self._checks[name] = RegisteredHealthCheck(name, check, critical, timeout)

    def unregister(self, name: str) -> bool:
        """Remove a health check."""

        return self._checks.pop(name, None) is not None

    async def check(self, name: str) -> ComponentHealth:
        """Evaluate one named check."""

        try:
            registration = self._checks[name]
        except KeyError as error:
            raise KeyError(f"unknown health check {name!r}") from error
        return await self._evaluate(registration)

    async def check_all(
        self,
        *,
        lifecycle_state: str = "running",
        selected_engine: str | None = None,
        connection_state: str | None = None,
        latency: float | None = None,
        active_consumers: int = 0,
        queue_depth: int | None = None,
        consumer_lag: int | None = None,
        publish_failures: int = 0,
        retry_count: int = 0,
        dead_letter_count: int = 0,
        circuit_breakers: Mapping[str, str] | None = None,
        degraded_capabilities: Sequence[str] = (),
        details: Mapping[str, object] | None = None,
    ) -> HealthReport:
        """Evaluate all checks and construct an aggregate report."""

        registrations = tuple(self._checks[name] for name in sorted(self._checks))
        components = await asyncio.gather(*(self._evaluate(item) for item in registrations))
        critical = frozenset(item.name for item in registrations if item.critical)
        return HealthReport.from_components(
            components,
            lifecycle_state=lifecycle_state,
            selected_engine=selected_engine,
            connection_state=connection_state,
            critical_components=critical,
            latency=latency,
            active_consumers=active_consumers,
            queue_depth=queue_depth,
            consumer_lag=consumer_lag,
            publish_failures=publish_failures,
            retry_count=retry_count,
            dead_letter_count=dead_letter_count,
            circuit_breakers=circuit_breakers,
            degraded_capabilities=degraded_capabilities,
            details=details,
        )

    async def _evaluate(self, registration: RegisteredHealthCheck) -> ComponentHealth:
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            async with asyncio.timeout(registration.timeout):
                result = registration.check()
                component = await result if inspect.isawaitable(result) else result
            if not isinstance(component, ComponentHealth):
                raise TypeError("health check did not return ComponentHealth")
            if component.latency is None:
                return ComponentHealth(
                    name=component.name,
                    status=component.status,
                    message=component.message,
                    latency=max(0.0, loop.time() - started),
                    details=component.details,
                    checked_at=component.checked_at,
                )
            return component
        except TimeoutError:
            return ComponentHealth(
                registration.name,
                HealthStatus.UNHEALTHY if registration.critical else HealthStatus.DEGRADED,
                message=f"health check timed out after {registration.timeout:g}s",
                latency=max(0.0, loop.time() - started),
            )
        except Exception as error:
            return ComponentHealth(
                registration.name,
                HealthStatus.UNHEALTHY if registration.critical else HealthStatus.DEGRADED,
                message=f"{type(error).__name__}: {error}",
                latency=max(0.0, loop.time() - started),
            )


class NoOpHealthCheck:
    """A healthy check useful when an optional service is disabled."""

    def __init__(self, name: str = "noop") -> None:
        self.name = name

    async def __call__(self) -> ComponentHealth:
        return ComponentHealth.healthy(self.name, details={"enabled": False})


__all__ = [
    "ComponentHealth",
    "HealthCheck",
    "HealthRegistry",
    "HealthReport",
    "HealthStatus",
    "NoOpHealthCheck",
    "RegisteredHealthCheck",
]
