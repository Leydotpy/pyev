"""Small transport-focused service-provider interface for pyev engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import ClassVar, Protocol, runtime_checkable

from pymq.capabilities import CapabilitySet


def _empty_mapping() -> Mapping[str, object]:
    return MappingProxyType({})


@dataclass(frozen=True, slots=True)
class Availability:
    """Result of an engine's side-effect-free availability probe."""

    available: bool
    reason: str | None = None

    def __bool__(self) -> bool:
        return self.available


@dataclass(frozen=True, slots=True)
class EngineHealth:
    """Transport-local health information."""

    engine: str
    connected: bool
    healthy: bool
    latency_ms: float | None = None
    details: Mapping[str, object] = field(default_factory=_empty_mapping)


@dataclass(frozen=True, slots=True)
class EnginePublishContext:
    """Portable context supplied with serialized envelope bytes."""

    message_id: str
    headers: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    partition_key: str | None = None
    ordering_key: str | None = None
    timeout: float | None = None
    ttl: float | None = None


@dataclass(frozen=True, slots=True)
class EnginePublishResult:
    """Transport result before normalization by the broker."""

    accepted: bool = True
    transport_id: str | None = None
    published_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class EngineAcknowledgementAdapter(Protocol):
    """Duck-typed acknowledgement operations supplied with an incoming message."""

    async def ack(self) -> None: ...

    async def nack(self, requeue: bool = True) -> None: ...

    async def reject(self) -> None: ...

    async def requeue(self) -> None: ...

    async def defer(self, delay: float) -> None: ...

    async def touch(self, extension: float) -> None: ...


@dataclass(frozen=True, slots=True)
class EngineIncomingMessage:
    """Transport-neutral incoming bytes and their engine adapter."""

    destination: str
    payload: bytes
    acknowledgement: EngineAcknowledgementAdapter
    headers: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    transport_metadata: Mapping[str, object] = field(default_factory=_empty_mapping)
    received_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    attempt: int = 1


EngineDeliveryCallback = Callable[[EngineIncomingMessage], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class EngineSubscription:
    """Physical subscription requested from a transport engine."""

    id: str
    pattern: str
    destination: str
    concurrency: int = 1
    capacity: int = 100
    headers: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("consumer concurrency must be at least 1")
        if self.capacity < 1:
            raise ValueError("consumer capacity must be at least 1")


class EngineConsumer(ABC):
    """Handle for a running transport consumer."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Return the stable subscription identifier."""

    @abstractmethod
    async def pause(self) -> None:
        """Pause new deliveries without discarding queued work."""

    @abstractmethod
    async def resume(self) -> None:
        """Resume deliveries."""

    @abstractmethod
    async def close(self) -> None:
        """Drain and close the consumer idempotently."""


class BaseEngine(ABC):
    """Minimal interface implemented by every pyev transport engine."""

    name: ClassVar[str]
    priority: ClassVar[int] = 0

    def __init__(self, config: Mapping[str, object] | None = None) -> None:
        self.config = MappingProxyType(dict(config or {}))

    @classmethod
    def is_available(cls, config: Mapping[str, object] | None = None) -> Availability:
        """Probe availability without opening connections."""

        del config
        return Availability(True)

    @property
    @abstractmethod
    def capabilities(self) -> CapabilitySet:
        """Describe truthful native and portable engine capabilities."""

    @abstractmethod
    async def connect(self) -> None:
        """Open transport resources idempotently."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Drain and close transport resources idempotently."""

    @abstractmethod
    async def publish(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
    ) -> EnginePublishResult:
        """Publish serialized envelope bytes to a physical destination."""

    @abstractmethod
    async def create_consumer(
        self,
        subscription: EngineSubscription,
        callback: EngineDeliveryCallback,
    ) -> EngineConsumer:
        """Create and start a consumer for a physical subscription."""

    @abstractmethod
    async def healthcheck(self) -> EngineHealth:
        """Return a non-secret transport health snapshot."""


@runtime_checkable
class BatchPublishEngine(Protocol):
    """Optional protocol for engines with native batch publishing."""

    async def publish_batch(
        self,
        items: Sequence[tuple[str, bytes, EnginePublishContext]],
    ) -> Sequence[EnginePublishResult]: ...


@runtime_checkable
class NativeRequestReplyEngine(Protocol):
    """Optional protocol for a transport-native request/reply optimization."""

    async def request_native(
        self,
        destination: str,
        payload: bytes,
        context: EnginePublishContext,
        timeout: float | None,
    ) -> bytes: ...
