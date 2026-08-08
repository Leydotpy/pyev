"""Portable operation options used by :class:`pyev.Broker`."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyev.capabilities import Capability


class DeliveryMode(StrEnum):
    """Requested end-to-end delivery intent.

    ``EXACTLY_ONCE`` is intentionally an explicit request. The broker validates it
    against the selected engine instead of silently weakening the guarantee.
    """

    FIRE_AND_FORGET = "fire-and-forget"
    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"
    EXACTLY_ONCE = "exactly-once"


@dataclass(frozen=True, slots=True)
class PublishOptions:
    """Options for one publish operation."""

    engine: str | None = None
    serializer: str | None = None
    delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE
    timeout: float | None = None
    partition_key: str | None = None
    ordering_key: str | None = None
    ttl: float | None = None
    required_capabilities: frozenset[Capability] = frozenset()
    metadata: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class BatchPublishOptions:
    """Options for a bounded batch publish operation."""

    publish: PublishOptions = field(default_factory=PublishOptions)
    concurrency: int = 10
    stop_on_error: bool = False

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("batch concurrency must be at least 1")


@dataclass(frozen=True, slots=True)
class RequestOptions:
    """Options for portable request/reply."""

    publish: PublishOptions = field(default_factory=PublishOptions)
    route: str | None = None


@dataclass(frozen=True, slots=True)
class ReplyOptions:
    """Options for publishing a response to a request delivery."""

    publish: PublishOptions = field(default_factory=PublishOptions)
