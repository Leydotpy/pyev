"""Structured, transport-independent engine capability declarations."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from .exceptions import UnsupportedCapabilityError


class Capability(StrEnum):
    """A portable feature that a transport engine may advertise."""

    PUBLISH_SUBSCRIBE = "publish_subscribe"
    WILDCARD_SUBSCRIPTIONS = "wildcard_subscriptions"
    DURABLE_SUBSCRIPTIONS = "durable_subscriptions"
    CONSUMER_GROUPS = "consumer_groups"
    COMPETING_CONSUMERS = "competing_consumers"
    FANOUT = "fanout"
    HEADERS_ROUTING = "headers_routing"
    MESSAGE_ORDERING = "message_ordering"
    PARTITION_ORDERING = "partition_ordering"
    TRANSACTIONS = "transactions"
    PUBLISHER_CONFIRMS = "publisher_confirms"
    NATIVE_DEAD_LETTER = "native_dead_letter"
    NATIVE_DELAY = "native_delay"
    SCHEDULED_DELIVERY = "scheduled_delivery"
    BATCH_PUBLISHING = "batch_publishing"
    BATCH_ACKNOWLEDGEMENT = "batch_acknowledgement"
    VISIBILITY_TIMEOUT = "visibility_timeout"
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    REQUEST_REPLY = "request_reply"
    MESSAGE_PRIORITIES = "message_priorities"
    QUEUE_DEPTH = "queue_depth"
    CONSUMER_LAG = "consumer_lag"

    # Readable aliases matching longer terminology used in documentation.
    NATIVE_DEAD_LETTER_QUEUES = NATIVE_DEAD_LETTER
    NATIVE_DELAYED_DELIVERY = NATIVE_DELAY
    EXACTLY_ONCE_PROCESSING = EXACTLY_ONCE


@dataclass(frozen=True, slots=True)
class CapabilitySpec:
    """One supported capability and its transport-specific attributes.

    Attributes can describe the scope or limits of a feature, for example
    ``{"scope": "partition"}`` for partition-local ordering.
    """

    capability: Capability
    attributes: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "capability", _coerce_capability(self.capability))
        object.__setattr__(
            self,
            "attributes",
            MappingProxyType(
                {str(key): _freeze_attribute(value) for key, value in self.attributes.items()}
            ),
        )


class CapabilitySet:
    """An immutable set of capabilities with optional structured attributes.

    ``CapabilitySet`` accepts an iterable of :class:`Capability` or
    :class:`CapabilitySpec` objects, or a mapping from capabilities to
    attribute mappings.  Instances are safe to share between broker tasks.
    """

    __slots__ = ("_values",)

    def __init__(
        self,
        capabilities: (
            Iterable[Capability | CapabilitySpec]
            | Mapping[Capability | str, Mapping[str, object] | None]
        ) = (),
    ) -> None:
        values: dict[Capability, Mapping[str, object]] = {}
        if isinstance(capabilities, Mapping):
            items = capabilities.items()
            for raw_capability, raw_attributes in items:
                capability = _coerce_capability(raw_capability)
                values[capability] = _freeze_attributes(raw_attributes or {})
        else:
            for item in capabilities:
                if isinstance(item, CapabilitySpec):
                    values[item.capability] = _freeze_attributes(item.attributes)
                else:
                    values[_coerce_capability(item)] = MappingProxyType({})
        self._values: Mapping[Capability, Mapping[str, object]] = MappingProxyType(values)

    @classmethod
    def of(cls, *capabilities: Capability | CapabilitySpec) -> CapabilitySet:
        """Build a capability set from positional capability declarations."""

        return cls(capabilities)

    @classmethod
    def empty(cls) -> CapabilitySet:
        """Return a capability set that advertises no optional features."""

        return cls()

    @property
    def capabilities(self) -> frozenset[Capability]:
        """Return the advertised capability names."""

        return frozenset(self._values)

    def supports(
        self,
        *capabilities: Capability | str,
        **required_attributes: object,
    ) -> bool:
        """Return whether all requested capabilities and attributes are present.

        Keyword attributes may be supplied when exactly one capability is
        requested.  Attribute values are compared for equality.
        """

        requested = tuple(_coerce_capability(value) for value in capabilities)
        if not requested:
            return not required_attributes
        if any(capability not in self._values for capability in requested):
            return False
        if required_attributes:
            if len(requested) != 1:
                raise ValueError(
                    "Capability attributes can only be checked for one capability at a time"
                )
            actual = self._values[requested[0]]
            return all(
                actual.get(key) == _freeze_attribute(value)
                for key, value in required_attributes.items()
            )
        return True

    def require(
        self,
        *capabilities: Capability | str,
        operation: str | None = None,
    ) -> None:
        """Raise when any requested capability is unavailable."""

        missing = tuple(
            capability
            for capability in (_coerce_capability(value) for value in capabilities)
            if capability not in self._values
        )
        if missing:
            raise UnsupportedCapabilityError(missing, operation=operation)

    def attributes_for(self, capability: Capability | str) -> Mapping[str, object]:
        """Return immutable attributes for a supported capability.

        Raises:
            UnsupportedCapabilityError: If the capability is not advertised.
        """

        normalized = _coerce_capability(capability)
        try:
            return self._values[normalized]
        except KeyError as exc:
            raise UnsupportedCapabilityError(normalized) from exc

    def attribute(
        self,
        capability: Capability | str,
        name: str,
        default: Any = None,
    ) -> Any:
        """Return one capability attribute, or ``default`` when absent."""

        normalized = _coerce_capability(capability)
        attributes = self._values.get(normalized)
        if attributes is None:
            return default
        return attributes.get(name, default)

    def with_capability(
        self,
        capability: Capability | str,
        /,
        **attributes: object,
    ) -> CapabilitySet:
        """Return a new set containing or replacing one capability."""

        values = dict(self._values)
        values[_coerce_capability(capability)] = attributes
        return type(self)(values)

    def without(self, *capabilities: Capability | str) -> CapabilitySet:
        """Return a new set without the specified capabilities."""

        removed = {_coerce_capability(value) for value in capabilities}
        return type(self)({key: value for key, value in self._values.items() if key not in removed})

    def union(self, other: CapabilitySet) -> CapabilitySet:
        """Return a new set with ``other`` taking precedence on duplicates."""

        values = dict(self._values)
        values.update(other._values)
        return type(self)(values)

    def to_dict(self) -> dict[str, dict[str, object]]:
        """Return a mutable, JSON-oriented representation for diagnostics."""

        return {
            capability.value: {key: _thaw_attribute(value) for key, value in attributes.items()}
            for capability, attributes in self._values.items()
        }

    def __contains__(self, capability: object) -> bool:
        try:
            normalized = _coerce_capability(capability)
        except (TypeError, ValueError):
            return False
        return normalized in self._values

    def __iter__(self) -> Iterator[Capability]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __bool__(self) -> bool:
        return bool(self._values)

    def __repr__(self) -> str:
        names = ", ".join(capability.value for capability in self._values)
        return f"{type(self).__name__}({names})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CapabilitySet):
            return NotImplemented
        return self._values == other._values


def _coerce_capability(value: object) -> Capability:
    if isinstance(value, Capability):
        return value
    if isinstance(value, str):
        return Capability(value)
    raise TypeError(f"Expected Capability or str, got {type(value).__name__}")


def _freeze_attributes(attributes: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(
        {str(key): _freeze_attribute(value) for key, value in attributes.items()}
    )


def _freeze_attribute(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_attribute(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_attribute(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_attribute(item) for item in value)
    return value


def _thaw_attribute(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_attribute(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_attribute(item) for item in value]
    if isinstance(value, frozenset):
        return sorted((_thaw_attribute(item) for item in value), key=repr)
    return value


__all__ = ["Capability", "CapabilitySet", "CapabilitySpec"]
