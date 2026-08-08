"""Typed event declarations, version registration, and reconstruction."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import TypeVar, cast

from .exceptions import (
    DuplicateRegistrationError,
    EventRegistrationError,
    MessageValidationError,
    UnknownEventError,
)
from .message import (
    EVENT_METADATA_ATTRIBUTE,
    EVENT_NAME_ATTRIBUTE,
    EVENT_VERSION_ATTRIBUTE,
    message_to_payload,
    to_json_value,
)
from .typing import JSONValue

_EVENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
TEvent = TypeVar("TEvent")
EventUpcaster = Callable[[Mapping[str, JSONValue]], Mapping[str, object]]


@dataclass(frozen=True, slots=True, order=True)
class EventKey:
    """The stable registry identity of one event schema version."""

    name: str
    version: int

    def __post_init__(self) -> None:
        _validate_event_identity(self.name, self.version)


@dataclass(frozen=True, slots=True)
class EventMetadata:
    """Metadata attached to an event class by :func:`event`."""

    name: str
    version: int
    event_type: type[object]

    def __post_init__(self) -> None:
        _validate_event_identity(self.name, self.version)

    @property
    def key(self) -> EventKey:
        """Return this declaration's registry key."""

        return EventKey(self.name, self.version)


@dataclass(frozen=True, slots=True)
class _Upcaster:
    target_version: int
    transform: EventUpcaster


class EventRegistry:
    """An isolated, thread-safe registry of versioned event classes.

    The registry never changes a message's schema version implicitly.  Callers
    must pass ``target_version`` to :meth:`reconstruct` to opt into an explicit
    sequence of registered upcasters.
    """

    def __init__(self) -> None:
        self._events: dict[EventKey, type[object]] = {}
        self._upcasters: dict[EventKey, _Upcaster] = {}
        self._lock = RLock()

    def register(
        self,
        event_type: type[TEvent],
        *,
        name: str | None = None,
        version: int | None = None,
        replace: bool = False,
    ) -> type[TEvent]:
        """Register and return an event class.

        ``name`` and ``version`` may be supplied for adapter-managed models. If
        omitted, the class must already have metadata from :func:`event`.
        """

        if not isinstance(event_type, type):
            raise TypeError("event_type must be a class")
        if name is None and version is None:
            metadata = get_event_metadata(event_type)
            name = metadata.name
            version = metadata.version
        elif name is None or version is None:
            raise EventRegistrationError("Event name and version must be supplied together")
        else:
            _attach_metadata(event_type, name, version)
        key = EventKey(name, version)
        with self._lock:
            existing = self._events.get(key)
            if existing is not None and existing is not event_type and not replace:
                raise DuplicateRegistrationError(
                    f"Event {name!r} version {version} is already registered",
                    context={
                        "event": name,
                        "version": version,
                        "existing_type": _qualified_name(existing),
                        "new_type": _qualified_name(event_type),
                    },
                )
            self._events[key] = cast(type[object], event_type)
        return event_type

    def unregister(self, name: str, version: int) -> type[object] | None:
        """Remove and return an event class, or ``None`` when absent."""

        key = EventKey(name, version)
        with self._lock:
            return self._events.pop(key, None)

    def resolve(self, name: str, version: int) -> type[object]:
        """Resolve an exact event name and schema version."""

        key = EventKey(name, version)
        with self._lock:
            try:
                return self._events[key]
            except KeyError as exc:
                raise UnknownEventError(
                    f"No event is registered for {name!r} version {version}",
                    context={"event": name, "version": version},
                ) from exc

    def get(self, name: str, version: int) -> type[object] | None:
        """Return an exact event class without raising when it is unknown."""

        key = EventKey(name, version)
        with self._lock:
            return self._events.get(key)

    def versions(self, name: str) -> tuple[int, ...]:
        """Return registered schema versions for ``name`` in ascending order."""

        with self._lock:
            return tuple(sorted(key.version for key in self._events if key.name == name))

    def latest_version(self, name: str) -> int:
        """Return the highest registered version for an event name."""

        versions = self.versions(name)
        if not versions:
            raise UnknownEventError(f"No event is registered for {name!r}", context={"event": name})
        return versions[-1]

    def registrations(self) -> Mapping[EventKey, type[object]]:
        """Return an immutable snapshot of all exact registrations."""

        with self._lock:
            return MappingProxyType(dict(self._events))

    def register_upcaster(
        self,
        name: str,
        from_version: int,
        to_version: int,
        transform: EventUpcaster,
        *,
        replace: bool = False,
    ) -> None:
        """Register one explicit, forward-only schema transformation."""

        _validate_event_identity(name, from_version)
        if not isinstance(to_version, int) or isinstance(to_version, bool) or to_version < 1:
            raise EventRegistrationError("Upcaster target version must be a positive integer")
        if to_version <= from_version:
            raise EventRegistrationError("An upcaster must move to a higher schema version")
        if not callable(transform):
            raise TypeError("transform must be callable")
        key = EventKey(name, from_version)
        with self._lock:
            existing = self._upcasters.get(key)
            if existing is not None and not replace:
                raise DuplicateRegistrationError(
                    f"An upcaster for {name!r} version {from_version} is already registered",
                    context={"event": name, "from_version": from_version},
                )
            self._upcasters[key] = _Upcaster(to_version, transform)

    def reconstruct(
        self,
        name: str,
        version: int,
        payload: Mapping[str, object],
        *,
        target_version: int | None = None,
    ) -> object:
        """Construct a typed event from a payload.

        By default the exact wire version is reconstructed.  Supplying a later
        ``target_version`` applies a complete registered upcaster chain before
        constructing that target class.
        """

        _validate_event_identity(name, version)
        destination_version = version if target_version is None else target_version
        if destination_version < version:
            raise EventRegistrationError("Downcasting event schemas is not supported")
        normalized = to_json_value(payload)
        if not isinstance(normalized, dict):  # defensive; payload is statically a mapping
            raise MessageValidationError("Event payload must be a JSON object")
        current_payload: Mapping[str, JSONValue] = normalized
        current_version = version

        while current_version < destination_version:
            key = EventKey(name, current_version)
            with self._lock:
                upcaster = self._upcasters.get(key)
            if upcaster is None or upcaster.target_version > destination_version:
                raise EventRegistrationError(
                    f"No complete upcaster path for {name!r} from version "
                    f"{current_version} to {destination_version}",
                    context={
                        "event": name,
                        "from_version": current_version,
                        "target_version": destination_version,
                    },
                )
            try:
                transformed = upcaster.transform(current_payload)
                normalized_result = to_json_value(transformed)
            except MessageValidationError:
                raise
            except Exception as exc:
                raise MessageValidationError(
                    f"Upcaster failed for {name!r} version {current_version}",
                    context={"event": name, "from_version": current_version},
                ) from exc
            if not isinstance(normalized_result, dict):
                raise MessageValidationError("An event upcaster must return a mapping")
            current_payload = normalized_result
            current_version = upcaster.target_version

        event_type = self.resolve(name, destination_version)
        return _construct_event(event_type, current_payload)

    def payload_for(self, value: object) -> dict[str, JSONValue]:
        """Convert a registered event instance to a JSON-safe payload."""

        metadata = get_event_metadata(value)
        registered_type = self.resolve(metadata.name, metadata.version)
        if not isinstance(value, registered_type):
            raise MessageValidationError(
                "Event instance does not match the registered class",
                context={"event": metadata.name, "version": metadata.version},
            )
        return message_to_payload(value)

    def __contains__(self, value: object) -> bool:
        if isinstance(value, EventKey):
            key = value
        elif isinstance(value, tuple) and len(value) == 2:
            try:
                key = EventKey(str(value[0]), int(value[1]))
            except (TypeError, ValueError, EventRegistrationError):
                return False
        else:
            return False
        with self._lock:
            return key in self._events

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)


default_event_registry = EventRegistry()
DEFAULT_EVENT_REGISTRY = default_event_registry


def event(
    name: str,
    *,
    version: int = 1,
    registry: EventRegistry | None = None,
    register: bool = True,
) -> Callable[[type[TEvent]], type[TEvent]]:
    """Decorate a typed model as a versioned pyev event.

    Args:
        name: Stable logical event name used in envelopes and routes.
        version: Positive application schema version.
        registry: Isolated registry to use instead of the default registry.
        register: Set to ``False`` to attach metadata without registration.
    """

    _validate_event_identity(name, version)

    def decorate(event_type: type[TEvent]) -> type[TEvent]:
        if not isinstance(event_type, type):
            raise TypeError("@event can only decorate a class")
        _attach_metadata(event_type, name, version)
        if register:
            selected_registry = default_event_registry if registry is None else registry
            selected_registry.register(event_type)
        return event_type

    return decorate


def get_event_metadata(value: object) -> EventMetadata:
    """Return validated metadata for an event class or instance."""

    event_type = value if isinstance(value, type) else type(value)
    metadata = event_type.__dict__.get(EVENT_METADATA_ATTRIBUTE)
    if isinstance(metadata, EventMetadata) and metadata.event_type is event_type:
        return metadata
    name = event_type.__dict__.get(EVENT_NAME_ATTRIBUTE)
    version = event_type.__dict__.get(EVENT_VERSION_ATTRIBUTE)
    if isinstance(name, str) and isinstance(version, int) and not isinstance(version, bool):
        _validate_event_identity(name, version)
        return EventMetadata(name, version, event_type)
    raise MessageValidationError(
        "Type is not a declared pyev event; decorate it with @event",
        context={"event_type": _qualified_name(event_type)},
    )


def _attach_metadata(event_type: type[object], name: str, version: int) -> None:
    _validate_event_identity(name, version)
    existing_name = event_type.__dict__.get(EVENT_NAME_ATTRIBUTE)
    existing_version = event_type.__dict__.get(EVENT_VERSION_ATTRIBUTE)
    if (existing_name is not None or existing_version is not None) and (
        existing_name != name or existing_version != version
    ):
        raise EventRegistrationError(
            "Event class already has different pyev metadata",
            context={
                "event_type": _qualified_name(event_type),
                "existing_name": existing_name,
                "existing_version": existing_version,
                "requested_name": name,
                "requested_version": version,
            },
        )
    metadata = EventMetadata(name, version, event_type)
    try:
        setattr(event_type, EVENT_NAME_ATTRIBUTE, name)
        setattr(event_type, EVENT_VERSION_ATTRIBUTE, version)
        setattr(event_type, EVENT_METADATA_ATTRIBUTE, metadata)
    except (AttributeError, TypeError) as exc:
        raise EventRegistrationError(
            "Event class does not allow pyev metadata to be attached",
            context={"event_type": _qualified_name(event_type)},
        ) from exc


def _construct_event(event_type: type[object], payload: Mapping[str, JSONValue]) -> object:
    try:
        if callable(model_validate := getattr(event_type, "model_validate", None)):
            return model_validate(dict(payload))
        if callable(from_dict := getattr(event_type, "from_dict", None)):
            return from_dict(dict(payload))
        return event_type(**dict(payload))
    except Exception as exc:
        metadata = get_event_metadata(event_type)
        raise MessageValidationError(
            f"Payload is invalid for event {metadata.name!r} version {metadata.version}",
            context={"event": metadata.name, "version": metadata.version},
        ) from exc


def _validate_event_identity(name: str, version: int) -> None:
    if not isinstance(name, str) or not _EVENT_NAME_PATTERN.fullmatch(name):
        raise EventRegistrationError(
            "Event name must be a non-empty namespaced identifier without whitespace",
            context={"event": name},
        )
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise EventRegistrationError(
            "Event schema version must be a positive integer",
            context={"event": name, "version": version},
        )


def _qualified_name(value: type[object]) -> str:
    return f"{value.__module__}.{value.__qualname__}"


__all__ = [
    "DEFAULT_EVENT_REGISTRY",
    "EventKey",
    "EventMetadata",
    "EventRegistry",
    "EventUpcaster",
    "default_event_registry",
    "event",
    "get_event_metadata",
]
