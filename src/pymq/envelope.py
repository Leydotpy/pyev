"""Immutable, versioned wire envelope used by every pyev transport engine."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Final, cast
from uuid import uuid4

from .event import EventRegistry, default_event_registry, get_event_metadata
from .exceptions import MessageValidationError, SerializationError
from .message import message_to_payload, to_json_value
from .typing import JSONValue

CURRENT_ENVELOPE_VERSION: Final[int] = 1
DEFAULT_MAX_ENVELOPE_BYTES: Final[int] = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Envelope:
    """A transport-independent serialized message and its immutable metadata.

    ``version`` is the application event schema version, while
    ``envelope_version`` independently versions this framework wire format.
    Nested mappings and sequences are defensively copied and frozen during
    construction.
    """

    id: str
    type: str
    version: int
    timestamp: datetime
    payload: Mapping[str, JSONValue]
    envelope_version: int = CURRENT_ENVELOPE_VERSION
    source: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    trace: Mapping[str, JSONValue] = field(default_factory=dict)
    content_type: str = "application/json"
    serializer: str = "json"
    headers: Mapping[str, str] = field(default_factory=dict)
    partition_key: str | None = None
    ordering_key: str | None = None
    expires_at: datetime | None = None
    reply_to: str | None = None

    def __post_init__(self) -> None:
        _require_nonempty("id", self.id)
        _require_nonempty("type", self.type)
        _require_positive_int("version", self.version)
        _require_positive_int("envelope_version", self.envelope_version)
        if self.envelope_version != CURRENT_ENVELOPE_VERSION:
            raise MessageValidationError(
                f"Unsupported envelope version {self.envelope_version}",
                context={
                    "envelope_version": self.envelope_version,
                    "supported_version": CURRENT_ENVELOPE_VERSION,
                },
            )
        _require_nonempty("content_type", self.content_type)
        _require_nonempty("serializer", self.serializer)
        timestamp = _normalize_datetime("timestamp", self.timestamp)
        expires_at = (
            None if self.expires_at is None else _normalize_datetime("expires_at", self.expires_at)
        )
        normalized_payload = to_json_value(self.payload)
        if not isinstance(normalized_payload, dict):
            raise MessageValidationError("Envelope payload must be a JSON object")
        normalized_trace = to_json_value(self.trace)
        if not isinstance(normalized_trace, dict):
            raise MessageValidationError("Envelope trace context must be a JSON object")
        headers = _normalize_headers(self.headers)
        for field_name in (
            "source",
            "correlation_id",
            "causation_id",
            "partition_key",
            "ordering_key",
            "reply_to",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_nonempty(field_name, value)

        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(
            self,
            "payload",
            cast(Mapping[str, JSONValue], _freeze_json(normalized_payload)),
        )
        object.__setattr__(
            self,
            "trace",
            cast(Mapping[str, JSONValue], _freeze_json(normalized_trace)),
        )
        object.__setattr__(self, "headers", MappingProxyType(headers))

    @classmethod
    def create(
        cls,
        payload: Mapping[str, object],
        *,
        type: str,
        version: int = 1,
        id: str | None = None,
        timestamp: datetime | None = None,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        trace: Mapping[str, object] | None = None,
        content_type: str = "application/json",
        serializer: str = "json",
        headers: Mapping[str, str] | None = None,
        partition_key: str | None = None,
        ordering_key: str | None = None,
        expires_at: datetime | None = None,
        ttl: float | timedelta | None = None,
        reply_to: str | None = None,
    ) -> Envelope:
        """Create an envelope from a JSON-compatible payload mapping.

        ``ttl`` is resolved against ``timestamp`` and cannot be combined with
        ``expires_at``.
        """

        created_at = timestamp or datetime.now(UTC)
        if ttl is not None and expires_at is not None:
            raise MessageValidationError("ttl and expires_at are mutually exclusive")
        if ttl is not None:
            seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
            if not math.isfinite(seconds) or seconds <= 0:
                raise MessageValidationError("ttl must be a finite number greater than zero")
            expires_at = created_at + timedelta(seconds=seconds)
        return cls(
            id=id or str(uuid4()),
            type=type,
            version=version,
            timestamp=created_at,
            payload=cast(Mapping[str, JSONValue], payload),
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            trace=cast(Mapping[str, JSONValue], trace or {}),
            content_type=content_type,
            serializer=serializer,
            headers=headers or {},
            partition_key=partition_key,
            ordering_key=ordering_key,
            expires_at=expires_at,
            reply_to=reply_to,
        )

    @classmethod
    def from_message(
        cls,
        message: object,
        *,
        id: str | None = None,
        timestamp: datetime | None = None,
        source: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        trace: Mapping[str, object] | None = None,
        content_type: str = "application/json",
        serializer: str = "json",
        headers: Mapping[str, str] | None = None,
        partition_key: str | None = None,
        ordering_key: str | None = None,
        expires_at: datetime | None = None,
        ttl: float | timedelta | None = None,
        reply_to: str | None = None,
    ) -> Envelope:
        """Create an envelope from a class decorated with :func:`pyev.event`."""

        metadata = get_event_metadata(message)
        return cls.create(
            message_to_payload(message),
            type=metadata.name,
            version=metadata.version,
            id=id,
            timestamp=timestamp,
            source=source,
            correlation_id=correlation_id,
            causation_id=causation_id,
            trace=trace,
            content_type=content_type,
            serializer=serializer,
            headers=headers,
            partition_key=partition_key,
            ordering_key=ordering_key,
            expires_at=expires_at,
            ttl=ttl,
            reply_to=reply_to,
        )

    @property
    def message_id(self) -> str:
        """Return :attr:`id` using the explicit message-ID spelling."""

        return self.id

    def is_expired(self, *, at: datetime | None = None) -> bool:
        """Return whether the envelope has expired at the supplied UTC time."""

        if self.expires_at is None:
            return False
        comparison = _normalize_datetime("at", at or datetime.now(UTC))
        return comparison >= self.expires_at

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-safe mapping in canonical wire shape."""

        return {
            "envelope_version": self.envelope_version,
            "id": self.id,
            "type": self.type,
            "version": self.version,
            "timestamp": _datetime_to_wire(self.timestamp),
            "source": self.source,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "trace": _thaw_json(self.trace),
            "content_type": self.content_type,
            "serializer": self.serializer,
            "headers": dict(self.headers),
            "partition_key": self.partition_key,
            "ordering_key": self.ordering_key,
            "expires_at": (None if self.expires_at is None else _datetime_to_wire(self.expires_at)),
            "reply_to": self.reply_to,
            "payload": _thaw_json(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Envelope:
        """Validate and reconstruct an envelope from its wire mapping."""

        if not isinstance(value, Mapping):
            raise MessageValidationError("Envelope representation must be a mapping")
        try:
            raw_id = value["id"]
            raw_type = value["type"]
            raw_version = value["version"]
            raw_timestamp = value["timestamp"]
            raw_payload = value["payload"]
        except KeyError as exc:
            raise MessageValidationError(
                f"Envelope is missing required field {exc.args[0]!r}",
                context={"field": exc.args[0]},
            ) from exc
        if not isinstance(raw_id, str) or not isinstance(raw_type, str):
            raise MessageValidationError("Envelope id and type must be strings")
        if not isinstance(raw_version, int) or isinstance(raw_version, bool):
            raise MessageValidationError("Envelope event version must be an integer")
        if not isinstance(raw_payload, Mapping):
            raise MessageValidationError("Envelope payload must be a mapping")

        envelope_version = value.get("envelope_version", CURRENT_ENVELOPE_VERSION)
        if not isinstance(envelope_version, int) or isinstance(envelope_version, bool):
            raise MessageValidationError("Envelope format version must be an integer")
        trace = value.get("trace", {})
        headers = value.get("headers", {})
        if not isinstance(trace, Mapping):
            raise MessageValidationError("Envelope trace context must be a mapping")
        if not isinstance(headers, Mapping):
            raise MessageValidationError("Envelope headers must be a mapping")

        return cls(
            id=raw_id,
            type=raw_type,
            version=raw_version,
            timestamp=_parse_datetime("timestamp", raw_timestamp),
            payload=cast(Mapping[str, JSONValue], raw_payload),
            envelope_version=envelope_version,
            source=_optional_string(value, "source"),
            correlation_id=_optional_string(value, "correlation_id"),
            causation_id=_optional_string(value, "causation_id"),
            trace=cast(Mapping[str, JSONValue], trace),
            content_type=_string(value, "content_type", "application/json"),
            serializer=_string(value, "serializer", "json"),
            headers=cast(Mapping[str, str], headers),
            partition_key=_optional_string(value, "partition_key"),
            ordering_key=_optional_string(value, "ordering_key"),
            expires_at=_parse_optional_datetime("expires_at", value.get("expires_at")),
            reply_to=_optional_string(value, "reply_to"),
        )

    def to_bytes(self) -> bytes:
        """Encode the canonical envelope mapping as compact UTF-8 JSON."""

        try:
            return json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise SerializationError(
                "Envelope could not be encoded as JSON",
                context={"message_id": self.id, "event": self.type},
            ) from exc

    @classmethod
    def from_bytes(
        cls,
        data: bytes | bytearray | memoryview,
        *,
        max_size: int = DEFAULT_MAX_ENVELOPE_BYTES,
    ) -> Envelope:
        """Decode and validate a canonical UTF-8 JSON envelope.

        ``max_size`` bounds memory amplification before JSON parsing begins.
        """

        if max_size < 1:
            raise ValueError("max_size must be positive")
        raw = bytes(data)
        if len(raw) > max_size:
            raise SerializationError(
                "Envelope exceeds the configured byte limit",
                context={"size": len(raw), "max_size": max_size},
            )
        try:
            decoded = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise SerializationError("Envelope is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise SerializationError("Envelope JSON root must be an object")
        try:
            return cls.from_dict(decoded)
        except MessageValidationError as exc:
            raise SerializationError(
                "Decoded envelope violates the pyev wire contract",
                context=exc.context,
            ) from exc

    def to_message(
        self,
        *,
        registry: EventRegistry | None = None,
        target_version: int | None = None,
    ) -> object:
        """Reconstruct the typed event registered for this envelope."""

        selected_registry = default_event_registry if registry is None else registry
        return selected_registry.reconstruct(
            self.type,
            self.version,
            cast(Mapping[str, object], self.payload),
            target_version=target_version,
        )

    def with_headers(
        self, headers: Mapping[str, str], *, replace_existing: bool = False
    ) -> Envelope:
        """Return a new envelope with validated additional headers."""

        merged = {} if replace_existing else dict(self.headers)
        merged.update(headers)
        return replace(self, headers=merged)


def _normalize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        if not isinstance(key, str) or not key:
            raise MessageValidationError("Envelope header names must be non-empty strings")
        if not isinstance(value, str):
            raise MessageValidationError(
                "Envelope header values must be strings", context={"header": key}
            )
        result[key] = value
    return result


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate envelope JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"Non-finite envelope JSON number is not permitted: {value}")


def _normalize_datetime(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise MessageValidationError(f"Envelope {name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise MessageValidationError(f"Envelope {name} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_datetime(name: str, value: object) -> datetime:
    if isinstance(value, datetime):
        return _normalize_datetime(name, value)
    if not isinstance(value, str):
        raise MessageValidationError(f"Envelope {name} must be an ISO-8601 string")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise MessageValidationError(f"Envelope {name} is not valid ISO-8601") from exc
    return _normalize_datetime(name, parsed)


def _parse_optional_datetime(name: str, value: object) -> datetime | None:
    return None if value is None else _parse_datetime(name, value)


def _datetime_to_wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise MessageValidationError(f"Envelope {key} must be a string or null")
    return item


def _string(value: Mapping[str, object], key: str, default: str) -> str:
    item = value.get(key, default)
    if not isinstance(item, str):
        raise MessageValidationError(f"Envelope {key} must be a string")
    return item


def _require_nonempty(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise MessageValidationError(f"Envelope {name} must be a non-empty string")


def _require_positive_int(name: str, value: object) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise MessageValidationError(f"Envelope {name} must be a positive integer")


def _freeze_json(value: JSONValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> JSONValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise SerializationError(
        "Envelope contains a non-JSON internal value",
        context={"value_type": type(value).__name__},
    )


__all__ = [
    "CURRENT_ENVELOPE_VERSION",
    "DEFAULT_MAX_ENVELOPE_BYTES",
    "Envelope",
]
