"""Shared redaction helpers for health, logging, and dead-letter records."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Final

REDACTED: Final = "[REDACTED]"
DEFAULT_SECRET_KEYS: Final[frozenset[str]] = frozenset(
    {
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "access_key",
        "private_key",
        "connection_string",
    }
)
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<auth>[^/@\s]+)@", re.I)
_SECRET_ASSIGNMENT = re.compile(
    r"(?P<prefix>\b(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|credential|connection[_-]?string)\b[\"']?\s*(?:=|:)\s*)"
    r"(?P<quote>[\"']?)(?P<value>[^\s,;\"']+)(?P=quote)",
    re.I,
)
_AUTHORIZATION_VALUE = re.compile(
    r"(?P<prefix>\bauthorization\b[\"']?\s*(?:=|:)\s*)"
    r"(?:(?:bearer|basic)\s+)?[^\s,;\"']+",
    re.I,
)


def is_sensitive_key(key: str, extra_keys: frozenset[str] = frozenset()) -> bool:
    """Return whether a mapping key conventionally contains a secret."""

    normalized = key.casefold().replace("-", "_")
    keys = DEFAULT_SECRET_KEYS | frozenset(item.casefold().replace("-", "_") for item in extra_keys)
    return normalized in keys or any(normalized.endswith(f"_{item}") for item in keys)


def redact_text(value: str) -> str:
    """Remove URL credentials and common inline secret assignments."""

    redacted = _URL_CREDENTIALS.sub(
        lambda match: f"{match.group('scheme')}{REDACTED}@",
        value,
    )
    redacted = _AUTHORIZATION_VALUE.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        redacted,
    )
    return _SECRET_ASSIGNMENT.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        redacted,
    )


def redact_value(
    value: object,
    *,
    extra_keys: frozenset[str] = frozenset(),
    max_depth: int = 8,
) -> object:
    """Recursively copy a value while redacting likely credentials."""

    if max_depth < 0:
        return "[MAX_DEPTH]"
    if isinstance(value, Mapping):
        return {
            str(key): (
                REDACTED
                if is_sensitive_key(str(key), extra_keys)
                else redact_value(item, extra_keys=extra_keys, max_depth=max_depth - 1)
            )
            for key, item in value.items()
        }
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        converted = [
            redact_value(item, extra_keys=extra_keys, max_depth=max_depth - 1) for item in value
        ]
        return tuple(converted) if isinstance(value, tuple) else converted
    return value


def redact_mapping(
    value: Mapping[str, object],
    *,
    extra_keys: frozenset[str] = frozenset(),
) -> dict[str, object]:
    """Return a redacted plain-dict copy of a mapping."""

    redacted = redact_value(value, extra_keys=extra_keys)
    assert isinstance(redacted, dict)
    return redacted


__all__ = [
    "DEFAULT_SECRET_KEYS",
    "REDACTED",
    "is_sensitive_key",
    "redact_mapping",
    "redact_text",
    "redact_value",
]
