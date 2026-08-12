"""Structured logging adapter with secret and payload redaction."""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping
from typing import Any

from .redaction import REDACTED, redact_mapping


class StructuredLogAdapter(logging.LoggerAdapter[logging.Logger]):
    """Attach stable structured fields without logging payloads by default."""

    def __init__(
        self,
        logger: logging.Logger,
        context: Mapping[str, object] | None = None,
        *,
        include_payload: bool = False,
    ) -> None:
        super().__init__(logger, redact_mapping(context or {}))
        self.include_payload = include_payload

    def process(
        self,
        msg: object,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[object, MutableMapping[str, Any]]:
        raw_extra = kwargs.get("extra")
        supplied = (
            {str(key): value for key, value in raw_extra.items()}
            if isinstance(raw_extra, Mapping)
            else {}
        )
        fields = {**dict(self.extra or {}), **redact_mapping(supplied)}
        if "payload" in fields and not self.include_payload:
            fields["payload"] = REDACTED
        kwargs["extra"] = fields
        return msg, kwargs

    def event(
        self,
        event_name: str,
        *,
        level: int = logging.INFO,
        message: str | None = None,
        **fields: object,
    ) -> None:
        """Log one framework event using ``event`` as a structured field."""

        self.log(level, message or event_name, extra={"event": event_name, **fields})


def get_logger(
    name: str = "pyev",
    *,
    context: Mapping[str, object] | None = None,
    include_payload: bool = False,
) -> StructuredLogAdapter:
    """Return a redacting structured adapter around a standard logger."""

    return StructuredLogAdapter(
        logging.getLogger(name),
        context,
        include_payload=include_payload,
    )


__all__ = ["StructuredLogAdapter", "get_logger"]
