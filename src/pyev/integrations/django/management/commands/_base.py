"""Shared lifecycle and output helpers for pyev management commands."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType

from pyev.broker import Broker
from pyev.envelope import Envelope
from pyev.exceptions import PyevError
from pyev.integrations.django import get_broker


def run[T](coroutine: Coroutine[object, object, T]) -> T:
    """Run one command coroutine in Django's synchronous command process."""

    return asyncio.run(coroutine)


@asynccontextmanager
async def running_broker() -> AsyncIterator[Broker]:
    """Start and stop a broker only when the command acquired its lifecycle."""

    broker = get_broker()
    owns_lifecycle = not broker.ready
    if owns_lifecycle:
        await broker.startup()
    try:
        yield broker
    finally:
        if owns_lifecycle:
            await broker.shutdown()


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, MappingProxyType):
        return dict(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def json_text(value: object) -> str:
    """Encode stable, human-readable command output."""

    return json.dumps(value, default=_json_default, indent=2, sort_keys=True)


async def replay_publish(broker: Broker, envelope: object, destination: str | None) -> object:
    """Republish a retained envelope through the stable broker API."""

    route = destination
    headers = None
    if isinstance(envelope, Envelope):
        route = route or envelope.type
        headers = envelope.headers
        try:
            message = envelope.to_message(registry=broker.event_registry)
        except PyevError:
            message = dict(envelope.payload)
    else:
        message = envelope
    return await broker.publish(message, route=route, headers=headers)


__all__ = ["json_text", "replay_publish", "run", "running_broker"]
