"""Flush optional producer buffers and perform graceful shutdown."""

from __future__ import annotations

import inspect
from typing import Any

from django.core.management.base import BaseCommand

from pyev.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "Flush engine producer buffers when supported, then shut down cleanly"

    def handle(self, *args: object, **options: Any) -> str:
        del args, options

        async def flush() -> dict[str, object]:
            async with running_broker() as broker:
                engine = broker.engine
                callback = getattr(engine, "flush", None) if engine is not None else None
                native = callable(callback)
                if callback is not None:
                    result = callback()
                    if inspect.isawaitable(result):
                        await result
                return {
                    "engine": engine.name if engine is not None else None,
                    "native_flush": native,
                    "graceful_shutdown": True,
                }

        output = json_text(run(flush()))
        return output
