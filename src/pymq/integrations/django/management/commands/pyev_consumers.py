"""Report portable consumer diagnostics."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand

from pymq.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "Show consumer counts exposed by the configured pyev broker"

    def handle(self, *args: object, **options: Any) -> str:
        del args, options

        async def inspect() -> dict[str, object]:
            async with running_broker() as broker:
                health = await broker.health()
                return {
                    "engine": health.selected_engine,
                    "active_consumers": health.active_consumers,
                    "consumer_lag": health.consumer_lag,
                    "queue_depth": health.queue_depth,
                    "note": "Counts are local to this management-command process.",
                }

        output = json_text(run(inspect()))
        return output
