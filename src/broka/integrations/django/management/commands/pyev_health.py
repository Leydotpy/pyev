"""Report aggregate pyev health."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from broka.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "Check this process's configured pyev broker"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--fail-on-unready", action="store_true")

    def handle(self, *args: object, **options: Any) -> str:
        del args

        async def check() -> dict[str, object]:
            async with running_broker() as broker:
                report = await broker.health()
                return {
                    "status": report.status.value,
                    "live": report.live,
                    "ready": report.ready,
                    "lifecycle_state": report.lifecycle_state,
                    "engine": report.selected_engine,
                    "connection_state": report.connection_state,
                    "active_consumers": report.active_consumers,
                    "queue_depth": report.queue_depth,
                    "publish_failures": report.publish_failures,
                    "retry_count": report.retry_count,
                    "dead_letter_count": report.dead_letter_count,
                    "checked_at": report.checked_at.isoformat(),
                }

        payload = run(check())
        if options["fail_on_unready"] and not payload["ready"]:
            raise CommandError(json_text(payload))
        output = json_text(payload)
        return output
