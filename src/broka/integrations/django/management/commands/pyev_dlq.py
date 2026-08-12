"""Inspect transport-independent dead-letter records."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from broka.deadletter import DeadLetterFilter, DeadLetterStatus
from broka.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "List sanitized records from the configured pyev dead-letter store"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--status", choices=[item.value for item in DeadLetterStatus])
        parser.add_argument("--limit", type=int, default=100)

    def handle(self, *args: object, **options: Any) -> str:
        del args
        if options["limit"] < 1:
            raise ValueError("--limit must be at least 1")

        async def inspect() -> list[dict[str, object]]:
            status = options["status"]
            filters = DeadLetterFilter(
                statuses=(frozenset({DeadLetterStatus(status)}) if status else None),
                limit=options["limit"],
            )
            async with running_broker() as broker:
                records = await broker.dead_letters.filter(filters)
                return [
                    {
                        "id": item.id,
                        "status": item.status.value,
                        "event_type": item.event_type,
                        "route": item.route,
                        "destination": item.destination,
                        "engine": item.engine,
                        "error_type": item.error_type,
                        "error_message": item.error_message,
                        "dead_lettered_at": item.dead_lettered_at.isoformat(),
                        "replay_count": item.replay_count,
                    }
                    for item in records
                ]

        output = json_text(run(inspect()))
        return output
