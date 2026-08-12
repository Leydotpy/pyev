"""Safely replay selected dead-letter records."""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from broka.deadletter import ReplayManager
from broka.integrations.django.management.commands._base import (
    json_text,
    replay_publish,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "Replay selected records from the configured pyev dead-letter store"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("record_ids", nargs="*")
        parser.add_argument("--all", action="store_true", dest="replay_all")
        parser.add_argument("--confirm", action="store_true")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--destination")

    def handle(self, *args: object, **options: Any) -> str:
        del args
        ids = tuple(options["record_ids"])
        if not ids and not options["replay_all"]:
            raise CommandError("provide one or more record IDs or use --all")
        if ids and options["replay_all"]:
            raise CommandError("record IDs and --all are mutually exclusive")

        async def replay() -> list[dict[str, object]]:
            async with running_broker() as broker:
                manager = ReplayManager(
                    broker.dead_letters.store,
                    lambda envelope, destination: replay_publish(broker, envelope, destination),
                    broker.dead_letters.policy,
                )
                if options["replay_all"]:
                    results = await manager.replay_all(
                        destination_override=options["destination"],
                        dry_run=options["dry_run"],
                        confirm=options["confirm"],
                    )
                else:
                    results = await manager.replay_selected(
                        ids,
                        destination_override=options["destination"],
                        dry_run=options["dry_run"],
                        confirm=options["confirm"],
                    )
                return [
                    {
                        "record_id": item.record_id,
                        "outcome": item.outcome.value,
                        "destination": item.destination,
                        "replay_count": item.replay_count,
                        "error": item.error,
                    }
                    for item in results
                ]

        output = json_text(run(replay()))
        return output
