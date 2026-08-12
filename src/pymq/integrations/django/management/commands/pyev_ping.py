"""Publish a portable pyev diagnostics message."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from pymq.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)


class Command(BaseCommand):
    help = "Publish a pyev diagnostics message through the configured engine"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--route", default="pyev.system.ping")

    def handle(self, *args: object, **options: Any) -> str:
        del args

        async def ping() -> dict[str, object]:
            async with running_broker() as broker:
                result = await broker.publish(
                    {"sent_at": datetime.now(UTC).isoformat()},
                    route=options["route"],
                )
                return {
                    "accepted": result.accepted,
                    "message_id": result.message_id,
                    "route": result.route,
                    "destination": result.destination,
                    "engine": result.engine,
                }

        output = json_text(run(ping()))
        return output
