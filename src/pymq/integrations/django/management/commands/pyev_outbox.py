"""Inspect and dispatch a configured Django outbox."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser

from pymq.integrations.django.management.commands._base import (
    json_text,
    run,
    running_broker,
)
from pymq.integrations.django.outbox import build_outbox_dispatcher, get_outbox_store
from pymq.reliability.outbox import OutboxStatus


class Command(BaseCommand):
    help = "List, dispatch, or purge a configured pyev transactional outbox"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "action", choices=("list", "dispatch", "purge"), default="list", nargs="?"
        )
        parser.add_argument("--status", choices=[item.value for item in OutboxStatus])
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--before-days", type=float, default=30.0)
        parser.add_argument("--confirm", action="store_true")

    def handle(self, *args: object, **options: Any) -> str:
        del args
        if options["limit"] < 1:
            raise CommandError("--limit must be at least 1")

        async def execute() -> object:
            store = get_outbox_store()
            assert store is not None
            action = options["action"]
            if action == "list":
                callback = getattr(store, "list", None)
                if callback is None:
                    raise CommandError("the configured OutboxStore does not support inspection")
                result = callback(
                    status=(OutboxStatus(options["status"]) if options["status"] else None),
                )
                messages = await result if inspect.isawaitable(result) else result
                return [
                    {
                        "id": item.id,
                        "status": item.status.value,
                        "destination": item.destination,
                        "attempts": item.attempts,
                        "available_at": item.available_at.isoformat(),
                        "last_error": item.last_error,
                    }
                    for item in tuple(messages)[: options["limit"]]
                ]
            if action == "dispatch":
                async with running_broker() as broker:
                    count = await build_outbox_dispatcher(broker, store=store).dispatch_once()
                    return {"leased": count}
            if not options["confirm"]:
                raise CommandError("outbox purge requires --confirm")
            callback = getattr(store, "purge_published", None)
            if callback is None:
                raise CommandError("the configured OutboxStore does not support purge")
            before = datetime.now(UTC) - timedelta(days=options["before_days"])
            result = callback(before=before)
            deleted = await result if inspect.isawaitable(result) else result
            return {"deleted": deleted, "before": before.isoformat()}

        output = json_text(run(execute()))
        return output
