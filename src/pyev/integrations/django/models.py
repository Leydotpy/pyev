"""Opt-in Django model foundations for durable pyev integrations.

Only abstract models live in pyev itself, so installing the Django integration
does not create database tables.  Applications that want a durable outbox must
subclass :class:`AbstractOutboxRecord` and create their own migration.
"""

from __future__ import annotations

from django.db import models


class AbstractOutboxRecord(models.Model):
    """Abstract storage schema consumed by ``DjangoModelOutboxStore``."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        LEASED = "leased", "Leased"
        PUBLISHED = "published", "Published"
        FAILED = "failed", "Failed"
        DEAD_LETTERED = "dead_lettered", "Dead lettered"

    id = models.UUIDField(primary_key=True, editable=False)
    payload = models.BinaryField()
    destination = models.CharField(max_length=512, db_index=True)
    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    created_at = models.DateTimeField(db_index=True)
    available_at = models.DateTimeField(db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(null=True, blank=True)
    lease_id = models.UUIDField(null=True, blank=True, db_index=True)
    leased_until = models.DateTimeField(null=True, blank=True, db_index=True)
    published_at = models.DateTimeField(null=True, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        abstract = True
        ordering = ("available_at", "created_at", "id")


__all__ = ["AbstractOutboxRecord"]
