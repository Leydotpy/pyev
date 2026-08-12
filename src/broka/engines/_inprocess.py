"""Shared, private primitives for in-process engines."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from fnmatch import fnmatchcase

from broka.engines.base import EnginePublishContext, EngineSubscription


class AcknowledgementAction(StrEnum):
    """Action selected by the delivery acknowledgement adapter."""

    PENDING = "pending"
    ACK = "ack"
    REJECT = "reject"
    REQUEUE = "requeue"
    DEFER = "defer"


class InProcessAcknowledgement:
    """Record an acknowledgement decision for an in-process consumer loop."""

    __slots__ = ("action", "delay", "touch_extension")

    def __init__(self) -> None:
        self.action = AcknowledgementAction.PENDING
        self.delay = 0.0
        self.touch_extension = 0.0

    def _finish(self, action: AcknowledgementAction) -> None:
        if self.action is action:
            return
        if self.action is not AcknowledgementAction.PENDING:
            raise RuntimeError(
                f"conflicting acknowledgement: {self.action.value} then {action.value}"
            )
        self.action = action

    async def ack(self) -> None:
        self._finish(AcknowledgementAction.ACK)

    async def nack(self, requeue: bool = True) -> None:
        self._finish(AcknowledgementAction.REQUEUE if requeue else AcknowledgementAction.REJECT)

    async def reject(self) -> None:
        self._finish(AcknowledgementAction.REJECT)

    async def requeue(self) -> None:
        self._finish(AcknowledgementAction.REQUEUE)

    async def defer(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("defer delay cannot be negative")
        self.delay = delay
        self._finish(AcknowledgementAction.DEFER)

    async def touch(self, extension: float) -> None:
        if extension <= 0:
            raise ValueError("touch extension must be positive")
        self.touch_extension += extension


@dataclass(frozen=True, slots=True)
class InProcessItem:
    """One serialized envelope waiting for in-process delivery."""

    destination: str
    payload: bytes
    context: EnginePublishContext
    attempt: int = 1


def subscription_matches(
    subscription: EngineSubscription,
    destination: str,
    context: EnginePublishContext,
) -> bool:
    """Match a logical transport destination and optional exact headers."""

    pattern = subscription.pattern or subscription.destination
    route_match = pattern == "*" or fnmatchcase(destination, pattern)
    if not route_match:
        return False
    return all(context.headers.get(key) == value for key, value in subscription.headers.items())
