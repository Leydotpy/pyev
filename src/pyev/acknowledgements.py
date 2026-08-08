"""Unified acknowledgement modes and transport-adapter contracts."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import NoReturn, Protocol, runtime_checkable

from .exceptions import UnsupportedCapabilityError


class AcknowledgementMode(StrEnum):
    """Policy controlling who acknowledges an inbound delivery."""

    AUTO = "auto"
    MANUAL = "manual"
    BATCH = "batch"
    NONE = "none"


@runtime_checkable
class AcknowledgementAdapter(Protocol):
    """Transport-specific operations used only through :class:`Delivery`."""

    async def ack(self) -> None:
        """Acknowledge successful processing."""

    async def nack(self, requeue: bool = True) -> None:
        """Negatively acknowledge processing, optionally requesting redelivery."""

    async def reject(self) -> None:
        """Reject a delivery without implicit redelivery."""

    async def requeue(self) -> None:
        """Explicitly place a delivery back into its queue."""

    async def defer(self, delay: float) -> None:
        """Defer a delivery for at least ``delay`` seconds."""

    async def touch(self, extension: float) -> None:
        """Extend a delivery lease or visibility timeout."""


class BaseAcknowledgementAdapter:
    """Convenience base whose unsupported operations fail explicitly.

    Engine adapters may override only the operations they can implement or
    safely emulate.  No method silently degrades to a no-op.
    """

    async def ack(self) -> None:
        """Acknowledge a delivery, unsupported by default."""

        self._unsupported("ack")

    async def nack(self, requeue: bool = True) -> None:
        """Negatively acknowledge a delivery, unsupported by default."""

        del requeue
        self._unsupported("nack")

    async def reject(self) -> None:
        """Reject a delivery, unsupported by default."""

        self._unsupported("reject")

    async def requeue(self) -> None:
        """Requeue a delivery, unsupported by default."""

        self._unsupported("requeue")

    async def defer(self, delay: float) -> None:
        """Defer a delivery, unsupported by default."""

        del delay
        self._unsupported("defer")

    async def touch(self, extension: float) -> None:
        """Extend a delivery lease, unsupported by default."""

        del extension
        self._unsupported("touch")

    @staticmethod
    def _unsupported(operation: str) -> NoReturn:
        raise UnsupportedCapabilityError("acknowledgement." + operation, operation=operation)


class NoOpAcknowledgementAdapter:
    """A successful adapter for transports with no physical acknowledgement.

    This is intended for local/in-memory engines where completing the framework
    state transition *is* the acknowledgement, not as a fallback for an engine
    that lacks a requested operation.
    """

    async def ack(self) -> None:
        """Complete a local acknowledgement."""

    async def nack(self, requeue: bool = True) -> None:
        """Complete a local negative acknowledgement."""

        del requeue

    async def reject(self) -> None:
        """Complete a local rejection."""

    async def requeue(self) -> None:
        """Complete a local requeue request."""

    async def defer(self, delay: float) -> None:
        """Complete a local defer request managed by the framework scheduler."""

        del delay

    async def touch(self, extension: float) -> None:
        """Complete a local lease extension."""

        del extension


AckCallback = Callable[[], Awaitable[None]]
NackCallback = Callable[[bool], Awaitable[None]]
DelayCallback = Callable[[float], Awaitable[None]]


class CallbackAcknowledgementAdapter(BaseAcknowledgementAdapter):
    """Adapter useful to engines that expose acknowledgement callables."""

    def __init__(
        self,
        *,
        ack: AckCallback | None = None,
        nack: NackCallback | None = None,
        reject: AckCallback | None = None,
        requeue: AckCallback | None = None,
        defer: DelayCallback | None = None,
        touch: DelayCallback | None = None,
    ) -> None:
        self._ack_callback = ack
        self._nack_callback = nack
        self._reject_callback = reject
        self._requeue_callback = requeue
        self._defer_callback = defer
        self._touch_callback = touch

    async def ack(self) -> None:
        """Invoke the configured acknowledgement callback."""

        if self._ack_callback is None:
            self._unsupported("ack")
        await self._ack_callback()

    async def nack(self, requeue: bool = True) -> None:
        """Invoke the configured negative-acknowledgement callback."""

        if self._nack_callback is None:
            self._unsupported("nack")
        await self._nack_callback(requeue)

    async def reject(self) -> None:
        """Invoke the configured rejection callback."""

        if self._reject_callback is None:
            self._unsupported("reject")
        await self._reject_callback()

    async def requeue(self) -> None:
        """Invoke the configured requeue callback."""

        if self._requeue_callback is None:
            self._unsupported("requeue")
        await self._requeue_callback()

    async def defer(self, delay: float) -> None:
        """Invoke the configured deferral callback."""

        if self._defer_callback is None:
            self._unsupported("defer")
        await self._defer_callback(delay)

    async def touch(self, extension: float) -> None:
        """Invoke the configured lease-extension callback."""

        if self._touch_callback is None:
            self._unsupported("touch")
        await self._touch_callback(extension)


__all__ = [
    "AckCallback",
    "AcknowledgementAdapter",
    "AcknowledgementMode",
    "BaseAcknowledgementAdapter",
    "CallbackAcknowledgementAdapter",
    "DelayCallback",
    "NackCallback",
    "NoOpAcknowledgementAdapter",
]
