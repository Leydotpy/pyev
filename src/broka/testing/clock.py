"""Deterministic clock and retry scheduler utilities."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


class DeterministicClock:
    """Controllable monotonic and UTC clock with optional auto-advance sleep."""

    def __init__(
        self,
        *,
        monotonic: float = 0.0,
        wall_time: datetime | None = None,
        auto_advance: bool = True,
    ) -> None:
        if monotonic < 0:
            raise ValueError("monotonic must be non-negative")
        selected_wall_time = wall_time or datetime(2026, 1, 1, tzinfo=UTC)
        if selected_wall_time.tzinfo is None:
            raise ValueError("wall_time must be timezone-aware")
        self._monotonic = float(monotonic)
        self._wall_time = selected_wall_time
        self.auto_advance = auto_advance
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def __call__(self) -> float:
        """Return monotonic time, making the object directly injectable."""

        return self._monotonic

    def monotonic(self) -> float:
        """Return monotonic time."""

        return self._monotonic

    def utcnow(self) -> datetime:
        """Return timezone-aware wall time."""

        return self._wall_time

    @property
    def now(self) -> float:
        """Readable monotonic time property."""

        return self._monotonic

    def advance(self, seconds: float) -> None:
        """Advance both clocks and wake sleepers whose deadlines passed."""

        if seconds < 0:
            raise ValueError("seconds must be non-negative")
        self._monotonic += seconds
        self._wall_time += timedelta(seconds=seconds)
        pending: list[tuple[float, asyncio.Future[None]]] = []
        for deadline, future in self._sleepers:
            if deadline <= self._monotonic:
                if not future.done():
                    future.set_result(None)
            else:
                pending.append((deadline, future))
        self._sleepers = pending

    async def sleep(self, seconds: float) -> None:
        """Sleep deterministically, either advancing immediately or awaiting advance."""

        if seconds < 0:
            raise ValueError("seconds must be non-negative")
        if self.auto_advance:
            self.advance(seconds)
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._sleepers.append((self._monotonic + seconds, future))
        self._sleepers.sort(key=lambda item: item[0])
        try:
            await future
        finally:
            self._sleepers = [item for item in self._sleepers if item[1] is not future]

    @property
    def pending_sleeps(self) -> tuple[float, ...]:
        """Return pending deadlines for assertions."""

        return tuple(deadline for deadline, _ in self._sleepers)


@dataclass(slots=True)
class DeterministicRetryScheduler:
    """Injectable sleeper that records every requested retry delay."""

    clock: DeterministicClock = field(default_factory=DeterministicClock)
    delays: list[float] = field(default_factory=list)

    async def sleep(self, seconds: float) -> None:
        """Record and perform a deterministic sleep."""

        self.delays.append(seconds)
        await self.clock.sleep(seconds)


__all__ = ["DeterministicClock", "DeterministicRetryScheduler"]
