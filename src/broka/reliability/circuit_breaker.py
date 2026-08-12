"""Asynchronous circuit breaker with sliding-window failure detection."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from broka.exceptions import CircuitOpenError

T = TypeVar("T")


class CircuitState(StrEnum):
    """Lifecycle states of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """Thresholds controlling circuit state transitions."""

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 1
    success_threshold: int = 1
    window_size: int = 20
    minimum_calls: int = 5
    failure_rate_threshold: float | None = None
    excluded_exceptions: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if self.recovery_timeout < 0:
            raise ValueError("recovery_timeout must be non-negative")
        if self.half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be at least 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be at least 1")
        if self.window_size < 1:
            raise ValueError("window_size must be at least 1")
        if not 1 <= self.minimum_calls <= self.window_size:
            raise ValueError("minimum_calls must be between 1 and window_size")
        if self.failure_rate_threshold is not None and not (
            0.0 <= self.failure_rate_threshold <= 1.0
        ):
            raise ValueError("failure_rate_threshold must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class CircuitBreakerSnapshot:
    """Immutable operational view of a circuit breaker."""

    name: str
    state: CircuitState
    consecutive_failures: int
    window_calls: int
    window_failures: int
    failure_rate: float
    half_open_inflight: int
    half_open_successes: int
    opened_at: float | None
    retry_after: float | None


class CircuitBreaker:
    """Protect an async operation from repeated downstream failure.

    The breaker supports both a consecutive-failure threshold and an optional
    sliding-window failure-rate threshold. State changes are serialized with an
    asyncio lock; user code and event listeners are never invoked while holding
    that lock.
    """

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        event_emitter: object | None = None,
    ) -> None:
        if not name:
            raise ValueError("name must not be empty")
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._clock = clock
        self._events = event_emitter
        self._state = CircuitState.CLOSED
        self._lock = asyncio.Lock()
        self._window: deque[bool] = deque(maxlen=self.config.window_size)
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open_inflight = 0
        self._half_open_successes = 0

    @property
    def state(self) -> CircuitState:
        """Return the current state (time-based transitions happen on calls)."""

        return self._state

    async def call(
        self,
        operation: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Execute an async operation when the circuit permits it."""

        await self.before_call()
        try:
            value = await operation(*args, **kwargs)
        except asyncio.CancelledError:
            await self._release_probe()
            raise
        except BaseException as error:
            if isinstance(error, self.config.excluded_exceptions):
                await self._record_excluded()
            else:
                await self.record_failure(error)
            raise
        else:
            await self.record_success()
            return value

    async def before_call(self) -> None:
        """Reserve permission for one call or raise :class:`CircuitOpenError`."""

        transition: tuple[CircuitState, CircuitState] | None = None
        retry_after: float | None = None
        async with self._lock:
            now = self._clock()
            if self._state is CircuitState.OPEN:
                assert self._opened_at is not None
                elapsed = max(0.0, now - self._opened_at)
                retry_after = max(0.0, self.config.recovery_timeout - elapsed)
                if retry_after <= 0.0:
                    previous = self._state
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_successes = 0
                    self._half_open_inflight = 0
                    transition = (previous, self._state)
                else:
                    self._raise_open(retry_after)
            if self._state is CircuitState.HALF_OPEN:
                if self._half_open_inflight >= self.config.half_open_max_calls:
                    self._raise_open(retry_after)
                self._half_open_inflight += 1
        if transition is not None:
            await self._emit_transition(*transition)

    async def record_success(self) -> None:
        """Record a successful protected operation."""

        transition: tuple[CircuitState, CircuitState] | None = None
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                self._half_open_successes += 1
                if self._half_open_successes >= self.config.success_threshold:
                    previous = self._state
                    self._close_locked()
                    transition = (previous, self._state)
            elif self._state is CircuitState.CLOSED:
                self._window.append(True)
                self._consecutive_failures = 0
        if transition is not None:
            await self._emit_transition(*transition)

    async def record_failure(self, error: BaseException | None = None) -> None:
        """Record a failed protected operation and open when policy requires."""

        transition: tuple[CircuitState, CircuitState] | None = None
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                previous_state: CircuitState = self._state
                self._open_locked()
                transition = (previous_state, self._state)
            elif self._state is CircuitState.CLOSED:
                self._window.append(False)
                self._consecutive_failures += 1
                if self._should_open_locked():
                    previous_state = self._state
                    self._open_locked()
                    transition = (previous_state, self._state)
        if transition is not None:
            await self._emit_transition(
                *transition,
                error_type=type(error).__name__ if error is not None else None,
            )

    async def reset(self) -> None:
        """Manually return the circuit to a clean closed state."""

        transition: tuple[CircuitState, CircuitState] | None = None
        async with self._lock:
            previous = self._state
            self._close_locked()
            if previous is not self._state:
                transition = (previous, self._state)
        if transition is not None:
            await self._emit_transition(*transition, manual=True)

    async def trip(self) -> None:
        """Manually open the circuit."""

        transition: tuple[CircuitState, CircuitState] | None = None
        async with self._lock:
            previous = self._state
            self._open_locked()
            if previous is not self._state:
                transition = (previous, self._state)
        if transition is not None:
            await self._emit_transition(*transition, manual=True)

    async def snapshot(self) -> CircuitBreakerSnapshot:
        """Return a consistent immutable state snapshot."""

        async with self._lock:
            failures = sum(not success for success in self._window)
            calls = len(self._window)
            retry_after: float | None = None
            if self._state is CircuitState.OPEN and self._opened_at is not None:
                retry_after = max(
                    0.0,
                    self.config.recovery_timeout - (self._clock() - self._opened_at),
                )
            return CircuitBreakerSnapshot(
                name=self.name,
                state=self._state,
                consecutive_failures=self._consecutive_failures,
                window_calls=calls,
                window_failures=failures,
                failure_rate=(failures / calls) if calls else 0.0,
                half_open_inflight=self._half_open_inflight,
                half_open_successes=self._half_open_successes,
                opened_at=self._opened_at,
                retry_after=retry_after,
            )

    async def _release_probe(self) -> None:
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)

    async def _record_excluded(self) -> None:
        # An excluded application error is evidence that the dependency was
        # reachable, so it counts as a successful breaker probe.
        await self.record_success()

    def _should_open_locked(self) -> bool:
        if self._consecutive_failures >= self.config.failure_threshold:
            return True
        threshold = self.config.failure_rate_threshold
        if threshold is None or len(self._window) < self.config.minimum_calls:
            return False
        failures = sum(not success for success in self._window)
        return failures / len(self._window) >= threshold

    def _open_locked(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._half_open_inflight = 0
        self._half_open_successes = 0

    def _close_locked(self) -> None:
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._consecutive_failures = 0
        self._window.clear()
        self._half_open_inflight = 0
        self._half_open_successes = 0

    def _raise_open(self, retry_after: float | None) -> None:
        raise CircuitOpenError(
            f"circuit {self.name!r} is open",
            retryable=True,
            context={"circuit": self.name, "retry_after": retry_after},
        )

    async def _emit_transition(
        self,
        previous: CircuitState,
        current: CircuitState,
        **details: object,
    ) -> None:
        emitter = self._events
        if emitter is None:
            return
        event_name = {
            CircuitState.OPEN: "circuit_opened",
            CircuitState.HALF_OPEN: "circuit_half_opened",
            CircuitState.CLOSED: "circuit_closed",
        }[current]
        emit = getattr(emitter, "emit", None)
        if emit is None:
            return
        result = emit(
            event_name,
            circuit=self.name,
            previous_state=previous.value,
            state=current.value,
            **details,
        )
        if inspect.isawaitable(result):
            await result


class CircuitBreakerRegistry:
    """An isolated collection of named circuit breakers."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}

    def register(self, breaker: CircuitBreaker, *, replace: bool = False) -> None:
        """Register a breaker, rejecting accidental name collisions."""

        if breaker.name in self._breakers and not replace:
            raise ValueError(f"circuit breaker {breaker.name!r} is already registered")
        self._breakers[breaker.name] = breaker

    def get(self, name: str) -> CircuitBreaker:
        """Return a registered breaker."""

        try:
            return self._breakers[name]
        except KeyError as error:
            raise KeyError(f"unknown circuit breaker {name!r}") from error

    def names(self) -> Sequence[str]:
        """Return registered names in deterministic order."""

        return tuple(sorted(self._breakers))

    async def snapshots(self) -> tuple[CircuitBreakerSnapshot, ...]:
        """Return snapshots for every breaker."""

        return tuple([await self._breakers[name].snapshot() for name in sorted(self._breakers)])


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitBreakerSnapshot",
    "CircuitState",
]
