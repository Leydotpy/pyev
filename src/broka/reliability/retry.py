"""Central retry policy and execution manager."""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol, TypeVar, runtime_checkable

from broka.exceptions import RetryExhaustedError
from broka.observability.redaction import redact_text

from .backoff import BackoffStrategy, ExponentialFullJitterBackoff, calculate_backoff

T = TypeVar("T")
type AsyncOperation[T] = Callable[[], Awaitable[T]]
type Sleep = Callable[[float], Awaitable[None]]
type Clock = Callable[[], float]


class FailureDecision(StrEnum):
    """An explicit classification of an operation failure."""

    RETRY = "retry"
    DO_NOT_RETRY = "do_not_retry"


@runtime_checkable
class ExceptionClassifier(Protocol):
    """Classify an exception without inspecting error-message text."""

    def classify(self, error: BaseException) -> FailureDecision:
        """Return whether ``error`` may be retried."""


@dataclass(frozen=True, slots=True)
class TypeExceptionClassifier:
    """Classify exceptions by type, with terminal types taking precedence."""

    retryable: tuple[type[BaseException], ...] = (Exception,)
    terminal: tuple[type[BaseException], ...] = ()

    def classify(self, error: BaseException) -> FailureDecision:
        if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            return FailureDecision.DO_NOT_RETRY
        if isinstance(error, self.terminal):
            return FailureDecision.DO_NOT_RETRY
        return (
            FailureDecision.RETRY
            if isinstance(error, self.retryable)
            else FailureDecision.DO_NOT_RETRY
        )


@dataclass(frozen=True, slots=True)
class CallableExceptionClassifier:
    """Adapt an application classifier callable."""

    function: Callable[[BaseException], bool | FailureDecision]

    def classify(self, error: BaseException) -> FailureDecision:
        result = self.function(error)
        if isinstance(result, FailureDecision):
            return result
        return FailureDecision.RETRY if result else FailureDecision.DO_NOT_RETRY


class TerminalAction(StrEnum):
    """Action a coordinator should take when retries are exhausted."""

    RAISE = "raise"
    DEAD_LETTER = "dead_letter"
    REJECT = "reject"


class CancellationBehaviour(StrEnum):
    """How cancellation is handled by a retry run."""

    PROPAGATE = "propagate"
    CLASSIFY = "classify"


@dataclass(frozen=True, slots=True)
class RetryError:
    """Sanitised metadata for a failed attempt."""

    attempt: int
    error_type: str
    message: str
    elapsed: float


@dataclass(frozen=True, slots=True)
class RetryNotification:
    """Information supplied to retry callbacks and operational events."""

    operation: str
    attempt: int
    next_attempt: int
    delay: float
    elapsed: float
    error: BaseException
    history: tuple[RetryError, ...]
    metadata: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class RetryContext:
    """Operation identity and low-cardinality metadata for a retry run."""

    operation: str = "operation"
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


class RetryBudget:
    """A thread-safe sliding-window retry budget shared across operations.

    The initial call does not consume the budget; every scheduled retry does.
    """

    def __init__(
        self,
        max_retries: int,
        *,
        window: float = 60.0,
        clock: Clock = time.monotonic,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if window <= 0:
            raise ValueError("window must be positive")
        self.max_retries = max_retries
        self.window = float(window)
        self._clock = clock
        self._uses: deque[float] = deque()
        self._lock = threading.Lock()

    def try_consume(self, *, now: float | None = None) -> bool:
        """Consume one retry token, returning ``False`` when exhausted."""

        current = self._clock() if now is None else now
        cutoff = current - self.window
        with self._lock:
            while self._uses and self._uses[0] <= cutoff:
                self._uses.popleft()
            if len(self._uses) >= self.max_retries:
                return False
            self._uses.append(current)
            return True

    def remaining(self, *, now: float | None = None) -> int:
        """Return the currently available number of retry tokens."""

        current = self._clock() if now is None else now
        cutoff = current - self.window
        with self._lock:
            while self._uses and self._uses[0] <= cutoff:
                self._uses.popleft()
            return max(0, self.max_retries - len(self._uses))

    def reset(self) -> None:
        """Return all tokens to the budget."""

        with self._lock:
            self._uses.clear()


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Validated policy controlling attempts, timing, and classification."""

    max_attempts: int = 3
    max_elapsed_time: float | None = None
    backoff: BackoffStrategy = field(default_factory=ExponentialFullJitterBackoff)
    classifier: ExceptionClassifier = field(default_factory=TypeExceptionClassifier)
    budget: RetryBudget | None = None
    attempt_timeout: float | None = None
    cancellation: CancellationBehaviour = CancellationBehaviour.PROPAGATE
    terminal_action: TerminalAction = TerminalAction.RAISE
    name: str = "default"

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.max_elapsed_time is not None and self.max_elapsed_time <= 0:
            raise ValueError("max_elapsed_time must be positive")
        if self.attempt_timeout is not None and self.attempt_timeout <= 0:
            raise ValueError("attempt_timeout must be positive")
        if not self.name:
            raise ValueError("name must not be empty")


type RetryCallback = Callable[[RetryNotification], Awaitable[None] | None]


class RetryManager:
    """Execute asynchronous operations according to a central retry policy."""

    def __init__(
        self,
        default_policy: RetryPolicy | None = None,
        *,
        sleep: Sleep = asyncio.sleep,
        clock: Clock = time.monotonic,
        on_retry: RetryCallback | None = None,
        event_emitter: object | None = None,
    ) -> None:
        self.default_policy = default_policy or RetryPolicy()
        self._sleep = sleep
        self._clock = clock
        self._on_retry = on_retry
        self._events = event_emitter

    async def run(
        self,
        operation: AsyncOperation[T],
        policy: RetryPolicy | None = None,
        *,
        context: RetryContext | None = None,
    ) -> T:
        """Run ``operation`` and either return its value or raise exhaustion.

        Cancellation propagates immediately by default. A terminally classified
        ordinary exception is re-raised unchanged so callers can distinguish it
        from a genuinely exhausted retry sequence.
        """

        selected = policy or self.default_policy
        retry_context = context or RetryContext()
        started = self._clock()
        history: list[RetryError] = []
        previous_delay: float | None = None

        for attempt in range(1, selected.max_attempts + 1):
            try:
                if selected.attempt_timeout is None:
                    return await operation()
                async with asyncio.timeout(selected.attempt_timeout):
                    return await operation()
            except asyncio.CancelledError as error:
                if selected.cancellation is CancellationBehaviour.PROPAGATE:
                    raise
                decision = selected.classifier.classify(error)
                if decision is FailureDecision.DO_NOT_RETRY:
                    raise
                caught: BaseException = error
            except Exception as error:
                decision = selected.classifier.classify(error)
                if decision is FailureDecision.DO_NOT_RETRY:
                    raise
                caught = error

            elapsed = max(0.0, self._clock() - started)
            history.append(
                RetryError(
                    attempt=attempt,
                    error_type=type(caught).__name__,
                    message=redact_text(str(caught)),
                    elapsed=elapsed,
                )
            )
            if attempt >= selected.max_attempts:
                self._raise_exhausted(selected, retry_context, history, elapsed, caught)
            if selected.max_elapsed_time is not None and elapsed >= selected.max_elapsed_time:
                self._raise_exhausted(selected, retry_context, history, elapsed, caught)
            if selected.budget is not None and not selected.budget.try_consume():
                self._raise_exhausted(
                    selected,
                    retry_context,
                    history,
                    elapsed,
                    caught,
                    reason="retry budget exhausted",
                )

            delay = calculate_backoff(
                selected.backoff,
                attempt,
                previous_delay=previous_delay,
            )
            if (
                selected.max_elapsed_time is not None
                and elapsed + delay > selected.max_elapsed_time
            ):
                self._raise_exhausted(
                    selected,
                    retry_context,
                    history,
                    elapsed,
                    caught,
                    reason="elapsed-time limit would be exceeded",
                )
            notification = RetryNotification(
                operation=retry_context.operation,
                attempt=attempt,
                next_attempt=attempt + 1,
                delay=delay,
                elapsed=elapsed,
                error=caught,
                history=tuple(history),
                metadata=retry_context.metadata,
            )
            await self._notify(notification)
            previous_delay = delay
            await self._sleep(delay)

        raise AssertionError("retry loop terminated unexpectedly")

    async def _notify(self, notification: RetryNotification) -> None:
        if self._on_retry is not None:
            result = self._on_retry(notification)
            if inspect.isawaitable(result):
                await result
        emitter = self._events
        if emitter is not None:
            emit = getattr(emitter, "emit", None)
            if emit is not None:
                result = emit(
                    "retried",
                    operation=notification.operation,
                    attempt=notification.attempt,
                    next_attempt=notification.next_attempt,
                    delay=notification.delay,
                    error_type=type(notification.error).__name__,
                )
                if inspect.isawaitable(result):
                    await result

    @staticmethod
    def _raise_exhausted(
        policy: RetryPolicy,
        context: RetryContext,
        history: list[RetryError],
        elapsed: float,
        error: BaseException,
        *,
        reason: str = "maximum attempts reached",
    ) -> None:
        details: dict[str, object] = {
            "operation": context.operation,
            "policy": policy.name,
            "attempts": len(history),
            "elapsed": elapsed,
            "reason": reason,
            "terminal_action": policy.terminal_action.value,
            "history": tuple(history),
            **dict(context.metadata),
        }
        raise RetryExhaustedError(
            f"{context.operation} failed after {len(history)} attempt(s): {reason}",
            retryable=False,
            context=details,
        ) from error

    async def call(
        self,
        operation: AsyncOperation[T],
        policy: RetryPolicy | None = None,
        *,
        context: RetryContext | None = None,
    ) -> T:
        """Alias for :meth:`run`, useful when composing service callables."""

        return await self.run(operation, policy, context=context)


def retry(
    policy: RetryPolicy | None = None,
    *,
    manager: RetryManager | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorate an async callable with a retry policy."""

    executor = manager if manager is not None else RetryManager(policy)

    def decorate(function: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        async def wrapped(*args: object, **kwargs: object) -> T:
            return await executor.run(
                lambda: function(*args, **kwargs),
                policy,
                context=RetryContext(operation=function.__qualname__),
            )

        wrapped.__name__ = function.__name__
        wrapped.__qualname__ = function.__qualname__
        wrapped.__doc__ = function.__doc__
        return wrapped

    return decorate


__all__ = [
    "AsyncOperation",
    "CallableExceptionClassifier",
    "CancellationBehaviour",
    "ExceptionClassifier",
    "FailureDecision",
    "RetryBudget",
    "RetryCallback",
    "RetryContext",
    "RetryError",
    "RetryManager",
    "RetryNotification",
    "RetryPolicy",
    "TerminalAction",
    "TypeExceptionClassifier",
    "retry",
]
