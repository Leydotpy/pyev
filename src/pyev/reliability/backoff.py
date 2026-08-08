"""Backoff strategies used by retry and recovery services.

Attempts are one-based: the first delay is calculated with ``attempt=1``.
Randomised strategies accept an injected random source so production code can
use system randomness while tests remain completely deterministic.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@runtime_checkable
class RandomSource(Protocol):
    """A source capable of sampling a uniform floating-point value."""

    def uniform(self, lower: float, upper: float) -> float:
        """Return a sample in the inclusive range ``lower`` to ``upper``."""


type UniformCallable = Callable[[float, float], float]


@runtime_checkable
class BackoffStrategy(Protocol):
    """Protocol implemented by retry delay strategies."""

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        """Return a non-negative delay for a one-based retry attempt."""


def _validate_attempt(attempt: int) -> None:
    if attempt < 1:
        raise ValueError("attempt must be at least 1")


def _validate_number(name: str, value: float, *, strictly_positive: bool = False) -> None:
    minimum = 0.0 if not strictly_positive else math.nextafter(0.0, 1.0)
    if not math.isfinite(value) or value < minimum:
        qualifier = "positive" if strictly_positive else "non-negative"
        raise ValueError(f"{name} must be a finite {qualifier} number")


def _bounded(value: float, maximum: float | None) -> float:
    return value if maximum is None else min(value, maximum)


def _sample(source: RandomSource | UniformCallable, lower: float, upper: float) -> float:
    sampler = source.uniform if isinstance(source, RandomSource) else source
    sampled = float(sampler(lower, upper))
    # A custom source is allowed, but it must honour the same contract as
    # random.Random.uniform. Clamping tiny floating point excursions is useful;
    # returning a wildly invalid value is a programming error.
    tolerance = max(1.0, abs(lower), abs(upper)) * 1e-12
    if sampled < lower - tolerance or sampled > upper + tolerance:
        raise ValueError("random source returned a value outside the requested range")
    return min(upper, max(lower, sampled))


@dataclass(frozen=True, slots=True)
class FixedBackoff:
    """Wait the same amount before every retry."""

    seconds: float = 1.0

    def __post_init__(self) -> None:
        _validate_number("seconds", self.seconds)

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        _validate_attempt(attempt)
        return self.seconds

    __call__ = delay


@dataclass(frozen=True, slots=True)
class LinearBackoff:
    """Increase the delay by a fixed amount after each attempt."""

    initial: float = 1.0
    increment: float = 1.0
    maximum: float | None = 60.0

    def __post_init__(self) -> None:
        _validate_number("initial", self.initial)
        _validate_number("increment", self.increment)
        if self.maximum is not None:
            _validate_number("maximum", self.maximum)
            if self.maximum < self.initial:
                raise ValueError("maximum must be greater than or equal to initial")

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        _validate_attempt(attempt)
        return _bounded(self.initial + self.increment * (attempt - 1), self.maximum)

    __call__ = delay


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """Increase the delay exponentially, without jitter."""

    initial: float = 1.0
    multiplier: float = 2.0
    maximum: float | None = 60.0

    def __post_init__(self) -> None:
        _validate_number("initial", self.initial)
        _validate_number("multiplier", self.multiplier, strictly_positive=True)
        if self.maximum is not None:
            _validate_number("maximum", self.maximum)
            if self.maximum < self.initial:
                raise ValueError("maximum must be greater than or equal to initial")

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        _validate_attempt(attempt)
        try:
            value = self.initial * self.multiplier ** (attempt - 1)
        except OverflowError:
            value = math.inf
        return _bounded(value, self.maximum)

    __call__ = delay


@dataclass(frozen=True, slots=True)
class ExponentialFullJitterBackoff:
    """Sample uniformly between zero and the exponential delay ceiling."""

    initial: float = 1.0
    multiplier: float = 2.0
    maximum: float | None = 60.0
    random_source: RandomSource | UniformCallable = field(default_factory=random.SystemRandom)

    def __post_init__(self) -> None:
        ExponentialBackoff(self.initial, self.multiplier, self.maximum)

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        ceiling = ExponentialBackoff(self.initial, self.multiplier, self.maximum).delay(attempt)
        return _sample(self.random_source, 0.0, ceiling)

    __call__ = delay


# The shorter name is convenient and is retained as the canonical public alias.
FullJitterBackoff = ExponentialFullJitterBackoff


@dataclass(frozen=True, slots=True)
class EqualJitterBackoff:
    """Return half the exponential delay plus jitter over the other half."""

    initial: float = 1.0
    multiplier: float = 2.0
    maximum: float | None = 60.0
    random_source: RandomSource | UniformCallable = field(default_factory=random.SystemRandom)

    def __post_init__(self) -> None:
        ExponentialBackoff(self.initial, self.multiplier, self.maximum)

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        ceiling = ExponentialBackoff(self.initial, self.multiplier, self.maximum).delay(attempt)
        half = ceiling / 2.0
        return half + _sample(self.random_source, 0.0, half)

    __call__ = delay


@dataclass(frozen=True, slots=True)
class DecorrelatedJitterBackoff:
    """Use decorrelated jitter based on the delay from the previous retry."""

    initial: float = 1.0
    multiplier: float = 3.0
    maximum: float = 60.0
    random_source: RandomSource | UniformCallable = field(default_factory=random.SystemRandom)

    def __post_init__(self) -> None:
        _validate_number("initial", self.initial)
        _validate_number("multiplier", self.multiplier, strictly_positive=True)
        _validate_number("maximum", self.maximum)
        if self.maximum < self.initial:
            raise ValueError("maximum must be greater than or equal to initial")

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        _validate_attempt(attempt)
        prior = self.initial if previous_delay is None else previous_delay
        _validate_number("previous_delay", prior)
        upper = max(self.initial, prior * self.multiplier)
        return min(self.maximum, _sample(self.random_source, self.initial, upper))

    __call__ = delay


@dataclass(frozen=True, slots=True)
class CallableBackoff:
    """Adapt an application callable to the :class:`BackoffStrategy` protocol."""

    function: Callable[[int, float | None], float]

    def delay(self, attempt: int, *, previous_delay: float | None = None) -> float:
        _validate_attempt(attempt)
        result = float(self.function(attempt, previous_delay))
        _validate_number("delay", result)
        return result

    __call__ = delay


def calculate_backoff(
    strategy: BackoffStrategy,
    attempt: int,
    *,
    previous_delay: float | None = None,
) -> float:
    """Calculate and validate a delay from any registered strategy."""

    value = float(strategy.delay(attempt, previous_delay=previous_delay))
    _validate_number("delay", value)
    return value


__all__ = [
    "BackoffStrategy",
    "CallableBackoff",
    "DecorrelatedJitterBackoff",
    "EqualJitterBackoff",
    "ExponentialBackoff",
    "ExponentialFullJitterBackoff",
    "FixedBackoff",
    "FullJitterBackoff",
    "LinearBackoff",
    "RandomSource",
    "UniformCallable",
    "calculate_backoff",
]
