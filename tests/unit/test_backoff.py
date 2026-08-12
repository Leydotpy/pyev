from __future__ import annotations

import pytest

from broka.reliability.backoff import (
    DecorrelatedJitterBackoff,
    EqualJitterBackoff,
    ExponentialBackoff,
    ExponentialFullJitterBackoff,
    FixedBackoff,
    LinearBackoff,
)


class MidpointRandom:
    def uniform(self, lower: float, upper: float) -> float:
        return (lower + upper) / 2


def test_deterministic_backoff_algorithms_and_caps() -> None:
    assert FixedBackoff(2).delay(4) == 2
    assert LinearBackoff(1, 2, 5).delay(4) == 5
    assert ExponentialBackoff(1, 2, 5).delay(4) == 5


def test_jitter_algorithms_accept_injected_random_source() -> None:
    random_source = MidpointRandom()
    assert ExponentialFullJitterBackoff(2, 2, 100, random_source).delay(3) == 4
    assert EqualJitterBackoff(2, 2, 100, random_source).delay(3) == 6
    assert DecorrelatedJitterBackoff(1, 3, 100, random_source).delay(2, previous_delay=4) == 6.5


@pytest.mark.parametrize("attempt", [0, -1])
def test_attempts_are_one_based(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt"):
        FixedBackoff().delay(attempt)


def test_invalid_backoff_configuration_fails_early() -> None:
    with pytest.raises(ValueError):
        ExponentialBackoff(initial=2, maximum=1)
    with pytest.raises(ValueError):
        LinearBackoff(increment=-1)
