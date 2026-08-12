from __future__ import annotations

import pytest

from pymq.subscription import SubscriptionOptions


def test_subscription_capacity_is_positive_and_explicit() -> None:
    assert SubscriptionOptions().capacity == 100
    assert SubscriptionOptions(capacity=25).capacity == 25
    with pytest.raises(ValueError, match="capacity"):
        SubscriptionOptions(capacity=0)
