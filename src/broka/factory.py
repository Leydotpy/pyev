"""Configuration-driven construction for the canonical Broker façade."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, cast

from broka.config import BrokerConfig

if TYPE_CHECKING:
    from broka.broker import Broker


class BrokerFactory:
    """Build brokers while keeping construction free of import-time runtime work."""

    @staticmethod
    def create(
        config: BrokerConfig | Mapping[str, object] | None = None,
        **dependencies: object,
    ) -> Broker:
        """Create a broker from validated config and injected dependencies."""

        from broka.broker import Broker

        broker_config = (
            config if isinstance(config, BrokerConfig) else BrokerConfig.from_mapping(config)
        )
        broker_type = cast(Callable[..., Broker], Broker)
        return broker_type(config=broker_config, **dependencies)


__all__ = ["BrokerFactory"]
