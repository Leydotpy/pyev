"""Optional application-framework integrations.

Importing this namespace does not import any third-party framework.
"""

from pymq.integrations.asgi import ASGIBrokerMiddleware, broker_lifespan
from pymq.integrations.cli import broker_daemon, serve_until_stopped, shutdown_signals

__all__ = [
    "ASGIBrokerMiddleware",
    "broker_daemon",
    "broker_lifespan",
    "serve_until_stopped",
    "shutdown_signals",
]
