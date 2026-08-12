"""Starlette lifecycle helpers."""

from pymq.integrations.fastapi import dependency, lifespan

__all__ = ["dependency", "lifespan"]
