"""Starlette lifecycle helpers."""

from pyev.integrations.fastapi import dependency, lifespan

__all__ = ["dependency", "lifespan"]
