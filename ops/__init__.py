"""
Operations package for the Election Analysis Pipeline

This package centralizes all operational tools including:
- Configuration management
- Pipeline orchestration
- Schema monitoring
- CLI utilities

The Config class is exposed at the package level for convenient imports:
    from ops import Config
"""

from .config_loader import Config

__all__ = ["Config"]
