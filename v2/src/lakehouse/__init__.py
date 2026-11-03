"""Lakehouse v2: ingestion of open soccer data into a DuckLake catalog.

The design spec is v2/docs/specs/2026-10-26-v2-ingestion-design.md.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("lakehouse")
except PackageNotFoundError:  # running from a source tree that is not installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
