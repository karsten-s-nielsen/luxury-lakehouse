"""Wheel version constants and rewrite utilities.

Single source of truth for the wheel version and HF Hub URL.
This module has zero external dependencies — stdlib only.

To bump the version, run: uv run python scripts/bump_wheel.py
"""

from __future__ import annotations

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WHEEL_VERSION = "0.3.20"
"""Must match the version in pyproject.toml. Enforced by test_wheel_constants.py."""

WHEEL_REPO = "luxury-lakehouse/build-artifacts"
"""HF Hub repository hosting the pre-built wheel."""

WHEEL_FILENAME = f"luxury_lakehouse-{WHEEL_VERSION}-py3-none-any.whl"
"""Wheel filename on HF Hub (PEP 427 format)."""

WHEEL_BASE_URL = f"https://huggingface.co/{WHEEL_REPO}/resolve/main/{WHEEL_FILENAME}"
"""Direct download URL for the wheel on HF Hub (no hash pinning)."""

# ---------------------------------------------------------------------------
# Compiled patterns (module-level per CLAUDE.md)
# ---------------------------------------------------------------------------

WHEEL_URL_RE: re.Pattern[str] = re.compile(
    r"luxury_lakehouse-\d+\.\d+\.\d+-py3-none-any\.whl"
    r"(#sha256=[a-fA-F0-9]+)?"
)
"""Matches any versioned wheel filename, with optional SHA-256 fragment."""

_WHEEL_VERSION_RE: re.Pattern[str] = re.compile(r'WHEEL_VERSION\s*=\s*"\d+\.\d+\.\d+"')
"""Matches the WHEEL_VERSION constant assignment in this file."""

_PYPROJECT_VERSION_RE: re.Pattern[str] = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)
"""Matches the version field in pyproject.toml [project] section."""

# ---------------------------------------------------------------------------
# Pure rewrite functions
# ---------------------------------------------------------------------------


def rewrite_wheel_url(text: str, version: str, sha256: str | None = None) -> str:
    """Replace all wheel filename references with the given version and optional hash.

    Handles PEP 723 headers, Terraform paths, shell scripts, and any other
    context containing ``luxury_lakehouse-X.Y.Z-py3-none-any.whl``.
    """
    replacement = f"luxury_lakehouse-{version}-py3-none-any.whl"
    if sha256:
        replacement += f"#sha256={sha256}"
    return WHEEL_URL_RE.sub(replacement, text)


def rewrite_wheel_version_constant(text: str, version: str) -> str:
    """Replace ``WHEEL_VERSION = "X.Y.Z"`` with the new version."""
    return _WHEEL_VERSION_RE.sub(f'WHEEL_VERSION = "{version}"', text)


def read_pyproject_version(project_root: Path) -> str:
    """Read the ``version`` field from ``pyproject.toml``."""
    pyproject = project_root / "pyproject.toml"
    match = _PYPROJECT_VERSION_RE.search(pyproject.read_text(encoding="utf-8"))
    if not match:
        msg = f"Could not find version in {pyproject}"
        raise ValueError(msg)
    return match.group(1)
