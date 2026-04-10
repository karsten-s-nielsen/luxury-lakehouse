"""Wheel conformance tests -- version consistency across all consumers.

Verifies that every file referencing the luxury-lakehouse wheel uses the
current version from ``shared.wheel``.  When a consumer drifts (e.g. still
references 0.1.0 while WHEEL_VERSION is 0.3.0), the test fails with an
actionable message pointing the developer to ``bump_wheel.py``.

These tests are designed to catch drift in CI *before* a stale wheel
URL causes a runtime failure on HF Jobs or in a deploy script.
"""

from __future__ import annotations

import glob as globmod
from pathlib import Path

import pytest

from shared.wheel import WHEEL_FILENAME

# ---------------------------------------------------------------------------
# Project root — src/tests/test_wheel_conformance.py is 3 levels deep
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BUMP_MSG = "Run: uv run python scripts/bump_wheel.py"


def _read_text(path: Path) -> str:
    """Read a file as UTF-8 text."""
    return path.read_text(encoding="utf-8")


def _find_pep723_scripts() -> list[Path]:
    """Return all PEP 723 scripts that consume the wheel.

    Includes ``scripts/*_hf.py`` and ``scripts/train_football2vec_*.py``.
    """
    scripts_dir = _PROJECT_ROOT / "scripts"
    patterns = ["*_hf.py", "train_football2vec_*.py"]
    paths: set[Path] = set()
    for pattern in patterns:
        paths.update(Path(p) for p in globmod.glob(str(scripts_dir / pattern)))
    return sorted(paths)


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestPep723ScriptsVersion:
    """PEP 723 scripts must reference the current wheel filename."""

    def test_pep723_scripts_use_current_version(self) -> None:
        scripts = _find_pep723_scripts()
        assert scripts, "No PEP 723 scripts found -- glob patterns may be wrong"

        stale: list[str] = []
        for script in scripts:
            text = _read_text(script)
            # Only check scripts that actually reference the wheel
            if "luxury_lakehouse-" in text and WHEEL_FILENAME not in text:
                stale.append(script.name)

        assert not stale, f"Stale wheel version in {len(stale)} PEP 723 script(s): {', '.join(stale)}. {_BUMP_MSG}"


class TestEmbeddedWorkerScripts:
    """Worker scripts that embed the wheel URL must import from shared.wheel."""

    def test_evolve_backend_url(self) -> None:
        path = _PROJECT_ROOT / "src" / "evolve" / "backends" / "hf_jobs.py"
        text = _read_text(path)
        assert "from shared.wheel import" in text, (
            "src/evolve/backends/hf_jobs.py should import WHEEL_BASE_URL from "
            f"shared.wheel instead of hardcoding the URL. {_BUMP_MSG}"
        )
        assert "{WHEEL_BASE_URL}" in text, (
            "src/evolve/backends/hf_jobs.py should interpolate WHEEL_BASE_URL "
            f"in the worker script f-string. {_BUMP_MSG}"
        )

    def test_benchmark_worker_url(self) -> None:
        path = _PROJECT_ROOT / "scripts" / "benchmark_hf_jobs.py"
        text = _read_text(path)
        assert "from shared.wheel import" in text, (
            "scripts/benchmark_hf_jobs.py should import WHEEL_BASE_URL from "
            f"shared.wheel instead of hardcoding the URL. {_BUMP_MSG}"
        )
        assert "{WHEEL_BASE_URL}" in text, (
            f"scripts/benchmark_hf_jobs.py should interpolate WHEEL_BASE_URL in the worker script f-string. {_BUMP_MSG}"
        )


class TestDeployScriptsVersion:
    """Deploy scripts must reference the current wheel or import from shared.wheel."""

    def test_deploy_sh(self) -> None:
        path = _PROJECT_ROOT / "scripts" / "deploy.sh"
        if not path.exists():
            pytest.skip("scripts/deploy.sh not found (may be deprecated)")
        text = _read_text(path)
        assert WHEEL_FILENAME in text, (
            f"scripts/deploy.sh does not contain WHEEL_FILENAME ({WHEEL_FILENAME!r}). {_BUMP_MSG}"
        )

    def test_manage_space_uses_import(self) -> None:
        path = _PROJECT_ROOT / "scripts" / "manage_space.py"
        text = _read_text(path)
        assert "from shared.wheel import" in text, (
            "scripts/manage_space.py should import wheel constants from "
            f"shared.wheel instead of hardcoding them. {_BUMP_MSG}"
        )

    def test_deploy_wheel_uses_import(self) -> None:
        path = _PROJECT_ROOT / "scripts" / "deploy_wheel.py"
        text = _read_text(path)
        assert "from shared.wheel import" in text, (
            "scripts/deploy_wheel.py should import wheel constants from "
            f"shared.wheel instead of hardcoding them. {_BUMP_MSG}"
        )
