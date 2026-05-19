"""Shared fixture for TC-3 calibration tests — adds scripts/ to sys.path.

Placed in src/tests/tc3/ so autouse naturally scopes to this directory only,
preventing sys.path pollution for the rest of the test suite.
"""

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = str(Path(__file__).resolve().parents[3] / "scripts")


@pytest.fixture(autouse=True, scope="session")
def _tc3_scripts_path() -> None:
    """Add scripts/ to sys.path for TC-3 tests only."""
    if _SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, _SCRIPTS_DIR)
