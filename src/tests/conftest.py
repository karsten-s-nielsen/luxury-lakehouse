"""Shared test configuration."""

from __future__ import annotations

import sys

# On Windows, jaxlib native extensions fail to load if pandas/matplotlib
# load OpenBLAS DLLs first (DLL load order conflict). Importing jax early
# — before conftest triggers matplotlib — avoids the clash.
if sys.platform == "win32":
    try:
        import jax  # noqa: F401
    except ImportError:
        pass

import matplotlib
import pytest

matplotlib.use("Agg")


@pytest.fixture(autouse=True)
def _restore_pyspark_modules() -> object:
    """Snapshot + restore `pyspark.*` entries in sys.modules around every test.

    Several test modules (test_guards.py, test_guard_conformance.py, etc.)
    inject `MagicMock()` into sys.modules['pyspark.sql'] to exercise guard
    helpers without a real Spark session but do NOT tear the mock down.
    That mock persists into subsequent tests — e.g. test_match_summary_render.py's
    plotly-based tests — where plotly's narwhals dependency tries
    `isinstance(df, pyspark_sql.DataFrame)` and gets a MagicMock (not a type),
    raising `TypeError: isinstance() arg 2 must be a type ...`.

    This autouse fixture snapshots any pyspark entries present before each test
    and restores exactly that state afterwards. Entries added during the test
    are removed; entries replaced are restored to their original object.
    """
    pyspark_entries = {k: v for k, v in sys.modules.items() if k == "pyspark" or k.startswith("pyspark.")}
    yield
    current = [k for k in list(sys.modules) if k == "pyspark" or k.startswith("pyspark.")]
    for k in current:
        if k not in pyspark_entries:
            del sys.modules[k]
        else:
            sys.modules[k] = pyspark_entries[k]
