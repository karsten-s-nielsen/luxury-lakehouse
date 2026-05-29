"""E2E + golden: regenerate the real pipeline on the IDSSE anchor and compare to the frozen golden.

This is the slow pre-commit gate (#23) and the C.3 golden check in one: it runs the REAL
``run_work_unit`` -> ``enrich_batch`` 250-frame loop on the committed IDSSE J03WMX fixture
(no Spark, no Databricks), then asserts the output reproduces ``golden.parquet`` and is
boundary-dup-free. Takes ~5 min (DAS-dominated), so it is gated behind ``AC1_E2E=1`` to keep
the default unit suite fast; CI regression is the fast ``test_differential.py`` (reads golden).

Run locally before committing:  AC1_E2E=1 uv run pytest src/tests/action_context/test_e2e.py -v
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("AC1_E2E") != "1",
    reason="slow real-pipeline e2e; set AC1_E2E=1 to run (pre-commit gate)",
)

_FLOAT_ATOL = 1e-6
_EXACT_COLS = {"data_source", "match_id", "action_id", "period_id", "type_name"}


def _run() -> pd.DataFrame:
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    root = "src/tests/fixtures/action_context"
    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMX", period=1),
        frames=ParquetFrameSource(root),
        actions=ParquetActionsSource(root),
        xt=ParquetXtSource(root),
        meta=ParquetMatchMetadataSource(root),
        sink=sink,
    )
    assert sink.df is not None
    return sink.df


def test_e2e_reproduces_golden_and_is_dup_free(golden_df: pd.DataFrame) -> None:
    result = _run()

    # M13: boundary-dup-free.
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"duplicate action rows: {dupes[dupes > 1].to_dict()}"

    # Same shape + columns as the frozen golden.
    assert list(result.columns) == list(golden_df.columns)
    assert len(result) == len(golden_df), f"row count {len(result)} != golden {len(golden_df)}"

    r = result.sort_values(["period_id", "action_id"]).reset_index(drop=True)
    g = golden_df.sort_values(["period_id", "action_id"]).reset_index(drop=True)

    mismatches: list[str] = []
    for col in g.columns:
        if col in _EXACT_COLS:
            if not r[col].astype(str).equals(g[col].astype(str)):
                mismatches.append(f"{col}: exact mismatch")
            continue
        rv = pd.to_numeric(r[col], errors="coerce").to_numpy(dtype=float)
        gv = pd.to_numeric(g[col], errors="coerce").to_numpy(dtype=float)
        both = ~(np.isnan(rv) | np.isnan(gv))
        if not np.array_equal(np.isnan(rv), np.isnan(gv)):
            mismatches.append(f"{col}: NaN pattern differs")
        elif both.any() and not np.allclose(rv[both], gv[both], atol=_FLOAT_ATOL, rtol=1e-4):
            mismatches.append(f"{col}: maxd={np.abs(rv[both] - gv[both]).max():.4g}")

    assert not mismatches, "e2e output diverged from golden:\n  " + "\n  ".join(mismatches)
