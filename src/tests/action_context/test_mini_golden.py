"""Fast CI golden gate: RECOMPUTE a tiny real-pipeline slice and compare to a frozen mini-golden.

This is the always-on counterpart to ``test_e2e.py``. ``test_e2e`` runs the full 97-action
IDSSE anchor (~5 min, DAS-dominated) and is gated behind ``AC1_E2E=1`` -> it never runs in CI.
``test_differential`` DOES run in CI but only *reads* the committed golden vs legacy oracles; it
never recomputes the pipeline, and DAS is a ``known_divergence`` it does not assert. So a value
shift in DAS / ghost-GK / any enrichment could ride ``main`` uncaught -- which is exactly what
happened: silly-kicks 4.2.0's DAS carrier-forwarding change landed in #328 and was only caught
when the gated e2e was finally run during the 4.4.0 adoption (ADR-036).

This test closes that gap. It recomputes the REAL ``run_work_unit`` -> ``enrich_batch`` on a
3-action / 2-batch slice of the IDSSE J03WMX_p1 fixture (``idsse/J03WMXmini_p1/``, all 3 actions
carry non-NaN DAS + ghost-GK) and asserts ALL 103 columns reproduce the frozen mini-golden.
Because the mini-golden is frozen from the same slice, it is self-consistent: a library/algorithm
change diverges the RECOMPUTE from the frozen golden -> the assertion fails in CI.

Runtime ~30s local (per-action ghost-GK brute-force KDE dominates); it will drop to a few seconds
once silly-kicks ships the FFT-KDE ghost-GK backend. NOT gated -- runs in the default suite.

Regenerate the mini-golden (after an INTENTIONAL, signed-off value change) with:
    uv run python scripts/build_ac1_mini_golden.py
and commit the updated ``idsse/J03WMXmini_p1/golden.parquet`` in the same PR.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

_ROOT = "src/tests/fixtures/action_context"
_MINI_DIR = f"{_ROOT}/idsse/J03WMXmini_p1"
_FLOAT_ATOL = 1e-6
_EXACT_COLS = {"data_source", "match_id", "action_id", "period_id", "type_name"}


def _recompute() -> pd.DataFrame:
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

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1),
        frames=ParquetFrameSource(_ROOT),
        actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT),
        meta=ParquetMatchMetadataSource(_ROOT),
        sink=sink,
    )
    assert sink.df is not None
    return sink.df


def test_mini_golden_recompute_matches_and_exercises_das_ghost() -> None:
    golden = pd.read_parquet(f"{_MINI_DIR}/golden.parquet")
    result = _recompute()

    # Same shape + columns as the frozen mini-golden.
    assert list(result.columns) == list(golden.columns), "result columns drifted from mini-golden"
    assert len(result) == len(golden), f"row count {len(result)} != mini-golden {len(golden)}"

    # The gate is only meaningful if the heaviest enrichments are actually exercised on this slice.
    assert golden["das_diff"].notna().all(), "mini-golden has NaN DAS -- a DAS shift would not be caught"
    assert golden["ghost_gk_x"].notna().all(), "mini-golden has NaN ghost-GK -- a ghost-GK shift would not be caught"

    # M13: boundary-dup-free.
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"duplicate action rows: {dupes[dupes > 1].to_dict()}"

    r = result.sort_values(["period_id", "action_id"]).reset_index(drop=True)
    g = golden.sort_values(["period_id", "action_id"]).reset_index(drop=True)

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

    detail = "\n  ".join(mismatches)
    assert not mismatches, f"mini-golden recompute diverged (intentional value change? regen mini-golden):\n  {detail}"
