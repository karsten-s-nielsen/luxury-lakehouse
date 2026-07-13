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
        is_slice=True,  # ADR-067: fixture = windowed frames + whole-match actions
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


_NEW_PLAYER_INFLUENCE = [
    "actor_reachable_area_m2", "off_ball_xt_team", "off_ball_xt_opponent",
    "off_ball_xt_diff", "reachable_area_team", "reachable_area_opponent", "reachable_area_diff",
]  # fmt: skip
_NEW_STRUCTURAL = ["structural_lbs", "structural_sgm", "structural_sdi"]
_PASS_OR_CROSS = {"pass", "cross"}


def test_new_ac_fields_emit_and_nan_contracts() -> None:
    """Golden-independent guards for the 11 new columns (spec §9).

    - Emit-drift: xcross_attempt + the 7 player-influence columns populate on the possessing-team
      tracking slice, so an upstream emit rename (column drops out of the enrich output and
      build_output fills it all-NaN) fails this RED without needing the frozen golden.
    - NaN contract: structural_* is NaN on every non-pass/cross action (silly-kicks contract).
    """
    result = _recompute()

    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE, *_NEW_STRUCTURAL]:
        assert col in result.columns, f"{col} missing from enrich output"

    # Emit-drift guard: these populate for the possessing team on the IDSSE mini slice.
    for col in ["xcross_attempt", *_NEW_PLAYER_INFLUENCE]:
        assert result[col].notna().any(), f"{col} all-NaN — aggregator not wired or emit renamed upstream"

    # Structural NaN contract: non-NaN only on pass/cross rows.
    non_pass = ~result["type_name"].isin(_PASS_OR_CROSS)
    for col in _NEW_STRUCTURAL:
        assert result.loc[non_pass, col].isna().all(), f"{col} must be NaN on non-pass/cross actions"
    # If the slice has any pass/cross, structural must populate on at least one (emit-drift guard).
    if result["type_name"].isin(_PASS_OR_CROSS).any():
        for col in _NEW_STRUCTURAL:
            assert result.loc[~non_pass, col].notna().any(), f"{col} all-NaN on pass/cross — emit drift?"


_XT_GK_COLS = [
    "xt_gk", "xt_gk_possession", "xt_gk_counter", "xt_gk_direct", "xt_gk_high_press",
    "xt_gk_low_block", "xt_gk_base", "xt_gk_pev", "xt_gk_rav", "xt_gk_dzv", "xt_gk_pressure",
    "xt_gk_origin_source", "xt_gk_dest_source", "xt_gk_origin_confidence",
    "xt_gk_completion_variant", "xt_gk_completion_source",
    "xt_gk_origin_x", "xt_gk_origin_y", "xt_gk_dest_x", "xt_gk_dest_y",
    "gk_completion",
]  # fmt: skip


def test_xt_gk_fields_present_and_scope_contract() -> None:
    """ADR-048 (silly-kicks 4.21.0/4.22.0) + 4.36.0 coords: the xT-GK + 4 resolved-coord + gk_completion columns.

    The mini slice (3 IDSSE open-play actions) contains NO GK-distribution action, so the
    scope contract here is all-NaN/None — a non-null value on an open-play row would mean the
    upstream in-scope mask drifted (incl. the 4.36.0 origin/dest coords). Presence is asserted
    column-by-column (build_output wiring);
    EMIT coverage lives in test_full_golden_xt_gk_emits (the full anchor has 2 goalkicks).
    """
    result = _recompute()
    for col in _XT_GK_COLS:
        assert col in result.columns, f"{col} missing from enrich output"
        assert result[col].isna().all(), f"{col} non-null on an open-play-only slice — xT-GK scope drift"


def test_full_golden_xt_gk_emits() -> None:
    """Emit-drift lock through the FROZEN full anchor (golden-reading, CI-cheap).

    The J03WMX_p1 anchor owns 2 goalkicks; the frozen golden must carry non-null xT-GK values on
    them (all five preset composites + completion provenance). An upstream emit-rename would
    surface at golden-regen time as this going all-NaN — this assertion makes that loud instead
    of silently freezing an empty feature family. (The 2-row count is NOT pinned — only non-empty.)
    """
    golden = pd.read_parquet(f"{_ROOT}/idsse/J03WMX_p1/golden.parquet")
    gk_rows = golden[golden["type_name"] == "goalkick"]
    assert len(gk_rows) > 0, "full anchor lost its goalkick rows — re-extract before trusting this gate"
    for col in _XT_GK_COLS:
        assert golden[col].notna().any(), f"{col} all-NaN in the full golden — emit drift at regen"
