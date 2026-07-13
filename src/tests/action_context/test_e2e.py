"""E2E + golden: regenerate the real pipeline on the IDSSE anchor and compare to the frozen golden.

This is the slow pre-commit gate (#23) and the C.3 golden check in one: it runs the REAL
``run_work_unit`` -> ``enrich_batch`` frame-batch loop on the committed IDSSE J03WMX fixture
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

# NOTE (D7, ADR-067): the row-count assertions in this module do NOT run in CI — the whole module is
# gated behind AC1_E2E=1 (~5 min, DAS-dominated). That gap is why the single-frame velocity crash
# reached production. The ALWAYS-ON guard for that class is
# src/tests/action_context/test_velocity_single_frame.py, which drives the same
# builder -> preprocess -> _finalize path in milliseconds and runs in the default suite.
pytestmark = pytest.mark.skipif(
    os.environ.get("AC1_E2E") != "1",
    reason="slow real-pipeline e2e; set AC1_E2E=1 to run (pre-commit gate)",
)

_FLOAT_ATOL = 1e-6
_EXACT_COLS = {"data_source", "match_id", "action_id", "period_id", "type_name"}


def _run(provider: str, match_id: str, period: int) -> pd.DataFrame:
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
        WorkUnit(provider=provider, match_id=match_id, period=period),
        frames=ParquetFrameSource(root),
        actions=ParquetActionsSource(root),
        xt=ParquetXtSource(root),
        meta=ParquetMatchMetadataSource(root),
        sink=sink,
        is_slice=True,  # ADR-067: fixture = windowed frames + whole-match actions
    )
    assert sink.df is not None
    return sink.df


def test_gs_e2e_convert_and_enrich_does_not_crash() -> None:
    """GradientSports adapter+convert+enrich CRASH-guard on the committed fixture — the coverage
    GS lacked (test_e2e only ran IDSSE), which is why GS's convert-path bugs stayed latent: the
    hexagon fixtures feed pre-built frames/meta, bypassing the bronze→frames driver layer.

    This drives the GS convert path (`period_elapsed_time`→`timestamp` aliasing — a destructive
    rename used to make the converter KeyError; the Int64→native-string id coercion) end-to-end
    and asserts it COMPLETES and returns the result schema. Bug #4 raised here before returning,
    so this catches that crash class.

    SCOPE: the committed `10517_p3` fixture (re-extracted 2026-06-10, ADR-047 PR) is one
    2500-frame period-3 batch with period-relative actions AND roster-derived meta (jersey
    maps, GK ids, team sides, ET flag) — it enriches ~25 real rows, so this now also
    exercises GS player RESOLUTION end-to-end (the pre-ADR-040 fixture had absolute-clock
    actions + roster-less meta and produced 0 rows by construction). Row VALUES are not
    golden-asserted here; only completion + schema. See
    feedback_test_production_driver_entry_point + project_gradientsports_player_id_space_bug.
    """
    from analytics.action_context.schema import RESULT_COLUMNS

    result = _run("gradientsports", "10517", 3)  # must not raise (the bug-#4 crash class)
    assert isinstance(result, pd.DataFrame)
    # Completed enrichment returns the full result schema (a crash would not reach this).
    expected = {c for c in RESULT_COLUMNS if c != "_ingested_at"}
    assert set(result.columns) == expected, "result schema drifted from RESULT_COLUMNS"
    # D7 (ADR-067): the schema assertion alone is VACUOUS as a data-loss guard. `_empty_result()`
    # (pipeline.py:119-123) returns a ZERO-ROW frame carrying the full RESULT_COLUMNS schema, so the
    # check above passes on an empty emit -- precisely the silent-zero class that shipped
    # skillcorner:1552423:2 as a "successful" unit with 0 of 550 actions.
    assert len(result) > 0, "GS e2e resolved zero actions -- an empty emit carries the full schema"


def test_skillcorner_p2_e2e_resolves_actions() -> None:
    """SkillCorner PERIOD-2 dispatch coverage (ADR-040 amendment, 2026-06-11).

    The fixture's bronze frames carry the ABSOLUTE broadcast clock (P2 = 2700s+)
    while its actions are period-relative — exactly the production shape that
    silently dropped ~90% of P2 actions when the dispatch layer lacked the
    SkillCorner re-base. Pre-fix this raises (the frames-side guard fires, or the
    completeness invariant catches the drop); post-fix the slice's actions
    resolve. Values are not golden-asserted; resolution + dup-freedom are.
    """
    result = _run("skillcorner", "1886347", 2)
    assert len(result) > 0, "SC P2 slice resolved zero actions — dispatch time-base regressed"
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"duplicate action rows: {dupes[dupes > 1].to_dict()}"
    # LINKING must resolve too — rows can emit with a dead linker (every frame-derived
    # feature NaN), which is exactly what the converter double-subtraction produced
    # (frames at ≈ -2700 s; rows present, links zero). frame_id non-null proves the
    # action↔frame linker found frames on the SAME clock.
    assert result["frame_id"].notna().any(), (
        "SC P2 rows emitted but ZERO actions linked to frames — frames/actions clock mismatch "
        "(dispatch vs converter re-base; exactly one layer must subtract)"
    )


def test_metrica_p2_e2e_resolves_actions() -> None:
    """Metrica PERIOD-2 dispatch coverage — same parity rule as SkillCorner: the
    metrica frame-number re-base (ADR-040) previously had no committed period>=2
    fixture, so a regression would have been invisible to the suite."""
    result = _run("metrica", "Sample_Game_1", 2)
    assert len(result) > 0, "Metrica P2 slice resolved zero actions — frame re-base regressed"
    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"duplicate action rows: {dupes[dupes > 1].to_dict()}"
    # Same linking proof as the SC test: emitted rows with a dead linker are not coverage.
    assert result["frame_id"].notna().any(), (
        "Metrica P2 rows emitted but ZERO actions linked to frames — frames/actions clock mismatch"
    )


def test_e2e_reproduces_golden_and_is_dup_free(golden_df: pd.DataFrame) -> None:
    result = _run("idsse", "J03WMX", 1)

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
