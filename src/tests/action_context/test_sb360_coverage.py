"""SB360 freeze-frame coverage (ADR-039): the single-frame-supportable metrics populate (partial/
sparse), the velocity/temporal ones stay NULL, and provenance is 'voronoi'.

Drives the REAL hexagon ``run_work_unit`` on the committed ``statsbomb/3835328`` SB360 fixture
(``sb360.parquet`` triggers the ``tier="sb360"`` path). No Spark/Databricks. See
``scripts/extract_action_context_fixture.py --provider statsbomb`` for the fixture recipe.
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.pipeline import run_work_unit
from analytics.action_context.work_unit import WorkUnit

_ROOT = "src/tests/fixtures/action_context"


def _run() -> pd.DataFrame:
    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="statsbomb", match_id="3835328", period=None),
        frames=ParquetFrameSource(_ROOT),
        actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT),
        meta=ParquetMatchMetadataSource(_ROOT),
        sink=sink,
        is_slice=True,  # ADR-067: fixture = windowed frames + whole-match actions
    )
    assert sink.df is not None
    return sink.df


def _nn(df: pd.DataFrame, col: str) -> int:
    return int(pd.to_numeric(df[col], errors="coerce").notna().sum())


def test_sb360_supported_metrics_populate() -> None:
    df = _run()
    # ADR-058: ghost-GK is NOT run on sb360 (velocity-degenerate); pitch_control_at_target__voronoi
    # IS now emitted (position-only). Both are sb360 enricher-tiering changes.
    assert _nn(df, "ghost_gk_x") == 0, "ghost-GK must be NULL on sb360 (ADR-058)"
    assert _nn(df, "pitch_control_at_target__voronoi") > 0, "voronoi at_target must populate on sb360"
    assert _nn(df, "obso_actual") > 0
    assert _nn(df, "pausa_composite") > 0
    assert _nn(df, "gk_pitch_control_share_weighted") > 0  # voronoi (position-only)
    # silly-kicks 4.87.0 velocity-availability contract (their ADR-063): GK zone closing-time is a
    # velocity-DERIVED quantity (time-to-intercept), so compute_zone_closing_times WITHHOLDS it as
    # honest NaN on velocity-less-by-design frames (speed_source='unavailable' — the SB360 freeze-
    # frame shape), independent of the voronoi pitch-control method used for the position-only
    # metrics. It previously populated a biased zero-velocity value; under 4.87.0 it is NULL.
    # Rebaselined from ">0" during the silly-kicks 4.87.0 adoption (P1b) — genuine coverage change,
    # not a lakehouse regression: freeze-frames have no velocity, so honest NULL beats a biased value.
    assert _nn(df, "gk_closing_time_mean_s__near_post") == 0, (
        "gk zone closing-time is velocity-derived -> NULL on velocity-less SB360 freeze-frames (sk 4.87.0)"
    )
    assert df["shape_graph_density_defending"].notna().any()
    # silly-kicks 4.90.1 velocity-availability contract (their ADR-066/PR-S160): xShotOccurrence's
    # `speed` is a TRAINED feature, so scoring on a velocity-less-by-design frame (the SB360 freeze-
    # frame shape, speed_source='unavailable') would make the model impute an input the source
    # structurally cannot carry — the ADR-053 fabrication shape. compute_xshot_occurrence now WITHHOLDS
    # it as honest NaN instead of the previous distance-only fallback. Rebaselined from ">0" during the
    # silly-kicks 4.90.1 adoption (mirrors the gk_closing_time 4.87.0 rebaseline above) — a genuine
    # coverage change, not a lakehouse regression: honest NULL beats a fabricated score. See ADR-078.
    assert _nn(df, "xshot_occurrence") == 0, (
        "xshot_occurrence's speed feature is velocity-derived -> honest NULL on velocity-less SB360 "
        "freeze-frames (silly-kicks 4.90.1)"
    )
    # Provenance: voronoi on the SB360 path
    assert df["pitch_control_method"].notna().all()
    assert set(df["pitch_control_method"].unique()) == {"voronoi"}


def test_sb360_new_fields_present() -> None:
    """The 11 new columns (structural/player-influence/xCross) must be present on the SB360
    schema even where freeze-frame limitations leave them NaN (honest NULL, ADR-039)."""
    df = _run()
    for col in (
        "structural_lbs", "structural_sgm", "structural_sdi",
        "actor_reachable_area_m2", "off_ball_xt_team", "off_ball_xt_opponent",
        "off_ball_xt_diff", "reachable_area_team", "reachable_area_opponent",
        "reachable_area_diff", "xcross_attempt",
    ):  # fmt: skip
        assert col in df.columns, f"{col} missing from SB360 output"


def test_sb360_unsupported_metrics_null() -> None:
    df = _run()
    # Velocity / temporal — entirely NULL on freeze-frames
    for c in ("das_diff", "blocking_score", "space_created_m2", "elastic_confidence"):
        assert _nn(df, c) == 0, f"{c} unexpectedly populated on SB360"
