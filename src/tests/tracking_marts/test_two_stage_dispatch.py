"""End-to-end two-stage Spark dispatch validation (pyspark required — runs in Docker, skipped locally).

Exercises the REAL ``mapInPandas`` plumbing the pure-closure tests cannot: Stage-1 build dispatch +
built-frame StructType match + Spark parquet spill write/read + Stage-2 per-mart score dispatch +
carry/flush, asserting the end-to-end output == the baseline. Serverless-only aspects (UC Volume paths,
Spark Connect) stay live-validated; this covers the dispatch LOGIC under local[*].

Run (from repo root), after ``docker build -t ll-pyspark-test -f docker/pyspark-test/Dockerfile .``::

    docker run --rm -v "${PWD}:/work" ll-pyspark-test \
      python -m pytest src/tests/tracking_marts/test_two_stage_dispatch.py -p no:cacheprovider -o addopts="" -q
"""

from __future__ import annotations

import tempfile

import pytest

pytest.importorskip("pyspark", reason="two-stage Spark dispatch validation runs in the pyspark Docker image")

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetXtSource,
)
from analytics.action_context.tracking_frame_spill import built_frame_struct_type
from ingestion.tracking_marts_dispatch import (
    MartSpec,
    build_stage2_mart_sdf,
    make_build_udf,
    make_mart_scorers,
    precompute_stage2a,
    run_stage1_build_and_spill,
)
from tests.tracking_marts import _oracle

# off_ball + defensive_credit only: pure-sk marts with NO HuggingFace weight download (gkdv/gk_decision
# need bundled HF models). Those marts share the identical dispatch plumbing, covered by the pure-closure
# test + live; this E2E proves the Spark wrapping on the HF-free surface.
_MARTS = ("off_ball_runs", "action_defensive", "defensive_credit_attributions")

# Force ≥2 scoring windows on the (small) committed fixtures so the seam-provider dispatch exercises the
# REAL windowed Stage-2 path (assign_score_windows + per-window mapInPandas + core-output trim + union),
# not a trivial single window. metrica is non-seam → windowing is ignored (whole-unit regroup).
_SCORE_WINDOW_FRAMES = 32


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    sess = (
        SparkSession.builder.master("local[2]")
        .appName("ll-tracking-marts-dispatch-test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield sess
    sess.stop()


def _mart_schema(mart: str):
    from ingestion.defensive_credit_writer import _AGG_TYPES, _LONG_TYPES, AGG_OUTPUT_COLUMNS, LONG_OUTPUT_COLUMNS
    from ingestion.defensive_credit_writer import _struct_type as _dc_struct_type
    from ingestion.off_ball_runs_writer import _struct_type as _off_ball_struct_type

    if mart == "off_ball_runs":
        return _off_ball_struct_type()
    if mart == "action_defensive":
        return _dc_struct_type(AGG_OUTPUT_COLUMNS, _AGG_TYPES)
    if mart == "defensive_credit_attributions":
        return _dc_struct_type(LONG_OUTPUT_COLUMNS, _LONG_TYPES)
    raise KeyError(mart)


@pytest.mark.parametrize("provider", ["metrica", "skillcorner"])
def test_two_stage_dispatch_matches_baseline(spark, provider: str) -> None:
    wu = _oracle.work_unit(provider)
    grid, xt_l, xt_w = ParquetXtSource(_oracle.FIXTURE_ROOT).grid()
    raw_frames = ParquetFrameSource(_oracle.FIXTURE_ROOT).frames(wu).frames.copy()
    raw_actions = ParquetActionsSource(_oracle.FIXTURE_ROOT).actions(wu)
    meta = _oracle.metadata(provider)
    # Guarantee the repartition keys exist on the raw frame (read_raw_trk_sdf projects them in prod).
    if "match_id" not in raw_frames.columns:
        raw_frames["match_id"] = wu.match_id
    if "period" not in raw_frames.columns:
        raw_frames["period"] = int(wu.period or 0)

    raw_sdf = spark.createDataFrame(raw_frames)
    build_udf = make_build_udf(provider, wu.match_id, int(wu.period or 0), raw_actions, meta, grid, xt_l, xt_w)

    inp = _oracle.build_inputs(provider)

    with tempfile.TemporaryDirectory() as d:
        spill_dir = f"{d}/{provider}"
        run_stage1_build_and_spill(spark, wu, raw_sdf, build_udf, built_frame_struct_type(provider), spill_dir)
        # Stage 2a (driver, from the spill): coverage-restricted actions + GoalMap + action→window routing,
        # exactly as TrackingMartsProcessor.process does it. gkdv is off → no carrier / stride survivors.
        stage2a = precompute_stage2a(
            spark, spill_dir, inp.actions, provider, gkdv_enabled=False, target_window_frames=_SCORE_WINDOW_FRAMES
        )
        scorers = make_mart_scorers(
            provider,
            wu.match_id,
            stage2a.restricted_actions,
            grid,
            xt_l,
            xt_w,
            _oracle.synthetic_xg_preds(inp),
            meta,
            None,
            None,
            _oracle._access_tier(inp),
            gkdv_enabled=False,
            goal_map=stage2a.invariants.goal_map,
            carrier=stage2a.invariants.carrier,
            scored_frame_keys=stage2a.scored_frame_keys,
        )
        for mart in _MARTS:
            spec = MartSpec(key=mart, table=mart, schema=_mart_schema(mart), score=scorers[mart])
            # Neutrality reference: the SAME restricted scorer over the WHOLE unit (in-memory). The windowed
            # dispatch reads the spill and scores window-by-window; its union must equal this byte-for-byte
            # (seam providers window at _SCORE_WINDOW_FRAMES; metrica ignores it → whole-unit regroup).
            whole = spec.score(inp.frames)
            windowed = build_stage2_mart_sdf(
                spark,
                wu,
                spill_dir,
                spec,
                action_window=stage2a.action_window,
                target_window_frames=_SCORE_WINDOW_FRAMES,
            ).toPandas()
            _oracle.assert_mart_equal(windowed, whole, f"{provider}:{mart} (windowed==whole-unit)")


def test_assign_score_windows_core_window_id_matches_pandas(spark) -> None:
    """S2W-IMPL-04 / S2W-FULL-02: the Spark core ``_window_id`` (``dense_rank-1 // target``) MUST equal the
    pure pandas core ``_window_id`` (``np.sort(unique) // target``) frame-for-frame.

    This is the cross-engine invariant the whole windowed path rests on: the driver computes ``action_window``
    (pandas ``assign_score_window_ids``) while the executor dispatch tags frames with Spark
    ``assign_score_windows``; if the two assigned a frame a DIFFERENT window, ``_keep_core_outputs`` would
    drop that action's row (no window claims it) — a silent row loss on exactly the 8 dense units. Analytically
    identical (both a 0-based dense ordinal over distinct ``frame_id`` // target); pinned here under real Spark.
    """
    from analytics.action_context.frame_windows import _HALO_COL, _WINDOW_COL, assign_score_window_ids
    from ingestion.frame_window_dispatch import assign_score_windows

    inp = _oracle.build_inputs("skillcorner")
    slim = inp.frames[["game_id", "period_id", "frame_id"]].copy()  # window-id depends only on these
    target = _SCORE_WINDOW_FRAMES  # small → >= 2 windows on the fixture

    def _core_map(tagged):
        core = tagged[~tagged[_HALO_COL]][["frame_id", _WINDOW_COL]].drop_duplicates("frame_id")
        return {int(f): int(w) for f, w in zip(core["frame_id"], core[_WINDOW_COL], strict=True)}

    pandas_map = _core_map(assign_score_window_ids(slim, "off_ball_runs", "skillcorner", target_window_frames=target))
    spark_map = _core_map(
        assign_score_windows(
            spark.createDataFrame(slim), "off_ball_runs", "skillcorner", target_window_frames=target
        ).toPandas()
    )
    assert spark_map == pandas_map, "cross-engine core window-id mismatch (pandas vs Spark dense_rank)"
    assert len(set(pandas_map.values())) >= 2, "vacuous: fixture must span >= 2 windows at this target"
