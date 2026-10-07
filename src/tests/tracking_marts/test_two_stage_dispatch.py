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
    run_stage1_build_and_spill,
)
from tests.tracking_marts import _oracle

# off_ball + defensive_credit only: pure-sk marts with NO HuggingFace weight download (gkdv/gk_decision
# need bundled HF models). Those marts share the identical dispatch plumbing, covered by the pure-closure
# test + live; this E2E proves the Spark wrapping on the HF-free surface.
_MARTS = ("off_ball_runs", "action_defensive", "defensive_credit_attributions")


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
    baseline = _oracle.score_all_marts(provider, inp, inp.frames, include_gkdv=False)
    scorers = make_mart_scorers(
        provider,
        wu.match_id,
        inp.actions,
        grid,
        xt_l,
        xt_w,
        _oracle.synthetic_xg_preds(inp),
        meta,
        None,
        None,
        _oracle._access_tier(inp),
        gkdv_enabled=False,
    )

    with tempfile.TemporaryDirectory() as d:
        spill_dir = f"{d}/{provider}"
        run_stage1_build_and_spill(spark, wu, raw_sdf, build_udf, built_frame_struct_type(provider), spill_dir)
        for mart in _MARTS:
            spec = MartSpec(table=mart, schema=_mart_schema(mart), score=scorers[mart])
            got = build_stage2_mart_sdf(spark, wu, spill_dir, spec).toPandas()
            _oracle.assert_mart_equal(got, baseline[mart], f"{provider}:{mart}")
