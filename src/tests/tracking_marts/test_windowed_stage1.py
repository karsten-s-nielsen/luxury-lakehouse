"""Real-Spark validation of the haloed frame-window dispatch (pyspark required — Docker; skipped locally).

Proves the ``ingestion.frame_window_dispatch`` Spark plumbing (``assign_frame_windows`` dense-rank windows
+ halo replication + ``windowed_build_sdf`` per-window ``mapInPandas`` build + halo trim) produces a built
frame value-identical to the WHOLE-UNIT build — the dispatch-level echo of the pure neutrality test
(``tests/action_context/test_frame_windows.py``). ``target_window_frames`` is forced small so the fixture
spans multiple windows (+ halo boundaries) under ``local[2]``.

Run (repo root), after ``docker build -t ll-pyspark-test - < docker/pyspark-test/Dockerfile``::

    MSYS_NO_PATHCONV=1 docker run --rm -v "D:/Development/karstenskyt__luxury-lakehouse:/work" ll-pyspark-test \\
      python -m pytest src/tests/tracking_marts/test_windowed_stage1.py -p no:cacheprovider -o addopts="" -q
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark", reason="windowed frame-window Spark dispatch runs in the pyspark Docker image")

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetXtSource,
)
from analytics.action_context.tracking_frame_spill import built_frame_struct_type
from ingestion.frame_window_dispatch import windowed_build_sdf
from ingestion.tracking_marts_dispatch import make_build_udf
from tests.tracking_marts import _oracle

_PROVIDER = "idsse"  # has the most frames of the committed fixtures -> spans several small windows.
_TARGET = 150


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    sess = (
        SparkSession.builder.master("local[2]")
        .appName("ll-haloed-window-dispatch-test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield sess
    sess.stop()


def _sort(df):
    keys = [c for c in ("frame_id", "period_id", "player_id", "is_ball") if c in df.columns]
    return df.sort_values(keys, kind="mergesort").reset_index(drop=True)


def test_windowed_dispatch_matches_whole_unit(spark) -> None:
    wu = _oracle.work_unit(_PROVIDER)
    grid, xt_l, xt_w = ParquetXtSource(_oracle.FIXTURE_ROOT).grid()
    raw_frames = ParquetFrameSource(_oracle.FIXTURE_ROOT).frames(wu).frames.copy()
    raw_actions = ParquetActionsSource(_oracle.FIXTURE_ROOT).actions(wu)
    meta = _oracle.metadata(_PROVIDER)
    if "match_id" not in raw_frames.columns:
        raw_frames["match_id"] = wu.match_id
    if "period" not in raw_frames.columns:
        raw_frames["period"] = int(wu.period or 0)
    assert raw_frames["frame"].nunique() > _TARGET, "fixture must span >1 window (else vacuous)"

    raw_sdf = spark.createDataFrame(raw_frames)
    build_udf = make_build_udf(_PROVIDER, wu.match_id, int(wu.period or 0), raw_actions, meta, grid, xt_l, xt_w)

    windowed = windowed_build_sdf(
        spark, raw_sdf, _PROVIDER, build_udf, built_frame_struct_type(_PROVIDER), target_window_frames=_TARGET
    ).toPandas()
    whole = _oracle.build_inputs(_PROVIDER).frames

    # Same row set + value-identical (the dispatch-level neutrality guarantee). Column order/dtype are
    # governed by built_frame_struct_type, not the pandas intermediate — compare values null-unified.
    assert len(windowed) == len(whole), f"row count {len(windowed)} != whole-unit {len(whole)}"
    _oracle.assert_mart_equal(windowed, whole, f"{_PROVIDER}:windowed-stage1")
