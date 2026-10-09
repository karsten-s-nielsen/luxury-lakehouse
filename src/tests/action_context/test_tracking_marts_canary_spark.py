"""Real-Spark validation of the ADR-087 two-probe canary's tracking-marts Spark primitives.

Covers the pieces ``compute_tracking_size_signals`` (Probe-M window counts) + ``build_fit_probe`` (one
window build) rely on, WITHOUT a bronze catalog:
- the per-``_window_id`` row-count aggregate over ``assign_frame_windows`` → the ``WindowRef`` list whose
  global max ``select_canary_probes`` picks as Probe M;
- ``windowed_build_sdf(only_window_id=W)`` builds ONLY window W (the Probe-M one-window bound).

Run (repo root), after ``docker build -t ll-pyspark-test - < docker/pyspark-test/Dockerfile``::

    MSYS_NO_PATHCONV=1 docker run --rm -v "D:/Development/karstenskyt__luxury-lakehouse:/work" ll-pyspark-test \\
      python -m pytest src/tests/action_context/test_tracking_marts_canary_spark.py -p no:cacheprovider -o addopts="" -q
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark", reason="tracking-marts canary Spark primitives run in the pyspark Docker image")

from analytics.action_context.canary import WindowRef, select_canary_probes
from analytics.action_context.frame_windows import _WINDOW_COL
from analytics.action_context.work_unit import WorkUnit
from ingestion.frame_window_dispatch import assign_frame_windows, windowed_build_sdf

_ROOT = Path("src/tests/fixtures/action_context")
_PROVIDER = "idsse"
_MATCH = "J03WMXmini"
_PERIOD = 1
_TARGET = 100  # small → several windows over the ~426-frame mini fixture


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    sess = (
        SparkSession.builder.master("local[2]")
        .appName("ll-tm-canary-test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield sess
    sess.stop()


def _frames() -> pd.DataFrame:
    frames = pd.read_parquet(_ROOT / _PROVIDER / f"{_MATCH}_p{_PERIOD}" / "frames.parquet")
    if "match_id" not in frames.columns:
        frames["match_id"] = _MATCH
    if "period" not in frames.columns:
        frames["period"] = _PERIOD
    return frames


def test_window_count_aggregate_feeds_global_densest(spark) -> None:
    """The per-``_window_id`` row-count aggregate over ``assign_frame_windows`` yields WindowRefs whose
    global argmax is exactly what ``select_canary_probes`` selects as Probe M."""
    frames = _frames()
    assert frames["frame"].nunique() > _TARGET, "fixture must span >1 window (else vacuous)"

    sdf = spark.createDataFrame(frames)
    assigned = assign_frame_windows(sdf, _PROVIDER, target_window_frames=_TARGET)
    counts = assigned.groupBy("match_id", "period", _WINDOW_COL).count().toPandas()
    assert len(counts) > 1, "must produce several windows"

    unit = WorkUnit(provider=_PROVIDER, match_id=_MATCH, period=_PERIOD)
    refs = [WindowRef(unit, int(r[_WINDOW_COL]), int(r["count"])) for _, r in counts.iterrows()]
    probes = select_canary_probes([unit], refs)

    true_max = counts.loc[counts["count"].idxmax()]
    assert probes.memory_window is not None
    assert probes.memory_window.window_id == int(true_max[_WINDOW_COL])
    assert probes.memory_window.n_rows == int(true_max["count"])


def test_only_window_id_builds_just_that_window(spark) -> None:
    """``windowed_build_sdf(only_window_id=W)`` builds ONLY window W's core frames — the Probe-M bound.
    Non-vacuous: two different windows build different (window-specific) row counts."""
    from pyspark.sql import types as T  # noqa: N812

    frames = _frames()
    sdf = spark.createDataFrame(frames)

    # Trivial identity build: return the window's (trimmed-to-core) frames projected to 3 cols.
    def _build(core: pd.DataFrame) -> pd.DataFrame:
        out = core[["match_id", "period", "frame"]].copy()
        out["period"] = out["period"].astype("int64")
        out["frame"] = out["frame"].astype("int64")
        return out

    schema = T.StructType(
        [
            T.StructField("match_id", T.StringType()),
            T.StructField("period", T.LongType()),
            T.StructField("frame", T.LongType()),
        ]
    )

    # Expected CORE frame-rows per window (build trims halo → core only).
    assigned = assign_frame_windows(sdf, _PROVIDER, target_window_frames=_TARGET).toPandas()
    core = assigned[~assigned["_is_halo"]]
    per_window = core.groupby(_WINDOW_COL).size()
    w0, w1 = per_window.index[0], per_window.index[1]

    n0 = windowed_build_sdf(
        spark, sdf, _PROVIDER, _build, schema, target_window_frames=_TARGET, only_window_id=int(w0)
    ).count()
    n1 = windowed_build_sdf(
        spark, sdf, _PROVIDER, _build, schema, target_window_frames=_TARGET, only_window_id=int(w1)
    ).count()
    total_core = len(core)

    assert n0 == int(per_window.loc[w0]), "window-0 build != its core frame count"
    assert n1 == int(per_window.loc[w1]), "window-1 build != its core frame count"
    # Non-vacuous: the filter builds ONE window, strictly fewer rows than the whole unit's core.
    assert 0 < n0 < total_core
    assert 0 < n1 < total_core
