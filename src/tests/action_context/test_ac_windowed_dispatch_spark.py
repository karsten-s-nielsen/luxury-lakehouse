"""Real-Spark validation of the AC-drain seam's single-owner ↔ window alignment (ADR-089 §4.5c).

The AC drain does NOT build+trim frames like tracking-marts; instead each window enriches its
``core+halo`` frames and claims each ACTION exactly once via the ordinal single-owner map
(``_owned_action_ids``): owner window = ``floor(searchsorted(sorted_frames, est_frame) / target)``.
For that to agree with the Spark grouping, the driver-collected ``sort_array(collect_list(distinct
frame))`` ordinal MUST equal the ``_window_id`` that ``assign_frame_windows`` (Spark ``dense_rank``)
assigns each CORE frame. The pure/local tests (``test_ac_windowed_seam.py``) exercise the owner math
through ``assign_window_ids``; THIS test closes the Spark-specific gap (dense_rank ordinal ==
searchsorted ordinal; driver collect == ``compute_sorted_frames``) without needing bronze/catalog.

Run (repo root), after ``docker build -t ll-pyspark-test - < docker/pyspark-test/Dockerfile``::

    MSYS_NO_PATHCONV=1 docker run --rm -v "D:/Development/karstenskyt__luxury-lakehouse:/work" ll-pyspark-test \\
      python -m pytest src/tests/action_context/test_ac_windowed_dispatch_spark.py -p no:cacheprovider -o addopts="" -q
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pyspark", reason="AC windowed-seam Spark dispatch runs in the pyspark Docker image")

from analytics.action_context.frame_windows import _WINDOW_COL
from ingestion.frame_window_dispatch import assign_frame_windows

_ROOT = Path("src/tests/fixtures/action_context")
_PROVIDER = "idsse"
_MATCH = "J03WMXmini"
_PERIOD = 1
_TARGET = 100  # small -> several windows over the 426-frame mini fixture


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    sess = (
        SparkSession.builder.master("local[2]")
        .appName("ll-ac-windowed-seam-test")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield sess
    sess.stop()


def test_spark_window_id_matches_searchsorted_owner(spark) -> None:
    """Every CORE frame's Spark ``_window_id`` equals the ordinal-map owner the single-owner math
    computes (``floor(searchsorted(sorted_frames, frame) / target)``) — so an action whose
    ``est_frame`` lands on a real frame is claimed by exactly the window that frame is core in."""
    frames = pd.read_parquet(_ROOT / _PROVIDER / f"{_MATCH}_p{_PERIOD}" / "frames.parquet")
    if "match_id" not in frames.columns:
        frames["match_id"] = _MATCH
    assert frames["frame"].nunique() > _TARGET, "fixture must span >1 window (else vacuous)"

    sdf = spark.createDataFrame(frames)
    assigned = assign_frame_windows(sdf, _PROVIDER, target_window_frames=_TARGET).toPandas()

    # Driver ordinal basis (mirrors _process_tracking_match's sort_array(collect_list(distinct))).
    sorted_frames = np.sort(frames["frame"].dropna().unique()).astype(float)

    core = assigned[~assigned["_is_halo"]][["frame", _WINDOW_COL]].drop_duplicates()
    assert not core.empty
    expected = np.searchsorted(sorted_frames, core["frame"].to_numpy(dtype=float), side="left") // _TARGET
    got = core[_WINDOW_COL].to_numpy(dtype="int64")
    assert np.array_equal(got, expected), "Spark _window_id (dense_rank) != searchsorted ordinal owner"

    # Every distinct frame appears as CORE in exactly ONE window (halo is additive, never replaces core).
    assert core["frame"].nunique() == len(core), "a frame is core in more than one window"
    assert core["frame"].nunique() == len(sorted_frames), "a frame lost its core window"
