"""Task 3 — build-once -> spill -> read round-trip faithfulness (rev3 two-stage transport).

The Stage-1 build spills a unit's oriented frames; Stage-2 reads them back and scores. Transport neutrality
reduces to: the spill round-trip preserves dtypes AND intra-unit frame order (TM-PLAN-12 / TM-PLAN-03), so
``scorer(read_back) == scorer(built)``. Pure pandas on the committed fixtures (sk 4.123 baseline).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from analytics.action_context.tracking_frame_spill import (
    built_frame_dtype_order,
    built_frame_dtypes,
    read_built_frames,
    spill_built_frames,
)
from ingestion.off_ball_runs_writer import compute_off_ball_runs
from tests.tracking_marts import _oracle

_FAST_PROVIDERS = ["metrica", "skillcorner", "idsse"]


@pytest.mark.parametrize("provider", _FAST_PROVIDERS)
def test_spill_roundtrip_local_preserves_dtypes_and_order(provider: str, tmp_path) -> None:
    """Local (pandas-written) path: read back == built frames, dtypes + row order preserved."""
    built = _oracle.build_inputs(provider).frames
    path = tmp_path / f"{provider}.parquet"
    n = spill_built_frames(built, path)
    assert n == len(built)
    got = read_built_frames(path)
    # Order-sensitive (no sort): a frame-order regression in the spill must fail here.
    assert_frame_equal(got, built.reset_index(drop=True), check_dtype=True, check_like=False)


def test_restore_dtypes_forces_built_schema(tmp_path) -> None:
    """Spark-written path simulation: on-disk dtypes decayed, ``restore_dtypes`` recovers the built schema."""
    # Simulate a Spark write that lost pandas metadata: coords float64, team_id object, player_id float.
    decayed = pd.DataFrame(
        {
            "x": pd.Series([1.0, 2.0], dtype="float64"),
            "y": pd.Series([3.0, 4.0], dtype="float64"),
            "team_id": pd.Series(["H", "A"], dtype="object"),
            "player_id": pd.Series([10, 11], dtype="int64"),
            "frame_id": pd.Series([0, 1], dtype="int64"),
        }
    )
    path = tmp_path / "decayed.parquet"
    spill_built_frames(decayed, path)
    restore = {"x": "float32", "y": "float32", "team_id": "category", "player_id": "Int64"}
    got = read_built_frames(path, restore_dtypes=restore)
    assert str(got["x"].dtype) == "float32"
    assert str(got["y"].dtype) == "float32"
    assert str(got["team_id"].dtype) == "category"
    assert str(got["player_id"].dtype) == "Int64"
    # Values unchanged, order preserved.
    assert got["frame_id"].tolist() == [0, 1]
    assert got["team_id"].astype("object").tolist() == ["H", "A"]


def test_built_frame_dtypes_captures_schema() -> None:
    built = _oracle.build_inputs("metrica").frames
    schema = built_frame_dtypes(built)
    assert set(schema) == set(built.columns)
    assert all(schema[c] == str(built[c].dtype) for c in built.columns)


@pytest.mark.parametrize("provider", sorted(_oracle.UNITS))
def test_built_frame_struct_schema_parity(provider: str) -> None:
    """The declared Stage-1 output schema matches a freshly-built fixture frame (ADR-033; catches the 4.128
    float32 shift at bump time). Pure pandas-dtype parity — the pyspark StructType build is live-validated.
    """
    built = _oracle.build_inputs(provider).frames
    declared = built_frame_dtype_order(provider)
    actual = tuple((c, str(d)) for c, d in built.dtypes.items())
    assert declared == actual, (
        f"{provider}: built-frame schema drift.\n declared={declared}\n actual  ={actual}\n"
        "Update _BUILT_FRAME_DTYPES_BASE / _SMOOTHING_PROVIDERS (e.g. the sk-4.128 float32 shift)."
    )


@pytest.mark.parametrize("provider", _FAST_PROVIDERS)
def test_off_ball_scoring_through_spill_matches_built(provider: str, tmp_path) -> None:
    """Scoring from the spilled+restored frames == scoring from the in-memory built frames (neutrality)."""
    inp = _oracle.build_inputs(provider)
    path = tmp_path / f"{provider}.parquet"
    spill_built_frames(inp.frames, path)
    streamed = read_built_frames(path, restore_dtypes=built_frame_dtypes(inp.frames))
    direct = compute_off_ball_runs(inp.actions, inp.frames, inp.xt)
    via_spill = compute_off_ball_runs(inp.actions, streamed, inp.xt)
    _oracle.assert_mart_equal(via_spill, direct, f"{provider}:off_ball_runs")


def test_all_six_marts_score_identically_through_spill() -> None:
    """Capstone (gkdv + rest_defense included): every mart scored from the spill == the direct baseline.

    This is the full Stage-2 per-mart neutrality proof the executor dispatch relies on — the Spark
    mapInPandas plumbing only distributes this pure body (and is itself covered by the
    ``_make_streaming_group_mapper`` carry/flush test + live Part-B). metrica (981 frames) keeps gkdv fast.
    Both sides go through ``_oracle.score_all_marts`` (one signature source — no call drift).
    """
    import tempfile

    provider = "metrica"
    inp = _oracle.build_inputs(provider)
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / f"{provider}.parquet"
        spill_built_frames(inp.frames, path)
        frames2 = read_built_frames(path, restore_dtypes=built_frame_dtypes(inp.frames))

    direct = _oracle.score_all_marts(provider, inp, inp.frames)
    via_spill = _oracle.score_all_marts(provider, inp, frames2)
    for mart in _oracle.MART_KEYS:
        _oracle.assert_mart_equal(via_spill[mart], direct[mart], f"{provider}:{mart}")
