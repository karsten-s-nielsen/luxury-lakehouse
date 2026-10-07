"""The two-stage dispatch's PURE closures (no Spark) reproduce the baseline.

``tracking_marts_dispatch.make_build_udf`` / ``make_mart_scorers`` are the per-unit UDF bodies the Spark
``mapInPandas`` dispatch wraps. They are pure pandas, so they are validated here directly on fixtures ==
the baseline; the Spark wrapping is then validated in Docker (``test_two_stage_dispatch`` — pyspark) + live.
This isolates "did the closure factoring change any output?" from "does the Spark plumbing work?".
"""

from __future__ import annotations

import pytest
from pandas.testing import assert_frame_equal

from analytics.action_context.local.parquet_sources import ParquetActionsSource, ParquetFrameSource, ParquetXtSource
from ingestion.tracking_marts_dispatch import make_build_udf, make_mart_scorers
from tests.tracking_marts import _oracle

_FAST = ["metrica", "skillcorner", "idsse"]


@pytest.mark.parametrize("provider", _FAST)
def test_build_udf_reproduces_build_unit_inputs_frames(provider: str) -> None:
    wu = _oracle.work_unit(provider)
    grid, xt_l, xt_w = ParquetXtSource(_oracle.FIXTURE_ROOT).grid()
    raw_frames = ParquetFrameSource(_oracle.FIXTURE_ROOT).frames(wu).frames
    raw_actions = ParquetActionsSource(_oracle.FIXTURE_ROOT).actions(wu)
    build = make_build_udf(
        provider, wu.match_id, int(wu.period or 0), raw_actions, _oracle.metadata(provider), grid, xt_l, xt_w
    )
    got = build(raw_frames)
    expected = _oracle.build_inputs(provider).frames
    assert_frame_equal(got.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=True)


def test_mart_scorers_match_baseline_all_six() -> None:
    """All six Stage-2 scorer closures == the direct baseline (metrica; gkdv included)."""
    provider = "metrica"
    inp = _oracle.build_inputs(provider)
    grid, xt_l, xt_w = ParquetXtSource(_oracle.FIXTURE_ROOT).grid()
    match_id, _ = _oracle.UNITS[provider]
    scorers = make_mart_scorers(
        provider,
        match_id,
        inp.actions,
        grid,
        xt_l,
        xt_w,
        _oracle.synthetic_xg_preds(inp),
        _oracle.metadata(provider),
        None,
        None,
        _oracle._access_tier(inp),
        gkdv_enabled=True,
    )
    baseline = _oracle.score_all_marts(provider, inp, inp.frames)
    assert set(scorers) == set(_oracle.MART_KEYS)
    for mart in _oracle.MART_KEYS:
        _oracle.assert_mart_equal(scorers[mart](inp.frames), baseline[mart], mart)
