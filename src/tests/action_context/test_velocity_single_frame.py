"""D1: a 1-frame player track must yield NaN velocity, not a crash.

SkillCorner match 1552423, period 2, frame batch 184 contained a player with exactly ONE frame.
``analytics.action_context.convert._derive_velocities_savgol`` re-implemented silly-kicks'
short-group velocity fallback but dropped its ``len(x_vals) <= 1`` guard, so ``np.gradient``
(which needs >= 2 points) raised ``ValueError``. The UDF re-raised with the group key, one raising
batch failed the WHOLE ``applyInPandas`` write, and the unit emitted 0 of 550 actions -- inside a
job that reported SUCCESS.

These tests drive ``convert_skillcorner_bronze_to_frames`` -- builder -> preprocess -> ``_finalize``
-- because after the delete-and-depend the velocity step lives in the BUILDER, not in ``_finalize``.
Asserting on a hand-built frame passed to ``_finalize`` would assert on padded NaN rather than on a
guard: ``_AC_FRAME_COLUMNS`` itself contains ``vx``/``vy``/``speed``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.sk_frame_adapters import (
    _AC_FRAME_COLUMNS,
    _finalize,
    convert_skillcorner_bronze_to_frames,
)
from tests.action_context.test_sk_frame_adapters import _period_relative_time, _sc_bronze

_ONE_FRAME_PLAYER = "afw"


def _one_frame_player_bronze() -> pd.DataFrame:
    """Standard synthetic SC bronze, but ``afw`` appears in EXACTLY ONE frame (the 1552423-p2 shape)."""
    bronze = _sc_bronze()
    drop = bronze.index[(bronze["player_id"] == _ONE_FRAME_PLAYER) & (bronze["frame"] != 0)]
    return bronze.drop(index=drop).reset_index(drop=True)


def test_single_frame_track_yields_nan_velocity_not_a_crash() -> None:
    bronze = _one_frame_player_bronze()
    prt = _period_relative_time(bronze, offset=0.0)

    frames, _report = convert_skillcorner_bronze_to_frames(
        bronze, game_id=99, home_team_id="H", period_relative_time=prt
    )

    once = frames[frames["player_id"].astype(str).str.contains(_ONE_FRAME_PLAYER)]
    assert len(once) == 1, f"fixture must give {_ONE_FRAME_PLAYER} exactly one frame, got {len(once)}"
    assert np.isnan(once["vx"].iloc[0]), "a 1-frame track has no velocity -- must be NaN, not a crash"
    assert np.isnan(once["vy"].iloc[0])

    # Non-degenerate tracks must still carry REAL velocities. Without this, the NaN assertion above
    # would pass even if velocity derivation were deleted outright.
    others = frames[~frames["player_id"].astype(str).str.contains(_ONE_FRAME_PLAYER) & ~frames["is_ball"]]
    assert others["vx"].notna().any(), "multi-frame tracks must carry real velocities"


def test_finalize_drops_the_preprocess_scratch_columns() -> None:
    """The silly-kicks preprocess scratch columns must be dropped before ``_finalize``'s
    symmetric-diff check, or EVERY unit fails on schema drift.

    The scratch columns are INJECTED here on purpose: a frame padded to exactly
    ``_AC_FRAME_COLUMNS`` carries none, so the drop would never be exercised and the assertion
    would pass vacuously.
    """
    base = pd.DataFrame({col: [np.nan] for col in _AC_FRAME_COLUMNS})
    base["frame_id"] = [1]
    base["is_ball"] = [False]
    dirty = base.assign(x_smoothed=1.0, y_smoothed=2.0, _preprocessed_with="savgol")

    out = _finalize(dirty, derive_velocities=True)

    assert set(out.columns) == set(_AC_FRAME_COLUMNS)
    for leaked in ("x_smoothed", "y_smoothed", "_preprocessed_with"):
        assert leaked not in out.columns


def test_empty_bronze_is_unsupported() -> None:
    """Empty bronze is unsupported UPSTREAM — before and after the delete-and-depend.

    Reviewer H3 proposed guarding an ``IndexError`` in ``smooth_frames``
    (``frames["frame_rate"].dropna().iloc[0]``) by passing ``preprocess=None`` on empty bronze. That
    guard is unreachable: the builder raises FIRST, at ``skillcorner.py:138``
    (``str(src["match_id"].iloc[0])``), whether or not ``preprocess`` is passed. The old code raised
    identically. So this is a pre-existing upstream contract, NOT a regression introduced here — and
    adding a lakehouse-side guard would invent a behaviour that never existed.

    Pinned so that if silly-kicks ever starts supporting empty input, we notice and can decide.
    """
    bronze = _sc_bronze()
    empty = bronze.iloc[0:0]
    prt = _period_relative_time(bronze, offset=0.0).iloc[0:0]

    with pytest.raises(IndexError):
        convert_skillcorner_bronze_to_frames(empty, game_id=99, home_team_id="H", period_relative_time=prt)
