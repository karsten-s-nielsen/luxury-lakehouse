"""pitch_control compute (spec D3/D8; plan Task 4; perf spec §7.3): the PURE pitch-control scorer.

`pitch_control_values()` derives velocity via the silly-kicks preprocess seam and scores each frame with
`compute_pitch_control` (Spearman). The per-frame summary — each team's mean controlled-area fraction over the pitch —
is the `pitch_control_values` bronze row, keyed per `(canonical_match_key, period, frame, canonical_team_key)`. The
layer-2 mart aggregates it to the per-player grain (spec D-open-1). `home_and_away()` is the pure home/away rule.

This module holds only output logic (no catalog, no bronze write): it is digested into the compute input key
(`input_key.TRACKING_CODE_MODULES`), so any change here re-computes. The per-match driver that reads the catalog and
writes bronze is `tracking_steps.TRACKING_FRAME_METRICS`.
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules for this boundary module (see frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
from silly_kicks.tracking.pitch_control import (  # pyright: ignore[reportMissingTypeStubs]
    SpearmanParams,
    compute_pitch_control,
)
from silly_kicks.tracking.preprocess import (  # pyright: ignore[reportMissingTypeStubs]
    PreprocessConfig,
    derive_velocities,
    smooth_frames,
)

from lakehouse.compute.control import ComputeAbandoned

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["PITCH_CONTROL_SCHEMA", "home_and_away", "pitch_control_values"]

#: Bronze schema for `pitch_control_values` (audit columns added by the bronze writer). One row per team per frame.
PITCH_CONTROL_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("canonical_match_key", pa.int64()),
        pa.field("period", pa.int32()),
        pa.field("frame", pa.int32()),
        pa.field("canonical_team_key", pa.int64()),
        pa.field("control_fraction", pa.float64()),
    ]
)


def pitch_control_values(
    frame: pd.DataFrame,
    *,
    home_team_id: int,
    away_team_id: int,
    params: SpearmanParams | None = None,
    should_stop: Callable[[], bool] | None = None,
    check_every: int = 500,
) -> pa.Table:
    """Score pitch control per frame and return the `pitch_control_values` rows (PURE; no catalog, no bronze write).

    `frame` is one match's canonical `TRACKING_FRAMES_COLUMNS` (from `build_match_frame`). Velocity is derived here via
    the silly-kicks preprocess seam. For each frame, `compute_pitch_control` gives the home team's control surface;
    the home fraction is its grid mean and the away fraction is its complement (control sums to 1 pointwise). Emits two
    rows per frame — one per team — at `(canonical_match_key, period, frame, canonical_team_key, control_fraction)`.

    `should_stop` (perf spec §7.5) is called every `check_every` frames, after velocity derivation; when it returns
    True the scorer raises `ComputeAbandoned` and returns nothing. It never changes the output of a finished call.
    """
    cfg = PreprocessConfig.default()
    velocity: Any = derive_velocities(smooth_frames(frame, config=cfg), config=cfg)
    match_key = int(velocity["game_id"].iloc[0])
    periods: list[int] = []
    frames: list[int] = []
    teams: list[int] = []
    fractions: list[float] = []
    for index, ((period_id, frame_id), grp) in enumerate(velocity.groupby(["period_id", "frame_id"], sort=True)):
        if should_stop is not None and index % check_every == 0 and should_stop():
            raise ComputeAbandoned(f"pitch_control: match {match_key} stopped at frame group {index}")
        surface = compute_pitch_control(grp, attacking_team_id=home_team_id, params=params)
        home_fraction = float(np.mean(surface.surface))
        for team, fraction in ((home_team_id, home_fraction), (away_team_id, 1.0 - home_fraction)):
            periods.append(int(period_id))
            frames.append(int(frame_id))
            teams.append(int(team))
            fractions.append(fraction)
    return pa.table(
        {
            "canonical_match_key": pa.array([match_key] * len(periods), pa.int64()),
            "period": pa.array(periods, pa.int32()),
            "frame": pa.array(frames, pa.int32()),
            "canonical_team_key": pa.array(teams, pa.int64()),
            "control_fraction": pa.array(fractions, pa.float64()),
        },
        schema=PITCH_CONTROL_SCHEMA,
    )


def home_and_away(frame: pd.DataFrame, home_team_key: int) -> tuple[int, int]:
    """The canonical home team and the frame's other (non-ball) team as away; home again when the frame shows no
    other team. PURE: the home key comes from the `HOME_TEAM_READ` input read (perf spec §7.4)."""
    teams = {int(t) for t in frame.loc[~frame["is_ball"], "team_id"].dropna().unique()}
    away = next((t for t in teams if t != home_team_key), home_team_key)
    return home_team_key, away
