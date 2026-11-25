"""line_breaking compute (spec D9; plan Task 5): a LAYER-2 actions x frames tracking compute.

Per match it reads the `fct_actions` gold view (the SPADL actions, Task 4b) and builds the canonical LTR frame
(`frames.build_match_frame`), then `detect_line_breaking` (Ward-clustering defensive-line crossing) labels each pass/
cross action — it aligns actions to frames internally. The per-action result is written to the `line_breaking_results`
bronze, keyed per `(canonical_match_key, period_id, action_id)`; the mart aggregates through-line passes per player.

`line_breaking_values()` is the PURE core (actions + frame -> arrow rows), unit-tested on a fixture, and
`actions_for_scorer()` shapes `fct_actions` for it. This module holds only output logic: it is digested into the compute
input key (`input_key.TRACKING_CODE_MODULES`). The layer-2 per-match driver that reads the catalog and writes bronze is
`tracking_steps.LINE_BREAKING`; it is layer 2 because it reads `fct_actions`, a view over another compute's bronze —
the run builds `fct_actions` between the layer-1 and layer-2 compute stages (spec D9 layered seam).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import Any, Final

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
from silly_kicks.tracking._line_breaking import detect_line_breaking  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking.utils import link_actions_to_frames  # pyright: ignore[reportMissingTypeStubs]

__all__ = [
    "COVERAGE_GAP_SECONDS",
    "LINE_BREAKING_SCHEMA",
    "MIN_LINK_RATE",
    "TIME_BASE_FLOOR_SECONDS",
    "TimeBaseError",
    "actions_for_scorer",
    "check_link_rate",
    "check_time_base",
    "line_breaking_values",
]

#: v1 ADR-040: a period-relative clock starts each period near 0 s. A period whose FIRST action or frame is at or after
#: this floor is on an absolute clock (a time-base mismatch). Min-based, so sparse coverage cannot trip it.
TIME_BASE_FLOOR_SECONDS: Final = 1800.0
#: silly-kicks' default `min_link_rate`: the share of the actions inside a period's frame coverage that must link.
MIN_LINK_RATE: Final = 0.5
#: A gap between consecutive frames longer than this is a gap in the tracking (no coverage): real tracking runs at
#: 10-25 fps (0.04-0.1 s per frame), so 1 s is well above a frame step and the 0.2 s link tolerance.
COVERAGE_GAP_SECONDS: Final = 1.0


class TimeBaseError(ValueError):
    """Actions and frames are not on one period-relative clock, or the frames cover actions they do not link.
    The step records the match as `compute_failed` and writes no rows (owner 2026-11-07: fail loud)."""


class FrameKeyError(ValueError):
    """The actions link to frames by time, but no linked action's (period, team) key matches a frame row, so
    silly-kicks cannot resolve any action's attacking direction and every result would be null (owner 2026-11-08:
    fail loud; the Mac lake had rounded GS team keys). The step records the match as `compute_failed`."""


LINE_BREAKING_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("canonical_match_key", pa.int64()),
        pa.field("period_id", pa.int32()),
        pa.field("action_id", pa.int32()),
        pa.field("canonical_player_key", pa.int64()),
        pa.field("line_break", pa.bool_()),
        pa.field("lines_broken", pa.int32()),
        pa.field("line_breaking_type", pa.string()),
    ]
)


def line_breaking_values(actions: pd.DataFrame, frame: pd.DataFrame) -> pa.Table:
    """Label each pass/cross action with its line-breaking result (PURE; no catalog, no bronze write).

    `actions` is one match's `fct_actions` rows shaped for the scorer — the silly-kicks action keys (`game_id`,
    `team_id`, `player_id`) taken from the canonical keys, plus `period_id`, `time_seconds`, `action_id` and the SPADL
    coords/type. `frame` is the canonical LTR `TRACKING_FRAMES_COLUMNS`. `detect_line_breaking` aligns actions to
    frames and returns per-action `line_break__ward` / `lines_broken__ward` / `line_breaking_type__ward`.
    """
    check_time_base(actions, frame)
    # The guard below decides on coverage; silly-kicks' own raw-rate warning would also fire on a short tracking slice.
    links, _report = link_actions_to_frames(actions, frame, min_link_rate=MIN_LINK_RATE, on_low_coverage="ignore")
    check_link_rate(actions, frame, links)
    check_action_keys(actions, frame, links)
    result: Any = detect_line_breaking(actions, frame, links=links)
    return pa.table(
        {
            "canonical_match_key": pa.array(actions["game_id"], pa.int64()),
            "period_id": pa.array(actions["period_id"], pa.int32()),
            "action_id": pa.array(actions["action_id"], pa.int32()),
            "canonical_player_key": pa.array(actions["player_id"].astype("Int64"), pa.int64()),
            "line_break": pa.array(result["line_break__ward"].astype("boolean"), pa.bool_()),
            "lines_broken": pa.array(result["lines_broken__ward"].astype("Int64"), pa.int32()),
            "line_breaking_type": pa.array(result["line_breaking_type__ward"].astype("string"), pa.string()),
        },
        schema=LINE_BREAKING_SCHEMA,
    )


def check_time_base(actions: pd.DataFrame, frame: pd.DataFrame) -> None:
    """The two-sided time-base guard (v1 ADR-040): per period, the first action and the first frame must both be
    before `TIME_BASE_FLOOR_SECONDS`. Raises `TimeBaseError`."""
    for side, rows in (("actions", actions), ("frames", frame)):
        if len(rows) == 0:
            continue
        first: Any = rows.groupby("period_id")["time_seconds"].min()
        for period, start in first.items():
            if pd.notna(start) and float(start) >= TIME_BASE_FLOOR_SECONDS:
                raise TimeBaseError(
                    f"period {int(period)} {side} start at {float(start):.2f} s: an absolute clock "
                    f"(the floor is {TIME_BASE_FLOOR_SECONDS:.0f} s for a period-relative one)"
                )


def _covered(times: Any, frame_times: Any) -> Any:
    """Which `times` fall inside the frames' contiguous coverage: the spans of consecutive frames no more than
    `COVERAGE_GAP_SECONDS` apart (an isolated frame is a zero-length span)."""
    ft = np.unique(frame_times[~np.isnan(frame_times)])
    if len(ft) == 0:
        return np.zeros(len(times), dtype=bool)
    breaks = np.flatnonzero(np.diff(ft) > COVERAGE_GAP_SECONDS)
    starts = ft[np.concatenate(([0], breaks + 1))]
    ends = ft[np.concatenate((breaks, [len(ft) - 1]))]
    span = np.searchsorted(starts, times, side="right") - 1
    inside = span >= 0
    inside[inside] = times[inside] <= ends[span[inside]]
    return inside


def check_link_rate(actions: pd.DataFrame, frame: pd.DataFrame, links: pd.DataFrame) -> None:
    """The coverage-aware link rate (v1 ADR-040 completeness): per period, of the actions inside the frames'
    contiguous coverage (`_covered`), at least `MIN_LINK_RATE` must link to a frame. Actions outside it (after a short
    tracking slice, or in a tracking gap) do not count. Raises `TimeBaseError`."""
    if len(actions) == 0 or len(frame) == 0:
        return
    linked = set(links.loc[links["frame_id"].notna(), "action_id"].astype("int64"))
    frame_times: Any = dict(iter(frame.groupby("period_id")["time_seconds"]))
    for key, rows in actions.groupby("period_id"):
        period: Any = key
        if period not in frame_times:
            continue
        times = rows["time_seconds"].to_numpy(dtype="float64")
        covered = rows[_covered(times, frame_times[period].to_numpy(dtype="float64"))]
        if len(covered) == 0:
            continue
        hits = sum(int(a) in linked for a in covered["action_id"])
        rate = hits / len(covered)
        if rate < MIN_LINK_RATE:
            raise TimeBaseError(
                f"period {int(period)} link rate {rate:.2f} ({hits} of {len(covered)} covered actions) is below "
                f"{MIN_LINK_RATE}: the frames cover these actions but do not link them"
            )


def check_action_keys(actions: pd.DataFrame, frame: pd.DataFrame, links: pd.DataFrame) -> None:
    """Per period: of the linked actions that carry a team key, at least one key must be a team of that period's
    player rows. silly-kicks resolves an action's direction by the (period, team) key, so with no match every result
    is null. A null-actor action has no team key and is not counted. Raises `FrameKeyError`."""
    if len(actions) == 0 or len(frame) == 0:
        return
    linked = set(links.loc[links["frame_id"].notna(), "action_id"].astype("int64"))
    players: Any = frame[~frame["is_ball"].astype("boolean").fillna(False) & frame["team_id"].notna()]
    teams: Any = {p: set(g.astype("int64")) for p, g in players.groupby("period_id")["team_id"]}
    acting: Any = actions[actions["action_id"].astype("int64").isin(sorted(linked)) & actions["team_id"].notna()]
    for key, rows in acting.groupby("period_id"):
        period: Any = key
        if teams.get(period, set()) & set(rows["team_id"].astype("int64")):
            continue
        raise FrameKeyError(
            f"period {int(period)}: no linked action's team key ({len(rows)} actions) matches a frame team "
            f"{sorted(teams.get(period, set()))}"
        )


def actions_for_scorer(fct_actions: pd.DataFrame) -> pd.DataFrame:
    """Rename `fct_actions` canonical keys to the silly-kicks action keys `detect_line_breaking` expects, and drop
    actions with no period-relative time. `detect_line_breaking` aligns actions to frames by `time_seconds`
    (`merge_asof`, which rejects a null key); an action with no time cannot be located in tracking, so it legitimately
    gets no line-breaking result rather than crashing the match."""
    out: Any = fct_actions.rename(
        columns={
            "canonical_match_key": "game_id",
            "canonical_team_key": "team_id",
            "canonical_player_key": "player_id",
        }
    )
    return out[out["time_seconds"].notna()]
