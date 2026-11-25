"""The canonical silly-kicks frame builder (spec D8; plan Task 3c).

A tracking compute (pitch_control, shape_graphs) needs each match as silly-kicks' `TRACKING_FRAMES_COLUMNS`: player
positions in the SPADL 0-105/0-68 pitch frame, a `team_id`, a ball row, and `is_goalkeeper`. v2 stores coordinates
**as the provider delivers them** (spec §11.4: centre-origin metres for the tracking providers), so the builder
shapes the per-provider bronze into that provider's `silly_kicks.tracking.<provider>.convert_to_frames` input and lets
silly-kicks own the coordinate transform, the per-period direction, and the canonical schema — never a hand-rolled
transform.

Goalkeeper (ruling R7, spec D8 amended 2026-11-05). `convert_to_frames` takes `is_goalkeeper` as an input column and
applies its own positional **derive** fallback (ADR-007) per team whose supplied flag is unreliable
(`TrackingConversionReport.n_teams_gk_derived`). So the builder supplies the **native** flag for every player in a
team's rostered-keeper SET (`fct_roster_gk` — the starter AND any substitute keeper), reading provider truth, never the
hand-rolled "furthest-back = GK" heuristic D8 bans; a GK substitution is therefore correct, because each keeper is
flagged on its own frames. A team with no keeper in the set is left `False`, so `convert_to_frames`'s derived fallback
fires. The derived path therefore stays inside silly-kicks; the builder imports no private silly-kicks symbol.
"""

# This module is an untyped-library boundary: pandas and silly_kicks ship no type stubs in this project, so every
# DataFrame/Series op is `Unknown` under strict pyright. Relax ONLY the three "unknown"-family rules for this file
# (the same noise tfidf.py silences per-line for sklearn/scipy) + the per-import missing-stub ignore below; real
# checks (reportArgumentType, returns, typed-code attributes) stay on. The shaping logic is covered by
# test_frame_builder.py instead.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any, Final

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking import gradientsports as _gs  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking import skillcorner as _sk  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking import sportec as _sportec  # pyright: ignore[reportMissingTypeStubs]

# The declared-read type is shared with the SPADL steps; it is re-exported here for the frame-builder callers.
from lakehouse.compute.reads import InputRead

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

__all__ = [
    "FRAME_INPUT_READS",
    "InputRead",
    "build_match_frame",
    "resolve_roster_goalkeepers",
    "shape_long_input",
    "shape_skillcorner_input",
]

_BALL_STATE_ALIVE = "alive"

#: Providers whose tracking feeds the LONG `convert_to_frames` input shape -> the silly-kicks converter module.
_LONG_CONVERT = {"gradientsports": _gs, "idsse": _sportec}


#: The four layer-0 view reads `build_match_frame` makes, in its order (perf spec §7.4; plan Task 3).
#: The frames and ball reads also filter on the match's provider record keys (perf spec §7.4a): a filter on the derived
#: `canonical_match_key` alone does not reach the per-provider dedup windows, so each read scanned a whole provider
#: table. `list_contains` is the list form DuckDB pushes down (`IN (SELECT ...)` and `= ANY(?)` do not). The canonical
#: filter stays, so the rows are exactly those of the canonical match. `EXCLUDE (match_key)` keeps the columns as they
#: were.
FRAME_INPUT_READS: Final[tuple[InputRead, ...]] = (
    InputRead(
        "fct_tracking_frames",
        "SELECT * EXCLUDE (match_key) FROM lake.main.fct_tracking_frames "
        "WHERE canonical_match_key = ? AND provider = ? AND list_contains(?::BIGINT[], match_key)",
        with_provider=True,
        with_record_keys=True,
    ),
    InputRead(
        "fct_tracking_ball",
        "SELECT * EXCLUDE (match_key) FROM lake.main.fct_tracking_ball "
        "WHERE canonical_match_key = ? AND list_contains(?::BIGINT[], match_key)",
        with_provider=False,
        with_record_keys=True,
    ),
    InputRead(
        "fct_match_params",
        "SELECT * FROM lake.main.fct_match_params WHERE canonical_match_key = ?",
        with_provider=False,
    ),
    InputRead(
        "fct_roster_gk",
        "SELECT canonical_team_key, canonical_player_key FROM lake.main.fct_roster_gk WHERE canonical_match_key = ?",
        with_provider=False,
    ),
)


def _check_unique(match_key: int, frames: pd.DataFrame, ball: pd.DataFrame) -> None:
    """Fail the match on any duplicate tracking row (owner 2026-11-08): one player row per (period, frame, player)
    and one ball row per (period, frame). A duplicate would silently inflate every tracking metric (the 2026-11-07/08
    Mac run scored GS frames with each player twice). Checked here, per match at use, because a full-table dbt test
    does not scale and crashed the dbt build."""
    players = int(frames.duplicated(["period", "frame", "canonical_player_key"]).sum())
    if players:
        raise ValueError(
            f"duplicate tracking rows for match {match_key}: {players} (period, frame, player) keys repeat"
        )
    balls = int(ball.duplicated(["period", "frame"]).sum())
    if balls:
        raise ValueError(f"duplicate tracking rows for match {match_key}: {balls} (period, frame) ball keys repeat")


def build_match_frame(
    cat: WorkingCatalog, match_key: int, *, provider: str, record_keys: Sequence[int]
) -> pd.DataFrame:
    """Build one match's canonical silly-kicks `TRACKING_FRAMES_COLUMNS` frame from the layer-0 views (spec D8).

    Reads `fct_tracking_frames` + `fct_tracking_ball` (positions, ball, `time_seconds`), `fct_match_params` (the
    per-match `home_team_key` / `home_team_start_left` / `frame_rate` / pitch size, R9) and `fct_roster_gk` (the
    native keeper SET per team — starter + any substitute, R9/D8-amended) for `match_key` — all gold VIEWS, never raw
    bronze (clean-forward, D1). Shapes them into the provider's `convert_to_frames` input (long for gradientsports and
    idsse, wide for skillcorner), sets the native `is_goalkeeper` from that keeper set, and lets silly-kicks own the
    coordinate transform + per-period direction.

    `record_keys` are the match's provider record keys (`input_key.match_record_keys`). The step driver looks them up
    once per match and passes the same tuple to the input key, so the key hashes exactly the rows built here.
    """
    conn = cat.conn
    frames_read, ball_read, params_read, gk_read = FRAME_INPUT_READS
    frames = conn.execute(frames_read.sql, frames_read.params(match_key, provider, record_keys)).df()
    ball = conn.execute(ball_read.sql, ball_read.params(match_key, provider, record_keys)).df()
    _check_unique(match_key, frames, ball)
    params = conn.execute(params_read.sql, params_read.params(match_key, provider)).df()
    if params.empty:
        raise ValueError(f"no fct_match_params row for match {match_key} (provider {provider})")
    p = params.iloc[0]
    # Fail loud here: a null rate would surface deep in silly-kicks (smooth_frames IndexError, merge_asof null keys).
    if pd.isna(p["frame_rate"]):
        raise ValueError(f"no frame_rate for match {match_key} (provider {provider}) in fct_match_params")
    gk_rows = conn.execute(gk_read.sql, gk_read.params(match_key, provider)).df()
    # The team's WHOLE keeper set (starter + any substitute), so a GK substitution is flagged correctly (D8 amended).
    goalkeeper_keys = set(zip(gk_rows["canonical_team_key"], gk_rows["canonical_player_key"], strict=False))

    if provider == "skillcorner":
        bronze = shape_skillcorner_input(
            frames,
            ball,
            frame_rate=float(p["frame_rate"]),
            pitch_length=float(p["pitch_length"]),
            pitch_width=float(p["pitch_width"]),
        )
        bronze = resolve_roster_goalkeepers(bronze, goalkeeper_keys)
        # skillcorner orientation is GEOMETRIC: home_team_start_left is null (R9), so pass None (the converter's
        # geometric net) rather than bool(NA), which raises "boolean value of NA is ambiguous".
        start_left = None if pd.isna(p["home_team_start_left"]) else bool(p["home_team_start_left"])
        frame, _ = _sk.convert_to_frames(
            bronze,
            home_team_id=p["home_team_key"],
            output_convention="ltr",
            home_team_start_left=start_left,
        )
        return frame

    raw = shape_long_input(frames, ball, frame_rate=float(p["frame_rate"]))
    raw = resolve_roster_goalkeepers(raw, goalkeeper_keys)
    # The extra-time direction is its own match fact (silly-kicks require_et_direction): a match with ET periods
    # (3, 4) is rejected without it. Null (no ET, or a provider that does not deliver it) passes None.
    et_left = p.get("home_team_start_left_extra_time")
    frame, _ = _LONG_CONVERT[provider].convert_to_frames(
        raw,
        p["home_team_key"],
        bool(p["home_team_start_left"]),
        home_team_start_left_extratime=None if et_left is None or pd.isna(et_left) else bool(et_left),
        output_convention="ltr",
    )
    return frame


def shape_long_input(
    frames: pd.DataFrame,
    ball: pd.DataFrame,
    *,
    frame_rate: float,
) -> pd.DataFrame:
    """Shape v2 gold-view rows into the LONG `convert_to_frames` input (gradientsports + sportec/idsse, spec D8).

    `frames` is `fct_tracking_frames` filtered to one match (canonical keys + `x_mm`/`y_mm` centre-origin
    millimetres); `ball` is `fct_tracking_ball` for the same match (+ `time_seconds`, R8). Emits one row per player per
    frame plus one ball row per frame, with `x_centered`/`y_centered` in metres (`/1000`), `is_goalkeeper` seeded
    `False` (the caller sets the native flag via `resolve_roster_goalkeepers`; the rest is the `convert_to_frames`
    derive fallback), and the silly-kicks canonical `team_id`/`player_id` taken from the canonical keys.
    `convert_to_frames` owns the centre-origin -> SPADL transform and the per-period direction. gradientsports and
    sportec share this input shape (`EXPECTED_INPUT_COLUMNS`); they differ only in which converter module + id dtype,
    which `build_match_frame` handles.
    """
    time_by_frame = ball[["period", "frame", "time_seconds"]]
    players = frames.merge(time_by_frame, on=["period", "frame"], how="left")
    player_rows = pd.DataFrame(
        {
            "game_id": players["canonical_match_key"],
            "period_id": players["period"],
            "frame_id": players["frame"],
            "time_seconds": players["time_seconds"],
            "frame_rate": frame_rate,
            "player_id": players["canonical_player_key"],
            "team_id": players["canonical_team_key"],
            "is_ball": False,
            "is_goalkeeper": False,
            "x_centered": players["x_mm"] / 1000.0,
            "y_centered": players["y_mm"] / 1000.0,
            "z": 0.0,
            "speed_native": np.nan,
            "ball_state": _BALL_STATE_ALIVE,
        }
    )
    ball_rows = pd.DataFrame(
        {
            "game_id": ball["canonical_match_key"],
            "period_id": ball["period"],
            "frame_id": ball["frame"],
            "time_seconds": ball["time_seconds"],
            "frame_rate": frame_rate,
            "player_id": pd.NA,
            "team_id": pd.NA,
            "is_ball": True,
            "is_goalkeeper": False,
            "x_centered": ball["ball_x_mm"] / 1000.0,
            "y_centered": ball["ball_y_mm"] / 1000.0,
            "z": 0.0,
            "speed_native": np.nan,
            "ball_state": _BALL_STATE_ALIVE,
        }
    )
    # Cast the id columns to Int64 on BOTH frames before concat: the ball frame's player_id/team_id are all-NA, and
    # concatenating an all-NA object column with a typed one raises a pandas FutureWarning about dtype determination.
    for col in ("player_id", "team_id"):
        player_rows[col] = player_rows[col].astype("Int64")
        ball_rows[col] = ball_rows[col].astype("Int64")
    # The DuckDB `.df()` read gives pandas nullable dtypes (e.g. a nullable `x_mm` -> `Float64` after `/1000`), which
    # silly-kicks hands to numpy and numpy rejects ("data type 'Float64' not understood"). Coerce the numeric columns
    # to plain numpy dtypes — matching the dtypes the test_frame_builder fixture (and so convert_to_frames) expects.
    for frame_df in (player_rows, ball_rows):
        for col in ("game_id", "period_id", "frame_id"):
            frame_df[col] = frame_df[col].astype("int64")
        for col in ("x_centered", "y_centered", "time_seconds"):
            frame_df[col] = frame_df[col].astype("float64")
    return pd.concat([player_rows, ball_rows], ignore_index=True)


def resolve_roster_goalkeepers(
    raw_frames: pd.DataFrame,
    goalkeeper_keys: Collection[tuple[object, object]],
) -> pd.DataFrame:
    """Set `is_goalkeeper` on every row whose `(team_id, player_id)` is in the team's rostered-keeper SET (spec D8,
    amended 2026-11-05).

    `goalkeeper_keys` is the set of `(team_id, player_id)` pairs from `fct_roster_gk` — EVERY keeper a team used, the
    starter AND any substitute, so a GK substitution is correct: each keeper is flagged on its own frames (the sub
    keeper only has frames after coming on; review B-CALC-IMPL-01). Returns a COPY: a non-ball row is flagged iff its
    `(team_id, player_id)` is in the set; the ball row and every team with no keeper in the set are left `False`, so
    `convert_to_frames` runs its positional derive fallback for them (R7). Pure — `raw_frames` is never mutated.
    """
    out = raw_frames.copy()
    # The wide (skillcorner) shape has no `is_ball` column and no ball row — treat every row as a player there.
    is_ball = out["is_ball"] if "is_ball" in out.columns else pd.Series(False, index=out.index)
    not_ball = ~is_ball.astype("boolean").fillna(False)
    # OR the per-keeper masks (the set is a handful of keepers per match); a nullable-NA cell never matches a key.
    flag = pd.Series(False, index=out.index)
    team_col: Any = out["team_id"]
    player_col: Any = out["player_id"]
    for team, player in goalkeeper_keys:
        pair = (team_col == team) & (player_col == player)
        flag = flag | pair.astype("boolean").fillna(False)
    out["is_goalkeeper"] = (not_ball & flag).astype(bool)
    return out


def shape_skillcorner_input(
    frames: pd.DataFrame,
    ball: pd.DataFrame,
    *,
    frame_rate: float,
    pitch_length: float,
    pitch_width: float,
) -> pd.DataFrame:
    """Shape v2 gold-view rows into the WIDE skillcorner `convert_to_frames` input (`EXPECTED_INPUT_COLUMNS`, D8).

    skillcorner takes per-PLAYER rows with the ball as `ball_x`/`ball_y`/`ball_z` COLUMNS (no ball row, no `is_ball`),
    `x`/`y` centre-origin metres, a CONTINUOUS `timestamp` (frame / frame_rate — skillcorner's broadcast clock, which
    its converter offsets per period), and the match `pitch_length`/`pitch_width` (metres)
    that it SCALES to the SPADL 105x68 frame. `is_goalkeeper` is seeded `False` (the caller sets the native flag via
    `resolve_roster_goalkeepers`; `convert_to_frames` derives the rest). `ball_z` is 0.0 — `fct_tracking_ball` carries
    no height and pitch control is 2-D; a height-dependent scorer would need the ball-z surfaced first.
    """
    ball_pos = ball[["period", "frame", "ball_x_mm", "ball_y_mm"]]
    rows = frames.merge(ball_pos, on=["period", "frame"], how="left")
    bronze = pd.DataFrame(
        {
            "match_id": rows["canonical_match_key"],
            "period": rows["period"],
            "frame": rows["frame"],
            # skillcorner has no per-frame clock; its converter wants the CONTINUOUS broadcast clock -> frame / fps.
            "timestamp": rows["frame"] / frame_rate,
            "player_id": rows["canonical_player_key"].astype("Int64"),
            "team_id": rows["canonical_team_key"].astype("Int64"),
            "is_goalkeeper": False,
            "x": rows["x_mm"] / 1000.0,
            "y": rows["y_mm"] / 1000.0,
            "ball_x": rows["ball_x_mm"] / 1000.0,
            "ball_y": rows["ball_y_mm"] / 1000.0,
            "ball_z": 0.0,
            "is_visible": rows["is_visible"],
            "frame_rate": frame_rate,
            "pitch_length": float(pitch_length),
            "pitch_width": float(pitch_width),
        }
    )
    # Coerce the DuckDB-read numerics off pandas nullable dtypes to plain numpy (see shape_long_input) so silly-kicks'
    # numpy path accepts them.
    for col in ("match_id", "period", "frame"):
        bronze[col] = bronze[col].astype("int64")
    for col in ("timestamp", "x", "y", "ball_x", "ball_y"):
        bronze[col] = bronze[col].astype("float64")
    return bronze
