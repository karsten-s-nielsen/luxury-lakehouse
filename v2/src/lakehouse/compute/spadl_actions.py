"""SPADL actions layer (spec D9; plan Task 4b): events -> SPADL actions, per provider.

Each provider's flattened event bronze is shaped into that provider's silly-kicks SPADL converter input, converted,
and validated (`validate_spadl`) into the `spadl_actions` bronze (0-105/0-68 coords, period-relative `time_seconds`).
`line_breaking` (Task 5) reads the `fct_actions` gold view over it. gradientsports is built first (direct converter);
idsse (`spadl.sportec`, kloppy-oriented) and skillcorner (`spadl.skillcorner`, inference) follow.

`gradientsports_spadl_actions()` is the PURE core (bronze rows -> validated SPADL arrow), unit round-trip-tested; the
match-iterating ComputeStep that reads the catalog and writes bronze wraps it (seeded e2e).

Every step is one `SpadlStep`: its per-match reads are declared (`SpadlRead`), and it skips a match whose exact input
key (the in-DuckDB hash of those reads, the code digest of `SPADL_CODE_MODULES` and the library versions) matches the
`_source_hash` of its `spadl_actions` rows (owner rule 2026-11-06: every per-match step has a skip guard).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Literal, cast

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pyarrow as pa
from silly_kicks.spadl import gradientsports as _gs_spadl  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl import skillcorner as _sk_spadl  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl import sportec as _sportec_spadl  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl import statsbomb as _sb_spadl  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl import validate_spadl  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl import wyscout as _ws_spadl  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.compute import input_key as ik
from lakehouse.compute.base import ComputeOutcome, MatchBronze, SkipReason, run_per_match_bronze
from lakehouse.compute.control import UNBOUNDED, ComputeControl
from lakehouse.compute.reads import InputRead
from lakehouse.ids.keys import player_key, team_key
from lakehouse.ids.native import (
    Provider,
    idsse_native_player_id,
    idsse_native_team_id,
    skillcorner_native_player_id,
    skillcorner_native_team_id,
)

if TYPE_CHECKING:
    from datetime import datetime

    from lakehouse.catalog.working import WorkingCatalog
    from lakehouse.config.model import ResourceCaps
    from lakehouse.runs._common import ErrorSink

__all__ = [
    "SPADL_ACTIONS_GRADIENTSPORTS",
    "SPADL_ACTIONS_IDSSE",
    "SPADL_ACTIONS_SCHEMA",
    "SPADL_ACTIONS_SKILLCORNER",
    "SPADL_ACTIONS_STATSBOMB",
    "SPADL_ACTIONS_WYSCOUT",
    "Pointers",
    "SpadlRead",
    "SpadlStep",
    "gradientsports_spadl_actions",
    "idsse_spadl_actions",
    "skillcorner_spadl_actions",
    "statsbomb_spadl_actions",
    "wyscout_spadl_actions",
]

#: The canonical-keyed `spadl_actions` bronze schema (audit columns added by the bronze writer). One row per action.
SPADL_ACTIONS_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("canonical_match_key", pa.int64()),
        pa.field("period_id", pa.int32()),
        pa.field("action_id", pa.int32()),
        pa.field("original_event_id", pa.string()),
        pa.field("canonical_player_key", pa.int64()),
        pa.field("canonical_team_key", pa.int64()),
        pa.field("time_seconds", pa.float64()),
        pa.field("start_x", pa.float64()),
        pa.field("start_y", pa.float64()),
        pa.field("end_x", pa.float64()),
        pa.field("end_y", pa.float64()),
        pa.field("type_id", pa.int32()),
        pa.field("result_id", pa.int32()),
        pa.field("bodypart_id", pa.int32()),
    ]
)

#: EXPECTED_INPUT_COLUMNS qualifiers that are v2 `possession_events_<name>` with the prefix stripped (spec D9).
_GS_POSSESSION_QUALIFIERS: tuple[str, ...] = (
    "possession_event_type",
    "body_type",
    "ball_height_type",
    "pass_outcome_type",
    "pass_type",
    "incompletion_reason_type",
    "cross_outcome_type",
    "cross_type",
    "cross_zone_type",
    "shot_outcome_type",
    "shot_type",
    "shot_nature_type",
    "shot_initial_height_type",
    "save_height_type",
    "save_rebound_type",
    "carry_type",
    "ball_carry_outcome",
    "carry_intent",
    "carry_defender_player_id",
    "challenge_type",
    "challenge_outcome_type",
    "challenger_player_id",
    "challenge_winner_player_id",
    "tackle_attempt_type",
    "clearance_outcome_type",
    "rebound_outcome_type",
    "keeper_touch_type",
    "touch_outcome_type",
    "touch_type",
)
#: game-level + foul fields: EXPECTED name -> v2 gradientsports_events column.
_GS_DIRECT: dict[str, str] = {
    "game_id": "game_id",
    "event_id": "game_event_id",
    "possession_event_id": "possession_event_id",
    "period_id": "game_events_period",
    "time_seconds": "game_events_start_game_clock",  # re-based to period-relative in the adapter (v1 ADR-040)
    "start_time": "start_time",
    "team_id": "game_events_team_id",
    "player_id": "game_events_player_id",
    "game_event_type": "game_events_game_event_type",
    "set_piece_type": "game_events_setpiece_type",
    "foul_type": "fouls_foul_type",
    "on_field_offense_type": "fouls_on_field_offense_type",
    "final_offense_type": "fouls_final_offense_type",
    "on_field_foul_outcome_type": "fouls_on_field_foul_outcome_type",
    "final_foul_outcome_type": "fouls_final_foul_outcome_type",
}


# validate_spadl checks the SPADL schema AND that the id columns are int64. At this stage the ids are provider-NATIVE —
# DFL string ids for idsse, nullable Int64 for gradientsports/skillcorner — because `_canonicalize_<provider>` resolves
# them to int64 canonical keys only AFTER conversion, and line_breaking joins `fct_actions` x frames on those int64
# canonical keys (not these native ids). So this one int64-id dtype warning is EXPECTED; `_validate_spadl_native_ids`
# suppresses exactly it and nothing else (review A-IMPL-03 / B-CALC-IMPL-03).
_SPADL_ID_DTYPE_WARNING = r"Column '\w+' has dtype '(object|Int64)', expected 'int64'"


def _validate_spadl_native_ids(actions: pd.DataFrame) -> None:
    """Run `validate_spadl` on provider-native-id actions, suppressing ONLY the expected int64-id dtype warning (the
    ids become int64 canonical keys downstream). Any OTHER `validate_spadl` warning — a real schema regression — still
    propagates."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=_SPADL_ID_DTYPE_WARNING, category=UserWarning)
        validate_spadl(actions)


#: The nominal seconds from kick-off at which each GS period starts (v1 ADR-040 `_GS_NOMINAL_PERIOD_START_SECONDS`).
#: GS `startGameClock` is this nominal start plus the period's elapsed time; silly-kicks' `time_seconds` is
#: period-relative, as are the GS frames, so the adapter subtracts it. Period 0 is the pre-match marker (a `G`
#: event with no team or player at clock 0, before FIRSTKICKOFF; Mac lake 2026-11-08), so it starts at 0 s.
_GS_NOMINAL_PERIOD_START_SECONDS: Final[Mapping[int, float]] = {
    0: 0.0,
    1: 0.0,
    2: 2700.0,
    3: 5400.0,
    4: 6300.0,
    5: 7200.0,
}


def _gs_nominal_start(period: Any) -> float | None:
    return None if pd.isna(period) else _GS_NOMINAL_PERIOD_START_SECONDS.get(int(period))


def _gs_period_relative(period: pd.Series, clock: pd.Series) -> pd.Series:
    """GS `startGameClock` (nominal absolute seconds) -> period-relative `time_seconds`. A row with a clock in a period
    that has no nominal start raises; a row with no clock stays NaN (nothing to re-base)."""
    seconds: Any = cast("pd.Series", pd.to_numeric(clock)).astype("float64")
    offset: Any = period.map(_gs_nominal_start).astype("float64")
    unknown = seconds.notna() & offset.isna()
    if unknown.any():
        labels = sorted({"<NA>" if pd.isna(p) else str(int(p)) for p in period[unknown]})
        raise ValueError(f"no nominal start for gradientsports period {', '.join(labels)}")
    return seconds - offset


def _gradientsports_spadl_input(events: pd.DataFrame, event_objects: pd.DataFrame) -> pd.DataFrame:
    """Map v2 `gradientsports_events` (+ the per-event ball object) to the SPADL converter's EXPECTED_INPUT_COLUMNS.

    The converter's Component-4 excludes voided possession events (`possessionEvents.nonEvent` — disallowed goals,
    annulled plays) only when the column is supplied under the EXACT raw camelCase name `nonEvent` (it checks
    `"nonEvent" in events.columns`, NOT the snake_case `non_event`); otherwise it warns "nonEvent column not supplied"
    and emits the voided plays as SPADL actions, which line_breaking / VAEP / xT would then score (review A-IMPL-02).

    `time_seconds` is period-relative (`_gs_period_relative`): the 2026-11-06 Mac lake held GS period-2 actions at
    2700 s+ against period-relative frames, so line_breaking linked 23 % of them (v1 ADR-040, lost in v2).
    """
    ball: Any = event_objects[event_objects["object_kind"] == "ball"][["event_index", "x_mm", "y_mm"]]
    merged: Any = events.merge(ball, on="event_index", how="left")
    columns: dict[str, Any] = {out: merged[src] for out, src in _GS_DIRECT.items()}
    columns["time_seconds"] = _gs_period_relative(merged["game_events_period"], merged["game_events_start_game_clock"])
    columns["ball_x"] = merged["x_mm"] / 1000.0  # centred metres (the converter offsets to 0-105/0-68)
    columns["ball_y"] = merged["y_mm"] / 1000.0
    for qualifier in _GS_POSSESSION_QUALIFIERS:
        columns[qualifier] = merged[f"possession_events_{qualifier}"]
    # Component-4 voided-event exclusion: the converter wants the raw camelCase `nonEvent` name, not snake_case. Fill
    # NA -> False (a null nonEvent is NOT a voided event) as a PLAIN bool: the converter's bool coercion chokes on
    # `pd.NA` (`bool(pd.NA)` raises "boolean value of NA is ambiguous"), which the DuckDB-read nullable-boolean column
    # carries — the e2e caught this where the unit fixture's Python `None` did not.
    columns["nonEvent"] = merged["possession_events_non_event"].astype("boolean").fillna(False).astype(bool)
    # Challenge team affiliation is a roster join the converter tolerates as null this cycle (spec D9 note).
    columns["challenger_team_id"] = pd.NA
    columns["challenge_winner_team_id"] = pd.NA
    return pd.DataFrame(columns)


def _gradientsports_actions_df(
    events: pd.DataFrame,
    event_objects: pd.DataFrame,
    *,
    home_team_id: object,
    home_team_start_left: bool,
    home_team_start_left_extratime: bool | None = None,
) -> pd.DataFrame:
    """The validated SPADL actions (GS-native ids) for one match (PURE). `home_team_start_left_extratime` is required
    by the converter when the match has extra-time periods (period_id in {3, 4}); None is fine for a match with none."""
    spadl_input = _gradientsports_spadl_input(events, event_objects)
    actions, _report = _gs_spadl.convert_to_actions(
        spadl_input,
        cast("Any", home_team_id),
        home_team_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
    )
    _validate_spadl_native_ids(actions)
    return actions


def gradientsports_spadl_actions(
    events: pd.DataFrame,
    event_objects: pd.DataFrame,
    *,
    home_team_id: object,
    home_team_start_left: bool,
    home_team_start_left_extratime: bool | None = None,
) -> pa.Table:
    """Convert a gradientsports match's flattened event bronze to validated SPADL actions (PURE; no catalog)."""
    actions = _gradientsports_actions_df(
        events,
        event_objects,
        home_team_id=home_team_id,
        home_team_start_left=home_team_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
    )
    return pa.Table.from_pandas(actions, preserve_index=False)


# idsse (DFL/sportec) acting-player columns, in priority order. Each DFL event populates only its own type's columns, so
# a left-to-right first-non-null picks the acting player; ThrowIn/TacklingGame carry no plain actor column (null player,
# which the sportec converter tolerates). The sportec converter was designed for this exact bronze (spec D9).
_IDSSE_PLAYER_COLS: tuple[str, ...] = (
    "play_player",
    "shot_at_goal_player",
    "free_kick_shot_at_goal_player",
    "free_kick_play_player",
    "corner_kick_play_player",
    "goal_kick_play_player",
    "other_ball_action_player",
    "foul_committing_player",
)
# idsse acting-team columns, same priority (plus the set-piece container teams as a fallback).
_IDSSE_TEAM_COLS: tuple[str, ...] = (
    "play_team",
    "shot_at_goal_team",
    "free_kick_shot_at_goal_team",
    "free_kick_play_team",
    "corner_kick_play_team",
    "goal_kick_play_team",
    "other_ball_action_team",
    "foul_team_fouler",
    "corner_kick_team",
    "free_kick_team",
    "goal_kick_team",
    "tackling_game_loser_team",
)


def _coalesce_first(events: pd.DataFrame, cols: tuple[str, ...]) -> Any:
    """First non-null value per row across the present columns (the acting player/team of a per-type DFL event)."""
    present = [c for c in cols if c in events.columns]
    if not present:
        return pd.Series([pd.NA] * len(events), index=events.index, dtype="string")
    return events[present].bfill(axis=1).iloc[:, 0]


#: One period's frame span from `idsse_tracking_frames`: (period, first_frame, last_frame).
PeriodBounds = tuple[int, int, int]


def _idsse_spadl_input(events: pd.DataFrame, period_bounds: list[PeriodBounds], frame_rate: float) -> pd.DataFrame:
    """Normalize v2 `idsse_events` into the `silly_kicks.spadl.sportec` input columns (spec D9).

    An idsse event carries no period/time, only a DFL `calculated_frame`. `period_bounds` gives each period's
    [first_frame, last_frame] span (from `idsse_tracking_frames`); an event's `calculated_frame` falls inside one
    period's range (the positions slice is sparse, so NOT every event sits on an exact tracking frame), which gives the
    period and the period-relative `time_seconds = (calculated_frame - first_frame) / frame_rate` (R8). An event whose
    frame is in no period range (or is null) is unlocatable — the sportec converter needs a non-null period for its
    per-period coord flip — so it is dropped. `event_type` is the DFL child tag (already a column); `x`/`y` are the DFL
    centre-origin metres (`x_position_mm / 1000`); the acting `player_id`/`team` coalesce the per-type DFL columns."""
    ev: Any = events.copy()
    cf = ev["calculated_frame"].to_numpy(dtype="float64")
    conds = [(cf >= f0) & (cf <= f1) for _, f0, f1 in period_bounds]
    ev["period"] = np.select(conds, [p for p, _, _ in period_bounds], default=-1)
    first_frame = np.select(conds, [float(f0) for _, f0, _ in period_bounds], default=np.nan)
    ev["timestamp_seconds"] = (cf - first_frame) / frame_rate
    ev = ev[ev["period"] != -1]
    return pd.DataFrame(
        {
            "match_id": ev["match_id"].astype("string"),
            "event_id": ev["event_id"].astype("Int64"),
            "event_type": ev["event_type"].astype("string"),
            "period": ev["period"].astype("Int64"),
            "timestamp_seconds": ev["timestamp_seconds"].astype("float64"),
            "player_id": _coalesce_first(ev, _IDSSE_PLAYER_COLS).astype("string"),
            "team": _coalesce_first(ev, _IDSSE_TEAM_COLS).astype("string"),
            "x": (ev["x_position_mm"] / 1000.0).astype("float64"),
            "y": (ev["y_position_mm"] / 1000.0).astype("float64"),
        }
    )


def _idsse_actions_df(
    events: pd.DataFrame,
    period_bounds: list[PeriodBounds],
    frame_rate: float,
    *,
    home_team_id: str,
    home_team_start_left: bool,
    home_team_start_left_extratime: bool | None = None,
) -> pd.DataFrame:
    """The validated SPADL actions (idsse DFL-native ids) for one match (PURE)."""
    spadl_input = _idsse_spadl_input(events, period_bounds, frame_rate)
    actions, _report = _sportec_spadl.convert_to_actions(
        spadl_input,
        home_team_id,
        home_team_start_left=home_team_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
    )
    _validate_spadl_native_ids(actions)
    return actions


def idsse_spadl_actions(
    events: pd.DataFrame,
    period_bounds: list[PeriodBounds],
    frame_rate: float,
    *,
    home_team_id: str,
    home_team_start_left: bool,
    home_team_start_left_extratime: bool | None = None,
) -> pa.Table:
    """Convert an idsse match's flattened DFL event bronze to validated SPADL actions (PURE; no catalog)."""
    actions = _idsse_actions_df(
        events,
        period_bounds,
        frame_rate,
        home_team_id=home_team_id,
        home_team_start_left=home_team_start_left,
        home_team_start_left_extratime=home_team_start_left_extratime,
    )
    return pa.Table.from_pandas(actions, preserve_index=False)


@dataclass(frozen=True)
class Pointers:
    """The canonical pointers of one match's own match, player and team surrogate keys (`record_key` ->
    `canonical_key`). A key with no pointer resolves to itself. A step reads them through its declared `pointers`
    read (`_pointer_read`), so the skip key covers exactly the pointers the output uses, and a pointer of another
    match never re-computes this one."""

    match: Mapping[int, int]
    player: Mapping[int, int]
    team: Mapping[int, int]

    @classmethod
    def of(cls, frame: pd.DataFrame) -> Pointers:
        by: dict[str, dict[int, int]] = {"match": {}, "player": {}, "team": {}}
        for entity, record, canonical in zip(frame["entity"], frame["record_key"], frame["canonical_key"], strict=True):
            by[str(entity)][int(record)] = int(canonical)
        return cls(match=by["match"], player=by["player"], team=by["team"])

    def match_key(self, match_key: int) -> int:
        """The canonical key of the surrogate `match_key`. The actions must carry the SAME `canonical_match_key` the
        tracking views carry, so a match that clusters cross-provider joins `fct_match_params`/`fct_tracking_frames`."""
        return self.match.get(match_key, match_key)


def _native_to_canonical(
    events: pd.DataFrame, native_col: str, key_col: str, canonical: Mapping[int, int]
) -> dict[int, int]:
    """Map a provider-native id (e.g. game_events_player_id) to its canonical key via the event's surrogate key column
    (`key_col`); a surrogate with no pointer stays itself."""
    pairs: Any = events[[native_col, key_col]].dropna().drop_duplicates()
    return {
        int(native): canonical.get(int(key), int(key))
        for native, key in zip(pairs[native_col], pairs[key_col], strict=True)
    }


def _string_canonical_map(
    provider: Provider, ids: Any, native_fn: Any, key_fn: Any, canonical: Mapping[int, int]
) -> dict[str, int]:
    """Map a provider-native id STRING (e.g. idsse 'DFL-OBJ-1', a skillcorner numeric id) to its canonical key: compute
    the surrogate key from the string (`key_fn(provider, native_fn(id))`), then resolve it through `canonical` (a
    surrogate with no pointer stays itself). The converters emit native ids only, so the surrogate is computed here."""
    unique = {str(v) for v in ids if v is not None and not pd.isna(v)}
    out: dict[str, int] = {}
    for sid in unique:
        surrogate = key_fn(provider, native_fn(sid))
        if surrogate is None:
            continue
        key = int(surrogate)
        out[sid] = canonical.get(key, key)
    return out


def _canonical_keys(native: Any, canonical: Mapping[Any, int], entity: str, canonical_match_key: int) -> pa.Int64Array:
    """The canonical int64 key of each native id, built as Python ints, never through a float column.

    `Series.map(dict)` returns float64 when a native id is null (a gradientsports null-actor action), and float64
    rounds a 64-bit key: the 2026-11-08 Mac lake had every GS action key rounded, so no action matched a frame or a
    dim row. A null native id stays a null key. A non-null native id with no canonical key raises (owner 2026-11-08:
    an orphan key fails the match, it is never written)."""
    keys: list[int | None] = []
    missing: set[Any] = set()
    for value in native:
        if value is None or pd.isna(value):
            keys.append(None)
            continue
        key = canonical.get(value)
        if key is None:
            missing.add(value)
        keys.append(key)
    if missing:
        raise ValueError(
            f"no canonical {entity} key for native id(s) {sorted(missing, key=str)[:10]} in match {canonical_match_key}"
        )
    return pa.array(keys, pa.int64())


def _canonical_table(
    actions: pd.DataFrame, canonical_match_key: int, players: Mapping[Any, int], teams: Mapping[Any, int]
) -> pa.Table:
    """The provider-native SPADL actions reshaped into the canonical-keyed `SPADL_ACTIONS_SCHEMA`."""
    acts: Any = actions
    n = len(acts)
    return pa.table(
        {
            "canonical_match_key": pa.array([canonical_match_key] * n, pa.int64()),
            "period_id": pa.array(acts["period_id"], pa.int32()),
            "action_id": pa.array(acts["action_id"], pa.int32()),
            "original_event_id": pa.array(acts["original_event_id"].astype("string"), pa.string()),
            "canonical_player_key": _canonical_keys(acts["player_id"], players, "player", canonical_match_key),
            "canonical_team_key": _canonical_keys(acts["team_id"], teams, "team", canonical_match_key),
            "time_seconds": pa.array(acts["time_seconds"], pa.float64()),
            "start_x": pa.array(acts["start_x"], pa.float64()),
            "start_y": pa.array(acts["start_y"], pa.float64()),
            "end_x": pa.array(acts["end_x"], pa.float64()),
            "end_y": pa.array(acts["end_y"], pa.float64()),
            "type_id": pa.array(acts["type_id"], pa.int32()),
            "result_id": pa.array(acts["result_id"], pa.int32()),
            "bodypart_id": pa.array(acts["bodypart_id"], pa.int32()),
        },
        schema=SPADL_ACTIONS_SCHEMA,
    )


def _canonicalize_gradientsports(
    actions: pd.DataFrame, events: pd.DataFrame, pointers: Pointers, match_key: int
) -> pa.Table:
    """Reshape GS-native SPADL actions into the canonical-keyed `SPADL_ACTIONS_SCHEMA` (match + player + team)."""
    players = _native_to_canonical(events, "game_events_player_id", "player_key", pointers.player)
    teams = _native_to_canonical(events, "game_events_team_id", "team_key", pointers.team)
    return _canonical_table(actions, pointers.match_key(match_key), players, teams)


def _canonicalize_statsbomb(
    actions: pd.DataFrame, events: pd.DataFrame, pointers: Pointers, match_key: int
) -> pa.Table:
    """Reshape statsbomb-native SPADL actions into the canonical-keyed `SPADL_ACTIONS_SCHEMA` (match + player + team).
    statsbomb ids are numeric and the event bronze carries `player_key`/`team_key` surrogate columns, so the
    native->canonical map goes through `_native_to_canonical` (the gradientsports path), NOT `_string_canonical_map`
    (whose `str(id)` would raise in `_positive_int`)."""
    players = _native_to_canonical(events, "player_id", "player_key", pointers.player)
    teams = _native_to_canonical(events, "team_id", "team_key", pointers.team)
    return _canonical_table(actions, pointers.match_key(match_key), players, teams)


def _canonicalize_wyscout(actions: pd.DataFrame, events: pd.DataFrame, pointers: Pointers, match_key: int) -> pa.Table:
    """Reshape wyscout-native SPADL actions into the canonical-keyed `SPADL_ACTIONS_SCHEMA` (match + player + team).
    wyscout ids are numeric and the event bronze carries `player_key`/`team_key` surrogate columns, so native->canonical
    goes through `_native_to_canonical` (the gradientsports path), NOT `_string_canonical_map`."""
    players = _native_to_canonical(events, "player_id", "player_key", pointers.player)
    teams = _native_to_canonical(events, "team_id", "team_key", pointers.team)
    return _canonical_table(actions, pointers.match_key(match_key), players, teams)


def _canonicalize_idsse(actions: pd.DataFrame, pointers: Pointers, match_key: int) -> pa.Table:
    """Reshape idsse DFL-native SPADL actions into the canonical-keyed `SPADL_ACTIONS_SCHEMA` (match/player/team)."""
    acts: Any = actions
    players = _string_canonical_map("idsse", acts["player_id"], idsse_native_player_id, player_key, pointers.player)
    teams = _string_canonical_map("idsse", acts["team_id"], idsse_native_team_id, team_key, pointers.team)
    return _canonical_table(actions, pointers.match_key(match_key), players, teams)


def _canonicalize_skillcorner(actions: pd.DataFrame, pointers: Pointers, match_key: int) -> pa.Table:
    """Reshape skillcorner-native SPADL actions into the canonical-keyed `SPADL_ACTIONS_SCHEMA` (match/player/team)."""
    acts: Any = actions
    players = _string_canonical_map(
        "skillcorner", acts["player_id"], skillcorner_native_player_id, player_key, pointers.player
    )
    teams = _string_canonical_map("skillcorner", acts["team_id"], skillcorner_native_team_id, team_key, pointers.team)
    return _canonical_table(actions, pointers.match_key(match_key), players, teams)


# The statsbomb tables that carry a match's home team, a provider fact on each tier: the owner (pining) tier's
# per-match `metadata` artifact, and the public open-data `matches` listing. A tier writes only its own table.
_STATSBOMB_HOME_TEAM_SOURCES: Final = ("statsbomb_match_metadata", "statsbomb_matches")


def _statsbomb_home_team_id(sources: Mapping[str, Sequence[tuple[Any, ...]] | None], match_key: int) -> int:
    """The match's statsbomb `home_team_id` from the rows of every source table (`None`: the lake has no such table).
    Strict: two different non-null values (the sources disagree) or no value at all raise `ValueError`; nothing is
    defaulted."""
    found: dict[str, set[int]] = {}
    for table in _STATSBOMB_HOME_TEAM_SOURCES:
        values = {int(row[0]) for row in sources.get(table) or () if row[0] is not None}
        if values:
            found[table] = values
    values = set[int]().union(*found.values())
    if not values:
        raise ValueError(
            f"no statsbomb home_team_id for match {match_key} in {' or '.join(_STATSBOMB_HOME_TEAM_SOURCES)}"
        )
    if len(values) > 1:
        raise ValueError(f"statsbomb home_team_id sources disagree for match {match_key}: {found}")
    return values.pop()


# skillcorner dynamic-events coordinate columns the converter reads in centred METRES; the v2 bronze stores the exact
# `*_mm` variant, so the adapter adds the metre column from `<base>_mm / 1000`.
_SKILLCORNER_MM_COORDS: tuple[str, ...] = (
    "x_start",
    "y_start",
    "x_end",
    "y_end",
    "player_targeted_x_reception",
    "player_targeted_y_reception",
)


def _skillcorner_spadl_input(events: pd.DataFrame) -> pd.DataFrame:
    """Shape the v2 `skillcorner_dynamic_events` bronze into the `silly_kicks.spadl.skillcorner` converter input.

    The converter consumes the raw dynamic-events columns (`event_type`, `player_id`, `team_id`, `period`, `time_start`
    the continuous broadcast clock it parses to period-relative seconds, and the pass/shot qualifier columns) plus
    centred-metre coordinates. The v2 bronze keeps those raw columns as-is and stores coords as exact `*_mm`, so the
    adapter adds the metre coord columns and passes the rest through (the converter selects what it needs; spec D9)."""
    out: Any = events.copy()
    for base in _SKILLCORNER_MM_COORDS:
        mm = f"{base}_mm"
        if mm in out.columns:
            out[base] = (out[mm] / 1000.0).astype("float64")
    if "time_start_ms" in out.columns:
        # The converter parses `time_start` as a broadcast-clock STRING (`.str.split(":")`); bronze stores
        # `time_start_ms` as int64 milliseconds on every write path (ADR-018), so format it as "M:SS.sss". A null (a
        # non-possession row the converter ignores) stays None.
        secs = out["time_start_ms"].astype("float64") / 1000.0
        minutes = secs // 60
        rem = secs - minutes * 60
        out["time_start"] = [f"{int(m)}:{r:.3f}" if pd.notna(m) else None for m, r in zip(minutes, rem, strict=True)]
    return out


def _skillcorner_actions_df(events: pd.DataFrame, match_metadata: dict[str, Any]) -> pd.DataFrame:
    """The validated SPADL actions (skillcorner-native ids) for one match (PURE). The skillcorner converter already
    carries `original_event_id` in its canonical schema, so `preserve_native` is left off (passing it raises on the
    overlapping column)."""
    spadl_input = _skillcorner_spadl_input(events)
    actions, _report = _sk_spadl.convert_to_actions(spadl_input, match_metadata)
    _validate_spadl_native_ids(actions)
    return actions


def skillcorner_spadl_actions(events: pd.DataFrame, match_metadata: dict[str, Any]) -> pa.Table:
    """Convert a skillcorner match's dynamic-events bronze to validated SPADL actions (PURE; no catalog)."""
    return pa.Table.from_pandas(_skillcorner_actions_df(events, match_metadata), preserve_index=False)


# The statsbomb converter's `_flatten_extra` reads these nested `extra` paths; v2 flattened each into a typed column
# (parsers/statsbomb.py `_TYPE_OBJECTS`). The adapter rebuilds exactly these paths so `convert_to_actions` dispatches
# every mapped event type — a missing path silently drops that type's qualifier. Dotted path -> v2 flat column.
_STATSBOMB_EXTRA_PATHS: dict[str, str] = {
    "pass.type.name": "pass_type_name",
    "pass.height.name": "pass_height_name",
    "pass.cross": "pass_cross",
    "pass.outcome.name": "pass_outcome_name",
    "pass.body_part.name": "pass_body_part_name",
    "pass.end_location": "pass_end_location",
    "shot.type.name": "shot_type_name",
    "shot.outcome.name": "shot_outcome_name",
    "shot.body_part.name": "shot_body_part_name",
    "shot.end_location": "shot_end_location",
    "dribble.outcome.name": "dribble_outcome_name",
    "goalkeeper.type.name": "goalkeeper_type_name",
    "goalkeeper.outcome.name": "goalkeeper_outcome_name",
    "goalkeeper.body_part.name": "goalkeeper_body_part_name",
    "foul_committed.card.name": "foul_committed_card_name",
    "duel.type.name": "duel_type_name",
    "duel.outcome.name": "duel_outcome_name",
    "interception.outcome.name": "interception_outcome_name",
    "clearance.body_part.name": "clearance_body_part_name",
    "carry.end_location": "carry_end_location",
    "related_events": "related_events",
    "block.offensive": "block_offensive",
}


def _is_na_scalar(value: Any) -> bool:
    """True for a scalar null (NaN / None / pd.NA); False for a list/array (a coordinate pair is never a null leaf)."""
    return value is None or (np.isscalar(value) and bool(pd.isna(value)))


def _set_nested(target: dict[str, Any], keys: list[str], value: Any) -> None:
    node = target
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def _statsbomb_extra(events: pd.DataFrame) -> list[dict[str, Any]]:
    """Rebuild the converter's nested `extra` dict per row from v2's flat statsbomb columns (reverse of the parser's
    `_TYPE_OBJECTS` flattening). An absent flat column, or a scalar null, becomes a null leaf."""
    columns = {
        column: (events[column].tolist() if column in events.columns else [None] * len(events))
        for column in set(_STATSBOMB_EXTRA_PATHS.values())
    }
    rebuilt: list[dict[str, Any]] = []
    for i in range(len(events)):
        extra: dict[str, Any] = {}
        for dotted, column in _STATSBOMB_EXTRA_PATHS.items():
            value = columns[column][i]
            _set_nested(extra, dotted.split("."), None if _is_na_scalar(value) else value)
        rebuilt.append(extra)
    return rebuilt


def _statsbomb_spadl_input(events: pd.DataFrame) -> pd.DataFrame:
    """Shape v2 `statsbomb_events` bronze into the `silly_kicks.spadl.statsbomb` converter input (spec D9 amendment).

    The converter reads a single nested `extra` dict (pass/shot/dribble/goalkeeper/foul/duel/interception/clearance/
    carry/related_events/block) that v2 flattened into typed columns — the adapter rebuilds it (`_statsbomb_extra`).
    `timestamp` is built from bronze `timestamp_ms` (int64 milliseconds, ADR-018) as a Timedelta with an EXPLICIT unit
    (the converter does `pd.to_timedelta(timestamp).dt.total_seconds()`, and an int64 without `unit=` reads as
    nanoseconds — a silent 10^6 error). GK actions are
    marked natively by the `Goal Keeper` type (the converter's `goalkeeper_ids` is a documented no-op for StatsBomb)."""
    out: Any = events
    return pd.DataFrame(
        {
            "game_id": out["match_key"],
            "event_id": out["event_id"].astype("string"),
            "period_id": out["period"],
            "timestamp": pd.to_timedelta(out["timestamp_ms"], unit="ms"),
            "team_id": out["team_id"],
            "player_id": out["player_id"],
            "type_name": out["type_name"].astype("string"),
            "location": out["location"],
            "extra": pd.Series(_statsbomb_extra(events), index=events.index, dtype="object"),
        }
    )


def _statsbomb_actions_df(events: pd.DataFrame, home_team_id: object) -> pd.DataFrame:
    """The validated SPADL actions (statsbomb-native ids) for one match (PURE). The converter carries
    `original_event_id`/`game_id`/`period_id` in its output, so `preserve_native` is left off."""
    spadl_input = _statsbomb_spadl_input(events)
    actions, _report = _sb_spadl.convert_to_actions(spadl_input, cast("Any", home_team_id))
    _validate_spadl_native_ids(actions)
    return actions


def statsbomb_spadl_actions(events: pd.DataFrame, home_team_id: object) -> pa.Table:
    """Convert a statsbomb match's flattened event bronze to validated SPADL actions (PURE; no catalog)."""
    return pa.Table.from_pandas(_statsbomb_actions_df(events, home_team_id), preserve_index=False)


#: Wyscout `matchPeriod` codes -> SPADL integer period_id (1H/2H regulation, E1/E2 extra time, P penalties).
_WS_PERIOD_IDS: dict[str, int] = {"1H": 1, "2H": 2, "E1": 3, "E2": 4, "P": 5}


def _ws_dict_list(cell: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    """Normalise a DuckLake `list<struct>` cell (positions / tags) to a Python list of PLAIN dicts with int values. The
    encrypted-lake `.df()` read can surface the list as a numpy array of dict-likes; the wyscout converter checks
    `isinstance(p, list)` and `isinstance(p[0], dict)`, so a numpy array would be silently treated as empty (the
    skillcorner-timedelta class of trap)."""
    if cell is None:
        return []
    return [{key: (None if item[key] is None else int(item[key])) for key in keys} for item in cell]


def _wyscout_spadl_input(events: pd.DataFrame) -> pd.DataFrame:
    """Shape v2 `wyscout_events` bronze into the `silly_kicks.spadl.wyscout` converter input (spec D9 amendment).

    v2 keeps `positions` (`list<struct(x,y)>`) and `tags` (`list<struct(id)>`) NESTED, so they pass through —
    normalised to Python lists of plain dicts (`_ws_dict_list`). Column remapping: v2 `id` is the unique event id
    (-> `event_id` -> `original_event_id`); v2 `event_id` is the Wyscout TYPE id (-> `type_id`); `sub_event_id` is text
    ("" for no subtype -> `subtype_id` 0, matching the converter's take-on sentinel); `match_period` -> integer
    `period_id`; `event_sec` seconds -> `milliseconds`."""
    out: Any = events
    # pandas' to_numeric stub types a Series input's result as a scalar union; pin it so `.fillna` type-checks.
    subtype: Any = pd.to_numeric(out["sub_event_id"], errors="coerce")
    return pd.DataFrame(
        {
            "game_id": out["match_id"],
            "event_id": out["id"],
            "period_id": out["match_period"].map(_WS_PERIOD_IDS).astype("int64"),
            "milliseconds": (out["event_sec"] * 1000.0).astype("float64"),
            "team_id": out["team_id"],
            "player_id": out["player_id"],
            "type_id": out["event_id"].astype("int64"),
            "subtype_id": subtype.fillna(0).astype("int64"),
            "positions": pd.Series(
                [_ws_dict_list(cell, ("x", "y")) for cell in out["positions"]], index=events.index, dtype="object"
            ),
            "tags": pd.Series(
                [_ws_dict_list(cell, ("id",)) for cell in out["tags"]], index=events.index, dtype="object"
            ),
        }
    )


def _wyscout_actions_df(
    events: pd.DataFrame, home_team_id: object, goalkeeper_ids: set[int] | None = None
) -> pd.DataFrame:
    """The validated SPADL actions (wyscout-native ids) for one match (PURE). The converter carries
    `original_event_id`/`game_id`/`period_id` in its output. `goalkeeper_ids` only reclassifies aerial duels by known
    keepers to `keeper_claim`; None this cycle (wyscout is events-only, no roster-GK wiring — spec D9 note)."""
    spadl_input = _wyscout_spadl_input(events)
    actions, _report = _ws_spadl.convert_to_actions(spadl_input, cast("Any", home_team_id), goalkeeper_ids)
    _validate_spadl_native_ids(actions)
    return actions


def wyscout_spadl_actions(
    events: pd.DataFrame, home_team_id: object, goalkeeper_ids: set[int] | None = None
) -> pa.Table:
    """Convert a wyscout match's flattened event bronze to validated SPADL actions (PURE; no catalog)."""
    return pa.Table.from_pandas(_wyscout_actions_df(events, home_team_id, goalkeeper_ids), preserve_index=False)


# --- the SPADL steps: declared reads, an exact skip key, one driver -------------------------------------------------


@dataclass(frozen=True)
class SpadlRead(InputRead):
    """One declared per-match read of a SPADL step (one `?` parameter: the provider surrogate `match_key`).

    `fetch` is how the driver hands the rows to the converter: `df` a DataFrame, `one` the first row tuple (or None),
    `all` the row tuples. `optional_table` names a table a lake may not have (a tier that never wrote it): when it is
    absent, the read is not run, its key part is `absent`, and the converter gets `None`."""

    fetch: Literal["df", "one", "all"] = "df"
    optional_table: str | None = None


def _read(name: str, sql: str, *, fetch: Literal["df", "one", "all"] = "df", optional: str | None = None) -> SpadlRead:
    return SpadlRead(name, sql, with_provider=False, fetch=fetch, optional_table=optional)


def _match_keys_sql(column: str, table: str) -> str:
    """A SELECT of one surrogate-key column of the match's rows of `table` (over the `k` parameter CTE)."""
    return f"SELECT e.{column} FROM lake.main.{table} AS e, k WHERE e.match_key = k.mk"  # noqa: S608 - constant names


def _pointer_read(*, player_keys: str, team_keys: str) -> SpadlRead:
    """The canonical pointers of the match itself and of its own player and team surrogate keys (`Pointers.of`)."""
    return _read(
        "pointers",
        "WITH k AS (SELECT CAST(? AS BIGINT) AS mk) "  # noqa: S608 - the subqueries are this module's constant SQL
        "SELECT p.entity, p.record_key, p.canonical_key FROM lake.main.ops_canonical_pointers AS p "
        "WHERE (p.entity = 'match' AND p.record_key = (SELECT mk FROM k)) "
        f"OR (p.entity = 'player' AND p.record_key IN ({player_keys})) "
        f"OR (p.entity = 'team' AND p.record_key IN ({team_keys}))",
    )


def _event_pointer_read(events_table: str) -> SpadlRead:
    """The pointer read of a provider whose event bronze carries `player_key`/`team_key` surrogate columns."""
    return _pointer_read(
        player_keys=_match_keys_sql("player_key", events_table), team_keys=_match_keys_sql("team_key", events_table)
    )


#: Inputs of one match, by read name (see `SpadlRead.fetch`).
SpadlInputs = Mapping[str, Any]


def _frame(inputs: SpadlInputs, name: str) -> pd.DataFrame:
    frame: Any = inputs[name]
    return cast("pd.DataFrame", frame)


def _convert_gradientsports(inputs: SpadlInputs, match_key: int) -> pa.Table | None:
    events = _frame(inputs, "gradientsports_events")
    mrow = inputs["gradientsports_matches"]
    if mrow is None:
        raise ValueError(f"no gradientsports_matches row for match {match_key}")
    extratime = None if mrow[2] is None else bool(mrow[2])
    actions = _gradientsports_actions_df(
        events,
        _frame(inputs, "gradientsports_event_objects"),
        home_team_id=mrow[0],
        home_team_start_left=bool(mrow[1]),
        home_team_start_left_extratime=extratime,
    )
    if len(actions) == 0:
        return None
    return _canonicalize_gradientsports(actions, events, Pointers.of(_frame(inputs, "pointers")), match_key)


def _convert_statsbomb(inputs: SpadlInputs, match_key: int) -> pa.Table | None:
    events = _frame(inputs, "statsbomb_events")
    sources = {table: inputs[table] for table in _STATSBOMB_HOME_TEAM_SOURCES}
    actions = _statsbomb_actions_df(events, home_team_id=_statsbomb_home_team_id(sources, match_key))
    if len(actions) == 0:
        return None
    return _canonicalize_statsbomb(actions, events, Pointers.of(_frame(inputs, "pointers")), match_key)


def _convert_wyscout(inputs: SpadlInputs, match_key: int) -> pa.Table | None:
    events = _frame(inputs, "wyscout_events")
    mrow = inputs["wyscout_match_teams"]
    if mrow is None or mrow[0] is None:
        raise ValueError(f"no wyscout_match_teams home side for match {match_key}")
    actions = _wyscout_actions_df(events, home_team_id=int(mrow[0]))
    if len(actions) == 0:
        return None
    return _canonicalize_wyscout(actions, events, Pointers.of(_frame(inputs, "pointers")), match_key)


def _convert_idsse(inputs: SpadlInputs, match_key: int) -> pa.Table | None:
    events = _frame(inputs, "idsse_events")
    period_bounds = [(int(p), int(f0), int(f1)) for p, f0, f1 in inputs["idsse_period_bounds"]]
    mrow = inputs["idsse_match_information"]
    if mrow is None or mrow[0] is None:
        raise ValueError(f"no idsse_match_information.general_home_team_id for match {match_key}")
    prow = inputs["fct_match_params"]
    if prow is None or prow[0] is None:
        raise ValueError(f"no fct_match_params.home_team_start_left for idsse match {match_key}")
    if prow[2] is None:
        raise ValueError(f"no fct_match_params.frame_rate for idsse match {match_key}")
    actions = _idsse_actions_df(
        events,
        period_bounds,
        float(prow[2]),
        home_team_id=str(mrow[0]),
        home_team_start_left=bool(prow[0]),
        home_team_start_left_extratime=None if prow[1] is None else bool(prow[1]),
    )
    if len(actions) == 0:
        return None
    return _canonicalize_idsse(actions, Pointers.of(_frame(inputs, "pointers")), match_key)


def _convert_skillcorner(inputs: SpadlInputs, match_key: int) -> pa.Table | None:
    events = _frame(inputs, "skillcorner_dynamic_events")
    mrow = inputs["skillcorner_matches"]
    if mrow is None or mrow[0] is None or mrow[1] is None or mrow[2] is None:
        raise ValueError(f"no skillcorner_matches pitch/home-team metadata for match {match_key}")
    metadata: dict[str, Any] = {
        "pitch_length": float(mrow[0]),
        "pitch_width": float(mrow[1]),
        "home_team": {"id": str(mrow[2])},
    }
    actions = _skillcorner_actions_df(events, metadata)
    if len(actions) == 0:
        return None
    return _canonicalize_skillcorner(actions, Pointers.of(_frame(inputs, "pointers")), match_key)


def _table_present(conn: Any, table: str) -> bool:
    row = conn.execute(ik.TABLE_PRESENT_SQL, [table]).fetchone()
    return bool(row and row[0])


def _fetch(conn: Any, read: SpadlRead, match_key: int) -> Any:
    result = conn.execute(read.sql, [match_key])
    if read.fetch == "one":
        return result.fetchone()
    if read.fetch == "all":
        return result.fetchall()
    return result.df()


@dataclass(frozen=True)
class SpadlStep:
    """A `spadl_actions:<provider>` layer-1 ComputeStep (spec D9): the provider's events -> canonical SPADL actions.

    Per match, the step computes its exact input key BEFORE it reads any rows into Python (perf spec §7.4, the
    tracking-step key): the in-DuckDB hash of each declared read in `reads`, the code digest of `SPADL_CODE_MODULES`
    and the library versions. A match whose `spadl_actions` rows were written from that key is skipped (`DONE`).
    Otherwise the driver runs the same reads and hands their rows to `convert`; `convert` gets nothing else from the
    catalog, so the key covers every input by construction. The key is the rows' `_source_hash`."""

    name: str
    events_table: str
    reads: tuple[SpadlRead, ...]
    convert: Callable[[SpadlInputs, int], pa.Table | None]
    output_table: str = "spadl_actions"
    layer: int = 1
    input_select: str = "+fct_actions"

    def __call__(
        self,
        cat: WorkingCatalog,
        *,
        run_id: str,
        caps: ResourceCaps,
        now: Callable[[], datetime],
        error: ErrorSink,
        control: ComputeControl = UNBOUNDED,
    ) -> ComputeOutcome:
        conn = cat.conn
        # A run that did not ingest the provider has no event bronze: the step visits no match (registry contract).
        matches = (
            conn.execute(ik.spadl_discovery_sql(self.events_table)).fetchall()
            if _table_present(conn, self.events_table)
            else []
        )
        static = ik.static_part(ik.SPADL_CODE_MODULES)
        provider = self.name.split(":", 1)[1]

        def produce(row: Any, match_key: int) -> MatchBronze | SkipReason:
            present = {r.name: r.optional_table is None or _table_present(conn, r.optional_table) for r in self.reads}
            hashes = [
                (r.name, ik.read_content_hash(conn, r, match_key, provider) if present[r.name] else "absent")
                for r in self.reads
            ]
            key = ik.combine_key(hashes, static)
            if (
                _table_present(conn, self.output_table)
                and conn.execute(ik.SPADL_SKIP_SQL, [match_key, key]).fetchone() is not None
            ):
                return SkipReason.DONE
            inputs = {r.name: _fetch(conn, r, match_key) if present[r.name] else None for r in self.reads}
            arrow = self.convert(inputs, match_key)
            if arrow is None or arrow.num_rows == 0:
                return SkipReason.EMPTY
            return MatchBronze(data=arrow, source_hash=key)

        return run_per_match_bronze(
            cat,
            step=self.name,
            output_table=self.output_table,
            schema=SPADL_ACTIONS_SCHEMA,
            matches=matches,
            match_key_of=lambda r: int(r[0]),
            produce=produce,
            run_id=run_id,
            now=now,
            error=error,
            control=control,
        )


_GS_EVENTS: Final = "gradientsports_events"
_SB_EVENTS: Final = "statsbomb_events"
_WS_EVENTS: Final = "wyscout_events"
_IDSSE_EVENTS: Final = "idsse_events"
_SK_EVENTS: Final = "skillcorner_dynamic_events"

#: The gradientsports SPADL step: events + ball objects + the match's orientation.
SPADL_ACTIONS_GRADIENTSPORTS = SpadlStep(
    name="spadl_actions:gradientsports",
    events_table=_GS_EVENTS,
    reads=(
        _read(_GS_EVENTS, "SELECT * FROM lake.main.gradientsports_events WHERE match_key = ?"),
        _read(
            "gradientsports_event_objects", "SELECT * FROM lake.main.gradientsports_event_objects WHERE match_key = ?"
        ),
        _read(
            "gradientsports_matches",
            "SELECT home_team_id, home_team_start_left, home_team_start_left_extra_time "
            "FROM lake.main.gradientsports_matches WHERE match_key = ?",
            fetch="one",
        ),
        _event_pointer_read(_GS_EVENTS),
    ),
    convert=_convert_gradientsports,
)

#: The statsbomb SPADL step (spec D9 amendment, 2026-11-05: SPADL for every ingested provider). statsbomb is
#: events-only in v2 (no tracking), so it contributes actions to `fct_actions` but no frames for `line_breaking`.
SPADL_ACTIONS_STATSBOMB = SpadlStep(
    name="spadl_actions:statsbomb",
    events_table=_SB_EVENTS,
    reads=(
        _read(_SB_EVENTS, "SELECT * FROM lake.main.statsbomb_events WHERE match_key = ?"),
        *(
            _read(
                table,
                f"SELECT DISTINCT home_team_id FROM lake.main.{table} "  # noqa: S608 - constant table names
                "WHERE match_key = ? AND home_team_id IS NOT NULL",
                fetch="all",
                optional=table,
            )
            for table in _STATSBOMB_HOME_TEAM_SOURCES
        ),
        _event_pointer_read(_SB_EVENTS),
    ),
    convert=_convert_statsbomb,
)

#: The wyscout SPADL step (spec D9 amendment, 2026-11-05). wyscout is events-only in v2 (no tracking).
SPADL_ACTIONS_WYSCOUT = SpadlStep(
    name="spadl_actions:wyscout",
    events_table=_WS_EVENTS,
    reads=(
        _read(_WS_EVENTS, "SELECT * FROM lake.main.wyscout_events WHERE match_key = ?"),
        _read(
            "wyscout_match_teams",
            "SELECT team_id FROM lake.main.wyscout_match_teams WHERE match_key = ? AND side = 'home'",
            fetch="one",
        ),
        _event_pointer_read(_WS_EVENTS),
    ),
    convert=_convert_wyscout,
)

#: The idsse SPADL step (spec D9). idsse carries no `period`/`timestamp_seconds` on the event bronze and no
#: surrogate player/team key: the step reads each period's frame range off `idsse_tracking_frames` (to place an
#: event's `calculated_frame`), the DFL home-team id off `idsse_match_information`, and the derived orientation +
#: frame_rate off `fct_match_params` (R9). Its player/team pointers come from the match's persons and teams.
SPADL_ACTIONS_IDSSE = SpadlStep(
    name="spadl_actions:idsse",
    events_table=_IDSSE_EVENTS,
    reads=(
        _read(_IDSSE_EVENTS, "SELECT * FROM lake.main.idsse_events WHERE match_key = ?"),
        _read(
            "idsse_period_bounds",
            "SELECT period, min(frame), max(frame) FROM lake.main.idsse_tracking_frames "
            "WHERE match_key = ? GROUP BY period ORDER BY period",
            fetch="all",
        ),
        _read(
            "idsse_match_information",
            "SELECT general_home_team_id FROM lake.main.idsse_match_information WHERE match_key = ?",
            fetch="one",
        ),
        _read(
            "fct_match_params",
            "WITH k AS (SELECT CAST(? AS BIGINT) AS mk) "
            "SELECT m.home_team_start_left, m.home_team_start_left_extra_time, m.frame_rate "
            "FROM lake.main.fct_match_params AS m, k WHERE m.canonical_match_key = coalesce("
            "(SELECT p.canonical_key FROM lake.main.ops_canonical_pointers AS p "
            "WHERE p.entity = 'match' AND p.record_key = k.mk), k.mk)",
            fetch="one",
        ),
        _pointer_read(
            player_keys=_match_keys_sql("player_key", "idsse_persons"),
            team_keys=_match_keys_sql("team_key", "idsse_teams"),
        ),
    ),
    convert=_convert_idsse,
)

#: The skillcorner SPADL step (spec D9). The converter is inference-based: it reads the dynamic-events bronze largely
#: as-is plus the match's pitch size + home-team id (off `skillcorner_matches`).
SPADL_ACTIONS_SKILLCORNER = SpadlStep(
    name="spadl_actions:skillcorner",
    events_table=_SK_EVENTS,
    reads=(
        _read(_SK_EVENTS, "SELECT * FROM lake.main.skillcorner_dynamic_events WHERE match_key = ?"),
        _read(
            "skillcorner_matches",
            "SELECT pitch_length, pitch_width, home_team_id FROM lake.main.skillcorner_matches WHERE match_key = ?",
            fetch="one",
        ),
        _event_pointer_read(_SK_EVENTS),
    ),
    convert=_convert_skillcorner,
)
