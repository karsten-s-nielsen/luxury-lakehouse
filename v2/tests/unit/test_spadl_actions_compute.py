"""SPADL actions — the gradientsports events -> SPADL adapter (spec D9; plan Task 4b).

The adapter's key correctness property is COMPLETENESS: it must produce exactly the converter's
`EXPECTED_INPUT_COLUMNS` (a missing/misnamed source column makes `convert_to_actions` raise). This test builds a
column-complete gradientsports_events fixture and asserts the mapping covers the contract, and that
`gradientsports_spadl_actions` runs through `convert_to_actions` + `validate_spadl` without error. Non-empty-action
behaviour on real GS type tokens is exercised by the seeded e2e (Task 11).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# A unit test OF the private SPADL adapter internals, so reportPrivateUsage is relaxed (as test_taipy_shape does).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
# pyright: reportPrivateUsage=false
from __future__ import annotations

import warnings
from typing import Any

import numpy as np
import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pytest
from silly_kicks.spadl.gradientsports import EXPECTED_INPUT_COLUMNS  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl.statsbomb import EXPECTED_INPUT_COLUMNS as _SB_EXPECTED  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.spadl.wyscout import EXPECTED_INPUT_COLUMNS as _WS_EXPECTED  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.compute.registry import STEP_OUTPUTS
from lakehouse.compute.spadl_actions import (
    _GS_DIRECT,
    _GS_POSSESSION_QUALIFIERS,
    Pointers,
    _canonicalize_gradientsports,
    _canonicalize_statsbomb,
    _canonicalize_wyscout,
    _gradientsports_spadl_input,
    _idsse_spadl_input,
    _skillcorner_spadl_input,
    _statsbomb_home_team_id,
    _statsbomb_spadl_input,
    _wyscout_spadl_input,
    gradientsports_spadl_actions,
    idsse_spadl_actions,
    statsbomb_spadl_actions,
    wyscout_spadl_actions,
)
from lakehouse.ids.native import PROVIDERS

HOME, AWAY = 100, 200


def _events() -> pd.DataFrame:
    """One gradientsports_events row carrying every v2 source column the adapter reads (values mostly null)."""
    source_cols = [
        *_GS_DIRECT.values(),
        *(f"possession_events_{q}" for q in _GS_POSSESSION_QUALIFIERS),
        "possession_events_non_event",
        "event_index",
    ]
    row: dict[str, Any] = dict.fromkeys(source_cols)
    row.update(
        {
            "event_index": 0,
            "game_id": 1,
            "game_event_id": 10,
            "possession_event_id": 100,
            "game_events_period": 1,
            "game_events_start_game_clock": 12.0,
            "start_time": 12.0,
            "game_events_team_id": HOME,
            "game_events_player_id": 7,
            "game_events_game_event_type": "OTB",
            "possession_events_possession_event_type": "PA",
        }
    )
    return pd.DataFrame([row])


def _ball_objects() -> pd.DataFrame:
    return pd.DataFrame([{"event_index": 0, "object_kind": "ball", "x_mm": 1_000, "y_mm": -2_000}])


def test_adapter_covers_the_converter_input_contract() -> None:
    shaped = _gradientsports_spadl_input(_events(), _ball_objects())
    assert set(EXPECTED_INPUT_COLUMNS) <= set(shaped.columns)  # every required column is produced
    assert shaped["ball_x"].iloc[0] == 1.0  # x_mm 1000 -> centred metres
    assert shaped["period_id"].iloc[0] == 1


def test_gradientsports_time_is_period_relative() -> None:
    """GS `startGameClock` is a NOMINAL absolute clock (nominal period start + period elapsed); the adapter subtracts
    the nominal start (v1 ADR-040), so GS actions share the per-period base of the GS frames and of every other
    provider's actions. The 2026-11-06 Mac lake held GS period-2 actions at 2700 s+ and line_breaking linked 23 %."""
    events = pd.concat([_events()] * 5, ignore_index=True)
    events["event_index"] = range(5)
    events["game_events_period"] = [1, 2, 3, 4, 5]
    events["game_events_start_game_clock"] = [120.0, 2820.0, 5460.0, 6360.0, 7201.0]
    events["possession_events_game_clock"] = [999.0] * 5  # the possession clock is NOT the time source
    shaped = _gradientsports_spadl_input(events, _ball_objects())
    assert list(shaped["time_seconds"]) == [120.0, 120.0, 60.0, 60.0, 1.0]


def test_gradientsports_unknown_period_with_a_clock_fails() -> None:
    events = _events()
    events["game_events_period"] = [6]
    with pytest.raises(ValueError, match="no nominal start for gradientsports period 6"):
        _gradientsports_spadl_input(events, _ball_objects())


def test_gradientsports_pre_match_period_zero_starts_at_zero() -> None:
    # The real lake (2026-11-08, match 59944652302746553): one period-0 event of type `G`, no team, no player,
    # startGameClock 0, before FIRSTKICKOFF. Period 0 is the pre-match marker, nominal start 0 s (v1: offset 0).
    events = _events()
    events["game_events_period"] = [0]
    events["game_events_game_event_type"] = ["G"]
    events["game_events_start_game_clock"] = [0.0]
    shaped = _gradientsports_spadl_input(events, _ball_objects())
    assert list(shaped["time_seconds"]) == [0.0]


def test_gradientsports_clockless_row_of_an_unknown_period_passes() -> None:
    # A row with no clock has no time to re-base, so an unknown period does not matter for it.
    events = _events()
    events["game_events_period"] = [6]
    events["game_events_start_game_clock"] = [None]
    shaped = _gradientsports_spadl_input(events, _ball_objects())
    assert bool(shaped["time_seconds"].isna().all())


def test_non_event_supplied_so_the_converter_excludes_voided_plays() -> None:
    """The adapter supplies the voided-event flag under the converter's EXACT camelCase `nonEvent` name, so Component-4
    excludes voided possession events (disallowed goals, annulled plays) instead of warning they are NOT excluded. The
    converter checks `"nonEvent" in events.columns` — the snake_case `non_event` was silently ignored (review
    A-IMPL-02)."""
    shaped = _gradientsports_spadl_input(_events(), _ball_objects())
    assert "nonEvent" in shaped.columns  # the EXACT name Component-4 checks, not the snake_case non_event
    # The DuckDB-read column is NULLABLE boolean (pd.NA for a live event), not Python None; the converter's bool
    # coercion raises "boolean value of NA is ambiguous" on pd.NA, so the adapter must normalise it. The e2e caught
    # this where the plain-None fixture did not.
    events = _events()
    events["possession_events_non_event"] = events["possession_events_non_event"].astype("boolean")  # None -> pd.NA
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gradientsports_spadl_actions(events, _ball_objects(), home_team_id=HOME, home_team_start_left=True)
    assert not any("nonEvent" in str(w.message) and "not supplied" in str(w.message) for w in caught)  # Component-4 on


def test_gradientsports_spadl_actions_runs_and_validates() -> None:
    # The adapter -> convert_to_actions -> validate_spadl plumbing holds (validate raises on a bad SPADL frame).
    table = gradientsports_spadl_actions(_events(), _ball_objects(), home_team_id=HOME, home_team_start_left=True)
    cols = set(table.column_names)
    assert {"game_id", "period_id", "time_seconds", "type_id", "start_x", "start_y"} <= cols


def _pointers() -> Pointers:
    """The match's pointers as the step's `pointers` read returns them: the match/player/team records resolve to
    DIVERGENT canonical keys (record_key != canonical_key), so a pass-through of the raw key is caught."""
    frame = pd.DataFrame(
        {"entity": ["match", "player", "team"], "record_key": [5, 50, 60], "canonical_key": [999, 7, 100]}
    )
    return Pointers.of(frame)


def _one_action() -> pd.DataFrame:
    """One GS-native SPADL action row (player/team are the native ids the adapter emits)."""
    return pd.DataFrame(
        [
            {
                "period_id": 1,
                "action_id": 0,
                "original_event_id": "e1",
                "player_id": 42,  # native game_events_player_id
                "team_id": 84,  # native game_events_team_id
                "time_seconds": 12.0,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 30.0,
                "type_id": 0,
                "result_id": 1,
                "bodypart_id": 0,
            }
        ]
    )


def _idsse_events() -> pd.DataFrame:
    """Two DFL events — a Play (pass) and a ShotAtGoal — in the flattened v2 `idsse_events` shape (only each type's own
    actor columns are populated; the rest are null, as the XML flattening gives)."""
    base: dict[str, Any] = dict.fromkeys(
        ["play_player", "play_team", "shot_at_goal_player", "shot_at_goal_team", "foul_committing_player"]
    )
    return pd.DataFrame(
        [
            {
                **base,
                "match_id": "DFL-MAT-1",
                "event_id": 1,
                "event_type": "Play",
                "calculated_frame": 10,
                "x_position_mm": 0,
                "y_position_mm": 0,
                "play_player": "DFL-OBJ-1",
                "play_team": "DFL-CLU-A",
            },
            {
                **base,
                "match_id": "DFL-MAT-1",
                "event_id": 2,
                "event_type": "ShotAtGoal",
                "calculated_frame": 20,
                "x_position_mm": 40_000,
                "y_position_mm": 0,
                "shot_at_goal_player": "DFL-OBJ-2",
                "shot_at_goal_team": "DFL-CLU-A",
            },
        ]
    )


# Period 1 spans frames 10..30; the two events (calculated_frame 10, 20) fall inside it. frame_rate 25 Hz.
_IDSSE_PERIOD_BOUNDS = [(1, 10, 30)]
_IDSSE_FRAME_RATE = 25.0


def test_idsse_spadl_input_coalesces_the_per_type_actor() -> None:
    shaped = _idsse_spadl_input(_idsse_events(), _IDSSE_PERIOD_BOUNDS, _IDSSE_FRAME_RATE)
    assert set(shaped.columns) == {
        "match_id",
        "event_id",
        "event_type",
        "period",
        "timestamp_seconds",
        "player_id",
        "team",
        "x",
        "y",
    }
    assert shaped["player_id"].tolist() == ["DFL-OBJ-1", "DFL-OBJ-2"]  # the right per-type actor per event
    assert shaped["team"].tolist() == ["DFL-CLU-A", "DFL-CLU-A"]
    # calculated_frame 10 -> (10-10)/25 = 0.0; calculated_frame 20 -> (20-10)/25 = 0.4; both in period 1
    assert shaped["period"].tolist() == [1, 1] and shaped["timestamp_seconds"].tolist() == [0.0, 0.4]


def test_idsse_event_outside_every_period_range_is_dropped() -> None:
    # An event whose calculated_frame is in no period range is unlocatable (no period for the coord flip) -> dropped.
    events = _idsse_events()
    events.loc[1, "calculated_frame"] = 9999  # outside [10, 30]
    shaped = _idsse_spadl_input(events, _IDSSE_PERIOD_BOUNDS, _IDSSE_FRAME_RATE)
    assert shaped["event_id"].tolist() == [1]  # only the in-range event survives


def test_skillcorner_input_formats_time_start_ms() -> None:
    """Bronze stores `time_start_ms` as int64 milliseconds on every write path (ADR-018, no-duration spec). The adapter
    formats it into the converter's `M:SS.sss` broadcast-clock string (the old duration column surfaced as a Timedelta
    on encrypted lakes and as numeric ms elsewhere — the two-type trap this replaces)."""
    events = pd.DataFrame(
        {
            "event_type": ["pass"],
            "player_id": [1],
            "team_id": [10],
            "period": [1],
            "time_start_ms": [83_500],  # 83.5 s -> "1:23.500"
            "x_start_mm": [10_000],
            "y_start_mm": [20_000],
            "x_end_mm": [30_000],
            "y_end_mm": [40_000],
            "player_targeted_x_reception_mm": [55_000],
            "player_targeted_y_reception_mm": [33_000],
        }
    )
    out = _skillcorner_spadl_input(events)
    assert out["time_start"].iloc[0] == "1:23.500"  # int ms -> M:SS.sss
    assert out["x_start"].iloc[0] == 10.0  # x_start_mm 10000 -> centred metres (the *_mm reshaping still holds)


def test_idsse_events_convert_to_spadl() -> None:
    # The idsse adapter -> sportec.convert_to_actions -> validate_spadl plumbing holds on DFL Play + ShotAtGoal events.
    table = idsse_spadl_actions(
        _idsse_events(), _IDSSE_PERIOD_BOUNDS, _IDSSE_FRAME_RATE, home_team_id="DFL-CLU-A", home_team_start_left=True
    )
    cols = set(table.column_names)
    assert {"game_id", "period_id", "time_seconds", "type_id", "start_x", "start_y"} <= cols
    assert table.num_rows > 0


def test_native_id_validate_suppresses_the_expected_id_dtype_warning() -> None:
    """idsse SPADL actions carry DFL string ids (canonicalised to int64 keys downstream, where line_breaking joins), so
    validate_spadl's int64-id dtype warning is EXPECTED and is suppressed — it must not reach the logs and bury a real
    validate_spadl warning (review A-IMPL-03 / B-CALC-IMPL-03)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        idsse_spadl_actions(
            _idsse_events(),
            _IDSSE_PERIOD_BOUNDS,
            _IDSSE_FRAME_RATE,
            home_team_id="DFL-CLU-A",
            home_team_start_left=True,
        )
    assert not any("expected 'int64'" in str(w.message) for w in caught)  # the known id-dtype warning is suppressed


def test_canonicalize_resolves_the_match_key_not_just_player_and_team() -> None:
    # The bronze `canonical_match_key` must be the RESOLVED canonical key (999), never the raw surrogate match_key (5)
    # — else a GS match that clusters cross-provider mislabels its actions and line_breaking's canonical join misses.
    events = pd.DataFrame([{"game_events_player_id": 42, "player_key": 50, "game_events_team_id": 84, "team_key": 60}])
    arrow = _canonicalize_gradientsports(_one_action(), events, _pointers(), match_key=5)
    row: Any = arrow.to_pylist()[0]
    assert row["canonical_match_key"] == 999  # resolved, not the raw 5
    assert row["canonical_player_key"] == 7
    assert row["canonical_team_key"] == 100


# 64-bit canonical keys above 2**53: a float64 round-trip cannot hold them (the 2026-11-08 Mac lake: every GS action
# key was rounded, e.g. -3933577190176248734 -> -3933577190176248832, so no action key matched a frame or a dim row).
_BIG_PLAYER, _BIG_TEAM = -3933577190176248734, 5121372762300994708


def _big_key_pointers() -> Pointers:
    frame = pd.DataFrame(
        {
            "entity": ["match", "player", "team"],
            "record_key": [5, 50, 60],
            "canonical_key": [999, _BIG_PLAYER, _BIG_TEAM],
        }
    )
    return Pointers.of(frame)


def test_canonical_keys_stay_exact_with_a_null_actor() -> None:
    # A GS null-actor action (no player, no team) next to a normal one: the null must not turn the keys into floats.
    actions = pd.concat(
        [_one_action(), _one_action().assign(action_id=1, player_id=pd.NA, team_id=pd.NA)], ignore_index=True
    )
    events = pd.DataFrame([{"game_events_player_id": 42, "player_key": 50, "game_events_team_id": 84, "team_key": 60}])
    rows: Any = _canonicalize_gradientsports(actions, events, _big_key_pointers(), match_key=5).to_pylist()
    assert (rows[0]["canonical_player_key"], rows[0]["canonical_team_key"]) == (_BIG_PLAYER, _BIG_TEAM)
    assert (rows[1]["canonical_player_key"], rows[1]["canonical_team_key"]) == (None, None)


def test_a_native_id_with_no_canonical_key_fails_the_match() -> None:
    # A non-null native actor that maps to no canonical key would write an orphan (null) key: fail loud instead.
    events = pd.DataFrame([{"game_events_player_id": 41, "player_key": 50, "game_events_team_id": 84, "team_key": 60}])
    with pytest.raises(ValueError, match=r"no canonical player key for native id\(s\) \[42\]"):
        _canonicalize_gradientsports(_one_action(), events, _big_key_pointers(), match_key=5)


# --- statsbomb (spec D9 amendment, 2026-11-05: SPADL for every ingested provider) ---------------------------------
def _statsbomb_events() -> pd.DataFrame:
    """Two statsbomb_events rows (a Pass + a Carry) carrying every v2 flat source column the adapter reads. `timestamp`
    is a pandas Timedelta — the real encrypted-lake `.df()` surfaces the v2 `pa.duration('ms')` column that way (the
    skillcorner-timedelta class of trap), and the converter does `pd.to_timedelta(timestamp).dt.total_seconds()`."""
    base: dict[str, Any] = dict.fromkeys(
        (
            "pass_type_name",
            "pass_height_name",
            "pass_cross",
            "pass_outcome_name",
            "pass_body_part_name",
            "pass_end_location",
            "shot_type_name",
            "shot_outcome_name",
            "shot_body_part_name",
            "shot_end_location",
            "dribble_outcome_name",
            "goalkeeper_type_name",
            "goalkeeper_outcome_name",
            "goalkeeper_body_part_name",
            "foul_committed_card_name",
            "duel_type_name",
            "duel_outcome_name",
            "interception_outcome_name",
            "clearance_body_part_name",
            "carry_end_location",
            "block_offensive",
        )
    )
    rows = [
        {
            **base,
            "match_key": 1,
            "event_id": "e1",
            "period": 1,
            "team_id": HOME,
            "player_id": 7,
            "player_key": 7001,
            "team_key": 7100,
            "type_name": "Pass",
            "location": [60.0, 40.0],
            "related_events": ["e2"],
            "pass_height_name": "Ground Pass",
            "pass_body_part_name": "Right Foot",
            "pass_cross": False,
            "pass_end_location": [80.0, 45.0],
        },
        {
            **base,
            "match_key": 1,
            "event_id": "e2",
            "period": 1,
            "team_id": HOME,
            "player_id": 7,
            "player_key": 7001,
            "team_key": 7100,
            "type_name": "Carry",
            "location": [80.0, 45.0],
            "related_events": ["e1"],
            "carry_end_location": [85.0, 44.0],
        },
    ]
    df = pd.DataFrame(rows)
    df["timestamp_ms"] = [12_000, 13_500]  # bronze int64 milliseconds (ADR-018)
    return df


def test_statsbomb_adapter_covers_the_converter_input_contract() -> None:
    shaped = _statsbomb_spadl_input(_statsbomb_events())
    assert set(_SB_EXPECTED) <= set(shaped.columns)  # every required column is produced
    assert shaped["period_id"].iloc[0] == 1
    # `extra` is rebuilt as the nested dict the converter's _flatten_extra reads, from v2's flat _TYPE_OBJECTS columns.
    extra0: Any = shaped["extra"].iloc[0]
    assert extra0["pass"]["height"]["name"] == "Ground Pass"
    assert extra0["pass"]["body_part"]["name"] == "Right Foot"
    assert extra0["related_events"] == ["e2"]


def test_statsbomb_input_converts_timestamp_ms() -> None:
    # Bronze stores `timestamp_ms` as int64 milliseconds (ADR-018). The adapter hands the converter a Timedelta built
    # with an explicit unit: `pd.to_timedelta(int64)` WITHOUT `unit=` reads nanoseconds (12000 ms -> 1.2e-05 s), the
    # silent 10^6 error the old pass-through had on register-path lakes.
    shaped = _statsbomb_spadl_input(_statsbomb_events())
    timestamp: Any = shaped["timestamp"]
    assert pd.to_timedelta(timestamp).dt.total_seconds().tolist() == [12.0, 13.5]


def test_statsbomb_spadl_actions_runs_and_validates() -> None:
    # The statsbomb adapter -> statsbomb.convert_to_actions -> validate_spadl plumbing holds on a Pass + Carry.
    table = statsbomb_spadl_actions(_statsbomb_events(), home_team_id=HOME)
    cols = set(table.column_names)
    assert {"game_id", "period_id", "time_seconds", "type_id", "start_x", "start_y", "original_event_id"} <= cols
    assert table.num_rows > 0


def _home_team_sources(
    *, metadata: list[int | None] | None, listing: list[int | None] | None
) -> dict[str, list[tuple[int | None]] | None]:
    """The rows of `statsbomb_match_metadata` (the owner tier) and `statsbomb_matches` (the public listing) for match
    1, as the step's declared reads hand them over. `None` is an absent table: a lake whose tier never wrote it."""
    return {
        "statsbomb_match_metadata": None if metadata is None else [(home,) for home in metadata],
        "statsbomb_matches": None if listing is None else [(home,) for home in listing],
    }


def test_statsbomb_home_team_from_the_owner_metadata() -> None:
    assert _statsbomb_home_team_id(_home_team_sources(metadata=[771], listing=None), 1) == 771


def test_statsbomb_home_team_from_the_public_listing() -> None:
    # The public open-data tier has no `metadata` artifact, so `statsbomb_match_metadata` does not exist; the
    # matches listing carries the same provider fact (guard 3 found every public match failing on the missing table).
    assert _statsbomb_home_team_id(_home_team_sources(metadata=None, listing=[771]), 1) == 771


def test_statsbomb_home_team_sources_that_agree() -> None:
    assert _statsbomb_home_team_id(_home_team_sources(metadata=[771], listing=[771, None]), 1) == 771


def test_statsbomb_home_team_sources_that_disagree_fail() -> None:
    with pytest.raises(ValueError, match="disagree"):
        _statsbomb_home_team_id(_home_team_sources(metadata=[771], listing=[772]), 1)


@pytest.mark.parametrize(("metadata", "listing"), [(None, None), ([None], None), (None, [None]), ([None], [None])])
def test_statsbomb_home_team_missing_fails(metadata: list[int | None] | None, listing: list[int | None] | None) -> None:
    with pytest.raises(ValueError, match="no statsbomb home_team_id"):
        _statsbomb_home_team_id(_home_team_sources(metadata=metadata, listing=listing), 1)


def test_canonicalize_statsbomb_resolves_native_ids() -> None:
    # statsbomb native player/team ids resolve to canonical keys via the event bronze's player_key/team_key surrogate
    # columns (the _native_to_canonical path — statsbomb ids are numeric, so _string_canonical_map's str path is wrong).
    events = pd.DataFrame([{"player_id": 42, "player_key": 50, "team_id": 84, "team_key": 60}])
    arrow = _canonicalize_statsbomb(_one_action(), events, _pointers(), match_key=5)
    row: Any = arrow.to_pylist()[0]
    assert row["canonical_match_key"] == 999
    assert row["canonical_player_key"] == 7
    assert row["canonical_team_key"] == 100


# --- wyscout (spec D9 amendment, 2026-11-05: SPADL for every ingested provider) -----------------------------------
def _wyscout_events() -> pd.DataFrame:
    """Two wyscout_events rows (a Pass + a Shot) with v2's event columns. v2 keeps `positions`/`tags` NESTED; here they
    are Python lists of dicts (the shape the converter needs). The v2 `id` is the unique event id, `event_id` is the
    Wyscout TYPE id, `sub_event_id` is text."""
    rows = [
        {
            "id": 111,
            "event_id": 8,
            "event_name": "Pass",
            "sub_event_id": "85",
            "sub_event_name": "Simple pass",
            "event_sec": 1.0,
            "match_id": 1,
            "match_period": "1H",
            "player_id": 7,
            "team_id": HOME,
            "player_key": 7001,
            "team_key": 7100,
            "positions": [{"x": 50, "y": 50}, {"x": 60, "y": 55}],
            "tags": [{"id": 1801}],
        },
        {
            "id": 112,
            "event_id": 10,
            "event_name": "Shot",
            "sub_event_id": "100",
            "sub_event_name": "Shot on target",
            "event_sec": 5.0,
            "match_id": 1,
            "match_period": "1H",
            "player_id": 7,
            "team_id": HOME,
            "player_key": 7001,
            "team_key": 7100,
            "positions": [{"x": 90, "y": 50}, {"x": 100, "y": 50}],
            "tags": [{"id": 101}],
        },
    ]
    return pd.DataFrame(rows)


def test_wyscout_adapter_covers_the_converter_input_contract() -> None:
    shaped = _wyscout_spadl_input(_wyscout_events())
    assert set(_WS_EXPECTED) <= set(shaped.columns)  # every required column is produced
    assert shaped["period_id"].iloc[0] == 1  # "1H" -> 1
    assert shaped["milliseconds"].iloc[0] == 1000.0  # event_sec 1.0 s -> ms
    assert shaped["type_id"].iloc[0] == 8  # v2 `event_id` (the Wyscout TYPE id) -> type_id
    assert shaped["subtype_id"].iloc[0] == 85
    assert shaped["event_id"].iloc[0] == 111  # v2 `id` (unique) -> event_id -> original_event_id


def test_wyscout_positions_and_tags_normalise_to_plain_dict_lists() -> None:
    # The encrypted-lake read can surface a list<struct> as a numpy ARRAY of dict-likes; the converter checks
    # `isinstance(p, list)` and `isinstance(p[0], dict)`, so the adapter MUST hand it a Python list of plain dicts
    # (the skillcorner-timedelta class of trap — a numpy array would be silently treated as no positions).
    events = _wyscout_events()
    events["positions"] = pd.Series(
        [np.array([{"x": 50, "y": 50}], dtype=object), np.array([{"x": 90, "y": 50}], dtype=object)],
        index=events.index,
    )
    shaped = _wyscout_spadl_input(events)
    p0: Any = shaped["positions"].iloc[0]
    assert isinstance(p0, list) and isinstance(p0[0], dict) and p0[0]["x"] == 50


def test_wyscout_missing_subtype_becomes_zero() -> None:
    events = _wyscout_events()
    events.loc[0, "sub_event_id"] = ""  # Wyscout writes "" for an event without a subtype
    shaped = _wyscout_spadl_input(events)
    assert shaped["subtype_id"].iloc[0] == 0


def test_wyscout_spadl_actions_runs_and_validates() -> None:
    # The wyscout adapter -> wyscout.convert_to_actions -> validate_spadl plumbing holds on a Pass + Shot.
    table = wyscout_spadl_actions(_wyscout_events(), home_team_id=HOME)
    cols = set(table.column_names)
    assert {"game_id", "period_id", "time_seconds", "type_id", "start_x", "original_event_id"} <= cols
    assert table.num_rows > 0


def test_canonicalize_wyscout_resolves_native_ids() -> None:
    events = pd.DataFrame([{"player_id": 42, "player_key": 50, "team_id": 84, "team_key": 60}])
    arrow = _canonicalize_wyscout(_one_action(), events, _pointers(), match_key=5)
    row: Any = arrow.to_pylist()[0]
    assert row["canonical_match_key"] == 999
    assert row["canonical_player_key"] == 7
    assert row["canonical_team_key"] == 100


def test_every_ingested_provider_has_a_spadl_step() -> None:
    # Owner scope (spec D9 amendment, 2026-11-05): SPADL runs for EVERY provider v2 ingests — "do not skip any". A new
    # provider must get a SPADL adapter + its DEFAULT_COMPUTE_STEPS entry, or this guard fails (it was RED at 3/5).
    spadl_providers = {name.split(":", 1)[1] for name, _table in STEP_OUTPUTS if name.startswith("spadl_actions:")}
    assert spadl_providers == set(PROVIDERS), spadl_providers.symmetric_difference(PROVIDERS)
