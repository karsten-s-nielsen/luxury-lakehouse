"""Regression tests for `_build_gradientsports_roster_dicts`.

Guards the GS roster column-name contract: `bronze.gradientsports_roster` columns
are dot-notation from `pd.json_normalize` of the GS API payload (`team.id`,
`shirtNumber`, `player.id`, `positionGroupType`) — NOT snake_case. The pre-fix code
read snake_case names, which KeyError / silently empty the GS MatchMeta dicts and
break GS carrier + possession resolution. Verified against the live bronze schema
2026-06-01.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.action_context import _build_gradientsports_roster_dicts

# Roster shaped EXACTLY like bronze.gradientsports_roster (dot-notation columns).
_BRONZE_ROSTER = pd.DataFrame(
    [
        {"team.id": "366", "shirtNumber": "1", "player.id": "1001", "positionGroupType": "GK", "match_id": "10502"},
        {"team.id": "366", "shirtNumber": "10", "player.id": "1010", "positionGroupType": "MID", "match_id": "10502"},
        {"team.id": "51", "shirtNumber": "9", "player.id": "2009", "positionGroupType": "FWD", "match_id": "10502"},
        {"team.id": "51", "shirtNumber": "1", "player.id": "2001", "positionGroupType": "GK", "match_id": "10502"},
    ]
)


def test_builds_nonempty_dicts_from_bronze_dot_notation() -> None:
    """The core regression: with real bronze columns the dicts populate.

    Pre-fix (snake_case reads) this raised KeyError on `roster_pdf["team_id"]`.
    """
    team_side_to_id, jersey_to_player_id, gk_player_ids = _build_gradientsports_roster_dicts(_BRONZE_ROSTER, "366")
    assert team_side_to_id, "team_side_to_id must not be empty"
    assert jersey_to_player_id, "jersey_to_player_id must not be empty"
    assert gk_player_ids, "gk_player_ids must not be empty"


def test_team_side_and_away_derivation() -> None:
    team_side_to_id, _, _ = _build_gradientsports_roster_dicts(_BRONZE_ROSTER, "366")
    assert team_side_to_id == {"home": "366", "away": "51"}


def test_jersey_to_native_player_id() -> None:
    """Resolved values are the native GS `player.id` (string) — the key that
    matches actions' `player_id_native` for identity resolution."""
    _, jersey_to_player_id, _ = _build_gradientsports_roster_dicts(_BRONZE_ROSTER, "366")
    assert jersey_to_player_id[("home", "1")] == "1001"
    assert jersey_to_player_id[("home", "10")] == "1010"
    assert jersey_to_player_id[("away", "9")] == "2009"
    assert jersey_to_player_id[("away", "1")] == "2001"


def test_gk_identified_from_position_group_type() -> None:
    _, _, gk_player_ids = _build_gradientsports_roster_dicts(_BRONZE_ROSTER, "366")
    assert set(gk_player_ids) == {"1001", "2001"}


def test_missing_position_group_type_yields_no_gk() -> None:
    roster = _BRONZE_ROSTER.drop(columns=["positionGroupType"])
    _, _, gk_player_ids = _build_gradientsports_roster_dicts(roster, "366")
    assert gk_player_ids == []


def test_snake_case_columns_raise_keyerror() -> None:
    """Documents the pre-fix bug + guards against re-introducing snake_case reads:
    bronze does NOT have `team_id`/`jersey_number`/`player_id`/`position`."""
    bad = pd.DataFrame(
        [{"team_id": "366", "jersey_number": "1", "player_id": "1001", "position": "GK", "match_id": "10502"}]
    )
    with pytest.raises(KeyError):
        _build_gradientsports_roster_dicts(bad, "366")


# ── Frame-id coercion (the second half of the GS player-id-space fix) ──────────
# convert_to_frames forces GS player_id/team_id to Int64; downstream compares to
# native-string action ids. _coerce_gradientsports_frame_ids_to_native_str realigns.


def test_coerces_int64_frame_ids_to_native_string() -> None:
    from analytics.action_context.convert import _coerce_gradientsports_frame_ids_to_native_str

    frames = pd.DataFrame(
        {
            "player_id": pd.array([3861, 1010, pd.NA], dtype="Int64"),
            "team_id": pd.array([366, 366, pd.NA], dtype="Int64"),
            "is_ball": [False, False, True],
        }
    )
    out = _coerce_gradientsports_frame_ids_to_native_str(frames)

    assert out["player_id"].dtype == object
    assert out["team_id"].dtype == object
    # Values match the native-string action-id space (player_id_native / team_id_native).
    assert out["player_id"].iloc[0] == "3861"
    assert out["player_id"].iloc[1] == "1010"  # no ".0" float artifact
    assert out["team_id"].iloc[0] == "366"
    # Ball-row stays null (not the string "<NA>"); ball rows are excluded downstream.
    assert pd.isna(out["player_id"].iloc[2])
    assert pd.isna(out["team_id"].iloc[2])


def test_coerced_ids_equal_native_string_action_ids() -> None:
    """The whole point: Int64(366) != '366', but the coerced value == '366' (action space)."""
    from analytics.action_context.convert import _coerce_gradientsports_frame_ids_to_native_str

    frames = pd.DataFrame({"player_id": pd.array([3861], dtype="Int64"), "team_id": pd.array([366], dtype="Int64")})
    out = _coerce_gradientsports_frame_ids_to_native_str(frames)
    # Mirrors _resolve_action_frame_context equality: frame id == native action id.
    assert (out["team_id"] == "366").all()
    assert (out["player_id"] == "3861").all()


# ── Adapter-layer guards (the bronze→meta/dicts prep the hexagon fixtures bypass) ──────────
# These exercise the pre-hexagon driver layer where the GS bugs lived: bronze metadata
# extraction + roster-dict construction, on bronze-shaped (dot-named, string-typed) inputs.
# The hexagon tests feed pre-built `meta`/`frames`, so this layer was never locally covered —
# see feedback_test_production_driver_entry_point. They also pin the narrow-read column sets
# (_GS_EVENTS_META_COLS / _GS_ROSTER_COLS) as sufficient, guarding the wide-toPandas fix
# against silently dropping a column the extractor needs.


def test_extract_metadata_works_with_only_narrow_event_cols() -> None:
    """The narrow events projection must give extract_gradientsports_match_metadata everything
    it needs. Uses string-typed values (live bronze returns 'true'/'366.0' strings)."""
    from ingestion.action_context import _GS_EVENTS_META_COLS
    from ingestion.spadl_adapter import extract_gradientsports_match_metadata

    events = pd.DataFrame(
        {
            "gameEvents.homeTeam": ["false", "true"],
            "gameEvents.teamId": ["51.0", "366.0"],
            "stadiumMetadata.homeTeamStartLeft": ["true", "true"],
            "stadiumMetadata.homeTeamStartLeftExtraTime": [None, None],
        }
    )
    # The pdf carries EXACTLY the narrow projection — proves the projection is sufficient.
    assert set(events.columns) == set(_GS_EVENTS_META_COLS)
    meta = extract_gradientsports_match_metadata(events)
    assert meta["home_team_id"] == 366
    assert meta["home_team_start_left"] is True
    assert meta["home_team_start_left_extratime"] is None


def test_roster_dicts_work_with_only_narrow_roster_cols() -> None:
    """The narrow roster projection (_GS_ROSTER_COLS) suffices for the dict build."""
    from ingestion.action_context import _GS_ROSTER_COLS

    roster = pd.DataFrame(
        {
            "team.id": ["366", "366", "51"],
            "shirtNumber": ["1", "10", "9"],
            "player.id": ["1001", "1010", "2009"],
            "positionGroupType": ["GK", "MID", "FWD"],
        }
    )
    assert set(roster.columns) == set(_GS_ROSTER_COLS)
    team_side_to_id, jersey_to_player_id, gk_player_ids = _build_gradientsports_roster_dicts(roster, "366")
    assert team_side_to_id == {"home": "366", "away": "51"}
    assert jersey_to_player_id[("home", "1")] == "1001"
    assert gk_player_ids == ["1001"]
