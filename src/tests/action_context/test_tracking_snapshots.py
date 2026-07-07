"""Unit tests for the tracking pre-shot snapshot builder (Task 0.4, canonical-SPADL pre-shot xG).

``build_tracking_snapshots`` generalizes ``build_sb360_snapshots`` to the tracking providers
(gradientsports / skillcorner / idsse / metrica): for each shot action it emits per-player rows
from the linked frame, carrying the teammate/keeper flags, the set cardinality, and the shooter
orientation.

IDENTITY CONTRACT (2026-07-07 fix): the shot action's ``team_id`` and the frame's ``team_id`` are
BOTH in the frame-compatible native id space (the driver applies ``_resolve_enrichment_identity``
before calling), and ``home_team_id`` is passed in that same space. Fixtures reflect that: native
string team ids (not the raw hashed BIGINT), a ``type_id`` shot filter (not ``type_name``), and an
explicit ``home_team_id`` for orientation.

The pandas core is pure (no pyspark) so the local hexagon and the driver share one impl.
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.sb360_snapshots import build_sb360_snapshots
from analytics.action_context.tracking_snapshots import _shot_type_id, build_tracking_snapshots

_NON_SHOT_TYPE_ID = -1  # sentinel that is never the canonical 'shot' type_id
# Red-herring value the converted frame carries in its own team_attacking_direction column; it must
# NEVER appear in the output (the meta-derived ltr/rtl wins). If it does, the merge-collision fix broke.
_FRAME_DIR_SENTINEL = "frame_dir_DROP_ME"


def _shot_row(team_id: str = "1") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "action_id": 7,
                "match_key": 100,
                "team_id": team_id,  # frame-compatible native id (string), per the IDENTITY CONTRACT
                "type_id": _shot_type_id(),
                "period_id": 1,
                "data_source": "gradientsports",
            }
        ]
    )


def _frames() -> pd.DataFrame:
    # Models the AC-converted frame shape (sk_frame_adapters / silly-kicks builder output): native
    # string team_id, plus the builder's OWN per-frame ``team_attacking_direction`` (a RED HERRING —
    # the freeze-frame builder must use its home/away-DERIVED value, not the frame's, and must not
    # KeyError on the merge collision) and other passthrough columns the real frame emits. The
    # sentinel value would be visible in the output if the frame's column ever leaked (live GS/SC
    # 2026-07-07 regression).
    return pd.DataFrame(
        [
            {
                "action_id": 7,
                "player_id": "a",
                "team_id": "1",
                "is_goalkeeper": False,
                "x": 95.0,
                "y": 40.0,
                "team_attacking_direction": _FRAME_DIR_SENTINEL,
                "period_id": 1,
                "frame_id": 500,
                "game_id": 42,
            },
            {
                "action_id": 7,
                "player_id": "b",
                "team_id": "2",
                "is_goalkeeper": True,
                "x": 105.0,
                "y": 34.0,
                "team_attacking_direction": _FRAME_DIR_SENTINEL,
                "period_id": 1,
                "frame_id": 500,
                "game_id": 42,
            },
            {
                "action_id": 7,
                "player_id": "c",
                "team_id": "2",
                "is_goalkeeper": False,
                "x": 90.0,
                "y": 20.0,
                "team_attacking_direction": _FRAME_DIR_SENTINEL,
                "period_id": 1,
                "frame_id": 500,
                "game_id": 42,
            },
        ]
    )


def test_is_teammate_resolves_against_native_frame_ids() -> None:
    snaps = build_tracking_snapshots(_shot_row(team_id="1"), _frames(), home_team_id="1")
    row = snaps[snaps.action_id == 7]
    assert len(row) == 3
    # shooter is team "1": 'a' (team "1") is a teammate; 'b'/'c' (team "2") are opponents.
    assert row[row.player_id == "a"].iloc[0].is_teammate == 1
    kpr = row[row.player_id == "b"].iloc[0]
    assert kpr.is_keeper == 1 and kpr.is_teammate == 0  # opponent GK
    assert row[row.player_id == "c"].iloc[0].is_teammate == 0
    # NOT all-zero (the bug this fix closes): at least one teammate resolved.
    assert int(row.is_teammate.sum()) == 1
    assert row.x.between(0, 105).all() and row.y.between(0, 68).all()


def test_cardinality_matches_player_count() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames(), home_team_id="1")
    assert snaps.set_cardinality.iloc[0] == 3


def test_output_carries_match_key_and_data_source() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames(), home_team_id="1")
    assert (snaps.match_key == 100).all()
    assert (snaps.data_source == "gradientsports").all()


def test_orientation_home_shooter_attacks_high_x() -> None:
    # shooter team "1" == home_team_id "1" -> attacks high x -> direction "ltr"
    snaps = build_tracking_snapshots(_shot_row(team_id="1"), _frames(), home_team_id="1")
    assert bool(snaps.shooter_attacks_high_x.iloc[0]) is True
    assert snaps.team_attacking_direction.iloc[0] == "ltr"


def test_frame_team_attacking_direction_does_not_shadow_meta() -> None:
    """Live GS/SC 2026-07-07 regression: the AC-converted frame carries its OWN per-frame
    ``team_attacking_direction``; ``fr.merge(meta, on="action_id")`` suffixed the collision so
    ``fr["team_attacking_direction"]`` KeyError'd. The builder must (1) not raise, and (2) emit the
    home/away-DERIVED value, NOT the frame's red-herring value.
    """
    frames = _frames()
    assert (frames["team_attacking_direction"] == _FRAME_DIR_SENTINEL).all()  # frame's own column present
    snaps = build_tracking_snapshots(_shot_row(team_id="1"), frames, home_team_id="1")  # home shooter -> ltr
    assert len(snaps) == 3  # did not raise / drop rows on the collision
    # meta wins: output uses the derived 'ltr', never the frame's injected sentinel.
    assert (snaps.team_attacking_direction == "ltr").all()
    assert _FRAME_DIR_SENTINEL not in set(snaps.team_attacking_direction.astype("string"))


def test_orientation_away_shooter_attacks_low_x() -> None:
    # shooter team "2" != home_team_id "1" -> away -> attacks low x -> direction "rtl"
    snaps = build_tracking_snapshots(_shot_row(team_id="2"), _frames(), home_team_id="1")
    assert bool(snaps.shooter_attacks_high_x.iloc[0]) is False
    assert snaps.team_attacking_direction.iloc[0] == "rtl"


def test_orientation_na_when_home_unknown() -> None:
    # home_team_id unknown -> never guess -> shooter_attacks_high_x NA, direction NA
    snaps = build_tracking_snapshots(_shot_row(team_id="1"), _frames(), home_team_id=None)
    assert pd.isna(snaps.shooter_attacks_high_x.iloc[0])
    assert pd.isna(snaps.team_attacking_direction.iloc[0])


def test_empty_inputs_return_schema() -> None:
    out = build_tracking_snapshots(pd.DataFrame(), pd.DataFrame())
    assert len(out) == 0
    for col in ("action_id", "match_key", "data_source", "player_id", "x", "y", "is_keeper", "is_teammate"):
        assert col in out.columns


def test_ball_row_is_excluded() -> None:
    players = _frames().assign(is_ball=False)
    ball = pd.DataFrame(
        [
            {
                "action_id": 7,
                "player_id": None,
                "team_id": None,
                "is_goalkeeper": False,
                "x": 52.5,
                "y": 34.0,
                "is_ball": True,
            }
        ]
    )
    frames = pd.concat([players, ball], ignore_index=True)
    snaps = build_tracking_snapshots(_shot_row(), frames, home_team_id="1")
    assert len(snaps) == 3  # ball dropped, 3 players kept
    assert snaps.set_cardinality.iloc[0] == 3


def test_non_shot_actions_are_filtered_by_type_id() -> None:
    actions = pd.concat(
        [_shot_row(), _shot_row().assign(action_id=8, type_id=_NON_SHOT_TYPE_ID)],
        ignore_index=True,
    )
    frames = pd.concat([_frames(), _frames().assign(action_id=8)], ignore_index=True)
    snaps = build_tracking_snapshots(actions, frames, home_team_id="1")
    assert set(snaps.action_id.unique()) == {7}  # the non-shot (action 8) is dropped by type_id


def test_actor_inclusion_matches_sb360_convention() -> None:
    """M3: build_sb360_snapshots includes EVERY freeze-frame row (no actor-specific filter —
    sb360_snapshots.py only reads teammate/keeper/location, never an ``actor`` column).
    StatsBomb 360 freeze-frames carry the acting player, so the actor is INCLUDED. The tracking
    builder must match: the shooter is one of the frame's player rows and must be kept.
    """
    sb_actions = pd.DataFrame({"action_id": [7], "original_event_id": ["ev7"], "team_id": [941]})
    sb360 = pd.DataFrame(
        {
            "id": ["ev7", "ev7", "ev7"],
            "teammate": [True, True, False],  # first row = the acting player (teammate)
            "keeper": [False, False, True],
            "location": ["[100.0, 40.0]", "[80.0, 20.0]", "[118.0, 40.0]"],
        }
    )
    sb_out = build_sb360_snapshots(sb_actions, sb360)
    assert len(sb_out) == 3  # sb360 keeps all 3 rows (actor NOT dropped)

    trk_out = build_tracking_snapshots(_shot_row(team_id="1"), _frames(), home_team_id="1")
    assert len(trk_out) == 3
    assert "a" in set(trk_out.player_id)  # the acting player 'a' is present — matches sb360
