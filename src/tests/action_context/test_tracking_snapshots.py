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

import json

import numpy as np
import pandas as pd

from analytics.action_context.sb360_freeze_frames import (
    build_sb360_freeze_frames,
    convert_statsbomb_locations_to_spadl,
)
from analytics.action_context.sb360_snapshots import build_sb360_snapshots
from analytics.action_context.tracking_snapshots import (
    _select_preshot_frame_per_action,
    _shot_type_id,
    build_tracking_snapshots,
    build_tracking_snapshots_spark,
)

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


def test_both_builders_include_actor() -> None:
    """N2 (Task 1.7): BOTH freeze-frame builders retain the shooter.

    The tracking builder is ID-based — the shooter is one of the frame's player rows and must appear
    by ``player_id``. The SB-360 builder emits ANONYMOUS frames (synthetic ``sb360_*`` ids), so the
    shooter cannot be matched by id; it is matched by POSITION — the acting player's converted 360
    ``location`` colocates with the shot's start, so an output row must sit on that position.
    """
    # ── Tracking builder: player 'a' (shooter's team "1") must be present by id ──────────────────
    trk = build_tracking_snapshots(_shot_row(team_id="1"), _frames(), home_team_id="1")
    assert "a" in set(trk.player_id), "tracking builder dropped the shooter row"

    # ── SB-360 builder: the actor's converted position must be present (ids are synthetic) ────────
    actor_loc = [100.0, 40.0]  # raw StatsBomb 120x80 location of the acting player
    actions = pd.DataFrame(
        {
            "original_event_id": ["ev1", None],  # 2nd row (distinct team, NaN event) only resolves the opponent
            "action_id": [1, 2],
            "team_id": ["TEAM_A", "TEAM_B"],
            "match_key": [100, 100],
        }
    )
    sb360 = pd.DataFrame(
        {
            "id": ["ev1", "ev1", "ev1"],
            "actor": [True, False, False],  # sb360_snapshots ignores 'actor'; the row is kept regardless
            "teammate": [True, True, False],
            "keeper": [False, False, True],
            "location": [json.dumps(actor_loc), json.dumps([80.0, 20.0]), json.dumps([118.0, 40.0])],
        }
    )
    out = build_sb360_freeze_frames(actions, sb360, 2)
    converted = convert_statsbomb_locations_to_spadl(pd.Series([actor_loc]), 2)[0]
    dists = np.linalg.norm(out[["x", "y"]].to_numpy() - converted, axis=1)
    assert dists.min() <= 0.01, f"SB-360 builder dropped the actor (nearest row {dists.min():.4f} m away)"


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


# ── ONE strictly-PRE-SHOT frame per shot + intra-frame player dedup (2026-07-07 full-cohort) ─────
# Live full-cohort backfill: rows-per-shot were exact multiples of the ~22 frame size (SC shots at
# 22/44/66/...; GS one at 374 = 17x22). A freeze frame must be ONE tracking instant per shot — the
# STRICTLY PRE-SHOT frame (last frame at-or-before the shot) — exactly one row per (action_id,
# player_id). FIX 1 = pre-shot merge_asof(backward); FIX 2 = dedup player rows within the frame.


def _timed_shot(time_seconds: float = 10.0, team_id: str = "1") -> pd.DataFrame:
    # A shot action with time_seconds (linkage is by time), skillcorner-shaped.
    return pd.DataFrame(
        [
            {
                "action_id": 7,
                "match_key": 100,
                "team_id": team_id,
                "type_id": _shot_type_id(),
                "period_id": 1,
                "data_source": "skillcorner",
                "time_seconds": time_seconds,
            }
        ]
    )


def _frame_players(frame_id: int, t: float, *, xa: float = 95.0) -> pd.DataFrame:
    # One frame at time t with a 4-player on-pitch set (2 per team, one GK). ``xa`` marks the frame:
    # player 'a' carries a frame-specific x so a test can tell WHICH frame the builder selected.
    rows = [
        {"player_id": "a", "team_id": "1", "is_goalkeeper": False, "x": xa, "y": 40.0},
        {"player_id": "b", "team_id": "2", "is_goalkeeper": True, "x": 105.0, "y": 34.0},
        {"player_id": "c", "team_id": "2", "is_goalkeeper": False, "x": 90.0, "y": 20.0},
        {"player_id": "d", "team_id": "1", "is_goalkeeper": False, "x": 80.0, "y": 30.0},
    ]
    for r in rows:
        r.update(frame_id=frame_id, period_id=1, time_seconds=t, is_ball=False)
    return pd.DataFrame(rows)


def _frame_index(*frames: pd.DataFrame) -> pd.DataFrame:
    trk = pd.concat(frames, ignore_index=True)
    return trk[["period_id", "frame_id", "time_seconds"]].rename(columns={"time_seconds": "frame_time"})


def test_select_preshot_frame_picks_last_at_or_before_shot() -> None:
    # shot at 10.06; PRE frame 500 @10.00 (offset +0.06), POST frame 501 @10.10 (offset -0.04, NEARER).
    # Strictly-pre-shot must pick 500 (the last frame <= the shot), NOT the marginally-nearer post one.
    shots = _timed_shot(10.06)
    fidx = _frame_index(_frame_players(500, 10.00), _frame_players(501, 10.10))
    best = _select_preshot_frame_per_action(shots, fidx)
    assert len(best) == 1
    assert int(best["frame_id"].iloc[0]) == 500  # pre-shot wins over nearer post-shot


def test_select_preshot_frame_picks_latest_of_several_before() -> None:
    # 3 pre-shot frames; shot after all of them -> the LAST (highest time <= shot) is chosen.
    shots = _timed_shot(10.30)
    fidx = _frame_index(_frame_players(500, 10.00), _frame_players(501, 10.10), _frame_players(502, 10.20))
    best = _select_preshot_frame_per_action(shots, fidx)
    assert len(best) == 1
    assert int(best["frame_id"].iloc[0]) == 502  # last frame before the shot


def test_select_preshot_frame_dropped_when_only_post_shot_frames() -> None:
    # No frame at-or-before the shot (all candidates AFTER) -> DROPPED (no nearest/post-shot fallback).
    shots = _timed_shot(9.0)
    fidx = _frame_index(_frame_players(500, 10.00), _frame_players(501, 10.10))
    best = _select_preshot_frame_per_action(shots, fidx)
    assert best.empty  # zero-context shot: never substitute a post-shot frame


def test_select_preshot_frame_dropped_when_only_stale_before() -> None:
    # The only at-or-before frame is 0.5s before the shot — beyond the 0.2s tolerance -> DROPPED.
    shots = _timed_shot(10.50)
    fidx = _frame_index(_frame_players(500, 10.00))  # offset 0.50 > _FREEZE_FRAME_TOLERANCE_SECONDS
    best = _select_preshot_frame_per_action(shots, fidx)
    assert best.empty  # never given a stale frame


def test_select_preshot_frame_kept_when_fresh_before() -> None:
    # A fresh pre-shot frame 0.05s before the shot (within 0.2s tolerance) -> kept.
    shots = _timed_shot(10.05)
    fidx = _frame_index(_frame_players(500, 10.00))  # offset 0.05 <= tolerance
    best = _select_preshot_frame_per_action(shots, fidx)
    assert len(best) == 1
    assert int(best["frame_id"].iloc[0]) == 500


def test_spark_selects_preshot_frame_over_nearer_post_shot() -> None:
    """Semantic end-to-end: the emitted player set comes from the PRE-SHOT frame, even though the
    post-shot frame is marginally nearer in time."""
    tracking = pd.concat(
        [_frame_players(500, 10.00, xa=95.0), _frame_players(501, 10.10, xa=50.0)],  # pre xa=95, post xa=50
        ignore_index=True,
    )
    out = build_tracking_snapshots_spark(_timed_shot(10.06), tracking, home_team_id="1")  # nearer post = 501
    assert len(out) == 4  # one frame's player set
    assert out.groupby(["action_id", "player_id"]).size().max() == 1
    # player 'a' x is 95.0 (the PRE-SHOT frame 500), NOT 50.0 (the nearer post-shot frame 501).
    assert float(out[out.player_id == "a"]["x"].iloc[0]) == 95.0


def test_spark_dedups_duplicated_frame_players() -> None:
    """FIX 2: a GS-style 16x content-divergent duplicate of one (period, frame) must contribute each
    player ONCE — not 16x. Reproduces the 374 = 17x22 live GS case in miniature."""
    frame = _frame_players(500, 10.0)
    copies = [frame.assign(x=frame["x"] + k * 0.01) for k in range(16)]  # content-divergent, same identity
    tracking = pd.concat(copies, ignore_index=True)
    assert len(tracking) == 64  # 4 players x 16 copies

    out = build_tracking_snapshots_spark(_timed_shot(10.0), tracking, home_team_id="1")
    assert len(out) == 4  # each player ONCE (NOT 64)
    assert out.groupby(["action_id", "player_id"]).size().max() == 1  # the invariant
    assert int(out["set_cardinality"].iloc[0]) == 4  # on-pitch count, not a multiple


def test_spark_one_frame_per_shot_with_many_preshot_frames() -> None:
    """FIX 1: with several candidate frames before the shot, exactly ONE (the last pre-shot) frame's
    player set is emitted — not k*(frame size)."""
    tracking = pd.concat(
        [_frame_players(500, 10.00), _frame_players(501, 10.10), _frame_players(502, 10.20)],
        ignore_index=True,
    )
    out = build_tracking_snapshots_spark(_timed_shot(10.30), tracking, home_team_id="1")
    assert len(out) == 4  # ONE frame's player set (NOT 3x4 = 12)
    assert out.groupby(["action_id", "player_id"]).size().max() == 1  # the invariant
    assert int(out["set_cardinality"].iloc[0]) == 4


def test_spark_duplicate_action_id_does_not_refan() -> None:
    """A duplicate shot row (same action_id) must not multiply the output — one player set per shot."""
    shots = pd.concat([_timed_shot(10.05), _timed_shot(10.05)], ignore_index=True)  # action_id 7 twice
    tracking = _frame_players(500, 10.00)  # 0.05s before -> within tolerance
    out = build_tracking_snapshots_spark(shots, tracking, home_team_id="1")
    assert len(out) == 4
    assert out.groupby(["action_id", "player_id"]).size().max() == 1
