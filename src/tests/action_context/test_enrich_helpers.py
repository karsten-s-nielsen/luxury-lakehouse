"""Unit tests for ``_fill_possession_from_set_piece_actions``.

The lakehouse-domain modeling helper that synthesizes ``team_in_possession`` for
SPADL set-piece restart actions in dead-ball windows, so silly-kicks 3.30.0+ can
compute FINITE DAS on actions whose possession is SPADL-determinable. silly-kicks
otherwise honestly returns NaN where carrier-derived possession is unavailable.

The helper reads ``type_id`` (canonical SPADL int, present from add_game_state
onward), NOT ``type_name`` (which build_output adds only at the bronze-output
stage). Restart type_ids are derived at first call from silly-kicks's
authoritative ``actiontypes`` list — drift-safe.

See [[project_sk330_dead_ball_robustness_handoff]] for the architectural split
between silly-kicks (no-crash + honest NaN) and lakehouse (modeling decision).
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.enrich import (
    _SET_PIECE_RESTART_TYPE_NAMES,
    _fill_possession_from_set_piece_actions,
    _set_piece_restart_type_ids,
)


def _name_to_type_id(name: str) -> int:
    """Resolve a SPADL action name to its canonical type_id via silly-kicks."""
    from silly_kicks.spadl.config import actiontypes

    return actiontypes.index(name)


def _make_frames_tip(*, team_in_possession_per_frame: dict[int, str | None]) -> pd.DataFrame:
    """Build a minimal frames_tip DataFrame: 2 teams x 11 players + 1 ball per frame.

    Mirrors the shape silly-kicks' ``derive_team_in_possession`` produces.
    """
    rows: list[dict[str, object]] = []
    for frame_id, tip in team_in_possession_per_frame.items():
        for team_id in ("A", "B"):
            for player_idx in range(11):
                rows.append(
                    {
                        "period_id": 1,
                        "frame_id": frame_id,
                        "team_id": team_id,
                        "player_id": f"{team_id}{player_idx}",
                        "is_ball": False,
                        "team_in_possession": tip,
                    }
                )
        rows.append(
            {
                "period_id": 1,
                "frame_id": frame_id,
                "team_id": None,
                "player_id": None,
                "is_ball": True,
                "team_in_possession": tip,
            }
        )
    return pd.DataFrame(rows)


def _make_actions(rows: list[tuple[int, str, str, int]]) -> pd.DataFrame:
    """Each row: (action_id, type_name, team_id, period_id). type_name → type_id via silly-kicks."""
    return pd.DataFrame(
        [{"action_id": a, "type_id": _name_to_type_id(t), "team_id": tm, "period_id": p} for a, t, tm, p in rows]
    )


def _make_links(rows: list[tuple[int, int]]) -> pd.DataFrame:
    """Each row: (action_id, frame_id).

    Matches the real shape returned by ``silly_kicks.tracking.link_actions_to_frames``:
    only (action_id, frame_id, time_offset_seconds, n_candidate_frames,
    link_quality_score). NO period_id — the helper pulls period_id from actions.
    """
    return pd.DataFrame([{"action_id": a, "frame_id": f} for a, f in rows])


# --- contract: writes when NaN ---


def test_fill_writes_team_id_on_dead_ball_linked_frame_for_goalkick() -> None:
    frames = _make_frames_tip(team_in_possession_per_frame={100: None, 101: None})
    actions = _make_actions([(1, "goalkick", "A", 1)])
    links = _make_links([(1, 100)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert (out.loc[out["frame_id"] == 100, "team_in_possession"] == "A").all()
    # Frame 101 untouched — no action linked there
    assert out.loc[out["frame_id"] == 101, "team_in_possession"].isna().all()


def test_fill_handles_every_set_piece_restart_type() -> None:
    type_names_sorted = sorted(_SET_PIECE_RESTART_TYPE_NAMES)
    frames = _make_frames_tip(team_in_possession_per_frame={i: None for i in range(100, 100 + len(type_names_sorted))})
    actions = _make_actions([(i, t, "A", 1) for i, t in enumerate(type_names_sorted)])
    links = _make_links([(i, 100 + i) for i in range(len(type_names_sorted))])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    for i, name in enumerate(type_names_sorted):
        rows = out.loc[out["frame_id"] == 100 + i, "team_in_possession"]
        assert (rows == "A").all(), f"{name} (type_id={_name_to_type_id(name)}) did not fill"


# --- contract: never overwrites carrier-derived ---


def test_fill_does_not_overwrite_carrier_derived_value() -> None:
    """Carrier inference is authoritative — set-piece fill must NEVER overwrite a
    non-NaN possession (the carrier already resolved who has the ball)."""
    frames = _make_frames_tip(team_in_possession_per_frame={100: "B"})  # B already in possession
    actions = _make_actions([(1, "goalkick", "A", 1)])  # but the goalkick belongs to A
    links = _make_links([(1, 100)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert (out.loc[out["frame_id"] == 100, "team_in_possession"] == "B").all()


# --- contract: open-play action types are not touched ---


def test_fill_skips_open_play_action_types() -> None:
    """pass/dribble/tackle/etc. — carrier inference is authoritative, no fill."""
    frames = _make_frames_tip(team_in_possession_per_frame={100: None, 101: None, 102: None})
    actions = _make_actions(
        [
            (1, "pass", "A", 1),
            (2, "dribble", "B", 1),
            (3, "tackle", "A", 1),
        ]
    )
    links = _make_links([(1, 100), (2, 101), (3, 102)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert out["team_in_possession"].isna().all()


# --- contract: defensive edge cases ---


def test_fill_handles_empty_actions() -> None:
    frames = _make_frames_tip(team_in_possession_per_frame={100: None})
    actions = pd.DataFrame(columns=["action_id", "type_id", "team_id", "period_id"])
    links = pd.DataFrame(columns=["action_id", "frame_id", "period_id"])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    pd.testing.assert_frame_equal(out, frames)


def test_fill_handles_set_piece_action_with_null_team_id() -> None:
    """An action with team_id=None gets skipped — fill would be ambiguous."""
    frames = _make_frames_tip(team_in_possession_per_frame={100: None})
    actions = pd.DataFrame(
        [
            {
                "action_id": 1,
                "type_id": _name_to_type_id("goalkick"),
                "team_id": None,
                "period_id": 1,
            }
        ]
    )
    links = _make_links([(1, 100)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert out["team_in_possession"].isna().all()


def test_fill_handles_two_set_pieces_on_same_frame() -> None:
    """Edge case: two set-piece actions at the exact same (period_id, frame_id).
    The helper picks the first deterministically — silly-kicks needs ONE value per frame."""
    frames = _make_frames_tip(team_in_possession_per_frame={100: None})
    actions = _make_actions([(1, "goalkick", "A", 1), (2, "throw_in", "B", 1)])
    links = _make_links([(1, 100), (2, 100)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    fill = out.loc[out["frame_id"] == 100, "team_in_possession"].iloc[0]
    assert fill in {"A", "B"}, f"Expected A or B, got {fill!r}"
    # All rows for frame 100 are filled with the SAME value (deterministic single-value fill)
    assert (out.loc[out["frame_id"] == 100, "team_in_possession"] == fill).all()


# --- guard: never inject a team that is not on the pitch (ADR-078) ---


def test_fill_skips_action_whose_team_is_not_on_the_pitch() -> None:
    """A set-piece action whose team_id is not one of the frame's two player teams
    (e.g. the IDSSE freekick_short ``__UNKNOWN_TEAM__`` sentinel) must NOT be injected into
    ``team_in_possession``.

    silly-kicks' ``add_das`` → accessible_space ``infer_playing_direction`` unions the frames'
    ``team_id`` column with ``team_in_possession`` and hard-raises ``ValueError`` when it finds a
    third team — killing the whole applyInPandas work-unit (the 2026-08-22 idsse-p2 drain failure).
    An action whose team is not on the pitch has unresolvable possession, so it correctly gets NaN DAS.
    """
    frames = _make_frames_tip(team_in_possession_per_frame={100: None})  # players are teams A / B only
    actions = _make_actions([(1, "freekick_short", "__UNKNOWN_TEAM__", 1)])
    links = _make_links([(1, 100)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert out.loc[out["frame_id"] == 100, "team_in_possession"].isna().all()
    # A third team must never appear in the possession column
    assert set(out["team_in_possession"].dropna().unique()) <= {"A", "B"}


def test_fill_is_surgical_valid_team_fills_while_off_pitch_team_skipped() -> None:
    """The guard is per-action: a valid on-pitch team still fills while an off-pitch (sentinel)
    team is skipped — one bad action must not suppress the legitimate fills."""
    frames = _make_frames_tip(team_in_possession_per_frame={100: None, 101: None})
    actions = _make_actions([(1, "goalkick", "A", 1), (2, "freekick_short", "__UNKNOWN_TEAM__", 1)])
    links = _make_links([(1, 100), (2, 101)])

    out = _fill_possession_from_set_piece_actions(frames, actions=actions, links=links)

    assert (out.loc[out["frame_id"] == 100, "team_in_possession"] == "A").all()  # valid team fills
    assert out.loc[out["frame_id"] == 101, "team_in_possession"].isna().all()  # off-pitch team skipped


# --- module-level invariants ---


def test_set_piece_restart_type_names_covers_every_spadl_restart() -> None:
    """Drift guard: if silly-kicks adds a new restart type, the lakehouse fill set must follow."""
    expected_restarts = {
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "shot_freekick",
        "corner_crossed",
        "corner_short",
        "goalkick",
        "shot_penalty",
    }
    assert _SET_PIECE_RESTART_TYPE_NAMES == expected_restarts


def test_set_piece_restart_type_ids_resolves_to_known_canonical_ints() -> None:
    """Validates the lazy name→id resolution against silly-kicks 3.30.0's canonical ordering.

    If this test fails after a silly-kicks bump, silly-kicks reordered or removed a
    restart action type — the names are still correct (drift-safe) but the test
    needs its `expected` set updated as evidence of the reorder.
    """
    expected = {2, 3, 4, 5, 6, 12, 13, 22}  # canonical positions of the 8 restart types
    assert _set_piece_restart_type_ids() == expected
