"""Parity + edge tests for the vectorized SB360 snapshot builder (ADR-058).

The oracle ``_legacy_build_snapshots`` is a VERBATIM copy of the pre-vectorization
``_run_sb360_enrichment`` loop (action_context.py, pre-ADR-058) — not a re-derivation — so the
vectorized helper is validated against the real prior behavior, including the dup-event
keep="last" tie-break and the malformed-location skips.
"""

from __future__ import annotations

import json

import pandas as pd

from analytics.action_context.sb360_snapshots import build_sb360_snapshots, resolve_home_team_id


def _legacy_build_snapshots(actions_pdf: pd.DataFrame, sb360_pdf: pd.DataFrame) -> pd.DataFrame:
    """VERBATIM copy of the legacy loop (frozen oracle)."""
    _event_ids = actions_pdf["original_event_id"].dropna()
    _action_ids = actions_pdf.loc[_event_ids.index, "action_id"]
    event_to_action = dict(zip(_event_ids, _action_ids, strict=True))
    action_to_team = dict(zip(actions_pdf["action_id"], actions_pdf["team_id"].astype(str), strict=False))
    all_teams = [str(t) for t in actions_pdf["team_id"].dropna().unique()]
    snapshots: list[dict] = []
    for _, row in sb360_pdf.iterrows():
        action_id = event_to_action.get(str(row.get("id", "")))
        if action_id is None:
            continue
        acting_team_id = action_to_team.get(action_id)
        if acting_team_id is None:
            continue
        opponent_teams = [t for t in all_teams if t != acting_team_id]
        opponent_team_id = opponent_teams[0] if opponent_teams else acting_team_id
        team_id = acting_team_id if bool(row.get("teammate", False)) else opponent_team_id
        is_gk = bool(row.get("keeper", False))
        loc = row.get("location")
        if loc is None:
            continue
        if isinstance(loc, str):
            try:
                loc = json.loads(loc)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(loc, (list, tuple)) or len(loc) < 2:
            continue
        snapshots.append(
            {
                "action_id": int(action_id),
                "team_id": team_id,
                "is_goalkeeper": is_gk,
                "x": float(loc[0]),
                "y": float(loc[1]),
            }
        )
    return pd.DataFrame(snapshots)


def _fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    actions = pd.DataFrame(  # uuidA maps to TWO actions (10 then 12): dict keeps LAST (12)
        {
            "action_id": [10, 12, 11],
            "original_event_id": ["uuidA", "uuidA", "uuidB"],
            "team_id": [941, 941, 911],
        }
    )
    sb360 = pd.DataFrame(  # two teams, a GK, a malformed + a single-value location, an unmapped event
        {
            "id": ["uuidA", "uuidA", "uuidB", "uuidB", "uuidZ"],
            "teammate": [True, False, True, False, True],
            "keeper": [False, True, False, False, False],
            "location": ["[40.5, 30.2]", "[100.0, 34.0]", "[12.0, 8.0]", "bad", "[1,1]"],
        }
    )
    return actions, sb360


def test_vectorized_matches_legacy_loop() -> None:
    actions, sb360 = _fixture()
    keys = ["action_id", "x", "y"]
    got = build_sb360_snapshots(actions, sb360).sort_values(keys).reset_index(drop=True)
    exp = _legacy_build_snapshots(actions, sb360).sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(got[exp.columns], exp, check_dtype=False)


def test_duplicate_event_keeps_last_action() -> None:
    actions, sb360 = _fixture()
    got = build_sb360_snapshots(actions, sb360)
    assert set(got["action_id"]) == {12, 11}  # uuidA -> 12 (last), NOT 10 (first)


def test_drops_unmapped_and_malformed() -> None:
    actions, sb360 = _fixture()
    got = build_sb360_snapshots(actions, sb360)
    assert len(got) == 3  # uuidZ unmapped + the "bad" location dropped


def test_empty_inputs_return_schema() -> None:
    cols = ["action_id", "team_id", "is_goalkeeper", "x", "y"]
    out = build_sb360_snapshots(pd.DataFrame(), pd.DataFrame())
    assert list(out.columns) == cols and len(out) == 0


def test_resolve_home_prefers_native() -> None:
    actions = pd.DataFrame({"team_id": [911, 941], "home_team_id_native": ["941", "941"]})
    assert resolve_home_team_id(actions) == "941"


def test_resolve_home_falls_back_sorted_deterministic() -> None:
    actions = pd.DataFrame({"team_id": [941, 911, 941]})  # no home_team_id_native column
    assert resolve_home_team_id(actions) == "911"  # sorted(["911","941"])[0] — deterministic
