"""Golden tests for the SB-360 -> SPADL freeze-frame builder (Task 2.1, pre-shot xG v3).

These goldens were captured + verified against LIVE StatsBomb-360 data (two real shots), then frozen
here. The load-bearing case is ``sb360_golden_away_p2`` (an away-team, high-y shot in period 2): a
mirrored / double-transformed / wrong-fidelity conversion would land the actor ~40 m off the shot's
``fct_action_values`` ground truth. The ``home_p1`` fixture is the control.

The core invariant being pinned: StatsBomb event + 360 data is ALREADY shooter-normalized (attacking
team -> high-x) and the 360 freeze-frame ``location`` is in the SAME raw 120x80 space the shot action's
SPADL conversion consumed. Therefore the freeze-frame builder needs NO orientation step — it reuses
``silly_kicks.spadl.statsbomb._convert_locations`` verbatim, so frame and action are byte-consistent.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.sb360_freeze_frames import (
    build_sb360_freeze_frames,
    convert_statsbomb_locations_to_spadl,
)
from analytics.action_context.tracking_snapshots import _SHOT_FF_COLUMNS

_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"
_GOLDEN_FILES = ("sb360_golden_away_p2.json", "sb360_golden_home_p1.json")


def _load_golden(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _actor_location(golden: dict) -> list[float]:
    actor = next(row for row in golden["freeze_frame"] if row["actor"])
    return actor["location"]


# ---------------------------------------------------------------------------
# (a) convert_statsbomb_locations_to_spadl
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("golden_file", _GOLDEN_FILES)
def test_conversion_actor_colocates_with_shot_ground_truth(golden_file: str) -> None:
    """The converted actor position lands on the shot's ``fct_action_values.start_x/y`` ground truth.

    This is the PROOF that (1) SB-360 is shooter-normalized so NO orientation step is needed, and (2)
    the y-flip + cell-center offset + fidelity are correct. A mirrored / double-transformed / wrong-
    fidelity conversion lands the actor ~40 m off — the ``away_p2`` (high-y, period-2) case is the hard
    gate. Do NOT tighten below ~1 m: the ground truth itself is a rounded ``fct_action_values`` value.
    """
    golden = _load_golden(golden_file)
    converted = convert_statsbomb_locations_to_spadl(
        pd.Series([_actor_location(golden)]), golden["shot_fidelity_version"]
    )
    gt = np.array([golden["ground_truth_start_x"], golden["ground_truth_start_y"]])
    dist = float(np.linalg.norm(converted[0] - gt))
    assert dist <= 2.0, f"{golden_file}: actor {converted[0]} is {dist:.4f} m from ground truth {gt}"


@pytest.mark.parametrize("golden_file", _GOLDEN_FILES)
def test_golden_precondition_ground_truth_is_acting_team_ltr(golden_file: str) -> None:
    """Pin the golden's precondition: ``fct_action_values`` shots are acting-team-LTR (shooter high-x).

    A future re-materialization of ``fct_action_values`` to a different orientation would silently break
    the ground truth these goldens assert against. This makes that dependency explicit — if this fails,
    the golden ground truth (not the builder) needs regenerating.
    """
    golden = _load_golden(golden_file)
    assert golden["ground_truth_start_x"] >= 52.5


def test_convert_locations_matches_silly_kicks_exactly() -> None:
    """Byte-identical to ``silly_kicks``' own transform — proves verbatim reuse, no hand-rolled scale."""
    from silly_kicks.spadl.statsbomb import _convert_locations

    ours = convert_statsbomb_locations_to_spadl(pd.Series([[60.0, 40.0]]), 2)
    theirs = _convert_locations(pd.Series([[60.0, 40.0]]), 2)
    np.testing.assert_array_equal(ours, theirs)


# ---------------------------------------------------------------------------
# (b) build_sb360_freeze_frames — full pipeline
# ---------------------------------------------------------------------------
def _build_inputs(golden: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Construct the (actions_df, sb360_raw_df) the builder consumes from a golden fixture.

    ``build_sb360_snapshots`` resolves a non-teammate row to the *opponent* team id, which it derives
    from the set of distinct ``team_id`` values in ``actions_df``. A single shot row would leave only
    ONE team, so a second (distinct-team) action row is added purely so the opponent resolves — it has
    a NaN ``original_event_id`` (dropped from the event->action map) and never appears in the output.
    """
    shooter_team = golden["shooter_team_id_native"]
    opponent_team = f"OPP_{shooter_team}"  # distinct sentinel so opponent resolution is non-degenerate
    actions_df = pd.DataFrame(
        {
            "original_event_id": [golden["event_id"], None],
            "action_id": [golden["action_id"], golden["action_id"] + 1],
            "team_id": [shooter_team, opponent_team],
            "match_key": [golden["match_key"], golden["match_key"]],
        }
    )
    sb360_raw_df = pd.DataFrame(
        {
            "id": [golden["event_id"]] * len(golden["freeze_frame"]),
            "actor": [row["actor"] for row in golden["freeze_frame"]],
            "teammate": [row["teammate"] for row in golden["freeze_frame"]],
            "keeper": [row["keeper"] for row in golden["freeze_frame"]],
            "location": [json.dumps(row["location"]) for row in golden["freeze_frame"]],
        }
    )
    return actions_df, sb360_raw_df


@pytest.mark.parametrize("golden_file", _GOLDEN_FILES)
def test_build_sb360_freeze_frames_full_pipeline(golden_file: str) -> None:
    golden = _load_golden(golden_file)
    actions_df, sb360_raw_df = _build_inputs(golden)

    out = build_sb360_freeze_frames(actions_df, sb360_raw_df, golden["shot_fidelity_version"])

    # Schema: exact columns, exact order.
    assert list(out.columns) == list(_SHOT_FF_COLUMNS)

    # One row per freeze-frame player; set_cardinality carries the player count.
    assert len(out) == golden["n_players"]
    assert (out["set_cardinality"] == golden["n_players"]).all()

    # Constant driver-stamped / orientation columns.
    assert (out["access_tier"] == "public").all()
    assert out["shooter_attacks_high_x"].all()  # True-constant: StatsBomb attack-normalizes
    assert (out["team_attacking_direction"] == "ltr").all()

    # Synthetic, globally-unique, clearly-synthetic player ids: sb360_{match_key}_{action_id}_{i}.
    pattern = re.compile(rf"^sb360_{golden['match_key']}_{golden['action_id']}_\d+$")
    assert all(pattern.match(pid) for pid in out["player_id"])
    assert out["player_id"].is_unique

    # Keeper flag carried faithfully (0 keepers in the away fixture, 1 in home — derive from data).
    expected_keepers = sum(1 for row in golden["freeze_frame"] if row["keeper"])
    assert int(out["is_keeper"].sum()) == expected_keepers

    # is_teammate recovers both classes (fixtures carry both teammate and opponent rows).
    assert set(out["is_teammate"].unique()) == {0, 1}

    # N2 actor-inclusion: the actor's converted position (~ground truth) is present among the rows.
    gt = np.array([golden["ground_truth_start_x"], golden["ground_truth_start_y"]])
    dists = np.linalg.norm(out[["x", "y"]].to_numpy() - gt, axis=1)
    assert dists.min() <= 2.0, f"{golden_file}: actor row missing (nearest {dists.min():.4f} m)"


def test_build_sb360_freeze_frames_empty_returns_schema() -> None:
    """No snapshots -> empty frame with the canonical columns (never a bare empty frame)."""
    empty_actions = pd.DataFrame(columns=["original_event_id", "action_id", "team_id", "match_key"])
    empty_raw = pd.DataFrame(columns=["id", "actor", "teammate", "keeper", "location"])
    out = build_sb360_freeze_frames(empty_actions, empty_raw, 2)
    assert list(out.columns) == list(_SHOT_FF_COLUMNS)
    assert out.empty
