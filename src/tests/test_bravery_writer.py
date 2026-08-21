"""Unit tests for ``ingestion.bravery_writer`` (Task 17g) on synthetic SPADL actions.

Exercises the PURE core ``_compute_bravery_group`` (the per-match ``applyInPandas`` closure body) on a
hand-built two-team match. Bravery is event-only, so no tracking frames / xT / Spark are needed here;
the Spark ``run_pipeline`` (``applyInPandas`` dispatch) is validated live in Part B.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from silly_kicks.spadl import config as spadlconfig

from ingestion.bravery_writer import OUTPUT_COLUMNS, _compute_bravery_group

_SHOT = spadlconfig.actiontype_id["shot"]
_CROSS = spadlconfig.actiontype_id["cross"]
_PASS = spadlconfig.actiontype_id["pass"]


def _match_actions() -> pd.DataFrame:
    """One synthetic match: team 'TA' takes 2 shots (1 blocked); team 'TB' takes 1 open-play cross.

    So the DEFENDING team 'TB' faced TA's shots (block rate 1/2 = 0.5) and the DEFENDING team 'TA'
    faced TB's cross (block rate 0/1 = 0.0). A non-shot/cross pass is included to prove it is ignored.
    """
    rows = [
        # actor TA — shot, blocked
        {"type_id": _SHOT, "team_id_native": "TA", "shot_blocked": True, "cross_blocked": pd.NA},
        # actor TA — shot, not blocked
        {"type_id": _SHOT, "team_id_native": "TA", "shot_blocked": False, "cross_blocked": pd.NA},
        # actor TB — open-play cross, not blocked
        {"type_id": _CROSS, "team_id_native": "TB", "shot_blocked": pd.NA, "cross_blocked": False},
        # actor TA — a pass (must be ignored by bravery)
        {"type_id": _PASS, "team_id_native": "TA", "shot_blocked": pd.NA, "cross_blocked": pd.NA},
    ]
    df = pd.DataFrame(rows)
    df["shot_blocked"] = df["shot_blocked"].astype("boolean")
    df["cross_blocked"] = df["cross_blocked"].astype("boolean")
    df["data_source"] = "skillcorner"
    df["match_id_native"] = "M1"
    df["game_id"] = 100
    return df


def test_bravery_columns_and_identity() -> None:
    result = _compute_bravery_group(_match_actions())

    assert list(result.columns) == list(OUTPUT_COLUMNS)
    assert not result.empty
    assert (result["data_source"] == "skillcorner").all()
    assert (result["match_id"] == "M1").all()


def test_bravery_grain_is_defending_team_per_match() -> None:
    """Grain (review-4 B4): one row per DEFENDING team = the OPPONENT of the actor."""
    result = _compute_bravery_group(_match_actions())

    # Two teams -> two defending-team rows, no duplicates.
    assert set(result["team_id"]) == {"TA", "TB"}
    assert result["team_id"].nunique() == len(result)

    # TB is the DEFENDING team that faced TA's shots.
    tb = result[result["team_id"] == "TB"].iloc[0]
    assert tb["bravery_shots"] == 0.5  # 1 of 2 shots blocked
    assert tb["n_shots_faced"] == 2
    assert tb["n_blocks_known"] == 1

    # TA is the DEFENDING team that faced TB's cross.
    ta = result[result["team_id"] == "TA"].iloc[0]
    assert ta["bravery_open_play_crosses"] == 0.0  # 0 of 1 cross blocked
    assert ta["n_open_play_crosses_faced"] == 1


def test_bravery_dtypes() -> None:
    result = _compute_bravery_group(_match_actions())

    assert result["bravery_shots"].dtype == np.float64
    assert result["bravery_pct_known_domain"].dtype == np.float64
    # n_* are nullable integers (n_blocks_known can be <NA> when there is no known block signal).
    assert str(result["n_shots_faced"].dtype) == "Int64"
    assert str(result["n_blocks_known"].dtype) == "Int64"
    # team_id is the NATIVE defending-team id (so dim_teams resolves it to team_key — review-4 B2).
    assert str(result["team_id"].dtype) == "string"
    assert set(result["team_id"]) <= {"TA", "TB"}


def test_bravery_set_piece_crosses_always_null_v1() -> None:
    """silly-kicks v1 always emits NaN bravery_set_piece_crosses (documented column limitation)."""
    result = _compute_bravery_group(_match_actions())
    assert result["bravery_set_piece_crosses"].isna().all()


def test_bravery_empty_when_no_final_actions() -> None:
    """A match with only passes -> compute_bravery's empty contract -> full-schema empty frame."""
    actions = _match_actions()
    passes_only = actions[actions["type_id"] == _PASS].copy()
    result = _compute_bravery_group(passes_only)
    assert list(result.columns) == list(OUTPUT_COLUMNS)
    assert result.empty
