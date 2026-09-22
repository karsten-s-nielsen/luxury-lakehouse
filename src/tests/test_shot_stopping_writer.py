"""Unit tests for ``ingestion.shot_stopping_writer`` (P1 Task B1) on synthetic SPADL actions.

Exercises the PURE core ``_compute_shot_stopping_group`` (the per-match ``applyInPandas`` closure body)
on a hand-built match. shot_stopping is event-only + an INJECTED per-shot Post-Shot xG (silly-kicks
TF-59 ``compute_shot_stopping``), so no tracking frames / Spark are needed here; the Spark
``run_pipeline`` (``applyInPandas`` dispatch + the PSxG merge) is validated live in Part B.

Key contracts under test:
  * ``compute_shot_stopping`` RAISES ``KeyError`` unless BOTH ``defending_gk_player_id`` AND
    ``defending_gk_team_id`` are present -- the writer DERIVES the team from the per-match
    ``player_id -> team_id_native`` map (owner-decided 2026-09-20).
  * ONE row per CANONICAL keeper (dtype-mixed keeper ids do not fragment).
  * ``goals_prevented == psxg_faced - goals_conceded``; the ``_excl_penalties`` companions.
  * the emitted keeper id is the NATIVE ``player_id_native`` (for the dim_players mart join).
  * the column set is pinned to the ``METRIC_CONTRACTS['shot_stopping']`` registry (SK-EXPORT drift guard).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from silly_kicks.metric_contracts import METRIC_CONTRACTS
from silly_kicks.shot_stopping import SS_KEYS
from silly_kicks.spadl import config as spadlconfig

from ingestion.shot_stopping_writer import (
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    _compute_shot_stopping_group,
)

_SHOT = spadlconfig.actiontype_id["shot"]
_SHOT_PENALTY = spadlconfig.actiontype_id["shot_penalty"]
_PASS = spadlconfig.actiontype_id["pass"]
_SUCCESS = spadlconfig.result_id["success"]
_FAIL = spadlconfig.result_id["fail"]

# Working player_ids: keeper of team TB (native "GK_TB") is 900; keeper of TA (native "GK_TA") is 901.
_GK_TB = 900
_GK_TA = 901


def _mk(**kw: object) -> dict[str, object]:
    base: dict[str, object] = {
        "data_source": "idsse",
        "match_id_native": "M1",
        "match_key": 111,
        "access_tier": "restricted",
        "game_id": 1,
        "action_id": 0,
        "period_id": 1,
        "type_id": _PASS,
        "result_id": _FAIL,
        "shot_blocked": pd.NA,
        "player_id": 10,
        "player_id_native": "P10",
        "team_id_native": "TA",
        "defending_gk_player_id": pd.NA,
        # PSxG is injected by the writer's LEFT-JOIN; NaN on non-shot rows (the on-target gate).
        "psxg_recalibrated": np.nan,
    }
    base.update(kw)
    return base


def _match_actions() -> pd.DataFrame:
    """One synthetic match. TA takes 3 shots at TB's keeper (900); TB takes 1 penalty at TA's keeper (901).

    TB keeper (900) faces 3 open-play on-target shots (PSxG 0.4/0.3/0.2), concedes 1 (the 0.4).
    TA keeper (901) faces 1 penalty (PSxG 0.75), concedes it. Both keepers have their own SPADL rows so
    the per-match ``player_id -> team_id_native`` / ``player_id_native`` maps resolve.
    """
    rows: list[dict[str, object]] = []
    aid = 0

    # The two keepers each appear as an actor once (so the maps carry them).
    rows.append(_mk(action_id=aid, type_id=_PASS, player_id=_GK_TB, player_id_native="GK_TB", team_id_native="TB"))
    aid += 1
    rows.append(_mk(action_id=aid, type_id=_PASS, player_id=_GK_TA, player_id_native="GK_TA", team_id_native="TA"))
    aid += 1

    # TA's 3 shots faced by TB's keeper 900. First is a goal.
    for psxg, res in ((0.4, _SUCCESS), (0.3, _FAIL), (0.2, _FAIL)):
        rows.append(
            _mk(
                action_id=aid,
                type_id=_SHOT,
                result_id=res,
                shot_blocked=False,
                player_id=20 + aid,  # the SHOOTER (team TA)
                player_id_native=f"SH{aid}",
                team_id_native="TA",
                defending_gk_player_id=_GK_TB,
                psxg_recalibrated=psxg,
            )
        )
        aid += 1

    # TB's penalty faced by TA's keeper 901. A goal.
    rows.append(
        _mk(
            action_id=aid,
            type_id=_SHOT_PENALTY,
            result_id=_SUCCESS,
            shot_blocked=False,
            player_id=50,
            player_id_native="SHP",
            team_id_native="TB",
            defending_gk_player_id=_GK_TA,
            psxg_recalibrated=0.75,
        )
    )
    aid += 1

    df = pd.DataFrame(rows)
    df["shot_blocked"] = df["shot_blocked"].astype("boolean")
    return df


def test_shot_stopping_columns_and_identity() -> None:
    out = _compute_shot_stopping_group(_match_actions())
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert not out.empty
    assert (out["data_source"] == "idsse").all()
    assert (out["match_id"] == "M1").all()
    assert (out["access_tier"] == "restricted").all()


def test_shot_stopping_grain_one_row_per_keeper_native_id() -> None:
    """One row per defending keeper, emitted with the NATIVE keeper id (for the dim_players join)."""
    out = _compute_shot_stopping_group(_match_actions())
    assert set(out["player_id"]) == {"GK_TB", "GK_TA"}
    assert out["player_id"].nunique() == len(out)
    # keeper team is the native defending team.
    tb = out[out["player_id"] == "GK_TB"].iloc[0]
    assert tb["team_id"] == "TB"


def test_shot_stopping_goals_prevented_identity() -> None:
    """goals_prevented == psxg_faced - goals_conceded (all shots); _excl_penalties excludes penalties."""
    out = _compute_shot_stopping_group(_match_actions())

    tb = out[out["player_id"] == "GK_TB"].iloc[0]
    assert tb["shots_faced"] == 3
    assert tb["goals_conceded"] == 1
    assert tb["psxg_faced"] == 0.9  # 0.4 + 0.3 + 0.2
    assert tb["goals_prevented"] == 0.9 - 1
    # TB keeper faced no penalties -> excl == incl.
    assert tb["shots_faced_excl_penalties"] == 3
    assert tb["goals_prevented_excl_penalties"] == 0.9 - 1

    ta = out[out["player_id"] == "GK_TA"].iloc[0]
    # TA keeper faced only 1 penalty -> excl-penalty companions are the empty-set floor (0 shots, GP 0).
    assert ta["shots_faced"] == 1
    assert ta["goals_conceded"] == 1
    assert ta["shots_faced_excl_penalties"] == 0
    assert ta["goals_prevented_excl_penalties"] == 0.0


def test_shot_stopping_report_census_conserves() -> None:
    """The ShotStoppingReport attribution census: attributed + unattributed == faced."""
    from ingestion.shot_stopping_writer import _derive_defending_gk_team, _score_shot_stopping

    prepped = _derive_defending_gk_team(_match_actions())
    _samples, report = _score_shot_stopping(prepped)
    assert report.n_shots_attributed + report.n_shots_unattributed == report.n_shots_faced
    assert report.n_shots_faced == 4  # 3 open-play + 1 penalty, all on-target


def test_shot_stopping_empty_returns_full_schema() -> None:
    """A match with only non-shot rows -> compute_shot_stopping's empty contract -> full-schema empty."""
    actions = _match_actions()
    passes_only = actions[actions["type_id"] == _PASS].copy()
    out = _compute_shot_stopping_group(passes_only)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty


def test_shot_stopping_metric_dtypes() -> None:
    """Count metric cols are Int64/long; psxg/goals_prevented are float64/double (sk contract)."""
    out = _compute_shot_stopping_group(_match_actions())
    assert out["psxg_faced"].dtype == np.float64
    assert out["goals_prevented"].dtype == np.float64
    assert str(out["shots_faced"].dtype) in ("Int64", "int64")


def test_shot_stopping_mart_matches_sk_columns() -> None:
    """Schema-drift guard (SK-EXPORT / METRIC_CONTRACTS registry, sk:ADR-098).

    The writer's ``_OUTPUT_TYPES`` must carry every sk metric column with zero per-package name
    knowledge (the uniform registry is the source of truth). The family's keys ('game_id','player_id')
    map to native identity (match_id + the native keeper player_id).
    """
    contract = METRIC_CONTRACTS["shot_stopping"]
    sk_metric_cols = set(contract["metric_columns"])
    assert sk_metric_cols.issubset(set(_OUTPUT_TYPES)), (
        f"writer missing sk metric columns: {sorted(sk_metric_cols - set(_OUTPUT_TYPES))}"
    )
    assert set(SS_KEYS) == {"game_id", "player_id"}
    # the writer emits native identity (match_id + native keeper player_id) + team_id + access_tier.
    assert {"match_id", "player_id", "team_id", "access_tier"}.issubset(set(_OUTPUT_TYPES))
