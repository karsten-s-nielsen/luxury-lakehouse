"""Unit tests for ``ingestion.match_outcome_writer`` (P1 Task A2) on synthetic SPADL actions + xG.

Exercises the PURE core ``_compute_match_outcome_group`` (the per-match ``applyInPandas`` closure body)
on a hand-built two-team match with an injected per-shot xG column. match_outcome is event-only
(silly-kicks TF-53 ``compute_match_outcome``) but REQUIRES an ``xg_column`` (no default) — the win /
draw / loss simplex + xPoints are integrated from the per-shot xG. Default ``MatchOutcomeParams()`` runs
both honesty corrections ON (possession-collapse + Dixon-Coles dependence, sk:ADR-097).

The column set is pinned to the silly-kicks ``METRIC_CONTRACTS['match_outcome']`` registry (SK-EXPORT).
match_outcome is per-TEAM, NOT per-player-evaluative — no governance model card.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import silly_kicks.spadl.config as spadlconfig
from silly_kicks.match_outcome import MATCH_OUTCOME_KEYS, MATCH_OUTCOME_METRIC_COLUMNS
from silly_kicks.metric_contracts import METRIC_CONTRACTS

from ingestion.match_outcome_writer import (
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    XG_COLUMN,
    _compute_match_outcome_group,
)

_PASS = spadlconfig.actiontype_id["pass"]
_SHOT = spadlconfig.actiontype_id["shot"]
_SUCCESS = spadlconfig.result_id["success"]
_FAIL = spadlconfig.result_id["fail"]
_FOOT = spadlconfig.bodypart_id["foot"]


def _mk(**kw: object) -> dict[str, object]:
    base: dict[str, object] = {
        "game_id": 1,
        "action_id": 0,
        "period_id": 1,
        "time_seconds": 0.0,
        "team_id_native": "TA",
        "player_id": 100,
        "start_x": 50.0,
        "start_y": 34.0,
        "end_x": 60.0,
        "end_y": 34.0,
        "type_id": _PASS,
        "result_id": _SUCCESS,
        "bodypart_id": _FOOT,
        XG_COLUMN: np.nan,
    }
    base.update(kw)
    return base


def _match_actions() -> pd.DataFrame:
    """One synthetic two-team match with a shot each, xG injected per shot."""
    rows: list[dict[str, object]] = []
    aid = 0
    for team in ("TA", "TB"):
        for i in range(4):
            rows.append(
                _mk(
                    action_id=aid,
                    team_id_native=team,
                    time_seconds=float(aid),
                    type_id=_PASS,
                    start_x=40.0 + i,
                    end_x=50.0 + i,
                )
            )
            aid += 1
        rows.append(
            _mk(
                action_id=aid,
                team_id_native=team,
                time_seconds=float(aid),
                type_id=_SHOT,
                result_id=_SUCCESS if team == "TA" else _FAIL,
                start_x=95.0,
                end_x=105.0,
                **{XG_COLUMN: 0.35 if team == "TA" else 0.15},
            )
        )
        aid += 1
    df = pd.DataFrame(rows)
    df["data_source"] = "statsbomb"
    df["match_id_native"] = "M1"
    df["access_tier"] = "public"
    return df


def test_match_outcome_group_simplex_and_xpoints() -> None:
    """A two-team match yields a valid simplex (p_win + p_draw + p_loss ~ 1) and xpoints = 3*p_win + p_draw."""
    out = _compute_match_outcome_group(_match_actions())
    assert len(out) == 2
    for _, r in out.iterrows():
        assert abs(float(r["p_win"]) + float(r["p_draw"]) + float(r["p_loss"]) - 1.0) < 1e-6
        assert abs(float(r["xpoints"]) - (3.0 * float(r["p_win"]) + float(r["p_draw"]))) < 1e-6


def test_match_outcome_group_identity_and_columns() -> None:
    """Native identity + access_tier stamped; exact OUTPUT_COLUMNS order; sk metric cols present."""
    out = _compute_match_outcome_group(_match_actions())
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert set(MATCH_OUTCOME_METRIC_COLUMNS).issubset(out.columns)
    assert (out["data_source"] == "statsbomb").all()
    assert (out["match_id"] == "M1").all()
    assert (out["access_tier"] == "public").all()
    assert set(out["team_id"]) == {"TA", "TB"}


def test_match_outcome_report_census_conserves() -> None:
    """MatchOutcomeReport conservation: scored + excluded == in."""
    from ingestion.match_outcome_writer import _score_match_outcome

    _samples, report = _score_match_outcome(_match_actions())
    assert report.n_matches_scored + report.n_matches_excluded_not_two_teams == report.n_matches_in
    assert report.n_matches_in == 1


def test_match_outcome_empty_returns_full_schema() -> None:
    out = _compute_match_outcome_group(_match_actions().iloc[0:0])
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty


def test_match_outcome_mart_matches_sk_columns() -> None:
    """Schema-drift guard: writer metric columns == METRIC_CONTRACTS['match_outcome'].metric_columns."""
    contract = METRIC_CONTRACTS["match_outcome"]
    sk_metric_cols = set(contract["metric_columns"])
    assert sk_metric_cols == {"p_win", "p_draw", "p_loss", "xpoints", "expected_goals"}
    assert sk_metric_cols.issubset(set(_OUTPUT_TYPES))
    assert set(MATCH_OUTCOME_KEYS) == {"game_id", "team_id"}
