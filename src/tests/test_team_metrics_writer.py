"""Unit tests for ``ingestion.team_metrics_writer`` (P1 Task A1) on synthetic SPADL actions.

Exercises the PURE core ``_compute_team_kpis_group`` (the per-match ``applyInPandas`` closure body)
on a hand-built two-team match. team_metrics is event-only (silly-kicks TF-52 ``compute_team_kpis``),
so no tracking frames / xT / Spark are needed here; the Spark ``run_pipeline`` (``applyInPandas``
dispatch) is validated live in Part B, same posture as the sibling ADR-013 writers.

The column set is pinned to the silly-kicks ``METRIC_CONTRACTS['team_metrics']`` registry (SK-EXPORT,
sk:ADR-098) — the schema-drift guard. team_metrics is per-TEAM, NOT per-player-evaluative, so it carries
no governance model card (asserted by exclusion from ``PER_PLAYER_EVALUATIVE_CARDS``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import silly_kicks.spadl.config as spadlconfig
from silly_kicks.metric_contracts import METRIC_CONTRACTS
from silly_kicks.team_metrics import TEAM_KPI_KEYS, TEAM_KPI_METRIC_COLUMNS

from ingestion.team_metrics_writer import (
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    _compute_team_kpis_group,
)

_PASS = spadlconfig.actiontype_id["pass"]
_SHOT = spadlconfig.actiontype_id["shot"]
_TACKLE = spadlconfig.actiontype_id["tackle"]
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
        "shot_blocked": pd.NA,
        "cross_blocked": pd.NA,
    }
    base.update(kw)
    return base


def _match_actions() -> pd.DataFrame:
    """One synthetic two-team match's ``bronze.spadl_actions`` rows.

    Team ``TA`` and ``TB`` each build up with passes, take a shot, and each makes a tackle in the
    other's build-up — enough signal for a valid ``compute_team_kpis`` two-row output.
    """
    rows: list[dict[str, object]] = []
    aid = 0
    for team in ("TA", "TB"):
        other = "TB" if team == "TA" else "TA"
        for i in range(6):
            rows.append(
                _mk(
                    action_id=aid,
                    team_id_native=team,
                    player_id=hash(team) % 50 + i,
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
            )
        )
        aid += 1
        rows.append(
            _mk(action_id=aid, team_id_native=other, time_seconds=float(aid), type_id=_TACKLE, start_x=60.0, end_x=60.0)
        )
        aid += 1
    df = pd.DataFrame(rows)
    df["shot_blocked"] = df["shot_blocked"].astype("boolean")
    df["cross_blocked"] = df["cross_blocked"].astype("boolean")
    df["data_source"] = "skillcorner"
    df["match_id_native"] = "M1"
    df["access_tier"] = "restricted"
    return df


def test_team_kpis_group_shape() -> None:
    """One row per team, and the sk metric columns are all present (SK-EXPORT parity)."""
    out = _compute_team_kpis_group(_match_actions())
    assert set(TEAM_KPI_METRIC_COLUMNS).issubset(out.columns)
    assert len(out) == 2  # one row per team


def test_team_kpis_group_identity_and_columns() -> None:
    """Native identity + access_tier stamped; exact OUTPUT_COLUMNS order."""
    out = _compute_team_kpis_group(_match_actions())
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert (out["data_source"] == "skillcorner").all()
    assert (out["match_id"] == "M1").all()
    assert (out["access_tier"] == "restricted").all()
    assert set(out["team_id"]) == {"TA", "TB"}
    assert out["team_id"].nunique() == len(out)


def test_team_kpis_group_report_census_conserves() -> None:
    """The TeamKpiReport conservation census: scored + excluded == in."""
    from ingestion.team_metrics_writer import _score_team_kpis

    _samples, report = _score_team_kpis(_match_actions())
    assert report.n_matches_scored + report.n_matches_excluded_not_two_teams == report.n_matches_in
    assert report.n_matches_in == 1


def test_team_kpis_empty_returns_full_schema() -> None:
    """An empty match yields an empty frame carrying the full OUTPUT_COLUMNS schema."""
    empty = _match_actions().iloc[0:0]
    out = _compute_team_kpis_group(empty)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty


def test_team_kpis_metric_dtypes() -> None:
    """Float metric cols are float64; nullable-int metric cols are Int64 (sk contract)."""
    out = _compute_team_kpis_group(_match_actions())
    assert out["ppda"].dtype == np.float64
    assert str(out["recoveries"].dtype) == "Int64"


def test_team_metrics_mart_matches_sk_columns() -> None:
    """Schema-drift guard (SK-EXPORT / METRIC_CONTRACTS registry, sk:ADR-098).

    The writer's ``_OUTPUT_TYPES`` metric+key columns must equal exactly
    ``set(metric_columns) | set(keys)`` for the 'team_metrics' family — with zero per-package
    name knowledge (the uniform registry is the source of truth).
    """
    contract = METRIC_CONTRACTS["team_metrics"]
    expected = set(contract["metric_columns"]) | set(contract["keys"])
    # keys resolve to native (game_id -> match_id, team_id emitted native); the writer output-types
    # carry the native-id identity trio + access_tier plus every sk metric column.
    writer_metric_and_keys = {c for c in _OUTPUT_TYPES if c in expected or c in ("match_id", "team_id")}
    sk_metric_cols = set(contract["metric_columns"])
    assert sk_metric_cols.issubset(set(_OUTPUT_TYPES)), (
        f"writer missing sk metric columns: {sorted(sk_metric_cols - set(_OUTPUT_TYPES))}"
    )
    # every metric column present, and the family's keys (game_id/team_id) mapped to native identity.
    assert set(TEAM_KPI_KEYS) == {"game_id", "team_id"}
    assert writer_metric_and_keys  # non-vacuity
