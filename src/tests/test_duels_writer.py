"""Unit tests for ``ingestion.duels_writer`` (P1 Task C2) on synthetic SPADL actions.

duels is a **STATEFUL Glicko-2** family (silly-kicks TF-55): ratings carry forward in ascending
``game_id`` across the whole corpus, so the writer is a SINGLE-DRIVER ORDERED PASS over all matches
(``compute_duel_ratings`` once over the full corpus, ascending ``game_id``), NOT a per-match
``applyInPandas`` (order matters; applyInPandas groups are unordered). This deviates from the bravery
template deliberately. These tests exercise the PURE core ``_score_all_matches`` / ``_stamp_identity`` on a
bounded two-match fixture; resume-equivalence is NOT relied on (the writer always does a full ordered pass).

Ground duels are extracted from a ``(tackle, take_on)`` adjacency (cross-team, within 5s, exactly one
succeeding) — the fixture below builds such pairs. The 6 ``DUEL_METRIC_COLUMNS`` + the
``duel_winner_source`` provenance are pinned to ``METRIC_CONTRACTS['duels']`` via the parity test.

duels is per-PLAYER evaluative (a per-player ground-duel rating) — ``wf-duels`` is a member of
``PER_PLAYER_EVALUATIVE_CARDS`` (asserted in ``test_ai_governance_md.py``).
"""

from __future__ import annotations

import pandas as pd
import silly_kicks.spadl.config as spadlconfig
from silly_kicks.duels import DUEL_METRIC_COLUMNS
from silly_kicks.metric_contracts import METRIC_CONTRACTS

from ingestion.duels_writer import (
    _OUTPUT_TYPES,
    OUTPUT_COLUMNS,
    _score_all_matches,
    _stamp_identity,
)

_TACKLE = spadlconfig.actiontype_id["tackle"]
_TAKE_ON = spadlconfig.actiontype_id["take_on"]
_SUCCESS = spadlconfig.result_id["success"]
_FAIL = spadlconfig.result_id["fail"]
_FOOT = spadlconfig.bodypart_id["foot"]


def _duel_pair(aid: int, game_id: int, t: float, winner_is_tackler: bool) -> list[dict[str, object]]:
    """A (tackle, take_on) adjacency: cross-team, within 5s, exactly one succeeds -> a derived duel."""
    tackler = "P1"  # team TA
    dribbler = "P2"  # team TB
    tackle = {
        "game_id": game_id,
        "action_id": aid,
        "period_id": 1,
        "time_seconds": t,
        "team_id_native": "TA",
        "player_id_native": tackler,
        "type_id": _TACKLE,
        "result_id": _SUCCESS if winner_is_tackler else _FAIL,
        "bodypart_id": _FOOT,
        "start_x": 50.0,
        "start_y": 34.0,
        "end_x": 50.0,
        "end_y": 34.0,
    }
    takeon = {
        "game_id": game_id,
        "action_id": aid + 1,
        "period_id": 1,
        "time_seconds": t + 0.5,
        "team_id_native": "TB",
        "player_id_native": dribbler,
        "type_id": _TAKE_ON,
        "result_id": _FAIL if winner_is_tackler else _SUCCESS,
        "bodypart_id": _FOOT,
        "start_x": 50.0,
        "start_y": 34.0,
        "end_x": 52.0,
        "end_y": 34.0,
    }
    return [tackle, takeon]


def _match(game_id: int, tackler_wins: int, dribbler_wins: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    aid = 0
    t = 0.0
    for _ in range(tackler_wins):
        rows.extend(_duel_pair(aid, game_id, t, winner_is_tackler=True))
        aid += 2
        t += 30.0
    for _ in range(dribbler_wins):
        rows.extend(_duel_pair(aid, game_id, t, winner_is_tackler=False))
        aid += 2
        t += 30.0
    df = pd.DataFrame(rows)
    df["data_source"] = "skillcorner"
    df["match_id_native"] = f"M{game_id}"
    df["access_tier"] = "restricted"
    return df


def _two_match_corpus() -> pd.DataFrame:
    # game 1: P1 wins 4, P2 wins 1; game 2: P1 wins 3, P2 wins 2. Ratings carry forward across games.
    return pd.concat([_match(1, 4, 1), _match(2, 3, 2)], ignore_index=True)


def test_duels_ordered_pass_produces_metric_columns() -> None:
    """The full ordered pass yields the 6 metric cols + duel_winner_source per (game, player)."""
    samples, report = _score_all_matches(_two_match_corpus())
    assert set(DUEL_METRIC_COLUMNS).issubset(samples.columns)
    assert "duel_winner_source" in samples.columns
    # both players appear in both games (they contested duels each game)
    assert set(samples["player_id"]) == {"P1", "P2"}
    assert report.n_matches == 2


def test_duels_ratings_carry_forward() -> None:
    """Glicko-2 is stateful: the dominant duel-winner's rating rises above 1500 by game 2."""
    samples, _report = _score_all_matches(_two_match_corpus())
    g2_p1 = samples[(samples["game_id"] == 2) & (samples["player_id"] == "P1")]
    assert len(g2_p1) == 1
    # P1 won the majority of duels across both games -> rating above the 1500 default seed.
    assert g2_p1["duel_rating"].iloc[0] > 1500.0


def test_duels_census_conserves() -> None:
    """DuelRatingReport census: contested == won + lost per row; n_duels accounted."""
    samples, report = _score_all_matches(_two_match_corpus())
    assert (samples["duels_contested"] == samples["duels_won"] + samples["duels_lost"]).all()
    assert report.n_duels >= 1


def test_duels_stamp_identity_and_columns() -> None:
    """Native identity + access_tier stamped; exact OUTPUT_COLUMNS order; team resolved from map."""
    corpus = _two_match_corpus()
    samples, _report = _score_all_matches(corpus)
    out = _stamp_identity(samples, corpus)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert (out["data_source"] == "skillcorner").all()
    assert set(out["match_id"]) == {"M1", "M2"}
    assert (out["access_tier"] == "restricted").all()
    # P1 -> team TA, P2 -> team TB
    p1 = out[out["player_id"] == "P1"]
    assert (p1["team_id"] == "TA").all()


def test_duels_registry_parity() -> None:
    """Schema-drift guard (SK-EXPORT / METRIC_CONTRACTS registry). 6 metric + 2 keys."""
    contract = METRIC_CONTRACTS["duels"]
    assert len(contract["metric_columns"]) == 6
    assert set(contract["keys"]) == {"game_id", "player_id"}
    assert set(contract["metric_columns"]).issubset(set(_OUTPUT_TYPES))


def test_duels_empty_returns_full_schema() -> None:
    """A corpus with no duels yields an empty frame carrying the full OUTPUT_COLUMNS schema."""
    corpus = _two_match_corpus()
    samples, _report = _score_all_matches(corpus)
    out = _stamp_identity(samples.iloc[0:0], corpus)
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert out.empty
