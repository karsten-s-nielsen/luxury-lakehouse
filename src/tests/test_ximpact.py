"""sk4123 xImpact (TF-63 ride-along, SK-XT-COUNTS PR commit 2): the ``win_prob_leverage`` + ``ximpact``
columns on ``fct_action_values``, computed by ``ingestion.spadl_vaep._score_ximpact``.

``ximpact = VAEP_adjusted x dP(win | goal at the pre-action state)`` (Paul/Klemp/Memmert 2025); the
leverage weight is ``silly_kicks.win_probability.goal_leverage``. These tests pin, without Spark:

  * ximpact == VAEP_adjusted x win_prob_leverage (the multiply + args wiring, XIMP-01/02);
  * leverage is state-sensitive (non-vacuity) and within [-1, 1];
  * an unresolved WP state (single-team group) propagates NaN to BOTH leverage and ximpact (honest-NaN);
  * ``games.home_team_id`` is resolved from the ``team_id_native == home_team_id_native`` mapping in the
    data (provider-agnostic, ADR-016) — a resolved leverage is the guard;
  * end-to-end byte-consistency with ``VAEP.rate_ximpact`` (XIMP-02).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("silly_kicks")
pytest.importorskip("xgboost")

from ingestion.spadl_vaep import (
    _reconstruct_fitted_vaep,
    _score_actions_adjusted,
    _score_ximpact,
)
from tests.test_vaep_adjusted_writer import (
    _FEATURE_FNS,
    _NB_PREV_ACTIONS,
    _boosters,
    _fit_reference_vaep,
    _synthetic_game,
)

_HOME = "home_club_native"
_AWAY = "away_club_native"
_HOME_TID = 1001
_AWAY_TID = 1002


def _wp():
    from silly_kicks.win_probability import WinProbabilityModel

    return WinProbabilityModel.bundled()


def _game_with_home() -> pd.DataFrame:
    """The Phase-D synthetic game, carrying the ``team_id`` ↔ ``team_id_native`` mapping + a
    ``home_team_id_native`` — so ``_score_ximpact`` resolves the home ``team_id`` from the data."""
    a = _synthetic_game().copy()
    a["team_id"] = np.where(a["team_id"] == 1, _HOME_TID, _AWAY_TID)
    a["team_id_native"] = np.where(a["team_id"] == _HOME_TID, _HOME, _AWAY)
    a["home_team_id_native"] = _HOME
    return a


def _adjusted_stub(vaep_values: np.ndarray) -> pd.DataFrame:
    """A minimal ``rate_adjusted``-shaped frame (carries ``vaep_value``) for the wiring tests."""
    n = len(vaep_values)
    return pd.DataFrame(
        {"xsuccess": np.full(n, 0.6), "offensive_value": 0.0, "defensive_value": 0.0, "vaep_value": vaep_values}
    )


def test_ximpact_equals_adjusted_times_leverage() -> None:
    a = _game_with_home()
    adj = _adjusted_stub(np.linspace(-0.1, 0.2, len(a)))
    leverage, ximpact = _score_ximpact(a, adj, _wp())
    expected = adj["vaep_value"].to_numpy() * leverage.to_numpy()
    assert np.allclose(ximpact.to_numpy(), expected, equal_nan=True)


def test_leverage_is_state_sensitive() -> None:
    """Non-vacuity: leverage responds to the game state (not a constant). A constant would fail."""
    a = _game_with_home()
    leverage, _ = _score_ximpact(a, _adjusted_stub(np.zeros(len(a))), _wp())
    finite = leverage.to_numpy()[~np.isnan(leverage.to_numpy())]
    assert finite.size > 1
    assert float(np.nanstd(finite)) > 0.0, "leverage is constant across states — not state-sensitive"


def test_leverage_within_unit_range() -> None:
    a = _game_with_home()
    leverage, _ = _score_ximpact(a, _adjusted_stub(np.zeros(len(a))), _wp())
    finite = leverage.to_numpy()[~np.isnan(leverage.to_numpy())]
    assert finite.size > 0
    assert (finite >= -1.0 - 1e-9).all() and (finite <= 1.0 + 1e-9).all()


def test_unresolved_state_propagates_nan() -> None:
    """A single-team group is excluded by win_probability (not two teams) → NaN leverage AND NaN ximpact,
    never 0-filled (the honest-NaN control)."""
    a = _game_with_home()
    a = a[a["team_id"] == _HOME_TID].reset_index(drop=True)
    adj = _adjusted_stub(np.full(len(a), 0.1))
    leverage, ximpact = _score_ximpact(a, adj, _wp())
    assert leverage.isna().all()
    assert ximpact.isna().all()


def test_home_team_id_resolved_from_native_mapping() -> None:
    """The home ``team_id`` is resolved from ``team_id_native == home_team_id_native`` → states resolve →
    some non-NaN leverage. Omitting the mapping (no ``team_id_native``) would leave the home flag unset."""
    a = _game_with_home()
    leverage, _ = _score_ximpact(a, _adjusted_stub(np.zeros(len(a))), _wp())
    assert leverage.notna().any()


def test_sentinel_team_action_does_not_exclude_the_match() -> None:
    """ADR-016 UNKNOWN_TEAM_SENTINEL actions (a NULL native team hashed to a stable sentinel) present a
    spurious THIRD team_id. Without excluding them from the WP frame, compute_win_probability's
    nteams==2 gate drops the ENTIRE match — the GS regression: all 64 GS matches carry two real teams
    plus the sentinel, so every action came back NaN. _score_ximpact must feed goal_leverage only the
    real-team actions: real-team leverage resolves, the sentinel row stays honest-NaN.
    """
    from silly_kicks.win_probability import goal_leverage

    from ingestion.spadl_adapter import UNKNOWN_TEAM_SENTINEL, hash_native_id_to_bigint

    a = _game_with_home()
    # A non-shot sentinel action with an unresolvable native team → a 3rd team_id in the match.
    sentinel = a.iloc[[len(a) // 2]].copy()
    sentinel["action_id"] = int(a["action_id"].max()) + 1  # unique — WP maps state by (game_id, action_id)
    sentinel["team_id_native"] = UNKNOWN_TEAM_SENTINEL
    sentinel["team_id"] = hash_native_id_to_bigint(UNKNOWN_TEAM_SENTINEL)
    if "type_id" in sentinel:
        sentinel["type_id"] = 8  # foul — a non-scoring type (mirrors production sentinels: tackle/foul)
    a3 = pd.concat([a, sentinel], ignore_index=True)

    # Non-vacuity: bare goal_leverage over the 3-team frame excludes the whole match (all-NaN).
    games = pd.DataFrame([{"game_id": a3["game_id"].iloc[0], "home_team_id": _HOME_TID}])
    bare = goal_leverage(a3, model=_wp(), games=games)
    assert bare.isna().all(), (
        "expected the sk nteams==2 gate to exclude a 3-team match (guard would be vacuous otherwise)"
    )

    # The fix: excluding the sentinel presents two teams → real-team leverage resolves; sentinel stays NaN.
    leverage, ximpact = _score_ximpact(a3, _adjusted_stub(np.zeros(len(a3))), _wp())
    sentinel_mask = (a3["team_id_native"] == UNKNOWN_TEAM_SENTINEL).to_numpy()
    assert leverage.to_numpy()[sentinel_mask].size == 1
    assert np.isnan(leverage.to_numpy()[sentinel_mask]).all(), "sentinel action must be honest-NaN"
    assert np.isnan(ximpact.to_numpy()[sentinel_mask]).all()
    assert np.isfinite(leverage.to_numpy()[~sentinel_mask]).any(), (
        "real-team leverage must resolve after excluding the sentinel"
    )


def test_writer_ximpact_matches_vaep_rate_ximpact() -> None:
    """XIMP-02 end-to-end: the writer's ximpact == VAEP.rate_ximpact(...) on the same game — pins the
    full args wiring + index alignment, not just the multiply."""
    from silly_kicks.win_probability import WinProbabilityModel
    from silly_kicks.xsuccess import XSuccessModel

    a = _game_with_home()
    ref = _fit_reference_vaep(a)
    recon = _reconstruct_fitted_vaep(*_boosters(ref), _FEATURE_FNS, _NB_PREV_ACTIONS)
    xs = XSuccessModel.bundled()
    adj = _score_actions_adjusted(a, recon, xs)
    _, ximpact = _score_ximpact(a, adj, WinProbabilityModel.bundled())

    games = pd.DataFrame([{"game_id": a["game_id"].iloc[0], "home_team_id": _HOME_TID}])
    ref_ximpact = recon.rate_ximpact(
        pd.Series(dtype="object"), a, xs, win_prob_model=WinProbabilityModel.bundled(), games=games
    )
    assert np.allclose(ximpact.to_numpy(), ref_ximpact.to_numpy(), equal_nan=True)
