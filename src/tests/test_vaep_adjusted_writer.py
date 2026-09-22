"""Phase D (sk4118) — VAEP_adjusted (TF-61) + xSuccess on the VAEP scoring path.

silly-kicks 4.116/4.121: ``VAEP.rate_adjusted`` (Paul/Klemp/Memmert 2025) re-scores an
already-fitted STANDARD (result-bearing) VAEP on a surgical result-feature counterfactual,
weighted by a per-action completion probability from ``XSuccessModel`` — outcome-bias-free,
**no retrain**. The lakehouse scoring UDF ships the two fitted XGBoost boosters as raw bytes,
so it reconstructs a ``VAEP`` around them (``_reconstruct_fitted_vaep``) and calls the
sk-canonical ``rate_adjusted`` (``_score_actions_adjusted``).

These tests exercise the pure, Spark-free reconstruction + adjusted-scoring helpers:
  * reconstruction reproduces the manual raw scoring path byte-identical (parity == the guard
    that the name-mangled model injection wired correctly + fails loud if sk renames it);
  * a result-free (HybridVAEP-like) xfn set makes ``rate_adjusted`` raise (non-vacuity);
  * a NaN xSuccess propagates to NaN adjusted values (never fabricated);
  * adjusted values differ from raw (the counterfactual is not a no-op);
  * the helper emits exactly the four additive columns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# silly-kicks + xgboost are required for these tests; skip cleanly where unavailable.
pytest.importorskip("silly_kicks")
pytest.importorskip("xgboost")

from ingestion.spadl_vaep import (
    _NB_PREV_ACTIONS,
    _reconstruct_fitted_vaep,
    _score_actions_adjusted,
    _vaep_feature_fns,
)

# The canonical 11-fn feature set the shipped boosters were trained on (SSOT lives in spadl_vaep).
_FEATURE_FNS = _vaep_feature_fns()


def _synthetic_game(n: int = 80, seed: int = 7) -> pd.DataFrame:
    """A single valid SPADL game: alternating teams, forward progression, a few scoring shots.

    Carries every column the 11 lakehouse VAEP feature functions + ``compute_labels`` +
    ``add_names`` read (ids drawn from the sk SPADL vocab so ``add_names`` resolves).
    """
    from silly_kicks.spadl import config as cfg

    rng = np.random.default_rng(seed)
    pass_id = cfg.actiontype_id["pass"]
    dribble_id = cfg.actiontype_id["dribble"]
    shot_id = cfg.actiontype_id["shot"]
    succ = cfg.result_id["success"]
    fail = cfg.result_id["fail"]
    foot = cfg.bodypart_id["foot"]

    rows = []
    x = 20.0
    t = 0.0
    for i in range(n):
        team = 1 + (i // 4) % 2  # switch possession every 4 actions
        # Occasionally a shot near the box that scores (drives non-trivial labels).
        is_shot = i % 17 == 16
        type_id = shot_id if is_shot else (dribble_id if i % 3 == 0 else pass_id)
        result_id = succ if (is_shot or rng.random() < 0.75) else fail
        start_x = float(np.clip(x, 1.0, 104.0))
        start_y = float(rng.uniform(5.0, 63.0))
        end_x = float(np.clip(start_x + (12.0 if is_shot else rng.uniform(-3.0, 9.0)), 1.0, 104.0))
        end_y = float(np.clip(start_y + rng.uniform(-8.0, 8.0), 1.0, 67.0))
        rows.append(
            {
                "game_id": 1,
                "period_id": 1 if t < 2700 else 2,
                "time_seconds": t,
                "team_id": team,
                "player_id": 1000 + (i % 11) + team * 100,
                "start_x": start_x,
                "start_y": start_y,
                "end_x": end_x,
                "end_y": end_y,
                "type_id": type_id,
                "result_id": result_id,
                "bodypart_id": foot,
                "action_id": i,
            }
        )
        x = end_x if result_id == succ else 105.0 - end_x  # turnover flips the field
        t += float(rng.uniform(1.5, 6.0))
    return pd.DataFrame(rows)


def _fit_reference_vaep(actions: pd.DataFrame):
    """Fit a real VAEP on the synthetic game using the lakehouse's exact 11-fn xfn set."""
    from silly_kicks.vaep import VAEP

    v = VAEP(xfns=list(_FEATURE_FNS), nb_prev_actions=_NB_PREV_ACTIONS)
    game = pd.Series(dtype="object")
    x = v.compute_features(game, actions)
    y = v.compute_labels(game, actions)
    # val_size>0: the lakehouse xgboost learner always enables early stopping, which requires an
    # eval set (val_size=0 -> "Must have at least 1 validation dataset for early stopping").
    v.fit(x, y, val_size=0.25, random_state=0)
    return v


def _boosters(v):
    """Extract the two fitted XGBClassifier boosters from a fitted VAEP (scores, concedes)."""
    models = v._VAEP__models
    return models["scores"], models["concedes"]


def _manual_raw_path(actions: pd.DataFrame, model_scores, model_concedes) -> pd.DataFrame:
    """Replicate the scoring UDF's raw offensive/defensive/vaep computation exactly."""
    import silly_kicks.spadl as spadl
    import silly_kicks.vaep.features as fs
    import silly_kicks.vaep.formula as formula

    named = spadl.add_names(actions)
    gamestates = fs.gamestates(named, nb_prev_actions=_NB_PREV_ACTIONS)
    x_game = pd.concat([fn(gamestates) for fn in _FEATURE_FNS], axis=1)
    p_scores = pd.Series(model_scores.predict_proba(x_game)[:, 1])
    p_concedes = pd.Series(model_concedes.predict_proba(x_game)[:, 1])
    return formula.value(named, p_scores, p_concedes)


class _StubXSuccess:
    """Deterministic xSuccess model returning a caller-supplied per-action probability array."""

    def __init__(self, values: np.ndarray) -> None:
        self._values = values

    def predict_success(self, actions: pd.DataFrame) -> np.ndarray:
        return np.asarray(self._values, dtype=float)[: len(actions)]


def test_reconstructed_vaep_reproduces_raw_path_byte_identical() -> None:
    """Parity: the reconstructed VAEP's rate() == the UDF's manual raw scoring path.

    This is the Hyrum's-law guard — if sk renames the private ``__models`` store, the
    reconstruction produces NotFittedError / different values and this test fails loud.
    """
    actions = _synthetic_game()
    ref = _fit_reference_vaep(actions)
    scores, concedes = _boosters(ref)

    recon = _reconstruct_fitted_vaep(scores, concedes, _FEATURE_FNS, _NB_PREV_ACTIONS)
    game = pd.Series(dtype="object")
    recon_rate = recon.rate(game, actions)
    manual = _manual_raw_path(actions, scores, concedes)

    for col in ("offensive_value", "defensive_value", "vaep_value"):
        np.testing.assert_allclose(recon_rate[col].to_numpy(), manual[col].to_numpy(), rtol=0, atol=1e-9)


def test_score_actions_adjusted_returns_four_finite_columns() -> None:
    actions = _synthetic_game()
    ref = _fit_reference_vaep(actions)
    recon = _reconstruct_fitted_vaep(*_boosters(ref), _FEATURE_FNS, _NB_PREV_ACTIONS)
    xs = _StubXSuccess(np.full(len(actions), 0.6))

    out = _score_actions_adjusted(actions, recon, xs)

    assert list(out.columns) == ["xsuccess", "offensive_value", "defensive_value", "vaep_value"]
    assert len(out) == len(actions)
    assert np.isfinite(out.to_numpy()).all()


def test_adjusted_differs_from_raw() -> None:
    """The surgical-result counterfactual + completion weighting is not a no-op."""
    actions = _synthetic_game()
    ref = _fit_reference_vaep(actions)
    scores, concedes = _boosters(ref)
    recon = _reconstruct_fitted_vaep(scores, concedes, _FEATURE_FNS, _NB_PREV_ACTIONS)

    raw = _manual_raw_path(actions, scores, concedes)
    adj = _score_actions_adjusted(actions, recon, _StubXSuccess(np.full(len(actions), 0.6)))

    assert not np.allclose(adj["vaep_value"].to_numpy(), raw["vaep_value"].to_numpy())


def test_nan_xsuccess_propagates_to_nan_adjusted() -> None:
    """A non-finite xSuccess yields a NaN adjusted value — never a fabricated number."""
    actions = _synthetic_game()
    ref = _fit_reference_vaep(actions)
    recon = _reconstruct_fitted_vaep(*_boosters(ref), _FEATURE_FNS, _NB_PREV_ACTIONS)

    xs_vals = np.full(len(actions), 0.5)
    xs_vals[3] = np.nan
    out = _score_actions_adjusted(actions, recon, _StubXSuccess(xs_vals))

    assert np.isnan(out["vaep_value"].to_numpy()[3])
    assert np.isnan(out["offensive_value"].to_numpy()[3])
    assert np.isnan(out["xsuccess"].to_numpy()[3])
    # The VAEP formula is a per-possession delta, so the NaN also poisons the immediate successor
    # (index 4, whose delta reads index 3) — that IS correct propagation. Rows away from the NaN
    # stay finite (the NaN is not fabricated across the whole game).
    assert np.isfinite(out["vaep_value"].to_numpy()[[0, 1, 2]]).all()
    assert np.isfinite(out["vaep_value"].to_numpy()[10:]).all()


def test_rate_adjusted_on_result_free_vaep_raises() -> None:
    """Non-vacuity guard: a result-free (HybridVAEP-like) xfn set makes the flip a no-op → raise."""
    import silly_kicks.vaep.features as fs

    actions = _synthetic_game()
    # A result-FREE feature set (no result_onehot) — forcing result_id changes no feature.
    result_free_fns = [fs.actiontype_onehot, fs.bodypart_onehot, fs.startlocation, fs.endlocation]
    ref = _fit_reference_vaep(actions)  # boosters are irrelevant; the flip check runs first
    recon = _reconstruct_fitted_vaep(*_boosters(ref), result_free_fns, _NB_PREV_ACTIONS)

    with pytest.raises(ValueError, match="result"):
        _score_actions_adjusted(actions, recon, _StubXSuccess(np.full(len(actions), 0.6)))
