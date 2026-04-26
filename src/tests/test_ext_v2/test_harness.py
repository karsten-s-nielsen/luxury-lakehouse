"""Tests for the ExT v2 Phase 0 Optuna harness.

Phase 0 has no Optuna axes active — the harness runs a single trial that
fits ``SinghProducer`` on the train fold and computes held-out NLL on the
pass-only subset of the holdout fold (per locked design decision A:
single-source ``fct_action_values``, NLL evaluated on ``action_type='pass'``).

The shape exists end-to-end so Phases 1-4 plug in axes via
``trial.suggest_*`` calls without restructuring the harness.
"""

from __future__ import annotations

import math

import numpy as np
import optuna
import pandas as pd
import pytest

from analytics.expected_threat import XTGrid
from analytics.ext_v2.harness import (
    Phase0Result,
    objective,
    run_phase0_harness,
)
from analytics.ext_v2.transition import SINGH_MOVE_TYPES, GridSpec

_SHOT_TYPES = ("shot", "shot_freekick", "shot_penalty")


def _make_actions_with_competitions(
    *,
    n_per_match: int = 200,
    matches_per_comp: int = 50,
    n_comps: int = 4,
    seed: int = 0,
) -> pd.DataFrame:
    """Build synthetic actions across several competitions and matches.

    Action type distribution: ~95% moves (across SINGH_MOVE_TYPES), ~5% shots.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for comp_idx in range(n_comps):
        comp_id = str(100 + comp_idx)
        for match_idx in range(matches_per_comp):
            match_key = comp_idx * 10_000 + match_idx
            types = rng.choice(
                [*SINGH_MOVE_TYPES, *_SHOT_TYPES],
                size=n_per_match,
                p=([0.95 / len(SINGH_MOVE_TYPES)] * len(SINGH_MOVE_TYPES))
                + ([0.05 / len(_SHOT_TYPES)] * len(_SHOT_TYPES)),
            )
            results = rng.choice(["success", "fail"], size=n_per_match, p=[0.7, 0.3])
            rows.append(
                pd.DataFrame(
                    {
                        "competition_id": [comp_id] * n_per_match,
                        "match_key": [match_key] * n_per_match,
                        "type_name": types,
                        "result_name": results,
                        "action_type": types,  # mirror of type_name; v1 ingest aliases
                        "start_x": rng.uniform(0, 105.0, n_per_match),
                        "start_y": rng.uniform(0, 68.0, n_per_match),
                        "end_x": rng.uniform(0, 105.0, n_per_match),
                        "end_y": rng.uniform(0, 68.0, n_per_match),
                    }
                )
            )
    return pd.concat(rows, ignore_index=True)


# ---------------------------------------------------------------------------
# objective — Phase 0 has no axes
# ---------------------------------------------------------------------------


class TestObjective:
    """Phase 0's objective must NOT call trial.suggest_* (no axes active)."""

    def test_returns_finite_float(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=10, n_comps=2)
        grid = GridSpec()
        # Take all of actions as train and all passes as holdout (synthetic test only)
        train = actions
        holdout_passes = actions[actions["type_name"] == "pass"].copy()
        study = optuna.create_study(direction="minimize")
        nll = objective(study.ask(), train, holdout_passes, grid=grid)
        assert isinstance(nll, float)
        assert math.isfinite(nll)
        assert nll > 0  # NLL of probabilities < 1 is strictly positive

    def test_no_suggest_calls_in_phase0(self) -> None:
        """Trial.params should remain empty after objective (Phase 0 has no axes)."""
        actions = _make_actions_with_competitions(n_per_match=50, matches_per_comp=5, n_comps=2)
        grid = GridSpec()
        study = optuna.create_study(direction="minimize")
        trial = study.ask()
        objective(trial, actions, actions[actions["type_name"] == "pass"].copy(), grid=grid)
        assert trial.params == {}


# ---------------------------------------------------------------------------
# run_phase0_harness — end-to-end smoke
# ---------------------------------------------------------------------------


class TestRunPhase0Harness:
    def test_returns_phase0_result(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=30, n_comps=3)
        result = run_phase0_harness(actions)
        assert isinstance(result, Phase0Result)

    def test_result_has_finite_nll(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=30, n_comps=3)
        result = run_phase0_harness(actions)
        assert math.isfinite(result.best_nll)
        assert result.best_nll > 0

    def test_result_has_xtgrid(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=30, n_comps=3)
        result = run_phase0_harness(actions)
        assert isinstance(result.best_xt_grid, XTGrid)
        assert result.best_xt_grid.shape == (12, 8)

    def test_train_and_holdout_counts_consistent(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=20, n_comps=4)
        result = run_phase0_harness(actions)
        # Holdout passes should be a subset; train should cover the complement
        assert result.n_train_actions > 0
        assert result.n_holdout_passes > 0
        assert result.n_holdout_passes < len(actions)  # only passes, only 15%
        # Holdout-pass count should be ~15% of total passes
        total_passes = (actions["type_name"] == "pass").sum()
        ratio = result.n_holdout_passes / total_passes
        assert 0.05 < ratio < 0.30  # generous binomial CI

    def test_single_trial_in_study(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=10, n_comps=2)
        result = run_phase0_harness(actions)
        assert len(result.study.trials) == 1


# ---------------------------------------------------------------------------
# run_phase0_harness — grid + holdout configuration
# ---------------------------------------------------------------------------


class TestRunPhase0HarnessConfig:
    def test_custom_grid_propagates(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=30, n_comps=3)
        grid = GridSpec(n_zones_x=6, n_zones_y=4)
        result = run_phase0_harness(actions, grid=grid)
        assert result.best_xt_grid.shape == (6, 4)

    def test_custom_holdout_fraction(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=30, n_comps=3)
        # Larger holdout → larger holdout count (deterministic via hash)
        r15 = run_phase0_harness(actions, holdout_fraction=0.15)
        r30 = run_phase0_harness(actions, holdout_fraction=0.30)
        assert r30.n_holdout_passes >= r15.n_holdout_passes


# ---------------------------------------------------------------------------
# run_phase0_harness — Optuna integration shape
# ---------------------------------------------------------------------------


class TestOptunaIntegration:
    def test_study_direction_is_minimize(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=10, n_comps=2)
        result = run_phase0_harness(actions)
        assert result.study.direction == optuna.study.StudyDirection.MINIMIZE

    def test_study_best_value_matches_result_nll(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=10, n_comps=2)
        result = run_phase0_harness(actions)
        assert result.study.best_value == result.best_nll

    def test_study_best_trial_matches_result(self) -> None:
        actions = _make_actions_with_competitions(n_per_match=100, matches_per_comp=10, n_comps=2)
        result = run_phase0_harness(actions)
        assert result.study.best_trial.number == result.best_trial.number


# ---------------------------------------------------------------------------
# run_phase0_harness — input validation
# ---------------------------------------------------------------------------


class TestRunPhase0HarnessValidation:
    def test_rejects_missing_columns(self) -> None:
        actions = pd.DataFrame({"competition_id": ["A"], "match_key": [1]})
        with pytest.raises(ValueError, match="missing required columns"):
            run_phase0_harness(actions)

    def test_vectorization_smoke_50k_actions(self) -> None:
        """50K actions runs end-to-end in <5s — catches gross O(n^2) regressions.

        Real fct_action_values has ~8.8M rows; this is ~0.6% of production
        scale. A vectorized pipeline scales linearly in n, so 50K → <5s
        implies 8.8M → <15min (acceptable for a one-off Phase 0 baseline).
        """
        import time

        actions = _make_actions_with_competitions(n_per_match=200, matches_per_comp=50, n_comps=5)
        assert len(actions) == 50_000  # sanity-check fixture size
        t0 = time.perf_counter()
        result = run_phase0_harness(actions)
        elapsed = time.perf_counter() - t0
        assert math.isfinite(result.best_nll)
        assert elapsed < 5.0, f"50K-action harness took {elapsed:.2f}s; vectorization regression?"

    def test_empty_actions_raises(self) -> None:
        empty = pd.DataFrame(
            {
                "competition_id": pd.Series([], dtype=str),
                "match_key": pd.Series([], dtype=int),
                "type_name": pd.Series([], dtype=str),
                "result_name": pd.Series([], dtype=str),
                "action_type": pd.Series([], dtype=str),
                "start_x": pd.Series([], dtype=float),
                "start_y": pd.Series([], dtype=float),
                "end_x": pd.Series([], dtype=float),
                "end_y": pd.Series([], dtype=float),
            }
        )
        with pytest.raises(ValueError, match="empty"):
            run_phase0_harness(empty)
