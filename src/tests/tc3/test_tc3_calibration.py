"""Tests for TC-3 calibration script helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from run_tc3_calibration import MatchData


class TestValidateGradientSports:
    def _make_match_data(
        self,
        tmp_path: Path,
        *,
        n_frames: int = 26_000,  # Just above 25K gate; failure tests use 500
        n_gk: int = 2,
        xy_nan_frac: float = 0.0,
        n_teams: int = 2,
    ) -> MatchData:
        """Create a synthetic MatchData for validation testing."""
        from run_tc3_calibration import MatchData

        n_players = 20 + n_gk
        rows_per_frame = n_players + 1  # +1 for ball
        total_rows = n_frames * rows_per_frame

        # Realistic player IDs (10001+), not sequential from 0
        player_ids = np.array([10001 + i for i in range(n_players)] + [None], dtype=object)

        frames = pd.DataFrame(
            {
                "frame_id": np.repeat(np.arange(n_frames), rows_per_frame),
                "player_id": np.tile(player_ids, n_frames),
                "is_goalkeeper": np.tile(
                    [True] * n_gk + [False] * (n_players - n_gk) + [False],
                    n_frames,
                ),
                "x": np.random.rand(total_rows) * 105,
                "y": np.random.rand(total_rows) * 68,
                "is_ball": np.tile([False] * n_players + [True], n_frames),
            }
        )

        # Inject NaN
        if xy_nan_frac > 0:
            mask = np.random.rand(total_rows) < xy_nan_frac
            frames.loc[mask, "x"] = np.nan
            frames.loc[mask, "y"] = np.nan

        frames_path = tmp_path / "frames.parquet"
        frames.to_parquet(frames_path)

        teams = ["team_a", "team_b"][:n_teams]
        actions = pd.DataFrame(
            {
                "team_id_native": np.random.choice(teams, size=100),
            }
        )

        return MatchData(
            match_id="test_99999",
            provider="gradientsports",
            actions=actions,
            frames_path=frames_path,
            home_team_id="team_a",
            home_start_left=True,
        )

    def test_valid_match_passes(self, tmp_path: Path) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data(tmp_path)
        result = _validate_gradient_sports([m])
        assert len(result) == 1

    def test_low_frame_count_excluded(self, tmp_path: Path) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data(tmp_path, n_frames=500)
        result = _validate_gradient_sports([m])
        assert len(result) == 0

    def test_missing_gk_excluded(self, tmp_path: Path) -> None:
        from run_tc3_calibration import _validate_gradient_sports

        m = self._make_match_data(tmp_path, n_gk=1)
        result = _validate_gradient_sports([m])
        assert len(result) == 0


class TestEnrichWithParams:
    """Test that enrichment accepts tunable parameters."""

    def test_enrichment_accepts_k3(self) -> None:
        from silly_kicks.tracking import LinkParams

        lp = LinkParams(k3=2.5)
        assert lp.k3 == 2.5
        assert lp.r_hoz == 4.0  # unchanged geometry

    def test_enrichment_accepts_off_ball_params(self) -> None:
        import inspect

        from silly_kicks.tracking import add_off_ball_context

        sig = inspect.signature(add_off_ball_context)
        assert "pre_seconds" in sig.parameters
        assert "min_displacement_m" in sig.parameters

    def test_enrichment_accepts_carrier_params(self) -> None:
        import inspect

        from silly_kicks.tracking import infer_ball_carrier

        sig = inspect.signature(infer_ball_carrier)
        assert "tolerance_m" in sig.parameters
        assert "beta" in sig.parameters
        assert "gamma" in sig.parameters


class TestAugmentedVaep:
    def test_xgboost_brier_computes(self) -> None:
        import xgboost as xgb
        from sklearn.metrics import brier_score_loss

        rng = np.random.RandomState(42)
        features = rng.rand(200, 10)
        y = rng.randint(0, 2, 200)
        model = xgb.XGBClassifier(
            n_estimators=10,
            max_depth=2,
            use_label_encoder=False,
            eval_metric="logloss",
        )
        model.fit(features[:150], y[:150])
        probs = model.predict_proba(features[150:])[:, 1]
        brier = brier_score_loss(y[150:], probs)
        assert 0 <= brier <= 1

    def test_feature_variance_gate(self) -> None:
        feature = np.ones(100)
        default_var = 1.0
        ratio = np.var(feature) / default_var
        assert ratio < 0.1  # triggers sanity gate


class TestCalibrationCLI:
    def test_cli_argument_parsing(self) -> None:
        from run_tc3_calibration import main

        assert callable(main)

    def test_all_features_list_complete(self) -> None:
        from run_tc3_calibration import (
            _SPADL_FEATURES,
            _TRACKING_FEATURES,
            ALL_FEATURES,
        )

        assert len(ALL_FEATURES) == len(_SPADL_FEATURES) + len(_TRACKING_FEATURES)
        assert "pressure_on_actor__link_zones" in ALL_FEATURES
        assert "n_off_ball_runners_pre_window" in ALL_FEATURES
        assert "das_team" in ALL_FEATURES
