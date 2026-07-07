"""Unit tests for scripts/train_xg_v3_hf.py — the extractable pure helpers.

The v3 trainer is a PEP 723 single-file that runs on HF Jobs with PyTorch. These
tests exercise ONLY the pure, torch-free helpers (population filter, SPADL-native
feature assembly, weight-envelope contract, GroupKFold split) — never the HF-Jobs
training path. The trainer is deliberately structured so every torch-dependent
definition sits behind a runtime guard, keeping the module importable without
``torch`` installed (torch is an HF-Jobs-only dep, absent in local CI).

Contract locked in here (design spec §4, ADR-012 §2):
- ``select_training_shots`` keeps the OPEN-PLAY family ``{shot, shot_freekick}``
  (excludes ``shot_penalty``) and drops ``action_result == 'yellow_card'`` rows.
- Feature assembly calls the SHARED ``analytics.xg_model.build_features`` (M2 parity)
  and sets ``set_cardinality`` from the per-shot player set (0 for zero-context).
- The serialized envelope carries ``feature_names`` + ``tabular_dim`` +
  ``coordinate_system == 'spadl_105x68'``.
- The CV split uses ``GroupKFold`` on ``match_key`` — no same-match leakage.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import train_xg_v3_hf as v3  # noqa: E402  (sys.path insert must precede import)

from analytics.xg_model import XGModelConfig  # noqa: E402


class TestPopulationFilter:
    """``select_training_shots`` — the exact training shot family (spec §4.1.1)."""

    def test_keeps_open_play_family_excludes_penalty_and_yellow_card(self) -> None:
        df = pd.DataFrame(
            {
                "action_type": ["shot", "shot_freekick", "shot_penalty", "pass", "shot"],
                "action_result": ["success", "fail", "success", "success", "yellow_card"],
            }
        )
        out = v3.select_training_shots(df)
        # shot_penalty excluded entirely; pass is not a shot; the yellow_card shot dropped.
        assert set(out["action_type"]) == {"shot", "shot_freekick"}
        assert len(out) == 2
        assert "shot_penalty" not in set(out["action_type"])
        assert "yellow_card" not in set(out["action_result"])

    def test_returns_a_fresh_frame_without_mutating_input(self) -> None:
        df = pd.DataFrame({"action_type": ["shot"], "action_result": ["success"]})
        before = df.copy()
        _ = v3.select_training_shots(df)
        pd.testing.assert_frame_equal(df, before)


class TestFeatureAssembly:
    """SPADL-native feature assembly delegates to the SHARED build_features (M2 parity)."""

    def test_set_cardinality_zero_for_zero_context_and_n_for_freeze_frame(self) -> None:
        shots = pd.DataFrame(
            {
                "start_x": [94.0, 88.0],
                "start_y": [34.0, 40.0],
                "action_result": ["success", "fail"],
            }
        )
        player_sets = [
            np.empty((0, 4), dtype=np.float64),  # zero-context row -> cardinality 0
            np.zeros((11, 4), dtype=np.float64),  # freeze-frame row of 11 players
        ]
        x, _y = v3.build_spadl_tabular(shots, player_sets, XGModelConfig())
        assert "set_cardinality" in x.columns
        assert float(x.iloc[0]["set_cardinality"]) == 0.0
        assert float(x.iloc[1]["set_cardinality"]) == 11.0

    def test_uses_shared_build_features(self) -> None:
        # The assembly helper must call the shared analytics.xg_model.build_features
        # (the M2 train/serve parity seam) — not a re-inlined copy.
        import analytics.xg_model as shared

        assert v3.build_features is shared.build_features

    def test_emits_spadl_geometry_columns(self) -> None:
        shots = pd.DataFrame({"start_x": [94.0], "start_y": [34.0], "action_result": ["success"]})
        x, _y = v3.build_spadl_tabular(shots, [np.empty((0, 4))], XGModelConfig())
        # penalty-spot geometry: distance ~11 m in canonical SPADL (goal at x=105).
        assert abs(float(x.iloc[0]["distance_to_goal"]) - 11.0) < 0.5


class TestEnvelopeContract:
    """ADR-012 §2 — envelope carries feature_names + tabular_dim + coordinate_system."""

    def test_build_envelope_injects_all_three_fields(self) -> None:
        weights = {
            "encoder_fc1_weight": np.zeros((32, 4), dtype=np.float64),
            "encoder_fc1_bias": np.zeros(32, dtype=np.float64),
        }
        feature_names = ["distance_to_goal", "shot_angle", "set_cardinality"]
        env = v3.build_weight_envelope(weights, feature_names)
        assert env["feature_names"] == feature_names
        assert env["tabular_dim"] == len(feature_names)
        assert env["coordinate_system"] == "spadl_105x68"

    def test_envelope_survives_serialization_roundtrip(self) -> None:
        import json

        weights = {"pred_fc1_weight": np.ones((4, 3), dtype=np.float64)}
        feature_names = ["a", "b", "c"]
        env = v3.build_weight_envelope(weights, feature_names)
        roundtripped = json.loads(v3.serialize_envelope(env).decode("utf-8"))
        assert roundtripped["feature_names"] == feature_names
        assert roundtripped["tabular_dim"] == 3
        assert roundtripped["coordinate_system"] == "spadl_105x68"


class TestGroupKFoldLeakage:
    """CV split is GroupKFold by match_key: fit-group ∩ measure-group == ∅."""

    def test_folds_never_share_a_match(self) -> None:
        match_keys = np.array([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6])
        splits = v3.groupkfold_splits(match_keys, n_splits=3)
        assert len(splits) == 3
        for train_idx, test_idx in splits:
            fit_groups = set(match_keys[train_idx].tolist())
            measure_groups = set(match_keys[test_idx].tolist())
            assert fit_groups.isdisjoint(measure_groups)

    def test_every_sample_measured_exactly_once(self) -> None:
        match_keys = np.array([10, 10, 20, 20, 30, 30, 40, 40])
        splits = v3.groupkfold_splits(match_keys, n_splits=4)
        measured: list[int] = []
        for _train_idx, test_idx in splits:
            measured.extend(test_idx.tolist())
        assert sorted(measured) == list(range(len(match_keys)))
