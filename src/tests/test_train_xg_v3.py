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

import base64
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

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


class TestFreezeFrameJoin:
    """Task 1.4 — freeze frames join on ``(match_key, action_id)``, not ``action_id`` alone.

    ``action_id`` is per-match (§5 invariant — the single most important one). Two matches can
    each carry a shot with ``action_id == 100``; grouping the freeze frame by ``action_id`` alone
    silently unions the two matches' player sets onto BOTH shots.
    """

    def test_parse_freeze_frames_joins_on_match_key_action_id(self) -> None:
        # Two shots share action_id=100 across two different matches.
        shots_df = pd.DataFrame({"match_key": [1, 2], "action_id": [100, 100]})
        # Match 1's freeze frame has 3 players; match 2's has 2 — deliberately different players.
        freeze_df = pd.DataFrame(
            {
                "match_key": [1, 1, 1, 2, 2],
                "action_id": [100, 100, 100, 100, 100],
                "x": [10.0, 20.0, 30.0, 40.0, 50.0],
                "y": [1.0, 2.0, 3.0, 4.0, 5.0],
                "is_keeper": [0, 0, 1, 0, 1],
                "is_teammate": [1, 1, 0, 1, 0],
            }
        )
        sets = v3.parse_freeze_frames_spadl(shots_df, freeze_df)
        assert len(sets) == 2
        # Each shot must see ONLY its own match's players (not the 5-row union).
        assert sets[0].shape[0] == 3, "match 1 shot must get exactly its 3 players"
        assert sets[1].shape[0] == 2, "match 2 shot must get exactly its 2 players"


class TestLoadDatasetBothRepos:
    """Task 1.5 — read BOTH the public repo and its ADR-049 ``-restricted`` companion, fail-loud."""

    class _TreeItem:
        def __init__(self, path: str, size: int = 128) -> None:
            self.path = path
            self.size = size

    class _FakeApi:
        """Minimal ``HfApi`` stand-in: ``list_repo_tree`` returns per-repo parquet listings.

        An unknown repo returns an EMPTY listing — functionally identical (for the helper) to a
        ``RepositoryNotFoundError`` the helper catches: both yield an empty frame, which the helper
        fail-loud check then handles.
        """

        def __init__(self, trees: dict[str, list[str]]) -> None:
            self._trees = trees

        def list_repo_tree(self, repo_id: str, repo_type: str = "dataset", recursive: bool = True) -> list:
            return [TestLoadDatasetBothRepos._TreeItem(p) for p in self._trees.get(repo_id, [])]

    @staticmethod
    def _make_download(file_map: dict[tuple[str, str], str]):
        def _download(repo_id: str, filename: str, repo_type: str = "dataset", token: str | None = None) -> str:
            return file_map[(repo_id, filename)]

        return _download

    def test_concats_public_and_restricted(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        pub = tmp_path / "pub.parquet"
        pd.DataFrame({"data_source": ["statsbomb"], "action_id": [1]}).to_parquet(pub)
        res = tmp_path / "res.parquet"
        pd.DataFrame({"data_source": ["skillcorner"], "action_id": [2]}).to_parquet(res)

        api = self._FakeApi({"repo": ["data/statsbomb.parquet"], "repo-restricted": ["data/skillcorner.parquet"]})
        monkeypatch.setattr(
            "huggingface_hub.hf_hub_download",
            self._make_download(
                {
                    ("repo", "data/statsbomb.parquet"): str(pub),
                    ("repo-restricted", "data/skillcorner.parquet"): str(res),
                }
            ),
        )
        out = v3.load_dataset_both_repos(api, "repo", "tok")
        assert set(out["data_source"]) == {"statsbomb", "skillcorner"}
        assert len(out) == 2

    def test_raises_when_restricted_companion_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        pub = tmp_path / "pub.parquet"
        pd.DataFrame({"data_source": ["statsbomb"], "action_id": [1]}).to_parquet(pub)

        # Restricted repo exists but yields ZERO parquet rows -> must fail loud (never train public-only).
        api = self._FakeApi({"repo": ["data/statsbomb.parquet"], "repo-restricted": []})
        monkeypatch.setattr(
            "huggingface_hub.hf_hub_download",
            self._make_download({("repo", "data/statsbomb.parquet"): str(pub)}),
        )
        with pytest.raises(RuntimeError, match="restricted"):
            v3.load_dataset_both_repos(api, "repo", "tok")


class TestUniformFeatures:
    """Task 1.6 — v3 ships EXACTLY the geometry-only D2 feature set (no StatsBomb categoricals)."""

    def test_uniform_feature_names_are_geometry_only(self) -> None:
        shots = pd.DataFrame(
            {
                "start_x": [94.0],
                "start_y": [34.0],
                "action_result": ["success"],
                # StatsBomb-native columns that must NEVER leak into v3's uniform feature set:
                "shot_body_part": ["Head"],
                "shot_technique": ["Volley"],
                "shot_type": ["Open Play"],
                "play_pattern": ["From Counter"],
                "period": [1],
                "minute": [10],
                "end_location_x": [105.0],
                "end_location_y": [34.0],
                "is_first_time": [True],
            }
        )
        x, _y = v3.build_spadl_tabular(shots, [np.empty((0, 4))], XGModelConfig())
        assert list(x.columns) == [
            "distance_to_goal",
            "shot_angle",
            "location_x",
            "location_y",
            "set_cardinality",
        ]
        assert not any(c.startswith("shot_body_part") for c in x.columns)
        assert "period" not in x.columns and "end_location_x" not in x.columns and "is_first_time" not in x.columns


class TestCalibrationOwnership:
    """Task 1.6 — the TRAINER fits OOF calibrators (per-provider + pooled) and a penalty constant,
    and ships them as EVIDENCE / serve-time parameters. The model output path stays RAW.
    """

    def test_fit_calibrators_returns_per_provider_and_pooled(self) -> None:
        rng = np.random.default_rng(0)
        n = 240
        raw = rng.uniform(0.0, 1.0, n)
        y = (rng.uniform(0.0, 1.0, n) < raw).astype(int)
        data_source = np.array(["statsbomb"] * 120 + ["skillcorner"] * 120)
        match_keys = np.repeat(np.arange(24), 10)  # 24 matches, group-disjoint folds
        cals = v3.fit_calibrators(raw, y, data_source, match_keys)
        assert set(cals.keys()) == {"per_provider", "pooled"}
        assert set(cals["per_provider"].keys()) == {"statsbomb", "skillcorner"}
        for entry in [cals["pooled"], *cals["per_provider"].values()]:
            assert entry["kind"] in {"platt", "isotonic"}
            assert isinstance(entry["params"], dict)  # JSON-safe (no pickle)

    def test_trainer_ships_calibrators_but_emits_raw(self) -> None:
        weights = {"pred_fc1_weight": np.ones((4, 3), dtype=np.float64)}
        cals = {
            "per_provider": {"statsbomb": {"kind": "platt", "params": {"a": 1.0, "b": 0.0}}},
            "pooled": {"kind": "platt", "params": {"a": 1.0, "b": 0.0}},
        }
        env = v3.build_weight_envelope(weights, ["a", "b", "c"], calibrators=cals, penalty_xg=0.76)
        # Calibrators ride alongside the weights as evidence + serve-time params.
        assert env["_calibrators"] == cals
        assert "per_provider" in env["_calibrators"] and "pooled" in env["_calibrators"]
        # The served weights are RAW — no calibration transform is baked into them.
        rt = json.loads(v3.serialize_envelope(env).decode("utf-8"))
        arr = np.frombuffer(base64.b64decode(rt["weights"]["pred_fc1_weight"]["data"]), dtype=np.float64).reshape(4, 3)
        np.testing.assert_array_equal(arr, np.ones((4, 3)))

    def test_penalties_excluded_from_training(self) -> None:
        df = pd.DataFrame(
            {
                "action_type": ["shot", "shot_penalty", "shot_freekick", "shot_penalty"],
                "action_result": ["success", "success", "fail", "fail"],
            }
        )
        out = v3.select_training_shots(df)
        assert (out["action_type"] == "shot_penalty").sum() == 0
        assert set(out["action_type"]) == {"shot", "shot_freekick"}

    def test_penalty_constant_computed_from_penalty_rows_not_training_set(self) -> None:
        df = pd.DataFrame(
            {
                "action_type": ["shot", "shot_penalty", "shot_penalty", "shot_freekick"],
                "action_result": ["fail", "success", "fail", "success"],
            }
        )
        # 1 of 2 penalties scored -> 0.5, computed over the LOADED (pre-filter) rows.
        assert abs(v3.penalty_goal_rate(df) - 0.5) < 1e-9
        # After the population filter there are ZERO penalties: computing there is 0/0 -> NaN.
        filtered = v3.select_training_shots(df)
        assert (filtered["action_type"] == "shot_penalty").sum() == 0
        assert np.isnan(v3.penalty_goal_rate(filtered))

    def test_envelope_carries_penalty_constant(self) -> None:
        env = v3.build_weight_envelope({"w": np.zeros((2, 2), dtype=np.float64)}, ["a", "b"], penalty_xg=0.76)
        assert env["_penalty_xg"] == 0.76
        rt = json.loads(v3.serialize_envelope(env).decode("utf-8"))
        assert rt["_penalty_xg"] == 0.76


class TestGateEvidence:
    """Task 1.4-1.7 follow-on — the ``_gate`` evidence block the scorer certifies context modes from.

    Without ``_gate`` the scorer fail-safes EVERY provider to ``tabular_only + ood_flag=True`` and the
    consumer drops ood-flagged cohorts, so the whole delivery certifies nothing. These tests pin the
    emitted shape AND the trainer↔scorer contract end-to-end (build here → certify in the scorer).
    """

    @staticmethod
    def _synthetic_oof() -> tuple[dict[str, np.ndarray], dict[str, object]]:
        """A 2-provider OOF bundle: ``statsbomb`` STRONG (perfectly-separating context, well-calibrated),
        ``weak`` uncorrelated context (AUC ~0.5). Calibrators map the strong raw context to calibrated xg
        whose sum matches the goal total (Platt a=10, b=-5 sends 0.9->0.982 / 0.1->0.018, mean 0.5)."""
        n = 200
        y = np.array([1] * 100 + [0] * 100)
        ctx_sb = np.where(y == 1, 0.9, 0.1)  # perfect separation -> AUC 1.0, CI lo 1.0
        tab_sb = np.tile([0.4, 0.6], 100)  # uncorrelated with y -> AUC ~0.5
        ctx_wk = np.tile([0.45, 0.55], 100)  # uncorrelated -> AUC ~0.5, CI lo well below the floor
        tab_wk = np.tile([0.45, 0.55], 100)
        oof: dict[str, np.ndarray] = {
            "proba": np.concatenate([ctx_sb, ctx_wk]),
            "tabular_proba": np.concatenate([tab_sb, tab_wk]),
            "y": np.concatenate([y, y]),
            "data_source": np.array(["statsbomb"] * n + ["weak"] * n),
            "match_key": np.concatenate([np.repeat(np.arange(20), 10), np.repeat(np.arange(20, 40), 10)]),
        }
        platt = {"kind": "platt", "params": {"a": 10.0, "b": -5.0}}
        calibrators: dict[str, object] = {
            "per_provider": {"statsbomb": platt, "weak": platt},
            "pooled": platt,
        }
        return oof, calibrators

    def test_envelope_carries_gate_evidence(self) -> None:
        oof, cals = self._synthetic_oof()
        gate = v3.build_gate_evidence(oof, cals)
        env = v3.build_weight_envelope({"w": np.zeros((2, 2), dtype=np.float64)}, ["a", "b"], gate=gate)

        g = env["_gate"]
        assert set(g.keys()) == {"sb_auc", "margin", "floor", "per_provider"}
        assert isinstance(g["sb_auc"], float)
        assert set(g["per_provider"].keys()) == {"statsbomb", "weak"}
        for entry in g["per_provider"].values():
            assert set(entry.keys()) == {"context", "tabular", "sum_xg", "sum_goals", "n"}
            for mode in ("context", "tabular"):
                assert set(entry[mode].keys()) == {"auc", "lo", "hi"}
            assert isinstance(entry["n"], int)
        # sb_auc is statsbomb's context AUC point estimate (perfect separation -> 1.0).
        assert abs(g["sb_auc"] - 1.0) < 1e-9
        # Survives the JSON envelope round-trip (no numpy leakage, no pickle).
        rt = json.loads(v3.serialize_envelope(env).decode("utf-8"))
        assert rt["_gate"]["per_provider"]["statsbomb"]["n"] == 200

    def test_gate_evidence_enables_certification(self) -> None:
        # End-to-end trainer↔scorer contract: the emitted _gate drives the scorer's certification.
        from ingestion.xg_shot_scorer import decide_scoring_mode, parse_gate

        oof, cals = self._synthetic_oof()
        gate = v3.build_gate_evidence(oof, cals)
        cfg, evidence = parse_gate({"_gate": gate})

        strong_mode, strong_ood = decide_scoring_mode(evidence["statsbomb"], cfg)
        assert strong_mode == "context_aware", "strong provider must select the context-aware mode"
        assert strong_ood is False, "strong provider (high context CI-lo, calibrated) must certify"

        _weak_mode, weak_ood = decide_scoring_mode(evidence["weak"], cfg)
        assert weak_ood is True, "weak provider (context AUC ~0.5) must fail certification -> ood_flag"


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
