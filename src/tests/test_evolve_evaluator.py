"""Tests for the Evolve evaluator bridge and search space validation."""

from __future__ import annotations

import importlib.util
import json
import sys
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator, validate_search_space
from evolve.runner import _eval_fingerprint, _evaluate_seeds, _load_cached_seeds, _write_evaluator_script

VALID_CONFIG: dict[str, Any] = {
    "hidden_dim": 256,
    "num_layers": 6,
    "num_heads": 8,
    "conditioning_type": "additive",
    "dropout": 0.1,
    "learning_rate": 1e-4,
    "vaep_loss_weight": 0.1,
    "batch_size": 256,
}


class TestSearchSpaceValidation:
    def test_valid_config_passes(self) -> None:
        assert validate_search_space(VALID_CONFIG) is True

    def test_hidden_dim_too_large(self) -> None:
        cfg = {**VALID_CONFIG, "hidden_dim": 1024}
        assert validate_search_space(cfg) is False

    def test_hidden_dim_too_small(self) -> None:
        cfg = {**VALID_CONFIG, "hidden_dim": 32}
        assert validate_search_space(cfg) is False

    def test_num_heads_must_divide_hidden_dim(self) -> None:
        cfg = {**VALID_CONFIG, "hidden_dim": 300, "num_heads": 8}
        assert validate_search_space(cfg) is False

    def test_dropout_out_of_range(self) -> None:
        cfg = {**VALID_CONFIG, "dropout": 0.8}
        assert validate_search_space(cfg) is False

    def test_unknown_conditioning_type(self) -> None:
        cfg = {**VALID_CONFIG, "conditioning_type": "transformer_xl"}
        assert validate_search_space(cfg) is False


def _write_candidate(tmp_path: Path, config: dict[str, Any]) -> Path:
    """Write a candidate program file that exposes a ``config`` dict."""
    p = tmp_path / "candidate.py"
    p.write_text(
        textwrap.dedent(f"""\
        config = {config!r}
        """),
    )
    return p


class TestEvolveEvaluator:
    def test_evaluate_calls_backend(self, tmp_path: Path) -> None:
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "spearman_rho": 0.5,
            "top1_accuracy": 0.6,
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        eval_cfg = EvalConfig(epochs=5, seed=42)

        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=eval_cfg,
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        mock_backend.train.assert_called_once_with(
            candidate_config=VALID_CONFIG,
            target="scoutgpt",
            epochs=5,
            seed=42,
        )
        expected_score = 0.7 * 0.5 + 0.3 * 0.6
        assert result["combined_score"] == pytest.approx(expected_score)
        assert result["spearman_rho"] == pytest.approx(0.5)
        assert result["top1_accuracy"] == pytest.approx(0.6)

    def test_openevolve_evaluator_script_is_self_contained(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the standalone evaluator script works in a fresh process.

        Simulates exactly what OpenEvolve's process_parallel does: load
        the script via importlib in an environment where no in-process
        globals are set.  The script must reconstruct its own evaluator
        from the config JSON written alongside it.

        We monkeypatch ``create_backend`` to return a mock backend so
        no GPU is needed.
        """
        from evolve.config import BackendConfig, EvolveConfig

        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        # Build a minimal EvolveConfig for the evaluator script
        config = EvolveConfig(
            target="scoutgpt",
            fitness=FitnessConfig(
                primary="spearman_rho",
                secondary="top1_accuracy",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            ),
            evaluation=EvalConfig(epochs=3, seed=42),
            backend=BackendConfig(type="local_cuda"),
        )

        # Write the evaluator script + config JSON
        script_path = _write_evaluator_script(tmp_path, config)
        assert (tmp_path / "_openevolve_evaluator_config.json").exists()

        # Monkeypatch create_backend to return a mock
        mock_backend = MagicMock()
        mock_backend.train.return_value = {"spearman_rho": 0.42, "top1_accuracy": 0.75}
        monkeypatch.setattr("evolve.backends.create_backend", lambda *a, **kw: mock_backend)

        # Load the script via importlib (same as OpenEvolve workers)
        spec = importlib.util.spec_from_file_location("test_eval_module", str(script_path))
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules["test_eval_module"] = module
        try:
            spec.loader.exec_module(module)

            result = module.evaluate(str(candidate_path))

            assert result["combined_score"] == pytest.approx(0.7 * 0.42 + 0.3 * 0.75)
            assert result["spearman_rho"] == pytest.approx(0.42)
            mock_backend.train.assert_called_once()
        finally:
            sys.modules.pop("test_eval_module", None)

    def test_evaluate_rejects_invalid_config(self, tmp_path: Path) -> None:
        invalid_config = {**VALID_CONFIG, "hidden_dim": 2000}
        candidate_path = _write_candidate(tmp_path, invalid_config)

        mock_backend = MagicMock()
        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        eval_cfg = EvalConfig(epochs=5, seed=42)

        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="scoutgpt",
            eval_config=eval_cfg,
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        assert result["combined_score"] == 0.0
        mock_backend.train.assert_not_called()


# ---------------------------------------------------------------------------
# Resume / partial seed recovery tests
# ---------------------------------------------------------------------------

_EVAL_CFG = EvalConfig(epochs=3, seed=42)
_FITNESS_CFG = FitnessConfig(
    primary="spearman_rho",
    secondary="top1_accuracy",
    combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
)


def _write_seed_result(seed_dir: Path, name: str, metrics: dict[str, float], fingerprint: str) -> None:
    """Write a cached seed result JSON file."""
    seed_dir.mkdir(parents=True, exist_ok=True)
    (seed_dir / f"{name}.json").write_text(
        json.dumps({"program": f"{name}.py", "fingerprint": fingerprint, "metrics": metrics}, indent=2)
    )


def _write_seed_program(tmp_path: Path, name: str) -> Path:
    """Write a minimal seed program file."""
    seed_dir = tmp_path / "seeds"
    seed_dir.mkdir(parents=True, exist_ok=True)
    p = seed_dir / f"{name}.py"
    p.write_text(f"config = {VALID_CONFIG!r}\n")
    return p


class TestSeedResume:
    def test_fingerprint_deterministic(self) -> None:
        fp1 = _eval_fingerprint(_EVAL_CFG)
        fp2 = _eval_fingerprint(_EVAL_CFG)
        assert fp1 == fp2

    def test_fingerprint_changes_with_epochs(self) -> None:
        other = EvalConfig(epochs=5, seed=42)
        assert _eval_fingerprint(_EVAL_CFG) != _eval_fingerprint(other)

    def test_load_cached_seeds_valid(self, tmp_path: Path) -> None:
        fp = _eval_fingerprint(_EVAL_CFG)
        seed_dir = tmp_path / "seed_results"
        _write_seed_result(seed_dir, "additive", {"combined_score": 0.5, "rho": 0.4}, fp)
        programs = [_write_seed_program(tmp_path, "additive")]

        cached = _load_cached_seeds(seed_dir, programs, fp)
        assert "additive" in cached
        assert cached["additive"]["combined_score"] == 0.5

    def test_load_cached_seeds_stale_fingerprint(self, tmp_path: Path) -> None:
        seed_dir = tmp_path / "seed_results"
        _write_seed_result(seed_dir, "additive", {"combined_score": 0.5, "rho": 0.4}, "old_fingerprint")
        programs = [_write_seed_program(tmp_path, "additive")]

        cached = _load_cached_seeds(seed_dir, programs, "new_fingerprint")
        assert len(cached) == 0

    def test_load_cached_seeds_skips_zero_score(self, tmp_path: Path) -> None:
        fp = _eval_fingerprint(_EVAL_CFG)
        seed_dir = tmp_path / "seed_results"
        _write_seed_result(seed_dir, "additive", {"combined_score": 0.0}, fp)
        programs = [_write_seed_program(tmp_path, "additive")]

        cached = _load_cached_seeds(seed_dir, programs, fp)
        assert len(cached) == 0

    def test_evaluate_seeds_skips_cached(self, tmp_path: Path) -> None:
        """With 2 seeds and 1 cached, only the uncached seed is evaluated."""
        mock_backend = MagicMock()
        mock_backend.train.return_value = {"spearman_rho": 0.3, "top1_accuracy": 0.7}

        evaluator = EvolveEvaluator(
            backend=mock_backend, target="scoutgpt", eval_config=_EVAL_CFG, fitness_config=_FITNESS_CFG
        )
        programs = [
            _write_seed_program(tmp_path, "additive"),
            _write_seed_program(tmp_path, "film"),
        ]
        cached = {"additive": {"combined_score": 0.9, "spearman_rho": 0.8, "top1_accuracy": 0.9}}

        best_path, best_metrics = _evaluate_seeds(
            evaluator, programs, tmp_path, eval_config=_EVAL_CFG, cached_seeds=cached
        )

        # Only film was evaluated (additive was cached)
        assert mock_backend.train.call_count == 1
        # Cached additive has higher score (0.9) than fresh film (0.7*0.3 + 0.3*0.7 = 0.42)
        assert best_path.stem == "additive"
        assert best_metrics["combined_score"] == 0.9

    def test_evaluate_seeds_all_cached(self, tmp_path: Path) -> None:
        """When all seeds are cached, no backend calls are made."""
        mock_backend = MagicMock()
        evaluator = EvolveEvaluator(
            backend=mock_backend, target="scoutgpt", eval_config=_EVAL_CFG, fitness_config=_FITNESS_CFG
        )
        programs = [_write_seed_program(tmp_path, "additive")]
        cached = {"additive": {"combined_score": 0.5, "spearman_rho": 0.4, "top1_accuracy": 0.6}}

        best_path, _ = _evaluate_seeds(evaluator, programs, tmp_path, eval_config=_EVAL_CFG, cached_seeds=cached)

        assert best_path.stem == "additive"
        mock_backend.train.assert_not_called()
