"""Tests for the Evolve evaluator bridge and search space validation."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator, validate_search_space

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
