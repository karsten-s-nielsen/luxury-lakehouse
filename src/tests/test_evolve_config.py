"""Tests for evolve engine configuration."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from evolve.config import (
    BackendConfig,
    EvolutionConfig,
    EvolveConfig,
    FitnessConfig,
    LLMConfig,
    LLMModelConfig,
)


class TestFitnessConfig:
    def test_valid_config(self) -> None:
        cfg = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            minimize=False,
        )
        assert cfg.primary == "spearman_rho"

    def test_weights_must_sum_to_one(self) -> None:
        with pytest.raises(ValueError, match=r"must sum to 1\.0"):
            FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.5, "top1_accuracy": 0.3},
            )

    def test_primary_must_be_in_weights(self) -> None:
        with pytest.raises(ValueError, match=r"primary.*must be in combined_weights"):
            FitnessConfig(
                primary="missing_key",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            )


class TestBackendConfig:
    def test_local_cuda_default(self) -> None:
        cfg = BackendConfig(type="local_cuda")
        assert cfg.device == "cuda:0"

    def test_unknown_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend type"):
            BackendConfig(type="quantum_computer")


class TestEvolveConfig:
    def test_from_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "config.yaml"
        p.write_text(
            yaml.dump(
                {
                    "target": "scoutgpt",
                    "description": "test run",
                    "fitness": {
                        "primary": "spearman_rho",
                        "combined_weights": {"spearman_rho": 0.7, "top1_accuracy": 0.3},
                    },
                    "evaluation": {"epochs": 5, "dataset": "test/dataset"},
                    "backend": {"type": "local_cuda"},
                    "llm": {
                        "models": [
                            {
                                "name": "test-model",
                                "weight": 1.0,
                                "api_base": "http://localhost",
                                "api_key_env": "TEST_KEY",
                            }
                        ],
                    },
                    "evolution": {"iterations": 10},
                }
            )
        )
        cfg = EvolveConfig.from_yaml(p)
        assert cfg.target == "scoutgpt"
        assert cfg.fitness.primary == "spearman_rho"
        assert cfg.evolution.iterations == 10


class TestLLMConfig:
    def test_model_weights_must_sum_to_one(self) -> None:
        with pytest.raises(ValueError, match=r"must sum to 1\.0"):
            LLMConfig(
                models=[
                    LLMModelConfig(name="a", weight=0.3, api_base="http://x", api_key_env="K"),
                    LLMModelConfig(name="b", weight=0.3, api_base="http://x", api_key_env="K"),
                ],
            )


class TestEvolutionConfigCodeEvolution:
    def test_default_is_false(self) -> None:
        cfg = EvolutionConfig()
        assert cfg.code_evolution is False

    def test_can_enable(self) -> None:
        cfg = EvolutionConfig(code_evolution=True)
        assert cfg.code_evolution is True
