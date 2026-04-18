"""Tests for the Football2Vec evolve target (search space, evaluator wiring, seed programs, workflow card)."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from openevolve.evaluation_result import EvaluationResult

from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator, _load_program, validate_search_space
from evolve.targets.football2vec.search_space import CandidateConfig, validate_candidate

# ---------------------------------------------------------------------------
# Search-space validation
# ---------------------------------------------------------------------------

VALID_CONFIG: dict[str, Any] = {
    "hidden_dim": 128,
    "num_layers": 4,
    "num_heads": 4,
    "dropout": 0.1,
    "mask_prob": 0.15,
    "spatial_mlp_dim": 64,
    "pooling_type": "mean",
    "spatial_injection": "additive",
    "position_embedding": "learnable",
    "learning_rate": 1e-4,
    "batch_size": 256,
}


class TestSearchSpace:
    def test_valid_config_passes(self) -> None:
        assert validate_candidate(VALID_CONFIG) is True

    @pytest.mark.parametrize(
        ("key", "bad_value"),
        [
            ("hidden_dim", 32),
            ("hidden_dim", 512),
            ("num_layers", 1),
            ("num_layers", 12),
            ("num_heads", 1),
            ("num_heads", 16),
            ("dropout", -0.1),
            ("dropout", 0.6),
            ("mask_prob", 0.05),
            ("mask_prob", 0.50),
            ("spatial_mlp_dim", 8),
            ("spatial_mlp_dim", 256),
            ("learning_rate", 1e-6),
            ("learning_rate", 1e-2),
            ("batch_size", 32),
            ("batch_size", 1024),
        ],
    )
    def test_out_of_range_rejected(self, key: str, bad_value: float) -> None:
        cfg = {**VALID_CONFIG, key: bad_value}
        assert validate_candidate(cfg) is False

    def test_divisibility_rejected(self) -> None:
        """hidden_dim must be divisible by num_heads."""
        cfg = {**VALID_CONFIG, "hidden_dim": 130, "num_heads": 8}
        assert validate_candidate(cfg) is False

    @pytest.mark.parametrize(
        ("key", "bad_value"),
        [
            ("pooling_type", "max"),
            ("pooling_type", "sum"),
            ("spatial_injection", "cross_attention"),
            ("spatial_injection", "stack"),
            ("position_embedding", "alibi"),
            ("position_embedding", "absolute"),
        ],
    )
    def test_invalid_enum_rejected(self, key: str, bad_value: str) -> None:
        cfg = {**VALID_CONFIG, key: bad_value}
        assert validate_candidate(cfg) is False

    def test_concat_guard_rejected(self) -> None:
        """spatial_injection='concat' requires spatial_mlp_dim <= hidden_dim/2."""
        cfg = {**VALID_CONFIG, "spatial_injection": "concat", "spatial_mlp_dim": 96, "hidden_dim": 128}
        assert validate_candidate(cfg) is False

    def test_concat_guard_accepts_within_bound(self) -> None:
        cfg = {**VALID_CONFIG, "spatial_injection": "concat", "spatial_mlp_dim": 32, "hidden_dim": 128}
        assert validate_candidate(cfg) is True

    def test_dataset_prefix_enforced(self) -> None:
        cfg = {**VALID_CONFIG, "dataset": "some-other-org/dataset"}
        assert validate_candidate(cfg) is False


# ---------------------------------------------------------------------------
# Evaluator wiring (mocked backend)
# ---------------------------------------------------------------------------


def _write_candidate(tmp_path: Path, config: dict[str, Any]) -> Path:
    p = tmp_path / "candidate.py"
    p.write_text(textwrap.dedent(f"config = {config!r}\n"))
    return p


class TestEvaluatorWiring:
    def test_dispatch_to_backend(self, tmp_path: Path) -> None:
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "val_accuracy": 0.62,
            "val_loss": 1.42,
            "param_count": 700_000.0,
            "epochs_trained": 5.0,
            "training_time_seconds": 240.0,
        }

        fitness = FitnessConfig(primary="val_accuracy", combined_weights={"val_accuracy": 1.0})
        evaluator = EvolveEvaluator(
            backend=mock_backend,
            target="football2vec",
            eval_config=EvalConfig(epochs=5, seed=42),
            fitness_config=fitness,
        )
        result = evaluator.evaluate(str(candidate_path))

        mock_backend.train.assert_called_once_with(
            candidate_config=VALID_CONFIG,
            target="football2vec",
            epochs=5,
            seed=42,
        )
        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == pytest.approx(0.62)
        assert result.metrics["val_accuracy"] == pytest.approx(0.62)


# ---------------------------------------------------------------------------
# Seed programs load + validate
# ---------------------------------------------------------------------------


_REPO = Path(__file__).resolve().parents[2]
_SEEDS_DIR = _REPO / "src" / "evolve" / "targets" / "football2vec" / "seed_programs"


class TestSeedPrograms:
    def test_seven_seeds_present(self) -> None:
        seeds = sorted(p.stem for p in _SEEDS_DIR.glob("*.py") if p.name != "__init__.py")
        expected = sorted(
            ["baseline", "wider", "deeper", "heavier_mask", "attention_pool", "film_spatial", "sinusoidal_pos"]
        )
        assert seeds == expected

    @pytest.mark.parametrize(
        "seed_name",
        ["baseline", "wider", "deeper", "heavier_mask", "attention_pool", "film_spatial", "sinusoidal_pos"],
    )
    def test_seed_program_loads_and_validates(self, seed_name: str) -> None:
        path = _SEEDS_DIR / f"{seed_name}.py"
        prog = _load_program(str(path))
        assert validate_candidate(prog.config) is True, f"seed {seed_name!r} fails validation: {prog.config}"


# ---------------------------------------------------------------------------
# Workflow card parses
# ---------------------------------------------------------------------------


class TestWorkflowCard:
    def test_card_parses_and_links_back(self) -> None:
        from workflows.card import WorkflowCard

        path = _REPO / "workflow-cards" / "wf-evolve-football2vec.yaml"
        card = WorkflowCard.from_yaml_file(path)
        assert card.id == "wf-evolve-football2vec"
        assert card.links is not None
        assert any("evolve/targets/football2vec" in s for s in card.links.source_code)
        assert "wf-football2vec-v2" in card.depends_on


# ---------------------------------------------------------------------------
# CandidateConfig pydantic warnings on extra keys (typo detection)
# ---------------------------------------------------------------------------


class TestExtraKeyWarning:
    def test_unknown_key_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        cfg = {**VALID_CONFIG, "hiddem_dim": 256}  # typo
        with caplog.at_level("WARNING", logger="evolve.targets.football2vec.search_space"):
            CandidateConfig(**cfg)
        assert any("hiddem_dim" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Per-target dispatch — verify football2vec resolves via the evaluator.py dispatcher
# ---------------------------------------------------------------------------


class TestPerTargetDispatch:
    def test_dispatcher_routes_football2vec(self) -> None:
        assert validate_search_space(VALID_CONFIG, target="football2vec") is True

    def test_dispatcher_rejects_invalid_football2vec(self) -> None:
        cfg = {**VALID_CONFIG, "pooling_type": "max"}
        assert validate_search_space(cfg, target="football2vec") is False
