"""Tests for the Evolve evaluator bridge and search space validation."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from openevolve.evaluation_result import EvaluationResult

from evolve.code_validator import ValidationProfile
from evolve.config import EvalConfig, FitnessConfig
from evolve.evaluator import EvolveEvaluator, Program, _load_program, validate_search_space
from evolve.runner import _build_parser, _eval_fingerprint, _evaluate_seeds, _load_cached_seeds, _write_evaluator_script

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
        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == pytest.approx(expected_score)
        assert result.metrics["spearman_rho"] == pytest.approx(0.5)
        assert result.metrics["top1_accuracy"] == pytest.approx(0.6)

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

            assert isinstance(result, EvaluationResult)
            assert result.metrics["combined_score"] == pytest.approx(0.7 * 0.42 + 0.3 * 0.75)
            assert result.metrics["spearman_rho"] == pytest.approx(0.42)
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

        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == 0.0
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


# ---------------------------------------------------------------------------
# Program dataclass / _load_program tests
# ---------------------------------------------------------------------------


class TestLoadProgram:
    def test_config_only(self, tmp_path: Path) -> None:
        """Level 1 program returns Program with no custom functions."""
        prog = tmp_path / "config_only.py"
        prog.write_text('config = {"hidden_dim": 256, "num_layers": 6}\n')
        result = _load_program(str(prog))
        assert isinstance(result, Program)
        assert result.config == {"hidden_dim": 256, "num_layers": 6}
        assert result.has_custom_embed is False
        assert result.has_custom_layers is False
        assert result.source_path == str(prog)

    def test_with_custom_embed(self, tmp_path: Path) -> None:
        prog = tmp_path / "with_embed.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_embed(self, x, y):
                return x + y
        """)
        )
        result = _load_program(str(prog))
        assert result.has_custom_embed is True
        assert result.has_custom_layers is False

    def test_with_custom_layers_and_embed(self, tmp_path: Path) -> None:
        prog = tmp_path / "with_both.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 256}

            def custom_layers(hidden_dim):
                return {"gate": None}

            def custom_embed(self, x, y):
                return x + y
        """)
        )
        result = _load_program(str(prog))
        assert result.has_custom_embed is True
        assert result.has_custom_layers is True

    def test_no_config_raises(self, tmp_path: Path) -> None:
        prog = tmp_path / "no_config.py"
        prog.write_text("x = 42\n")
        with pytest.raises(ValueError, match="config"):
            _load_program(str(prog))


# ---------------------------------------------------------------------------
# Evaluator validation gate tests (Level 2)
# ---------------------------------------------------------------------------


class TestEvaluatorValidationGate:
    """Tests that the evaluator rejects invalid Level 2 programs before dispatch."""

    _PROFILE = ValidationProfile(
        patch_method="_embed",
        patch_signature=["self", "x", "y"],
        return_shape="(batch, hidden_dim)",
        known_model_attrs=frozenset({"linear"}),
        allowed_namespaces=frozenset({"torch", "math"}),
        layers_args=["hidden_dim"],
        rejected_builtins=frozenset({"eval", "exec", "open"}),
    )

    def test_invalid_program_returns_zero_score(self, tmp_path: Path) -> None:
        """Program with import should be rejected before backend is called."""
        prog = tmp_path / "bad.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                import os
                return x + y
        """)
        )
        backend = MagicMock()
        backend.train = MagicMock(return_value={"combined_score": 1.0})
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="combined_score",
                combined_weights={"combined_score": 1.0},
            ),
            code_evolution=True,
            validation_profile=self._PROFILE,
        )
        result = evaluator.evaluate(str(prog))
        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == 0.0
        backend.train.assert_not_called()

    def test_valid_config_only_dispatches(self, tmp_path: Path) -> None:
        """Config-only program should pass and reach the backend."""
        prog = tmp_path / "good.py"
        prog.write_text(
            'config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,'
            ' "conditioning_type": "additive", "dropout": 0.1}\n'
        )
        backend = MagicMock()
        backend.train = MagicMock(
            return_value={
                "spearman_rho": 0.5,
                "top1_accuracy": 0.8,
            }
        )
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            ),
            code_evolution=False,
            validation_profile=self._PROFILE,
        )
        result = evaluator.evaluate(str(prog))
        assert isinstance(result, EvaluationResult)
        assert result.metrics["spearman_rho"] == 0.5
        backend.train.assert_called_once()

    def test_code_evolution_disabled_rejects(self, tmp_path: Path) -> None:
        prog = tmp_path / "has_code.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                return x + y
        """)
        )
        backend = MagicMock()
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="combined_score",
                combined_weights={"combined_score": 1.0},
            ),
            code_evolution=False,
            validation_profile=self._PROFILE,
        )
        result = evaluator.evaluate(str(prog))
        assert isinstance(result, EvaluationResult)
        assert result.metrics["combined_score"] == 0.0
        backend.train.assert_not_called()

    def test_level2_passes_program_path(self, tmp_path: Path) -> None:
        prog = tmp_path / "l2.py"
        prog.write_text(
            textwrap.dedent("""\
            config = {"hidden_dim": 256, "num_layers": 6, "num_heads": 8,
                      "conditioning_type": "additive", "dropout": 0.1}

            def custom_embed(self, x, y):
                return self.linear(x) + y
        """)
        )
        backend = MagicMock()
        backend.train = MagicMock(
            return_value={
                "spearman_rho": 0.5,
                "top1_accuracy": 0.8,
            }
        )
        evaluator = EvolveEvaluator(
            backend=backend,
            target="scoutgpt",
            eval_config=EvalConfig(),
            fitness_config=FitnessConfig(
                primary="spearman_rho",
                combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
            ),
            code_evolution=True,
            validation_profile=self._PROFILE,
        )
        evaluator.evaluate(str(prog))
        # Backend should have received program_path
        call_kwargs = backend.train.call_args.kwargs
        assert call_kwargs.get("program_path") == str(prog)


# ---------------------------------------------------------------------------
# Runner --code-evolution flag tests
# ---------------------------------------------------------------------------


class TestRunnerCodeEvolutionFlag:
    def test_default_is_false(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--target", "scoutgpt"])
        assert args.code_evolution is False

    def test_flag_enables(self) -> None:
        parser = _build_parser()
        args = parser.parse_args(["--target", "scoutgpt", "--code-evolution"])
        assert args.code_evolution is True


# ---------------------------------------------------------------------------
# Error text capture tests (EvaluationResult artifact pipeline)
# ---------------------------------------------------------------------------


class TestErrorTextCapture:
    """Tests that the evaluator correctly surfaces error text as EvaluationResult artifacts."""

    def test_error_text_returned_on_backend_error(self, tmp_path: Path) -> None:
        """When backend returns _error_text in metrics, evaluator returns EvaluationResult
        with an 'error' artifact containing the text, and _error_text is NOT in metrics."""
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "spearman_rho": 0.0,
            "top1_accuracy": 0.0,
            "_error_text": "Traceback (most recent call last):\n  RuntimeError: CUDA OOM",
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend, target="scoutgpt", eval_config=EvalConfig(), fitness_config=fitness
        )
        result = evaluator.evaluate(str(candidate_path))

        assert isinstance(result, EvaluationResult)
        assert result.has_artifacts()
        assert "error" in result.artifacts
        assert "CUDA OOM" in str(result.artifacts["error"])
        assert "_error_text" not in result.metrics

    def test_no_artifact_on_success(self, tmp_path: Path) -> None:
        """Successful evaluation returns EvaluationResult with no artifacts."""
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
        evaluator = EvolveEvaluator(
            backend=mock_backend, target="scoutgpt", eval_config=EvalConfig(), fitness_config=fitness
        )
        result = evaluator.evaluate(str(candidate_path))

        assert isinstance(result, EvaluationResult)
        assert not result.has_artifacts()

    def test_error_text_stripped_before_combined_score(self, tmp_path: Path) -> None:
        """_error_text in metrics doesn't affect combined_score computation."""
        candidate_path = _write_candidate(tmp_path, VALID_CONFIG)

        mock_backend = MagicMock()
        mock_backend.train.return_value = {
            "spearman_rho": 0.5,
            "top1_accuracy": 0.6,
            "_error_text": "some error traceback",
        }

        fitness = FitnessConfig(
            primary="spearman_rho",
            secondary="top1_accuracy",
            combined_weights={"spearman_rho": 0.7, "top1_accuracy": 0.3},
        )
        evaluator = EvolveEvaluator(
            backend=mock_backend, target="scoutgpt", eval_config=EvalConfig(), fitness_config=fitness
        )
        result = evaluator.evaluate(str(candidate_path))

        expected_score = 0.7 * 0.5 + 0.3 * 0.6
        assert result.metrics["combined_score"] == pytest.approx(expected_score)
        assert "_error_text" not in result.metrics


# ---------------------------------------------------------------------------
# Remote worker error capture tests
# ---------------------------------------------------------------------------


class TestRemoteWorkerErrorCapture:
    """Tests that the remote worker captures tracebacks and outputs valid JSON on error."""

    def test_remote_worker_captures_traceback(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """When train_and_evaluate raises, remote worker outputs JSON with _error_text."""
        import io

        # Write a dummy candidate.json
        candidate = tmp_path / "candidate.json"
        candidate.write_text(json.dumps(VALID_CONFIG))

        # Mock module whose train_and_evaluate raises
        mock_module = MagicMock()
        mock_module.train_and_evaluate.side_effect = RuntimeError("CUDA out of memory")
        monkeypatch.setattr("importlib.import_module", lambda *a, **kw: mock_module)

        # Capture stdout
        captured = io.StringIO()
        monkeypatch.setattr(sys, "stdout", captured)

        # Set argv. Use the explicit 3-arg form: pytest 9.1.1 broke the 2-arg
        # string-target form (`setattr("sys.argv", ...)`) for this test — main()
        # then read pytest's own argv instead of the mocked one. The 3-arg form
        # (module object + attr name) is unambiguous across pytest versions.
        monkeypatch.setattr(sys, "argv", ["remote_worker", str(candidate), "cpu", "1", "42", "scoutgpt"])

        from evolve.remote_worker import main

        main()

        output = captured.getvalue().strip()
        result = json.loads(output)

        assert result["combined_score"] == 0.0
        assert result["error"] == 1.0
        assert "_error_text" in result
        assert "CUDA out of memory" in result["_error_text"]


# ---------------------------------------------------------------------------
# Per-target search-space dispatch (D1 — see EV1 spec)
# ---------------------------------------------------------------------------


class TestPerTargetDispatch:
    """Regression guard for the target-aware validate_search_space dispatcher."""

    def test_scoutgpt_dispatch(self) -> None:
        """validate_search_space(cfg, target='scoutgpt') accepts a valid ScoutGPT config."""
        assert validate_search_space(VALID_CONFIG, target="scoutgpt") is True

    def test_default_target_is_scoutgpt(self) -> None:
        """Backward-compat: validate_search_space(cfg) without target still validates as ScoutGPT."""
        assert validate_search_space(VALID_CONFIG) is True

    def test_unknown_target_returns_false(self, caplog: pytest.LogCaptureFixture) -> None:
        """Unknown target name is logged at ERROR and rejected (not raised)."""
        with caplog.at_level(logging.ERROR, logger="evolve.evaluator"):
            result = validate_search_space(VALID_CONFIG, target="nonexistent")
        assert result is False
        assert any("nonexistent" in rec.message and rec.levelname == "ERROR" for rec in caplog.records), (
            f"Expected ERROR log mentioning 'nonexistent'; got: {[(r.levelname, r.message) for r in caplog.records]}"
        )

    def test_target_module_without_validate_candidate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A target module missing `validate_candidate` is logged at ERROR and returns False."""
        # Build a fake target package under a temp dir and put it on sys.path.
        pkg = tmp_path / "evolve" / "targets" / "faulty"
        pkg.mkdir(parents=True)
        (tmp_path / "evolve" / "__init__.py").write_text("")
        (tmp_path / "evolve" / "targets" / "__init__.py").write_text("")
        (pkg / "__init__.py").write_text("")
        # search_space module exists but has no `validate_candidate` attribute.
        (pkg / "search_space.py").write_text("# intentionally empty\n")

        monkeypatch.syspath_prepend(str(tmp_path))
        # Drop any cached evolve.targets.faulty imports so the new path wins.
        for mod in list(sys.modules):
            if mod.startswith("evolve.targets.faulty"):
                del sys.modules[mod]

        with caplog.at_level(logging.ERROR, logger="evolve.evaluator"):
            result = validate_search_space(VALID_CONFIG, target="faulty")
        assert result is False
        assert any("faulty" in rec.message and rec.levelname == "ERROR" for rec in caplog.records), (
            f"Expected ERROR log mentioning 'faulty'; got: {[(r.levelname, r.message) for r in caplog.records]}"
        )
