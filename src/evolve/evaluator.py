"""Evaluator bridge between OpenEvolve's interface and ComputeBackend.

OpenEvolve calls ``evaluate(program_path) -> dict[str, float]`` for every
candidate.  This module loads the candidate config from the generated Python
file, validates it against the search space bounds, delegates training to
the configured :class:`~evolve.backends.base.ComputeBackend`, and computes a
combined fitness score from the weighted metrics.
"""

from __future__ import annotations

import ast
import logging
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from openevolve.evaluation_result import EvaluationResult
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from evolve.backends.base import ComputeBackend, fail_metrics
from evolve.code_validator import ValidationProfile, validate_program
from evolve.config import EvalConfig, FitnessConfig

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Typed candidate config with search space validation
# ---------------------------------------------------------------------------

_BOUNDS: dict[str, tuple[float, float]] = {
    "hidden_dim": (64, 512),
    "num_layers": (2, 12),
    "num_heads": (2, 16),
    "dropout": (0.0, 0.5),
    "learning_rate": (1e-5, 1e-2),
    "vaep_loss_weight": (0.0, 1.0),
    "player_prediction_weight": (0.0, 1.0),
    "batch_size": (64, 512),
}


class CandidateConfig(BaseModel):
    """Typed schema for candidate architecture configs.

    Defines all known fields with defaults so that typos in key names
    are surfaced: an unknown key like ``"hiddem_dim"`` goes into
    ``__pydantic_extra__`` and triggers a logged warning, while
    ``hidden_dim`` silently falls back to its default — the combination
    makes typos visible.

    Search space bounds and cross-field constraints (e.g. divisibility)
    are validated in the model validator.
    """

    model_config = ConfigDict(extra="allow")

    # Model architecture
    conditioning_type: Literal["additive", "cross_attention", "film", "gated"] = "additive"
    hidden_dim: int = 256
    num_layers: int = 6
    num_heads: int = 8
    dropout: float = 0.1
    max_seq_len: int = 128
    num_players: int = 100
    spatial_mlp_dim: int = 64
    vaep_loss_weight: float = 0.1
    player_prediction_weight: float = 0.0

    # Training hyperparams
    learning_rate: float = 1e-4
    weight_decay: float = 0.01
    batch_size: int = 256
    dataset: str = "luxury-lakehouse/scoutgpt-training-data"

    @field_validator("dataset")
    @classmethod
    def _validate_dataset_prefix(cls, v: str) -> str:
        if not v.startswith("luxury-lakehouse/"):
            msg = f"dataset must be a luxury-lakehouse/ HF repo, got '{v}'"
            raise ValueError(msg)
        return v

    @model_validator(mode="after")
    def _validate_search_space(self) -> CandidateConfig:
        for key, (lo, hi) in _BOUNDS.items():
            val = getattr(self, key, None)
            if val is not None and not (lo <= val <= hi):
                msg = f"{key}={val!r} not in [{lo}, {hi}]"
                raise ValueError(msg)
        if self.hidden_dim % self.num_heads != 0:
            msg = f"hidden_dim={self.hidden_dim} not divisible by num_heads={self.num_heads}"
            raise ValueError(msg)
        if self.__pydantic_extra__:
            _log.warning(
                "Candidate config has unrecognised keys (possible typos?): %s",
                sorted(self.__pydantic_extra__),
            )
        return self


def validate_search_space(config: dict[str, Any]) -> bool:
    """Validate *config* against the :class:`CandidateConfig` schema.

    Returns ``True`` if the config is valid, ``False`` otherwise.
    Invalid configs are logged at WARNING level with the rejection reason.
    """
    try:
        CandidateConfig(**config)
    except (ValidationError, ValueError) as exc:
        _log.warning("Search space rejection: %s", exc)
        return False
    return True


# ---------------------------------------------------------------------------
# Program dataclass (Level 2+)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Program:
    """Parsed evolve program — config dict + optional custom functions."""

    config: dict[str, Any]
    has_custom_embed: bool
    has_custom_layers: bool
    source_path: str


def _extract_config(tree: ast.Module, source: str, filename: str) -> dict[str, Any]:
    """Extract the config dict from a parsed AST via ast.literal_eval."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "config":
                value_source = ast.get_source_segment(source, node.value)
                if value_source is None:
                    msg = f"Cannot extract config value from {filename}"
                    raise ValueError(msg)
                raw = ast.literal_eval(value_source)
                if not isinstance(raw, dict):
                    msg = f"config must be a dict, got {type(raw).__name__} in {filename}"
                    raise ValueError(msg)
                return raw  # type: ignore[return-value]
    msg = f"No 'config = {{...}}' assignment found in {filename}"
    raise ValueError(msg)


def _load_program(program_path: str) -> Program:
    """Load an evolve program file, extracting config and detecting custom functions."""
    source = Path(program_path).read_text()
    tree = ast.parse(source, filename=program_path)

    # Extract config via AST (same as Level 1)
    config = _extract_config(tree, source, program_path)

    # Detect custom functions via AST walk (no execution)
    func_names = {node.name for node in ast.iter_child_nodes(tree) if isinstance(node, ast.FunctionDef)}

    return Program(
        config=config,
        has_custom_embed="custom_embed" in func_names,
        has_custom_layers="custom_layers" in func_names,
        source_path=program_path,
    )


# ---------------------------------------------------------------------------
# Config loader (Level 1 — kept for _EVALUATOR_SCRIPT compatibility)
# ---------------------------------------------------------------------------


def _load_config_from_program(program_path: str) -> dict[str, Any]:
    """Extract the ``config`` dict from a candidate ``.py`` file using AST parsing.

    Safely parses the file as an AST and extracts the literal value assigned
    to ``config`` without executing any code.  Only Python literal values
    (dicts, lists, strings, numbers, booleans, None) are supported — this is
    intentional to prevent code injection from LLM-generated candidates.

    Args:
        program_path: Filesystem path to the candidate ``.py`` file.

    Returns:
        The ``config`` dict defined in the file.

    Raises:
        ValueError: If the file has no ``config`` assignment, or the value
            is not a literal dict.
    """
    source = Path(program_path).read_text()
    tree = ast.parse(source, filename=program_path)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "config":
                value_source = ast.get_source_segment(source, node.value)
                if value_source is None:
                    msg = f"Cannot extract config value from '{program_path}'"
                    raise ValueError(msg)
                raw = ast.literal_eval(value_source)
                if not isinstance(raw, dict):
                    msg = f"'config' in '{program_path}' must be a dict, got {type(raw).__name__}"
                    raise ValueError(msg)
                return raw  # type: ignore[return-value]

    msg = f"No 'config' assignment found in '{program_path}'"
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


class EvolveEvaluator:
    """Bridge between OpenEvolve's evaluate interface and :class:`ComputeBackend`.

    OpenEvolve calls :meth:`evaluate` with the path to a generated Python
    program.  This class loads the candidate config, validates it, runs
    training via the backend, and returns a combined fitness score.
    """

    def __init__(
        self,
        backend: ComputeBackend,
        target: str,
        eval_config: EvalConfig,
        fitness_config: FitnessConfig,
        code_evolution: bool = False,
        validation_profile: ValidationProfile | None = None,
    ) -> None:
        self._backend = backend
        self._target = target
        self._eval_config = eval_config
        self._fitness_config = fitness_config
        self._code_evolution = code_evolution
        self._validation_profile = validation_profile

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, program_path: str) -> EvaluationResult:
        """Evaluate a candidate program and return fitness metrics.

        On any failure (bad config, training error, missing metrics) returns
        an :class:`EvaluationResult` with zero scores and an ``"error"``
        artifact containing the traceback or rejection reason, so the
        evolutionary loop can continue without crashing.

        Args:
            program_path: Path to the ``.py`` file containing a ``config`` dict
                and optional ``custom_embed`` / ``custom_layers`` functions.

        Returns:
            :class:`EvaluationResult` with at least ``"combined_score"`` in
            metrics and all raw metrics from the training backend.  On
            failure, an ``"error"`` artifact carries the traceback or
            rejection reason for the LLM prompt sampler.
        """
        try:
            program = _load_program(program_path)
        except Exception:
            _log.exception("Failed to load program %s", program_path)
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "load_error"},
                artifacts={"error": traceback.format_exc()},
            )

        config = program.config

        # When custom_embed is present, conditioning_type is ignored (the custom
        # function replaces the built-in conditioning).  Override to a valid value
        # so search-space validation doesn't reject creative type names the LLM
        # may invent (e.g. "adaptive_gating").
        if program.has_custom_embed and "conditioning_type" in config:
            config = {**config, "conditioning_type": "additive"}

        if not validate_search_space(config):
            _log.warning("Program %s rejected: search space validation failed", program_path)
            filename = Path(program_path).name
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "search_space"},
                artifacts={"error": f"Search space validation failed for {filename}"},
            )

        # Level 2 validation gate
        send_program_path: str | None = None
        if program.has_custom_embed or program.has_custom_layers:
            if self._validation_profile is None:
                _log.error("Level 2 program but no ValidationProfile configured")
                return EvaluationResult(
                    metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "no_profile"},
                    artifacts={"error": "Level 2 program but no ValidationProfile configured"},
                )
            source = Path(program_path).read_text()
            valid, reason = validate_program(
                source,
                self._validation_profile,
                code_evolution=self._code_evolution,
            )
            if not valid:
                _log.warning("Program %s rejected: %s", program_path, reason)
                return EvaluationResult(
                    metrics={**fail_metrics(), **self._fail_score(), "reject_reason": reason},
                    artifacts={"error": f"Code validation rejected: {reason}"},
                )
            send_program_path = program_path

        train_kwargs: dict[str, Any] = {
            "candidate_config": config,
            "target": self._target,
            "epochs": self._eval_config.epochs,
            "seed": self._eval_config.seed,
        }
        if send_program_path is not None:
            train_kwargs["program_path"] = send_program_path

        try:
            metrics = self._backend.train(**train_kwargs)
        except Exception:
            _log.exception("Backend training failed for %s", program_path)
            return EvaluationResult(
                metrics={**fail_metrics(), **self._fail_score(), "reject_reason": "backend_error"},
                artifacts={"error": traceback.format_exc()},
            )

        # Pop _error_text from metrics (if present) and surface as artifact
        error_text = metrics.pop("_error_text", None)
        combined = self._compute_combined_score(metrics)
        result_metrics = {**metrics, "combined_score": combined}

        if error_text is not None:
            return EvaluationResult(metrics=result_metrics, artifacts={"error": error_text})
        return EvaluationResult.from_dict(result_metrics)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _compute_combined_score(self, metrics: dict[str, float]) -> float:
        """Compute the weighted combined score from *metrics*."""
        weights = self._fitness_config.combined_weights
        if not weights:
            return metrics.get(self._fitness_config.primary, 0.0)
        score = 0.0
        for key, w in weights.items():
            score += w * metrics.get(key, 0.0)
        return score

    def _fail_score(self) -> dict[str, float]:
        """Return a zero-score dict for all configured fitness weight keys."""
        return {key: 0.0 for key in self._fitness_config.combined_weights}
