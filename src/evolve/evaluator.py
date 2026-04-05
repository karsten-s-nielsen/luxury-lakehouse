"""Evaluator bridge between OpenEvolve's interface and ComputeBackend.

OpenEvolve calls ``evaluate(program_path) -> dict[str, float]`` for every
candidate.  This module loads the candidate config from the generated Python
file, validates it against the search space bounds, delegates training to
the configured :class:`~evolve.backends.base.ComputeBackend`, and computes a
combined fitness score from the weighted metrics.
"""

from __future__ import annotations

import importlib.util
import logging
from typing import Any

from evolve.backends.base import ComputeBackend
from evolve.config import EvalConfig, FitnessConfig

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Search space bounds
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

_VALID_CONDITIONING_TYPES: frozenset[str] = frozenset({"additive", "cross_attention", "film", "gated"})

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_search_space(config: dict[str, Any]) -> bool:
    """Validate *config* against known search space bounds.

    Returns ``True`` if the config is valid, ``False`` otherwise.
    Invalid configs are logged at WARNING level with the rejection reason.
    """
    for key, (lo, hi) in _BOUNDS.items():
        val = config.get(key)
        if val is None:
            continue
        if not (lo <= val <= hi):
            _log.warning(
                "Search space rejection: %s=%r not in [%s, %s]",
                key,
                val,
                lo,
                hi,
            )
            return False

    # num_heads must evenly divide hidden_dim
    hidden_dim = config.get("hidden_dim")
    num_heads = config.get("num_heads")
    if hidden_dim is not None and num_heads is not None and hidden_dim % num_heads != 0:
        _log.warning(
            "Search space rejection: hidden_dim=%d not divisible by num_heads=%d",
            hidden_dim,
            num_heads,
        )
        return False

    # conditioning_type must be in the allowed set
    cond = config.get("conditioning_type")
    if cond is not None and cond not in _VALID_CONDITIONING_TYPES:
        _log.warning(
            "Search space rejection: conditioning_type=%r not in %s",
            cond,
            sorted(_VALID_CONDITIONING_TYPES),
        )
        return False

    return True


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def _load_config_from_program(program_path: str) -> dict[str, Any]:
    """Dynamically load a Python file and extract its ``config`` dict.

    Args:
        program_path: Filesystem path to the candidate ``.py`` file generated
            by the LLM.

    Returns:
        The ``config`` dict defined in the loaded module.

    Raises:
        ImportError: If the module cannot be loaded.
        ValueError: If the loaded module has no ``config`` attribute or it is
            not a dict.
    """
    spec = importlib.util.spec_from_file_location("_candidate", program_path)
    if spec is None or spec.loader is None:
        msg = f"Cannot create import spec for '{program_path}'"
        raise ImportError(msg)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    raw = getattr(module, "config", None)
    if raw is None:
        msg = f"Module '{program_path}' has no 'config' attribute"
        raise ValueError(msg)
    if not isinstance(raw, dict):
        msg = f"'config' in '{program_path}' must be a dict, got {type(raw).__name__}"
        raise ValueError(msg)

    return raw  # type: ignore[return-value]


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
    ) -> None:
        self._backend = backend
        self._target = target
        self._eval_config = eval_config
        self._fitness_config = fitness_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, program_path: str) -> dict[str, float]:
        """Evaluate a candidate program and return fitness metrics.

        On any failure (bad config, training error, missing metrics) returns
        ``{"combined_score": 0.0, ...}`` so that the evolutionary loop can
        continue without crashing.

        Args:
            program_path: Path to the ``.py`` file containing a ``config`` dict.

        Returns:
            Mapping with at least ``"combined_score"`` and all raw metrics
            from the training backend.
        """
        try:
            config = _load_config_from_program(program_path)
        except (ImportError, ValueError):
            _log.warning("Failed to load config from '%s'", program_path, exc_info=True)
            return self._zero_result()

        if not validate_search_space(config):
            return self._zero_result()

        try:
            metrics = self._backend.train(
                candidate_config=config,
                target=self._target,
                epochs=self._eval_config.epochs,
                seed=self._eval_config.seed,
            )
        except Exception:
            _log.warning("Backend training failed for '%s'", program_path, exc_info=True)
            return self._zero_result()

        combined = self._compute_combined_score(metrics)
        return {**metrics, "combined_score": combined}

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

    def _zero_result(self) -> dict[str, float]:
        """Return a safe zero-score result dict."""
        result: dict[str, float] = {"combined_score": 0.0}
        for key in self._fitness_config.combined_weights:
            result[key] = 0.0
        return result
