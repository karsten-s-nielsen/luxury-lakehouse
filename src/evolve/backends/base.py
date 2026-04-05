"""ComputeBackend Protocol — the structural interface all backends must satisfy."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


def fail_metrics() -> dict[str, float]:
    """Return a fresh failure-result dict.

    Shared sentinel used by all backends and the pool to signal that an
    evaluation failed without propagating exceptions to the evolution loop.
    Returns a new dict each call to avoid shared mutable state.
    """
    return {"combined_score": 0.0, "error": 1.0}


@runtime_checkable
class ComputeBackend(Protocol):
    """Structural protocol for compute backends used by the Evolve engine.

    Any class that implements ``train`` and ``available`` with the correct
    signatures satisfies this protocol — no explicit inheritance required.
    """

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Train a candidate architecture and return scalar evaluation metrics.

        Args:
            candidate_config: Architecture hyper-parameters produced by the LLM.
            target: Short name of the target domain (``evolve.targets.<target>.evaluator``).
            epochs: Number of training epochs to run.
            seed: Random seed for reproducibility.

        Returns:
            Mapping of metric name → float value (e.g. ``{"top1_acc": 0.815}``).
        """
        ...

    def available(self) -> bool:
        """Return True if this backend can accept work right now."""
        ...
