"""LocalCudaBackend — runs training evaluations on a local CUDA device."""

from __future__ import annotations

import importlib
import logging
from typing import Any

_log = logging.getLogger(__name__)


class LocalCudaBackend:
    """Compute backend that dispatches training to a local CUDA GPU.

    The evaluator for each target is loaded lazily via
    ``evolve.targets.<target>.evaluator`` so this module has no hard
    dependency on any particular target implementation.
    """

    def __init__(self, device: str = "cuda:0") -> None:
        self._device = device
        _log.info("LocalCudaBackend initialised", extra={"device": device})

    # ------------------------------------------------------------------
    # ComputeBackend protocol
    # ------------------------------------------------------------------

    def train(
        self,
        candidate_config: dict[str, Any],
        target: str,
        epochs: int,
        seed: int,
    ) -> dict[str, float]:
        """Load the target evaluator and run ``train_and_evaluate``.

        Args:
            candidate_config: Architecture hyper-parameters produced by the LLM.
            target: Short name of the target domain; the evaluator module is
                resolved as ``evolve.targets.<target>.evaluator``.
            epochs: Number of training epochs to run.
            seed: Random seed for reproducibility.

        Returns:
            Scalar evaluation metrics returned by ``train_and_evaluate``.
        """
        module_path = f"evolve.targets.{target}.evaluator"
        _log.info(
            "LocalCudaBackend.train starting",
            extra={"target": target, "epochs": epochs, "seed": seed, "device": self._device},
        )
        evaluator = importlib.import_module(module_path)
        metrics: dict[str, float] = evaluator.train_and_evaluate(
            candidate_config=candidate_config,
            device=self._device,
            epochs=epochs,
            seed=seed,
        )
        _log.info(
            "LocalCudaBackend.train complete",
            extra={"target": target, "metrics": metrics},
        )
        return metrics

    def available(self) -> bool:
        """Return True when a CUDA device is accessible on this machine."""
        try:
            import torch  # type: ignore[import-untyped]

            return bool(torch.cuda.is_available())
        except ImportError:
            _log.warning("torch not installed; LocalCudaBackend unavailable")
            return False
