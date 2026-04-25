"""Stage-2 adversary configuration, lambda schedule, and module registry.

Central home for Football2Vec v2 stage-2 adversarial training configuration.
The module replaces the training loop's hardcoded TeamClassifierHead + linear
lambda ramp with a typed config + registry + schedule-function trio, enabling:

- Byte-equivalent backward compatibility at production defaults
- L1 config-space search over three axes (lambda_schedule_shape, lambda_max,
  lambda_warmup_epochs) in Phase 2 of EV2
- L2 code-space search over the adversary head architecture via the registry
  (_ADVERSARY_REGISTRY) which grows with promoted variants from EV2 Phase 2

References:
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks."
    JMLR 17(1), pp. 1-35.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import torch
import torch.nn as nn

from analytics.football2vec_transformer import GradientReversalLayer


@dataclass(frozen=True)
class AdversaryConfig:
    """Stage-2 adversary configuration.

    Attributes:
        architecture: Named entry in _ADVERSARY_REGISTRY. "linear" matches
            the current production TeamClassifierHead byte-for-byte.
        lambda_schedule_shape: Ramp shape for the gradient-reversal lambda
            across training epochs.
        lambda_max: Peak lambda value reached at epoch = lambda_warmup_epochs.
        lambda_warmup_epochs: Number of epochs to ramp from 0 to lambda_max.
    """

    architecture: Literal["linear"] = "linear"
    lambda_schedule_shape: Literal["linear", "sigmoid", "cosine"] = "linear"
    lambda_max: float = 0.2
    lambda_warmup_epochs: int = 5


def lambda_schedule(cfg: AdversaryConfig, epoch: int, total_epochs: int) -> float:
    """Compute the gradient-reversal lambda at a given epoch.

    Three shapes, all (0 -> lambda_max) over lambda_warmup_epochs, then flat at lambda_max:

    - linear: lambda = lambda_max * min(epoch / warmup, 1.0)
    - sigmoid: lambda = lambda_max * sigmoid(10 * (progress - 0.5))
        where progress = min(epoch / warmup, 1.0); Ganin-2016 style DANN schedule
    - cosine: lambda = lambda_max * 0.5 * (1 - cos(pi * progress)); smoothly rises
        from 0 at epoch 0 to lambda_max at epoch = warmup, then holds

    Args:
        cfg: Adversary config — reads lambda_schedule_shape, lambda_max, lambda_warmup_epochs.
        epoch: Current epoch (0-indexed).
        total_epochs: Total epochs in the training run — reserved for shape
            variants that interpolate across the full run rather than the warmup window.

    Returns:
        Lambda value for this epoch, in [0.0, lambda_max].

    Raises:
        ValueError: unknown lambda_schedule_shape.
    """
    del total_epochs  # reserved for future shape variants that use it
    warmup = max(1, cfg.lambda_warmup_epochs)
    progress = min(epoch / warmup, 1.0)

    if cfg.lambda_schedule_shape == "linear":
        return cfg.lambda_max * progress

    if cfg.lambda_schedule_shape == "sigmoid":
        # Standard Ganin-2016 DANN sigmoid ramp, centered at progress=0.5.
        # Scale factor 10 chosen to saturate within the warmup window.
        return cfg.lambda_max * (1.0 / (1.0 + math.exp(-10.0 * (progress - 0.5))))

    if cfg.lambda_schedule_shape == "cosine":
        return cfg.lambda_max * 0.5 * (1.0 - math.cos(math.pi * progress))

    msg = f"unknown lambda_schedule_shape {cfg.lambda_schedule_shape!r}; expected linear|sigmoid|cosine"
    raise ValueError(msg)


class LinearAdversaryHead(nn.Module):
    """Baseline adversary — CLS pool, GRL, single Linear classifier.

    Byte-equivalent wiring to football2vec_transformer.TeamClassifierHead,
    but with the EV2 (encoder_output, attention_mask) -> logits signature.
    CLS pooling picks encoder_output[:, 0].

    Args:
        hidden_dim: Input feature dimension.
        num_competitions: Number of competition classes.
    """

    def __init__(self, hidden_dim: int, num_competitions: int) -> None:
        super().__init__()
        # lambda_val is injected per-epoch by the training loop before the forward pass.
        self.grl = GradientReversalLayer(lambda_val=1.0)
        self.classifier = nn.Linear(hidden_dim, num_competitions)

    def forward(self, encoder_output: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """CLS-pool, GRL, then Linear classifier.

        Args:
            encoder_output: (B, S, hidden_dim) per-token encoder output.
            attention_mask: (B, S) bool — unused by the linear baseline (CLS is always valid).

        Returns:
            (B, num_competitions) unnormalized logits.
        """
        del attention_mask  # CLS is always a valid position
        cls = encoder_output[:, 0]
        return self.classifier(self.grl(cls))


def _build_linear_head(hidden_dim: int, num_competitions: int) -> nn.Module:
    return LinearAdversaryHead(hidden_dim, num_competitions)


_ADVERSARY_REGISTRY: dict[str, Callable[[int, int], nn.Module]] = {
    "linear": _build_linear_head,
}


def build_adversary(cfg: AdversaryConfig, hidden_dim: int, num_competitions: int) -> nn.Module:
    """Build the adversary module for cfg.architecture from the registry.

    Args:
        cfg: Adversary config — only cfg.architecture is read.
        hidden_dim: Encoder hidden dimension.
        num_competitions: Number of competition classes.

    Returns:
        nn.Module taking (encoder_output, attention_mask) -> (B, num_competitions) logits.

    Raises:
        ValueError: cfg.architecture is not registered in _ADVERSARY_REGISTRY.
    """
    builder = _ADVERSARY_REGISTRY.get(cfg.architecture)
    if builder is None:
        msg = f"unknown architecture {cfg.architecture!r}; registered: {sorted(_ADVERSARY_REGISTRY)}"
        raise ValueError(msg)
    return builder(hidden_dim, num_competitions)
