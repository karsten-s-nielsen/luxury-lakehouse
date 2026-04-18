"""ScoutGPT search-space schema — extracted from evolve/evaluator.py for per-target dispatch."""

from __future__ import annotations

import logging
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

_log = logging.getLogger(__name__)

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
    """Typed schema for ScoutGPT candidate architecture configs."""

    model_config = ConfigDict(extra="allow")

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


def validate_candidate(config: dict[str, Any]) -> bool:
    """Validate ScoutGPT candidate config. Returns True on pass, False on reject (with logged reason)."""
    try:
        CandidateConfig(**config)
    except (ValidationError, ValueError) as exc:
        _log.warning("Search space rejection: %s", exc)
        return False
    return True
