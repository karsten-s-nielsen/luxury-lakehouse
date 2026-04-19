"""Football2Vec search-space schema for the Evolve engine (Level 1)."""

from __future__ import annotations

import logging
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

_log = logging.getLogger(__name__)

_BOUNDS: dict[str, tuple[float, float]] = {
    "hidden_dim": (64, 256),
    "num_layers": (2, 8),
    "num_heads": (2, 8),
    "dropout": (0.0, 0.4),
    "mask_prob": (0.10, 0.30),
    "spatial_mlp_dim": (16, 128),
    "learning_rate": (1e-5, 1e-3),
    "batch_size": (64, 512),
}


class CandidateConfig(BaseModel):
    """Typed schema for Football2Vec stage-1 candidate configs.

    Defines all known fields with defaults so that typos in key names are
    surfaced: an unknown key like ``"hiddem_dim"`` goes into
    ``__pydantic_extra__`` and triggers a logged warning.
    """

    model_config = ConfigDict(extra="allow")

    # Architecture (scalars)
    hidden_dim: int = 128
    num_layers: int = 4
    num_heads: int = 4
    dropout: float = 0.1
    mask_prob: float = 0.15
    spatial_mlp_dim: int = 64

    # Architecture (enums)
    pooling_type: Literal["mean", "attention", "cls"] = "mean"
    spatial_injection: Literal["additive", "concat", "film"] = "additive"
    position_embedding: Literal["learnable", "sinusoidal", "rope"] = "learnable"

    # Training hyperparams
    learning_rate: float = 1e-4
    batch_size: int = 256
    dataset: str = "luxury-lakehouse/football2vec-training-data"

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
        if self.spatial_injection == "concat" and self.spatial_mlp_dim > self.hidden_dim // 2:
            msg = (
                f"spatial_injection='concat' requires spatial_mlp_dim <= hidden_dim/2, "
                f"got {self.spatial_mlp_dim} > {self.hidden_dim // 2}"
            )
            raise ValueError(msg)
        if self.__pydantic_extra__:
            _log.warning(
                "Candidate config has unrecognised keys (possible typos?): %s",
                sorted(self.__pydantic_extra__),
            )
        return self


def validate_candidate(config: dict[str, Any]) -> bool:
    """Validate a Football2Vec candidate config. Returns True on pass, False on reject."""
    try:
        CandidateConfig(**config)
    except (ValidationError, ValueError) as exc:
        _log.warning("Search space rejection: %s", exc)
        return False
    return True
