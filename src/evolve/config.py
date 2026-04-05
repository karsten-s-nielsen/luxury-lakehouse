"""Pydantic configuration models for the Evolve Engine."""

from __future__ import annotations

import math
from pathlib import Path

import yaml
from pydantic import BaseModel, model_validator

_VALID_BACKEND_TYPES = frozenset({"local_cuda", "docker", "hf_jobs", "remote_ssh"})


class FitnessConfig(BaseModel):
    """Defines how candidate architectures are scored."""

    primary: str
    secondary: str | None = None
    combined_weights: dict[str, float] = {}
    minimize: bool = False

    @model_validator(mode="after")
    def _validate_weights(self) -> FitnessConfig:
        if self.combined_weights:
            total = sum(self.combined_weights.values())
            if not math.isclose(total, 1.0, abs_tol=1e-6):
                msg = f"combined_weights must sum to 1.0, got {total}"
                raise ValueError(msg)
            if self.primary not in self.combined_weights:
                msg = f"primary '{self.primary}' must be in combined_weights keys: {list(self.combined_weights.keys())}"
                raise ValueError(msg)
        return self


class BackendConfig(BaseModel):
    """Compute backend for running training evaluations."""

    type: str
    device: str = "cuda:0"
    docker_image: str | None = None
    hf_flavor: str | None = None
    ssh_host: str | None = None
    ssh_user: str | None = None
    ssh_remote_dir: str | None = None
    ssh_python_path: str | None = None

    @model_validator(mode="after")
    def _validate_type(self) -> BackendConfig:
        if self.type not in _VALID_BACKEND_TYPES:
            msg = f"Unknown backend type '{self.type}'. Must be one of: {sorted(_VALID_BACKEND_TYPES)}"
            raise ValueError(msg)
        return self


class EvalConfig(BaseModel):
    """Training evaluation parameters."""

    epochs: int = 5
    dataset: str = "luxury-lakehouse/scoutgpt-training-data"
    timeout_seconds: int = 900
    seed: int = 42


class LLMModelConfig(BaseModel):
    """Configuration for a single LLM provider."""

    name: str
    weight: float
    api_base: str
    api_key_env: str


class LLMConfig(BaseModel):
    """LLM ensemble configuration for code generation."""

    models: list[LLMModelConfig] = []
    temperature: float = 0.7
    max_tokens: int = 4096

    @model_validator(mode="after")
    def _validate_model_weights(self) -> LLMConfig:
        if self.models:
            total = sum(m.weight for m in self.models)
            if not math.isclose(total, 1.0, abs_tol=1e-6):
                msg = f"Model weights must sum to 1.0, got {total}"
                raise ValueError(msg)
        return self


class EvolutionConfig(BaseModel):
    """Evolutionary search hyperparameters."""

    iterations: int = 150
    population_size: int = 200
    num_islands: int = 3
    migration_interval: int = 30
    parallel_evaluations: int = 1
    diff_based: bool = True
    early_stopping_patience: int = 40


class EvolveConfig(BaseModel):
    """Top-level configuration for an evolutionary architecture search run."""

    target: str
    description: str = ""
    fitness: FitnessConfig
    evaluation: EvalConfig = EvalConfig()
    backend: BackendConfig = BackendConfig(type="local_cuda")
    llm: LLMConfig = LLMConfig()
    evolution: EvolutionConfig = EvolutionConfig()

    @classmethod
    def from_yaml(cls, path: Path) -> EvolveConfig:
        """Load configuration from a YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)
