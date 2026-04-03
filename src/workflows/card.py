"""WorkflowCard Pydantic v2 model for parsing YAML workflow cards.

A workflow card is a YAML frontmatter document (between ``---`` delimiters)
followed by an optional Markdown body.  The Pydantic model validates all
fields and provides template variable resolution for ``{catalog}`` placeholders
in dataset and table IDs.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Literal unions — kept as module-level type aliases for readability
# ---------------------------------------------------------------------------

StatusLiteral = Literal["draft", "development", "production", "deprecated"]

TypeLiteral = Literal[
    "training-and-inference",
    "training",
    "inference",
    "grid-computation",
    "heuristic",
    "ingestion",
    "data-movement",
    "validation",
]

TriggerLiteral = Literal["manual", "scheduled", "event-driven"]

RuntimeLiteral = Literal["hf-jobs", "databricks-notebook", "databricks-workflow"]

StrategyLiteral = Literal["skip-guard", "full-overwrite", "upsert", "none"]

ReferenceRoleLiteral = Literal["methodology", "algorithm", "dataset", "inspiration"]

SourceLiteral = Literal["huggingface", "delta-table", "uc-volume"]

ModelDestinationLiteral = Literal["huggingface", "mlflow-registry", "uc-volume"]

DistributionLiteral = Literal["applyInPandas", "driver-bound", "none"]

CostRuntimeLiteral = Literal["hf-jobs", "databricks"]

# ---------------------------------------------------------------------------
# Nested models — ordered from leaf to root
# ---------------------------------------------------------------------------


class Reference(BaseModel):
    """Academic or data provenance reference."""

    citation: str
    role: ReferenceRoleLiteral


class DatasetRef(BaseModel):
    """Reference to an input or output dataset."""

    id: str
    source: SourceLiteral | None = None
    destination: Literal["huggingface"] | None = None
    description: str | None = None


class ModelRef(BaseModel):
    """Reference to an input or output model artifact."""

    id: str
    source: SourceLiteral | None = None
    destination: ModelDestinationLiteral | None = None
    format: str | None = None
    alias: str | None = None


class TableRef(BaseModel):
    """Reference to an output Delta table."""

    id: str
    destination: Literal["delta-table"] = "delta-table"
    mart: str | None = None
    synced: str | None = None


class Inputs(BaseModel):
    """Input datasets and upstream model dependencies."""

    datasets: list[DatasetRef] = Field(default_factory=list)
    models: list[ModelRef] | None = None


class Outputs(BaseModel):
    """Output artifacts: models, tables, and datasets."""

    models: list[ModelRef] | None = None
    tables: list[TableRef] | None = None
    datasets: list[DatasetRef] | None = None


class TrainingExecution(BaseModel):
    """Training phase execution configuration."""

    trigger: TriggerLiteral
    runtime: RuntimeLiteral
    flavor: str | None = None
    script: str
    timeout: str
    also_trainable_via: str | None = None


class InferenceExecution(BaseModel):
    """Inference phase execution configuration."""

    trigger: TriggerLiteral
    runtime: RuntimeLiteral
    entry_point: str
    module: str
    distribution: DistributionLiteral
    partition_key: str | None = None
    schedule: str | None = None
    timeout: str
    environment: str | None = None


class Execution(BaseModel):
    """Combined execution specs across all phase types."""

    training: TrainingExecution | None = None
    inference: InferenceExecution | None = None
    export: InferenceExecution | None = None
    ingestion: InferenceExecution | None = None
    sync: InferenceExecution | None = None

    model_config = ConfigDict(extra="allow")


class Idempotency(BaseModel):
    """Idempotency strategy and partition keys."""

    strategy: StrategyLiteral
    key: str | list[str]
    description: str


class Performance(BaseModel):
    """Performance budget metadata."""

    training_time: str | None = None
    inference_timeout: str | None = None
    memory_ceiling: str | None = None


class TrainingCost(BaseModel):
    """Cost estimate for training runs."""

    runtime: CostRuntimeLiteral
    flavor: str
    rate_usd_per_hour: float
    typical_duration_minutes: float
    typical_cost_usd: float


class InferenceCost(BaseModel):
    """Cost estimate for inference runs."""

    runtime: Literal["databricks"]
    sku: str
    typical_dbu: float
    typical_cost_usd: float


class Cost(BaseModel):
    """Combined training and inference cost estimates."""

    training: TrainingCost | None = None
    inference: InferenceCost | None = None


class MonitoringMetric(BaseModel):
    """A single monitoring metric with optional thresholds."""

    name: str
    baseline: float
    warn_above: float | None = None
    warn_below: float | None = None
    alert_above: float | None = None
    alert_below: float | None = None


class Monitoring(BaseModel):
    """Monitoring configuration: validator, metrics, SLA."""

    validator: str | None = None
    metrics: list[MonitoringMetric] = Field(default_factory=list)
    freshness_sla_hours: float | None = None


class Links(BaseModel):
    """Related documentation and source code links."""

    model_card: str | None = None
    dataset_cards: list[str] = Field(default_factory=list)
    source_code: list[str] = Field(default_factory=list)
    tests: str | None = None
    hf_model: str | None = None
    hf_dataset: str | None = None


# ---------------------------------------------------------------------------
# Frontmatter parser
# ---------------------------------------------------------------------------

_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)", re.DOTALL)


def _split_frontmatter(text: str) -> tuple[str, str]:
    """Split YAML frontmatter from Markdown body.

    Returns:
        (yaml_text, body) where body may be empty.
    """
    match = _FRONTMATTER_RE.match(text)
    if not match:
        msg = "Workflow card must start with YAML frontmatter delimited by ---"
        raise ValueError(msg)
    return match.group(1), match.group(2)


# ---------------------------------------------------------------------------
# Root model
# ---------------------------------------------------------------------------


class WorkflowCard(BaseModel):
    """Pydantic v2 model for a workflow card YAML document."""

    # === Identity (required) ===
    name: str
    id: str
    version: str
    status: StatusLiteral
    type: TypeLiteral
    domain: str
    owners: list[str]

    # === Classification (optional) ===
    tags: list[str] = Field(default_factory=list)

    # === Academic Provenance (optional) ===
    references: list[Reference] = Field(default_factory=list)

    # === Inputs / Outputs (optional) ===
    inputs: Inputs | None = None
    outputs: Outputs | None = None

    # === Execution (optional) ===
    execution: Execution | None = None

    # === Dependencies (optional) ===
    depends_on: list[str] = Field(default_factory=list)

    # === Idempotency (optional) ===
    idempotency: Idempotency | None = None

    # === Performance (optional) ===
    performance: Performance | None = None

    # === Cost (optional) ===
    cost: Cost | None = None

    # === Monitoring (optional) ===
    monitoring: Monitoring | None = None

    # === Links (optional) ===
    links: Links | None = None

    # === Markdown body (not part of YAML frontmatter) ===
    body: str = ""

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml_string(cls, text: str) -> WorkflowCard:
        """Parse a workflow card from a YAML-frontmatter string."""
        yaml_text, body = _split_frontmatter(text)
        data: dict[str, object] = yaml.safe_load(yaml_text) or {}
        data["body"] = body
        return cls.model_validate(data)

    @classmethod
    def from_yaml_file(cls, path: Path) -> WorkflowCard:
        """Parse a workflow card from a YAML file on disk."""
        return cls.from_yaml_string(path.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Template resolution
    # ------------------------------------------------------------------

    def resolve_templates(self, catalog: str) -> WorkflowCard:
        """Return a new card with ``{catalog}`` replaced in all IDs.

        The original instance is not mutated.  Logs a warning for any
        ``{token}`` placeholders that remain after resolution.
        """
        raw = self.model_dump()
        resolved_json = _resolve_catalog_in_dict(raw, catalog)
        card = WorkflowCard.model_validate(resolved_json)
        _warn_unresolved_tokens(resolved_json, card.id)
        return card


_UNRESOLVED_TOKEN_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")


def _resolve_catalog_in_dict(obj: object, catalog: str) -> object:
    """Recursively replace ``{catalog}`` in string values."""
    if isinstance(obj, str):
        return obj.replace("{catalog}", catalog)
    if isinstance(obj, list):
        return [_resolve_catalog_in_dict(item, catalog) for item in obj]
    if isinstance(obj, dict):
        return {k: _resolve_catalog_in_dict(v, catalog) for k, v in obj.items()}
    return obj


def _warn_unresolved_tokens(obj: object, card_id: str) -> None:
    """Log a warning for any ``{token}`` placeholders remaining after resolution."""
    if isinstance(obj, str):
        for match in _UNRESOLVED_TOKEN_RE.finditer(obj):
            # nosemgrep: python-logger-credential-disclosure
            _logger.warning("Unresolved template token %s in card %s: %s", match.group(), card_id, obj)
    elif isinstance(obj, list):
        for item in obj:
            _warn_unresolved_tokens(item, card_id)
    elif isinstance(obj, dict):
        for v in obj.values():
            _warn_unresolved_tokens(v, card_id)
