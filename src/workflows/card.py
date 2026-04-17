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
from pydantic import BaseModel, ConfigDict, Field, model_validator

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

TriggerLiteral = Literal["manual", "scheduled", "event-driven", "orchestrated"]

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
    """Reference to an output Delta table.

    `dbt_model` is set when the table is produced by a dbt model rather than
    by a Python execution phase on this card. The value is the dbt model
    name (e.g. `fct_goalkeeper_stats`); the corresponding SQL file must exist
    at `dbt_project/models/**/<dbt_model>.sql`. Enforced by
    `test_card_dbt_model_field`.
    """

    id: str
    destination: Literal["delta-table"] = "delta-table"
    mart: str | None = None
    synced: str | None = None
    dbt_model: str | None = None


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
    """Training phase execution configuration.

    Used for any phase that declares a `script:` (HF Jobs or similar) rather
    than a wheel `entry_point:`. `orchestrated_by` mirrors the equivalent
    field on `InferenceExecution` so that either shape can be a valid
    orchestrated sub-operation (and so static typing works uniformly when
    a phase is typed as the union `InferenceExecution | TrainingExecution`).
    """

    trigger: TriggerLiteral
    runtime: RuntimeLiteral
    flavor: str | None = None
    script: str
    timeout: str
    also_trainable_via: str | None = None
    # Mirror of InferenceExecution.orchestrated_by — see the bidirectional
    # validator below and the identical one on InferenceExecution.
    orchestrated_by: str | None = None

    @model_validator(mode="after")
    def _orchestrated_bidirectional(self) -> TrainingExecution:
        if self.trigger == "orchestrated" and not self.orchestrated_by:
            msg = "trigger='orchestrated' requires orchestrated_by to name the parent workflow"
            raise ValueError(msg)
        if self.trigger != "orchestrated" and self.orchestrated_by:
            msg = "orchestrated_by is only valid when trigger='orchestrated'"
            raise ValueError(msg)
        return self


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
    # trigger=orchestrated fans out via a super-task workflow; this field names
    # the parent card (e.g. "wf-hf-sync") that invokes this sub-operation.
    orchestrated_by: str | None = None

    @model_validator(mode="after")
    def _orchestrated_bidirectional(self) -> InferenceExecution:
        if self.trigger == "orchestrated" and not self.orchestrated_by:
            msg = "trigger='orchestrated' requires orchestrated_by to name the parent workflow"
            raise ValueError(msg)
        if self.trigger != "orchestrated" and self.orchestrated_by:
            msg = "orchestrated_by is only valid when trigger='orchestrated'"
            raise ValueError(msg)
        return self


class OrchestrationExecution(BaseModel):
    """Super-task execution that fans out into declared sub-operation cards.

    The `sub_operations` field is the canonical list of card ids invoked by
    this super-task. Bidirectional consistency (each listed card declares
    `orchestrated_by` pointing back here) is enforced by the card-parity test,
    not by this Pydantic model.
    """

    trigger: TriggerLiteral
    runtime: RuntimeLiteral
    entry_point: str
    module: str
    distribution: DistributionLiteral
    timeout: str
    environment: str | None = None
    schedule: str | None = None
    sub_operations: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _sub_operations_non_empty(self) -> OrchestrationExecution:
        if not self.sub_operations:
            msg = "orchestration phase requires a non-empty sub_operations list"
            raise ValueError(msg)
        return self


class Execution(BaseModel):
    """Combined execution specs across all phase types.

    Export/ingestion/sync/import phases accept either the Inference shape
    (Databricks entry_point + module + distribution) or the Training shape
    (hf-jobs script + flavor), because the same logical phase can run on
    either runtime depending on the card (e.g. the daily hf_sync
    sub-operations use the Databricks shape; the manual publish scripts
    like wf-publish-xg-shots use the hf-jobs shape).
    """

    training: TrainingExecution | None = None
    inference: InferenceExecution | None = None
    export: InferenceExecution | TrainingExecution | None = None
    ingestion: InferenceExecution | TrainingExecution | None = None
    sync: InferenceExecution | TrainingExecution | None = None
    orchestration: OrchestrationExecution | None = None
    # `import` is a Python reserved word — use alias so YAML key "import" maps
    # to Python attribute `import_`. populate_by_name lets existing code that
    # accesses `.import_` still work after this change.
    import_: InferenceExecution | TrainingExecution | None = Field(default=None, alias="import")

    model_config = ConfigDict(extra="allow", populate_by_name=True)


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
    """Per-execution-phase cost estimates.

    Keys mirror the phase keys on `Execution`: every cost phase declared on
    a card must match an execution phase declared on the same card (enforced
    by `test_card_cost_phase_parity`). The union types let a phase carry
    either an hf-jobs cost shape (flavor + rate_per_hour) or a Databricks
    cost shape (sku + dbu) — some phases run on either runtime depending on
    the card (e.g. `export` runs on databricks for the daily hf_sync
    sub-operations but on hf-jobs for the manual publish cards).
    """

    training: TrainingCost | None = None
    inference: InferenceCost | None = None
    export: TrainingCost | InferenceCost | None = None
    ingestion: TrainingCost | InferenceCost | None = None
    sync: TrainingCost | InferenceCost | None = None
    orchestration: TrainingCost | InferenceCost | None = None
    # `import` is a Python reserved word — alias lets YAML use "import"
    # while Python code accesses `.import_`.
    import_: TrainingCost | InferenceCost | None = Field(default=None, alias="import")

    model_config = ConfigDict(populate_by_name=True)


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
