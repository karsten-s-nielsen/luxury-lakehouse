"""Port/adapter infrastructure for the freshness gate.

Each workflow exposes a :class:`SkipGuard` adapter whose ``check()``
method returns a :class:`FilterResult` describing whether the workflow
has new work and how to chunk it for fan-out.

The freshness gate task (:mod:`ingestion.freshness_gate`) calls every
registered guard once at job start and uses the results to skip or
invoke downstream tasks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FilterResult:
    """What the freshness gate learns from a single workflow's guard.

    Attributes:
        workflow_id: The ``wf-xxx`` identifier matching the workflow card.
        count: Number of unprocessed items.  ``0`` means skip entirely.
        chunks: Pre-computed fan-out partitions — a list of ID lists.
            ``None`` means single-task execution (no fan-out).
            ``len(chunks) > 1`` triggers ``for_each_task``.
            The adapter owns chunk sizing (knows its data shape).
        metadata: Pass-through context for the pipeline — avoids
            re-computing what the guard already discovered (e.g.,
            ``need_global`` flag, competitions DataFrame).
    """

    workflow_id: str
    count: int
    chunks: list[list[str]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize for ``dbutils.jobs.taskValues.set``."""
        return json.dumps(
            {
                "workflow_id": self.workflow_id,
                "count": self.count,
                "chunks": self.chunks,
                "metadata": self.metadata,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> FilterResult:
        """Deserialize from ``dbutils.jobs.taskValues.get``."""
        data = json.loads(raw)
        return cls(**data)


class SkipGuard(Protocol):
    """Port: each workflow exposes its freshness check.

    Implementations live alongside their pipeline module as a
    module-level ``skip_guard`` object or function.
    """

    workflow_id: str

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Run the skip guard and return a FilterResult.

        Must be safe to call from the ``default`` environment —
        only Spark SQL, no analytics imports.
        """
        ...


_GUARD_MODULES: list[str] = [
    "ingestion.pitch_control_batch",
    "ingestion.off_ball_xt",
    "ingestion.defcon_lite_360",
    "ingestion.defcon_lite_tracking",
    "ingestion.elastic_sync",
    "ingestion.pausa",
    "ingestion.line_breaking",
    "ingestion.formations_efpi",
    "ingestion.formations_shape_graph",
    "ingestion.spadl_vaep",
    "ingestion.player_embeddings_v1",
    "ingestion.xg_model",
    "ingestion.xg_model_v2",
    "ingestion.expected_threat",
    "ingestion.export_embeddings_training_data",
    "ingestion.prepare_360_training_data",
    "ingestion.entity_resolution",
    "ingestion.statsbomb",
    "ingestion.metrica",
    "ingestion.wyscout",
    "ingestion.idsse",
    "ingestion.skillcorner",
    "ingestion.import_obso_results",
    "ingestion.import_psxg_predictions",
    "ingestion.import_space_creation",
    "ingestion.tracking_metadata",
    "ingestion.model_validation",
    "ingestion.sync_hf_costs",
    "ingestion.player_embeddings_v2",
]


def get_workflow_guards() -> dict[str, SkipGuard]:
    """Build the guard registry with per-module imports.

    Returns a dict mapping ``workflow_id`` to its ``SkipGuard`` adapter.
    Each module is imported individually so that a missing third-party
    dependency (e.g., ``statsbombpy`` in the ``default`` environment)
    skips that guard rather than crashing the entire registry.
    """
    import importlib
    import logging

    _logger = logging.getLogger(__name__)
    guards: dict[str, SkipGuard] = {}

    for module_path in _GUARD_MODULES:
        try:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard  # type: ignore[attr-defined]
            guards[guard.workflow_id] = guard
        except Exception:
            _logger.warning("Skipping guard from %s (import failed)", module_path, exc_info=True)

    return guards
