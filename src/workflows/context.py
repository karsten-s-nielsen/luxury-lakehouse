"""Immutable context for a single workflow run."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class WorkflowContext:
    """Immutable correlation context for a single workflow run.

    The ``log_extra()`` dict is the observability integration surface —
    every structured log line should include these fields so future
    OpenTelemetry hooks can attach span attributes without changing
    pipeline code.
    """

    workflow_id: str
    phase: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Populated from workflow card YAML
    workflow_name: str = ""
    workflow_type: str = ""

    # Runtime metadata — set at context creation, not mutated
    partition_key: str = ""

    def log_extra(self) -> dict[str, str]:
        """Fields injected into every structured log line for this run."""
        extra = {
            "workflow_id": self.workflow_id,
            "workflow_phase": self.phase,
            "run_id": self.run_id,
            "workflow_name": self.workflow_name,
            "started_at": self.started_at.isoformat(),
        }
        if self.workflow_type:
            extra["workflow_type"] = self.workflow_type
        if self.partition_key:
            extra["partition_key"] = self.partition_key
        return extra
