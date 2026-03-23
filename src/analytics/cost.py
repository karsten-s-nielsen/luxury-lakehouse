"""HF Jobs cost tracking for workflow executions.

Writes ``_workflow_cost.json`` to HF Hub repos to track running, completed,
failed, and skipped job costs.  All HF Hub uploads are wrapped in try/except
so that cost tracking never crashes a compute job.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate constants (USD / hour) — centralised for all HF Jobs scripts
# ---------------------------------------------------------------------------
HF_RATE_CPU_BASIC: float = 0.01
HF_RATE_A10G_SMALL: float = 1.00
HF_RATE_A10G_LARGE: float = 1.50

# Workflow ID validation
_WORKFLOW_ID_RE: re.Pattern[str] = re.compile(r"^wf-[a-zA-Z0-9_-]+$")

# Transient HTTP error detection (429 Too Many Requests, 5xx Server Errors)
_TRANSIENT_HTTP_RE: re.Pattern[str] = re.compile(r"\b(429|5\d{2})\b")

_COST_FILE = "_workflow_cost.json"

# Max retries for transient HTTP errors (429, 5xx)
_MAX_RETRIES = 3


class HFJobsCostRecorder:
    """Records cost telemetry for a single HF Jobs execution.

    Standalone class (not a ``LifecycleHook``) because HF Jobs scripts run
    outside the workflow runner.  All uploads are fire-and-forget — failures
    are logged as warnings but never propagate.
    """

    def __init__(
        self,
        workflow_id: str,
        phase: str,
        rate_usd_per_hour: float,
        repo_id: str,
        repo_type: str = "dataset",
    ) -> None:
        if not _WORKFLOW_ID_RE.match(workflow_id):
            msg = f"workflow_id must match {_WORKFLOW_ID_RE.pattern!r}, got {workflow_id!r}"
            raise ValueError(msg)

        self.workflow_id = workflow_id
        self.phase = phase
        self.rate_usd_per_hour = rate_usd_per_hour
        self.repo_id = repo_id
        self.repo_type = repo_type
        self.hf_job_id: str | None = os.environ.get("HF_JOB_ID")

        self._api = HfApi()
        self._started_at: datetime | None = None
        self._start_mono: float | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Upload RUNNING state to ``_workflow_cost.json``."""
        self._started_at = datetime.now(tz=timezone.utc)
        self._start_mono = time.monotonic()

        payload = self._base_payload()
        payload["state"] = "RUNNING"
        self._upload(payload)

    def complete(self, metadata: dict[str, object], row_count: int | None = None) -> dict[str, object]:
        """Upload COMPLETED state and return enriched metadata (new dict).

        The original *metadata* dict is never mutated.
        """
        duration, cost = self._elapsed_cost()
        now_iso = datetime.now(tz=timezone.utc).isoformat()

        payload = self._base_payload()
        payload.update(
            {
                "state": "COMPLETED",
                "ended_at": now_iso,
                "duration_seconds": duration,
                "estimated_cost_usd": cost,
                "row_count": row_count,
                "updated_at": now_iso,
            }
        )
        self._upload(payload)

        # Return a NEW dict with cost fields injected
        enriched: dict[str, object] = {**metadata}
        enriched["elapsed_seconds"] = duration
        enriched["rate_usd_per_hour"] = self.rate_usd_per_hour
        enriched["estimated_cost_usd"] = cost
        enriched["workflow_id"] = self.workflow_id
        enriched["workflow_phase"] = self.phase
        enriched["row_count"] = row_count
        return enriched

    def fail(self, error: Exception) -> None:
        """Upload FAILED state with partial cost."""
        duration, cost = self._elapsed_cost()
        now_iso = datetime.now(tz=timezone.utc).isoformat()

        payload = self._base_payload()
        payload.update(
            {
                "state": "FAILED",
                "ended_at": now_iso,
                "duration_seconds": duration,
                "estimated_cost_usd": cost,
                "error": str(error),
                "updated_at": now_iso,
            }
        )
        self._upload(payload)

    def skip(self, reason: str) -> None:
        """Upload SKIPPED state with zero cost."""
        now_iso = datetime.now(tz=timezone.utc).isoformat()

        payload = self._base_payload()
        # If start() was never called, set started_at to now
        if payload.get("started_at") is None:
            payload["started_at"] = now_iso
        payload.update(
            {
                "state": "SKIPPED",
                "ended_at": now_iso,
                "duration_seconds": 0,
                "estimated_cost_usd": 0.0,
                "reason": reason,
                "updated_at": now_iso,
            }
        )
        self._upload(payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _base_payload(self) -> dict[str, object]:
        """Common fields shared by all states."""
        return {
            "workflow_id": self.workflow_id,
            "phase": self.phase,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "rate_usd_per_hour": self.rate_usd_per_hour,
            "hf_job_id": self.hf_job_id,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }

    def _elapsed_cost(self) -> tuple[float, float]:
        """Return ``(duration_seconds, estimated_cost_usd)``."""
        if self._start_mono is None:
            return 0.0, 0.0
        duration = time.monotonic() - self._start_mono
        cost = self.rate_usd_per_hour * duration / 3600.0
        return duration, cost

    def _upload(self, payload: dict[str, object]) -> None:
        """Upload *payload* as ``_workflow_cost.json``. Never raises."""
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                self._api.upload_file(
                    path_or_fileobj=body,
                    path_in_repo=_COST_FILE,
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                )
                return
            except Exception as exc:
                last_exc = exc
                # Retry on transient errors; give up immediately on others
                exc_str = str(exc)
                if _TRANSIENT_HTTP_RE.search(exc_str) or isinstance(exc, (ConnectionError, TimeoutError)):
                    wait = 2**attempt
                    logger.warning(
                        "Cost upload attempt %d/%d failed (retrying in %ds): %s",
                        attempt + 1,
                        _MAX_RETRIES,
                        wait,
                        exc,
                    )
                    if attempt < _MAX_RETRIES - 1:
                        time.sleep(wait)
                else:
                    break

        logger.warning("Cost upload to %s failed after %d attempts: %s", self.repo_id, _MAX_RETRIES, last_exc)
