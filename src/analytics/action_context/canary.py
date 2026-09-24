"""In-preflight drain canary (ADR-087): dry-run one unit per provider through the FULL processor
BEFORE the fan-out spins up, so a systemic compute-path defect (e.g. gk_decision on tracking frames)
fails preflight — cheaply, and before any corpus write — instead of burning the worker retry budget.

Pure orchestration (no Spark/Delta): the ``processor`` is the same ``GameProcessorPort`` the drain uses,
injected by the caller. ``analytics`` cannot import ``ingestion`` (import-linter), so this lives here and
the preflight (in ``ingestion``) supplies the concrete processor.
"""

from __future__ import annotations

import logging

from analytics.action_context.drain import GameProcessorPort, unit_label
from analytics.action_context.work_unit import WorkUnit


def select_canary_units(units: list[WorkUnit]) -> list[WorkUnit]:
    """A SINGLE representative canary unit (the first discovered). A full-processor dry-run costs ~one
    real drain unit (defcon / pitch-control dominate), so a per-provider fan of dry-runs blows the
    preflight task budget (600 s). One unit catches a GLOBAL systemic defect (e.g. the gk_decision
    `is_actor` bug, which hit every provider) BEFORE the fan-out; provider-specific defects fall to the
    runtime circuit-breaker (ADR-087 amendment 2026-09-23)."""
    return units[:1]


def run_canary(processor: GameProcessorPort, units: list[WorkUnit], logger: logging.Logger) -> None:
    """Dry-run one unit per provider present; raise on the FIRST failure (naming the unit + error).

    A compute-path defect surfaces here (dry_run computes every writer, persists nothing), before the
    N-worker fan-out and before any writes. The raise fails the preflight task, so the dependent
    ``compute_*`` task is skipped. A canary unit whose DATA (not code) is bad blocks only that provider;
    the runtime circuit-breaker is the backstop for defects a canary unit does not exercise.
    """
    for u in select_canary_units(units):
        label = unit_label(u)
        try:
            processor.process(u, dry_run=True)
        except Exception as exc:
            logger.error("drain_canary_failed unit=%s err=%s", label, exc, exc_info=True)
            raise RuntimeError(f"drain canary FAILED on {label}: {exc}") from exc
        logger.info("drain_canary_ok unit=%s", label)
