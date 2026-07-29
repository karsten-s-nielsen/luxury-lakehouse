"""Self-heal policy for checkpoint-broken synced tables (spec).

Pure domain over the thin lifecycle ports — no databricks-sdk import here. A synced table whose
source mart was dropped+recreated (any ``dbt --full-refresh``) fails its streaming update with
SQLSTATE ``XXKST`` (``DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE``); the only fix is to recreate
the synced table (reset the checkpoint). Detection runs read-only in the daily job (Reader port
only); the destructive recreate runs in the privileged maintenance path (all four ports).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from ingestion.synced_table_lifecycle import (
    PostgresGhostPort,
    SyncedTableReaderPort,
    SyncedTableWriterPort,
    WarehousePort,
)

logger = logging.getLogger(__name__)

# SQLSTATE is the stable primary signal (spec M2); the message marker is the secondary signal.
_CHECKPOINT_MISMATCH_SQLSTATE = "XXKST"
_CHECKPOINT_MISMATCH_MESSAGE_MARKER = "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE"


def _error_messages(events: list[dict[str, Any]]) -> list[str]:
    """Extract only the error/exception message text (P9 — field-scoped, not a whole-blob match)."""
    out: list[str] = []
    for e in events:
        err = e.get("error") or {}
        for exc in err.get("exceptions") or []:
            msg = exc.get("message")
            if msg:
                out.append(str(msg))
        top = e.get("message")
        if top:
            out.append(str(top))
    return out


def is_checkpoint_mismatch_failure(reader: SyncedTableReaderPort, pipeline_id: str) -> bool:
    """True only if the latest failed update was the source-recreated checkpoint mismatch.

    Depends ONLY on the read-only port (the SP can run this; it cannot destroy). Fail-safe: any
    query error or inconclusive payload returns ``False`` — never recreate on doubt.
    """
    try:
        events = reader.latest_failed_events(pipeline_id)
    except Exception:
        logger.exception("pipeline %s: failed-event query errored; treating as not-self-healable", pipeline_id)
        return False
    return any(
        _CHECKPOINT_MISMATCH_SQLSTATE in m or _CHECKPOINT_MISMATCH_MESSAGE_MARKER in m for m in _error_messages(events)
    )


class HealOutcome(Enum):
    HEALED = "healed"  # recreated + re-synced fresh + online
    UNHEALABLE = "unhealable"  # not a checkpoint mismatch — surfaced, never touched
    HEAL_FAILED = "heal_failed"  # tried + failed (permission / ghost / create / timeout)
    SKIPPED_PREFLIGHT = "skipped_preflight"  # verify-before-destroy aborted; no delete done


@dataclass(frozen=True)
class HealPorts:
    """The four thin ports the destructive heal composes (only the privileged identity holds these)."""

    reader: SyncedTableReaderPort
    writer: SyncedTableWriterPort
    ghost: PostgresGhostPort
    warehouse: WarehousePort


def heal_synced_table(ports: HealPorts, config: Any, catalog: str, schema: str) -> HealOutcome:
    """Recreate a checkpoint-broken synced table. Verify-before-destroy: never delete on doubt.

    Order (spec): preflight -> ensure_cdf -> sdk_delete + drop_pg_ghost -> create -> trigger + wait.
    A create that hits "already exists" => HEAL_FAILED (the delete did not take; the checkpoint was
    NOT reset — L1; treating it as success would be a false-positive heal). Enabling CDF immediately
    before create covers the recreated TRIGGERED table's initial-full-load-then-go-forward-CDF (L7).
    A failure after delete leaves the table absent; recovery is the maintenance create-all pass that
    re-creates any missing SYNCED_TABLES entry on its next run (M4).
    """
    eff_schema = config.schema_override or schema
    fqn = f"{catalog}.{eff_schema}.{config.name}"
    src_fqn = f"{catalog}.{eff_schema}.{config.source_table}"

    # Preflight: only act if the checkpoint mismatch is STILL present (idempotent / race-safe).
    pid = ports.reader.get_pipeline_id(fqn)
    if not is_checkpoint_mismatch_failure(ports.reader, pid):
        logger.warning("%s: checkpoint mismatch not present at heal time -> SKIPPED_PREFLIGHT", config.name)
        return HealOutcome.SKIPPED_PREFLIGHT

    try:
        ports.warehouse.ensure_cdf(src_fqn)
        ports.writer.sdk_delete(fqn)
        ports.ghost.drop_pg_ghost(eff_schema, config.name)
        try:
            ports.writer.create_synced_table(config, catalog, schema)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                logger.error("%s: create hit 'already exists' -> delete did not take; HEAL_FAILED", config.name)
                return HealOutcome.HEAL_FAILED
            raise
        ports.writer.trigger_refresh(ports.reader.get_pipeline_id(fqn))
        state = ports.writer.wait_until_online(fqn)
    except Exception:
        logger.exception(
            "%s: heal failed mid-sequence (recovery: maintenance create-all re-creates if absent)", config.name
        )
        return HealOutcome.HEAL_FAILED

    if state == "SYNCED_TABLE_ONLINE":
        logger.info("%s: checkpoint reset via recreate -> HEALED", config.name)
        return HealOutcome.HEALED
    logger.error("%s: recreated but not online (%s) -> HEAL_FAILED", config.name, state)
    return HealOutcome.HEAL_FAILED


class _HealStateRecorder(Protocol):
    def mark_healed(self, table_name: str, event_at: datetime) -> None: ...


def run_heal_pass(
    ports: HealPorts,
    stranded: list[str],
    configs_by_name: dict[str, Any],
    catalog: str,
    schema: str,
    state: _HealStateRecorder,
    *,
    now: datetime,
    enabled: bool,
) -> dict[str, HealOutcome]:
    """Heal each stranded table in the privileged maintenance path. Returns name -> outcome.

    ``enabled`` is the kill-switch, resolved + injected at the CLI boundary (R6) so this stays a pure
    policy function. On success the heal records ``mark_healed`` so detect-side recurrence clears
    (P1). A ``HEAL_FAILED`` logs at ERROR (never warning) so a permanently-failing heal is visible to
    error-log queries / paging, not a silent loop (H3 / ADR-002).
    """
    if not enabled:
        logger.warning("synced-table heal disabled by kill-switch (SYNCED_TABLE_HEAL_ENABLED=0)")
        return {}
    outcomes: dict[str, HealOutcome] = {}
    for name in stranded:
        config = configs_by_name.get(name)
        if config is None:
            logger.error("heal: no SyncedTableConfig for stranded table %r -- skipping", name)
            continue
        outcome = heal_synced_table(ports, config, catalog, schema)
        outcomes[name] = outcome
        if outcome is HealOutcome.HEALED:
            # Strand-state recording is best-effort: the table is already healed, so a telemetry-write
            # failure (e.g. warehouse hiccup) must NOT downgrade a real heal. Surfaced at ERROR (never
            # a silent warning-swallow — ADR-002) so stale recurrence tracking is visible.
            try:
                state.mark_healed(name, now)
            except Exception:
                logger.exception("heal: %s healed but mark_healed failed (recurrence tracking may be stale)", name)
        elif outcome is HealOutcome.HEAL_FAILED:
            logger.error("heal: %s -> HEAL_FAILED (needs attention)", name)
        elif outcome is HealOutcome.SKIPPED_PREFLIGHT:
            # ERROR, not warning (ADR-002). The detect side had already classified this table as
            # stranded, so a preflight that disagrees means one of two things, and BOTH warrant a
            # human: the strand cleared on its own (benign, but the detect/heal pair disagreed), or
            # the classifier could not see the strand through an in-flight DLT retry and this heal
            # silently no-opped on a real one. Before 2026-07-28 this branch did not exist and the
            # only trace was a warning inside heal_synced_table -- invisible to error-log queries,
            # so a recovery path that never recovered looked identical to one with nothing to do.
            logger.error(
                "heal: %s -> SKIPPED_PREFLIGHT (detect said stranded, preflight disagreed; "
                "table NOT healed and recurrence will re-fire next detect run)",
                name,
            )
    return outcomes
