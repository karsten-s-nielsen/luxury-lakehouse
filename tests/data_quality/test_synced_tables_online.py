"""Live health check: every synced table in ``ingestion.refresh_synced_tables.SYNCED_TABLES``
must be in ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` state.

Uses the Databricks SDK ``w.postgres.get_synced_table()`` — consistent with all
other consumers post SDK synced table migration (ADR-026).

Requires live Databricks API access (DATABRICKS_HOST + DATABRICKS_TOKEN).
Skipped when those env vars are unset.
"""

from __future__ import annotations

import logging
import os

import pytest

from ingestion.databricks_auth import has_databricks_auth

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (run `uv sync --extra sdk`)")

requires_databricks = pytest.mark.skipif(
    not has_databricks_auth(),
    reason="DATABRICKS_HOST + DATABRICKS_TOKEN env vars required for live state check",
)

_LOGGER = logging.getLogger("test_synced_tables_online")

_CATALOG = os.environ.get("UC_CATALOG", "soccer_analytics")
_DEFAULT_SCHEMA = os.environ.get("GOLD_SCHEMA", "dev_gold")


@requires_databricks
def test_all_synced_tables_online() -> None:
    """Every entry in ``SYNCED_TABLES`` must report
    ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` from the postgres API."""
    from databricks.sdk import WorkspaceClient

    from ingestion.refresh_synced_tables import (
        HEALTHY_ONLINE_STATES,
        SYNCED_TABLES,
        is_synced_table_healthy,
        is_synced_table_not_found,
    )

    ws = WorkspaceClient()
    failures: list[str] = []
    skipped: list[str] = []

    for config in SYNCED_TABLES:
        schema = config.schema_override or _DEFAULT_SCHEMA
        full_name = f"{_CATALOG}.{schema}.{config.name}"
        name = f"synced_tables/{full_name}"
        try:
            meta = ws.postgres.get_synced_table(name=name)
            status = getattr(meta, "status", None)
            raw_state = getattr(status, "detailed_state", None)
            # SDK returns SyncedTableState enum; extract .value for string comparison
            detailed_state = raw_state.value if raw_state else "UNKNOWN"
        except Exception as exc:
            # A configured synced table that has not been created yet (fresh
            # install, or a mart whose first sync hasn't run) is expected —
            # skip it. A table that EXISTS but is OFFLINE/FAILED still fails
            # below (detailed_state != ONLINE), which is the real signal this
            # health check guards.
            if is_synced_table_not_found(exc):
                _LOGGER.warning("SKIP %s — synced table not created yet: %s", full_name, exc)
                skipped.append(full_name)
                continue
            failures.append(f"{full_name}: SDK error — {exc}")
            continue
        if not is_synced_table_healthy(detailed_state):
            failures.append(
                f"{full_name}: detailed_state={detailed_state!r} "
                f"(expected one of {sorted(HEALTHY_ONLINE_STATES)}). "
                f"A genuinely-degraded table (OFFLINE / *_FAILED / PROVISIONING / actively-*_UPDATE) is "
                f"the real signal here — investigate the pipeline via the Databricks UI. Only if it is a "
                f"broken checkpoint/strand does `python scripts/delete_synced_table.py {config.name}` + "
                f"`python scripts/migrate_synced_tables.py` apply; do NOT churn a healthy idle table."
            )
        else:
            _LOGGER.info("OK %s — %s", full_name, detailed_state)

    if skipped:
        _LOGGER.warning("%d synced table(s) skipped (not created yet): %s", len(skipped), ", ".join(skipped))

    assert not failures, "Synced-table health check failures:\n" + "\n".join(f"  - {msg}" for msg in failures)
