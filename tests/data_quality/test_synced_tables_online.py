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

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (run `uv sync --extra sdk`)")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")),
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
        SYNCED_TABLE_ONLINE_STATE,
        SYNCED_TABLES,
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
        if detailed_state != SYNCED_TABLE_ONLINE_STATE:
            failures.append(
                f"{full_name}: detailed_state={detailed_state!r} "
                f"(expected {SYNCED_TABLE_ONLINE_STATE!r}). "
                f"Investigate via Databricks UI or "
                f"`python scripts/delete_synced_table.py {config.name}` "
                f"followed by `python scripts/migrate_synced_tables.py`."
            )
        else:
            _LOGGER.info("OK %s — %s", full_name, detailed_state)

    if skipped:
        _LOGGER.warning("%d synced table(s) skipped (not created yet): %s", len(skipped), ", ".join(skipped))

    assert not failures, "Synced-table health check failures:\n" + "\n".join(f"  - {msg}" for msg in failures)
