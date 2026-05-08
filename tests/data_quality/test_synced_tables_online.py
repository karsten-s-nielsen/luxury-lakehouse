"""Live health check: every synced table in ``ingestion.refresh_synced_tables.SYNCED_TABLES``
must be in ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` state.

Background: the daily Lakebase Maintenance GH Action only verifies event-log
ownership, SP grants, and PG indexes. It does NOT check synced-table online
state. The career/season behavioral embedding tables have failed three times
now (session 17 OFFLINE, session 43/44 event_log drift, session 69
OFFLINE_FAILED post-mart-rebuild) without this check; each time the
detection happened via dashboard staleness instead of at deploy time.

This test asserts every entry of ``SYNCED_TABLES`` is in the online state.
Wired into ``.github/workflows/lakebase-grants.yml`` as a new step (post-grants).

Requires live Databricks API access (DATABRICKS_HOST + DATABRICKS_TOKEN).
Skipped when those env vars are unset.

Failure mode this test prevents: a synced table going OFFLINE_FAILED in
production and Taipy queries returning stale data for hours/days before
anyone notices.
"""

from __future__ import annotations

import logging
import os

import pytest
import requests

# Live test — needs Databricks credentials AND the `[sdk]` extra installed
# (via `uv sync --frozen --extra sdk` as the lakebase-grants.yml workflow
# does). Skips locally when either is missing so dev-machine pytest runs
# don't fail on a missing extras dep.
pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (run `uv sync --extra sdk`)")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")),
    reason="DATABRICKS_HOST + DATABRICKS_TOKEN env vars required for live state check",
)

_LOGGER = logging.getLogger("test_synced_tables_online")

_CATALOG = os.environ.get("UC_CATALOG", "soccer_analytics")
_DEFAULT_SCHEMA = os.environ.get("GOLD_SCHEMA", "dev_gold")


def _get_synced_table_state(host: str, token: str, full_name: str) -> tuple[str, dict[str, object]]:
    """Query the Databricks REST API for a synced table's detailed_state.

    Returns ``(detailed_state, raw_json)``. ``detailed_state`` is a string
    like ``"SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"``,
    ``"SYNCED_TABLE_OFFLINE_FAILED"``, etc.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if not host.startswith("https://"):
        host = f"https://{host}"
    resp = requests.get(
        f"{host.rstrip('/')}/api/2.0/database/synced_tables/{full_name}",
        headers=headers,
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    data: dict[str, object] = resp.json()
    sync_status = data.get("data_synchronization_status") or {}
    assert isinstance(sync_status, dict)
    detailed_state = str(sync_status.get("detailed_state") or "UNKNOWN")
    return detailed_state, data


@requires_databricks
def test_all_synced_tables_online() -> None:
    """Every entry in ``SYNCED_TABLES`` must report
    ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`` from the database REST API.

    Failures: any state other than ONLINE_NO_PENDING_UPDATE — including
    OFFLINE, OFFLINE_FAILED, and PIPELINE_FAILED — surfaces here as a
    deploy-time gate instead of via dashboard staleness."""
    from ingestion.refresh_synced_tables import SYNCED_TABLE_ONLINE_STATE, SYNCED_TABLES

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    failures: list[str] = []
    for table_name, schema_override in SYNCED_TABLES:
        schema = schema_override or _DEFAULT_SCHEMA
        full_name = f"{_CATALOG}.{schema}.{table_name}"
        try:
            state, _data = _get_synced_table_state(host, token, full_name)
        except requests.HTTPError as exc:
            failures.append(f"{full_name}: HTTP {exc.response.status_code} from synced_tables API — {exc}")
            continue
        if state != SYNCED_TABLE_ONLINE_STATE:
            failures.append(
                f"{full_name}: detailed_state={state!r} (expected {SYNCED_TABLE_ONLINE_STATE!r}). "
                f"Investigate via Databricks UI → Lakeflow Connect → synced tables OR "
                f"via `python scripts/delete_synced_table.py {table_name}` followed by "
                f"UI recreate per `feedback_synced_table_deletion.md`."
            )
        else:
            _LOGGER.info("OK %s — %s", full_name, state)
    assert not failures, "Synced-table health check failures:\n" + "\n".join(f"  - {msg}" for msg in failures)
