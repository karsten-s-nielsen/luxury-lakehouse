"""AC-1 — live-warehouse parity: code DDL ↔ LIVE ``bronze.spadl_action_context``.

The in-process test (``test_action_context_schema_parity.py``) pins
``ACTION_CONTEXT_DDL`` against ``RESULT_COLUMNS`` — code-vs-code. It cannot catch
the drift that actually bit us: the LIVE bronze table lagging the code by 3
columns (101 live vs 104 code, missing ghost_gk_x/ghost_gk_y/ghost_gk_spread),
which silently broke the gold mart's ``SELECT ghost_gk_*`` until a migration was
applied by hand.

This test closes that gap: every column the code DDL declares MUST exist in the
live bronze table. It runs only when live Databricks credentials are present
(mirrors ``test_synced_tables_online.py`` / ``test_statsbomb_bronze_coverage.py``)
and is skipped in offline CI.

Mechanism: Databricks SDK statement-execution against
``soccer_analytics.information_schema.columns``.
"""

from __future__ import annotations

import logging
import os
import re

import pytest

from ingestion.databricks_auth import has_databricks_auth

pytest.importorskip("databricks.sdk", reason="databricks-sdk not installed (run `uv sync --extra sdk`)")

requires_databricks = pytest.mark.skipif(
    not has_databricks_auth(),
    reason="requires live Databricks credentials (DATABRICKS_HOST + token or GitHub OIDC)",
)

_LOGGER = logging.getLogger("test_action_context_live_ddl_parity")

_CATALOG = os.environ.get("UC_CATALOG", "soccer_analytics")
_BRONZE_SCHEMA = "bronze"
_TABLE = "spadl_action_context"
_WAREHOUSE_ENV = "DATABRICKS_SQL_WAREHOUSE_ID"

# Same column-extraction regex as test_action_context_schema_parity.py.
_DDL_COL_RE = re.compile(r"(\w+)\s+\w+")


def _execute_sql(query: str) -> list[list[str]]:
    """Run a SQL statement via the Databricks SDK and return data rows."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    ws = WorkspaceClient()
    warehouse_id = os.environ.get(_WAREHOUSE_ENV)
    if not warehouse_id:
        for wh in ws.warehouses.list():
            if wh.name and wh.name.startswith("soccer-analytics-warehouse") and wh.id:
                warehouse_id = wh.id
                break
    if not warehouse_id:
        pytest.skip("no SQL warehouse available")

    resp = ws.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=_CATALOG,
        statement=query,
        wait_timeout="50s",
    )
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        msg = resp.status.error if resp.status else "unknown error"
        pytest.fail(f"SQL execution failed: {msg}")
    result = resp.result
    if not result or not result.data_array:
        return []
    return [list(row) for row in result.data_array]


def _live_columns() -> set[str]:
    # _BRONZE_SCHEMA / _TABLE are module constants, not user input — S608 N/A.
    query = (
        "SELECT column_name FROM information_schema.columns "  # noqa: S608
        f"WHERE table_schema = '{_BRONZE_SCHEMA}' AND table_name = '{_TABLE}'"
    )
    rows = _execute_sql(query)
    return {str(r[0]) for r in rows}


@requires_databricks
def test_live_bronze_has_every_ddl_column() -> None:
    """Every column declared in ``ACTION_CONTEXT_DDL`` must exist in live bronze.

    This is the mart-breaking drift direction: the gold mart selects the code's
    column set, so any code column missing from the live table fails the mart.
    """
    from analytics.action_context.schema import ACTION_CONTEXT_DDL

    ddl_cols = set(_DDL_COL_RE.findall(ACTION_CONTEXT_DDL))
    live_cols = _live_columns()

    if not live_cols:
        pytest.skip(f"{_BRONZE_SCHEMA}.{_TABLE} not found in live warehouse (not yet created)")

    missing_in_live = ddl_cols - live_cols
    assert not missing_in_live, (
        f"{_BRONZE_SCHEMA}.{_TABLE} is missing {len(missing_in_live)} column(s) the code DDL "
        f"declares — the gold mart will fail to select them. Apply the bronze migration.\n"
        f"  Missing in live: {sorted(missing_in_live)}"
    )

    # Live-only columns are non-fatal (audit/partition columns the DDL may omit),
    # but worth surfacing so writer/DDL drift in the other direction is visible.
    extra_in_live = live_cols - ddl_cols
    if extra_in_live:
        _LOGGER.warning(
            "%s.%s has %d column(s) not in ACTION_CONTEXT_DDL: %s",
            _BRONZE_SCHEMA,
            _TABLE,
            len(extra_in_live),
            sorted(extra_in_live),
        )


@requires_databricks
def test_action_context_coverage_is_frames_required() -> None:
    """Frames-required contract (ADR-057): bronze.spadl_action_context contains ONLY frame-bearing
    providers — the 4 tracking providers + statsbomb matches that HAVE freeze-frames. Zero wyscout /
    zero statsbomb-without-360 rows.

    This is the post-recompute contract the row-absence model depends on (an SB action-context row
    with no tracking is the exact bug this pipeline prevents). Live-only (skipped offline); it
    trivially holds on an empty/wiped mart (no rows -> no violations) and becomes a real assertion
    once the frames-required recompute lands.
    """
    provider_q = f"SELECT DISTINCT data_source FROM {_BRONZE_SCHEMA}.{_TABLE}"  # noqa: S608 — module constants
    rows = _execute_sql(provider_q)
    provs = {str(r[0]) for r in rows}
    if not provs:
        pytest.skip(f"{_BRONZE_SCHEMA}.{_TABLE} is empty (pre-recompute) — coverage assertion N/A")

    assert "wyscout" not in provs, f"event-only rows present (frames-required violation): {sorted(provs)}"
    assert provs <= {"idsse", "metrica", "skillcorner", "gradientsports", "statsbomb"}, (
        f"unexpected provider in {_TABLE}: {sorted(provs)}"
    )

    # Every statsbomb AC match must map to a bronze.statsbomb_360 match (canonical long-cast id
    # join, mirroring discovery's _find_sb360_new_ids). A non-empty result is a frames-required
    # violation. Schema/table refs are module constants.
    orphan_q = (
        f"SELECT DISTINCT cast(a.match_id as string) FROM {_BRONZE_SCHEMA}.{_TABLE} a "  # noqa: S608
        f"LEFT ANTI JOIN {_BRONZE_SCHEMA}.statsbomb_360 s "
        f"ON cast(a.match_id as long) = cast(s.match_id as long) "
        f"WHERE a.data_source = 'statsbomb'"
    )
    orphans = _execute_sql(orphan_q)
    assert not orphans, (
        f"statsbomb action-context rows without freeze-frames (frames-required violation): {[r[0] for r in orphans]}"
    )
