#!/usr/bin/env python3
"""Fix Lakebase synced table pipeline event_log ownership drift (Bug B-2d).

Each Lakebase synced table is backed by a Databricks Delta Live Tables (DLT)
pipeline, which owns an internal ``event_log_<pipeline_id>`` table in the same
schema. On this workspace 33 of 34 synced tables are currently in
``SYNCED_TABLE_ONLINE_PIPELINE_FAILED`` because the pipeline's run_as user
has neither ``SELECT`` nor ``MODIFY`` on its own event_log:

    PERMISSION_DENIED: User does not have MODIFY on Table
      'soccer_analytics.dev_gold.event_log_<pipeline_id>'

**Root cause**: the event_log tables are owned by the ``luxury-lakehouse-ingestion-dev``
service principal (because it created them), but the pipeline's ``run_as_user_name``
is ``karstenskyt@gmail.com`` (a workspace admin). UC requires the caller to be
the object owner or schema owner for ``ALTER TABLE ... OWNER TO`` statements.

**Proven fix**: transfer ownership to the ``dbt-owners-dev`` group, which
contains BOTH identities (``karstenskyt@gmail.com`` + ingestion SP). Both
principals gain full privileges via group membership, so the pipeline
updates can once again write to the event_log, and future runs don't drift.

Verified on ``dim_competitions_synced`` during the gold-data-repair session:
before the fix the pipeline update was failing; after ``ALTER TABLE OWNER TO
dbt-owners-dev`` the pipeline immediately went CREATED -> COMPLETED in ~45 s.

**Authentication wrinkle** — ``ALTER TABLE OWNER TO`` can only be executed
by a principal with MANAGE on the event_log. Two fix paths handle this:

1. **SP-owned tables** (Phase B): The ingestion SP owns the event_log but has
   no persistent OAuth secret. We submit a one-shot notebook run with
   ``run_as: {service_principal_name: <ingestion_sp>}`` via the Jobs API.
   The run executes as the SP in Databricks' managed runtime.

2. **User-owned tables** (Phase B2): When synced tables are recreated via the
   Databricks UI, the event_log may be owned by the human user who triggered
   the initial sync. These tables are detected via ``WorkspaceClient.
   current_user.me()`` and fixed directly via the Statement Execution API
   (which runs as the caller's PAT identity). No SP notebook needed.

Usage:
    # Dry run — discover state, report, do nothing
    uv run python scripts/fix_event_log_ownership.py --dry-run

    # Fix all 37 synced tables (default)
    uv run python scripts/fix_event_log_ownership.py

    # Subset
    uv run python scripts/fix_event_log_ownership.py --tables fct_shots_synced,dim_teams_synced

    # Different owner (staging)
    uv run python scripts/fix_event_log_ownership.py --target-owner dbt-owners-staging

Exit codes:
    0 — success (or dry-run completed without errors)
    1 — discovery / fix / verify / refresh had any failure
    2 — CLI validation failure

Auth: uses ``WorkspaceClient`` for environment-agnostic credentials (PAT,
OAuth M2M, CLI profile). The caller must be a workspace admin to submit
``run_as`` runs against a service principal.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
import uuid
from typing import TYPE_CHECKING, Any

import requests

from ingestion.refresh_synced_tables import (
    DEFAULT_CATALOG,
    DEFAULT_SCHEMA,
    SYNCED_TABLES,
    _classify_pipeline_poll_response,
)
from shared.constants import IDENTIFIER_RE

# PR-Cycle-B (2026-05-01): databricks-sdk is in the [sdk] optional extra.
# Lazy-import keeps this script importable for pytest collection of
# test_fix_event_log_ownership.py (which tests pure helper functions).
# Same pattern as src/ingestion/refresh_synced_tables.py.
if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
else:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        WorkspaceClient = None  # type: ignore[assignment, misc]

# Windows consoles default to cp1252 which crashes on UTF-8 characters used in
# docstrings (em-dash, arrows). Reconfigure stdout/stderr to UTF-8 at module
# load so --help / JSON log lines render consistently regardless of platform.
# Python 3.7+ exposes ``reconfigure`` on TextIOWrapper.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOG_SOURCE = "fix_event_log_ownership"

# Ingestion service principal — needs MANAGE on the event_log tables to run
# ``ALTER TABLE ... OWNER TO``. Captured at script-runtime via ``run_as`` on
# a submit run. Defaults to the dev-env ingestion SP application_id; can be
# overridden via ``--sp-application-id`` for staging/prod.
_DEFAULT_INGESTION_SP_APP_ID = "008b207b-96a8-4d54-b185-a77479a55abe"

# Group name validation — accepts alphanumerics, hyphens, and underscores.
# IDENTIFIER_RE from shared.constants disallows hyphens (SQL identifier pattern),
# but Databricks group names (e.g., ``dbt-owners-dev``) legitimately contain them.
# This regex is the defensive floor against SQL injection — any backtick,
# semicolon, space, quote, or wildcard is rejected so the quoted name is safe
# inside ``OWNER TO \`<name>\``` syntax.
_GROUP_NAME_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z][a-zA-Z0-9_\-]*$")

# Databricks service principal application_id pattern. Canonical UUID-36
# format: 8-4-4-4-12 lowercase hex (UC/Jobs APIs accept uppercase too).
_UUID_RE: re.Pattern[str] = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

# Project SQL warehouse name prefix — used to resolve the warehouse ID when
# ``DATABRICKS_SQL_WAREHOUSE_ID`` / ``DATABRICKS_HTTP_PATH`` are not set.
_PROJECT_WAREHOUSE_NAME_PREFIX = "soccer-analytics-warehouse"

# HTTP timeouts — (connect, read) in seconds
_TIMEOUT_DEFAULT: tuple[int, int] = (10, 30)
_TIMEOUT_SUBMIT: tuple[int, int] = (10, 60)
_TIMEOUT_SQL: tuple[int, int] = (10, 120)

# Poll interval for the notebook submit run (seconds)
_SUBMIT_POLL_INTERVAL_S = 10
_SUBMIT_MAX_POLL_S = 300  # 5 min — typical run is ~45 s

# Poll interval + ceiling for the Statement Execution API. A single
# information_schema query is normally sub-second; the 180 s ceiling is
# a defensive floor in case the API wedges on a metadata lookup.
_SQL_POLL_INTERVAL_S: int = 2
_SQL_MAX_POLL_S: int = 180  # 3 min ceiling on a single SQL statement


# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------


def _log(event: str, **kwargs: object) -> None:
    """Emit a single JSON-line structured log record to stdout."""
    record = {"source": _LOG_SOURCE, "event": event, **kwargs}
    print(json.dumps(record, default=str), flush=True)


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested)
# ---------------------------------------------------------------------------


def _event_log_table_name(pipeline_id: str) -> str:
    """Convert a pipeline_id to its DLT event_log table name.

    Example:
        >>> _event_log_table_name("4ea189db-aa43-4144-8825-da54cf965b7f")
        'event_log_4ea189db_aa43_4144_8825_da54cf965b7f'
    """
    return f"event_log_{pipeline_id.replace('-', '_')}"


def _event_log_fqn(*, catalog: str, schema: str, pipeline_id: str) -> str:
    """Build the fully qualified name of a DLT event_log table."""
    return f"{catalog}.{schema}.{_event_log_table_name(pipeline_id)}"


def _build_alter_owner_notebook_source(event_log_fqns: list[str], target_owner: str) -> str:
    """Build a Databricks SQL notebook source with one ALTER TABLE per fqn.

    The notebook is created as a pure SQL notebook (``language=SQL`` on upload)
    so each cell is raw SQL — no Python magic required. Cells are separated by
    ``-- COMMAND ----------`` markers (the SOURCE-format convention). Each
    ALTER gets its own cell so a parse error in one statement does not mask
    the others. Target owner is wrapped in backticks so hyphenated group names
    parse as identifiers (e.g., ``dbt-owners-dev``).

    Callers MUST pre-validate ``event_log_fqns`` and ``target_owner`` with
    ``_validate_group_name`` and ``IDENTIFIER_RE`` — this helper does no
    input sanitization itself.

    NOTE: an earlier revision tried ``# MAGIC %sql`` cells inside a PYTHON
    notebook. The workspace then parsed the source as a SQL stream and failed
    with ``PARSE_SYNTAX_ERROR at '%'`` on the first ``# MAGIC`` line, so we
    switched to a pure SQL notebook.
    """
    cells = [f"ALTER TABLE {fqn} OWNER TO `{target_owner}`;" for fqn in event_log_fqns]
    body = "\n-- COMMAND ----------\n".join(cells)
    return f"-- Databricks notebook source\n{body}\n"


def _classify_table_ownership(
    table_owners: dict[str, str],
    target_owner: str,
    fixer_principal: str | None = None,
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Partition ``table_owners`` into (already_correct, needs_fix, skip_wrong_owner, missing).

    The fix path runs ``ALTER TABLE ... OWNER TO <target_owner>`` as a specific
    principal (``fixer_principal`` — typically the SP that currently owns the
    tables). That principal can only ALTER tables it currently owns, so tables
    owned by someone ELSE (neither ``target_owner`` nor ``fixer_principal``)
    must be excluded from the notebook — otherwise the cell fails with
    ``PERMISSION_DENIED``. One real-world case: ``workflow_cost_live_synced``
    has an event_log owned by ``karstenskyt@gmail.com`` directly, matching its
    own pipeline run_as, so it works as-is and should not be touched.

    Args:
        table_owners: mapping of event_log fqn -> current owner string.
            A fqn absent from this dict is NOT classified as ``missing`` here —
            the caller detects missing entries by comparing the requested fqn
            list to the dict's keys.
        target_owner: the desired owner group name (e.g., ``dbt-owners-dev``).
        fixer_principal: the principal that will run the ALTER statements
            (the application ID / UUID of the service principal). Tables
            owned by this principal are fixable via the SP notebook.
            If ``None``, any table not owned by target_owner is classified
            as ``needs_fix`` (legacy behaviour, pre-3-bucket).

    Returns:
        Four lists in insertion order:
          - ``already_correct`` — current owner == target_owner (no action)
          - ``needs_fix`` — current owner == fixer_principal (or fixer_principal
            is None and owner != target_owner) → include in notebook
          - ``skip_wrong_owner`` — current owner is a third principal that the
            fixer cannot modify → log a warning and leave alone
          - ``missing`` — always empty from this helper (caller's responsibility)
    """
    already_correct: list[str] = []
    needs_fix: list[str] = []
    skip_wrong_owner: list[str] = []
    missing: list[str] = []
    for fqn, owner in table_owners.items():
        if owner == target_owner:
            already_correct.append(fqn)
        elif fixer_principal is None or owner == fixer_principal:
            needs_fix.append(fqn)
        else:
            skip_wrong_owner.append(fqn)
    return already_correct, needs_fix, skip_wrong_owner, missing


def _validate_group_name(name: str) -> bool:
    r"""Return True iff ``name`` is safe to interpolate inside ``OWNER TO `<name>` ``.

    Accepts alphanumerics, hyphens, and underscores (Databricks group name
    alphabet). Rejects anything with backticks, semicolons, quotes, wildcards,
    or whitespace — the defensive floor against SQL injection even though
    the statement executes inside a ``%sql`` notebook cell.
    """
    return bool(_GROUP_NAME_RE.match(name))


def _validate_uuid(value: str) -> bool:
    """Return True iff ``value`` matches the canonical UUID-36 format."""
    return bool(_UUID_RE.match(value))


# ---------------------------------------------------------------------------
# Databricks REST helpers
# ---------------------------------------------------------------------------


def _get_host_and_headers(ws: WorkspaceClient) -> tuple[str, dict[str, str]]:
    """Resolve the workspace host and auth headers from a WorkspaceClient."""
    host = ws.config.host
    if not host:
        msg = "WorkspaceClient could not resolve a Databricks workspace host"
        raise RuntimeError(msg)
    host = host if host.startswith("https://") else f"https://{host}"
    headers = ws.config.authenticate()
    headers["Content-Type"] = "application/json"
    return host, headers


def _get_pipeline_id(
    *,
    host: str,
    headers: dict[str, str],
    catalog: str,
    schema: str,
    table: str,
) -> str:
    """Fetch the pipeline_id backing a synced table via the REST API.

    The returned pipeline_id is validated against the canonical UUID-36
    format before being returned to the caller — the value is later
    interpolated into SQL strings (via ``_event_log_table_name``) and URL
    paths (via ``_trigger_pipeline_update`` / ``_poll_pipeline_refresh``),
    so trusting the API output unchecked would be a latent injection risk.
    """
    full_name = f"{catalog}.{schema}.{table}"
    resp = requests.get(
        f"{host}/api/2.0/database/synced_tables/{full_name}",
        headers=headers,
        verify=True,
        timeout=_TIMEOUT_DEFAULT,
    )
    resp.raise_for_status()
    pipeline_id = resp.json().get("data_synchronization_status", {}).get("pipeline_id")
    if not pipeline_id:
        msg = f"synced_tables API returned no pipeline_id for {full_name}"
        raise RuntimeError(msg)
    if not _validate_uuid(pipeline_id):
        msg = f"synced_tables API returned non-UUID pipeline_id {pipeline_id!r} for {full_name}"
        raise RuntimeError(msg)
    return pipeline_id


def _resolve_warehouse_id(ws: WorkspaceClient) -> str:
    """Resolve the project SQL warehouse ID via the SDK.

    Matches the strategy used in ``ingestion.dbt_runner``: list all warehouses
    and pick the one whose name starts with the project prefix. Avoids
    depending on ``DATABRICKS_SQL_WAREHOUSE_ID`` / ``DATABRICKS_HTTP_PATH``
    being set in the caller's environment.
    """
    warehouses = list(ws.warehouses.list())
    for wh in warehouses:
        if wh.name and wh.name.startswith(_PROJECT_WAREHOUSE_NAME_PREFIX) and wh.id:
            return wh.id
    msg = (
        f"No SQL warehouse with name starting with {_PROJECT_WAREHOUSE_NAME_PREFIX!r} "
        "found via WorkspaceClient.warehouses.list()"
    )
    raise RuntimeError(msg)


def _execute_sql(
    *,
    host: str,
    headers: dict[str, str],
    warehouse_id: str,
    sql: str,
) -> list[list[Any]]:
    """Execute a SQL statement via the Statement Execution API and return rows.

    Uses INLINE disposition for small result sets (information_schema queries
    scoped to a few hundred rows). Polls until the statement reaches a
    terminal state (SUCCEEDED / FAILED / CANCELED).
    """
    submit_resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "30s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        verify=True,
        timeout=_TIMEOUT_SQL,
    )
    submit_resp.raise_for_status()
    result = submit_resp.json()

    statement_id = result.get("statement_id", "")
    status = result.get("status", {}).get("state")

    start_time = time.monotonic()
    while status in ("PENDING", "RUNNING"):
        if time.monotonic() - start_time > _SQL_MAX_POLL_S:
            msg = f"SQL statement timed out after {_SQL_MAX_POLL_S}s: {sql[:200]}"
            raise RuntimeError(msg)
        time.sleep(_SQL_POLL_INTERVAL_S)
        poll_resp = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            verify=True,
            timeout=_TIMEOUT_DEFAULT,
        )
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")

    if status != "SUCCEEDED":
        error = result.get("status", {}).get("error", {})
        msg = f"SQL statement failed ({status}): {error.get('message', 'unknown error')}"
        raise RuntimeError(msg)

    data_array = result.get("result", {}).get("data_array", [])
    if not isinstance(data_array, list):
        return []
    return data_array


def _query_event_log_owners(
    *,
    host: str,
    headers: dict[str, str],
    warehouse_id: str,
    catalog: str,
    schema_to_tables: dict[str, list[str]],
) -> dict[str, str]:
    """Query ``information_schema.tables`` in bulk for current event_log owners.

    Args:
        schema_to_tables: mapping of schema -> list of event_log table names
            (without catalog/schema prefix). Tables are grouped per schema so
            the query is a single UNION ALL across schemas.

    Returns:
        Mapping of fully qualified event_log name -> owner string. A table
        absent from the result (because it doesn't exist) is simply omitted
        from the dict; the caller detects that by comparison.
    """
    results: dict[str, str] = {}
    for schema, tables in schema_to_tables.items():
        if not tables:
            continue
        # All inputs are pre-validated before reaching this function:
        # - ``catalog`` and ``schema`` match IDENTIFIER_RE (validated in _validate_cli_args
        #   for --catalog/--schema; per-table schema overrides in SYNCED_TABLES are
        #   hard-coded literals).
        # - ``tables`` entries are derived from pipeline_id strings returned by the
        #   Databricks API and passed through _event_log_table_name, which only
        #   replaces hyphens with underscores. A malicious pipeline_id could in
        #   principle contain shell/SQL metacharacters, but the Databricks API
        #   guarantees pipeline IDs are UUID-36 hex strings.
        in_list = ", ".join(f"'{t}'" for t in tables)
        # ruff S608: identifier + table-name inputs are pre-validated; see comment above.
        sql = (
            f"SELECT table_name, table_owner "  # noqa: S608
            f"FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema}' "
            f"  AND table_name IN ({in_list})"
        )
        rows = _execute_sql(host=host, headers=headers, warehouse_id=warehouse_id, sql=sql)
        for row in rows:
            if len(row) >= 2 and row[0] is not None:
                fqn = f"{catalog}.{schema}.{row[0]}"
                results[fqn] = row[1] or ""
    return results


# ---------------------------------------------------------------------------
# Phase B — SP notebook submit (the workaround for lack of an OAuth secret)
# ---------------------------------------------------------------------------


def _upload_notebook(*, host: str, headers: dict[str, str], path: str, source: str) -> None:
    """Upload a SOURCE-format SQL notebook to the workspace via raw REST.

    The SDK's ``w.workspace.upload`` sometimes chokes on format detection.
    The raw REST API with explicit ``format=SOURCE, language=SQL`` is reliable
    and matches the pure-SQL notebook source produced by
    ``_build_alter_owner_notebook_source``.
    """
    content_b64 = base64.b64encode(source.encode("utf-8")).decode("ascii")
    resp = requests.post(
        f"{host}/api/2.0/workspace/import",
        headers=headers,
        json={
            "path": path,
            "format": "SOURCE",
            "language": "SQL",
            "content": content_b64,
            "overwrite": True,
        },
        verify=True,
        timeout=_TIMEOUT_DEFAULT,
    )
    resp.raise_for_status()


def _delete_notebook(*, host: str, headers: dict[str, str], path: str) -> None:
    """Best-effort notebook deletion. Swallows errors — clean-up, not critical."""
    try:
        requests.post(
            f"{host}/api/2.0/workspace/delete",
            headers=headers,
            json={"path": path, "recursive": False},
            verify=True,
            timeout=_TIMEOUT_DEFAULT,
        )
    except Exception as exc:
        _log("notebook_delete_failed", path=path, error=str(exc)[:200])


def _submit_run_as_sp(
    *,
    host: str,
    headers: dict[str, str],
    notebook_path: str,
    sp_app_id: str,
    run_name: str,
) -> int:
    """Submit a one-shot serverless run of ``notebook_path`` as the given SP.

    The workspace admin running this script must have permission to submit
    runs with ``run_as: {service_principal_name: ...}`` — typically the
    ``admins`` group membership grants this.

    Returns the ``run_id``.
    """
    submit_body = {
        "run_name": run_name,
        "tasks": [
            {
                "task_key": "fix_grants",
                "notebook_task": {"notebook_path": notebook_path},
                "environment_key": "default",
            }
        ],
        "environments": [
            {
                "environment_key": "default",
                "spec": {"client": "1"},  # serverless client 1
            }
        ],
        "run_as": {"service_principal_name": sp_app_id},
    }
    resp = requests.post(
        f"{host}/api/2.1/jobs/runs/submit",
        headers=headers,
        json=submit_body,
        verify=True,
        timeout=_TIMEOUT_SUBMIT,
    )
    resp.raise_for_status()
    run_id = resp.json().get("run_id")
    if not isinstance(run_id, int):
        msg = f"runs/submit returned no run_id: {resp.json()!r}"
        raise RuntimeError(msg)
    return run_id


def _poll_submit_run(*, ws: WorkspaceClient, run_id: int) -> tuple[str, str]:
    """Poll a submit run until terminal. Returns (life_cycle_state, result_state)."""
    start = time.monotonic()
    while True:
        info = ws.jobs.get_run(run_id=run_id)
        life = "?"
        result = "?"
        if info.state is not None:
            if info.state.life_cycle_state is not None:
                life = info.state.life_cycle_state.value
            if info.state.result_state is not None:
                result = info.state.result_state.value
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return life, result
        if time.monotonic() - start > _SUBMIT_MAX_POLL_S:
            return "TIMEOUT", result
        time.sleep(_SUBMIT_POLL_INTERVAL_S)


def _fetch_run_output(*, ws: WorkspaceClient, run_id: int) -> str:
    """Return the most informative error string from a terminal-FAILED job run.

    For a ``notebook_task`` running ``%sql`` cells, the actual error (e.g.,
    ``PERMISSION_DENIED`` on ``ALTER TABLE``) is typically surfaced in
    ``run.tasks[0].state.state_message``, NOT in
    ``jobs.get_run_output().error`` (which is reserved for explicit Python
    exceptions from ``dbutils.notebook.exit(...)`` style handlers). Check
    order, richest → sparsest:

        1. Task-level notebook output ``error`` (explicit Python exception)
        2. Task-level notebook output ``error_trace`` (traceback)
        3. Task-level ``state_message`` (best source for %sql cell errors)
        4. Run-level ``state_message``
        5. Fallback constant

    Accepts ``run_id`` (rather than a run object) because the orchestrator
    only tracks run_ids — we re-fetch the run here to get a consistent
    terminal snapshot. Best-effort throughout: any fetch failure is
    swallowed into the returned string so the caller can still log it.
    """
    try:
        run = ws.jobs.get_run(run_id=run_id)
    except Exception as exc:
        return f"(fetch run failed: {exc})"[:2000]

    # 1 + 2. Task-level notebook output (may raise if task had no output)
    try:
        if run.tasks:
            first_task_run_id = run.tasks[0].run_id
            if first_task_run_id is not None:
                out = ws.jobs.get_run_output(run_id=first_task_run_id)
                if out.error:
                    return f"[notebook_error] {out.error[:2000]}"
                if out.error_trace:
                    return f"[notebook_trace] {out.error_trace[:2000]}"
    except Exception as exc:
        # get_run_output can fail (e.g., task produced no output for %sql
        # cells). Log for traceability and fall through to the
        # state_message fallbacks below — which is where %sql errors live.
        _log("fetch_run_output_fallback", run_id=run_id, error=str(exc)[:200])

    # 3. Task-level state_message — best source for %sql cell errors
    if run.tasks:
        task_state = run.tasks[0].state
        if task_state is not None and task_state.state_message:
            return f"[task_state] {task_state.state_message[:2000]}"

    # 4. Run-level state_message
    if run.state is not None and run.state.state_message:
        return f"[run_state] {run.state.state_message[:2000]}"

    # 5. Fallback
    return "(no error surfaced — check run history manually)"


# ---------------------------------------------------------------------------
# Phase D — trigger refresh (re-uses ingestion.refresh_synced_tables helpers)
# ---------------------------------------------------------------------------


def _trigger_pipeline_update(*, host: str, headers: dict[str, str], pipeline_id: str) -> bool:
    """POST an update to a pipeline. Returns True on success (or already-running)."""
    resp = requests.post(
        f"{host}/api/2.0/pipelines/{pipeline_id}/updates",
        headers=headers,
        json={},
        verify=True,
        timeout=_TIMEOUT_DEFAULT,
    )
    if resp.status_code == 409:
        return True  # already running
    resp.raise_for_status()
    return True


def _poll_pipeline_refresh(*, host: str, headers: dict[str, str], pipeline_id: str, max_wait_s: int = 900) -> str:
    """Poll a pipeline until its most-recent-update reaches a terminal state.

    Re-uses the classification logic from ``ingestion.refresh_synced_tables`` so
    the semantics match the daily-job refresh code path exactly.

    Returns ``"IDLE"`` on success, ``"FAILED"`` / ``"DELETED"`` / ``"TIMEOUT"``
    on problems.
    """
    start = time.monotonic()
    while True:
        resp = requests.get(
            f"{host}/api/2.0/pipelines/{pipeline_id}",
            headers=headers,
            verify=True,
            timeout=_TIMEOUT_DEFAULT,
        )
        resp.raise_for_status()
        classification = _classify_pipeline_poll_response(resp.json())
        if classification is not None:
            return classification
        if time.monotonic() - start > max_wait_s:
            return "TIMEOUT"
        time.sleep(15)


# ---------------------------------------------------------------------------
# CLI + orchestrator
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix Lakebase synced-table pipeline event_log ownership drift.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover state and report, but do not mutate anything.",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated subset of synced table names (default: all 34).",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CATALOG,
        help=f"Unity Catalog catalog name (default: {DEFAULT_CATALOG}).",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=DEFAULT_SCHEMA,
        help=(
            f"Default schema for synced tables without a per-table override "
            f"(default: {DEFAULT_SCHEMA}). Per-table overrides in "
            f"ingestion.refresh_synced_tables.SYNCED_TABLES still apply."
        ),
    )
    parser.add_argument(
        "--target-owner",
        type=str,
        default="dbt-owners-dev",
        help="Group to ALTER ownership to (default: dbt-owners-dev).",
    )
    parser.add_argument(
        "--sp-application-id",
        type=str,
        default=_DEFAULT_INGESTION_SP_APP_ID,
        help=(
            "Service principal application_id to run_as for the ALTER TABLE "
            f"statements (default: {_DEFAULT_INGESTION_SP_APP_ID})."
        ),
    )
    parser.add_argument(
        "--skip-trigger-refresh",
        action="store_true",
        help="After fixing ownership, do NOT trigger pipeline refresh (default: trigger + poll).",
    )
    return parser.parse_args(argv)


def _validate_cli_args(args: argparse.Namespace) -> None:
    """Validate identifier-style CLI args and abort via sys.exit(2) on failure."""
    if not IDENTIFIER_RE.match(args.catalog):
        print(
            f"ERROR: Invalid --catalog {args.catalog!r}. Must match {IDENTIFIER_RE.pattern}",
            file=sys.stderr,
        )
        sys.exit(2)
    if not IDENTIFIER_RE.match(args.schema):
        print(
            f"ERROR: Invalid --schema {args.schema!r}. Must match {IDENTIFIER_RE.pattern}",
            file=sys.stderr,
        )
        sys.exit(2)
    if not _validate_group_name(args.target_owner):
        print(
            f"ERROR: Invalid --target-owner {args.target_owner!r}. Must match {_GROUP_NAME_RE.pattern}",
            file=sys.stderr,
        )
        sys.exit(2)
    if not _validate_uuid(args.sp_application_id):
        print(
            f"ERROR: Invalid --sp-application-id {args.sp_application_id!r}. "
            "Must be a UUID-36 string (e.g., 008b207b-96a8-4d54-b185-a77479a55abe).",
            file=sys.stderr,
        )
        sys.exit(2)


def _select_tables(args: argparse.Namespace) -> dict[str, str]:
    """Return an ordered mapping of selected table_name -> schema.

    Honours per-table schema overrides from ``SYNCED_TABLES``, falls back to
    ``args.schema`` otherwise. If ``args.tables`` is set, filters to that
    subset (and aborts on any unknown name).
    """
    table_schema_map: dict[str, str] = {name: (override or args.schema) for name, override in SYNCED_TABLES}
    if not args.tables:
        return table_schema_map

    requested = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = [t for t in requested if t not in table_schema_map]
    if unknown:
        print(
            f"ERROR: Unknown tables: {', '.join(unknown)}. Valid: {', '.join(table_schema_map.keys())}",
            file=sys.stderr,
        )
        sys.exit(2)
    return {t: table_schema_map[t] for t in requested}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _validate_cli_args(args)

    _log(
        "start",
        dry_run=args.dry_run,
        catalog=args.catalog,
        target_owner=args.target_owner,
        sp_application_id=args.sp_application_id,
        skip_trigger_refresh=args.skip_trigger_refresh,
    )

    selected = _select_tables(args)
    _log("tables_selected", count=len(selected), tables=list(selected.keys()))

    ws = WorkspaceClient()
    host, headers = _get_host_and_headers(ws)

    # ------------------------------------------------------------------
    # Phase A — discover pipeline IDs + current event_log owners
    # ------------------------------------------------------------------
    _log("phase", name="A_discover")

    pipeline_ids: dict[str, str] = {}  # synced_table_name -> pipeline_id
    schema_to_tables: dict[str, list[str]] = {}  # schema -> [event_log_table_name, ...]
    event_log_fqns: dict[str, str] = {}  # synced_table_name -> event_log fqn
    discovery_errors = 0

    for table_name, schema in selected.items():
        try:
            pid = _get_pipeline_id(
                host=host,
                headers=headers,
                catalog=args.catalog,
                schema=schema,
                table=table_name,
            )
        except Exception as exc:
            _log("pipeline_id_lookup_failed", table=table_name, error=str(exc)[:200])
            discovery_errors += 1
            continue
        pipeline_ids[table_name] = pid
        event_log_name = _event_log_table_name(pid)
        event_log_fqns[table_name] = _event_log_fqn(catalog=args.catalog, schema=schema, pipeline_id=pid)
        schema_to_tables.setdefault(schema, []).append(event_log_name)

    if not pipeline_ids:
        _log("abort", reason="no_pipeline_ids_resolved", errors=discovery_errors)
        return 1

    # Resolve warehouse for information_schema queries
    try:
        warehouse_id = _resolve_warehouse_id(ws)
    except Exception as exc:
        _log("warehouse_resolution_failed", error=str(exc)[:200])
        return 1
    _log("warehouse_resolved", warehouse_id=warehouse_id)

    try:
        owner_map = _query_event_log_owners(
            host=host,
            headers=headers,
            warehouse_id=warehouse_id,
            catalog=args.catalog,
            schema_to_tables=schema_to_tables,
        )
    except Exception as exc:
        _log("information_schema_query_failed", error=str(exc)[:200])
        return 1

    # Classify: for each selected synced table, is its event_log owned correctly?
    # We use the fqns as keys (matching what _query_event_log_owners returns).
    fqn_to_owner: dict[str, str] = {}
    for fqn in event_log_fqns.values():
        if fqn in owner_map:
            fqn_to_owner[fqn] = owner_map[fqn]

    already_correct, needs_fix, skip_wrong_owner, _ = _classify_table_ownership(
        fqn_to_owner,
        args.target_owner,
        fixer_principal=args.sp_application_id,
    )
    missing_fqns = [fqn for fqn in event_log_fqns.values() if fqn not in owner_map]

    # Detect current user identity for the "direct fix" path — tables owned
    # by the caller can be ALTERed via the Statement Execution API directly,
    # without needing the SP run_as notebook.  Handles the case where synced
    # tables were recreated via the Databricks UI (owner = the UI user).
    current_user_name: str | None = None
    try:
        me = ws.current_user.me()
        if me.user_name:
            current_user_name = me.user_name
            _log("current_user_detected", user_name=current_user_name)
    except Exception as exc:
        _log("current_user_detection_failed", error=str(exc)[:200])

    # Split skip_wrong_owner into tables the current user can fix directly
    # (via Statement Execution API) and tables that are truly unfixable.
    fixable_by_self: list[str] = []
    truly_skipped: list[str] = []
    for fqn in skip_wrong_owner:
        if current_user_name and fqn_to_owner.get(fqn) == current_user_name:
            fixable_by_self.append(fqn)
        else:
            truly_skipped.append(fqn)

    # Per-table report
    fixable_by_self_set = set(fixable_by_self)
    for table_name, fqn in event_log_fqns.items():
        current_owner = owner_map.get(fqn)
        if current_owner is None:
            _log(
                "table_state",
                synced_table=table_name,
                event_log=fqn,
                status="missing_from_information_schema",
            )
        elif current_owner == args.target_owner:
            _log(
                "table_state",
                synced_table=table_name,
                event_log=fqn,
                status="already_correct",
                current_owner=current_owner,
            )
        elif current_owner == args.sp_application_id:
            _log(
                "table_state",
                synced_table=table_name,
                event_log=fqn,
                status="needs_fix",
                current_owner=current_owner,
            )
        elif fqn in fixable_by_self_set:
            _log(
                "table_state",
                synced_table=table_name,
                event_log=fqn,
                status="fixable_by_self",
                current_owner=current_owner,
                note="owned by the current user — will fix via Statement Execution API",
            )
        else:
            _log(
                "table_state",
                synced_table=table_name,
                event_log=fqn,
                status="skip_wrong_owner",
                current_owner=current_owner,
                note=("current owner is neither target, fixer SP, nor current user; fixer lacks MANAGE. Not touched."),
            )

    _log(
        "discovery_summary",
        selected=len(selected),
        pipeline_ids_resolved=len(pipeline_ids),
        pipeline_id_lookup_errors=discovery_errors,
        already_correct=len(already_correct),
        needs_fix=len(needs_fix),
        fixable_by_self=len(fixable_by_self),
        skip_wrong_owner=len(truly_skipped),
        missing=len(missing_fqns),
    )

    if not needs_fix and not fixable_by_self:
        _log(
            "complete",
            outcome="no_fix_needed",
            already_correct=len(already_correct),
            skip_wrong_owner=len(truly_skipped),
        )
        return 0 if discovery_errors == 0 else 1

    # ------------------------------------------------------------------
    # Dry run — print the SQL that would be generated and exit
    # ------------------------------------------------------------------
    if args.dry_run:
        if needs_fix:
            notebook_source = _build_alter_owner_notebook_source(needs_fix, args.target_owner)
            _log(
                "dry_run_preview",
                method="sp_notebook",
                statement_count=len(needs_fix),
                notebook_source=notebook_source,
            )
        if fixable_by_self:
            direct_stmts = [f"ALTER TABLE {fqn} OWNER TO `{args.target_owner}`;" for fqn in fixable_by_self]
            _log(
                "dry_run_preview",
                method="statement_execution_api",
                statement_count=len(fixable_by_self),
                statements=direct_stmts,
            )
        return 0 if discovery_errors == 0 else 1

    # ------------------------------------------------------------------
    # Phase B — submit run_as SP notebook (for SP-owned tables)
    # ------------------------------------------------------------------
    if needs_fix:
        _log("phase", name="B_apply_fix", count=len(needs_fix))

        notebook_source = _build_alter_owner_notebook_source(needs_fix, args.target_owner)
        # UUID-derived suffix avoids path collisions when two invocations fire within
        # the same second (e.g., automated retries after a transient failure).
        suffix = uuid.uuid4().hex[:12]
        notebook_path = f"/Shared/fix_event_log_ownership_{suffix}"

        try:
            _upload_notebook(host=host, headers=headers, path=notebook_path, source=notebook_source)
            _log("notebook_uploaded", path=notebook_path, statements=len(needs_fix))
        except Exception as exc:
            _log("notebook_upload_failed", error=str(exc)[:200])
            return 1

        # Notebook now exists in /Shared/ — guarantee cleanup via try/finally even
        # if a network flake escapes from _submit_run_as_sp / _poll_submit_run /
        # _fetch_run_output. _delete_notebook is best-effort (swallows its own
        # errors) so the finally block cannot itself raise.
        try:
            try:
                run_id = _submit_run_as_sp(
                    host=host,
                    headers=headers,
                    notebook_path=notebook_path,
                    sp_app_id=args.sp_application_id,
                    run_name=f"fix_event_log_ownership_{suffix}",
                )
                _log("run_submitted", run_id=run_id)
            except Exception as exc:
                _log("run_submit_failed", error=str(exc)[:200])
                return 1

            life, result_state = _poll_submit_run(ws=ws, run_id=run_id)
            _log("run_terminal", run_id=run_id, life_cycle_state=life, result_state=result_state)

            if life != "TERMINATED" or result_state != "SUCCESS":
                error_snippet = _fetch_run_output(ws=ws, run_id=run_id)
                _log("run_failed", run_id=run_id, error=error_snippet)
                return 1
        finally:
            _delete_notebook(host=host, headers=headers, path=notebook_path)

    # ------------------------------------------------------------------
    # Phase B2 — direct fix for tables owned by the current user
    # ------------------------------------------------------------------
    # Tables in skip_wrong_owner that are owned by the caller can be ALTERed
    # directly via the Statement Execution API (which runs as the PAT/OAuth
    # identity). No SP notebook needed — the current user IS the owner.
    direct_fix_errors = 0
    if fixable_by_self:
        _log("phase", name="B2_direct_fix", count=len(fixable_by_self))
        for fqn in fixable_by_self:
            # Input safety: fqn is derived from validated catalog + schema +
            # UUID-format pipeline_id (via _event_log_fqn); target_owner is
            # validated by _validate_group_name. Same guarantees as the notebook.
            sql = f"ALTER TABLE {fqn} OWNER TO `{args.target_owner}`;"
            try:
                _execute_sql(host=host, headers=headers, warehouse_id=warehouse_id, sql=sql)
                _log("direct_fix_applied", event_log=fqn)
            except Exception as exc:
                _log("direct_fix_failed", event_log=fqn, error=str(exc)[:200])
                direct_fix_errors += 1
        if direct_fix_errors:
            _log(
                "direct_fix_partial",
                applied=len(fixable_by_self) - direct_fix_errors,
                failed=direct_fix_errors,
            )

    # ------------------------------------------------------------------
    # Phase C — verify ownership was updated
    # ------------------------------------------------------------------
    _log("phase", name="C_verify")

    try:
        post_owner_map = _query_event_log_owners(
            host=host,
            headers=headers,
            warehouse_id=warehouse_id,
            catalog=args.catalog,
            schema_to_tables=schema_to_tables,
        )
    except Exception as exc:
        _log("verify_query_failed", error=str(exc)[:200])
        return 1

    # Verify all tables that had a fix attempted (both SP + direct paths)
    all_to_verify = needs_fix + fixable_by_self
    still_broken: list[str] = []
    for fqn in all_to_verify:
        new_owner = post_owner_map.get(fqn)
        if new_owner != args.target_owner:
            still_broken.append(fqn)
            _log(
                "verify_mismatch",
                event_log=fqn,
                expected_owner=args.target_owner,
                actual_owner=new_owner,
            )

    if still_broken:
        _log("verify_failed", count=len(still_broken))
        return 1

    total_fixed = len(all_to_verify)
    _log("verify_passed", fixed=total_fixed)

    # ------------------------------------------------------------------
    # Phase D — trigger pipeline refreshes on the fixed tables
    # ------------------------------------------------------------------
    if args.skip_trigger_refresh:
        _log(
            "complete",
            outcome="fixed_without_refresh",
            already_correct=len(already_correct),
            fixed=total_fixed,
        )
        return 0

    _log("phase", name="D_refresh")

    # Map fqns back to synced-table names so we can look up their pipeline_ids.
    fqn_to_table: dict[str, str] = {fqn: table_name for table_name, fqn in event_log_fqns.items()}

    refresh_complete: list[str] = []
    refresh_failed: list[str] = []

    for fqn in all_to_verify:
        table_name = fqn_to_table[fqn]
        pid = pipeline_ids[table_name]
        try:
            _trigger_pipeline_update(host=host, headers=headers, pipeline_id=pid)
            _log("refresh_triggered", synced_table=table_name, pipeline_id=pid)
        except Exception as exc:
            _log("refresh_trigger_failed", synced_table=table_name, error=str(exc)[:200])
            refresh_failed.append(table_name)
            continue

        try:
            state = _poll_pipeline_refresh(host=host, headers=headers, pipeline_id=pid)
        except Exception as exc:
            _log("refresh_poll_error", synced_table=table_name, error=str(exc)[:200])
            refresh_failed.append(table_name)
            continue

        if state == "IDLE":
            refresh_complete.append(table_name)
            _log("refresh_complete", synced_table=table_name)
        else:
            refresh_failed.append(table_name)
            _log("refresh_non_idle", synced_table=table_name, state=state)

    _log(
        "complete",
        outcome="fixed_and_refreshed" if not refresh_failed else "fixed_with_refresh_errors",
        already_correct=len(already_correct),
        fixed=total_fixed,
        refresh_complete=len(refresh_complete),
        refresh_failed=len(refresh_failed),
    )
    return 0 if not refresh_failed and discovery_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
