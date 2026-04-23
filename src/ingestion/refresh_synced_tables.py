"""Trigger SNAPSHOT refresh on Lakebase synced tables.

Lakebase synced tables with ``scheduling_policy = "SNAPSHOT"`` do not
auto-refresh.  This module triggers a pipeline update for each synced table
via the Databricks REST API, optionally waiting for all pipelines to reach
``IDLE`` state before exiting.

Run this after any upstream dbt rebuild that materialises new data into
Gold Delta tables, OR as the final task in the daily Databricks job to
propagate warm-tier observability data into Lakebase.

Usage (local):
    python -m ingestion.refresh_synced_tables                     # Fire-and-forget
    python -m ingestion.refresh_synced_tables --wait              # Wait until all syncs complete
    python -m ingestion.refresh_synced_tables --tables fct_shots_synced,dim_teams_synced

Console-script entry point: ``refresh_synced_tables`` (registered in pyproject.toml).

Auth: uses ``WorkspaceClient`` for environment-agnostic credentials —
PAT, OAuth M2M, CLI profile, and Databricks runtime context all work.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Mapping

import requests
from databricks.sdk import WorkspaceClient

from shared.constants import IDENTIFIER_RE

DEFAULT_CATALOG = "soccer_analytics"
DEFAULT_SCHEMA = "dev_gold"

# Default expected owner of DLT pipeline event_log tables. Synced-table refresh
# depends on the SP that invokes the refresh being able to read the event_log;
# if the event_log drifts to a user principal (as happened 2026-04-02→2026-04-14
# for 33 of 34 synced tables), the pipeline's most-recent-update will fail with
# SYNCED_TABLE_ONLINE_PIPELINE_FAILED and the silent-success bug in the old
# poll path would hide it for days. Group ownership via ``dbt-owners-{env}``
# includes both the developer user and the ingestion SP, so both can ALTER the
# event_log. See ``scripts/fix_event_log_ownership.py`` for the backfill tool.
_DEFAULT_EXPECTED_EVENT_LOG_OWNER = "dbt-owners-dev"

_CACHED_HOST: str | None = None


def _get_workspace_client() -> WorkspaceClient:
    """Return a Databricks ``WorkspaceClient`` instance.

    Extracted as a single seam that tests can patch at module level
    (via ``monkeypatch.setattr("ingestion.refresh_synced_tables.WorkspaceClient", ...)``
    or via an autouse fixture) so unit tests never hit real credentials
    resolution. CI has no ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` env
    vars, and ``WorkspaceClient()`` otherwise fails with
    ``default auth: cannot configure default credentials``.

    Not cached here — ``_get_host`` caches the resolved host string so
    the constructor is called at most once per process during normal
    operation.
    """
    return WorkspaceClient()


def _get_host() -> str:
    """Resolve the Databricks workspace URL at runtime via WorkspaceClient.

    Uses ``WorkspaceClient().config.host`` which auto-resolves the workspace
    URL from the SDK's unified auth context. Works in all environments:
    - Local dev: reads DATABRICKS_HOST env var / CLI profile
    - CI: reads DATABRICKS_HOST env var (set via GitHub Actions secrets)
    - Taipy HF Space: reads DATABRICKS_HOST secret env var
    - Databricks job: reads from runtime workspace context (env var NOT set)

    A prior implementation read ``os.environ["DATABRICKS_HOST"]`` directly,
    which raised KeyError in Databricks jobs where OAuth M2M auth still works
    but the env var is not set in the task runtime. WorkspaceClient handles
    all four cases consistently.

    Cached per-process after first lookup to avoid creating a new
    WorkspaceClient on every HTTP call during a refresh run (34 tables x
    3 HTTP calls each = 102 calls).
    """
    global _CACHED_HOST
    if _CACHED_HOST is None:
        ws = _get_workspace_client()
        host = ws.config.host
        if not host:
            msg = "WorkspaceClient could not resolve a Databricks workspace host"
            raise RuntimeError(msg)
        _CACHED_HOST = host if host.startswith("https://") else f"https://{host}"
    return _CACHED_HOST


# Synced tables: (table_name, schema_override or None for DEFAULT_SCHEMA).
# Tables in non-default schemas (e.g., observability) use the override.
SYNCED_TABLES: list[tuple[str, str | None]] = [
    ("fct_shots_synced", None),
    ("fct_xg_predictions_synced", None),
    # v2 xG mart added 2026-04-22 (PR 3, ADR-013): first mart applying the
    # ML-inference-outputs pattern (bronze raw → dbt staging → gold mart).
    ("fct_xg_predictions_v2_synced", None),
    ("fct_passes_synced", None),
    ("fct_player_stats_synced", None),
    ("fct_match_summary_synced", None),
    ("fct_player_embeddings_synced", None),
    ("fct_action_values_synced", None),
    ("fct_tracking_frames_synced", None),
    ("fct_physical_stats_synced", None),
    ("fct_defensive_values_synced", None),
    ("fct_defcon_actions_synced", None),
    ("fct_defcon_pressure_synced", None),
    ("fct_workflow_costs_synced", None),
    ("fct_formation_labels_synced", None),
    ("fct_player_positions_synced", None),
    ("fct_position_maps_synced", None),
    ("fct_player_embeddings_career_synced", None),
    ("fct_player_embeddings_season_synced", None),
    ("fct_line_breaking_results_synced", None),
    ("fct_pausa_rankings_synced", None),
    ("fct_player_percentiles_synced", None),
    ("fct_off_ball_xt_synced", None),
    ("fct_goalkeeper_stats_synced", None),
    ("fct_player_embeddings_career_360_synced", None),
    ("fct_player_embeddings_season_360_synced", None),
    ("fct_space_creation_synced", None),
    ("fct_pausa_values_synced", None),
    ("fct_pass_timing_synced", None),
    ("fct_tracking_avg_positions_synced", None),
    ("fct_tracking_shape_timeline_synced", None),
    # Pre-aggregated marts added 2026-04-17 (perf/base-case-query-bottlenecks)
    ("fct_heatmap_agg_synced", None),
    ("fct_vaep_breakdown_agg_synced", None),
    ("fct_gk_actions_detail_synced", None),
    # Pre-aggregated mart added 2026-04-18 (perf/conversion-funnel, D58)
    ("fct_funnel_stages_agg_synced", None),
    # Match Summary redesign 2026-04-19 — discipline events for Row 1 / Row 2
    ("fct_discipline_events_synced", None),
    ("workflow_cost_live_synced", "observability"),
    ("dim_players_synced", None),
    ("dim_teams_synced", None),
    ("dim_competitions_synced", None),
    # Kimball conformed match dimension added 2026-04-20 (ADR-011)
    ("dim_matches_synced", None),
]

POLL_INTERVAL_S = 30
MAX_POLL_ATTEMPTS = 60  # 30 min max wait


def _get_auth_headers() -> dict[str, str]:
    """Get Databricks auth headers via WorkspaceClient.

    Auto-detects credentials in priority order:
    PAT (DATABRICKS_TOKEN) → OAuth M2M (DATABRICKS_CLIENT_ID/SECRET) →
    CLI profile → ambient runtime context (Databricks job).
    """
    ws = _get_workspace_client()
    return ws.config.authenticate()


def _get_pipeline_id(
    table: str,
    headers: dict[str, str],
    *,
    catalog: str,
    schema: str,
) -> str:
    """Fetch the pipeline_id backing a synced table.

    catalog/schema are required keyword args — never reads module state.
    """
    full_name = f"{catalog}.{schema}.{table}"
    resp = requests.get(
        f"{_get_host()}/api/2.0/database/synced_tables/{full_name}",
        headers=headers,
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    return resp.json()["data_synchronization_status"]["pipeline_id"]


def _trigger_refresh(pipeline_id: str, headers: dict[str, str]) -> tuple[str, bool]:
    """Trigger a pipeline update. Returns (update_id, already_running)."""
    resp = requests.post(
        f"{_get_host()}/api/2.0/pipelines/{pipeline_id}/updates",
        headers=headers,
        json={},
        verify=True,
        timeout=(10, 30),
    )
    if resp.status_code == 409:
        # Pipeline already has an active update — not an error
        return ("", True)
    resp.raise_for_status()
    return (resp.json().get("update_id", ""), False)


# Update lifecycle states that mean "still in flight — keep polling".
# Source: Databricks REST API /api/2.0/pipelines/{id} latest_updates[].state enum.
_UPDATE_IN_FLIGHT_STATES: frozenset[str] = frozenset(
    {
        "RUNNING",
        "CREATED",
        "QUEUED",
        "WAITING_FOR_RESOURCES",
        "INITIALIZING",
        "SETTING_UP_TABLES",
        "RESETTING",
        "RESYNCING",
    }
)


def _classify_pipeline_poll_response(pipeline_json: Mapping[str, object]) -> str | None:
    """Classify a single ``/api/2.0/pipelines/<id>`` response into a terminal poll state.

    The top-level ``state`` field reports whether the pipeline is currently
    executing (``RUNNING``) or not (``IDLE``/``FAILED``/``DELETED``). It does
    NOT reflect the success or failure of the most recent update — that lives
    in ``latest_updates[0].state``. A pipeline whose last update FAILED still
    reports top-level ``IDLE`` once the failing update finishes.

    Returns:
        ``"IDLE"``    — most recent update reached ``COMPLETED`` (success).
        ``"FAILED"``  — most recent update reached ``FAILED`` or ``CANCELED``.
        ``"DELETED"`` — the pipeline itself has been deleted.
        ``None``      — no terminal classification yet; caller should continue
                        polling (update is still in flight, no updates exist
                        yet, or the reported update state is unrecognised).
    """
    top_state = pipeline_json.get("state")
    if top_state == "DELETED":
        return "DELETED"

    latest_updates = pipeline_json.get("latest_updates") or []
    if not isinstance(latest_updates, list) or not latest_updates:
        return None

    first = latest_updates[0]
    if not isinstance(first, dict):
        return None
    upd_state = first.get("state")
    if upd_state == "COMPLETED":
        return "IDLE"
    if upd_state in ("FAILED", "CANCELED"):
        return "FAILED"
    if upd_state in _UPDATE_IN_FLIGHT_STATES:
        return None
    # Unknown update state — defensive: continue polling. If the state never
    # transitions to a recognised value, the caller's MAX_POLL_ATTEMPTS
    # ceiling will eventually surface the problem as a TIMEOUT error.
    return None


def _event_log_fqn(*, catalog: str, schema: str, pipeline_id: str) -> str:
    """Compute the fully-qualified name of a DLT pipeline's event_log table.

    DLT creates an ``event_log_<pipeline_id_with_dashes_replaced>`` table in
    the same schema as the synced table it backs. Keep in sync with
    ``scripts/fix_event_log_ownership.py:_event_log_fqn`` — the backfill tool
    uses the identical naming rule.
    """
    return f"{catalog}.{schema}.event_log_{pipeline_id.replace('-', '_')}"


def _fetch_table_owner(fqn: str, headers: dict[str, str]) -> str | None:
    """Fetch the owner principal of a Unity Catalog table via the tables API.

    Returns:
        The owner principal (e.g., ``"dbt-owners-dev"``, an SP application id,
        or a user email), or ``None`` if the table does not exist (HTTP 404).

    Raises:
        ``requests.HTTPError`` for non-200/404 responses so the caller can
        decide whether to abort the refresh run.
    """
    resp = requests.get(
        f"{_get_host()}/api/2.1/unity-catalog/tables/{fqn}",
        headers=headers,
        verify=True,
        timeout=(10, 30),
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    owner = resp.json().get("owner")
    return owner if isinstance(owner, str) else None


def _check_event_log_ownership(
    pipeline_id: str,
    headers: dict[str, str],
    *,
    catalog: str,
    schema: str,
    expected_owner: str,
) -> tuple[bool, str, str | None]:
    """Verify the DLT pipeline's event_log is owned by ``expected_owner``.

    Returns:
        ``(is_ok, fqn, actual_owner)``. ``is_ok`` is ``True`` when ownership
        matches OR when the event_log does not yet exist (brand-new pipeline,
        no drift possible). ``is_ok`` is ``False`` only when the event_log
        exists but its owner differs from ``expected_owner`` — in that case
        the caller MUST NOT trigger a refresh, because the DLT update will
        fail on event_log write permission and the silent-success bug would
        hide it until the next manual audit.
    """
    fqn = _event_log_fqn(catalog=catalog, schema=schema, pipeline_id=pipeline_id)
    actual = _fetch_table_owner(fqn, headers)
    if actual is None:
        return (True, fqn, None)
    return (actual == expected_owner, fqn, actual)


def _poll_pipeline(pipeline_id: str, headers: dict[str, str]) -> str:
    """Poll a pipeline until its MOST RECENT UPDATE reaches a terminal state.

    Returns one of:
        ``"IDLE"``    — most recent update COMPLETED (success).
        ``"FAILED"``  — most recent update FAILED or CANCELED.
        ``"DELETED"`` — pipeline was deleted.
        ``"TIMEOUT"`` — poll exhausted ``MAX_POLL_ATTEMPTS`` without a terminal state.

    NOTE: We deliberately do NOT return when the top-level pipeline ``state``
    reaches ``IDLE`` — that just means "not currently executing" and can
    coexist with a FAILED most-recent-update. The previous version of this
    function had that bug, producing silent-success reports on failed syncs.
    The ``"IDLE"`` return value is retained for the success path so that
    callers (see :func:`main`) can keep their existing equality check against
    ``"IDLE"`` for "COMPLETE" — the semantics now mean "most recent update
    COMPLETED", not "pipeline currently idle".
    """
    for _ in range(MAX_POLL_ATTEMPTS):
        resp = requests.get(
            f"{_get_host()}/api/2.0/pipelines/{pipeline_id}",
            headers=headers,
            verify=True,
            timeout=(10, 30),
        )
        resp.raise_for_status()
        classification = _classify_pipeline_poll_response(resp.json())
        if classification is not None:
            return classification
        time.sleep(POLL_INTERVAL_S)
    return "TIMEOUT"


def main() -> None:
    """Trigger snapshot refresh on synced tables."""
    parser = argparse.ArgumentParser(description="Refresh Lakebase synced tables.")
    parser.add_argument("--wait", action="store_true", help="Poll until all pipelines reach IDLE")
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated subset of table names (default: all 34)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CATALOG,
        help=f"Unity Catalog catalog name (default: {DEFAULT_CATALOG})",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=DEFAULT_SCHEMA,
        help=(
            f"Default schema for synced tables that have no per-table override "
            f"(default: {DEFAULT_SCHEMA}). Per-table overrides in SYNCED_TABLES "
            f"(e.g., observability) still apply."
        ),
    )
    parser.add_argument(
        "--expected-event-log-owner",
        type=str,
        default=_DEFAULT_EXPECTED_EVENT_LOG_OWNER,
        help=(
            f"Expected owner of DLT pipeline event_log tables. Each synced "
            f"table is pre-checked before its refresh is triggered; drift "
            f"here is a hard error. Default: {_DEFAULT_EXPECTED_EVENT_LOG_OWNER}."
        ),
    )
    parser.add_argument(
        "--skip-event-log-check",
        action="store_true",
        help=(
            "Skip the event_log ownership pre-check. Emergency bypass only — "
            "normal refresh runs should leave this off so drift fails loudly."
        ),
    )
    args = parser.parse_args()

    # Validate identifiers per CLAUDE.md security rule (regex prevents SQL injection
    # via the catalog.schema.table string interpolated into the synced-table URL).
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

    # Build lookup: table_name -> schema (per-table override beats CLI default)
    table_schema_map: dict[str, str] = {name: (override or args.schema) for name, override in SYNCED_TABLES}
    all_table_names = list(table_schema_map.keys())

    if args.tables:
        selected = [t.strip() for t in args.tables.split(",") if t.strip()]
        for t in selected:
            if t not in table_schema_map:
                print(f"ERROR: Unknown table '{t}'. Valid: {', '.join(all_table_names)}")
                sys.exit(1)
    else:
        selected = all_table_names

    headers = _get_auth_headers()
    headers["Content-Type"] = "application/json"

    total = len(selected)
    errors = 0
    triggered: list[tuple[str, str]] = []  # (table, pipeline_id)

    for i, table in enumerate(selected, 1):
        try:
            schema = table_schema_map[table]
            pipeline_id = _get_pipeline_id(table, headers, catalog=args.catalog, schema=schema)

            # Pre-check event_log ownership — ownership drift here has already
            # caused a multi-day outage once (2026-04-02→2026-04-14, 33 of 34
            # synced tables). Fail fast with an actionable error rather than
            # trigger the refresh and let the DLT update fail silently.
            if not args.skip_event_log_check:
                ok, fqn, actual = _check_event_log_ownership(
                    pipeline_id,
                    headers,
                    catalog=args.catalog,
                    schema=schema,
                    expected_owner=args.expected_event_log_owner,
                )
                if not ok:
                    print(
                        f"[{i}/{total}] DRIFT: {table} event_log {fqn} owned by "
                        f"{actual!r}, expected {args.expected_event_log_owner!r}. "
                        f"Fix: python scripts/fix_event_log_ownership.py "
                        f"--tables {table}"
                    )
                    errors += 1
                    continue

            _, already_running = _trigger_refresh(pipeline_id, headers)
            if already_running:
                print(f"[{i}/{total}] Already running: {table}")
            else:
                print(f"[{i}/{total}] Triggered refresh: {table}")
            triggered.append((table, pipeline_id))
        except Exception as exc:
            print(f"[{i}/{total}] ERROR triggering {table}: {exc}")
            errors += 1

    if args.wait and triggered:
        print(f"\nWaiting for {len(triggered)} pipelines (polling every {POLL_INTERVAL_S}s)...")
        for table, pipeline_id in triggered:
            try:
                state = _poll_pipeline(pipeline_id, headers)
                if state == "IDLE":
                    print(f"  {table}: COMPLETE")
                else:
                    print(f"  {table}: {state}")
                    errors += 1
            except Exception as exc:
                print(f"  {table}: POLL ERROR — {exc}")
                errors += 1

    print(f"\nSummary: {len(triggered)} triggered, {errors} errors")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
