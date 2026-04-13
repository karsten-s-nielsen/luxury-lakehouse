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

import requests
from databricks.sdk import WorkspaceClient

from shared.constants import IDENTIFIER_RE

DEFAULT_CATALOG = "soccer_analytics"
DEFAULT_SCHEMA = "dev_gold"

_CACHED_HOST: str | None = None


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
        ws = WorkspaceClient()
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
    ("workflow_cost_live_synced", "observability"),
    ("dim_players_synced", None),
    ("dim_teams_synced", None),
    ("dim_competitions_synced", None),
]

POLL_INTERVAL_S = 30
MAX_POLL_ATTEMPTS = 60  # 30 min max wait


def _get_auth_headers() -> dict[str, str]:
    """Get Databricks auth headers via WorkspaceClient.

    Auto-detects credentials in priority order:
    PAT (DATABRICKS_TOKEN) → OAuth M2M (DATABRICKS_CLIENT_ID/SECRET) →
    CLI profile → ambient runtime context (Databricks job).
    """
    ws = WorkspaceClient()
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


def _poll_pipeline(pipeline_id: str, headers: dict[str, str]) -> str:
    """Poll a pipeline until it reaches IDLE or fails. Returns final state."""
    for _ in range(MAX_POLL_ATTEMPTS):
        resp = requests.get(
            f"{_get_host()}/api/2.0/pipelines/{pipeline_id}",
            headers=headers,
            verify=True,
            timeout=(10, 30),
        )
        resp.raise_for_status()
        state: str = resp.json().get("state", "UNKNOWN")
        if state in ("IDLE", "FAILED", "DELETED"):
            return state
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
