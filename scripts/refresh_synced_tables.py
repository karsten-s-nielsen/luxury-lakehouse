#!/usr/bin/env python3
"""Trigger SNAPSHOT refresh on Lakebase synced tables.

Lakebase synced tables with ``scheduling_policy = "SNAPSHOT"`` do not
auto-refresh.  This script triggers a pipeline update for each synced table
via the Databricks REST API, optionally waiting for all pipelines to reach
``IDLE`` state before exiting.

Run this script after any upstream dbt rebuild that materialises new data into
Gold Delta tables.

Usage:
    python scripts/refresh_synced_tables.py                     # Fire-and-forget (all 34)
    python scripts/refresh_synced_tables.py --wait              # Wait until all syncs complete
    python scripts/refresh_synced_tables.py --tables fct_shots_synced,dim_teams_synced

Requires:
    - ``databricks`` CLI configured with an OAUTH profile
    - Network access to the Databricks workspace
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import requests

_raw_host = os.environ["DATABRICKS_HOST"]  # Required — fail fast if missing
DATABRICKS_HOST = f"https://{_raw_host}" if not _raw_host.startswith("https://") else _raw_host
CATALOG = "soccer_analytics"
DEFAULT_SCHEMA = "dev_gold"

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


def _get_auth_token() -> str:
    """Get a workspace token via Databricks CLI OAuth."""
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "OAUTH"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["access_token"]


def _get_pipeline_id(table: str, headers: dict[str, str], schema: str = DEFAULT_SCHEMA) -> str:
    """Fetch the pipeline_id backing a synced table."""
    full_name = f"{CATALOG}.{schema}.{table}"
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/database/synced_tables/{full_name}",
        headers=headers,
        verify=True,
        timeout=(10, 30),
    )
    resp.raise_for_status()
    return resp.json()["data_synchronization_status"]["pipeline_id"]


def _trigger_refresh(pipeline_id: str, headers: dict[str, str]) -> tuple[str, bool]:
    """Trigger a pipeline update. Returns (update_id, already_running)."""
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/pipelines/{pipeline_id}/updates",
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
            f"{DATABRICKS_HOST}/api/2.0/pipelines/{pipeline_id}",
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
        help="Comma-separated subset of table names (default: all 11)",
    )
    args = parser.parse_args()

    # Build lookup: table_name -> schema
    table_schema_map: dict[str, str] = {name: (override or DEFAULT_SCHEMA) for name, override in SYNCED_TABLES}
    all_table_names = list(table_schema_map.keys())

    if args.tables:
        selected = [t.strip() for t in args.tables.split(",") if t.strip()]
        for t in selected:
            if t not in table_schema_map:
                print(f"ERROR: Unknown table '{t}'. Valid: {', '.join(all_table_names)}")
                sys.exit(1)
    else:
        selected = all_table_names

    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    total = len(selected)
    errors = 0
    triggered: list[tuple[str, str]] = []  # (table, pipeline_id)

    for i, table in enumerate(selected, 1):
        try:
            schema = table_schema_map[table]
            pipeline_id = _get_pipeline_id(table, headers, schema=schema)
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
