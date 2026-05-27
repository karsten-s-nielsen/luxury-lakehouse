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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import requests

from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    timed_check,
)
from ingestion.utils import get_spark_session
from shared.constants import IDENTIFIER_RE

# PR-Cycle-B (2026-05-01): databricks-sdk is in the [sdk] optional extra
# (kept out of default deps so the wheel deployed to Databricks serverless
# stays lean; the runtime auto-provides the SDK there). Module-level import
# would make this whole module unimportable when the extra isn't installed,
# breaking pytest collection on any test file that imports any helper from
# here. The try/except + TYPE_CHECKING pattern keeps:
#   - module importable without the SDK (collection works)
#   - WorkspaceClient at module level for tests to monkeypatch (existing
#     mocks at test_refresh_synced_tables.py:51,68 keep working)
#   - type-checker sees the real symbol via TYPE_CHECKING
#   - functions that actually need the SDK raise a clear error if it's
#     missing (better than the original ModuleNotFoundError on import)
if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
else:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        WorkspaceClient = None  # type: ignore[assignment, misc]

from dataclasses import dataclass
from typing import Literal

_SCHEDULING_POLICIES = ("SNAPSHOT", "TRIGGERED")


@dataclass(frozen=True)
class SyncedTableConfig:
    """Single source of truth for a Lakebase synced table definition.

    All consumers — create, delete, refresh, grants, indexes — read from the
    ``SYNCED_TABLES`` list of these configs. No metadata split between TF and Python.
    """

    name: str  # e.g. "fct_shots_synced"
    source_table: str  # e.g. "fct_shots"
    primary_key_columns: tuple[str, ...]
    scheduling_policy: Literal["SNAPSHOT", "TRIGGERED"] = "SNAPSHOT"
    schema_override: str | None = None  # None -> DEFAULT_SCHEMA ("dev_gold")

    def __post_init__(self) -> None:
        if self.scheduling_policy not in _SCHEDULING_POLICIES:
            msg = (
                f"Invalid scheduling_policy {self.scheduling_policy!r} for {self.name}. "
                f"Must be one of {_SCHEDULING_POLICIES}"
            )
            raise ValueError(msg)


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

    Raises ``ImportError`` with an actionable message when the
    ``[sdk]`` extra isn't installed.
    """
    if WorkspaceClient is None:
        msg = (
            "databricks-sdk is not installed. Install the [sdk] extra: "
            "`uv sync --extra sdk` (CI does this automatically; locally "
            "it's an opt-in to keep the wheel lean for Databricks serverless)."
        )
        raise ImportError(msg)
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


SYNCED_TABLES: list[SyncedTableConfig] = [
    # ── Fact tables ──────────────────────────────────────────────────────────
    SyncedTableConfig("fct_shots_synced", "fct_shots", ("shot_id",)),
    SyncedTableConfig("fct_xg_predictions_v2_synced", "fct_xg_predictions_v2", ("shot_id",)),
    SyncedTableConfig("fct_passes_synced", "fct_passes", ("pass_id",), "TRIGGERED"),
    SyncedTableConfig("fct_player_stats_synced", "fct_player_stats", ("player_stats_id",)),
    SyncedTableConfig("fct_match_summary_synced", "fct_match_summary", ("match_key",)),
    SyncedTableConfig("fct_player_embeddings_synced", "fct_player_embeddings", ("embedding_id",), "TRIGGERED"),
    SyncedTableConfig("fct_action_values_synced", "fct_action_values", ("action_value_id",), "TRIGGERED"),
    SyncedTableConfig("fct_tracking_frames_synced", "fct_tracking_frames", ("tracking_id",), "TRIGGERED"),
    SyncedTableConfig("fct_physical_stats_synced", "fct_physical_stats", ("physical_stats_id",)),
    SyncedTableConfig("fct_defensive_values_synced", "fct_defensive_values", ("defensive_value_id",), "TRIGGERED"),
    SyncedTableConfig("fct_defcon_actions_synced", "fct_defcon_actions", ("defcon_action_id",), "TRIGGERED"),
    SyncedTableConfig("fct_defcon_pressure_synced", "fct_defcon_pressure", ("pressure_id",), "TRIGGERED"),
    SyncedTableConfig("fct_workflow_costs_synced", "fct_workflow_costs", ("task_key", "usage_date", "job_run_id")),
    SyncedTableConfig("fct_formation_labels_synced", "fct_formation_labels", ("formation_label_id",)),
    SyncedTableConfig("fct_player_positions_synced", "fct_player_positions", ("position_id",)),
    SyncedTableConfig("fct_position_maps_synced", "fct_position_maps", ("position_map_id",)),
    SyncedTableConfig("fct_player_embeddings_career_synced", "fct_player_embeddings_career", ("canonical_player_id",)),
    SyncedTableConfig("fct_player_embeddings_season_synced", "fct_player_embeddings_season", ("embedding_season_id",)),
    SyncedTableConfig(
        "fct_line_breaking_results_synced",
        "fct_line_breaking_results",
        ("line_breaking_id",),
        "TRIGGERED",
    ),
    SyncedTableConfig("fct_pausa_rankings_synced", "fct_pausa_rankings", ("player_id",)),
    SyncedTableConfig(
        "fct_player_percentiles_synced",
        "fct_player_percentiles",
        ("player_id", "competition_id", "season_id"),
    ),
    SyncedTableConfig("fct_off_ball_xt_synced", "fct_off_ball_xt", ("off_ball_xt_id",), "TRIGGERED"),
    SyncedTableConfig("fct_goalkeeper_stats_synced", "fct_goalkeeper_stats", ("gk_stat_id",)),
    SyncedTableConfig(
        "fct_player_embeddings_career_360_synced",
        "fct_player_embeddings_career_360",
        ("canonical_player_id",),
    ),
    SyncedTableConfig(
        "fct_player_embeddings_season_360_synced",
        "fct_player_embeddings_season_360",
        ("embedding_season_360_id",),
    ),
    SyncedTableConfig("fct_space_creation_synced", "fct_space_creation", ("space_creation_id",), "TRIGGERED"),
    SyncedTableConfig("fct_pausa_values_synced", "fct_pausa_values", ("pass_id",), "TRIGGERED"),
    SyncedTableConfig("fct_pass_timing_synced", "fct_pass_timing", ("player_id", "match_id")),
    SyncedTableConfig("fct_tracking_avg_positions_synced", "fct_tracking_avg_positions", ("avg_position_id",)),
    SyncedTableConfig(
        "fct_tracking_shape_timeline_synced",
        "fct_tracking_shape_timeline",
        ("shape_timeline_id",),
        "TRIGGERED",
    ),
    # Pre-aggregated marts
    SyncedTableConfig(
        "fct_heatmap_agg_synced",
        "fct_heatmap_agg",
        ("competition_id", "team_id", "action_type", "x_bin", "y_bin"),
    ),
    SyncedTableConfig(
        "fct_vaep_breakdown_agg_synced",
        "fct_vaep_breakdown_agg",
        ("competition_id", "team_id", "player_id", "action_type"),
    ),
    SyncedTableConfig("fct_gk_actions_detail_synced", "fct_gk_actions_detail", ("gk_action_id",)),
    SyncedTableConfig("fct_funnel_stages_agg_synced", "fct_funnel_stages_agg", ("match_id", "team_id", "game_state")),
    SyncedTableConfig("fct_discipline_events_synced", "fct_discipline_events", ("event_id",)),
    SyncedTableConfig("fct_tracking_context_synced", "fct_tracking_context", ("tracking_context_id",)),
    SyncedTableConfig("fct_action_context_synced", "fct_action_context", ("action_context_id",), "TRIGGERED"),
    # ── Cost / Observability ─────────────────────────────────────────────────
    SyncedTableConfig("workflow_cost_live_synced", "workflow_cost_live", ("run_id",), schema_override="observability"),
    # ── Dimension tables ─────────────────────────────────────────────────────
    SyncedTableConfig("dim_players_synced", "dim_players", ("player_key",)),
    SyncedTableConfig("dim_teams_synced", "dim_teams", ("team_key",)),
    SyncedTableConfig("dim_competitions_synced", "dim_competitions", ("competition_id",)),
    SyncedTableConfig("dim_matches_synced", "dim_matches", ("match_key",)),
]

POLL_INTERVAL_S = 30
MAX_POLL_ATTEMPTS = 60  # 30 min max wait

SYNCED_TABLE_ONLINE_STATE = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"

# detailed_state values that mean the synced table has failed permanently;
# distinguished from in-flight states by the fact that further polling is pointless.
_SYNCED_TABLE_TERMINAL_FAILURE_STATES: frozenset[str] = frozenset(
    {
        "SYNCED_TABLE_OFFLINE",
        "SYNCED_TABLE_OFFLINE_FAILED",
    }
)


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
    *,
    catalog: str,
    schema: str,
) -> str:
    """Fetch the pipeline_id backing a synced table via the SDK postgres API.

    catalog/schema are required keyword args — never reads module state.
    """
    full_name = f"{catalog}.{schema}.{table}"
    ws = _get_workspace_client()
    meta = ws.postgres.get_synced_table(name=f"synced_tables/{full_name}")
    status = getattr(meta, "status", None)
    pid = getattr(status, "pipeline_id", None) if status else None
    if not pid:
        msg = f"Synced table {full_name} has no pipeline_id in status"
        raise RuntimeError(msg)
    return pid


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


# Default concurrency for parallel polling. Each worker only does HTTP I/O
# (a `_poll_pipeline` call which sleeps `POLL_INTERVAL_S` between requests),
# so threads are correct here, not processes. 10 workers means at most 10
# in-flight requests at any moment — well under any reasonable Databricks
# REST API rate limit (the actual request rate is ~1/30s per pipeline).
_DEFAULT_POLL_MAX_WORKERS = 10


def _poll_pipelines_parallel(
    triggered: list[tuple[str, str]],
    headers: dict[str, str],
    *,
    max_workers: int = _DEFAULT_POLL_MAX_WORKERS,
) -> dict[str, str]:
    """Poll multiple pipelines concurrently; return ``{table: terminal_state}``.

    Replaces the previous sequential for-loop in :func:`main` whose worst-case
    wait was ``Sum(MAX_POLL_ATTEMPTS * POLL_INTERVAL_S)`` per pipeline (30 min
    per pipeline times N pipelines). The 2026-04-25 PR 5b deploy hit this: one slow
    pipeline (`fct_workflow_costs_synced`, fixed in PR #203) ran out the full
    30-min ceiling while the other 40 had reached IDLE within 3-4 min, so the
    script took 40 min instead of ~5 min. Parallel polling collapses the
    worst case to a single 30-min ceiling regardless of N.

    Each table is polled in its own thread; results print as they complete
    (via :func:`concurrent.futures.as_completed`) so a slow pipeline does not
    delay reporting on faster ones. Per-pipeline exceptions are caught and
    surfaced as ``"ERROR: <message>"`` strings rather than propagated, so a
    single transient network blip does not abort the whole batch.

    Args:
        triggered: ``[(table_name, pipeline_id), ...]`` from the trigger phase.
        headers: Auth + content-type headers for the polling HTTP requests.
        max_workers: Concurrency cap. Default 10; tune via the CLI flag.

    Returns:
        ``{table: state}`` where state is the value from
        :func:`_poll_pipeline` (``"IDLE"`` / ``"FAILED"`` / ``"DELETED"`` /
        ``"TIMEOUT"``) or ``"ERROR: <message>"`` if polling raised.
    """
    results: dict[str, str] = {}

    def _poll_one(item: tuple[str, str]) -> tuple[str, str]:
        table, pipeline_id = item
        try:
            return (table, _poll_pipeline(pipeline_id, headers))
        except Exception as exc:
            return (table, f"ERROR: {exc}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_poll_one, item): item for item in triggered}
        for future in as_completed(futures):
            table, state = future.result()
            results[table] = state
            if state == "IDLE":
                print(f"  {table}: COMPLETE")
            else:
                print(f"  {table}: {state}")

    return results


def wait_until_online(
    table_fqn: str,
    *,
    timeout_s: int = 600,
    poll_interval_s: int = 15,
) -> None:
    """Poll a Lakebase synced table until detailed_state == SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE.

    Introduced in PR 4b (2026-04-23) as G1 of the SDK-synced-table-path
    hardening. Not called anywhere in PR 4b itself — ships ready for the
    future PR that switches synced-table creation to
    ``w.postgres.synced_tables.*`` SDK path. See On-Deck entry "SDK
    synced-table path hardening (G2 + G3 from Kimball PR 4)" in TODO.md.

    Args:
        table_fqn: Fully-qualified Unity Catalog name of the synced table,
            e.g. ``"soccer_analytics.dev_gold.fct_action_values_synced"``.
        timeout_s: Maximum total wait time. Default 600s (10 min).
        poll_interval_s: Seconds between status polls. Default 15s.

    Raises:
        TimeoutError: if the table does not reach ``SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE``
            within ``timeout_s``; message includes table FQN + last-seen detailed_state
            + elapsed seconds.
        RuntimeError: if the table hits a terminal failure state
            (``SYNCED_TABLE_OFFLINE``, ``SYNCED_TABLE_OFFLINE_FAILED``).
        requests.HTTPError: propagated on 4xx/5xx from the status endpoint
            (including HTTP 404 when the table does not exist).
    """
    if not IDENTIFIER_RE.match(table_fqn.split(".")[-1]):
        raise ValueError(f"Invalid table_fqn last-segment: {table_fqn!r}")

    ws = _get_workspace_client()

    start = time.monotonic()
    last_state: str | None = None
    while True:
        meta = ws.postgres.get_synced_table(name=f"synced_tables/{table_fqn}")
        status = getattr(meta, "status", None)
        raw_state = getattr(status, "detailed_state", None) if status else None
        # SDK returns SyncedTableState enum; extract .value for string comparison
        detailed_state_str = raw_state.value if raw_state else "UNKNOWN"
        last_state = detailed_state_str

        if detailed_state_str == SYNCED_TABLE_ONLINE_STATE:
            return

        if detailed_state_str in _SYNCED_TABLE_TERMINAL_FAILURE_STATES:
            raise RuntimeError(f"Synced table {table_fqn} reached terminal failure state {detailed_state_str!r}")

        elapsed = time.monotonic() - start
        if elapsed > timeout_s:
            raise TimeoutError(
                f"Synced table {table_fqn} did not reach {SYNCED_TABLE_ONLINE_STATE} "
                f"within {timeout_s}s (last detailed_state: {last_state!r}, elapsed: {elapsed:.1f}s)"
            )

        time.sleep(poll_interval_s)


class _RefreshSyncedTablesGuard:
    """Watermark guard that derives upstream tables from SYNCED_TABLES."""

    workflow_id = "wf-refresh-synced-tables"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        upstream = _derive_upstream_tables(catalog, schema)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)


def _derive_upstream_tables(catalog: str, default_schema: str) -> list[str]:
    """Derive upstream Delta table FQNs from SYNCED_TABLES.

    For each ``SyncedTableConfig``, uses ``source_table`` and qualifies with
    the override schema (or default).
    """
    tables: list[str] = []
    for config in SYNCED_TABLES:
        effective_schema = config.schema_override or default_schema
        tables.append(f"{catalog}.{effective_schema}.{config.source_table}")
    return tables


skip_guard = _RefreshSyncedTablesGuard()


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
    parser.add_argument(
        "--max-workers",
        type=int,
        default=_DEFAULT_POLL_MAX_WORKERS,
        help=(
            f"Concurrency cap for --wait polling (default: {_DEFAULT_POLL_MAX_WORKERS}). "
            f"Each worker polls one pipeline; total wait collapses from "
            f"Sum-per-pipeline to max-of-pipelines."
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

    # Watermark guard — skip if no upstream table has changed.
    # This module was historically Spark-free (pure REST API client).
    # The guard requires a Spark session for DESCRIBE HISTORY; if Spark is
    # unavailable (e.g., manual CLI invocation outside Databricks), fail open.
    spark: SparkSession | None = None
    try:
        spark = get_spark_session()
        fr = timed_check(skip_guard, spark, args.catalog, args.schema)
        if fr.count == 0:
            print("Watermark skip: no upstream changes since last refresh")
            return
    except Exception:
        print("Spark unavailable for watermark check — proceeding with refresh", file=sys.stderr)
        spark = None

    # Build lookup: table_name -> config
    table_config_map: dict[str, SyncedTableConfig] = {c.name: c for c in SYNCED_TABLES}
    all_table_names = list(table_config_map.keys())

    if args.tables:
        selected = [t.strip() for t in args.tables.split(",") if t.strip()]
        for t in selected:
            if t not in table_config_map:
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
            config = table_config_map[table]
            schema = config.schema_override or args.schema
            pipeline_id = _get_pipeline_id(table, catalog=args.catalog, schema=schema)

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
        print(
            f"\nWaiting for {len(triggered)} pipelines "
            f"(polling every {POLL_INTERVAL_S}s, up to {args.max_workers} in parallel)..."
        )
        poll_results = _poll_pipelines_parallel(triggered, headers, max_workers=args.max_workers)
        errors += sum(1 for state in poll_results.values() if state != "IDLE")

    # Record watermarks after successful refresh (only if Spark was available)
    if errors == 0 and spark is not None:
        upstream = _derive_upstream_tables(args.catalog, args.schema)
        record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)

    print(f"\nSummary: {len(triggered)} triggered, {errors} errors")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
