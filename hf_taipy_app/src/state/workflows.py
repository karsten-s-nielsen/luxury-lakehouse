"""AI/ML Workflows page state — thin orchestrator.

Imports DAG rendering from workflows_dag, stat/card logic from workflows_stats,
and SQL queries from queries.workflows. Owns state variable declarations,
the main wf_refresh() entry point, filter callbacks, auto-refresh timer,
and register_page_refresher registration.

State prefix: wf_
Route key: AI-ML-Workflows
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

import pandas as pd
from cache import ttl_cache
from queries.workflows import fetch_cold_costs, fetch_warm_costs

from state.shared import register_page_refresher, register_page_teardown
from state.workflows_dag import (
    DAG_MAX_HEIGHT_PX,
    TYPE_LABELS,
    RawHtml,
    build_badges_html,
    build_cost_html,
    build_dag_html,
    build_data_flow_html,
    build_deps_html,
    build_exec_html,
    build_idempotency_html,
    build_monitoring_html,
    build_references_html,
    build_source_html,
    wf_on_dag_click,
)
from state.workflows_stats import (
    WF_TABLE_COLS,
    HFCostData,
    _stat_detail_html,  # noqa: F401 — re-exported for test access
    build_table_data,
    build_task_key_to_wf_id,
    classify_freshness,
    classify_runtime,
    compute_stats,
    filter_card_ids,
    load_cards_from_yaml,
    wf_style_freshness,
    wf_style_runtime,
    wf_style_status,
    wf_style_type,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dashboard state
# ---------------------------------------------------------------------------
wf_selected_workflow: str | None = None  # None = dashboard, set = detail view

wf_dag_html: RawHtml = RawHtml("")
wf_dag_height: str = "700px"  # Dynamic height for DAG container — computed from node count
wf_cards_loaded: bool = False  # True after YAML cards loaded successfully
wf_no_cards_warning: str = ""  # Non-empty when YAML cards fail to load (warning_var target)

wf_total_workflows: str = "0"
wf_workflows_detail: RawHtml = RawHtml("")
wf_freshness_summary: str = "\u2014"
wf_freshness_detail: RawHtml = RawHtml("")
wf_total_cost_30d: str = "$0.00"
wf_cost_detail: RawHtml = RawHtml("")
wf_run_volume: str = "0"
wf_run_volume_detail: str = ""

wf_table_data: pd.DataFrame = pd.DataFrame(columns=pd.Index(WF_TABLE_COLS))

wf_type_filter: str | None = "All"
wf_type_lov: list[str] = ["All"]
wf_runtime_filter: str | None = "All"
wf_runtime_lov: list[str] = ["All"]
wf_freshness_filter: str | None = "All"
wf_freshness_lov: list[str] = ["All"]

# ---------------------------------------------------------------------------
# Detail state
# ---------------------------------------------------------------------------
wf_detail_title: str = ""
wf_detail_badges_html: RawHtml = RawHtml("")
wf_detail_meta: str = ""
wf_detail_overview: str = ""
wf_detail_data_flow_html: RawHtml = RawHtml("")
wf_detail_exec_html: RawHtml = RawHtml("")
wf_detail_monitoring_html: RawHtml = RawHtml("")
wf_detail_cost_html: RawHtml = RawHtml("")
wf_detail_references_html: RawHtml = RawHtml("")
wf_detail_deps_html: RawHtml = RawHtml("")
wf_detail_idempotency_html: RawHtml = RawHtml("")
wf_detail_source_html: RawHtml = RawHtml("")

# Admin (Phase 2 foundation)
wf_is_admin: bool = False

# ---------------------------------------------------------------------------
# Internal (NOT exported — not bound to UI)
# ---------------------------------------------------------------------------
_cards: dict[str, dict[str, Any]] = {}
_task_key_to_wf_id: dict[str, str] = {}  # entry_point -> workflow_id reverse lookup
_unfiltered_dag_html: RawHtml = RawHtml("")  # Cached full DAG (no filters applied)
_ws_client: Any = None  # Lazy WorkspaceClient singleton (avoids re-init on each cache miss)
_ws_client_lock = threading.Lock()  # Guards lazy init from concurrent timer ticks
_wf_card_ids: list[str] = []  # Parallel to wf_table_data rows — maps row index to card ID

_refresh_timer: threading.Timer | None = None
_REFRESH_INTERVAL_SECONDS = 120  # 2 minutes

__all__ = [
    # RawHtml wrapper (used by main.py content provider)
    "RawHtml",
    # Dashboard
    "wf_selected_workflow",
    "wf_dag_html",
    "wf_dag_height",
    "wf_cards_loaded",
    "wf_no_cards_warning",
    "wf_total_workflows",
    "wf_workflows_detail",
    "wf_freshness_summary",
    "wf_freshness_detail",
    "wf_total_cost_30d",
    "wf_cost_detail",
    "wf_run_volume",
    "wf_run_volume_detail",
    "wf_table_data",
    "wf_type_filter",
    "wf_type_lov",
    "wf_runtime_filter",
    "wf_runtime_lov",
    "wf_freshness_filter",
    "wf_freshness_lov",
    # Detail
    "wf_detail_title",
    "wf_detail_badges_html",
    "wf_detail_meta",
    "wf_detail_overview",
    "wf_detail_data_flow_html",
    "wf_detail_exec_html",
    "wf_detail_monitoring_html",
    "wf_detail_cost_html",
    "wf_detail_references_html",
    "wf_detail_deps_html",
    "wf_detail_idempotency_html",
    "wf_detail_source_html",
    # Admin
    "wf_is_admin",
    # Callbacks
    "wf_on_dag_click",
    "wf_on_back_click",
    "wf_on_type_filter",
    "wf_on_runtime_filter",
    "wf_on_freshness_filter",
    "wf_on_table_action",
    "wf_refresh",
    # Table cell style callbacks
    "wf_style_type",
    "wf_style_runtime",
    "wf_style_freshness",
    "wf_style_status",
    # Auto-refresh callback (invoked by timer)
    "_wf_auto_refresh_tick",
]


# ---------------------------------------------------------------------------
# Jobs API — last run + duration + freshness
# ---------------------------------------------------------------------------


@ttl_cache(ttl=120)
def _fetch_job_runs() -> dict[str, dict[str, Any]]:
    """Fetch recent job runs from Databricks Jobs API.

    Returns dict keyed by workflow_id with latest run info:
    {workflow_id: {"last_run": datetime, "duration_seconds": int, "state": str}}
    """
    global _ws_client
    try:
        with _ws_client_lock:
            if _ws_client is None:
                from databricks.sdk import WorkspaceClient

                _ws_client = WorkspaceClient()
        ws = _ws_client
        # Find runs from the last 30 days.
        # Skip DISABLED/EXCLUDED tasks — they inherit the job's end_time but
        # never executed, so their timestamps clobber real execution data from
        # earlier runs.  See 2026-03-27 investigation: a job run with 18/19
        # tasks DISABLED overwrote all real SUCCESS entries.
        skip_states = {"DISABLED", "EXCLUDED"}
        runs: dict[str, dict[str, Any]] = {}
        for run in ws.jobs.list_runs(
            expand_tasks=True,
            start_time_from=int(pd.Timestamp.now(tz="UTC").timestamp() * 1000 - 30 * 86_400_000),
        ):
            if not run.tasks:
                continue
            for task in run.tasks:
                key = task.task_key or ""
                if not key:
                    continue
                result_state = task.state.result_state.value if task.state and task.state.result_state else "UNKNOWN"
                if result_state in skip_states:
                    continue
                end_time = task.end_time or 0
                if key not in runs or end_time > runs[key].get("end_time_ms", 0):
                    duration = (task.execution_duration or 0) // 1000  # ms -> seconds
                    runs[key] = {
                        "last_run": (pd.Timestamp(end_time, unit="ms", tz="UTC") if end_time else None),
                        "duration_seconds": duration,
                        "state": result_state,
                        "end_time_ms": end_time,
                    }
        logger.info("Fetched run data for %d task keys from Jobs API", len(runs))
        # Re-key from task_key to workflow_id using the reverse lookup
        return {_task_key_to_wf_id.get(k, k): v for k, v in runs.items()}
    except Exception:
        logger.warning("Jobs API query failed \u2014 run data unavailable", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# HF cost history (reads _workflow_cost.json + _cost_history/ from HF repos)
# ---------------------------------------------------------------------------


def _discover_hf_repos_from_cards() -> list[tuple[str, str, str]]:
    """Parse workflow cards to find HF Jobs repos for live status checking."""
    repos: list[tuple[str, str, str]] = []
    for card in _cards.values():
        execution = card.get("execution") or {}
        has_hf_jobs = False
        for phase in ("training", "inference"):
            phase_cfg = execution.get(phase) or {}
            rt = (phase_cfg.get("runtime") or "").lower().replace("_", "-")
            if rt == "hf-jobs":
                has_hf_jobs = True
                break
        if not has_hf_jobs:
            continue
        outputs = card.get("outputs") or {}
        for ds in outputs.get("datasets") or []:
            if ds.get("destination") == "huggingface" and ds.get("id"):
                repos.append((ds["id"], "dataset", card.get("id", "")))
        for model in outputs.get("models") or []:
            if model.get("destination") == "huggingface" and model.get("id"):
                repos.append((model["id"], "model", card.get("id", "")))
    return repos


def _fetch_hf_cost_history_impl(
    api: Any,
    repos: list[tuple[str, str, str]],
) -> dict[str, HFCostData]:
    """Read _workflow_cost.json + _cost_history/ from HF Hub repos.

    For each repo:
    - _workflow_cost.json: detect RUNNING state
    - _cost_history/*.json: completed/failed run records (last 30 days)

    Returns dict keyed by workflow_id. Separated from the cached wrapper
    for testability.
    """
    from datetime import datetime, timezone

    cutoff = datetime.now(tz=timezone.utc).timestamp() - 30 * 86_400
    result: dict[str, HFCostData] = {}

    for repo_id, repo_type, workflow_id in repos:
        if not workflow_id:
            continue
        cost_data = result.setdefault(workflow_id, HFCostData())

        # 1. Check live status from _workflow_cost.json
        try:
            local_path = api.hf_hub_download(
                repo_id=repo_id,
                filename="_workflow_cost.json",
                repo_type=repo_type,
            )
            with open(local_path) as f:
                live = json.load(f)
            if isinstance(live, dict) and live.get("state") == "RUNNING":
                cost_data.is_running = True
        except Exception:
            logger.debug("Live status check failed for %s/%s", repo_type, repo_id, exc_info=True)

        # 2. List _cost_history/ files for completed runs
        try:
            files = api.list_repo_files(repo_id=repo_id, repo_type=repo_type)
            history_files = [f for f in files if f.startswith("_cost_history/") and f.endswith(".json")]
            for hf in history_files:
                try:
                    local = api.hf_hub_download(repo_id=repo_id, filename=hf, repo_type=repo_type)
                    with open(local) as f:
                        run_data = json.load(f)
                    if not isinstance(run_data, dict):
                        continue
                    # Filter to last 30 days
                    ended = run_data.get("ended_at", "")
                    if ended:
                        try:
                            end_ts = datetime.fromisoformat(ended).timestamp()
                            if end_ts < cutoff:
                                continue
                        except (ValueError, TypeError):
                            pass
                    cost_data.runs.append(run_data)
                except Exception:
                    logger.debug("Failed to read history file %s from %s", hf, repo_id, exc_info=True)
        except Exception:
            logger.debug("Failed to list _cost_history/ in %s/%s", repo_type, repo_id, exc_info=True)

        # 2b. Legacy fallback: use _workflow_cost.json if no history files
        if not cost_data.runs and not cost_data.is_running:
            try:
                local_path = api.hf_hub_download(
                    repo_id=repo_id,
                    filename="_workflow_cost.json",
                    repo_type=repo_type,
                )
                with open(local_path) as f:
                    legacy = json.load(f)
                if isinstance(legacy, dict) and legacy.get("state") in ("COMPLETED", "FAILED", "SKIPPED"):
                    cost_data.runs.append(legacy)
            except Exception:
                logger.debug("Legacy _workflow_cost.json fallback failed for %s", repo_id, exc_info=True)

        # 3. Determine latest completed/failed run
        if cost_data.runs:
            # Sort by ended_at descending, pick first
            def _run_sort_key(r: dict[str, Any]) -> str:
                return r.get("ended_at", "") or ""

            sorted_runs = sorted(cost_data.runs, key=_run_sort_key, reverse=True)
            cost_data.latest_run = sorted_runs[0]

    return result


@ttl_cache(ttl=60)
def _fetch_hf_cost_history() -> dict[str, HFCostData]:
    """Fetch HF cost history from HF Hub repos. 60s TTL."""
    try:
        from huggingface_hub import HfApi

        api = HfApi()
        repos = _discover_hf_repos_from_cards()
        return _fetch_hf_cost_history_impl(api, repos)
    except Exception:
        logger.debug("HF cost history fetch failed", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def wf_on_table_action(state: Any, var_name: str, payload: dict[str, Any]) -> None:
    """Table row clicked — switch to detail view.

    Uses a hidden _wf_card_ids list (parallel to table rows) to map
    row index to card ID, avoiding a visible _card_id column in the table.
    """
    idx = payload.get("index", 0) if isinstance(payload, dict) else 0
    if 0 <= idx < len(_wf_card_ids):
        card_id = _wf_card_ids[idx]
        if card_id in _cards:
            _show_detail(state, card_id)


def wf_on_back_click(state: Any, id: str, payload: dict[str, Any]) -> None:
    """Back to dashboard. Signature matches Taipy on_action (state, id, payload)."""
    state.wf_selected_workflow = None


def wf_on_type_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Type filter changed — rebuild table."""
    _refresh_table(state)


def wf_on_runtime_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Runtime filter changed — rebuild table."""
    _refresh_table(state)


def wf_on_freshness_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Freshness filter changed — rebuild table."""
    _refresh_table(state)


# ---------------------------------------------------------------------------
# Detail population
# ---------------------------------------------------------------------------


def _show_detail(state: Any, workflow_id: str) -> None:
    """Populate all detail state variables for a workflow."""
    card = _cards.get(workflow_id, {})
    cold = fetch_cold_costs()
    warm = fetch_warm_costs()

    state.wf_selected_workflow = workflow_id
    state.wf_detail_title = card.get("name", workflow_id)
    state.wf_detail_badges_html = build_badges_html(card)
    _dash = "\u2014"
    domain = card.get("domain", _dash)
    owners = ", ".join(card.get("owners", []))
    version = card.get("version", "?")
    state.wf_detail_meta = f"Domain: {domain} | Owner: {owners} | v{version}"
    state.wf_detail_overview = card.get("body", "").strip()
    state.wf_detail_data_flow_html = build_data_flow_html(card)
    state.wf_detail_exec_html = build_exec_html(card)
    state.wf_detail_monitoring_html = build_monitoring_html(card)
    state.wf_detail_cost_html = build_cost_html(card, cold, warm)
    state.wf_detail_references_html = build_references_html(card)
    state.wf_detail_deps_html = build_deps_html(card, _cards)
    state.wf_detail_idempotency_html = build_idempotency_html(card)
    state.wf_detail_source_html = build_source_html(card)


# ---------------------------------------------------------------------------
# Table refresh (used by filter callbacks)
# ---------------------------------------------------------------------------


def _refresh_table(state: Any) -> None:
    """Rebuild dashboard table AND DAG with current filters."""
    global _wf_card_ids

    cold = fetch_cold_costs()
    jobs = _fetch_job_runs()
    hf_costs = _fetch_hf_cost_history()

    # Filter cards
    matched_ids = filter_card_ids(
        _cards,
        jobs,
        state.wf_type_filter,
        state.wf_runtime_filter,
        state.wf_freshness_filter,
        hf_costs=hf_costs,
    )

    # Check if any filter is active
    all_filters_default = all(
        f in (None, "All") for f in (state.wf_type_filter, state.wf_runtime_filter, state.wf_freshness_filter)
    )

    if all_filters_default:
        # No filters — reuse cached full DAG (built once in wf_refresh)
        state.wf_dag_html = _unfiltered_dag_html
        state.wf_dag_height = f"{max(200, min(DAG_MAX_HEIGHT_PX, len(_cards) * 50 + 80))}px"
    else:
        # Build filtered DAG: matched cards + their immediate neighbors for context
        dag_cards: dict[str, dict[str, Any]] = {}
        for card_id in matched_ids:
            dag_cards[card_id] = _cards[card_id]
            # Add upstream dependencies
            for dep_id in _cards[card_id].get("depends_on", []):
                if dep_id in _cards:
                    dag_cards[dep_id] = _cards[dep_id]
            # Add downstream dependents
            for other_id, other_card in _cards.items():
                if card_id in other_card.get("depends_on", []):
                    dag_cards[other_id] = other_card
        if dag_cards:
            state.wf_dag_html = build_dag_html(dag_cards, highlight_ids=matched_ids)
            state.wf_dag_height = f"{max(200, min(DAG_MAX_HEIGHT_PX, len(dag_cards) * 50 + 80))}px"
        else:
            state.wf_dag_html = RawHtml("")
            state.wf_dag_height = "0px"

    table_df, card_ids = build_table_data(
        _cards,
        cold,
        jobs,
        state.wf_type_filter,
        state.wf_runtime_filter,
        state.wf_freshness_filter,
        hf_costs=hf_costs,
    )
    state.wf_table_data = table_df
    _wf_card_ids = card_ids

    # Recompute stats for the filtered subset
    warm = fetch_warm_costs()
    compute_stats(
        state,
        _cards,
        cold,
        warm,
        jobs,
        visible_card_ids=matched_ids if not all_filters_default else None,
        hf_costs=hf_costs,
    )


# ---------------------------------------------------------------------------
# Auto-refresh timer
# ---------------------------------------------------------------------------


def _start_auto_refresh(state: Any) -> None:
    """Start the 2-minute auto-refresh timer for the Workflows page."""
    global _refresh_timer
    _stop_auto_refresh()

    def _tick() -> None:
        global _refresh_timer
        try:
            state.invoke_callback("_wf_auto_refresh_tick", [])
        except Exception:
            logger.debug("Auto-refresh tick failed", exc_info=True)
        # Schedule next tick
        _refresh_timer = threading.Timer(_REFRESH_INTERVAL_SECONDS, _tick)
        _refresh_timer.daemon = True
        _refresh_timer.start()

    _refresh_timer = threading.Timer(_REFRESH_INTERVAL_SECONDS, _tick)
    _refresh_timer.daemon = True
    _refresh_timer.start()
    logger.info("Auto-refresh started (%ds interval)", _REFRESH_INTERVAL_SECONDS)


def _stop_auto_refresh() -> None:
    """Cancel the auto-refresh timer."""
    global _refresh_timer
    if _refresh_timer is not None:
        _refresh_timer.cancel()
        _refresh_timer = None
        logger.info("Auto-refresh stopped")


def _wf_auto_refresh_tick(state: Any) -> None:
    """Callback invoked by the timer — re-fetches data and updates state."""
    global _wf_card_ids

    logger.debug("Auto-refresh tick")
    cold = fetch_cold_costs()
    warm = fetch_warm_costs()
    jobs = _fetch_job_runs()
    hf_costs = _fetch_hf_cost_history()

    table_df, card_ids = build_table_data(
        _cards,
        cold,
        jobs,
        state.wf_type_filter,
        state.wf_runtime_filter,
        state.wf_freshness_filter,
        hf_costs=hf_costs,
    )
    state.wf_table_data = table_df
    _wf_card_ids = card_ids
    compute_stats(state, _cards, cold, warm, jobs, hf_costs=hf_costs)


# ---------------------------------------------------------------------------
# Main refresh (page entry point)
# ---------------------------------------------------------------------------


def wf_refresh(state: Any) -> None:
    """Page entry point — loads cards, queries costs, builds dashboard."""
    global _cards, _unfiltered_dag_html, _task_key_to_wf_id, _wf_card_ids

    if not _cards:
        _cards = load_cards_from_yaml()
        _task_key_to_wf_id = build_task_key_to_wf_id(_cards)
    if not _cards:
        logger.warning("No workflow cards loaded")
        state.wf_no_cards_warning = "No workflow cards loaded. Check that the workflow-cards/ directory is available."
        return
    state.wf_no_cards_warning = ""  # Clear on successful load
    state.wf_cards_loaded = True

    # Build filter LOVs from card metadata
    types = sorted({c.get("type", "") for c in _cards.values()})
    state.wf_type_lov = ["All"] + [TYPE_LABELS.get(tp, tp) for tp in types]
    state.wf_type_filter = "All"

    # Build runtime LOV from card execution config
    runtime_values: set[str] = set()
    for c in _cards.values():
        exec_cfg = c.get("execution") or {}
        rt_str = classify_runtime(exec_cfg)
        if rt_str != "\u2014":
            runtime_values.add(rt_str)
    state.wf_runtime_lov = ["All"] + sorted(runtime_values)
    state.wf_runtime_filter = "All"

    # Build DAG (cached for filter resets — see _refresh_table)
    _unfiltered_dag_html = build_dag_html(_cards)
    state.wf_dag_html = _unfiltered_dag_html

    # Query costs + job runs (job_runs already re-keyed to workflow_id)
    cold = fetch_cold_costs()
    warm = fetch_warm_costs()
    jobs = _fetch_job_runs()

    # Build freshness LOV from computed freshness values (jobs keyed by workflow_id)
    freshness_values: set[str] = set()
    for card_id, c in _cards.items():
        sla_hours = (c.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours is None:
            continue
        run = jobs.get(card_id, {})
        last_run = run.get("last_run")
        if last_run is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run).total_seconds() / 3600
            freshness_values.add(classify_freshness(age_hours, sla_hours))
    state.wf_freshness_lov = ["All"] + sorted(freshness_values)
    state.wf_freshness_filter = "All"

    # Fetch HF cost history
    hf_costs = _fetch_hf_cost_history()

    # Build table
    table_df, card_ids = build_table_data(_cards, cold, jobs, "All", "All", "All", hf_costs=hf_costs)
    state.wf_table_data = table_df
    _wf_card_ids = card_ids

    # Stats (uses jobs for freshness, cold for cost, hf_costs for HF data)
    compute_stats(state, _cards, cold, warm, jobs, hf_costs=hf_costs)

    # Clear detail state (dashboard mode)
    state.wf_selected_workflow = None

    # Start auto-refresh timer (2-minute interval)
    _start_auto_refresh(state)

    logger.info("Workflows page loaded: %d cards, %d cost rows", len(_cards), len(cold))


register_page_refresher("AI-ML-Workflows", wf_refresh, is_dashboard=True)
register_page_teardown("AI-ML-Workflows", _stop_auto_refresh)
