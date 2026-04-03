"""AI/ML Workflows — stat computation, card loading, table building.

Card loading (WorkflowCard.from_yaml_file), cost computation, metric formatting,
StatCard data preparation, table cell style callbacks, and classification helpers.
Detail section HTML builders live in workflows_dag (rendering module).

State prefix: wf_
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from html import escape as html_escape
from pathlib import Path
from typing import Any

import pandas as pd

from state.workflows_dag import (
    COLOR_HEX,
    FRESHNESS_HEX,
    RUNTIME_HEX,
    TYPE_COLORS,
    TYPE_LABELS,
    RawHtml,
)
from workflows.card import WorkflowCard

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HF cost history data model
# ---------------------------------------------------------------------------


@dataclass
class HFCostData:
    """Per-workflow HF cost history from _cost_history/ files.

    Aggregates completed/failed run records and tracks live RUNNING state
    from _workflow_cost.json.
    """

    runs: list[dict[str, Any]] = field(default_factory=list)
    is_running: bool = False
    latest_run: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Stat detail HTML helper
# ---------------------------------------------------------------------------

# Stat card detail: base CSS for content-provider iframes (dark theme, no margin).
# Content provider iframes are sandboxed documents — they do NOT inherit the app theme.
# Keep font-family and color in sync with the app's dark theme if it changes.
_STAT_DETAIL_STYLE = (
    "margin:0;padding:0;background:transparent;"
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
    "font-size:0.8rem;color:rgba(255,255,255,0.6);line-height:1.4;"
)


def _stat_detail_html(inner: str) -> RawHtml:
    """Wrap colored HTML in a dark-themed body for stat card content provider."""
    if not inner:
        return RawHtml("")
    return RawHtml(f'<body style="{_STAT_DETAIL_STYLE}">{inner}</body>')


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def classify_freshness(age_hours: float, sla_hours: float) -> str:
    """Classify workflow freshness against its SLA threshold.

    Three tiers: OK (within 75% of SLA), Warning (within SLA), Stale (beyond SLA).
    Used by table rendering, filter matching, and stats computation.
    """
    if age_hours <= sla_hours * 0.75:
        return "OK"
    if age_hours <= sla_hours:
        return "Warning"
    return "Stale"


def classify_runtime(exec_cfg: dict[str, Any]) -> str:
    """Classify workflow runtime from execution config phases.

    Returns human-readable label: 'DB', 'HF', 'DB + HF', or em-dash.
    """
    rts: list[str] = []
    for phase in ("training", "inference", "export", "import", "ingestion", "sync"):
        rt = ((exec_cfg.get(phase) or {}).get("runtime") or "").lower()
        if "hf" in rt:
            if "HF" not in rts:
                rts.append("HF")
        elif "databricks" in rt:
            if "DB" not in rts:
                rts.append("DB")
    return " + ".join(rts) if rts else "\u2014"


# ---------------------------------------------------------------------------
# Table cell style callbacks (resolved by name via Taipy style[column])
# ---------------------------------------------------------------------------

# Type label -> hex color (derived from TYPE_COLORS + COLOR_HEX + TYPE_LABELS)
_TYPE_LABEL_COLORS: dict[str, str] = {TYPE_LABELS[k]: COLOR_HEX[v] for k, v in TYPE_COLORS.items() if k in TYPE_LABELS}

# Type label -> CSS class (matches DAG node colors)
_TYPE_CELL_STYLES: dict[str, str] = {
    "Train+Infer": "ll-cell-type-train",
    "Training": "ll-cell-type-train",
    "Inference": "ll-cell-type-train",
    "Grid Compute": "ll-cell-type-grid",
    "Heuristic": "ll-cell-type-heuristic",
    "Ingestion": "ll-cell-type-ingestion",
    "Data Movement": "ll-cell-type-data-movement",
    "Validation": "ll-cell-type-validation",
}


def wf_style_type(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Type column cells."""
    return _TYPE_CELL_STYLES.get(str(value), "")


def wf_style_runtime(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Runtime column cells."""
    s = str(value)
    if "+" in s:
        return "ll-cell-rt-both"
    if s == "DB":
        return "ll-cell-rt-db"
    if s == "HF":
        return "ll-cell-rt-hf"
    return ""


def wf_style_freshness(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Freshness column cells."""
    s = str(value)
    if s == "OK":
        return "ll-cell-fresh-ok"
    if s == "Warning":
        return "ll-cell-fresh-warning"
    if s == "Stale":
        return "ll-cell-fresh-stale"
    return ""


_STATUS_CLASSES: dict[str, str] = {
    "RUNNING": "ll-cell-status-running",
    "COMPLETED": "ll-cell-status-completed",
    "FAILED": "ll-cell-status-failed",
    "SKIPPED": "ll-cell-status-skipped",
}


def wf_style_status(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Status column cells."""
    return _STATUS_CLASSES.get(str(value), "")


# ---------------------------------------------------------------------------
# Card loading from YAML
# ---------------------------------------------------------------------------


def load_cards_from_yaml() -> dict[str, dict[str, Any]]:
    """Load workflow card YAML files from workflow-cards/ directory.

    Searches relative to app root (works both locally and on HF Spaces).
    Returns dict keyed by card 'id' field.
    """
    # Try multiple paths: HF Space root, local dev
    candidates = [
        Path("workflow-cards"),
        Path(__file__).parent.parent.parent / "workflow-cards",  # hf_taipy_app/../workflow-cards
        Path(__file__).parent.parent.parent.parent / "workflow-cards",  # repo root
    ]
    cards_dir: Path | None = None
    for p in candidates:
        if p.is_dir() and list(p.glob("*.yaml")):
            cards_dir = p
            break

    if cards_dir is None:
        logger.warning("No workflow-cards directory found")
        return {}

    cards: dict[str, dict[str, Any]] = {}
    for yaml_path in sorted(cards_dir.glob("*.yaml")):
        try:
            card = WorkflowCard.from_yaml_file(yaml_path)
            data: dict[str, Any] = card.model_dump()
            data["_file"] = yaml_path.name
            if card.id:
                cards[card.id] = data
        except Exception:
            logger.exception("Failed to parse %s", yaml_path.name)

    logger.info("Loaded %d workflow cards", len(cards))
    return cards


def build_task_key_to_wf_id(cards: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Build entry_point -> workflow_id reverse lookup from loaded cards.

    Looks at both training and inference phases for entry_point values.
    """
    mapping: dict[str, str] = {}
    for card_id, card in cards.items():
        exec_cfg = card.get("execution") or {}
        for phase in ("training", "inference"):
            ep = (exec_cfg.get(phase) or {}).get("entry_point", "")
            if ep:
                mapping[ep] = card_id
    return mapping


# ---------------------------------------------------------------------------
# Table data builder
# ---------------------------------------------------------------------------

WF_TABLE_COLS = [
    "Name",
    "Type",
    "Runtime",
    "Trigger",
    "Status",
    "Last Run",
    "Last Duration",
    "Cost (30d)",
    "Avg/Run",
    "Freshness",
]


def build_table_data(
    cards: dict[str, dict[str, Any]],
    cold_costs: pd.DataFrame,
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    runtime_filter: str | None = "All",
    freshness_filter: str | None = "All",
    hf_costs: dict[str, HFCostData] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Build dashboard table DataFrame from cards + cost data.

    All data sources are keyed by workflow_id for unified lookup:
    - cold_costs: DB cold-tier costs (workflow_id column)
    - job_runs: Databricks Jobs API (re-keyed to workflow_id)
    - hf_costs: HF Hub cost history (keyed by workflow_id)

    Returns (DataFrame, card_ids) where card_ids is parallel to rows
    for mapping row index to card ID.
    """
    card_ids: list[str] = []
    hf = hf_costs or {}

    # Build cost lookups keyed by workflow_id
    cold_cost_lookup: dict[str, float] = {}
    cold_run_count_lookup: dict[str, int] = {}
    if not cold_costs.empty and "workflow_id" in cold_costs.columns:
        cold_cost_lookup = (
            cold_costs.set_index("workflow_id")["total_cost_usd"].apply(lambda x: float(x or 0)).to_dict()
        )
        cold_run_count_lookup = cold_costs.set_index("workflow_id")["run_count"].apply(lambda x: int(x or 0)).to_dict()

    rows = []
    for card_id, card in cards.items():
        wf_type = card.get("type", "")

        # Apply type filter
        if type_filter and type_filter != "All" and TYPE_LABELS.get(wf_type, wf_type) != type_filter:
            continue

        # Determine runtime(s) and trigger.
        exec_cfg = card.get("execution") or {}
        runtime_str = classify_runtime(exec_cfg)
        trigger_str = "\u2014"
        for phase in ("training", "inference", "export", "import", "ingestion", "sync"):
            trigger_val = (exec_cfg.get(phase) or {}).get("trigger")
            if trigger_val:
                trigger_str = trigger_val.capitalize()
                break

        # Apply runtime filter
        if runtime_filter and runtime_filter != "All" and runtime_str != runtime_filter:
            continue

        # --- Cost (30d): DB cold-tier + HF history, both keyed by workflow_id ---
        db_cost = cold_cost_lookup.get(card_id, 0.0)
        db_runs = cold_run_count_lookup.get(card_id, 0)
        hf_data = hf.get(card_id)
        hf_cost = sum(float(r.get("estimated_cost_usd") or 0) for r in (hf_data.runs if hf_data else []))
        hf_runs = len(hf_data.runs) if hf_data else 0
        total_cost = db_cost + hf_cost
        total_runs = db_runs + hf_runs

        # --- Last Run + Duration: pick most recent across Jobs API and HF history ---
        job_run = job_runs.get(card_id, {})
        jobs_last_run_ts = job_run.get("last_run")
        jobs_duration_secs = job_run.get("duration_seconds", 0)

        hf_last_run_ts: pd.Timestamp | None = None
        hf_duration_secs = 0
        if hf_data and hf_data.latest_run:
            ended = hf_data.latest_run.get("ended_at", "")
            if ended:
                try:
                    ts = pd.Timestamp(ended)
                    if isinstance(ts, pd.Timestamp):
                        hf_last_run_ts = ts
                except (ValueError, TypeError):
                    pass
            hf_duration_secs = int(hf_data.latest_run.get("duration_seconds") or 0)

        # Pick whichever is more recent
        last_run_ts, duration_secs = _pick_latest_run(
            jobs_last_run_ts, jobs_duration_secs, hf_last_run_ts, hf_duration_secs
        )

        last_run_str = "\u2014"
        duration_str = "\u2014"
        if last_run_ts is not None:
            last_run_str = last_run_ts.strftime("%Y-%m-%d %H:%M")
            if duration_secs > 0:
                mins, secs = divmod(duration_secs, 60)
                duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        # --- Cost display ---
        if total_cost > 0:
            cost_val = f"${total_cost:7.2f}"
            avg_run_val = f"${total_cost / total_runs:7.2f}" if total_runs > 0 else "\u2014"
        elif last_run_ts is not None:
            cost_val = "  $0.00"
            avg_run_val = "  $0.00"
        else:
            cost_val = "\u2014"
            avg_run_val = "\u2014"

        # --- Freshness ---
        freshness_str = "\u2014"
        sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours and last_run_ts is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
            freshness_str = classify_freshness(age_hours, sla_hours)

        if freshness_filter and freshness_filter != "All" and freshness_str != freshness_filter:
            continue

        # --- Status ---
        status_str = _resolve_status(hf_data, job_run, jobs_last_run_ts, hf_last_run_ts)

        rows.append(
            {
                "Name": card.get("name", card_id),
                "Type": TYPE_LABELS.get(wf_type, wf_type),
                "Runtime": runtime_str,
                "Trigger": trigger_str,
                "Status": status_str,
                "Last Run": last_run_str,
                "Last Duration": duration_str,
                "Cost (30d)": cost_val,
                "Avg/Run": avg_run_val,
                "Freshness": freshness_str,
            }
        )
        card_ids.append(card_id)

    if not rows:
        return pd.DataFrame(columns=pd.Index(WF_TABLE_COLS)), card_ids
    return pd.DataFrame(rows), card_ids


def _pick_latest_run(
    jobs_ts: pd.Timestamp | None,
    jobs_dur: int,
    hf_ts: pd.Timestamp | None,
    hf_dur: int,
) -> tuple[pd.Timestamp | None, int]:
    """Pick the most recent run timestamp and its duration across two sources."""
    if jobs_ts is not None and hf_ts is not None:
        return (jobs_ts, jobs_dur) if jobs_ts >= hf_ts else (hf_ts, hf_dur)
    if jobs_ts is not None:
        return jobs_ts, jobs_dur
    if hf_ts is not None:
        return hf_ts, hf_dur
    return None, 0


def _resolve_status(
    hf_data: HFCostData | None,
    job_run: dict[str, Any],
    jobs_last_run_ts: pd.Timestamp | None,
    hf_last_run_ts: pd.Timestamp | None,
) -> str:
    """Resolve workflow status from HF + Databricks sources."""
    hf_is_running = hf_data.is_running if hf_data else False
    jobs_is_running = job_run.get("state") in ("RUNNING", "PENDING")

    if hf_is_running or jobs_is_running:
        return "RUNNING"
    if job_run or (hf_data and hf_data.latest_run):
        if jobs_last_run_ts is not None and (hf_last_run_ts is None or jobs_last_run_ts >= hf_last_run_ts):
            run_state = job_run.get("state", "")
        elif hf_data and hf_data.latest_run:
            run_state = hf_data.latest_run.get("state", "")
        else:
            run_state = ""

        if run_state in ("SUCCESS", "COMPLETED"):
            return "COMPLETED"
        if run_state in ("FAILED", "ERROR", "TIMEDOUT", "CANCELED"):
            return "FAILED"
        if run_state in ("SKIPPED", "DISABLED", "EXCLUDED"):
            return "SKIPPED"
    return "\u2014"


# ---------------------------------------------------------------------------
# Filter card IDs (for DAG highlight)
# ---------------------------------------------------------------------------


def filter_card_ids(
    cards: dict[str, dict[str, Any]],
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    runtime_filter: str | None,
    freshness_filter: str | None,
    hf_costs: dict[str, HFCostData] | None = None,
) -> set[str]:
    """Return card IDs that match all active filters.

    job_runs is keyed by workflow_id (card_id).
    hf_costs provides HF Jobs last-run data for freshness on HF-only workflows.
    """
    matched: set[str] = set()
    for card_id, card in cards.items():
        wf_type = card.get("type", "")

        if type_filter and type_filter != "All" and TYPE_LABELS.get(wf_type, wf_type) != type_filter:
            continue

        # Runtime
        exec_cfg = card.get("execution") or {}
        runtime_str = classify_runtime(exec_cfg)
        if runtime_filter and runtime_filter != "All" and runtime_str != runtime_filter:
            continue

        # Freshness: pick most recent last_run across Jobs API and HF history
        if freshness_filter and freshness_filter != "All":
            sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
            job_run = job_runs.get(card_id, {})
            db_last_run = job_run.get("last_run")

            hf_data = (hf_costs or {}).get(card_id)
            hf_latest = hf_data.latest_run if hf_data else None
            hf_last_run = pd.Timestamp(hf_latest["ended_at"]) if hf_latest and hf_latest.get("ended_at") else None

            last_run_ts = db_last_run
            if hf_last_run and (last_run_ts is None or hf_last_run > last_run_ts):
                last_run_ts = hf_last_run

            freshness = "\u2014"
            if sla_hours and last_run_ts is not None:
                age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
                freshness = classify_freshness(age_hours, sla_hours)
            if freshness != freshness_filter:
                continue

        matched.add(card_id)
    return matched


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------


def compute_stats(
    state: Any,
    cards: dict[str, dict[str, Any]],
    cold: pd.DataFrame,
    warm: pd.DataFrame,
    jobs: dict[str, dict[str, Any]],
    visible_card_ids: set[str] | None = None,
    hf_costs: dict[str, HFCostData] | None = None,
) -> None:
    """Compute stats bar metrics.

    When visible_card_ids is set, stats reflect only the filtered subset.
    All data sources keyed by workflow_id.
    """
    hf = hf_costs or {}
    cards_subset = {k: v for k, v in cards.items() if k in visible_card_ids} if visible_card_ids is not None else cards
    state.wf_total_workflows = str(len(cards_subset))

    # Workflow type breakdown for detail line
    type_counts: dict[str, int] = {}
    for card in cards_subset.values():
        wf_type: str = card.get("type") or ""
        label: str = TYPE_LABELS.get(wf_type, wf_type)
        type_counts[label] = type_counts.get(label, 0) + 1
    sorted_types = sorted(type_counts.items(), key=lambda x: (-x[1], x[0]))
    colored_parts = [
        f'<span style="color:{_TYPE_LABEL_COLORS.get(t, "#8b949e")}">{n} {html_escape(t)}</span>'
        for t, n in sorted_types
    ]
    state.wf_workflows_detail = _stat_detail_html(", ".join(colored_parts))

    # Total 30d cost — scoped to visible workflow card IDs.
    card_ids_subset = set(cards_subset.keys())
    if not cold.empty and "workflow_id" in cold.columns:
        cost_df = cold[cold["workflow_id"].isin(list(card_ids_subset))]
    else:
        cost_df = pd.DataFrame()

    dbx_cost = float(cost_df["total_cost_usd"].sum()) if not cost_df.empty else 0.0

    # HF Jobs costs from cost history (actual), warm tier (estimated), or YAML (projected)
    hf_cost, hf_tier = _compute_hf_cost(cards_subset, card_ids_subset, hf, warm)

    total = dbx_cost + hf_cost
    state.wf_total_cost_30d = f"${total:.2f}"

    # Cost detail: breakdown by runtime with colored labels
    cost_parts: list[str] = []
    if dbx_cost > 0:
        cost_parts.append(f'${dbx_cost:.2f} <span style="color:{RUNTIME_HEX["db"]}">DB</span> (actual)')
    if hf_cost > 0:
        cost_parts.append(f'${hf_cost:.2f} <span style="color:{RUNTIME_HEX["hf"]}">HF</span> ({hf_tier})')
    state.wf_cost_detail = _stat_detail_html(" + ".join(cost_parts))

    # Freshness summary
    _compute_freshness_stats(state, cards_subset, jobs, hf)

    # Run volume: DB cold runs + HF history runs
    db_runs = int(cost_df["run_count"].sum()) if not cost_df.empty and "run_count" in cost_df.columns else 0
    hf_runs = sum(len(hf[cid].runs) for cid in card_ids_subset if cid in hf)
    num_runs = db_runs + hf_runs
    state.wf_run_volume = str(num_runs)

    # Count currently running jobs (both runtimes)
    running_db = sum(1 for r in jobs.values() if r.get("state") in ("RUNNING", "PENDING"))
    running_hf = sum(1 for cid in card_ids_subset if cid in hf and hf[cid].is_running)
    total_running = running_db + running_hf

    if num_runs > 0:
        daily_rate = num_runs / 30
        avg_cost = total / num_runs
        detail_parts_vol = []
        if total_running > 0:
            detail_parts_vol.append(f"{total_running} running now")
        if daily_rate >= 1:
            detail_parts_vol.append(f"~{daily_rate:.0f}/day")
        else:
            detail_parts_vol.append(f"~{daily_rate:.1f}/day")
        detail_parts_vol.append(f"${avg_cost:.2f} avg/run")
        state.wf_run_volume_detail = " \u00b7 ".join(detail_parts_vol)
    else:
        if total_running > 0:
            state.wf_run_volume_detail = f"{total_running} running now"
        else:
            state.wf_run_volume_detail = ""


def _compute_hf_cost(
    cards_subset: dict[str, dict[str, Any]],
    card_ids_subset: set[str],
    hf: dict[str, HFCostData],
    warm: pd.DataFrame,
) -> tuple[float, str]:
    """Compute HF Jobs cost from history (actual), warm tier (estimated), or YAML (projected)."""
    hf_history_cost = sum(
        sum(float(r.get("estimated_cost_usd") or 0) for r in hf[cid].runs) for cid in card_ids_subset if cid in hf
    )
    if hf_history_cost > 0:
        return hf_history_cost, "actual"

    # Fallback: warm tier (estimated from CostEstimateHook)
    hf_entry_points: set[str] = set()
    for card in cards_subset.values():
        cost_cfg = card.get("cost") or {}
        for phase in ("training", "inference"):
            phase_cost = cost_cfg.get(phase)
            if phase_cost and phase_cost.get("runtime", "").lower() in ("hf-jobs", "hf_jobs", "hf jobs"):
                ep = ((card.get("execution") or {}).get(phase) or {}).get("entry_point", "")
                if ep:
                    hf_entry_points.add(ep)
    hf_warm_cost = 0.0
    if not warm.empty and hf_entry_points:
        hf_df = warm[warm["task_key"].isin(list(hf_entry_points))]
        if not hf_df.empty:
            hf_warm_cost = float(hf_df["estimated_cost_usd"].sum())
    if hf_warm_cost > 0:
        return hf_warm_cost, "estimated"

    # Last resort: YAML projected costs
    hf_cost = 0.0
    for card in cards_subset.values():
        cost_cfg = card.get("cost") or {}
        for phase in ("training", "inference"):
            phase_cost = cost_cfg.get(phase)
            if phase_cost and phase_cost.get("runtime", "").lower() in ("hf-jobs", "hf_jobs", "hf jobs"):
                hf_cost += float(phase_cost.get("typical_cost_usd") or 0)
    return hf_cost, "projected"


def _compute_freshness_stats(
    state: Any,
    cards_subset: dict[str, dict[str, Any]],
    jobs: dict[str, dict[str, Any]],
    hf: dict[str, HFCostData],
) -> None:
    """Compute freshness summary with status breakdown."""
    monitored = 0
    fresh_count = 0
    warning_count = 0
    stale_count = 0
    now_utc = pd.Timestamp.now(tz="UTC")
    for _card_id, card in cards_subset.items():
        sla = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla is None:
            continue
        monitored += 1
        hf_data = hf.get(_card_id)
        if hf_data and hf_data.is_running:
            fresh_count += 1
            continue
        jobs_last_run = jobs.get(_card_id, {}).get("last_run")
        hf_last_run: pd.Timestamp | None = None
        if hf_data and hf_data.latest_run:
            ended = hf_data.latest_run.get("ended_at", "")
            if ended:
                try:
                    hf_last_run = pd.Timestamp(ended)
                except (ValueError, TypeError):
                    pass
        last_run = max(filter(None, [jobs_last_run, hf_last_run]), default=None)
        if last_run is not None:
            age_hours = (now_utc - last_run).total_seconds() / 3600
            status = classify_freshness(age_hours, sla)
            if status == "OK":
                fresh_count += 1
            elif status == "Warning":
                warning_count += 1
            else:
                stale_count += 1
        else:
            stale_count += 1
    if monitored > 0:
        state.wf_freshness_summary = f"{fresh_count}/{monitored} within SLA"
        detail_parts: list[str] = []
        if warning_count:
            detail_parts.append(f'<span style="color:{FRESHNESS_HEX["warning"]}">{warning_count} warning</span>')
        if stale_count:
            detail_parts.append(f'<span style="color:{FRESHNESS_HEX["stale"]}">{stale_count} stale</span>')
        state.wf_freshness_detail = _stat_detail_html(" \u2014 ".join(detail_parts))
    else:
        state.wf_freshness_summary = "No SLAs configured"
        state.wf_freshness_detail = RawHtml("")


__all__ = [
    "HFCostData",
    "WF_TABLE_COLS",
    "_stat_detail_html",
    "build_table_data",
    "build_task_key_to_wf_id",
    "classify_freshness",
    "classify_runtime",
    "compute_stats",
    "filter_card_ids",
    "load_cards_from_yaml",
    # Table cell style callbacks (bound to Taipy state)
    "wf_style_freshness",
    "wf_style_runtime",
    "wf_style_status",
    "wf_style_type",
]
