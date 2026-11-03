"""The Runs page (spec §17, §19; audit F6): the hub for one run. It lists the run history and a non-blocking "Run
now", and a sidebar Run selector drills into the selected run — its steps (layer, duration, rows, state), spend,
inter-layer dependencies and log lines. The per-run log view folds in the former standalone Logs page (audit F6), so a
run's logs are read beside its steps for that same run, not on a page pinned to the latest run.

"Run now" calls `RunService.trigger()`, which starts a detached local process or triggers the owner-created HF job and
returns at once; the page never runs the ingestion itself.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.observability import LogRecord
from lakehouse.services.runs import RunSummary, TriggerResult
from lakehouse.ui.pages._logs_data import filtered_records, log_table_cols
from lakehouse.ui.pages._monitoring_data import DBT_HEADERS, STEP_HEADERS, spend_line, step_dict
from lakehouse.ui.template import (
    Button,
    Control,
    NavSection,
    PageConfig,
    Prose,
    Section,
    Table,
    Text,
    build_page,
    table_columns,
)

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = [
    "CONFIG",
    "DEFAULT_N",
    "ICON",
    "ROUTE",
    "TITLE",
    "on_log_filter",
    "on_pick_run",
    "page",
    "refresh",
    "run_now",
]

TITLE = "Runs"
ICON = "play_circle"
ROUTE = "runs"
DEFAULT_N = 200  # the most recent N log lines of the selected run (spec §9, folded from the Logs page)
CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="The run history, a non-blocking Run now, and the full detail of the selected run.",
    body=(
        Button("Run now", "on_run_now", primary=True),
        Text("runs_progress"),
        Prose(
            "The run history. Pick a run in the sidebar to see its steps, spend, dependencies and logs. Written is the "
            "rows added or changed; any errors is worse than none; Status is ok, running, failed or skipped."
        ),
        Table("runs_rows"),
        Text("runs_detail_header"),
        Table("runs_detail_steps", render_var="runs_has_detail"),
        Text("runs_detail_summary"),
        Table("runs_detail_dbt", render_var="runs_has_detail"),
        Text("runs_detail_note"),
        Section(
            "Logs",
            (
                Control(
                    "input",
                    "runs_log_level",
                    label="Level",
                    help="Show only lines of this exact level, for example ERROR or WARNING.",
                    on_change="on_log_filter",
                ),
                Control(
                    "input",
                    "runs_log_text",
                    label="Text contains",
                    help="Show only lines whose message or logger contains this text.",
                    on_change="on_log_filter",
                ),
            ),
        ),
        Table("runs_logs_rows", render_var="runs_has_detail"),
    ),
    sidebar_widgets=(
        Control(
            "selector",
            "runs_selected",
            label="Run",
            help="Pick a past or in-flight run to see its steps, spend, dependencies and logs.",
            lov="runs_run_lov",
            on_change="on_pick_run",
            value_by_id=True,  # lov is (run_id, label); bind the run id, not the tuple
        ),
    ),
)

# Bound client state (built at module scope, fact 8): the overview (progress + history table), and the selected run's
# detail (header, steps, dependency summary + dbt table + note, and the cached/filtered logs). The sidebar Run selector
# (`runs_selected`, `runs_run_lov`) is declared in lakehouse.ui.filter_state: the sidebar binds only the default scope.
runs_progress: str = ""
runs_rows: dict[str, list[str]] = {}
runs_detail_header: str = ""
runs_detail_steps: dict[str, list[str]] = {}
runs_detail_summary: str = ""
runs_detail_dbt: dict[str, list[str]] = {}
runs_detail_note: str = ""
runs_logs_rows: dict[str, list[str]] = {}
runs_log_cache: list[LogRecord] = []  # the selected run's records, filtered client-side by on_log_filter
runs_log_level: str = ""
runs_log_text: str = ""
runs_has_detail: bool = False  # gates the detail tables (the table control ignores render; a part wrapper hides them)

_RUN_HEADERS = ("Started", "Trigger", "Status", "Written", "Errors", "Duration")


def run_now(services: Services) -> TriggerResult:
    """The "Run now" action: start a run and return immediately (never blocks the page on the ingestion)."""
    return services.runs.trigger()


def _duration_text(run: RunSummary) -> str:
    """How long the run took: wall-clock elapsed (ended minus started), or "in flight" while it is still running.
    Stated as a duration so the operator reads elapsed time directly, not an end clock time they must subtract."""
    if run.ended_at is None:
        return "in flight"
    seconds = max(0.0, (run.ended_at - run.started_at).total_seconds())
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m"


def _run_dict(run: RunSummary) -> dict[str, str]:
    """One run as a native-table row dict."""
    written = run.counts.added + run.counts.changed
    return {
        "Started": f"{run.started_at:%Y-%m-%d %H:%M}",
        "Trigger": run.trigger,
        "Status": run.status,
        "Written": f"{written} written",
        "Errors": f"{run.error_count} errors" if run.error_count else "no errors",
        "Duration": _duration_text(run),
    }


def _run_label(run: RunSummary) -> str:
    """The Run selector label: the started time and the status, so the operator picks a run by when it ran."""
    return f"{run.started_at:%Y-%m-%d %H:%M} · {run.status}"


def _progress_text(services: Services, history: list[RunSummary]) -> str:
    """The in-flight progress line, or empty when nothing is running."""
    running = [run for run in history if run.status == "running"]
    if not running:
        return ""
    step = services.runs.progress(running[0].run_id)
    where = step.module_id or "starting"
    total = "?" if step.total is None else str(step.total)
    return f"**In flight:** {where} — {step.step} ({step.done} of {total})\n"


def _clear_detail(state: Any) -> None:
    """No run to show (an empty lake): clear the detail and hide its tables."""
    state.runs_detail_header = "No runs yet."
    state.runs_detail_steps = table_columns([], STEP_HEADERS)
    state.runs_detail_summary = ""
    state.runs_detail_dbt = table_columns([], DBT_HEADERS)
    state.runs_detail_note = ""
    state.runs_log_cache = []
    state.runs_logs_rows = log_table_cols([])
    state.runs_has_detail = False


def _fill_detail(state: Any, services: Services, summary: RunSummary) -> None:
    """Fill the selected run's steps, spend, dependencies and logs."""
    rows = services.runs.task_rows(summary.run_id)
    dependency = services.runs.dependency(summary.run_id)
    run_edges = " · ".join(f"{src} → {dst}" for src, dst in dependency.run_edges) or "—"
    state.runs_detail_header = (
        f"## Run detail\n\n"
        f"Started {summary.started_at:%Y-%m-%d %H:%M}.\n\n"
        f"**{spend_line(rows)}**\n\n"
        "### Steps\n\n"
        "The duration is measured process time; higher means that step took longer.\n"
    )
    state.runs_detail_steps = table_columns([step_dict(row) for row in rows], STEP_HEADERS)
    state.runs_detail_summary = (
        "### Layer dependencies\n\n"
        "These edges are containment and run order, not data dependencies.\n\n"
        "Layers: bronze is the raw ingest, silver is cleaned, gold is the published marts.\n\n"
        f"Run layers: {run_edges}\n\n"
        "dbt models (silver feeds gold):\n"
    )
    state.runs_detail_dbt = table_columns(
        [{"From (silver)": src, "To (gold)": dst} for src, dst in dependency.dbt_edges], DBT_HEADERS
    )
    state.runs_detail_note = f"> {dependency.note}" if dependency.note else ""
    records = services.runs.logs(summary.run_id, limit=DEFAULT_N)
    state.runs_log_cache = records
    shown = filtered_records(records, getattr(state, "runs_log_level", ""), getattr(state, "runs_log_text", ""))
    state.runs_logs_rows = log_table_cols(shown)
    state.runs_has_detail = True


def refresh(state: Any, services: Services) -> None:
    """Fill the history, the in-flight progress and the Run selector, then the selected run's detail (§4). The
    selection defaults to the latest run and falls back to it when a stale selection no longer exists."""
    history = services.runs.history()
    state.runs_progress = _progress_text(services, history)
    state.runs_rows = table_columns([_run_dict(run) for run in history], _RUN_HEADERS)
    state.runs_run_lov = [(run.run_id, _run_label(run)) for run in history]
    valid = {run.run_id for run in history}
    selected = getattr(state, "runs_selected", "") or ""
    if selected not in valid:
        selected = history[0].run_id if history else ""
    state.runs_selected = selected
    if selected:
        _fill_detail(state, services, next(run for run in history if run.run_id == selected))
    else:
        _clear_detail(state)


def on_pick_run(state: Any, services: Services, value: str) -> None:
    """Reload the detail for the newly picked run. Services-backed, dispatched by the Gui-level on_change (spec S03)."""
    state.runs_selected = value
    refresh(state, services)


def on_log_filter(state: Any, services: Any, value: str) -> None:
    """Re-filter the selected run's cached log records client-side (the dispatcher passes `services`; this handler
    does not use it). Shared handler shape `(state, services, value)` per spec C06."""
    records: list[LogRecord] = getattr(state, "runs_log_cache", [])
    shown = filtered_records(records, getattr(state, "runs_log_level", ""), getattr(state, "runs_log_text", ""))
    state.runs_logs_rows = log_table_cols(shown)


page = Markdown(build_page(CONFIG))
