"""The Monitoring page (spec §7.2-§7.4, §6.3): spend per run and the inter-layer dependency view.

It shows only the run's steps (layer, duration, rows, state), the run-level spend estimate, and the layer/model
dependency edges — never a match name or id — so it is safe for an anonymous caller by column scope (§6.4). Spend is
a dated static-table estimate over measured process time, not live billing (§7.3).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.ui.pages._monitoring_data import DBT_HEADERS, STEP_HEADERS, spend_line, step_dict
from lakehouse.ui.template import NavSection, PageConfig, Table, Text, build_page, table_columns

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = ["CONFIG", "ICON", "ROUTE", "TITLE", "page", "refresh", "spend_line"]

TITLE = "Monitoring"
ICON = "monitoring"
ROUTE = "monitoring"
CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="Spend per run and the inter-layer dependency view. It carries no team or player identity.",
    body=(
        Text("monitoring_top"),
        Table("monitoring_steps_rows", render_var="monitoring_has_run"),
        Text("monitoring_deps"),
        Table("monitoring_dbt_rows", render_var="monitoring_has_run"),
        Text("monitoring_note"),
    ),
    reads_match_data=False,  # run steps, spend and layer edges only — no match/team/player identity (spec §6.3)
)

# Bound client state (built at module scope, fact 8). The tables are gated on `monitoring_has_run`: the `table`
# control ignores `render`, so a `part` wrapper hides them on a no-runs lake (fact 4).
monitoring_top: str = ""  # the prose above the steps table ("Latest run …", spend, Steps heading)
monitoring_deps: str = ""  # the prose above the dbt table (Layer dependencies, run-layer edges)
monitoring_note: str = ""  # the optional dependency note, shown after the dbt table
monitoring_steps_rows: dict[str, list[str]] = {}  # the per-step native table
monitoring_dbt_rows: dict[str, list[str]] = {}  # the silver→gold dbt native table
monitoring_has_run: bool = False  # gates the tables so a no-runs lake shows only "No runs yet."


def refresh(state: Any, services: Services) -> None:
    """Fill the spend + dependency view into the display variables (§4). Display-only; gated by `monitoring_has_run`.
    The anonymous public page renders the same data via the Taipy-free `_monitoring_data` module (ADR-011, §2.1)."""
    history = services.runs.history(limit=1)
    state.monitoring_has_run = bool(history)
    if not history:
        state.monitoring_top = "No runs yet."
        state.monitoring_deps = ""
        state.monitoring_note = ""
        state.monitoring_steps_rows = table_columns([], STEP_HEADERS)
        state.monitoring_dbt_rows = table_columns([], DBT_HEADERS)
        return
    latest = history[0]
    rows = services.runs.task_rows(latest.run_id)
    dependency = services.runs.dependency(latest.run_id)
    run_edges = " · ".join(f"{src} → {dst}" for src, dst in dependency.run_edges) or "—"
    state.monitoring_top = (
        f"Latest run started {latest.started_at:%Y-%m-%d %H:%M}.\n\n"
        f"**{spend_line(rows)}**\n\n"
        "## Steps\n\n"
        "The duration is measured process time; higher means that step took longer.\n"
    )
    state.monitoring_steps_rows = table_columns([step_dict(row) for row in rows], STEP_HEADERS)
    state.monitoring_deps = (
        "## Layer dependencies\n\n"
        "These edges are containment and run order, not data dependencies.\n\n"
        "Layers: bronze is the raw ingest, silver is cleaned, gold is the published marts.\n\n"
        f"Run layers: {run_edges}\n\n"
        "dbt models (silver feeds gold):\n"
    )
    state.monitoring_dbt_rows = table_columns(
        [{"From (silver)": src, "To (gold)": dst} for src, dst in dependency.dbt_edges], DBT_HEADERS
    )
    state.monitoring_note = f"> {dependency.note}" if dependency.note else ""


page = Markdown(build_page(CONFIG))
