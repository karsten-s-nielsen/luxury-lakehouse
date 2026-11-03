"""The Settings page (spec §8.3, §10.5, §17.5, §18, §19; audit F8): the schedule (the one schedule for all active
sources, off by default) plus the read-only config — the saved version and where the lake lives, the retention and
resource caps, and the recent config history. The schedule folded in from its own page (audit F8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.errors import HttpStatusError
from lakehouse.ui.pages import schedule
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

__all__ = ["CONFIG", "ICON", "ROUTE", "TITLE", "page", "refresh"]

TITLE = "Settings"
ICON = "settings"
ROUTE = "settings"
CONFIG = PageConfig(
    title=TITLE,
    icon=ICON,
    nav_section=NavSection("Operations"),
    description="The schedule, the saved config and where the lake lives, retention and compute, and config history.",
    body=(
        Section(
            "Schedule",
            (
                Prose(
                    "One schedule runs every active data source. A missed run is not replayed, "
                    "and at most one run starts on wake."
                ),
                Table("schedule_rows"),
                Text("schedule_status_note"),
                Control(
                    "input",
                    "draft_cron",
                    label="Cron",
                    help="Five cron fields (minute hour day month weekday) or a macro such as @daily.",
                ),
                Control(
                    "toggle",
                    "draft_enabled",
                    label="Run on this schedule",
                    help="Turn the schedule on or off. Off by default.",
                ),
                Button("Save schedule", "on_save_schedule", primary=True),
                Button("Pause", "on_pause_schedule"),
                Button("Resume", "on_resume_schedule"),
                Button("Run now", "on_run_now"),
            ),
        ),
        Text("cfg_prose"),
        Table("cfg_history_rows"),
    ),
)

# Bound client state (page-binding): the schedule draft + status (the exact names _collect reads for the schedule
# actions), the read-only prose and the config-history native table. Built at module scope so the page `Markdown`
# binds them (as-built fact 8); `render_for` seeds its recording state from these defaults (C04).
draft_cron: str = ""
draft_enabled: bool = False
schedule_version: int = 0
schedule_rows: dict[str, list[str]] = {}  # the schedule-status native table (one row)
schedule_status_note: str = ""  # shown when the schedule status could not be read, so the config section still renders
cfg_prose: str = ""
cfg_history_rows: dict[str, list[str]] = {}

_HISTORY_HEADERS = ("Version", "Saved by", "When", "Changes")

_SOURCE_WORDS = {
    "cli": "the --lake option",
    "env": "the LAKEHOUSE_LAKE_ROOT variable",
    "bootstrap": "the saved bootstrap file",
}


def _prose(services: Services) -> str:
    """The read-only prose (version, where the lake lives, retention and compute), ending with the history heading.
    The config history itself renders as a native table (`cfg_history_rows`), not in this Markdown."""
    settings, version = services.settings.load()
    resolved = services.settings.storage_roots()
    lake = resolved.roots.lake
    restricted = resolved.roots.restricted
    lake_text = str(lake.local_path) if lake.local_path is not None else f"hf://buckets/{lake.hf_bucket}"
    restricted_text = (
        str(restricted.local_path) if restricted.local_path is not None else f"hf://buckets/{restricted.hf_bucket}"
    )
    source = _SOURCE_WORDS.get(resolved.source, resolved.source)
    active = sum(1 for module in settings.modules if module.active)
    return (
        f"Config version **{version}**, {active} of {len(settings.modules)} sources active.\n\n"
        "## Where the lake lives\n\n"
        f"Resolved from {source}.\n\n"
        f"- Lake root: `{lake_text}`\n"
        f"- Restricted root: `{restricted_text}`\n\n"
        "## Retention and compute\n\n"
        f"- Keep run-end versions {settings.retention.version_days} days, "
        f"snapshots {settings.retention.snapshot_days} days.\n"
        f"- DuckDB {settings.caps.threads} threads, {settings.caps.memory_limit} memory.\n"
        f"- Publish the catalog every {settings.checkpoint.minutes} minutes or {settings.checkpoint.matches} units; "
        f"keep a superseded run version {settings.checkpoint.grace_minutes} minutes; "
        f"batch up to {settings.checkpoint.move_batch} private-to-public moves.\n"
        f"- Merge small catalog data files at each run end for up to "
        f"{settings.maintenance.merge_budget_seconds} seconds; the rest waits for the next run.\n"
        f"- Raw archive {'on' if settings.raw_archive else 'off'}, "
        f"auto-accept recommended {'on' if settings.auto_accept_recommended else 'off'}.\n\n"
        "## Recent config changes\n"
    )


def _history_dicts(services: Services) -> list[dict[str, str]]:
    """The config history as native-table row dicts."""
    return [
        {
            "Version": str(entry.version),
            "Saved by": entry.saved_by,
            "When": f"{entry.saved_at:%Y-%m-%d %H:%M}",
            "Changes": ", ".join(entry.changes),
        }
        for entry in services.settings.history(limit=10)
    ]


def refresh(state: Any, services: Services) -> None:
    """Fill the schedule status + draft and the read-only config view (§4). The schedule section is editable; the
    config section is display-only. A schedule-backend read error (for example the job-control token lacks job access)
    is isolated: the schedule section shows an "unavailable" note and the config section still renders — one refused
    HF call never blanks the saved config, lake roots, retention and history, none of which touch the job API."""
    try:
        schedule.refresh(state, services)  # the status table, the draft and the optimistic-concurrency version
        state.schedule_status_note = ""
    except HttpStatusError as exc:
        schedule.fill_unavailable(state, services)
        state.schedule_status_note = (
            f"> **Schedule status unavailable** ({exc}). The job-control token `LAKEHOUSE_HF_JOBS_TOKEN` needs "
            "read and write access to the scheduled job; the schedule controls will not work until it does."
        )
    state.cfg_prose = _prose(services)
    state.cfg_history_rows = table_columns(_history_dicts(services), _HISTORY_HEADERS)


page = Markdown(build_page(CONFIG))
