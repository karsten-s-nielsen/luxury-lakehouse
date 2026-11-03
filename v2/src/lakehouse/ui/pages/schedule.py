"""The schedule status + edit logic (spec §17.5, §18, §19; audit F8): the one schedule for all active data sources,
off by default, with the backend, whether it is installed, the cron and the next run.

Audit F8 folded the schedule into the Settings page (`settings.py`) as a section. This module is therefore a helper
(no `page`/`CONFIG`): `settings.refresh` calls `refresh` here to fill the schedule status and seed the draft, and the
schedule edit controls + actions live on the Settings page.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lakehouse.ui.template import table_columns

if TYPE_CHECKING:
    from lakehouse.ui.shell import Services

__all__ = ["SCHEDULE_HEADERS", "fill_unavailable", "refresh"]

SCHEDULE_HEADERS = ("Backend", "State", "Installed", "Cron", "Next run", "Last result")


def fill_unavailable(state: Any, services: Services) -> None:
    """The schedule status could not be read (for example the job-control token lacks job access). Show an empty
    status table and a safe draft so the Settings page still renders its config section (settings.refresh). The
    optimistic-concurrency version still loads from the lake — a lake read, not the schedule backend — so it fails
    loudly if the lake itself is unreachable."""
    state.schedule_rows = table_columns([], SCHEDULE_HEADERS)
    state.draft_cron = ""
    state.draft_enabled = False
    _settings, version = services.settings.load()
    state.schedule_version = int(version)


def refresh(state: Any, services: Services) -> None:
    """Fill the status table and seed the draft + the optimistic-concurrency version from the services (§4)."""
    status = services.schedule.get()
    state_label = "on" if status.enabled else "off"
    installed = "installed" if status.installed else "not installed"
    cron = status.cron or "—"
    next_run = "—" if status.next_run is None else f"{status.next_run:%Y-%m-%d %H:%M}"
    last = status.last_result or "—"
    row = {
        "Backend": status.backend,
        "State": state_label,
        "Installed": installed,
        "Cron": cron,
        "Next run": next_run,
        "Last result": last,
    }
    state.schedule_rows = table_columns([row], SCHEDULE_HEADERS)
    state.draft_cron = status.cron or ""
    state.draft_enabled = bool(status.enabled)
    _settings, version = services.settings.load()
    state.schedule_version = int(version)
