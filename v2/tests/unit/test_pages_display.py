"""The display-only pages as bound Taipy pages (page-binding plan PB5): runs, settings and monitoring fill a display
variable from the services via `refresh` and build a `page` Markdown. runs keeps `on_run_now` (no input); monitoring
has no actions; settings carries the folded-in schedule actions (audit F8)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.ui.pages import monitoring, runs, settings
from lakehouse.ui.template import action_names


class _EmptyRuns:
    def history(self, limit: int = 500) -> list[Any]:
        return []


def test_runs_display() -> None:
    state = SimpleNamespace()
    runs.refresh(state, cast("Any", SimpleNamespace(runs=_EmptyRuns())))
    assert all(not column for column in state.runs_rows.values())  # no runs: the native table is empty
    assert action_names(runs.CONFIG) == ("on_run_now",)  # derived from the Button, not a hand-kept ACTIONS tuple
    assert isinstance(runs.page, Markdown)


def test_monitoring_display() -> None:
    state = SimpleNamespace()
    monitoring.refresh(state, cast("Any", SimpleNamespace(runs=_EmptyRuns())))
    assert "no runs yet" in state.monitoring_top.lower()
    assert isinstance(monitoring.page, Markdown)
    assert action_names(monitoring.CONFIG) == ()  # display-only, no write action


def test_settings_display() -> None:
    settings_ns = SimpleNamespace(
        modules=[SimpleNamespace(active=True)],
        retention=SimpleNamespace(version_days=7, snapshot_days=30),
        caps=SimpleNamespace(threads=4, memory_limit="8GB"),
        checkpoint=SimpleNamespace(minutes=5, matches=50, grace_minutes=2, move_batch=50),
        maintenance=SimpleNamespace(merge_budget_seconds=300),
        raw_archive=True,
        auto_accept_recommended=False,
    )
    roots = SimpleNamespace(
        roots=SimpleNamespace(
            lake=SimpleNamespace(local_path="/lake", hf_bucket=None),
            restricted=SimpleNamespace(local_path="/restricted", hf_bucket=None),
        ),
        source="cli",
    )

    class _Settings:
        def load(self) -> tuple[Any, int]:
            return (settings_ns, 3)

        def storage_roots(self) -> Any:
            return roots

        def history(self, limit: int = 10) -> list[Any]:
            return []

    # audit F8 folded the schedule into Settings, so settings.refresh now also reads the schedule service.
    schedule_status = SimpleNamespace(
        backend="cron", enabled=False, installed=True, cron="@daily", next_run=None, last_result="ok"
    )

    class _Schedule:
        def get(self) -> Any:
            return schedule_status

    state = SimpleNamespace()
    services = SimpleNamespace(settings=_Settings(), schedule=_Schedule())
    settings.refresh(state, cast("Any", services))
    assert "Config version **3**" in state.cfg_prose
    assert state.schedule_status_note == ""  # a clean read leaves no unavailable note
    # the T3 checkpoint block shows beside retention and compute (spec D1, the same-pattern rule)
    assert (
        "Publish the catalog every 5 minutes or 50 units; keep a superseded run version 2 minutes; "
        "batch up to 50 private-to-public moves." in state.cfg_prose
    )
    # the T12 maintenance budget shows with its unit and what it means (the UI rule: interpretable without help)
    assert (
        "Merge small catalog data files at each run end for up to 300 seconds; the rest waits for the next run."
        in state.cfg_prose
    )
    assert isinstance(settings.page, Markdown)

    # a schedule-backend read error must NOT blank the config section: the schedule section shows an "unavailable"
    # note (a surfaced fallback, never silent) and the config prose + history still render.
    from lakehouse.errors import HttpStatusError

    class _Broken:
        def get(self) -> Any:
            raise HttpStatusError(403, "scheduled job sched-1: status")

    broken_state = SimpleNamespace()
    settings.refresh(broken_state, cast("Any", SimpleNamespace(settings=_Settings(), schedule=_Broken())))
    assert "Config version **3**" in broken_state.cfg_prose  # the config section still rendered
    note = broken_state.schedule_status_note
    assert "Schedule status unavailable" in note and "403" in note
    assert "LAKEHOUSE_HF_JOBS_TOKEN" in note  # names the fix
    assert all(not column for column in broken_state.schedule_rows.values())  # the status table is empty, not stale
    assert broken_state.schedule_version == 3  # the version still loaded, so a later Save has its token
    # the folded-in schedule actions, in first-seen order (audit F8); settings is no longer action-free
    assert action_names(settings.CONFIG) == (
        "on_save_schedule",
        "on_pause_schedule",
        "on_resume_schedule",
        "on_run_now",
    )
