"""The Settings page as a bound Taipy page (page-binding PB4; audit F8): the schedule section (folded in from its own
page) plus the read-only config view. `settings` owns `draft_cron`, `draft_enabled` and `schedule_version` (the exact
names `_collect` reads for the schedule actions); `schedule.refresh` (a helper) fills the status and seeds the draft,
and `settings.refresh` fills that plus the read-only config prose and history."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from lakehouse.ui.pages import schedule, settings
from lakehouse.ui.template import action_names


def _cells(columns: dict[str, list[str]]) -> list[str]:
    return [cell for column in columns.values() for cell in column]


def _schedule_services(*, enabled: bool, cron: str | None, version: int) -> Any:
    status = SimpleNamespace(
        backend="cron", enabled=enabled, installed=True, cron=cron, next_run=None, last_result="ok"
    )

    class _Schedule:
        def get(self) -> Any:
            return status

    class _Settings:
        def load(self) -> tuple[Any, int]:
            return (SimpleNamespace(), version)

    return SimpleNamespace(schedule=_Schedule(), settings=_Settings())


def test_schedule_helper_refresh_seeds() -> None:
    state = SimpleNamespace()
    schedule.refresh(state, _schedule_services(enabled=True, cron="@daily", version=7))
    cells = _cells(state.schedule_rows)
    assert any("@daily" in c for c in cells) and "on" in cells
    assert state.draft_cron == "@daily"
    assert state.draft_enabled is True
    assert state.schedule_version == 7  # the optimistic-concurrency version for the save


def test_settings_defaults_and_actions() -> None:
    assert settings.draft_cron == "" and settings.draft_enabled is False and settings.schedule_version == 0
    # the schedule actions fold onto the Settings page (audit F8), in first-seen order.
    assert action_names(settings.CONFIG) == (
        "on_save_schedule",
        "on_pause_schedule",
        "on_resume_schedule",
        "on_run_now",
    )


def _full_settings_services(version: int) -> Any:
    settings_obj = SimpleNamespace(
        modules=[SimpleNamespace(active=True), SimpleNamespace(active=False)],
        retention=SimpleNamespace(version_days=30, snapshot_days=7),
        caps=SimpleNamespace(threads=4, memory_limit="8GB"),
        checkpoint=SimpleNamespace(minutes=10, matches=50, grace_minutes=5, move_batch=100),
        maintenance=SimpleNamespace(merge_budget_seconds=60),
        raw_archive=True,
        auto_accept_recommended=False,
    )
    roots = SimpleNamespace(
        lake=SimpleNamespace(local_path=Path("/lake"), hf_bucket=None),
        restricted=SimpleNamespace(local_path=Path("/restricted"), hf_bucket=None),
    )
    resolved = SimpleNamespace(roots=roots, source="cli")
    status = SimpleNamespace(
        backend="cron", enabled=False, installed=True, cron="@daily", next_run=None, last_result="ok"
    )
    history = [
        SimpleNamespace(version=1, saved_by="ops", saved_at=datetime(2026, 10, 28, tzinfo=UTC), changes=["init"])
    ]

    class _Settings:
        def load(self) -> tuple[Any, int]:
            return (settings_obj, version)

        def storage_roots(self) -> Any:
            return resolved

        def history(self, limit: int = 10) -> list[Any]:
            return history

    class _Schedule:
        def get(self) -> Any:
            return status

    return SimpleNamespace(settings=_Settings(), schedule=_Schedule())


def test_settings_refresh_fills_schedule_and_config() -> None:
    state = SimpleNamespace()
    settings.refresh(state, _full_settings_services(version=3))
    assert any("@daily" in c for c in _cells(state.schedule_rows))  # the schedule section
    assert state.schedule_version == 3
    assert "Config version **3**" in state.cfg_prose  # the read-only config prose
    assert any("init" in c for c in _cells(state.cfg_history_rows))  # the config history


def test_settings_page_builds() -> None:
    from taipy.gui import Markdown  # pyright: ignore[reportMissingTypeStubs]

    assert isinstance(settings.page, Markdown)
