"""S1 Task 11: the anonymous-route seam cannot expose match data (plan T11).

`ANON_VIEWABLE_ROUTES` lists the routes an anonymous caller may view. A standing test asserts every such route's
page declares `reads_match_data is False`, so adding a match-reading page to the set fails CI. An independent test
runs `monitoring.refresh` against a recording double and asserts it binds no match identity, so the self-declared
flag cannot lie.
"""

from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module
from types import SimpleNamespace
from typing import Any

from lakehouse.ui import shell
from lakehouse.ui.pages import monitoring


def _route_configs() -> dict[str, Any]:
    configs: dict[str, Any] = {}
    for name in shell.PAGE_MODULES:
        module = import_module(f"lakehouse.ui.pages.{name}")
        configs[module.ROUTE] = module.CONFIG
    return configs


def test_every_anon_route_declares_no_match_data() -> None:
    configs = _route_configs()
    for route in shell.ANON_VIEWABLE_ROUTES:
        assert route in configs, f"ANON_VIEWABLE_ROUTES names {route!r} but no page has that ROUTE"
        assert configs[route].reads_match_data is False, (
            f"{route!r} is anonymously viewable but its page declares reads_match_data=True"
        )


class _RecordingState:
    """Records every display variable the page binds, so a match-scoped bind cannot hide."""

    def __init__(self, recorder: list[object]) -> None:
        object.__setattr__(self, "_recorder", recorder)

    def __setattr__(self, name: str, value: object) -> None:
        recorder: list[object] = object.__getattribute__(self, "_recorder")
        recorder.append(value)


def _runs_double(recorder: list[object]) -> SimpleNamespace:
    def record(value: object) -> object:
        recorder.append(value)
        return value

    def history(limit: int = 1) -> object:
        return record([SimpleNamespace(run_id="r1", started_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC))])

    def task_rows(run_id: str) -> object:
        return record(
            [
                SimpleNamespace(
                    task="ingest",
                    layer="bronze",
                    duration_seconds=1.0,
                    row_count=5,
                    state="ok",
                    cost_usd=None,
                    cost_error=None,
                )
            ]
        )

    def dependency(run_id: str) -> object:
        return record(SimpleNamespace(run_edges=[("bronze", "dbt")], dbt_edges=[("stg_runs", "mart_runs")], note=None))

    return SimpleNamespace(history=history, task_rows=task_rows, dependency=dependency)


def test_monitoring_refresh_binds_no_match_identity() -> None:
    recorder: list[object] = []
    services = SimpleNamespace(runs=_runs_double(recorder))
    monitoring.refresh(_RecordingState(recorder), services)  # type: ignore[arg-type]
    blob = " ".join(repr(value) for value in recorder)
    assert "match_key" not in blob and "native_match_id" not in blob, (
        "monitoring declares reads_match_data=False but a value it read or bound carries a match identity"
    )
