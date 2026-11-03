"""The run calls the maintenance at its end (T12 spec D3; plan Task 5).

The maintenance runs after dbt and before the cleanup, only on a run that was not stopped, with the saved budget; a
guard stop is recorded as a run error, the run still cleans up and publishes its data, and ends `failed` (Reviewer B
T12-PLAN-04). The `catalog totals` line carries the merged file count and time.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

import lakehouse.runs.run as run_module
from lakehouse.catalog.maintenance import MaintenanceReport, TableMaintenance
from lakehouse.catalog.reader import CatalogReader
from lakehouse.errors import GuardStopError
from lakehouse.runs.dbt import DbtResult
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, exit_code, run_ingestion

MODULE = "idsse_public"


def _adapter() -> FakeAdapter:
    natives = ("J03WPY", "J03WMX")
    files = {(native, "events"): f"{native}-a\n{native}-b".encode() for native in natives}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in natives], files)


def _run(kit: LakeKit, **kwargs: Any) -> RunResult:
    save_settings(kit.storage, [active(MODULE)])
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={MODULE: _adapter()},
        parsers={"idsse": FakeParser("idsse")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
        **kwargs,
    )


def _dbt_into(order: list[str]) -> Any:
    def dbt(cat: Any, *, run_id: str, caps: Any) -> DbtResult:
        order.append("dbt")
        return DbtResult(success=True, results=[], run_results_key=f"runs/dbt/{run_id}/run_results.json")

    return dbt


def test_maintenance_runs_after_dbt_before_cleanup(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    budgets: list[float | None] = []
    real_maintain, real_cleanup = run_module.maintain_tables, run_module.run_cleanup

    def maintain(cat: Any, **kwargs: Any) -> MaintenanceReport:
        order.append("maintain")
        budgets.append(kwargs.get("budget_s"))
        return real_maintain(cat, **kwargs)

    def cleanup(*args: Any, **kwargs: Any) -> Any:
        order.append("cleanup")
        return real_cleanup(*args, **kwargs)

    monkeypatch.setattr(run_module, "maintain_tables", maintain)
    monkeypatch.setattr(run_module, "run_cleanup", cleanup)
    assert _run(kit, dbt=_dbt_into(order)).status == "ok"
    assert order == ["dbt", "maintain", "cleanup"]
    assert budgets == [300.0]  # the saved setting's default


def test_no_maintenance_on_a_stopped_run(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def spy(cat: Any, **kwargs: Any) -> None:
        calls.append("maintain")

    monkeypatch.setattr(run_module, "maintain_tables", spy)
    _run(kit, dbt=None, stop=lambda: True)
    assert calls == []


def test_maintenance_guard_stop_fails_the_run(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    def guard(cat: Any, **kwargs: Any) -> MaintenanceReport:
        raise GuardStopError("bronze tables without auto_compact = false: skillcorner_tracking_players")

    monkeypatch.setattr(run_module, "maintain_tables", guard)
    result = _run(kit, dbt=None)
    assert (result.status, exit_code(result)) == ("failed", 1)
    with CatalogReader(kit.storage).session() as conn:
        errors = read_table(conn, "ops_errors")
        matches = read_table(conn, "ops_matches")
    [error] = [e for e in errors if e["kind"] == "maintenance_guard"]
    assert "auto_compact" in error["message"]
    assert len(matches) == 2, "the run still published its data"


def test_catalog_totals_carry_the_maintenance(
    kit: LakeKit, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    report = MaintenanceReport(
        tables=(TableMaintenance("ops_probe", 40, 2, True, 1, 2, 17),), budget_spent=False, left=(), ms=21
    )

    def fixed(cat: Any, **kwargs: Any) -> MaintenanceReport:
        return report

    monkeypatch.setattr(run_module, "maintain_tables", fixed)
    _run(kit, dbt=None)
    [totals] = [json.loads(line) for line in capsys.readouterr().out.splitlines() if '"catalog totals"' in line]
    assert (totals["merged_files"], totals["merge_ms"]) == (39, 21)
