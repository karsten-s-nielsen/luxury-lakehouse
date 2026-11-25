"""Stop handling inside `finish` (perf spec §7.5 steps 4-5; plan Task 6).

A stop request (SIGTERM on a local scheduler, 60 s grace) that arrives during the compute or a dbt build makes the run
call `stopped()`, skip every later phase — the remaining dbt builds, compute layers, maintenance AND cleanup (today's
terminated path) — and go straight to the run row and the run-end commit, which publishes what was completed.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

import lakehouse.runs.run as run_module
from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute.base import COMPUTE_L1_SELECT, ComputeOutcome
from lakehouse.runs.dbt import DbtResult
from lakehouse.runs.run import RunResult, run_ingestion

MODULE = "idsse_public"


def _adapter() -> FakeAdapter:
    natives = ("J03WPY", "J03WMX")
    files = {(native, "events"): f"{native}-a\n{native}-b".encode() for native in natives}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in natives], files)


def _run(kit: LakeKit, flag: dict[str, bool], **kwargs: Any) -> RunResult:
    save_settings(kit.storage, [active(MODULE)])
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={MODULE: _adapter()},
        parsers={"idsse": FakeParser("idsse")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
        stop=lambda: flag["stop"],
        **kwargs,
    )


def _dbt(order: list[Any], flag: dict[str, bool], *, stop_on: str | None = None) -> Any:
    def dbt(cat: Any, *, run_id: str, caps: Any, select: str | None = None) -> DbtResult:
        order.append(("dbt", select))
        if stop_on is not None and select == stop_on:
            flag["stop"] = True  # the stop request arrives while this build runs
        return DbtResult(success=True, results=[], run_results_key=f"runs/dbt/{run_id}/run_results.json")

    return dbt


class _Step:
    output_table = "recording_values"
    input_select = "+fct_tracking_frames"

    def __init__(self, order: list[Any], flag: dict[str, bool], *, layer: int, stops: bool = False) -> None:
        self._order, self._flag, self._stops = order, flag, stops
        self.layer = layer
        self.name = f"recording_l{layer}"

    def __call__(self, cat: Any, *, run_id: str, caps: Any, now: Any, error: Any, control: Any) -> ComputeOutcome:
        self._order.append(("compute", self.layer))
        if self._stops:
            self._flag["stop"] = True  # the stop request arrives during this compute layer
        return ComputeOutcome(step=self.name, matches=0, rows=0, empty_matches=0, failed_matches=0)


def _wrap(order: list[Any], label: str, real: Any) -> Any:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        order.append(label)
        return real(*args, **kwargs)

    return wrapped


@pytest.fixture
def order(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    seen: list[Any] = []
    monkeypatch.setattr(run_module, "run_resolution", _wrap(seen, "resolve", run_module.run_resolution))
    monkeypatch.setattr(run_module, "maintain_tables", _wrap(seen, "maintain", run_module.maintain_tables))
    monkeypatch.setattr(run_module, "run_cleanup", _wrap(seen, "cleanup", run_module.run_cleanup))
    return seen


def _published_status(kit: LakeKit, run_id: str) -> tuple[str, str | None]:
    with CatalogReader(kit.storage).session() as conn:
        row = conn.execute("SELECT status, reason FROM lake.main.ops_runs WHERE run_id = ?", [run_id]).fetchone()
    assert row is not None, "the run row was not published by the run-end commit"
    return str(row[0]), row[1]


def test_stop_during_compute_skips_later_phases_and_cleanup(kit: LakeKit, order: list[Any]) -> None:
    flag = {"stop": False}
    steps = [_Step(order, flag, layer=1, stops=True), _Step(order, flag, layer=2)]
    result = _run(kit, flag, dbt=_dbt(order, flag), compute=steps)
    assert order == [
        "resolve",
        ("dbt", None),
        ("compute", 1),
    ]  # no middle build, no layer 2, no mart, no maintain/cleanup
    assert result.status == "failed"
    assert any(e.kind == "terminated" for e in result.errors)
    assert _published_status(kit, result.run_id) == ("failed", "terminated")  # the run-end commit still ran


def test_stop_during_dbt_build_skips_the_rest(kit: LakeKit, order: list[Any]) -> None:
    flag = {"stop": False}
    steps = [_Step(order, flag, layer=1), _Step(order, flag, layer=2)]
    result = _run(kit, flag, dbt=_dbt(order, flag, stop_on=COMPUTE_L1_SELECT), compute=steps)
    assert order == ["resolve", ("dbt", None), ("compute", 1), ("dbt", COMPUTE_L1_SELECT)]
    assert result.status == "failed"
    assert _published_status(kit, result.run_id) == ("failed", "terminated")


def test_compute_backend_logged_once(kit: LakeKit, order: list[Any], caplog: pytest.LogCaptureFixture) -> None:
    flag = {"stop": False}
    steps = [_Step(order, flag, layer=1), _Step(order, flag, layer=2)]
    with caplog.at_level(logging.INFO, logger="lakehouse.runs.run"):
        result = _run(kit, flag, dbt=_dbt(order, flag), compute=steps)
    assert result.status == "ok"
    backend = [r for r in caplog.records if r.getMessage() == "compute backend"]
    assert len(backend) == 1
    assert getattr(backend[0], "numba", None) is True
