"""The run's compute phase: layered build/compute alternation + `run_compute` orchestration (spec D2/D8/D9; Task 1).

A calc run does a full dbt build, then **layer-1** compute steps (spadl_actions, pitch_control, shape_graphs) writing
bronze, then a `+fct_actions` build, then the **layer-2** step (line_breaking) that reads `fct_actions`, then a
`+fct_tracking_metrics` build that repopulates the staging + mart views. A run with no compute steps is today's single
build, unchanged. `run_compute(layer=N)` runs only the steps of that layer; a failing step (or a step's failing match)
is recorded via the run's ErrorSink and the rest still run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

import lakehouse.runs.run as run_module
from lakehouse.compute.base import COMPUTE_L1_SELECT, COMPUTE_MART_SELECT, ComputeOutcome, run_compute
from lakehouse.config.model import ResourceCaps
from lakehouse.runs.dbt import DbtResult
from lakehouse.runs.run import RunResult, run_ingestion

if TYPE_CHECKING:
    from lakehouse.catalog.working import WorkingCatalog

MODULE = "idsse_public"
CAPS = ResourceCaps()


def _drop_error(**kwargs: Any) -> None:
    """A typed no-op ErrorSink for tests that do not assert on recorded errors (a bare lambda is untyped)."""
    return None


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


def _dbt_recorder(order: list[Any]) -> Any:
    def dbt(cat: Any, *, run_id: str, caps: Any, select: str | None = None) -> DbtResult:
        order.append(("dbt", select))
        return DbtResult(success=True, results=[], run_results_key=f"runs/dbt/{run_id}/run_results.json")

    return dbt


def _wrap(order: list[Any], label: str, real: Any) -> Any:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        order.append(label)
        return real(*args, **kwargs)

    return wrapped


class _RecordingStep:
    """A fake compute step that records `("compute", layer)` when it runs (spec D9 layers)."""

    output_table = "recording_values"
    input_select = "+fct_tracking_frames"

    def __init__(self, order: list[Any], *, layer: int) -> None:
        self._order = order
        self.layer = layer
        self.name = f"recording_l{layer}"

    def __call__(
        self, cat: Any, *, run_id: str, caps: Any, now: Any, error: Any, control: Any = None
    ) -> ComputeOutcome:
        self._order.append(("compute", self.layer))
        return ComputeOutcome(step=self.name, matches=0, rows=0, empty_matches=0, failed_matches=0)


def test_finish_runs_layered_build_compute_alternation(kit: LakeKit, monkeypatch: Any) -> None:
    # spec D9: full build -> layer-1 compute -> +fct_actions -> layer-2 compute -> +fct_tracking_metrics
    order: list[Any] = []
    monkeypatch.setattr(run_module, "run_resolution", _wrap(order, "resolve", run_module.run_resolution))
    monkeypatch.setattr(run_module, "maintain_tables", _wrap(order, "maintain", run_module.maintain_tables))
    monkeypatch.setattr(run_module, "run_cleanup", _wrap(order, "cleanup", run_module.run_cleanup))
    result = _run(
        kit, dbt=_dbt_recorder(order), compute=[_RecordingStep(order, layer=1), _RecordingStep(order, layer=2)]
    )
    assert result.status == "ok"
    assert order == [
        "resolve",
        ("dbt", None),
        ("compute", 1),
        ("dbt", COMPUTE_L1_SELECT),
        ("compute", 2),
        ("dbt", COMPUTE_MART_SELECT),
        "maintain",
        "cleanup",
    ]


def test_layer2_build_runs_after_layer1_compute(kit: LakeKit) -> None:
    # fct_actions (the +fct_actions middle build) is built only after the layer-1 compute wrote spadl_actions bronze
    order: list[Any] = []
    _run(kit, dbt=_dbt_recorder(order), compute=[_RecordingStep(order, layer=1), _RecordingStep(order, layer=2)])
    assert order.index(("compute", 1)) < order.index(("dbt", COMPUTE_L1_SELECT)) < order.index(("compute", 2))


def test_empty_compute_is_a_single_unselected_build(kit: LakeKit) -> None:
    order: list[Any] = []
    result = _run(kit, dbt=_dbt_recorder(order), compute=[])
    assert result.status == "ok"
    assert order == [("dbt", None)]


def test_run_compute_runs_only_the_requested_layer() -> None:
    seen: list[str] = []

    def mk(step_name: str, step_layer: int) -> Any:
        class Step:
            name, output_table, input_select, layer = step_name, "t", "", step_layer

            def __call__(
                self, cat: Any, *, run_id: str, caps: Any, now: Any, error: Any, control: Any = None
            ) -> ComputeOutcome:
                seen.append(step_name)
                return ComputeOutcome(step=step_name, matches=0, rows=0, empty_matches=0, failed_matches=0)

        return Step()

    steps = [mk("a1", 1), mk("b2", 2), mk("c1", 1)]
    run_compute(
        cast("WorkingCatalog", None),
        run_id="r",
        caps=CAPS,
        now=lambda: NOW,
        steps=steps,
        layer=1,
        error=_drop_error,
    )
    assert seen == ["a1", "c1"]  # only the layer-1 steps ran


def test_run_compute_records_a_failing_step_and_runs_the_rest() -> None:
    recorded: list[dict[str, Any]] = []

    def error(**kwargs: Any) -> None:
        recorded.append(kwargs)

    class Boom:
        name, output_table, input_select, layer = "boom", "x", "", 1

        def __call__(
            self, cat: Any, *, run_id: str, caps: Any, now: Any, error: Any, control: Any = None
        ) -> ComputeOutcome:
            raise RuntimeError("kaboom")

    class Fine:
        name, output_table, input_select, layer = "fine", "y", "", 1

        def __call__(
            self, cat: Any, *, run_id: str, caps: Any, now: Any, error: Any, control: Any = None
        ) -> ComputeOutcome:
            return ComputeOutcome(step="fine", matches=1, rows=1, empty_matches=0, failed_matches=0)

    result = run_compute(
        cast("WorkingCatalog", None),
        run_id="r",
        caps=CAPS,
        now=lambda: NOW,
        steps=[Boom(), Fine()],
        layer=1,
        error=error,
    )
    assert [o.step for o in result.outcomes] == ["fine"]
    assert len(recorded) == 1
    assert recorded[0]["kind"] == "compute_failed"
    assert recorded[0]["severity"] == "error"
    assert "boom" in recorded[0]["message"]
