"""The tracking-compute profiler's phase summary (perf plan Task 1; spec §6 step 1)."""

from __future__ import annotations

import cProfile
import importlib.util
import pstats
import time
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import pytest

V2_ROOT = Path(__file__).resolve().parents[2]


def _load_profiler() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "v2_profile_tracking_compute", V2_ROOT / "scripts" / "profile_tracking_compute.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def b() -> None:
    # Spin, never `time.sleep`: Taipy's server (taipy/gui/server.py:283) runs `gevent.monkey.patch_time()` in the
    # pytest process during the integration tests, and a patched sleep is a greenlet switch that cProfile books to
    # the caller, so a sleep-based `b` reads ~0 s in a full run.
    started = time.perf_counter()
    while time.perf_counter() - started < 0.01:
        pass


def a() -> None:
    b()


def test_summarise_profile_sums_named_functions() -> None:
    profiler = cProfile.Profile()
    profiler.enable()
    a()
    profiler.disable()
    stats = pstats.Stats(profiler)

    result = _load_profiler().summarise_profile(stats, {"x": ("b",)})

    assert set(result) == {"x"}
    assert result["x"] >= 0.01


def test_summarise_profile_subtracts_a_child_called_from_its_parent() -> None:
    """`-child<parent` removes only the child's time spent under that parent ("combine net of its influence")."""
    profiler = cProfile.Profile()
    profiler.enable()
    a()
    profiler.disable()
    stats = pstats.Stats(profiler)

    result = _load_profiler().summarise_profile(stats, {"gross": ("a",), "net": ("a", "-b<a")})

    assert result["gross"] >= 0.01
    assert 0.0 <= result["net"] < 0.005


def test_shape_phase_counts_nested_shape_graphs_once() -> None:
    """`infer_positions` calls `compute_shape_graph` again (via `_decompose_middle`), so summing both cumulative times
    counts the nested calls twice: the Mac GS profile reported a `shape` phase of 6207 s inside a 4858 s step. The
    phase must never exceed the time of the work that contains it."""
    import numpy as np
    from silly_kicks.tracking._shape_graph import (  # pyright: ignore[reportMissingTypeStubs]
        compute_shape_graph,
        infer_positions,
    )

    rng = np.random.default_rng(0)
    frames = [rng.uniform(0, 100, size=(10, 2)) for _ in range(200)]

    def work() -> None:
        for positions in frames:
            graph = compute_shape_graph(positions)
            list(infer_positions(graph, positions, 1.0))

    profiler = cProfile.Profile()
    profiler.enable()
    work()
    profiler.disable()
    stats = pstats.Stats(profiler)
    module = _load_profiler()
    whole = module.summarise_profile(stats, {"work": ("work",)})["work"]

    shape = module.summarise_profile(stats, module.PHASES)["shape"]

    assert 0.0 < shape <= whole * 1.01, (shape, whole)


@pytest.mark.parametrize(("active", "expected"), [(True, "numba"), (False, "numpy")])
def test_profile_reports_the_backend_from_the_one_backend_reader(
    monkeypatch: pytest.MonkeyPatch, active: bool, expected: str
) -> None:
    # `lakehouse.compute.backend.numba_active` is the one reader of the private silly-kicks flag (backend.py).
    def no_work(*_args: object) -> Callable[[], int]:
        return lambda: 0

    module = _load_profiler()
    monkeypatch.setattr(module, "_work", no_work)
    monkeypatch.setattr(module, "numba_active", lambda: active, raising=False)

    result = module.profile_step("pitch_control", object(), 1, "idsse")

    assert result["backend"] == expected
