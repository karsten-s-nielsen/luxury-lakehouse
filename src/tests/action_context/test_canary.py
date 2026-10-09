"""Unit tests for the in-preflight drain canary (ADR-087, two-probe bounded + scope-aware)."""

from __future__ import annotations

import logging

import pytest

from analytics.action_context.canary import (
    CanaryProbes,
    WindowRef,
    run_canary,
    select_canary_probes,
)
from analytics.action_context.work_unit import WorkUnit


def _u(provider: str, mid: str, period: int | None = 1, n: int | None = None) -> WorkUnit:
    return WorkUnit(provider=provider, match_id=mid, period=period, n_frames=n)


# ── selection ───────────────────────────────────────────────────────────────────────────────────


def test_probe_c_picks_smallest_unit_per_provider() -> None:
    units = [
        _u("skillcorner", "A", 1, n=900),
        _u("skillcorner", "B", 1, n=300),  # smallest skillcorner
        _u("idsse", "J", 1, n=500),
        _u("idsse", "K", 1, n=800),  # J smaller
    ]
    probes = select_canary_probes(units)
    got = {(u.provider, u.match_id) for u in probes.correctness_units}
    assert got == {("skillcorner", "B"), ("idsse", "J")}  # smallest of each provider
    assert probes.memory_window is None  # no window_counts → no Probe M


def test_probe_m_picks_global_densest_window_even_in_non_densest_unit() -> None:
    # CANARY-SPEC-01 regression: the densest WINDOW lives in a unit that is NOT the densest unit.
    small_unit = _u("skillcorner", "SMALL", 1, n=1000)
    big_unit = _u("idsse", "BIG", 1, n=5000)  # denser UNIT overall
    window_counts = [
        WindowRef(big_unit, window_id=0, n_rows=2000),
        WindowRef(big_unit, window_id=1, n_rows=2000),  # big unit: many mid windows
        WindowRef(small_unit, window_id=0, n_rows=3500),  # ONE very dense window in the small unit
    ]
    probes = select_canary_probes([small_unit, big_unit], window_counts)
    assert probes.memory_window is not None
    assert (probes.memory_window.unit.match_id, probes.memory_window.window_id) == ("SMALL", 0)
    assert probes.memory_window.n_rows == 3500


def test_selection_is_deterministic_on_ties() -> None:
    a = _u("idsse", "A", 1, n=100)
    b = _u("idsse", "B", 1, n=100)  # same size → tie-break on match_id
    assert select_canary_probes([b, a]).correctness_units[0].match_id == "A"
    wc = [WindowRef(a, 0, 50), WindowRef(b, 0, 50)]  # same n_rows → tie-break identity
    mw = select_canary_probes([a, b], wc).memory_window
    assert mw is not None and mw.unit.match_id == "A"


def test_empty_units_and_empty_windows() -> None:
    assert select_canary_probes([]) == CanaryProbes(memory_window=None, correctness_units=())
    assert select_canary_probes([_u("idsse", "J", 1, n=10)], []).memory_window is None


# ── run_canary ──────────────────────────────────────────────────────────────────────────────────


class _OkProcessor:
    def __init__(self) -> None:
        self.processed: list[tuple[str, str, bool]] = []
        self.built: list[tuple[str, int]] = []

    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        self.processed.append((unit.provider, unit.match_id, dry_run))
        return 0

    def build_fit_probe(self, unit: WorkUnit, window_id: int) -> int:
        self.built.append((unit.match_id, window_id))
        return 0


class _NoBuildProcessor:
    """A Probe-C-only processor (the AC SparkGameProcessor shape) — no build_fit_probe."""

    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        return 0


def test_run_canary_probe_c_only_uses_dry_run() -> None:
    proc = _OkProcessor()
    probes = select_canary_probes([_u("idsse", "J", 1, n=5), _u("idsse", "K", 1, n=9)])
    run_canary(proc, probes, logging.getLogger("t"))
    assert proc.processed == [("idsse", "J", True)]  # smallest idsse unit, dry_run
    assert proc.built == []  # no Probe M


def test_run_canary_runs_probe_m_when_window_present() -> None:
    proc = _OkProcessor()
    u = _u("idsse", "J", 1, n=5)
    probes = select_canary_probes([u], [WindowRef(u, 2, 999)])
    run_canary(proc, probes, logging.getLogger("t"))
    assert proc.processed == [("idsse", "J", True)]
    assert proc.built == [("J", 2)]


def test_run_canary_raises_on_probe_c_failure() -> None:
    class _FailC:
        def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
            raise ValueError(f"boom {unit.match_id}")

    probes = select_canary_probes([_u("idsse", "J", 1, n=5)])
    with pytest.raises(RuntimeError, match="canary FAILED \\(correctness\\) on idsse:J:1"):
        run_canary(_FailC(), probes, logging.getLogger("t"))


def test_run_canary_raises_on_probe_m_failure() -> None:
    class _FailM(_OkProcessor):
        def build_fit_probe(self, unit: WorkUnit, window_id: int) -> int:
            raise ValueError("build boom")

    u = _u("idsse", "J", 1, n=5)
    probes = select_canary_probes([u], [WindowRef(u, 0, 7)])
    with pytest.raises(RuntimeError, match="canary FAILED \\(memory-fit\\)"):
        run_canary(_FailM(), probes, logging.getLogger("t"))


def test_run_canary_raises_if_probe_m_selected_but_processor_lacks_build_fit_probe() -> None:
    u = _u("idsse", "J", 1, n=5)
    probes = select_canary_probes([u], [WindowRef(u, 0, 7)])
    with pytest.raises(RuntimeError, match="no build_fit_probe"):
        run_canary(_NoBuildProcessor(), probes, logging.getLogger("t"))


def test_run_canary_empty_probes_is_noop() -> None:
    proc = _OkProcessor()
    run_canary(proc, select_canary_probes([]), logging.getLogger("t"))
    assert proc.processed == []
    assert proc.built == []
