"""Pure-function tests for the ADR-063 xT-grid watermark guard + materiality gate.

These exercise the guard's DECISION logic without Spark (review L10 — mock-heavy Spark-chain tests on
the exact code whose silent failure caused the bug give false confidence; test the pure functions instead).
"""

from __future__ import annotations

import numpy as np

from ingestion.expected_threat import _decide_rebuild, _grid_drift


class TestDecideRebuild:
    def test_upstream_changed_rebuilds_all_comps_and_global(self) -> None:
        comps, need_global = _decide_rebuild(
            find_new=["7"], all_comps=["7", "37", "0"], upstream_changed=True, global_exists=True
        )
        assert need_global is True  # upstream re-derived → global stale even though it "exists"
        assert comps == ["0", "37", "7"]  # ALL comps, not just the new one

    def test_no_change_builds_only_new_comps(self) -> None:
        comps, need_global = _decide_rebuild(find_new=["99"], all_comps=[], upstream_changed=False, global_exists=True)
        assert comps == ["99"]
        assert need_global is False

    def test_no_change_missing_global_rebuilds_global_only(self) -> None:
        comps, need_global = _decide_rebuild(find_new=[], all_comps=[], upstream_changed=False, global_exists=False)
        assert comps == []
        assert need_global is True  # global absent → first build

    def test_nothing_to_do(self) -> None:
        comps, need_global = _decide_rebuild(find_new=[], all_comps=[], upstream_changed=False, global_exists=True)
        assert comps == []
        assert need_global is False


class TestGridDrift:
    def _g(self, vals: list[float]) -> np.ndarray:
        return np.repeat(np.array(vals, dtype=float)[:, None], 8, axis=1)

    def test_no_previous_is_material(self) -> None:
        assert _grid_drift(self._g([0.01, 0.02]), None) is None

    def test_identical_grids_zero_drift(self) -> None:
        g = self._g([0.01, 0.05, 0.20])
        assert _grid_drift(g, g.copy()) == 0.0

    def test_relative_change_above_floor(self) -> None:
        prev = self._g([0.10, 0.20])
        new = self._g([0.11, 0.20])  # +10% on the first cell
        drift = _grid_drift(new, prev)
        assert drift is not None
        assert abs(drift - 0.10) < 1e-9

    def test_below_floor_cells_ignored(self) -> None:
        # A huge relative swing on a sub-floor cell (0.001 -> 0.002) must NOT count as material.
        prev = self._g([0.001, 0.20])
        new = self._g([0.002, 0.20])  # +100% but below the 0.005 floor
        assert _grid_drift(new, prev) == 0.0

    def test_shape_mismatch_is_material(self) -> None:
        prev = self._g([0.10, 0.20])
        new = self._g([0.10, 0.20, 0.30])
        assert _grid_drift(new, prev) is None
