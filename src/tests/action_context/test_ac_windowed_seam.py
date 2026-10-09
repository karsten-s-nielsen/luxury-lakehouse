"""T7-B — AC drain haloed frame-window seam (ADR-089, spec §4.5c).

The AC drain was frame-batched halo-less (``frame_batch_id = floor(frame / size)``); each
``enrich_batch`` group saw only its own frames, so sk's composed savgol velocity (reach ``H =
(window_frames-1) + max_gap_frames``) was DEGRADED within ``H`` of every batch boundary. The seam
replaces that with the dense-ordinal ``_window_id`` + a halo: each window builds on ``core+halo``
so boundary velocity is CONTINUOUS, while each ACTION is still enriched in EXACTLY ONE window
(single-owner). This is a VALUE CHANGE at former batch boundaries, neutral in the interior.

Two oracles, each isolating a different property (window-dependent features — obso_peak,
elastic_sync, sync_score — are batch-size-scoped BY DESIGN per ADR-047, so the whole-unit build is
NOT the interior oracle; the same-batch halo-off run is):
  * **halo on vs off at the SAME target** — interior actions byte-identical (the halo does not
    reach them), boundary actions changed (the correction). ``halo=0`` == the current pre-seam
    halo-less floor batching (for aligned IDSSE the dense-ordinal and raw-floor windows coincide).
  * **single-window enrich (``target`` >> unit)** — full velocity context; a boundary action moves
    TOWARD it with the halo and AWAY without, proving the halo is load-bearing (not vacuous).

Driven through the real local hexagon (``run_work_unit`` -> ``enrich_batch``) on the committed
IDSSE mini fixture (gap-free dense, F0=11500 a multiple of 250 so ordinal and raw-floor windows
align), exactly as ``scripts/build_ac1_mini_golden.py`` freezes the golden.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path("src/tests/fixtures/action_context")
_PROVIDER = "idsse"
_MATCH = "J03WMXmini"
_PERIOD = 1


def _run(target: int, monkeypatch: pytest.MonkeyPatch | None = None, *, halo: int | None = None) -> pd.DataFrame:
    """Run the full local hexagon on the mini fixture with ``AC_FRAME_BATCH_SIZE=target``.

    ``halo`` (when given) monkeypatches ``frame_windows.halo_frames`` to that constant so the
    halo-less (``halo=0``) degraded result can be compared against the haloed correction.
    """
    from analytics.action_context import frame_windows
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    if monkeypatch is not None:
        monkeypatch.setenv("AC_FRAME_BATCH_SIZE", str(target))
        if halo is not None:
            monkeypatch.setattr(frame_windows, "halo_frames", lambda *_a, **_k: halo)

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider=_PROVIDER, match_id=_MATCH, period=_PERIOD),
        frames=ParquetFrameSource(str(_ROOT)),
        actions=ParquetActionsSource(str(_ROOT)),
        xt=ParquetXtSource(str(_ROOT)),
        meta=ParquetMatchMetadataSource(str(_ROOT)),
        sink=sink,
        is_slice=True,
    )
    assert sink.df is not None
    return sink.df.sort_values("action_id").reset_index(drop=True)


def _numeric_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c != "action_id" and pd.api.types.is_numeric_dtype(df[c])]


def _to_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce nullable numeric columns to float64 (``<NA>`` -> ``np.nan``) so comparisons /
    ``to_numpy(float)`` don't choke on ``NAType``."""
    return df[cols].apply(lambda s: pd.to_numeric(s, errors="coerce").astype("float64"))


def _action_ordinals() -> tuple[np.ndarray, dict[int, int]]:
    """(sorted distinct frames, {action_id: frame ordinal}) via the global anchor — the SAME
    frame<->time line the dispatcher uses for single-owner ownership."""
    from analytics.action_context.pipeline import compute_ownership_anchors

    frames = pd.read_parquet(_ROOT / _PROVIDER / f"{_MATCH}_p{_PERIOD}" / "frames.parquet")
    actions = pd.read_parquet(_ROOT / _PROVIDER / f"{_MATCH}_p{_PERIOD}" / "actions.parquet")
    actions = actions[actions["period_id"] == _PERIOD]
    sorted_frames = np.sort(frames["frame"].dropna().unique()).astype(float)
    t0, f0, slope = compute_ownership_anchors(frames, "frame")[_PERIOD]
    ords: dict[int, int] = {}
    for _, a in actions.iterrows():
        est = f0 + (float(a["time_seconds"]) - t0) * slope
        ords[int(a["action_id"])] = int(np.searchsorted(sorted_frames, est, side="left"))
    return sorted_frames, ords


def test_single_owner_each_action_enriched_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every action is enriched in EXACTLY one window — the action set equals the single-window
    oracle's (none dropped by the halo trim, none double-counted), and is unique."""
    oracle = _run(10_000, monkeypatch)
    windowed = _run(100, monkeypatch)
    assert not windowed["action_id"].duplicated().any()
    assert set(windowed["action_id"]) == set(oracle["action_id"])


def test_interior_action_unchanged_by_halo(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-boundary action does not move (spec R1: the value change is isolated to the band).

    Oracle = the SAME batch size with the halo OFF (``halo=0`` == the current pre-seam halo-less
    floor batching — for aligned IDSSE the dense-ordinal and raw-floor windows coincide), NOT the
    whole-unit build (window-dependent features — obso_peak/elastic_sync/sync_score — are
    batch-size-scoped BY DESIGN per ADR-047). The affected band is the deepest WINDOW-DEPENDENT
    feature reach (the backward-looking actor_pre_window / DAS-carrier hysteresis, wider than the
    sk-preprocess velocity ``H``), so an action must be FAR from a boundary to be unaffected: the
    action deepest in its window's interior is byte-identical halo-on vs halo-off. The boundary
    correction itself (near-boundary actions DO shift, toward the full-context value) is proven
    non-vacuously by ``test_halo_corrects_boundary_velocity_non_vacuous``.
    """
    target = 250
    _frames, ords = _action_ordinals()
    far = max(ords, key=lambda a: min(ords[a] % target, target - (ords[a] % target)))

    haloed = _run(target, monkeypatch).set_index("action_id")
    halo_less = _run(target, monkeypatch, halo=0).set_index("action_id")
    cols = _numeric_cols(haloed.reset_index())
    pd.testing.assert_frame_equal(
        _to_float(haloed.loc[[far]], cols),
        _to_float(halo_less.loc[[far]], cols),
        check_dtype=False,
        rtol=1e-6,
        atol=1e-6,
    )


def test_halo_corrects_boundary_velocity_non_vacuous(monkeypatch: pytest.MonkeyPatch) -> None:
    """A boundary action's enrichment moves TOWARD the single-window oracle with the halo and
    AWAY from it without one — proving the halo is load-bearing (the §4.5c correction), not that
    the windowing happens to be a no-op on this fixture."""
    _frames, ords = _action_ordinals()
    # Place a window boundary exactly at the middle action's ordinal -> that action sits at a
    # window start (pos 0): halo-less it has no left context, haloed it has H frames of it.
    mid = sorted(ords.values())[len(ords) // 2]
    target = mid
    if target < 2 or target >= len(_frames) - 1:
        pytest.skip("fixture action layout unsuitable for a boundary placement")

    boundary_action = next(aid for aid, o in ords.items() if o == mid)
    oracle = _run(10_000, monkeypatch).set_index("action_id")
    haloed = _run(target, monkeypatch).set_index("action_id")
    halo_less = _run(target, monkeypatch, halo=0).set_index("action_id")

    cols = _numeric_cols(oracle.reset_index())

    def _row(df: pd.DataFrame) -> np.ndarray:
        return _to_float(df.loc[[boundary_action]], cols).to_numpy(dtype=float)[0]

    ref = _row(oracle)
    d_haloed = np.nansum(np.abs(_row(haloed) - ref))
    d_halo_less = np.nansum(np.abs(_row(halo_less) - ref))

    assert d_halo_less > 0.0, "halo-less result already matches the oracle — boundary test vacuous"
    assert d_haloed < d_halo_less, f"halo did not correct the boundary action ({d_haloed} !< {d_halo_less})"


def test_ac_seam_in_both_dispatchers() -> None:
    """Lockstep (ADR-089): BOTH the local hexagon (``run_work_unit``) and the Spark driver
    (``_process_tracking_match``) wire the dense-ordinal window seam + the distinct-frame ordinal
    map, or whichever drifts re-grows the halo-less batch-boundary velocity defect (spec §4.5c)."""
    import inspect

    from analytics.action_context import pipeline
    from ingestion import action_context

    local_src = inspect.getsource(pipeline.run_work_unit)
    assert "assign_window_ids" in local_src
    assert "compute_sorted_frames" in local_src
    assert "SEAM_PROVIDERS" in local_src

    spark_src = inspect.getsource(action_context._process_tracking_match)
    assert "assign_frame_windows" in spark_src
    assert "_sorted_frames_by_period" in spark_src
    assert "SEAM_PROVIDERS" in spark_src
