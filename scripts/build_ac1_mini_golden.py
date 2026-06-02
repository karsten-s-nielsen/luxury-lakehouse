"""Build the AC-1 mini-fixture + frozen mini-golden for the fast CI recompute gate.

Derives a tiny slice of the committed IDSSE ``J03WMX_p1`` fixture (no Databricks needed) and
runs the REAL ``run_work_unit`` -> ``enrich_batch`` to freeze a self-consistent mini-golden that
``src/tests/action_context/test_mini_golden.py`` recomputes-and-asserts in CI.

Why this gate exists
--------------------
``test_e2e.py`` is the only check that recomputes the real pipeline vs the golden, but it is
gated behind ``AC1_E2E=1`` (~5 min) and never runs in CI; ``test_differential.py`` runs in CI
but only *reads* the committed golden (DAS is a ``known_divergence`` it does not assert). So a
value shift (e.g. silly-kicks 4.2.0's DAS carrier-forwarding change, #328) could ride ``main``
uncaught. The mini-golden recompute closes that gap with a ~30s always-on test. See ADR-036.

Slice
-----
Frames in ``[TS_LO, TS_HI]`` s; actions in ``[ACT_LO, ACT_HI]`` s (a 3-action block whose every
action carries non-NaN DAS + ghost-GK, so a shift in either is caught). ~2-batch frame window
with margin for actor-pre-window + DAS carrier hysteresis. The golden is frozen from the same
slice, so it is self-consistent: window-edge truncation of cross-frame/cross-action deps is
irrelevant — a library/algorithm change diverges the RECOMPUTE from the FROZEN golden.

Regenerate (after an INTENTIONAL, signed-off value change) and commit the updated parquet:

    uv run python scripts/build_ac1_mini_golden.py
    git add src/tests/fixtures/action_context/idsse/J03WMXmini_p1/golden.parquet
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

_ROOT = Path("src/tests/fixtures/action_context")
_SRC = _ROOT / "idsse" / "J03WMX_p1"
_DST = _ROOT / "idsse" / "J03WMXmini_p1"

# Slice bounds (seconds). action_ids 33,34,35 (t=65.3,68.9,71.7) — all DAS-ok + ghost-ok.
TS_LO, TS_HI = 60.0, 77.0
ACT_LO, ACT_HI = 64.0, 73.0


def _write_slice() -> None:
    _DST.mkdir(parents=True, exist_ok=True)
    frames = pd.read_parquet(_SRC / "frames.parquet")
    actions = pd.read_parquet(_SRC / "actions.parquet")
    meta = pd.read_parquet(_SRC / "meta.parquet")

    mini_frames = frames[(frames["timestamp"] >= TS_LO) & (frames["timestamp"] <= TS_HI)].copy()
    mini_actions = actions[
        (actions["period_id"] == 1) & (actions["time_seconds"] >= ACT_LO) & (actions["time_seconds"] <= ACT_HI)
    ].copy()

    mini_frames.to_parquet(_DST / "frames.parquet", index=False)
    mini_actions.to_parquet(_DST / "actions.parquet", index=False)
    meta.to_parquet(_DST / "meta.parquet", index=False)
    print(f"mini frames: {len(mini_frames)} rows ({mini_frames['frame'].nunique()} frames)")
    print(f"mini actions: {len(mini_actions)} rows (period 1, t in [{ACT_LO}, {ACT_HI}])")


def _freeze_golden() -> None:
    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.work_unit import WorkUnit

    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    t0 = time.perf_counter()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1),
        frames=ParquetFrameSource(str(_ROOT)),
        actions=ParquetActionsSource(str(_ROOT)),
        xt=ParquetXtSource(str(_ROOT)),
        meta=ParquetMatchMetadataSource(str(_ROOT)),
        sink=sink,
    )
    elapsed = time.perf_counter() - t0
    if sink.df is None:
        msg = "pipeline produced no result"
        raise RuntimeError(msg)
    df = sink.df
    if not df["das_diff"].notna().all() or not df["ghost_gk_x"].notna().all():
        msg = "slice does not exercise DAS+ghost-GK on every action; adjust the window before freezing"
        raise RuntimeError(msg)
    df.to_parquet(_DST / "golden.parquet", index=False)
    print(f"recompute {elapsed:.1f}s -> {len(df)} rows x {len(df.columns)} cols; froze {_DST / 'golden.parquet'}")


def main() -> None:
    _write_slice()
    _freeze_golden()


if __name__ == "__main__":
    main()
