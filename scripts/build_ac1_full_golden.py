"""Regenerate the full AC-1 golden (IDSSE J03WMX p1) via the real run_work_unit chain.

Companion to ``scripts/build_ac1_mini_golden.py`` — the full anchor (97 actions) had no
committed regen recipe (re-baselined manually via AC1_E2E=1 + freeze, ADR-036). This is the
durable, reviewable recipe. Runs the REAL ``run_work_unit`` -> ``enrich_batch`` chain on the
committed fixture (no Spark/Databricks) and freezes ``golden.parquet``.

Regenerate (after an INTENTIONAL, signed-off value change — e.g. a silly-kicks bump or a new
enrichment column) and commit the updated parquet:

    uv run python scripts/build_ac1_full_golden.py
    git add src/tests/fixtures/action_context/idsse/J03WMX_p1/golden.parquet

The KDE/pitch-control backends default to the production settings (ghost-GK fft-cic), so values
match serverless. ALWAYS review the column diff before trusting the freeze (capture-before-cleanup).
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

_ROOT = Path("src/tests/fixtures/action_context")
_DST = _ROOT / "idsse" / "J03WMX_p1" / "golden.parquet"


def main() -> None:
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
        WorkUnit(provider="idsse", match_id="J03WMX", period=1),
        frames=ParquetFrameSource(str(_ROOT)),
        actions=ParquetActionsSource(str(_ROOT)),
        xt=ParquetXtSource(str(_ROOT)),
        meta=ParquetMatchMetadataSource(str(_ROOT)),
        sink=sink,
    )
    if sink.df is None:
        msg = "pipeline produced no result"
        raise RuntimeError(msg)
    sink.df.to_parquet(_DST, index=False)
    print(
        f"recompute {time.perf_counter() - t0:.1f}s -> {len(sink.df)} rows x {len(sink.df.columns)} cols; froze {_DST}"
    )


if __name__ == "__main__":
    main()
