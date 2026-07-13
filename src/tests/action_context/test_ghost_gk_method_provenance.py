"""The selected kde_backend reaches add_ghost_gk (computation), and ghost_gk_method == that backend.

This is the anti-drift guard for the ghost_gk_method provenance column: a spy on the SOURCE add_ghost_gk
proves the chosen backend reaches the COMPUTATION (not just the label), and the column assertion proves the
label matches. Reuses the proven test_mini_golden recompute path on the IDSSE mini fixture.
"""

from __future__ import annotations

import pandas as pd

_ROOT = "src/tests/fixtures/action_context"


def _recompute(kde_backend: str) -> pd.DataFrame:
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

        def write(self, wu, result_df):
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id="J03WMXmini", period=1, kde_backend=kde_backend),
        frames=ParquetFrameSource(_ROOT),
        actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT),
        meta=ParquetMatchMetadataSource(_ROOT),
        sink=sink,
        is_slice=True,  # ADR-067: fixture = windowed frames + whole-match actions
    )
    assert sink.df is not None
    return sink.df


def test_backend_reaches_computation_and_label(monkeypatch):
    # We patch the SOURCE module (silly_kicks.tracking.features), NOT `enrich.add_ghost_gk`, because
    # enrich.py imports add_ghost_gk FUNCTION-LOCALLY (a per-call `from ... import add_ghost_gk` inside
    # _enrich_tracking_match / _enrich_sb360_match) — there is no module-level enrich.add_ghost_gk to patch.
    # If a future refactor hoists that import to module scope, this patch silently misses (`seen` stays
    # empty -> "got [] expected all 'fft'"); patch `analytics.action_context.enrich.add_ghost_gk` instead.
    import silly_kicks.tracking.features as skf

    seen: list[str | None] = []
    real = skf.add_ghost_gk

    def spy(*args, **kwargs):
        seen.append(kwargs.get("kde_backend"))
        return real(*args, **kwargs)

    monkeypatch.setattr(skf, "add_ghost_gk", spy)
    result = _recompute("fft")  # non-default (default fft-cic) -> proves selection; fast-approx -> quick

    assert seen and all(b == "fft" for b in seen), f"add_ghost_gk got {seen}, expected all 'fft'"
    assert (result["ghost_gk_method"] == "fft").all(), "ghost_gk_method label != selected backend"
