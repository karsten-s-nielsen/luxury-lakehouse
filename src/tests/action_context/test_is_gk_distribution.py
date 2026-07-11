"""F1 — ``is_gk_distribution`` GK-distribution domain marker (silly-kicks 4.43.0 ``gk_distribution_mask``).

Validates that BOTH action-context enrichment arms populate the non-nullable boolean domain marker
that silly-kicks' rho retention loader consumes on ``fct_action_context``:

  * tracking arm (FULL domain — ``gk_distribution_mask(actions, frames, resolve_gk="robust")``):
    asserted through the FROZEN full IDSSE golden (has goalkicks) — bool, never NULL, True on every
    goalkick. The always-on ``test_mini_golden`` additionally recomputes the tracking arm on the
    open-play mini slice (all-False), so recompute + True-path are both covered CI-cheaply.
  * SB360 arm (GOAL-KICKS-ONLY — ``gk_distribution_mask(actions, frames=None)``): recomputes the real
    ``run_work_unit`` on the committed ``statsbomb/3835328`` SB360 fixture and asserts the
    goal-kicks-only contract (no non-goalkick row is ever flagged — SB360 freeze-frames are
    shot-centric, so acting-GK open-play passes are deliberately undetectable on this arm).

See ``docs/superpowers/specs/2026-07-10-f1-gk-distribution-domain-design.md``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.pipeline import run_work_unit
from analytics.action_context.work_unit import WorkUnit

_ROOT = "src/tests/fixtures/action_context"
_FULL_GOLDEN = f"{_ROOT}/idsse/J03WMX_p1/golden.parquet"
_COL = "is_gk_distribution"


def _run(provider: str, match_id: str, period: int | None) -> pd.DataFrame:
    class _Collect:
        df: pd.DataFrame | None = None

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    sink = _Collect()
    run_work_unit(
        WorkUnit(provider=provider, match_id=match_id, period=period),
        frames=ParquetFrameSource(_ROOT),
        actions=ParquetActionsSource(_ROOT),
        xt=ParquetXtSource(_ROOT),
        meta=ParquetMatchMetadataSource(_ROOT),
        sink=sink,
    )
    assert sink.df is not None
    return sink.df


def _assert_boolean_non_null(col: pd.Series) -> None:
    """Producer contract: always True/False, never NULL (mask never emits NULL; both arms compute it)."""
    assert col.notna().all(), f"{_COL} has NULLs — the producer must always emit True/False"
    assert col.map(lambda v: isinstance(v, (bool, np.bool_))).all(), f"{_COL} is not a pure boolean column"


def test_full_golden_tracking_arm_domain() -> None:
    """Frozen full IDSSE golden (tracking arm, full domain): non-null bool, True on every goalkick.

    Reads the committed golden (CI-cheap). If ``is_gk_distribution`` is absent, the golden predates
    F1 — regenerate via ``scripts/build_ac1_full_golden.py`` and commit it in the same PR.
    """
    golden = pd.read_parquet(_FULL_GOLDEN)
    assert _COL in golden.columns, f"{_COL} missing from the full golden — regen build_ac1_full_golden.py"
    _assert_boolean_non_null(golden[_COL])

    goalkicks = golden[golden["type_name"] == "goalkick"]
    assert len(goalkicks) > 0, "full anchor lost its goalkick rows — re-extract before trusting this gate"
    assert bool(goalkicks[_COL].all()), "every goalkick must be True (is_gk_distribution goal-kick term)"


def test_sb360_arm_goalkicks_only() -> None:
    """SB360 arm (frames=None): non-null bool; goal-kicks-only — no non-goalkick row is ever flagged."""
    df = _run("statsbomb", "3835328", None)
    assert _COL in df.columns, f"{_COL} missing from the SB360 enrich output"
    _assert_boolean_non_null(df[_COL])

    is_gk = df[_COL].to_numpy(dtype=bool)
    is_goalkick = (df["type_name"] == "goalkick").to_numpy(dtype=bool)
    # Goal-kicks-only contract: every flagged row MUST be a goalkick (frames=None cannot detect the
    # acting-GK open-play-pass term). This is the honest SB360 coverage limit.
    assert int((is_gk & ~is_goalkick).sum()) == 0, "SB360 arm (frames=None) flagged a non-goalkick row"
    if is_goalkick.any():
        assert bool(is_gk[is_goalkick].all()), "every SB360 goalkick must be flagged True"
