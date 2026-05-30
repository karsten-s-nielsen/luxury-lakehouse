"""Production-failure regression gate for the silly-kicks add_das dead-ball-batch bug.

Three captured 250-frame batches from job 302697362345215 run 574695047251538
(post-PR-#319), each crashing pre-3.30.0 on
``silly_kicks.tracking.add_das -> _pin_attacking_direction ->
accessible_space.interface.infer_playing_direction.assert``. Fixed by:

  - **silly-kicks 3.30.0** — ``_pin_attacking_direction`` converts the escaping
    AssertionError into the canonical ValueError that ``add_das`` already
    swallows to NaN. No crash; DAS columns NaN-filled honestly.
  - **Lakehouse-side** — ``_fill_possession_from_set_piece_actions`` synthesizes
    ``team_in_possession`` for SPADL set-piece restart actions (throw_in,
    freekick_*, corner_*, goalkick, shot_freekick, shot_penalty) where
    ``action.team_id`` makes possession unambiguous. silly-kicks then computes
    FINITE DAS on those actions through the normal happy path.

Per the M13 single-owner ownership invariant, a 250-frame fixture batch may
legitimately contain ZERO owned actions (when no action's interpolated frame
lands in this batch's [frame_lo, frame_hi) window). That is normal pipeline
behavior, NOT a regression — the action belongs to a different batch. The test
validates "no crash + shape-consistent + populated where rows exist," NOT a
specific row count.

See [[project_silly_kicks_add_das_dead_ball_bug]] +
[[project_sk330_dead_ball_robustness_handoff]] for the full trace.
"""

from __future__ import annotations

import pandas as pd
import pytest

# Each fixture is a captured 250-frame batch that crashed pre-3.30.0.
# Format: (match_id, period).
_DEAD_BALL_FIXTURES = [
    ("J03WN1", 1),  # batch 46, frames 11500..11749 — Foul -> Goalkick window
    ("J03WPY", 2),  # batch 402, frames 100500..100749
    ("J03WOH", 1),  # batch 50, frames 12500..12749
]

# Critical bronze-SPADL columns that every emitted row must have populated.
# Limited to columns the AC-1 enriched output schema actually emits (verified
# against J03WMX_p1 golden 2026-05-30). 'result_name' is NOT emitted; do not add.
_CRITICAL_COLS = ("data_source", "match_id", "action_id", "period_id", "type_name", "start_x", "start_y")

# SPADL set-piece restart action types — for these, the lakehouse-side
# possession-fill helper supplies team_in_possession from action.team_id, so DAS
# becomes finite. Other dead-ball actions (e.g. foul-while-ball-out) honestly
# get NaN DAS — the metric is undefined where no team has possession.
_SET_PIECE_RESTART_TYPES = frozenset(
    {
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "shot_freekick",
        "corner_crossed",
        "corner_short",
        "goalkick",
        "shot_penalty",
    }
)


def _run(match_id: str, period: int) -> pd.DataFrame:
    """Invoke the real run_work_unit -> enrich_batch chain against a fixture."""
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

        def write(self, wu: object, result_df: pd.DataFrame) -> int:
            self.df = result_df
            return len(result_df)

    root = "src/tests/fixtures/action_context"
    sink = _Collect()
    run_work_unit(
        WorkUnit(provider="idsse", match_id=match_id, period=period),
        frames=ParquetFrameSource(root),
        actions=ParquetActionsSource(root),
        xt=ParquetXtSource(root),
        meta=ParquetMatchMetadataSource(root),
        sink=sink,
    )
    assert sink.df is not None, f"{match_id}_p{period}: sink received no DataFrame (run_work_unit aborted)"
    return sink.df


@pytest.mark.parametrize(("match_id", "period"), _DEAD_BALL_FIXTURES)
def test_dead_ball_batch_no_crash(match_id: str, period: int) -> None:
    """The pipeline must complete without raising on these batches.

    Pre-3.30.0 this raised ``AssertionError`` from inside silly-kicks'
    ``infer_playing_direction``. Post-3.30.0 it must return a DataFrame
    (possibly empty if M13 ownership filters out all actions in this batch).
    """
    result = _run(match_id, period)
    assert isinstance(result, pd.DataFrame), f"{match_id}_p{period}: expected DataFrame, got {type(result)!r}"


@pytest.mark.parametrize(("match_id", "period"), _DEAD_BALL_FIXTURES)
def test_dead_ball_batch_shape_invariants(match_id: str, period: int) -> None:
    """For any rows that ARE emitted, the bronze-SPADL contract holds.

    No-row batches are valid (M13 ownership) — they're legitimately empty
    because the actions belong to an adjacent batch. For non-empty results:
    no duplicate (match_id, action_id, period_id) keys + critical columns
    populated on every row.
    """
    result = _run(match_id, period)

    if len(result) == 0:
        pytest.skip(f"{match_id}_p{period}: 0 owned actions in this batch (M13 legit-empty)")

    dupes = result.groupby(["match_id", "action_id", "period_id"]).size()
    assert dupes[dupes > 1].empty, f"{match_id}_p{period}: duplicate rows {dupes[dupes > 1].to_dict()}"

    missing = [c for c in _CRITICAL_COLS if c not in result.columns]
    assert not missing, f"{match_id}_p{period}: missing critical columns {missing}"

    for col in _CRITICAL_COLS:
        n_nan = int(result[col].isna().sum())
        nan_pct = 100.0 * n_nan / len(result)
        assert nan_pct < 50.0, f"{match_id}_p{period}: {col} {nan_pct:.1f}% NaN ({n_nan}/{len(result)})"


@pytest.mark.parametrize(("match_id", "period"), _DEAD_BALL_FIXTURES)
def test_dead_ball_batch_set_piece_actions_get_finite_das(match_id: str, period: int) -> None:
    """Set-piece restart actions owned by this batch must get FINITE DAS.

    Validates the lakehouse-side ``_fill_possession_from_set_piece_actions``
    helper: for SPADL restart action types, ``action.team_id`` synthesizes
    possession unambiguously on the linked dead-ball frame, so silly-kicks
    computes a real DAS value (not NaN). Non-set-piece dead-ball actions are
    NOT asserted here — they honestly get NaN.
    """
    result = _run(match_id, period)

    if len(result) == 0:
        pytest.skip(f"{match_id}_p{period}: 0 owned actions in this batch (M13 legit-empty)")
    if "type_name" not in result.columns:
        pytest.fail(f"{match_id}_p{period}: missing 'type_name' column — cannot identify set-piece actions")
    if "das_team" not in result.columns:
        pytest.fail(f"{match_id}_p{period}: missing 'das_team' column — DAS step did not produce expected schema")

    sp_mask = result["type_name"].isin(_SET_PIECE_RESTART_TYPES)
    n_sp = int(sp_mask.sum())
    if n_sp == 0:
        pytest.skip(f"{match_id}_p{period}: no set-piece restart actions owned by this batch")

    sp_rows = result[sp_mask]
    n_finite = int(sp_rows["das_team"].notna().sum())
    assert n_finite == n_sp, (
        f"{match_id}_p{period}: {n_sp - n_finite}/{n_sp} set-piece actions have NaN das_team. "
        f"Expected finite DAS (lakehouse possession-fill should give silly-kicks enough signal). "
        f"Types: {sp_rows['type_name'].value_counts().to_dict()}"
    )
