"""Tests for src/ingestion/spadl_enrichments.py.

Layered test strategy (see PR-LL2 spec §Test plan):
    - Contract tests (TestContract): shape/dtype/empty/mutation
    - Plausibility tests (TestPlausibility): real-fixture sanity checks
    - Boundary-F1 test (TestBoundaryF1): heuristic vs StatsBomb native, F1≥0.85

silly-kicks owns the algorithm-level golden tests for the 3 helpers (verified
comprehensive: 597 LOC test_add_possessions.py + 438 LOC test_add_gk_role.py
+ 422 LOC test_add_pre_shot_gk_context.py in silly-kicks repo).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ingestion.spadl_enrichments import apply_spadl_enrichments

# silly-kicks action-type IDs (resolved via spadlconfig at silly-kicks 1.7.0+).
# We hardcode the canonical IDs here rather than importing silly-kicks at
# module-import time so unrelated test runs don't pull silly-kicks. The IDs
# below come from silly_kicks.spadl.config.actiontype_id at 1.7.0:
_PASS_TYPE_ID = 0
_SHOT_TYPE_ID = 11
_KEEPER_SAVE_TYPE_ID = 14
_GOALKICK_TYPE_ID = 22
_FOOT_BODY_ID = 0
_SUCCESS_RES_ID = 1


def _build_minimal_spadl_fixture(n: int = 5, *, team_id: int = 100) -> pd.DataFrame:
    """Build a minimal SPADL-shaped fixture for unit tests."""
    return pd.DataFrame(
        [
            {
                "game_id": 1,
                "match_id": 1,
                "original_event_id": str(i),
                "action_id": i,
                "period_id": 1,
                "time_seconds": float(i),
                "team_id": team_id,
                "player_id": 200,
                "type_id": _PASS_TYPE_ID,
                "result_id": _SUCCESS_RES_ID,
                "bodypart_id": _FOOT_BODY_ID,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 34.0,
            }
            for i in range(n)
        ]
    )


class TestContract:
    def test_returns_dataframe(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        assert isinstance(result, pd.DataFrame)

    def test_adds_six_new_columns(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        for col in [
            "possession_id_heuristic",
            "gk_role",
            "gk_was_distributing",
            "gk_was_engaged",
            "gk_actions_in_possession",
            "defending_gk_player_id",
        ]:
            assert col in result.columns, f"missing {col}"

    def test_preserves_input_columns(self):
        actions = _build_minimal_spadl_fixture()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        for col in actions.columns:
            assert col in result.columns

    def test_does_not_mutate_input(self):
        actions = _build_minimal_spadl_fixture()
        cols_before = list(actions.columns)
        apply_spadl_enrichments(actions, source="statsbomb")
        assert list(actions.columns) == cols_before

    def test_handles_empty_input(self):
        # Slice from a non-empty fixture so column structure is preserved
        # (silly-kicks's helpers require the canonical SPADL columns even
        # on empty input — pd.DataFrame([]) has zero columns).
        actions = _build_minimal_spadl_fixture(n=1).iloc[0:0]
        result = apply_spadl_enrichments(actions, source="statsbomb")
        assert "possession_id_heuristic" in result.columns
        assert len(result) == 0

    def test_invalid_source_raises_value_error(self):
        actions = _build_minimal_spadl_fixture()
        with pytest.raises(ValueError, match=r"source"):
            apply_spadl_enrichments(actions, source="unknown_provider")

    def test_action_id_preserved(self):
        actions = _build_minimal_spadl_fixture(n=10)
        original_ids = set(actions["action_id"].tolist())
        result = apply_spadl_enrichments(actions, source="statsbomb")
        # silly-kicks's add_dribbles may insert synthetic rows with new
        # action_ids — original ones must still be present.
        assert original_ids.issubset(set(result["action_id"].tolist()))


def _build_match_with_gk_actions() -> pd.DataFrame:
    """Build a fixture with mixed GK + outfield + shot actions for plausibility checks.

    Match structure: 3 outfield passes by team A, then a shot by team A,
    a save by team B's GK, GK distribution (goalkick) by GK, then 2 more
    passes by team B, ending with another shot by team A. Two distinct
    teams (A=100, B=200), GK player_id=999.
    """
    rows = [
        {
            "action_id": 0,
            "type_id": _PASS_TYPE_ID,
            "team_id": 100,
            "player_id": 200,
            "time_seconds": 0.0,
            "start_x": 50.0,
            "start_y": 34.0,
            "end_x": 60.0,
            "end_y": 34.0,
        },
        {
            "action_id": 1,
            "type_id": _PASS_TYPE_ID,
            "team_id": 100,
            "player_id": 201,
            "time_seconds": 1.0,
            "start_x": 60.0,
            "start_y": 34.0,
            "end_x": 70.0,
            "end_y": 34.0,
        },
        {
            "action_id": 2,
            "type_id": _PASS_TYPE_ID,
            "team_id": 100,
            "player_id": 202,
            "time_seconds": 2.0,
            "start_x": 70.0,
            "start_y": 34.0,
            "end_x": 90.0,
            "end_y": 34.0,
        },
        # Team A shot
        {
            "action_id": 3,
            "type_id": _SHOT_TYPE_ID,
            "team_id": 100,
            "player_id": 202,
            "time_seconds": 3.0,
            "start_x": 95.0,
            "start_y": 34.0,
            "end_x": 105.0,
            "end_y": 34.0,
        },
        # Team B GK save (in box) — shot_stopping
        {
            "action_id": 4,
            "type_id": _KEEPER_SAVE_TYPE_ID,
            "team_id": 200,
            "player_id": 999,
            "time_seconds": 4.0,
            "start_x": 5.0,
            "start_y": 34.0,
            "end_x": 5.0,
            "end_y": 34.0,
        },
        # Team B GK distribution (goalkick by same player) — distribution role
        {
            "action_id": 5,
            "type_id": _GOALKICK_TYPE_ID,
            "team_id": 200,
            "player_id": 999,
            "time_seconds": 6.0,
            "start_x": 5.0,
            "start_y": 34.0,
            "end_x": 50.0,
            "end_y": 34.0,
        },
        # Team B regular passes
        {
            "action_id": 6,
            "type_id": _PASS_TYPE_ID,
            "team_id": 200,
            "player_id": 300,
            "time_seconds": 8.0,
            "start_x": 50.0,
            "start_y": 34.0,
            "end_x": 60.0,
            "end_y": 34.0,
        },
        {
            "action_id": 7,
            "type_id": _PASS_TYPE_ID,
            "team_id": 200,
            "player_id": 301,
            "time_seconds": 9.0,
            "start_x": 60.0,
            "start_y": 34.0,
            "end_x": 70.0,
            "end_y": 34.0,
        },
        # Team A shot (defending GK 999 was engaged 5+ actions ago)
        {
            "action_id": 8,
            "type_id": _SHOT_TYPE_ID,
            "team_id": 100,
            "player_id": 202,
            "time_seconds": 13.0,
            "start_x": 95.0,
            "start_y": 34.0,
            "end_x": 105.0,
            "end_y": 34.0,
        },
    ]
    for r in rows:
        r["game_id"] = 1
        r["match_id"] = 1
        r["original_event_id"] = str(r["action_id"])
        r["period_id"] = 1
        r["result_id"] = _SUCCESS_RES_ID
        r["bodypart_id"] = _FOOT_BODY_ID
    return pd.DataFrame(rows)


class TestPlausibility:
    def test_gk_role_assigned_on_keeper_actions(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        save_row = result[result["action_id"] == 4].iloc[0]
        assert pd.notna(save_row["gk_role"])
        assert save_row["gk_role"] in {"shot_stopping", "sweeping"}

    def test_distribution_tagged_after_keeper_action(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        distribution_row = result[result["action_id"] == 5].iloc[0]
        assert distribution_row["gk_role"] == "distribution"

    def test_outfield_pass_gets_null_gk_role(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        for action_id in [0, 1, 2]:
            row = result[result["action_id"] == action_id].iloc[0]
            assert pd.isna(row["gk_role"])

    def test_gk_was_engaged_only_on_shot_rows(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        non_shots = result[result["type_id"] != _SHOT_TYPE_ID]
        for engaged in non_shots["gk_was_engaged"]:
            assert bool(engaged) is False

    def test_possession_id_heuristic_starts_at_zero(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        sorted_result = result.sort_values(["game_id", "period_id", "action_id"]).reset_index(drop=True)
        assert sorted_result["possession_id_heuristic"].iloc[0] == 0

    def test_possession_id_heuristic_monotonic(self):
        actions = _build_match_with_gk_actions()
        result = apply_spadl_enrichments(actions, source="statsbomb")
        sorted_result = result.sort_values(["game_id", "period_id", "action_id"]).reset_index(drop=True)
        ids = sorted_result["possession_id_heuristic"].to_numpy()
        for i in range(1, len(ids)):
            assert ids[i] >= ids[i - 1], f"possession_id_heuristic not monotonic at row {i}"


_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "spadl_3match_statsbomb_for_f1.parquet"


def _boundary_metrics(heuristic: pd.Series, native: pd.Series) -> dict[str, float]:
    """Boundary precision / recall / F1 between two possession-id sequences.

    Boundaries are invariant under counter relabeling, so heuristic
    possession_id (0-indexed) and native possession_id (provider's
    offset) compare directly on where they emit a boundary.

    Returns a dict with keys ``precision``, ``recall``, ``f1``.

    NOTE: silly-kicks 1.8.0+ exposes ``boundary_metrics`` as a public
    utility in ``silly_kicks.spadl.utils``. Once luxury-lakehouse's
    silly-kicks dep is bumped to ≥1.8.0 in a follow-up cycle, this
    local helper can be replaced with the public import.
    """
    h_changes = heuristic.ne(heuristic.shift(1)).iloc[1:].to_numpy()
    n_changes = native.ne(native.shift(1)).iloc[1:].to_numpy()
    tp = int((h_changes & n_changes).sum())
    fp = int((h_changes & ~n_changes).sum())
    fn = int((~h_changes & n_changes).sum())
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


@pytest.mark.skipif(
    not _FIXTURE_PATH.exists(),
    reason="boundary-recall fixture missing — run scripts/build_test_fixtures.py",
)
class TestBoundaryRecall:
    """Validate add_possessions heuristic against StatsBomb's native possession_id.

    Empirical findings from the LL2 design cycle (3 StatsBomb open-data matches,
    measured 2026-04-29):
        - Recall: ~0.93 — every real possession boundary is detected.
        - Precision: ~0.42 — heuristic emits ~2x more boundaries than StatsBomb's
          native annotation, since the algorithm class can't merge brief
          opposing-team actions back into the containing possession.
        - F1: ~0.58 (peak ~0.605 at max_gap_seconds=10.0 — parameter tuning
          can't close the gap meaningfully).

    Recall is the meaningful regression metric. F1 conflates two signals
    with very different magnitudes, under-representing the heuristic's
    actual usefulness for downstream possession-based metrics. Consumers
    needing strict StatsBomb-equivalent semantics should use the native
    possession_id directly via ``statsbomb_possession_id``.

    See ADR-016 §4 for the empirical baselines + the silly-kicks PR-S8
    follow-up that propagates this framing into the library docstring.
    """

    def test_boundary_recall_against_native_statsbomb(self):
        all_actions = pd.read_parquet(_FIXTURE_PATH)

        # Drop synthetic dribble rows — they have NaN ``possession`` because
        # silly-kicks's _add_dribbles inserts them with no native counterpart.
        non_synthetic = all_actions[all_actions["possession"].notna()].copy()

        metrics_per_match: dict[int, dict[str, float]] = {}
        for match_id, match_df in non_synthetic.groupby("match_id"):
            enriched = apply_spadl_enrichments(match_df.copy(), source="statsbomb")
            enriched = enriched.sort_values(["game_id", "period_id", "action_id"]).reset_index(drop=True)
            heuristic = enriched["possession_id_heuristic"]
            native = enriched["possession"].astype(np.int64)
            # pandas-stubs types groupby keys as Scalar (which includes complex);
            # narrow to int explicitly since we know match_id is int64 in the fixture.
            metrics_per_match[int(match_id)] = _boundary_metrics(heuristic, native)  # type: ignore[arg-type]

        avg_recall = float(np.mean([m["recall"] for m in metrics_per_match.values()]))
        avg_precision = float(np.mean([m["precision"] for m in metrics_per_match.values()]))
        avg_f1 = float(np.mean([m["f1"] for m in metrics_per_match.values()]))

        per_match_str = ", ".join(
            f"{m}=(r={x['recall']:.3f},p={x['precision']:.3f},f1={x['f1']:.3f})" for m, x in metrics_per_match.items()
        )

        assert avg_recall >= 0.85, (
            f"boundary recall {avg_recall:.4f} below 0.85 threshold. "
            f"Full metrics — avg recall={avg_recall:.4f} precision={avg_precision:.4f} F1={avg_f1:.4f}. "
            f"Per-match: {per_match_str}."
        )
