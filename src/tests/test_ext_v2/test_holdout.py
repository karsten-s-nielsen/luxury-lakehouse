"""Tests for the ExT v2 hash-based deterministic holdout split.

Per design spec §5.3: 15% match-based stratified holdout. Implementation
choice (locked 2026-04-26): hash ``(competition_id, match_key)`` via sha256
and bucket in [0, 100), with bucket < holdout_fraction * 100 ⇒ holdout.

This gives determinism across runs, stability as data evolves, per-comp
stratification, train/holdout disjointness, cross-machine reproducibility,
and order-invariance — all without persisting a seed file.
"""

from __future__ import annotations

import hashlib
from itertools import product

import pandas as pd
import pytest

from analytics.ext_v2.holdout import (
    DEFAULT_HOLDOUT_FRACTION,
    REQUIRED_COLUMNS,
    _bucket,
    holdout_split,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_actions(
    *,
    n_comps: int = 3,
    matches_per_comp: int = 100,
    rows_per_match: int = 5,
    competition_id_type: type = int,
    match_key_type: type = int,
) -> pd.DataFrame:
    """Build a synthetic actions dataframe with controlled comp/match structure."""
    rows = []
    for comp_idx in range(n_comps):
        comp_id = competition_id_type(comp_idx + 100)
        for match_idx in range(matches_per_comp):
            mk = match_key_type(comp_idx * 10_000 + match_idx)
            for row_idx in range(rows_per_match):
                rows.append(
                    {
                        "competition_id": comp_id,
                        "match_key": mk,
                        "start_x": float(row_idx),
                        "start_y": float(row_idx + 1),
                        "end_x": float(row_idx + 2),
                        "end_y": float(row_idx + 3),
                        "action_type": "pass",
                        "action_result": "success",
                    }
                )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _bucket — hash helper
# ---------------------------------------------------------------------------


class TestBucket:
    """Pure helper: sha256(f"{competition_id}|{match_key}") % 100."""

    def test_returns_int_in_0_99(self) -> None:
        for cid, mk in product(["a", "b", "global"], [1, 1000, 99999]):
            b = _bucket(cid, mk)
            assert isinstance(b, int)
            assert 0 <= b < 100

    def test_documented_formula(self) -> None:
        h = hashlib.sha256(b"test_comp|42", usedforsecurity=False).hexdigest()
        expected = int(h, 16) % 100
        assert _bucket("test_comp", 42) == expected

    def test_int_string_competition_equivalent(self) -> None:
        # ``competition_id`` may arrive as int or str; both must hash equivalently.
        assert _bucket(11, 100) == _bucket("11", "100")

    def test_int_string_match_key_equivalent(self) -> None:
        assert _bucket("X", 42) == _bucket("X", "42")

    def test_different_keys_different_buckets_in_aggregate(self) -> None:
        # Sanity: a few thousand keys → many distinct buckets
        buckets = {_bucket("comp", i) for i in range(2000)}
        assert len(buckets) > 50  # of 100 possible


# ---------------------------------------------------------------------------
# holdout_split — output shape + row conservation
# ---------------------------------------------------------------------------


class TestHoldoutSplitShape:
    def test_returns_tuple_of_dataframes(self) -> None:
        actions = _make_actions()
        train, holdout = holdout_split(actions)
        assert isinstance(train, pd.DataFrame)
        assert isinstance(holdout, pd.DataFrame)

    def test_no_rows_lost(self) -> None:
        actions = _make_actions(n_comps=5, matches_per_comp=50, rows_per_match=3)
        train, holdout = holdout_split(actions)
        assert len(train) + len(holdout) == len(actions)

    def test_columns_preserved(self) -> None:
        actions = _make_actions()
        train, holdout = holdout_split(actions)
        assert list(train.columns) == list(actions.columns)
        assert list(holdout.columns) == list(actions.columns)

    def test_empty_input_returns_two_empty(self) -> None:
        empty = pd.DataFrame({"competition_id": [], "match_key": [], "start_x": []})
        train, holdout = holdout_split(empty)
        assert len(train) == 0
        assert len(holdout) == 0
        assert list(train.columns) == ["competition_id", "match_key", "start_x"]


# ---------------------------------------------------------------------------
# holdout_split — disjointness invariant
# ---------------------------------------------------------------------------


class TestHoldoutSplitDisjointness:
    """Match-level disjointness: no (comp, match) appears in both splits."""

    def test_match_keys_disjoint(self) -> None:
        actions = _make_actions(n_comps=5, matches_per_comp=100, rows_per_match=3)
        train, holdout = holdout_split(actions)
        train_keys = set(zip(train["competition_id"], train["match_key"], strict=True))
        holdout_keys = set(zip(holdout["competition_id"], holdout["match_key"], strict=True))
        assert train_keys.isdisjoint(holdout_keys)

    def test_all_rows_of_a_match_co_locate(self) -> None:
        """All rows for the same (comp_id, match_key) end up in the same split."""
        actions = _make_actions(n_comps=3, matches_per_comp=20, rows_per_match=10)
        train, holdout = holdout_split(actions)
        for (cid, mk), group in actions.groupby(["competition_id", "match_key"]):
            n_train = len(train[(train["competition_id"] == cid) & (train["match_key"] == mk)])
            n_holdout = len(holdout[(holdout["competition_id"] == cid) & (holdout["match_key"] == mk)])
            both_train = n_train == len(group) and n_holdout == 0
            both_holdout = n_train == 0 and n_holdout == len(group)
            assert both_train or both_holdout, f"match ({cid}, {mk}) split across both folds"


# ---------------------------------------------------------------------------
# holdout_split — per-competition stratification
# ---------------------------------------------------------------------------


class TestHoldoutSplitStratification:
    """Per-comp holdout fraction approaches the global threshold for large comps."""

    def test_large_comp_fraction_in_tolerance(self) -> None:
        # 1000 matches x 3 comps; loose binomial CI for p=0.15 at n=1000
        actions = _make_actions(n_comps=3, matches_per_comp=1000, rows_per_match=1)
        _, holdout = holdout_split(actions)
        for comp_id in actions["competition_id"].unique():
            n_total = actions[actions["competition_id"] == comp_id]["match_key"].nunique()
            n_holdout = holdout[holdout["competition_id"] == comp_id]["match_key"].nunique()
            ratio = n_holdout / n_total
            assert 0.10 < ratio < 0.20, f"comp {comp_id}: holdout ratio {ratio:.3f}"

    def test_global_fraction_in_tolerance(self) -> None:
        # 5000 total matches → tight CI on global ratio
        actions = _make_actions(n_comps=5, matches_per_comp=1000, rows_per_match=1)
        _, holdout = holdout_split(actions)
        ratio = holdout["match_key"].nunique() / actions["match_key"].nunique()
        assert 0.13 < ratio < 0.17, f"global holdout ratio {ratio:.3f}"


# ---------------------------------------------------------------------------
# holdout_split — determinism
# ---------------------------------------------------------------------------


class TestHoldoutSplitDeterminism:
    """Hash-based split is deterministic across runs, order, and subsets."""

    def test_idempotent_across_runs(self) -> None:
        actions = _make_actions(n_comps=3, matches_per_comp=50)
        t1, h1 = holdout_split(actions)
        t2, h2 = holdout_split(actions)
        pd.testing.assert_frame_equal(t1, t2)
        pd.testing.assert_frame_equal(h1, h2)

    def test_order_invariant(self) -> None:
        """Shuffling input rows yields the same holdout match-set."""
        actions = _make_actions(n_comps=3, matches_per_comp=50)
        shuffled = actions.sample(frac=1.0, random_state=42).reset_index(drop=True)
        _, h1 = holdout_split(actions)
        _, h2 = holdout_split(shuffled)
        keys1 = set(zip(h1["competition_id"], h1["match_key"], strict=True))
        keys2 = set(zip(h2["competition_id"], h2["match_key"], strict=True))
        assert keys1 == keys2

    def test_subset_invariant(self) -> None:
        """A match's bucket assignment doesn't depend on other matches present."""
        actions = _make_actions(n_comps=3, matches_per_comp=50)
        _, h_full = holdout_split(actions)
        kept_cids = list(actions["competition_id"].unique()[:2])
        subset = actions[actions["competition_id"].isin(kept_cids)]
        _, h_subset = holdout_split(subset)
        keys_full = {
            (c, m) for c, m in zip(h_full["competition_id"], h_full["match_key"], strict=True) if c in kept_cids
        }
        keys_subset = set(zip(h_subset["competition_id"], h_subset["match_key"], strict=True))
        assert keys_full == keys_subset


# ---------------------------------------------------------------------------
# holdout_split — type tolerance
# ---------------------------------------------------------------------------


class TestHoldoutSplitTypeTolerance:
    """competition_id and match_key may be int or str; both hash equivalently."""

    def test_competition_id_int_or_string(self) -> None:
        actions_int = _make_actions(n_comps=3, matches_per_comp=20, competition_id_type=int)
        actions_str = actions_int.copy()
        actions_str["competition_id"] = actions_str["competition_id"].astype(str)
        _, h_int = holdout_split(actions_int)
        _, h_str = holdout_split(actions_str)
        assert set(h_int["match_key"]) == set(h_str["match_key"])

    def test_match_key_int_or_string(self) -> None:
        actions_int = _make_actions(n_comps=3, matches_per_comp=20, match_key_type=int)
        actions_str = actions_int.copy()
        actions_str["match_key"] = actions_str["match_key"].astype(str)
        _, h_int = holdout_split(actions_int)
        _, h_str = holdout_split(actions_str)
        assert set(h_int["match_key"].astype(str)) == set(h_str["match_key"].astype(str))


# ---------------------------------------------------------------------------
# holdout_split — small-comp behavior (per design §10 small-comp lock)
# ---------------------------------------------------------------------------


class TestHoldoutSplitSmallComp:
    def test_single_match_comp_lands_in_one_split(self) -> None:
        """Comp with 1 match: rows for that match all in train OR all in holdout."""
        actions = pd.DataFrame(
            {
                "competition_id": ["X"] * 10,
                "match_key": [42] * 10,
                "start_x": list(range(10)),
            }
        )
        train, holdout = holdout_split(actions)
        is_train = len(train) == 10 and len(holdout) == 0
        is_holdout = len(train) == 0 and len(holdout) == 10
        assert is_train or is_holdout

    def test_two_match_comp_conserves_rows(self) -> None:
        actions = pd.DataFrame(
            {
                "competition_id": ["X", "X"] * 5,
                "match_key": [1, 2] * 5,
                "start_x": list(range(10)),
            }
        )
        train, holdout = holdout_split(actions)
        assert len(train) + len(holdout) == 10


# ---------------------------------------------------------------------------
# holdout_split — custom holdout_fraction
# ---------------------------------------------------------------------------


class TestHoldoutSplitCustomFraction:
    def test_zero_fraction_all_train(self) -> None:
        actions = _make_actions()
        train, holdout = holdout_split(actions, holdout_fraction=0.0)
        assert len(train) == len(actions)
        assert len(holdout) == 0

    def test_one_fraction_all_holdout(self) -> None:
        actions = _make_actions()
        train, holdout = holdout_split(actions, holdout_fraction=1.0)
        assert len(train) == 0
        assert len(holdout) == len(actions)

    def test_subset_relationship(self) -> None:
        """Holdout at p1 < p2 ⇒ holdout(p1) is a subset of holdout(p2)."""
        actions = _make_actions(n_comps=3, matches_per_comp=200)
        _, h_15 = holdout_split(actions, holdout_fraction=0.15)
        _, h_30 = holdout_split(actions, holdout_fraction=0.30)
        keys_15 = set(zip(h_15["competition_id"], h_15["match_key"], strict=True))
        keys_30 = set(zip(h_30["competition_id"], h_30["match_key"], strict=True))
        assert keys_15.issubset(keys_30)


# ---------------------------------------------------------------------------
# holdout_split — input validation
# ---------------------------------------------------------------------------


class TestHoldoutSplitValidation:
    def test_rejects_negative_fraction(self) -> None:
        actions = _make_actions(n_comps=1, matches_per_comp=1)
        with pytest.raises(ValueError, match="holdout_fraction"):
            holdout_split(actions, holdout_fraction=-0.1)

    def test_rejects_fraction_above_one(self) -> None:
        actions = _make_actions(n_comps=1, matches_per_comp=1)
        with pytest.raises(ValueError, match="holdout_fraction"):
            holdout_split(actions, holdout_fraction=1.1)

    def test_rejects_missing_competition_id(self) -> None:
        actions = pd.DataFrame({"match_key": [1, 2], "start_x": [1.0, 2.0]})
        with pytest.raises(ValueError, match="competition_id"):
            holdout_split(actions)

    def test_rejects_missing_match_key(self) -> None:
        actions = pd.DataFrame({"competition_id": ["X"], "start_x": [1.0]})
        with pytest.raises(ValueError, match="match_key"):
            holdout_split(actions)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


class TestPublicSurface:
    def test_default_fraction(self) -> None:
        assert DEFAULT_HOLDOUT_FRACTION == 0.15

    def test_required_columns(self) -> None:
        assert "competition_id" in REQUIRED_COLUMNS
        assert "match_key" in REQUIRED_COLUMNS
