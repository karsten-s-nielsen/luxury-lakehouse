"""football2vec publisher input/output leak assertions (spec §6.8 / D10 / Task 18).

The embeddings are pre-aggregated + stochastic, so a publish-time row filter cannot un-mix a private
contribution. The publisher instead enforces the boundary with two assertions + a fail-closed default:

  (a) INPUT  — the materialised source had ZERO ``access_tier != 'public'`` rows;
  (b) OUTPUT — the published player vocabulary is a SUBSET of players with >=1 public row (a private-only
               player's id is an existence leak and must be entirely absent);
  (c) FAIL-CLOSED — if the public career/season aggregate is not provably public-recomputed, publish ONLY
               the per-match (split) embeddings and signal failure.

These tests exercise the pure decision logic with mocked source frames — no Databricks, no HF Hub.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"


@pytest.fixture(autouse=True)
def _ensure_publisher_importable() -> None:
    """Add scripts/ to sys.path so the PEP-723 publisher module imports as a plain module."""
    scripts_str = str(_SCRIPTS_DIR)
    if scripts_str not in sys.path:
        sys.path.insert(0, scripts_str)


def _per_match(rows: list[tuple[str, str]], tier: str | list[str] = "public") -> pd.DataFrame:
    """Build a per-match embeddings frame: rows are (canonical_player_id, match_id)."""
    tiers = tier if isinstance(tier, list) else [tier] * len(rows)
    return pd.DataFrame(
        {
            "canonical_player_id": [r[0] for r in rows],
            "match_id": [r[1] for r in rows],
            "behavioral_vector": [[0.1, 0.2] for _ in rows],
            "access_tier": tiers,
        }
    )


def _agg(player_ids: list[str], tier: str | list[str] = "public") -> pd.DataFrame:
    """Build a career/season aggregate frame keyed by canonical_player_id."""
    tiers = tier if isinstance(tier, list) else [tier] * len(player_ids)
    return pd.DataFrame(
        {
            "canonical_player_id": player_ids,
            "behavioral_vector": [[0.3, 0.4] for _ in player_ids],
            "access_tier": tiers,
        }
    )


# ---------------------------------------------------------------------------
# Output-vocabulary assertion (b) — the existence-leak guard
# ---------------------------------------------------------------------------


def test_public_player_vocabulary_is_the_per_match_public_player_set() -> None:
    import publish_football2vec_embeddings_hf as pub

    per_match = _per_match([("p1", "m1"), ("p2", "m1"), ("p1", "m2")])
    assert pub.public_player_vocabulary(per_match) == {"p1", "p2"}


def test_output_vocabulary_subset_passes_when_all_published_ids_are_public() -> None:
    import publish_football2vec_embeddings_hf as pub

    pub.assert_output_vocabulary_subset(_agg(["p1", "p2"]), public_ids={"p1", "p2", "p3"}, table_label="career")


def test_output_vocabulary_subset_raises_on_private_only_player_id() -> None:
    import publish_football2vec_embeddings_hf as pub

    from ingestion.hf_leak_guard import LeakDetectedError

    # "ghost" is a private-only player: present in the aggregate, absent from the public vocabulary.
    with pytest.raises(LeakDetectedError, match="not in the public vocabulary"):
        pub.assert_output_vocabulary_subset(_agg(["p1", "ghost"]), public_ids={"p1"}, table_label="career")


# ---------------------------------------------------------------------------
# Input assertion (a) — per-match must be all-public (hard raise)
# ---------------------------------------------------------------------------


def test_per_match_with_restricted_row_raises_hard() -> None:
    import publish_football2vec_embeddings_hf as pub

    from ingestion.hf_leak_guard import LeakDetectedError

    per_match = _per_match([("p1", "m1"), ("p2", "m2")], tier=["public", "restricted"])
    with pytest.raises(LeakDetectedError):
        pub.select_publishable_tables(per_match, _agg(["p1"]), _agg(["p1"]))


def test_per_match_with_null_tier_raises_hard() -> None:
    import publish_football2vec_embeddings_hf as pub

    from ingestion.hf_leak_guard import LeakDetectedError

    per_match = _per_match([("p1", "m1"), ("p2", "m2")], tier=["public", None])  # type: ignore[list-item]
    with pytest.raises(LeakDetectedError):
        pub.select_publishable_tables(per_match, _agg(["p1"]), _agg(["p1"]))


# ---------------------------------------------------------------------------
# select_publishable_tables — happy path + fail-closed
# ---------------------------------------------------------------------------


def test_happy_path_publishes_all_three_and_drops_access_tier() -> None:
    import publish_football2vec_embeddings_hf as pub

    per_match = _per_match([("p1", "m1"), ("p2", "m2")])
    career = _agg(["p1", "p2"])
    season = _agg(["p1", "p2"])

    tables, withheld = pub.select_publishable_tables(per_match, career, season)

    assert withheld is None
    assert set(tables) == {"per_match", "career", "season"}
    # R2: the internal access_tier column is dropped from every uploaded frame.
    for frame in tables.values():
        assert "access_tier" not in frame.columns


def test_fail_closed_when_career_season_unavailable_publishes_only_per_match() -> None:
    import publish_football2vec_embeddings_hf as pub

    per_match = _per_match([("p1", "m1"), ("p2", "m2")])
    tables, withheld = pub.select_publishable_tables(per_match, None, None)

    assert set(tables) == {"per_match"}
    assert withheld is not None and "public-recomputed" in withheld
    assert "access_tier" not in tables["per_match"].columns


def test_fail_closed_when_career_has_private_only_player_withholds_aggregates() -> None:
    import publish_football2vec_embeddings_hf as pub

    per_match = _per_match([("p1", "m1"), ("p2", "m2")])
    career = _agg(["p1", "ghost"])  # ghost not in public per-match vocabulary -> output assertion fails
    season = _agg(["p1", "p2"])

    tables, withheld = pub.select_publishable_tables(per_match, career, season)

    # Fail-closed: per-match still ships, career/season withheld (NOT a hard raise).
    assert set(tables) == {"per_match"}
    assert withheld is not None and "not provably public-recomputed" in withheld


def test_fail_closed_when_career_aggregate_carries_a_non_public_row() -> None:
    import publish_football2vec_embeddings_hf as pub

    per_match = _per_match([("p1", "m1"), ("p2", "m2")])
    career = _agg(["p1", "p2"], tier=["public", "restricted"])  # input assertion fails for career
    season = _agg(["p1", "p2"])

    tables, withheld = pub.select_publishable_tables(per_match, career, season)

    assert set(tables) == {"per_match"}
    assert withheld is not None
