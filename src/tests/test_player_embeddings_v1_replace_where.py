"""TDD guard for player_embeddings_v1 replace_where scoping (D45 Part B).

The v1 Doc2Vec pipeline previously wrote to player_embeddings_raw with
``replace_where=f"data_source = '{source}'"``, which replaced the ENTIRE
partition for that data_source. After v2 ran first and wrote 22K rows,
v1 processing a single new match would clobber all 22K v2 rows and write
back just the one new match row in 32d format.

The fix: scope replace_where to the specific match_ids v1 processed.
"""

from __future__ import annotations

import inspect


def test_run_pipeline_v1_uses_match_id_scoped_replace_where() -> None:
    """v1's write must scope replace_where to specific match_ids, not
    data_source alone."""
    from ingestion import player_embeddings_v1

    source = inspect.getsource(player_embeddings_v1.run_pipeline_v1)

    # Old bug pattern — must be gone.
    assert "replace_where=f\"data_source = '{source_str}'\"" not in source, (
        "D45 regression: player_embeddings_v1.run_pipeline_v1 still uses "
        "data-source-only replace_where. This clobbers v2 128d rows when "
        "v1 processes a single new match. Fix: include match_id IN (...) "
        "in the replace_where predicate."
    )

    # Fix pattern — must be present.
    assert "match_id IN" in source or "match_id in" in source, (
        "D45 fix: player_embeddings_v1.run_pipeline_v1 must build a "
        "match_id IN (...) predicate for replace_where so it only replaces "
        "rows for the matches it actually processed."
    )


def test_run_pipeline_v1_replace_where_is_composed_correctly() -> None:
    """When v1 writes, replace_where must combine data_source AND match_id
    — replacing only the rows v1 owns, not the whole source partition."""
    from ingestion import player_embeddings_v1

    source = inspect.getsource(player_embeddings_v1.run_pipeline_v1)

    # The predicate must reference both data_source and match_id
    # within the same replace_where assignment. Locate the predicate string.
    predicate_idx = source.find("predicate =")
    assert predicate_idx >= 0, (
        "v1 fix expected to assemble a `predicate = ...` string before "
        "passing it to write_delta_table as replace_where=predicate"
    )
    # Scan a reasonable window after the assignment
    predicate_block = source[predicate_idx : predicate_idx + 300]

    assert "data_source" in predicate_block, "predicate must reference data_source"
    assert "match_id" in predicate_block, "predicate must reference match_id (scoped delete)"
