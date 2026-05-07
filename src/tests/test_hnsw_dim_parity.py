"""Meta-test: HNSW vector dimensions in scripts/create_indexes.py match the
Python source-of-truth for each embedding model.

PR-Cycle-B (2026-05-01) discovered that scripts/create_indexes.py hardcoded
``vector(128)`` for behavioral career/season HNSW indexes. The
source-of-truth was bumped to 192 in PR #158 (commit b4ebf94 — "promote EV1
iter-15 to Football2VecConfig defaults", 2026-04-19), but the index-creation
literals were never updated. The drift was masked because the synced tables
hadn't been recreated since the bump; Track A recovery (session 69)
recreated them, surfacing the dim mismatch on the next scheduled Lakebase
Maintenance run with pgvector error ``expected 128 dimensions, not 192``.

Same drift class for the kNN VERIFY_QUERIES block — the test query's
embedding cast must use the same dim as the index it exercises.

This test asserts dim parity at PR-CI time. AST-based; no Databricks /
network dependency.

Source-of-truth map:
- behavioral career/season → ``football2vec_transformer.Football2VecConfig.hidden_dim``
- behavioral 360 (career/season) → ``ingestion.player_embeddings_v2._V360_BEHAVIORAL_DIM``
- statistical career/season → literal 13 (statistical features count, domain-fixed)
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from analytics.football2vec_transformer import Football2VecConfig
from ingestion.player_embeddings_v2 import _V360_BEHAVIORAL_DIM

_VECTOR_DIM_PATTERN = re.compile(r"vector\((\d+)\)")
_CREATE_INDEXES_PATH = Path(__file__).resolve().parents[2] / "scripts" / "create_indexes.py"


def _parse_module_list(var_name: str) -> list[tuple[str, str, str]]:
    """Extract a top-level list-of-tuples assignment from create_indexes.py.

    AST-based to avoid importing the module (which requires DATABRICKS_HOST
    + psycopg2 at module load).
    """
    tree = ast.parse(_CREATE_INDEXES_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == var_name:
            assert node.value is not None, f"{var_name} declared but unassigned"
            return ast.literal_eval(node.value)
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id == var_name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{var_name} not found in scripts/create_indexes.py")


def _extract_dim(using_or_query: str) -> int:
    """Extract the single vector(N) dim literal from a string."""
    matches = _VECTOR_DIM_PATTERN.findall(using_or_query)
    assert matches, f"No vector(N) literal found in: {using_or_query!r}"
    # Behavioral HNSW USING clauses have one cast; verify queries may have two
    # (inner SELECT and outer ORDER BY) — they MUST match each other.
    unique = set(matches)
    assert len(unique) == 1, f"vector(N) dims disagree within one entry: {matches} in {using_or_query!r}"
    return int(matches[0])


def test_career_behavioral_hnsw_matches_football_config() -> None:
    """``idx_embeddings_career_behavioral_hnsw`` dim must equal
    ``Football2VecConfig.hidden_dim``. Bump scripts/create_indexes.py if
    Football2VecConfig changed."""
    indexes = _parse_module_list("HNSW_INDEXES")
    found = [(name, using) for name, _table, using in indexes if name == "idx_embeddings_career_behavioral_hnsw"]
    assert found, "idx_embeddings_career_behavioral_hnsw missing from HNSW_INDEXES"
    assert len(found) == 1, f"duplicate idx_embeddings_career_behavioral_hnsw entries: {found}"
    name, using = found[0]
    dim = _extract_dim(using)
    assert dim == Football2VecConfig.hidden_dim, (
        f"{name} declares vector({dim}) but Football2VecConfig.hidden_dim={Football2VecConfig.hidden_dim}. "
        f"Update scripts/create_indexes.py to vector({Football2VecConfig.hidden_dim})."
    )


def test_season_behavioral_hnsw_matches_football_config() -> None:
    """``idx_embeddings_season_behavioral_hnsw`` dim must equal
    ``Football2VecConfig.hidden_dim``."""
    indexes = _parse_module_list("HNSW_INDEXES")
    found = [(name, using) for name, _table, using in indexes if name == "idx_embeddings_season_behavioral_hnsw"]
    assert found, "idx_embeddings_season_behavioral_hnsw missing from HNSW_INDEXES"
    name, using = found[0]
    dim = _extract_dim(using)
    assert dim == Football2VecConfig.hidden_dim, (
        f"{name} declares vector({dim}) but Football2VecConfig.hidden_dim={Football2VecConfig.hidden_dim}. "
        f"Update scripts/create_indexes.py to vector({Football2VecConfig.hidden_dim})."
    )


def test_360_behavioral_hnsw_matches_360_dim() -> None:
    """360 behavioral indexes dim must equal ``_V360_BEHAVIORAL_DIM`` (208)."""
    indexes = _parse_module_list("HNSW_INDEXES")
    expected_360 = {
        "idx_fct_emb_career_360_behavioral_hnsw",
        "idx_fct_emb_season_360_behavioral_hnsw",
    }
    by_name = {name: using for name, _table, using in indexes}
    missing = expected_360 - by_name.keys()
    assert not missing, f"360 HNSW indexes missing from HNSW_INDEXES: {missing}"
    for name in sorted(expected_360):
        dim = _extract_dim(by_name[name])
        assert dim == _V360_BEHAVIORAL_DIM, (
            f"{name} declares vector({dim}) but _V360_BEHAVIORAL_DIM={_V360_BEHAVIORAL_DIM}. "
            f"Update scripts/create_indexes.py."
        )


def test_career_knn_verify_query_matches_index_dim() -> None:
    """``VERIFY_QUERIES`` kNN cast for career behavioral must use the same
    dim as the index. Drift here causes pgvector ``expected N dimensions,
    not M`` at EXPLAIN ANALYZE time — exactly the failure that opened
    PR-Cycle-B.

    VERIFY_QUERIES uses f-strings (``f"... {SCHEMA}.fct_..."``) which
    ``ast.literal_eval`` cannot evaluate. We text-search the source for
    the career-behavioral block and extract the dim literal directly.
    """
    indexes = _parse_module_list("HNSW_INDEXES")
    by_name = {name: using for name, _table, using in indexes}
    index_dim = _extract_dim(by_name["idx_embeddings_career_behavioral_hnsw"])

    src = _CREATE_INDEXES_PATH.read_text(encoding="utf-8")
    marker = "fct_player_embeddings_career: behavioral cosine kNN"
    pos = src.find(marker)
    assert pos >= 0, f"verify-query marker not found: {marker}"
    # Take a window after the marker that covers the full VERIFY_QUERIES tuple.
    block = src[pos : pos + 600]
    block_dims = _VECTOR_DIM_PATTERN.findall(block)
    assert block_dims, f"No vector(N) literal found in career kNN verify block: {block!r}"
    unique = set(block_dims)
    assert len(unique) == 1, (
        f"career kNN verify block has inconsistent vector dims: {block_dims}. "
        f"Inner SELECT and outer ORDER BY casts MUST match."
    )
    query_dim = int(block_dims[0])
    assert query_dim == index_dim, (
        f"VERIFY_QUERIES career kNN uses vector({query_dim}) but "
        f"idx_embeddings_career_behavioral_hnsw declares vector({index_dim}). "
        f"Bring them in sync — pgvector rejects the cast at EXPLAIN ANALYZE otherwise."
    )
