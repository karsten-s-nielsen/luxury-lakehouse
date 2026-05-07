"""D62 structural guards for dbt player embedding models.

When the 360 import ships, bronze.player_embeddings_raw will contain both
v2 rows (data_source='statsbomb'/'wyscout', 192d) and 360 rows
(data_source='football2vec_360', 208d). The staging model must NOT collapse
same-(player,match) rows with different data_source, and the non-360 marts
must exclude 360 rows to avoid the player_best_dim CTE promoting 208d
vectors over 192d ones.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STAGING = _REPO_ROOT / "dbt_project" / "models" / "staging" / "embeddings" / "stg_player_embeddings.sql"
_MART_SEASON = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_player_embeddings_season.sql"
_MART_CAREER = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_player_embeddings_career.sql"


@pytest.fixture(scope="module")
def staging_sql() -> str:
    return _STAGING.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def mart_season_sql() -> str:
    return _MART_SEASON.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def mart_career_sql() -> str:
    return _MART_CAREER.read_text(encoding="utf-8")


def test_stg_player_embeddings_partitions_by_data_source(staging_sql: str) -> None:
    """D62 fix: the staging dedup must include data_source in the partition so
    v2 and 360 rows for the same (player, match) coexist as separate rows."""
    # First strip SQL line comments from the file so embedded `--` comments
    # inside the partition clause do not trip the regex or assertions.
    uncommented = re.sub(r"--[^\n]*", "", staging_sql)
    # Match the row_number() ... partition by <cols> order by ... construct
    # in a comment-free, multi-line-tolerant way. The partition clause runs
    # until the next `order by`.
    pattern = re.compile(
        r"row_number\s*\(\s*\)\s*over\s*\(\s*partition\s+by\s+(.+?)\s+order\s+by",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(uncommented)
    assert m is not None, "stg_player_embeddings.sql has no row_number window"
    partition_cols = m.group(1).strip()
    assert "data_source" in partition_cols, (
        "D62 regression: stg_player_embeddings.sql partitions by "
        f"`{partition_cols}` — must include data_source so v2 "
        "and 360 rows coexist for the same (player, match)."
    )
    assert "canonical_player_id" in partition_cols
    assert "match_id" in partition_cols


def test_fct_player_embeddings_season_excludes_360(mart_season_sql: str) -> None:
    """D62 fix: the non-360 season mart must exclude football2vec_360 rows."""
    assert (
        "data_source != 'football2vec_360'" in mart_season_sql or "data_source <> 'football2vec_360'" in mart_season_sql
    ), (
        "D62 regression: fct_player_embeddings_season.sql must include "
        "`where data_source != 'football2vec_360'` to prevent the "
        "player_best_dim CTE from promoting 208d vectors over 192d."
    )


def test_fct_player_embeddings_career_excludes_360(mart_career_sql: str) -> None:
    """D62 fix: the non-360 career mart must exclude football2vec_360 rows."""
    assert (
        "data_source != 'football2vec_360'" in mart_career_sql or "data_source <> 'football2vec_360'" in mart_career_sql
    ), (
        "D62 regression: fct_player_embeddings_career.sql must include "
        "`where data_source != 'football2vec_360'` to prevent the "
        "player_best_dim CTE from promoting 208d vectors over 192d."
    )
