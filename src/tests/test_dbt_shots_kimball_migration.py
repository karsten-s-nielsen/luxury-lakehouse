"""PR 3 Kimball migration regression tests for fct_shots.

Mirrors test_dbt_passes_kimball_migration.py (PR 2). Asserts that after the
migration:
  - int_unified_shots emits match_key (view materialization, INNER JOIN dim_matches)
  - fct_shots contract declares match_key NOT NULL, adds competition_key,
    retains competition_id as nullable legacy INT, and drops match_id entirely
  - fct_shots.sql clusters on match_key and emits no match_id in its final CTE
  - Live per-source rowcounts are preserved from the 2026-04-22 pre-migration
    baseline (statsbomb=87,999; wyscout=43,078; total=131,077); INNER JOIN to
    dim_matches does not drop any rows (verified pre-migration: zero orphans).
  - Referential integrity: every fct_shots.match_key resolves to dim_matches.

Requires a live Databricks SQL warehouse via DATABRICKS_HOST,
DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield c
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Static tests — yml contract + SQL body (no warehouse needed)
# ---------------------------------------------------------------------------


def test_marts_yml_fct_shots_has_match_key_not_match_id() -> None:
    yml = Path("dbt_project/models/marts/_marts__models.yml").read_text(encoding="utf-8")
    block_start = yml.index("- name: fct_shots\n")
    next_model = yml.index("\n  - name:", block_start + 1)
    block = yml[block_start:next_model]
    assert "- name: match_key" in block, "fct_shots contract missing match_key"
    assert "- name: match_id" not in block, "fct_shots contract still has legacy match_id"
    assert "- name: competition_key" in block, "fct_shots contract missing competition_key"
    # competition_id stays as nullable legacy INT until PR 8 sweep
    assert "- name: competition_id" in block, "fct_shots must retain competition_id legacy INT"


def test_fct_shots_sql_clusters_on_match_key() -> None:
    sql_text = Path("dbt_project/models/marts/fct_shots.sql").read_text(encoding="utf-8")
    assert "liquid_clustered_by=['match_key']" in sql_text, (
        "fct_shots.sql config must use liquid_clustered_by=['match_key']"
    )


def test_fct_shots_sql_final_cte_has_no_match_id() -> None:
    sql_text = Path("dbt_project/models/marts/fct_shots.sql").read_text(encoding="utf-8")
    final_match = re.search(r"final as \(([\s\S]*?)\n\)", sql_text)
    assert final_match is not None, "Expected `final as (...)` CTE in fct_shots.sql"
    final_block = final_match.group(1)
    assert "match_key" in final_block, "fct_shots.sql final CTE missing match_key"
    assert not re.search(r"\bmatch_id\b", final_block), "fct_shots.sql final CTE must not emit match_id"


# ---------------------------------------------------------------------------
# int_unified_shots — upgraded to view in silver schema
# ---------------------------------------------------------------------------


@requires_databricks
def test_int_unified_shots_materialized_in_silver(conn: object) -> None:
    """PR 3 upgrades int_unified_shots from ephemeral to view; must appear in silver."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_silver.int_unified_shots")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_key" in cols
    assert "match_id" not in cols
    assert "native_match_id" not in cols, "int_unified_shots must not leak native_match_id"
    assert "provider" not in cols, "int_unified_shots must not leak provider (join dim_matches)"


@requires_databricks
def test_int_unified_shots_match_key_not_null(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_silver.int_unified_shots WHERE match_key IS NULL")
    assert cur.fetchone()[0] == 0, "int_unified_shots has NULL match_keys — dim join broken"


# ---------------------------------------------------------------------------
# fct_shots — live table checks
# ---------------------------------------------------------------------------


@requires_databricks
def test_fct_shots_live_has_no_match_id_column(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_shots")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_id" not in cols, "fct_shots still has legacy match_id — migration incomplete"
    assert "match_key" in cols, "fct_shots missing match_key — migration incomplete"
    assert "competition_key" in cols, "fct_shots missing competition_key"
    assert "competition_id" in cols, "fct_shots must retain competition_id legacy INT"


@requires_databricks
def test_fct_shots_live_column_types(conn: object) -> None:
    """Kimball keys are BIGINT; legacy competition_id stays INT."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_shots")
    schema = {row[0]: row[1] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert schema.get("match_key") == "bigint", f"match_key type: {schema.get('match_key')}"
    assert schema.get("competition_key") == "bigint", f"competition_key type: {schema.get('competition_key')}"
    assert schema.get("competition_id") == "int", f"competition_id type drifted: {schema.get('competition_id')}"


@requires_databricks
def test_fct_shots_live_match_key_not_null(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_gold.fct_shots WHERE match_key IS NULL")
    assert cur.fetchone()[0] == 0, "fct_shots has NULL match_keys — int_unified_shots INNER JOIN broken"


@requires_databricks
def test_fct_shots_live_match_key_joins_to_dim(conn: object) -> None:
    """Referential integrity: every fct_shots.match_key must resolve in dim_matches."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        """
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_shots s
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON s.match_key = dm.match_key
        WHERE dm.match_key IS NULL
        """
    )
    assert cur.fetchone()[0] == 0, "fct_shots has match_keys not in dim_matches — referential integrity violation"


@requires_databricks
def test_fct_shots_live_shot_id_unique(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT shot_id, count(*) c FROM soccer_analytics.dev_gold.fct_shots "
        "GROUP BY shot_id HAVING count(*) > 1 LIMIT 5"
    )
    dupes = cur.fetchall()
    assert not dupes, f"fct_shots has duplicate shot_id: {dupes}"


@requires_databricks
def test_fct_shots_live_baselines_preserved(conn: object) -> None:
    """Pre-migration rowcounts captured 2026-04-22. Must not drift post-migration
    (INNER JOIN dim_matches was verified to produce zero orphans pre-migration)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT data_source, count(*) FROM soccer_analytics.dev_gold.fct_shots GROUP BY data_source")
    counts = {row[0]: row[1] for row in cur.fetchall()}
    assert counts.get("statsbomb") == 87_999, counts
    assert counts.get("wyscout") == 43_078, counts


@requires_databricks
def test_fct_shots_live_total_unchanged(conn: object) -> None:
    """Pre-migration total of 131,077 captured 2026-04-22. Migration must preserve."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_gold.fct_shots")
    assert cur.fetchone()[0] == 131_077
