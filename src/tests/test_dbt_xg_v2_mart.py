"""PR 3 regression tests for fct_xg_predictions_v2 gold mart (ADR-013).

Asserts after Phase 4 of PR 3:
  - Mart SQL + staging SQL files exist and follow the ADR-013 pattern
    (INNER JOIN fct_shots ON shot_id, contract enforced, clustered on match_key).
  - Mart contract block in _marts__models.yml has all expected columns.
  - `xg` source declares `xg_predictions_v2` table.
  - (Live) fct_xg_predictions_v2 exposes match_key + competition_key and
    does NOT expose legacy match_id.
  - (Live) CI bound ordering: xg_ci_lower <= xg_set_encoder <= xg_ci_upper.
  - (Live) INNER JOIN to fct_shots preserves every staging row.

Live tests skip gracefully when the mart isn't built (xg_v2_enabled default
is false; flipped on in Databricks job config).
"""

from __future__ import annotations

import os
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
# Static tests
# ---------------------------------------------------------------------------


def test_mart_file_exists() -> None:
    assert Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").is_file()


def test_staging_v2_file_exists() -> None:
    assert Path("dbt_project/models/staging/xg/stg_xg__predictions_v2.sql").is_file()


def test_v2_source_declared() -> None:
    yml = Path("dbt_project/models/staging/xg/_xg__sources.yml").read_text(encoding="utf-8")
    assert "- name: xg_predictions_v2" in yml, "xg.xg_predictions_v2 source not declared"


def test_mart_sql_inner_joins_fct_shots_on_shot_id() -> None:
    text = Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").read_text(encoding="utf-8")
    flat = " ".join(text.split())
    assert "inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id" in flat
    assert "s.match_key" in flat
    assert "s.competition_key" in flat
    assert "'enforced': true" in flat
    assert "liquid_clustered_by=['match_key']" in text


def test_mart_contract_block_present() -> None:
    yml = Path("dbt_project/models/marts/_marts__models.yml").read_text(encoding="utf-8")
    assert "- name: fct_xg_predictions_v2" in yml
    block_start = yml.index("- name: fct_xg_predictions_v2\n")
    block = yml[block_start : block_start + 4000]
    expected_cols = (
        "shot_id",
        "match_key",
        "competition_key",
        "competition_id",
        "xg_set_encoder",
        "xg_ci_lower",
        "xg_ci_upper",
    )
    for col in expected_cols:
        assert f"- name: {col}" in block, f"contract block missing column {col}"


# ---------------------------------------------------------------------------
# Live tests — require xg_v2_enabled=true at build time
# ---------------------------------------------------------------------------


@requires_databricks
def test_live_mart_has_kimball_keys(conn: object) -> None:
    """fct_xg_predictions_v2 must have match_key + competition_key + no match_id."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    try:
        cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_xg_predictions_v2")
    except Exception as exc:
        pytest.skip(f"fct_xg_predictions_v2 not built (xg_v2_enabled=false?): {exc}")
        return
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_key" in cols
    assert "competition_key" in cols
    assert "match_id" not in cols, "mart must not expose legacy match_id (ADR-013)"


@requires_databricks
def test_live_ci_bound_ordering(conn: object) -> None:
    """Rows must satisfy xg_ci_lower <= xg_set_encoder <= xg_ci_upper (non-null)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    try:
        cur.execute(
            "SELECT count(*) FROM soccer_analytics.dev_gold.fct_xg_predictions_v2 "
            "WHERE xg_set_encoder IS NOT NULL "
            "AND (xg_ci_lower > xg_set_encoder OR xg_set_encoder > xg_ci_upper)"
        )
    except Exception as exc:
        pytest.skip(f"fct_xg_predictions_v2 not built: {exc}")
        return
    (violations,) = cur.fetchone()
    assert violations == 0, f"{violations} rows violate CI bound ordering"


@requires_databricks
def test_live_inner_join_preserves_rows(conn: object) -> None:
    """Every staging row lands in the mart (INNER JOIN fct_shots preserves)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    try:
        cur.execute(
            "SELECT "
            " (SELECT count(*) FROM soccer_analytics.dev_silver.stg_xg__predictions_v2) AS stg, "
            " (SELECT count(*) FROM soccer_analytics.dev_gold.fct_xg_predictions_v2) AS mart"
        )
    except Exception as exc:
        pytest.skip(f"v2 staging/mart unavailable: {exc}")
        return
    stg, mart = cur.fetchone()
    assert stg == mart, (
        f"staging={stg} vs mart={mart} — INNER JOIN dropped rows. Investigate "
        "bronze.xg_predictions_v2 rows that don't resolve to fct_shots.shot_id."
    )
