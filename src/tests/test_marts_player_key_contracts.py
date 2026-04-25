# ruff: noqa: S608 — _MARTS are module-level constant table names, not user input.
"""PR 5b live invariants — player_key must be populated on six embedding marts.

This is the post-deploy gate that ensures the warn-severity dbt
relationships test isn't masking a 100%-NULL column. dbt's relationships
test compares non-NULL values to dim_players; an all-NULL column trivially
passes. This test asserts non-NULL-rate >= 99% on each of the six marts.

Skips when DATABRICKS_* env vars are absent (air-gapped CI). Otherwise
runs against dev_gold via the standard SQL warehouse connection.

Mirrors the connection pattern used by other live tests
(test_int_player_xref_invariants.py, test_marts_live_schema.py): the
``pytest.importorskip("databricks.sql")`` guards against missing the
optional databricks-sql-connector dependency in air-gapped CI runs.
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

_MARTS = (
    "fct_player_embeddings",
    "fct_player_embeddings_season",
    "fct_player_embeddings_career",
    "fct_player_embeddings_season_360",
    "fct_player_embeddings_career_360",
    "fct_player_percentiles",
)


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


@requires_databricks
@pytest.mark.parametrize("mart", _MARTS)
def test_player_key_is_populated(conn, mart: str) -> None:
    """Each PR 5b mart must have at least 99% non-NULL player_key."""
    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    cur = conn.cursor()
    cur.execute(f"SELECT count(*) AS total, count(player_key) AS non_null FROM {table}")
    row = cur.fetchall()[0]
    assert row is not None, f"empty result on {table}"
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        # Mart unbuilt (var('embeddings_enabled', false) on the embedding marts)
        # — skip rather than fail. The post-deploy step explicitly enables the
        # embedding marts; a 0-row count means the gate hasn't run yet.
        pytest.skip(f"{mart} has zero rows — embeddings_enabled may be off")

    rate = non_null / total
    assert rate >= 0.99, (
        f"{mart}: player_key non-NULL rate {rate:.4f} below 0.99 threshold "
        f"(total={total}, non_null={non_null}). Investigate dim_players join."
    )
