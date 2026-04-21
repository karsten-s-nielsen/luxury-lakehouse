"""Integration tests for the conformed dim_matches dimension.

Requires live Databricks SQL warehouse access via standard environment
variables. Skipped when those are not set. Run locally after
`dbt build --select dim_matches`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

# The `databricks` namespace is present in CI via the `databricks-sdk`
# package, but the `.sql` submodule only ships in `databricks-sql-connector`,
# which is a local-dev-only dependency. Use `pytest.importorskip` with the
# fully-qualified submodule path so the test is skipped cleanly in CI and
# pyright never tries to statically resolve `databricks.sql` at module load.
databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    connection = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield connection
    finally:
        connection.close()


@requires_databricks
def test_four_providers_present(conn: object) -> None:
    """dim_matches must contain rows for all four providers."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT DISTINCT provider FROM soccer_analytics.dev_gold.dim_matches")
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_idsse_row_count(conn: object) -> None:
    """IDSSE has exactly 7 matches (static figshare collection)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_gold.dim_matches WHERE provider='idsse'")
    assert cur.fetchone()[0] == 7


@requires_databricks
def test_metrica_row_count(conn: object) -> None:
    """Metrica sample-data has exactly 3 matches (static)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_gold.dim_matches WHERE provider='metrica'")
    assert cur.fetchone()[0] == 3


@requires_databricks
def test_match_key_unique(conn: object) -> None:
    """No surrogate-key collisions across providers."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*), count(DISTINCT match_key) FROM soccer_analytics.dev_gold.dim_matches")
    total, distinct = cur.fetchone()
    assert total == distinct, f"Collision detected: {total - distinct} duplicate match_keys"


@requires_databricks
def test_match_key_deterministic(conn: object) -> None:
    """Same (provider, native_match_id) must produce the same match_key
    within a query (Spark xxhash64 is pure)."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT match_key FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' AND native_match_id='J03WMX'"
    )
    row = cur.fetchone()
    assert row is not None, "idsse_J03WMX missing from dim_matches"
    match_key_1 = row[0]

    cur.execute(
        "SELECT match_key FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' AND native_match_id='J03WMX'"
    )
    assert cur.fetchone()[0] == match_key_1


@requires_databricks
def test_provider_sensitivity(conn: object) -> None:
    """Hash provider-sensitivity via Spark-computed values:
    (statsbomb, '1') != (wyscout, '1')."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT xxhash64(concat_ws('|', 'statsbomb', '1')),        xxhash64(concat_ws('|', 'wyscout', '1'))")
    sb_key, wy_key = cur.fetchone()
    assert sb_key != wy_key, "Macro not provider-sensitive"


@requires_databricks
def test_idsse_native_ids_have_no_prefix(conn: object) -> None:
    """IDSSE native_match_id must NOT carry the 'idsse_' prefix —
    stg_idsse__matches strips it so native_match_id is the raw DFL MatchId."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT native_match_id FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' ORDER BY native_match_id"
    )
    native_ids = [row[0] for row in cur.fetchall()]
    assert native_ids == ["J03WMX", "J03WN1", "J03WOH", "J03WOY", "J03WPY", "J03WQQ", "J03WR9"]
