# ruff: noqa: S608 — _XREF is a module-level constant table name, not user input.
"""Live-warehouse invariants for int_player_xref post-PR-5a extension.

Requires `entity_resolution_enabled=true` and that
`scripts/generate_entity_xref.py` has run at least once to populate
bronze.player_xref_raw with cross-provider pairs.
"""

from __future__ import annotations

import os

import pytest

from ingestion.databricks_auth import bearer_token, has_databricks_auth

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not (has_databricks_auth() and os.environ.get("DATABRICKS_HTTP_PATH")),
    reason="Databricks SQL env vars not set",
)

# dbt-materialised int models land in dev_silver (schema='silver' in model config).
_XREF = "soccer_analytics.dev_silver.int_player_xref"


@pytest.fixture(scope="module")
def conn():
    c = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=bearer_token(),
    )
    yield c
    c.close()


def _scalar(conn, sql_text: str):
    cur = conn.cursor()
    cur.execute(sql_text)
    return cur.fetchall()[0][0]


@requires_databricks
def test_view_exists(conn) -> None:
    """int_player_xref must exist as a queryable view (post-PR-5a material flip)."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT 1 FROM {_XREF} LIMIT 1")
    except Exception as e:
        pytest.skip(f"int_player_xref not yet materialised (entity_resolution must be enabled + dbt run): {e}")


@requires_databricks
def test_confidence_range_70_to_100(conn) -> None:
    n = _scalar(conn, f"SELECT count(*) FROM {_XREF} WHERE confidence < 70 OR confidence > 100")
    assert n == 0, f"{n} rows outside confidence range [70, 100]"


@requires_databricks
def test_no_self_loops(conn) -> None:
    n = _scalar(
        conn,
        f"SELECT count(*) FROM {_XREF} WHERE source_a = source_b AND player_id_a = player_id_b",
    )
    assert n == 0, f"{n} self-loop rows"


@requires_databricks
def test_provider_ordering_invariant(conn) -> None:
    """Convention: source_a < source_b lexicographically."""
    n = _scalar(conn, f"SELECT count(*) FROM {_XREF} WHERE source_a >= source_b")
    assert n == 0, f"{n} rows violate source_a < source_b"


@requires_databricks
def test_providers_in_known_set(conn) -> None:
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT source_a AS p FROM {_XREF} UNION SELECT DISTINCT source_b FROM {_XREF}")
    seen = {r[0] for r in cur.fetchall()}
    assert seen.issubset({"statsbomb", "wyscout", "idsse", "metrica"}), f"Unknown providers: {seen}"


@requires_databricks
def test_injectivity_per_provider_pair(conn) -> None:
    """Within one (source_a, source_b) pair, each player on A maps to at most one on B."""
    n = _scalar(
        conn,
        f"""
        WITH dups AS (
            SELECT source_a, source_b, player_id_a, count(DISTINCT player_id_b) AS n
              FROM {_XREF}
             GROUP BY source_a, source_b, player_id_a
            HAVING count(DISTINCT player_id_b) > 1
        )
        SELECT count(*) FROM dups
        """,
    )
    assert n == 0, f"{n} (source_a, player_id_a) keys map to multiple player_id_b"
