"""Integration tests for PR 2 of the Kimball Migration.

Asserts that `int_unified_passes`, `int_running_score`, `fct_passes`,
`fct_line_breaking_results`, and `fct_match_summary` have all migrated
from native `match_id` to the surrogate `match_key` (ADR-011), and that
the four-provider union (StatsBomb, Wyscout, IDSSE, Metrica) is
populated end-to-end.

Requires a live Databricks SQL warehouse via DATABRICKS_HOST,
DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

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
# int_unified_passes — view with 4-provider union
# ---------------------------------------------------------------------------


@requires_databricks
def test_int_unified_passes_has_four_providers(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT DISTINCT data_source FROM soccer_analytics.dev_silver.int_unified_passes")
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_int_unified_passes_match_key_not_null(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_silver.int_unified_passes WHERE match_key IS NULL")
    assert cur.fetchone()[0] == 0, "int_unified_passes has NULL match_keys — dim join broken"


# ---------------------------------------------------------------------------
# fct_passes
# ---------------------------------------------------------------------------


@requires_databricks
def test_fct_passes_has_no_match_id_column(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_passes")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_id" not in cols, "fct_passes still has legacy match_id — migration incomplete"
    assert "match_key" in cols, "fct_passes missing match_key — migration incomplete"


@requires_databricks
def test_fct_passes_match_key_joins_to_dim(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        """
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_passes fp
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON fp.match_key = dm.match_key
        WHERE dm.match_key IS NULL
        """
    )
    assert cur.fetchone()[0] == 0, "fct_passes has match_keys not in dim_matches — referential integrity violation"


@requires_databricks
def test_fct_passes_covers_all_four_providers(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT DISTINCT data_source FROM soccer_analytics.dev_gold.fct_passes")
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_fct_passes_sb_ws_baseline_preserved(conn: object) -> None:
    """Pre-migration StatsBomb and Wyscout rowcounts captured 2026-04-21.
    These MUST not drift post-migration (IDSSE+Metrica rows are net-new).
    """
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT data_source, count(*) FROM soccer_analytics.dev_gold.fct_passes "
        "WHERE data_source IN ('statsbomb', 'wyscout') GROUP BY data_source"
    )
    counts = {row[0]: row[1] for row in cur.fetchall()}
    assert counts.get("statsbomb") == 3_386_907, counts
    assert counts.get("wyscout") == 1_665_508, counts


# ---------------------------------------------------------------------------
# int_running_score — Task 4a
# ---------------------------------------------------------------------------


@requires_databricks
def test_int_running_score_match_key_joins_to_dim(conn: object) -> None:
    """Because int_running_score is ephemeral, we exercise it via a
    downstream consumer (fct_passes). For each pass row the rs.* columns
    are populated only for SB/WS (2-provider int_running_score). The
    check: matches with non-null home_score_after must join to dim_matches
    via their match_key.
    """
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        """
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_passes fp
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON fp.match_key = dm.match_key
        WHERE dm.match_key IS NULL
          AND fp.data_source IN ('statsbomb', 'wyscout')
        """
    )
    assert cur.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# fct_line_breaking_results
# ---------------------------------------------------------------------------


@requires_databricks
def test_fct_line_breaking_results_has_match_key(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_line_breaking_results")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_id" not in cols
    assert "match_key" in cols


@requires_databricks
def test_fct_line_breaking_results_match_key_joins_to_dim(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        """
        SELECT count(*)
        FROM soccer_analytics.dev_gold.fct_line_breaking_results lb
        LEFT JOIN soccer_analytics.dev_gold.dim_matches dm
          ON lb.match_key = dm.match_key
        WHERE dm.match_key IS NULL
        """
    )
    assert cur.fetchone()[0] == 0


@requires_databricks
def test_fct_line_breaking_results_baselines_preserved(conn: object) -> None:
    """Post-migration rowcounts per data_source must match preflight baselines."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT data_source, count(*) FROM soccer_analytics.dev_gold.fct_line_breaking_results GROUP BY data_source"
    )
    counts = {row[0]: row[1] for row in cur.fetchall()}
    assert counts.get("statsbomb_360") == 275_884, counts
    assert counts.get("idsse_tracking") == 5_372, counts
    assert counts.get("metrica_tracking") == 2_037, counts


# ---------------------------------------------------------------------------
# fct_match_summary
# ---------------------------------------------------------------------------


@requires_databricks
def test_fct_match_summary_has_match_key(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("DESCRIBE soccer_analytics.dev_gold.fct_match_summary")
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    assert "match_id" not in cols
    assert "match_key" in cols


@requires_databricks
def test_fct_match_summary_covers_all_four_providers(conn: object) -> None:
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        """
        SELECT dm.provider, count(ms.match_key)
        FROM soccer_analytics.dev_gold.dim_matches dm
        LEFT JOIN soccer_analytics.dev_gold.fct_match_summary ms
          ON dm.match_key = ms.match_key
        GROUP BY dm.provider
        """
    )
    rows = {row[0]: row[1] for row in cur.fetchall()}
    assert rows.get("statsbomb") == 3464, rows
    assert rows.get("wyscout") == 1941, rows
    assert rows.get("idsse") == 7, rows
    assert rows.get("metrica") == 3, rows
