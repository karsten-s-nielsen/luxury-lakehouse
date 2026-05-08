"""Live data-quality checks for fct_xg_predictions_v2.

Validates CI bound ordering and staging-to-mart row preservation on the
live warehouse. Static structural tests (file existence, contract block)
live in src/tests/test_xg_v2_adr013_static.py and run in python-ci.

Requires live Databricks SQL warehouse via DATABRICKS_HOST,
DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN. Skips when those are absent.
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
