"""PR 6 live invariant — IDSSE rows in fct_passes must have non-NULL
is_progressive populated by the ball-frame tracking lookup.

Pre-PR-6 IDSSE rows had `false` literal (start coords only, no end coords).
Post-PR-6 they evaluate the standard cross-provider distance_to_goal rule
on derived end coords (stg_idsse__passes JOIN stg_idsse__tracking on
match_id, period, end_frame=frame).

Threshold = 0.95 conservative initial floor. Raise to actual measured
rate at first dev rebuild per spec §10 #7. NULL is_progressive is the
honest "unknown" semantics for passes whose end_frame is null OR whose
tracking lookup misses (half-time / ball-out boundaries).
"""

from __future__ import annotations

import os

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)

# Threshold tuned at first dev rebuild — see spec §10 #7. Raise after measurement.
_THRESHOLD = 0.95


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
def test_idsse_is_progressive_coverage(conn) -> None:
    """At least <_THRESHOLD>% of IDSSE rows in fct_passes must have
    non-NULL is_progressive."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) AS total, count(is_progressive) AS non_null "
        "FROM soccer_analytics.dev_gold.fct_passes "
        "WHERE data_source = 'idsse'"
    )
    row = cur.fetchall()[0]
    total = int(row[0])
    non_null = int(row[1])

    if total == 0:
        pytest.skip("fct_passes has zero IDSSE rows — pipeline not yet built")

    rate = non_null / total
    assert rate >= _THRESHOLD, (
        f"IDSSE is_progressive non-NULL rate {rate:.4f} below {_THRESHOLD} threshold "
        f"(total={total}, non_null={non_null}). Investigate ball_at_end_frame JOIN "
        "in stg_idsse__passes — likely an end_frame ↔ stg_idsse__tracking miss."
    )


@requires_databricks
def test_idsse_has_some_progressive_passes(conn) -> None:
    """Smoke check: pre-PR-6 IDSSE had 0 progressive passes (literal false).
    Post-PR-6 the count must be > 0 OR is_progressive is NULL for all rows.

    This catches regressions where the new CASE expression resolves to NULL
    for ALL rows (e.g., the LEFT JOIN to ball_at_end_frame misses every
    pass due to a key-format mismatch).
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) AS total, "
        "  sum(case when is_progressive then 1 else 0 end) AS prog "
        "FROM soccer_analytics.dev_gold.fct_passes "
        "WHERE data_source = 'idsse'"
    )
    row = cur.fetchall()[0]
    total = int(row[0])
    prog = int(row[1] or 0)

    if total == 0:
        pytest.skip("fct_passes has zero IDSSE rows")

    assert prog > 0, (
        f"IDSSE has zero progressive passes ({total} total IDSSE rows). "
        "Pre-PR-6 this was literal false; post-PR-6 it should be the standard "
        "cross-provider distance_to_goal rule. Zero progressives suggests the "
        "ball_at_end_frame JOIN miss is universal — investigate."
    )
