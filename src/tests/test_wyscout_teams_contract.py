"""Contract test for stg_wyscout__teams + live bronze.wyscout_teams schema (PR 5a)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

STAGING_PATH = Path("dbt_project/models/staging/wyscout/stg_wyscout__teams.sql")


def test_staging_model_exists() -> None:
    assert STAGING_PATH.exists()


def test_staging_model_selects_team_id() -> None:
    src = STAGING_PATH.read_text()
    assert "as team_id" in src


def test_staging_model_selects_team_name() -> None:
    src = STAGING_PATH.read_text()
    assert "as team_name" in src


def test_staging_model_preserves_bronze_passthrough_cols() -> None:
    src = STAGING_PATH.read_text()
    for col in ("wyId", "officialName", "_ingested_at"):
        assert col in src, f"Bronze passthrough missing: {col}"


databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(v) for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


@requires_databricks
def test_bronze_wyscout_teams_populated() -> None:
    conn = databricks_sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM soccer_analytics.bronze.wyscout_teams")
        n = cur.fetchall()[0][0]
        assert n >= 100, f"bronze.wyscout_teams has too few rows: {n}"
        # Verify expected cols are present
        cur.execute("DESCRIBE TABLE soccer_analytics.bronze.wyscout_teams")
        cols = {r[0] for r in cur.fetchall() if r[0] and not r[0].startswith("#")}
        for expected in ("wyId", "officialName", "name", "city", "area", "type", "_ingested_at"):
            assert expected in cols, f"missing col: {expected}"
    finally:
        conn.close()
