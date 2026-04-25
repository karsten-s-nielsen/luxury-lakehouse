"""stg_idsse__tracking must now surface team_id from bronze (PR 5a)."""

from pathlib import Path


def test_staging_includes_team_id_passthrough() -> None:
    src = Path("dbt_project/models/staging/idsse/stg_idsse__tracking.sql").read_text()
    assert "team_id" in src, "team_id column must be surfaced in stg_idsse__tracking"


def test_staging_sql_comments_reference_pr5a() -> None:
    src = Path("dbt_project/models/staging/idsse/stg_idsse__tracking.sql").read_text()
    assert "PR 5a" in src, "rationale comment for team_id passthrough missing"
