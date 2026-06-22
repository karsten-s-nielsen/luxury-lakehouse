"""Merge-time guard for the GK pooled-mart IDSSE fix (no warehouse needed).

The dbt singular test (assert_psxg_pooled_keeps_idsse) only runs in the daily live
build, not at PR time (dbt-ci.yml is parse-only). These pure text assertions run in
python-ci and fail a PR if the NULL-safe season join is reverted or the GK marts are
disabled — the only merge-time protection for the IDSSE-survival fix.
"""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_POOLED = _ROOT / "dbt_project" / "models" / "marts" / "fct_gk_shot_stopping_pooled.sql"
_DBT_PROJECT = _ROOT / "dbt_project" / "dbt_project.yml"


def test_pooled_season_join_is_null_safe():
    sql = _POOLED.read_text(encoding="utf-8").lower()
    # IDSSE has NULL season_id; a plain `=` join silently drops it (NULL = NULL -> UNKNOWN).
    assert "season_id <=> c.season_id" in sql or "season_id is not distinct from" in sql, (
        "fct_gk_shot_stopping_pooled season join must be NULL-safe (<=> or IS NOT DISTINCT FROM) "
        "or all IDSSE keepers vanish from the pooled rollup."
    )


def test_goalkeeper_marts_enabled():
    cfg = _DBT_PROJECT.read_text(encoding="utf-8")
    assert "goalkeeper_enabled: true" in cfg, (
        "The GK marts (incl. fct_gk_shot_stopping_pooled) must be built — goalkeeper_enabled: true."
    )
