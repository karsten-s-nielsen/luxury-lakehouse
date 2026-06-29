"""SQL-text guards for per-match `access_tier` propagation through the dbt marts.

dbt PR CI is parse-only (the Thrift endpoint is unreachable from GitHub runners),
so `dbt build`/`dbt test` run ONLY in the daily live cron. These python tests are
the merge-time guard that the marts still SELECT `access_tier` per row (spec
2026-06-29 §6.4) and that the football2vec career/season aggregates are built
public-only (§6.8 / Task 17). They parse the .sql files directly — no warehouse.

Aggregate SEMANTICS (the public aggregate actually equals the public-row recompute)
are guarded separately by the daily dbt data test
`dbt_project/tests/assert_career_season_public_only.sql`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MARTS_DIR = _REPO_ROOT / "dbt_project" / "models" / "marts"

# Marts that must carry `access_tier` per row for the publish-time split (spec §6.4).
_MARTS = [
    "fct_action_values",
    "fct_action_context",
    "fct_shot_psxg",
    "fct_tracking_frames",
    "fct_player_embeddings",
]

# Career/season aggregates that must source from public-tier rows ONLY (spec §6.8).
_PUBLIC_ONLY_AGGREGATES = [
    "fct_player_embeddings_career",
    "fct_player_embeddings_season",
]


def _mart_sql(model: str) -> str:
    return (_MARTS_DIR / f"{model}.sql").read_text(encoding="utf-8")


@pytest.mark.parametrize("model", _MARTS)
def test_mart_selects_access_tier(model: str) -> None:
    assert "access_tier" in _mart_sql(model), f"{model}.sql must carry access_tier per-row (spec §6.4)"


def test_dim_matches_has_access_tier_and_visibility() -> None:
    sql = _mart_sql("dim_matches")
    assert "access_tier" in sql, "dim_matches.sql must expose access_tier as a per-match attribute (spec §6.4)"
    assert "visibility" in sql, "dim_matches.sql must expose raw visibility as a per-match attribute (spec §6.4)"


@pytest.mark.parametrize("model", _PUBLIC_ONLY_AGGREGATES)
def test_career_season_aggregate_filters_to_public_tier(model: str) -> None:
    sql = _mart_sql(model)
    assert "access_tier = 'public'" in sql or "access_tier='public'" in sql, (
        f"{model}.sql must aggregate from public-tier rows only "
        "(spec §6.8 — a pre-mixed career/season vector cannot be filtered at publish time)"
    )
