"""Static structural invariants for fct_xg_predictions_v2 (ADR-013).

Asserts that the mart SQL, staging SQL, source declaration, and contract
block follow the ADR-013 pattern without needing a live warehouse.

Live data-quality checks (CI bound ordering, staging=mart row preservation)
run in tests/data_quality/test_dbt_xg_v2_mart.py via data-quality-ci.yml.
"""

from __future__ import annotations

from pathlib import Path


def test_mart_file_exists() -> None:
    assert Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").is_file()


def test_staging_v2_file_exists() -> None:
    assert Path("dbt_project/models/staging/xg/stg_xg__predictions_v2.sql").is_file()


def test_v2_source_declared() -> None:
    yml = Path("dbt_project/models/staging/xg/_xg__sources.yml").read_text(encoding="utf-8")
    assert "- name: xg_predictions_v2" in yml, "xg.xg_predictions_v2 source not declared"


def test_mart_sql_inner_joins_fct_shots_on_shot_id() -> None:
    text = Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").read_text(encoding="utf-8")
    flat = " ".join(text.split())
    assert "inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id" in flat
    assert "s.match_key" in flat
    assert "s.competition_key" in flat
    assert "'enforced': true" in flat
    assert "liquid_clustered_by=['match_key']" in text


def test_mart_contract_block_present() -> None:
    yml = Path("dbt_project/models/marts/_marts__models.yml").read_text(encoding="utf-8")
    assert "- name: fct_xg_predictions_v2" in yml
    block_start = yml.index("- name: fct_xg_predictions_v2\n")
    block = yml[block_start : block_start + 4000]
    expected_cols = (
        "shot_id",
        "match_key",
        "competition_key",
        "competition_id",
        "xg_set_encoder",
        "xg_ci_lower",
        "xg_ci_upper",
    )
    for col in expected_cols:
        assert f"- name: {col}" in block, f"contract block missing column {col}"
