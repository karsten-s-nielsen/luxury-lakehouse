"""Static structural invariants for fct_xg_predictions_v2 without a warehouse.

Post-Task-2.4 (§C2): fct_xg_predictions_v2 is no longer the ADR-013 v2 mart —
it is a BACK-COMPAT VIEW projecting fct_shot_xg (canonical-SPADL xg_model_v3)
into the exact legacy schema, so existing consumers (Taipy, the
fct_xg_predictions_v2_synced Lakebase table) keep working. These tests assert
that view shape (materialization, bridge, legacy columns, coverage restriction)
and that the source declaration + contract block are intact.

Live data-quality checks (CI bound ordering, staging=mart row preservation)
run in the daily dbt cron via the singular tests under dbt_project/tests/.
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


def test_mart_is_backcompat_view_over_shot_xg() -> None:
    text = Path("dbt_project/models/marts/fct_xg_predictions_v2.sql").read_text(encoding="utf-8")
    flat = " ".join(text.split())
    # Now a VIEW over fct_shot_xg (not the ADR-013 table); enforced contract kept.
    assert "materialized='view'" in flat
    assert "'enforced': true" in flat
    assert "{{ ref('fct_shot_xg') }}" in flat
    # shot_id reconstructed via the (match_key, action_id) -> original_event_id ->
    # fct_shots.event_id bridge; legacy Kimball columns carried from fct_shots.
    assert "{{ ref('fct_shots') }}" in flat
    assert "original_event_id" in flat
    assert "s.match_key" in flat
    assert "s.competition_key" in flat
    # Legacy coverage restriction + legacy column names.
    assert "data_source in ('statsbomb', 'wyscout')" in flat
    assert "as xg_set_encoder" in flat
    assert "as xg_ci_lower" in flat
    assert "as xg_ci_upper" in flat


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
