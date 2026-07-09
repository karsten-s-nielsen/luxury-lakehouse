"""Static SQL-text guards for the pre-shot xG v3 mart chain (Task 1.10 + 2.4).

dbt PR CI is parse-only (the Thrift endpoint is unreachable from GitHub runners),
so the singular tests under `dbt_project/tests/*.sql` run ONLY in the daily live
cron — they are DAILY guards, not PR gates (reference_dbt_ci_parse_only_tests_daily).
These python tests are the merge-time guard that the three new singular tests still
encode their key invariants, and that the `fct_shot_xg` / `stg_xg__shot_predictions`
models + `fct_xg_predictions_v2` table keep their contracts. They parse the .sql / .yml
files directly — no warehouse.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODELS = _REPO_ROOT / "dbt_project" / "models"
_TESTS = _REPO_ROOT / "dbt_project" / "tests"


def _flat(path: Path) -> str:
    """Return the file's text with runs of whitespace collapsed to single spaces."""
    return " ".join(path.read_text(encoding="utf-8").split())


# ---------------------------------------------------------------------------
# Model + source + contract presence (Task 1.10)
# ---------------------------------------------------------------------------


def test_staging_shot_predictions_file_exists() -> None:
    assert (_MODELS / "staging" / "xg" / "stg_xg__shot_predictions.sql").is_file()


def test_shot_xg_mart_file_exists() -> None:
    assert (_MODELS / "marts" / "fct_shot_xg.sql").is_file()


def test_shot_predictions_source_declared() -> None:
    yml = (_MODELS / "staging" / "xg" / "_xg__sources.yml").read_text(encoding="utf-8")
    assert "- name: xg_shot_predictions" in yml, "xg.xg_shot_predictions source not declared"


def test_shot_xg_mart_is_contract_enforced_and_key_resolved() -> None:
    flat = _flat(_MODELS / "marts" / "fct_shot_xg.sql")
    # ADR-013: writer emits native ids + predictions; the mart resolves Kimball
    # surrogates via an INNER JOIN to the identity fact on (match_key, action_id).
    assert "'enforced': true" in flat
    assert "inner join {{ ref('fct_action_values') }}" in flat
    assert "match_key" in flat and "action_id" in flat
    # Surrogates come from the identity fact, not re-derived.
    assert "competition_key" in flat
    assert "player_key" in flat
    assert "team_key" in flat


def test_shot_xg_contract_block_declares_columns() -> None:
    yml = (_MODELS / "marts" / "_marts__models.yml").read_text(encoding="utf-8")
    assert "- name: fct_shot_xg\n" in yml
    block_start = yml.index("- name: fct_shot_xg\n")
    block = yml[block_start : block_start + 4000]
    expected_cols = (
        "match_key",
        "action_id",
        "data_source",
        "xg",
        "xg_ci_low",
        "xg_ci_high",
        "scoring_mode",
        "ood_flag",
        "competition_key",
        "team_key",
        "player_key",
    )
    for col in expected_cols:
        assert f"- name: {col}" in block, f"fct_shot_xg contract block missing column {col}"


# ---------------------------------------------------------------------------
# Singular test invariants (Task 1.10)
# ---------------------------------------------------------------------------


def test_shot_xg_key_in_action_values_is_antijoin_on_shot_key() -> None:
    flat = _flat(_TESTS / "assert_shot_xg_key_in_action_values.sql")
    assert "{{ ref('fct_shot_xg') }}" in flat
    assert "left join {{ ref('fct_action_values') }}" in flat
    # (match_key, action_id) is the shot key — never action_id alone.
    assert "sx.match_key = av.match_key" in flat
    assert "sx.action_id = av.action_id" in flat
    assert "av.match_key is null" in flat


def test_av_ac_consistency_is_key_antijoin_without_coordinates() -> None:
    sql = (_TESTS / "assert_av_ac_action_id_consistency.sql").read_text(encoding="utf-8")
    flat = " ".join(sql.split())
    assert "left join {{ ref('fct_action_context') }}" in flat
    assert "{{ ref('fct_action_values') }}" in flat
    # (match_key, action_id) anti-join keys.
    assert "av.match_key = ac.match_key" in flat
    assert "av.action_id = ac.action_id" in flat
    assert "ac.match_key is null" in flat
    # Restricted to tracking providers (fct_action_context only exists for them).
    assert "av.data_source in ('gradientsports', 'skillcorner', 'idsse', 'metrica')" in flat
    # N3: KEY anti-join ONLY — a raw coordinate equality would false-fail on an
    # orientation-convention mismatch (AV acting-team-LTR vs AC home-LTR). Check the QUERY
    # LOGIC only (strip ``--`` comments first): the explanatory comment in the .sql names the
    # columns precisely to document that it does NOT compare them, and must not false-trip this
    # guard. Comments are documentation; the coordinate check targets executable SQL.
    code_only = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    assert "start_x" not in code_only, "cross-mart consistency test must not compare start_x (spec N3)"
    assert "start_y" not in code_only, "cross-mart consistency test must not compare start_y (spec N3)"


# ---------------------------------------------------------------------------
# fct_xg_predictions_v2 -> TABLE projecting fct_shot_xg (Task 2.4 / C-b)
# ---------------------------------------------------------------------------


def test_v2_is_materialized_table_over_shot_xg() -> None:
    flat = _flat(_MODELS / "marts" / "fct_xg_predictions_v2.sql")
    # Materialized as a TABLE (not a view): a SNAPSHOT Lakebase synced table must
    # source from a Delta table (C-b). Enforced contract + bridge unchanged.
    assert "materialized='table'" in flat
    assert "{{ ref('fct_shot_xg') }}" in flat
    # Legacy schema keeps the enforced contract with the exact legacy columns.
    assert "'enforced': true" in flat
    # Legacy coverage restriction: only the event-only providers v2 ever scored.
    assert "data_source in ('statsbomb', 'wyscout')" in flat
    # Bridge back to shot_id via fct_action_values.original_event_id -> fct_shots.event_id.
    assert "original_event_id" in flat
    assert "{{ ref('fct_shots') }}" in flat
    # Legacy column mapping.
    assert "as xg_set_encoder" in flat
    assert "as xg_ci_lower" in flat
    assert "as xg_ci_upper" in flat


def test_v2_view_shot_id_1to1_test_is_fanout_guard() -> None:
    flat = _flat(_TESTS / "assert_xg_v2_view_shot_id_1to1.sql")
    assert "{{ ref('fct_xg_predictions_v2') }}" in flat
    assert "group by shot_id" in flat
    assert "having count(*) > 1" in flat
