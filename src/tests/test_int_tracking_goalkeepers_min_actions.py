"""int_tracking_goalkeepers re-home invariants (PR-1 Task 3).

Source-text pins guarding the three load-bearing properties of the AC re-home:
  1. sources from AC-1 (stg_action_context__values), not the retired TC-1 staging;
  2. THE TRAP data_source filter is present (else GS + statsbomb-360 leak in);
  3. the n_actions>=2 GK mis-tag threshold is present.

These run in Python CI (dbt tests are parse-only there). The live behavioural guard is
assert_tracking_gk_provider_scope.sql (provider scope) in dbt-live-ci.
"""

from pathlib import Path

_MODEL = Path("dbt_project/models/intermediate/int_tracking_goalkeepers.sql")


def test_sourced_from_action_context_not_tracking_context() -> None:
    src = _MODEL.read_text(encoding="utf-8")
    assert "ref('stg_action_context__values')" in src, "int_tracking_goalkeepers must ref AC-1"
    # Check the dbt dependency (ref), not the string — the header comment documents the
    # re-home history and legitimately names the retired TC-1 staging model.
    assert "ref('stg_spadl__tracking_context')" not in src, "TC-1 staging ref must be gone (pipeline retired)"


def test_has_provider_scope_trap_filter() -> None:
    src = _MODEL.read_text(encoding="utf-8")
    assert "data_source in ('idsse', 'metrica', 'skillcorner')" in src, (
        "THE TRAP data_source filter missing — AC would admit gradientsports + statsbomb-360"
    )


def test_has_min_actions_threshold() -> None:
    src = _MODEL.read_text(encoding="utf-8")
    assert "having count(*) >= 2" in src, "n_actions>=2 GK mis-tag threshold was dropped"
