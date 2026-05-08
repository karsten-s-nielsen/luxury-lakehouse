"""Static structural invariants for fct_pausa_values (ADR-013).

Asserts that the mart SQL, staging SQL, writer target, workflow card, and
surrogate-key recipe follow the ADR-013 pattern without needing a live
warehouse.

Live data-quality checks (row count, JOIN coverage, md5 surrogate format)
run in tests/data_quality/test_pausa_adr013_compliance.py via data-quality-ci.yml.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MART_PATH = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_pausa_values.sql"
_STG_PATH = _REPO_ROOT / "dbt_project" / "models" / "staging" / "pausa" / "stg_pausa__values.sql"


class TestFctPausaValuesAdr013Compliance:
    """Static structural invariants for the ADR-013 second application."""

    def test_mart_file_exists(self) -> None:
        assert _MART_PATH.exists(), (
            f"ADR-013 mart file missing: {_MART_PATH}. "
            "PR 7 requires fct_pausa_values.sql to live alongside fct_xg_predictions_v2.sql."
        )

    def test_mart_has_contract_enforced(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        assert re.search(r"contract\s*=\s*\{\s*['\"]enforced['\"]\s*:\s*true\s*\}", sql), (
            f"fct_pausa_values.sql must declare contract={{'enforced': true}}. Source: {_MART_PATH}"
        )

    def test_mart_inner_joins_fct_passes_on_pass_id(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        assert re.search(
            r"inner\s+join\s+\{\{\s*ref\(\s*['\"]fct_passes['\"]\s*\)\s*\}\}.*on\s+\w+\.pass_id\s*=\s*\w+\.pass_id",
            sql,
            re.IGNORECASE | re.DOTALL,
        ), (
            "fct_pausa_values.sql must INNER JOIN fct_passes ON pass_id per ADR-013 "
            f"normative §3 (identity-fact key inheritance). Source: {_MART_PATH}"
        )

    def test_writer_targets_bronze(self) -> None:
        """src/ingestion/pausa.py must write to bronze, not gold."""
        from ingestion import pausa

        assert pausa._BRONZE_SCHEMA == "bronze"
        assert pausa._TABLE_NAME == "pausa_values"

    def test_workflow_card_lists_dbt_model(self) -> None:
        """wf-obso-pausa.yaml must declare dbt_model: fct_pausa_values."""
        card_path = _REPO_ROOT / "workflow-cards" / "wf-obso-pausa.yaml"
        assert card_path.exists()
        text = card_path.read_text(encoding="utf-8")
        assert "dbt_model: fct_pausa_values" in text, (
            "wf-obso-pausa.yaml outputs.tables must declare dbt_model: fct_pausa_values "
            "for the new ADR-013 gold mart entry. Source: " + str(card_path)
        )
        assert "bronze.pausa_values" in text, (
            "wf-obso-pausa.yaml outputs.tables must list the bronze raw target "
            "(catalog.bronze.pausa_values). Source: " + str(card_path)
        )

    def test_staging_resolves_surrogate_via_dbt_utils_macro(self) -> None:
        sql = _STG_PATH.read_text(encoding="utf-8")
        assert "dbt_utils.generate_surrogate_key" in sql, (
            "stg_pausa__values.sql must compute the surrogate pass_id via "
            "dbt_utils.generate_surrogate_key([match_key, event_id, data_source]) "
            "to match fct_passes' recipe. Source: " + str(_STG_PATH)
        )
        flat = " ".join(sql.split())
        assert re.search(
            r"generate_surrogate_key\(\s*\[\s*['\"][^'\"]*match_key['\"]\s*,"
            r"\s*['\"][^'\"]*event_id['\"]\s*,\s*['\"][^'\"]*data_source['\"]\s*,?\s*\]",
            flat,
        ), (
            "stg_pausa__values surrogate-key inputs must be ordered "
            "[match_key, event_id, data_source] to match fct_passes; "
            "any reorder produces a different md5 and silently drops every JOIN."
        )

    def test_staging_joins_dim_matches_to_resolve_match_key(self) -> None:
        sql = _STG_PATH.read_text(encoding="utf-8")
        flat = " ".join(sql.split())
        assert re.search(
            r"inner\s+join\s+\{\{\s*ref\(\s*['\"]dim_matches['\"]\s*\)\s*\}\}",
            flat,
            re.IGNORECASE,
        ), (
            "stg_pausa__values must INNER JOIN dim_matches to resolve match_key "
            "before computing the surrogate. Source: " + str(_STG_PATH)
        )
