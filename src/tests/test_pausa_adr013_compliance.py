"""ADR-013 second-application compliance test for fct_pausa_values.

PR 7 promotes ``fct_pausa_values`` from a Python writer direct-write to a
dbt-built mart following the [ADR-013](docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md)
pattern (consumer-side ML inference output: Python writer → bronze raw →
dbt staging → gold mart with contract).

This test asserts the static structural invariants without needing a live
warehouse connection:

1. The mart file ``fct_pausa_values.sql`` exists at the canonical path.
2. The mart has ``contract: enforced: true``.
3. The mart resolves keys via ``INNER JOIN fct_passes ON pass_id`` (not
   via dim_matches/dim_teams/dim_players JOINs in the mart layer — those
   live in fct_passes).
4. The Python writer (``src/ingestion/pausa.py``) targets the bronze schema
   (``_BRONZE_SCHEMA == "bronze"``) and emits the ``pausa_values`` table.

Mirrors the structural-invariant pattern of test_xg_v2_adr013_compliance
(if it exists) — extends to the second ADR-013 application.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MART_PATH = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_pausa_values.sql"


class TestFctPausaValuesAdr013Compliance:
    """Static structural invariants for the ADR-013 second application."""

    def test_mart_file_exists(self) -> None:
        assert _MART_PATH.exists(), (
            f"ADR-013 mart file missing: {_MART_PATH}. "
            "PR 7 requires fct_pausa_values.sql to live alongside fct_xg_predictions_v2.sql."
        )

    def test_mart_has_contract_enforced(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        # Match either dict-style or YAML-mapping-style contract config
        # 'contract={'enforced': true}' OR 'contract: {enforced: true}'.
        assert re.search(r"contract\s*=\s*\{\s*['\"]enforced['\"]\s*:\s*true\s*\}", sql), (
            f"fct_pausa_values.sql must declare contract={{'enforced': true}}. Source: {_MART_PATH}"
        )

    def test_mart_inner_joins_fct_passes_on_pass_id(self) -> None:
        sql = _MART_PATH.read_text(encoding="utf-8")
        # ADR-013 normative: identity-fact INNER JOIN inheritance.
        # Match either {{ ref('fct_passes') }} or fct_passes via ref macro.
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
