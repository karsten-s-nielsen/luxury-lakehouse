"""Unit tests for scripts/fix_event_log_ownership.py helpers.

Tests the pure helpers (no Databricks I/O): event_log name derivation,
notebook-source construction, and ownership classification. The I/O-heavy
Phase A/B/C/D functions are exercised only via the dry-run self-test the
controller runs after static checks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# scripts/ is not on the default Python path — insert it so the script module
# imports cleanly as a top-level module.
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import fix_event_log_ownership as fix_mod  # type: ignore[import-not-found]  # noqa: E402

# ---------------------------------------------------------------------------
# _event_log_table_name
# ---------------------------------------------------------------------------


def test_event_log_table_name_converts_dashes_to_underscores() -> None:
    assert (
        fix_mod._event_log_table_name("4ea189db-aa43-4144-8825-da54cf965b7f")
        == "event_log_4ea189db_aa43_4144_8825_da54cf965b7f"
    )


def test_event_log_table_name_no_dashes() -> None:
    assert fix_mod._event_log_table_name("abc") == "event_log_abc"


def test_event_log_table_name_empty_string() -> None:
    assert fix_mod._event_log_table_name("") == "event_log_"


# ---------------------------------------------------------------------------
# _build_alter_owner_notebook_source
# ---------------------------------------------------------------------------


def test_build_alter_owner_notebook_source_single_table() -> None:
    src = fix_mod._build_alter_owner_notebook_source(
        ["soccer_analytics.dev_gold.event_log_abc"],
        "dbt-owners-dev",
    )
    # Pure SQL notebook header, not Python-with-magic
    assert src.startswith("-- Databricks notebook source\n")
    assert "ALTER TABLE soccer_analytics.dev_gold.event_log_abc OWNER TO `dbt-owners-dev`" in src
    # No magic cells
    assert "# MAGIC" not in src
    assert "%sql" not in src
    # single table should have NO COMMAND separator
    assert "COMMAND ----------" not in src


def test_build_alter_owner_notebook_source_multiple_tables() -> None:
    src = fix_mod._build_alter_owner_notebook_source(
        [
            "soccer_analytics.dev_gold.event_log_a",
            "soccer_analytics.dev_gold.event_log_b",
            "soccer_analytics.dev_gold.event_log_c",
        ],
        "dbt-owners-dev",
    )
    assert src.count("ALTER TABLE") == 3
    # Two COMMAND separators between 3 cells
    assert src.count("-- COMMAND ----------") == 2
    # All three tables in order
    idx_a = src.find("event_log_a")
    idx_b = src.find("event_log_b")
    idx_c = src.find("event_log_c")
    assert 0 < idx_a < idx_b < idx_c


def test_build_alter_owner_notebook_source_backticks_group_name() -> None:
    """Group names with hyphens need backticks to parse as identifiers."""
    src = fix_mod._build_alter_owner_notebook_source(
        ["a.b.c"],
        "my-group-with-hyphens",
    )
    assert "OWNER TO `my-group-with-hyphens`" in src


def test_build_alter_owner_notebook_source_ends_with_newline() -> None:
    """Notebook sources should end in a newline for clean upload."""
    src = fix_mod._build_alter_owner_notebook_source(["a.b.c"], "grp")
    assert src.endswith("\n")


# ---------------------------------------------------------------------------
# _classify_table_ownership
# ---------------------------------------------------------------------------


_SP = "008b207b-96a8-4d54-b185-a77479a55abe"


def test_classify_table_ownership_all_correct() -> None:
    owners = {"a": "dbt-owners-dev", "b": "dbt-owners-dev"}
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert sorted(already) == ["a", "b"]
    assert needs_fix == []
    assert skip == []
    assert missing == []


def test_classify_table_ownership_mixed_with_fixer() -> None:
    """With fixer_principal set, tables owned by a third principal are skip_wrong_owner."""
    owners = {
        "a": "dbt-owners-dev",  # correct
        "b": _SP,  # SP-owned → fixable
        "c": "dbt-owners-dev",  # correct
        "d": "karstenskyt@gmail.com",  # user-owned → neither target nor fixer → skip
    }
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert sorted(already) == ["a", "c"]
    assert needs_fix == ["b"]
    assert skip == ["d"]
    assert missing == []


def test_classify_table_ownership_fixer_none_legacy_behaviour() -> None:
    """Without fixer_principal (legacy mode), any non-target owner goes to needs_fix."""
    owners = {
        "a": "dbt-owners-dev",
        "b": _SP,
        "c": "karstenskyt@gmail.com",
    }
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev")
    assert already == ["a"]
    assert sorted(needs_fix) == ["b", "c"]
    assert skip == []
    assert missing == []


def test_classify_table_ownership_missing() -> None:
    """The classifier can only report on what it's given; the caller tracks missing."""
    owners = {"a": "dbt-owners-dev"}
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert already == ["a"]
    assert needs_fix == []
    assert skip == []
    assert missing == []


def test_classify_table_ownership_case_sensitive() -> None:
    """Owner comparison is exact — 'DBT-OWNERS-DEV' != 'dbt-owners-dev'."""
    owners = {"a": "DBT-OWNERS-DEV"}
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert already == []
    assert needs_fix == []
    # "DBT-OWNERS-DEV" is neither target nor fixer → skip
    assert skip == ["a"]
    assert missing == []


def test_classify_table_ownership_empty_input() -> None:
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership({}, "dbt-owners-dev", fixer_principal=_SP)
    assert already == []
    assert needs_fix == []
    assert skip == []
    assert missing == []


def test_classify_table_ownership_sp_owned_table_needs_fix() -> None:
    """SP-owned tables are classified as needs_fix (the SP can ALTER them)."""
    owners = {"a": _SP, "b": _SP}
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert already == []
    assert sorted(needs_fix) == ["a", "b"]
    assert skip == []
    assert missing == []


def test_classify_table_ownership_workflow_cost_live_case() -> None:
    """Real-world case: workflow_cost_live_synced event_log owned by user, not SP.

    Should be skip_wrong_owner so the SP-notebook doesn't try to ALTER it
    and hit PERMISSION_DENIED.
    """
    owners = {"soccer_analytics.observability.event_log_x": "karstenskyt@gmail.com"}
    already, needs_fix, skip, missing = fix_mod._classify_table_ownership(owners, "dbt-owners-dev", fixer_principal=_SP)
    assert already == []
    assert needs_fix == []
    assert skip == ["soccer_analytics.observability.event_log_x"]
    assert missing == []


# ---------------------------------------------------------------------------
# _validate_group_name (defensive SQL injection guard for owner group)
# ---------------------------------------------------------------------------


def test_validate_group_name_accepts_dbt_owners_dev() -> None:
    assert fix_mod._validate_group_name("dbt-owners-dev") is True


def test_validate_group_name_accepts_alphanumeric_with_hyphens() -> None:
    assert fix_mod._validate_group_name("my-group-123") is True


def test_validate_group_name_rejects_sql_injection_attempt() -> None:
    assert fix_mod._validate_group_name("a`; DROP TABLE x; --") is False


def test_validate_group_name_rejects_backticks() -> None:
    assert fix_mod._validate_group_name("has`backtick") is False


def test_validate_group_name_rejects_spaces() -> None:
    assert fix_mod._validate_group_name("has space") is False


def test_validate_group_name_rejects_empty() -> None:
    assert fix_mod._validate_group_name("") is False


# ---------------------------------------------------------------------------
# _validate_uuid (for --sp-application-id)
# ---------------------------------------------------------------------------


def test_validate_uuid_accepts_canonical_lowercase() -> None:
    assert fix_mod._validate_uuid("008b207b-96a8-4d54-b185-a77479a55abe") is True


def test_validate_uuid_accepts_canonical_uppercase() -> None:
    assert fix_mod._validate_uuid("008B207B-96A8-4D54-B185-A77479A55ABE") is True


def test_validate_uuid_rejects_missing_dashes() -> None:
    # pragma: allowlist secret — this is a canonical test fixture UUID (ingestion SP
    # application_id, a public identifier), rendered here without dashes to exercise
    # the validator's reject path. Not a credential.
    assert fix_mod._validate_uuid("008b207b96a84d54b185a77479a55abe") is False  # pragma: allowlist secret


def test_validate_uuid_rejects_wrong_length() -> None:
    assert fix_mod._validate_uuid("008b207b-96a8-4d54-b185") is False


def test_validate_uuid_rejects_non_hex() -> None:
    assert fix_mod._validate_uuid("008b207b-96a8-4d54-b185-a77479a55abz") is False


def test_validate_uuid_rejects_empty() -> None:
    assert fix_mod._validate_uuid("") is False


# ---------------------------------------------------------------------------
# _event_log_fqn (compose a fully qualified name from catalog.schema + pipeline_id)
# ---------------------------------------------------------------------------


def test_event_log_fqn_default_schema() -> None:
    fqn = fix_mod._event_log_fqn(
        catalog="soccer_analytics",
        schema="dev_gold",
        pipeline_id="4ea189db-aa43-4144-8825-da54cf965b7f",
    )
    assert fqn == "soccer_analytics.dev_gold.event_log_4ea189db_aa43_4144_8825_da54cf965b7f"


def test_event_log_fqn_observability_schema() -> None:
    fqn = fix_mod._event_log_fqn(
        catalog="soccer_analytics",
        schema="observability",
        pipeline_id="abc-def",
    )
    assert fqn == "soccer_analytics.observability.event_log_abc_def"


# ---------------------------------------------------------------------------
# _get_pipeline_id — validates API response (defensive UUID check)
# ---------------------------------------------------------------------------


def _stub_sdk_meta(pipeline_id: str | None) -> MagicMock:
    """Return a MagicMock that looks like a SyncedTable with status.pipeline_id."""
    meta = MagicMock()
    if pipeline_id is not None:
        meta.status.pipeline_id = pipeline_id
    else:
        meta.status = None
    return meta


def test_get_pipeline_id_accepts_valid_uuid() -> None:
    """Happy path: SDK returns a canonical UUID — function returns it."""
    mock_ws = MagicMock()
    mock_ws.postgres.get_synced_table.return_value = _stub_sdk_meta(
        "4ea189db-aa43-4144-8825-da54cf965b7f",
    )
    pid = fix_mod._get_pipeline_id(
        ws=mock_ws,
        catalog="soccer_analytics",
        schema="dev_gold",
        table="dim_competitions_synced",
    )
    assert pid == "4ea189db-aa43-4144-8825-da54cf965b7f"
    mock_ws.postgres.get_synced_table.assert_called_once_with(
        name="synced_tables/soccer_analytics.dev_gold.dim_competitions_synced",
    )


def test_get_pipeline_id_rejects_non_uuid() -> None:
    """Defensive guard: SDK returned a non-UUID string — RuntimeError."""
    mock_ws = MagicMock()
    mock_ws.postgres.get_synced_table.return_value = _stub_sdk_meta("not-a-uuid")
    with pytest.raises(RuntimeError, match="non-UUID pipeline_id"):
        fix_mod._get_pipeline_id(
            ws=mock_ws,
            catalog="soccer_analytics",
            schema="dev_gold",
            table="dim_competitions_synced",
        )


def test_get_pipeline_id_rejects_sql_injection_in_pipeline_id() -> None:
    """A pipeline_id containing backticks / semicolons must not pass through."""
    mock_ws = MagicMock()
    mock_ws.postgres.get_synced_table.return_value = _stub_sdk_meta(
        "abc-def`; DROP TABLE x; --",
    )
    with pytest.raises(RuntimeError, match="non-UUID pipeline_id"):
        fix_mod._get_pipeline_id(
            ws=mock_ws,
            catalog="soccer_analytics",
            schema="dev_gold",
            table="dim_competitions_synced",
        )


def test_get_pipeline_id_missing_pipeline_id() -> None:
    """SDK returned status with no pipeline_id — RuntimeError."""
    mock_ws = MagicMock()
    meta = MagicMock()
    meta.status.pipeline_id = None
    mock_ws.postgres.get_synced_table.return_value = meta
    with pytest.raises(RuntimeError, match="no pipeline_id"):
        fix_mod._get_pipeline_id(
            ws=mock_ws,
            catalog="soccer_analytics",
            schema="dev_gold",
            table="dim_competitions_synced",
        )


def test_get_pipeline_id_missing_status() -> None:
    """SDK returned no status object — RuntimeError."""
    mock_ws = MagicMock()
    mock_ws.postgres.get_synced_table.return_value = _stub_sdk_meta(None)
    with pytest.raises(RuntimeError, match="no pipeline_id"):
        fix_mod._get_pipeline_id(
            ws=mock_ws,
            catalog="soccer_analytics",
            schema="dev_gold",
            table="dim_competitions_synced",
        )
