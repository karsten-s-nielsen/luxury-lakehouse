"""Fresh-install robustness: maintenance steps tolerate not-yet-created synced tables.

A configured ``SYNCED_TABLES`` entry whose Databricks synced table has not been
created yet is the EXPECTED state on a fresh lakehouse install (and for any mart
whose first sync hasn't run, e.g. ``fct_action_context_synced`` before AC-1's
gold mart is populated). The Lakebase maintenance scripts must degrade
gracefully — skip-with-warning — rather than hard-failing, while still failing
loudly on genuine errors.

These tests pin that behavior at the two layers it lives:
  - the shared SDK-message classifier ``is_synced_table_not_found``
  - the PostgreSQL-side ``create_indexes._is_missing_relation`` (SQLSTATE 42P01)
  - the real ``grant_synced_table_permissions._enumerate_pipelines`` skip path
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from ingestion.refresh_synced_tables import is_synced_table_not_found


class TestIsSyncedTableNotFound:
    """The shared SDK-message classifier."""

    @pytest.mark.parametrize(
        "message",
        [
            "Synced table soccer_analytics.dev_gold.fct_action_context_synced not found",
            "RESOURCE_DOES_NOT_EXIST: synced table does not exist",
            "The resource does not exist",
            "No synced table with name synced_tables/...",
            'relation "dev_gold.fct_action_context_synced" does not exist',  # PG message too
        ],
    )
    def test_not_found_messages_classified_true(self, message: str) -> None:
        assert is_synced_table_not_found(Exception(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "PERMISSION_DENIED: caller lacks CAN_MANAGE",
            "INTERNAL_SERVER_ERROR",
            "connection reset by peer",
            "Synced table is OFFLINE_FAILED",  # exists but unhealthy — must NOT be skipped
            "pipeline update timed out",
        ],
    )
    def test_genuine_errors_classified_false(self, message: str) -> None:
        assert is_synced_table_not_found(Exception(message)) is False

    def test_case_insensitive(self) -> None:
        assert is_synced_table_not_found(Exception("RESOURCE NOT FOUND")) is True


def _load_script_module(name: str, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load a scripts/*.py module by file path (scripts/ is not a package on sys.path)."""
    # create_indexes reads DATABRICKS_HOST at import time — provide a dummy so the
    # module is importable in offline CI (no live call is ever made here).
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.databricks.invalid")
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestCreateIndexesMissingRelation:
    """create_indexes._is_missing_relation classifies PG UndefinedTable (42P01)."""

    def test_pgcode_42p01_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pytest.importorskip("psycopg2")
        mod = _load_script_module("create_indexes", monkeypatch)
        exc = SimpleNamespace(pgcode="42P01")
        assert mod._is_missing_relation(exc) is True  # type: ignore[arg-type]

    def test_other_pgcode_is_not_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pytest.importorskip("psycopg2")
        mod = _load_script_module("create_indexes", monkeypatch)
        # 42501 = insufficient_privilege — a genuine error, must NOT be skipped.
        assert mod._is_missing_relation(SimpleNamespace(pgcode="42501")) is False  # type: ignore[arg-type]
        assert mod._is_missing_relation(Exception("no pgcode attr")) is False


class TestEnumeratePipelinesSkip:
    """grant_synced_table_permissions._enumerate_pipelines skips not-yet-created tables."""

    def test_missing_table_skipped_existing_resolved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = _load_script_module("grant_synced_table_permissions", monkeypatch)

        # Two configured tables: one created (resolves a pipeline_id), one not.
        configs = [
            SimpleNamespace(name="fct_shots_synced", schema_override=None),
            SimpleNamespace(name="fct_action_context_synced", schema_override=None),
        ]
        monkeypatch.setattr(mod, "SYNCED_TABLES", configs)

        def fake_get_synced_table(name: str) -> object:
            if "fct_action_context_synced" in name:
                raise Exception("Synced table fct_action_context_synced not found")
            return SimpleNamespace(status=SimpleNamespace(pipeline_id="pid-shots-123"))

        fake_ws = SimpleNamespace(postgres=SimpleNamespace(get_synced_table=fake_get_synced_table))

        resolved = mod._enumerate_pipelines(fake_ws)  # type: ignore[arg-type]

        # The created table resolves; the not-yet-created one is skipped (no raise).
        assert resolved == [("fct_shots_synced", mod.DEFAULT_SCHEMA, "pid-shots-123")]

    def test_genuine_error_still_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = _load_script_module("grant_synced_table_permissions", monkeypatch)
        monkeypatch.setattr(mod, "SYNCED_TABLES", [SimpleNamespace(name="fct_shots_synced", schema_override=None)])

        def fake_get_synced_table(name: str) -> object:
            raise Exception("PERMISSION_DENIED: caller lacks CAN_MANAGE")

        fake_ws = SimpleNamespace(postgres=SimpleNamespace(get_synced_table=fake_get_synced_table))

        with pytest.raises(Exception, match="PERMISSION_DENIED"):
            mod._enumerate_pipelines(fake_ws)  # type: ignore[arg-type]
