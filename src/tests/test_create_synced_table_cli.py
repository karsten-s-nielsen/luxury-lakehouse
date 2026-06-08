from __future__ import annotations


def test_create_cli_resolves_known_config() -> None:
    from ingestion.refresh_synced_tables import SYNCED_TABLES

    names = {c.name for c in SYNCED_TABLES}
    assert "fct_action_context_synced" in names


def test_create_cli_module_imports() -> None:
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location("create_synced_table", Path("scripts/create_synced_table.py"))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # import-time only; main() not invoked
    assert hasattr(mod, "main")


def test_create_cli_function_local_imports_resolve() -> None:
    """main() imports SYNCED_TABLES + wait_until_online + SdkWriterAdapter function-locally; a
    wrong module path passes the module-load test above but fails at runtime. Assert the EXACT
    import targets resolve (regression: wait_until_online lives in refresh_synced_tables, not
    synced_table_lifecycle)."""
    from ingestion.refresh_synced_tables import SYNCED_TABLES, wait_until_online  # noqa: F401
    from ingestion.synced_table_lifecycle import SdkWriterAdapter  # noqa: F401
