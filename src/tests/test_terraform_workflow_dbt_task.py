"""D59: assert Terraform changes for the dbt_build task and ingestion SP grants.

Tests parse the Terraform module HCL directly via regex-based content checks.
A full ``terraform show -json`` parse is overkill for these structural checks
and would require a working tfplan in the test environment.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_catalog_main_tf() -> str:
    return (REPO_ROOT / "terraform" / "modules" / "catalog" / "main.tf").read_text(encoding="utf-8")


def _read_workflows_main_tf() -> str:
    return (REPO_ROOT / "terraform" / "modules" / "workflows" / "main.tf").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Ingestion SP grants on gold schema (D59-8)
# ---------------------------------------------------------------------------


def test_ingestion_sp_has_create_table_on_gold() -> None:
    """D59 requires the ingestion SP to materialize tables in dev_gold.

    Verified at terraform/modules/catalog/main.tf:118-130 area.
    """
    src = _read_catalog_main_tf()
    gold_grant_idx = src.find("ingestion_sp_gold_schema")
    assert gold_grant_idx != -1, "ingestion_sp_gold_schema grant resource missing"
    block = src[gold_grant_idx : gold_grant_idx + 700]
    assert "CREATE_TABLE" in block, (
        "ingestion_sp_gold_schema grant must include CREATE_TABLE for dbt to materialize tables"
    )
    assert "MODIFY" in block, "ingestion_sp_gold_schema grant must include MODIFY for dbt to write to existing tables"


def test_ingestion_sp_has_create_table_on_silver() -> None:
    """D59 also requires CREATE_TABLE + MODIFY on dev_silver because dbt staging
    models materialize as views in the silver schema and seeds materialize as
    tables in silver.
    """
    src = _read_catalog_main_tf()
    silver_grant_idx = src.find("ingestion_sp_silver_schema")
    assert silver_grant_idx != -1, "ingestion_sp_silver_schema grant resource missing"
    block = src[silver_grant_idx : silver_grant_idx + 700]
    assert "CREATE_TABLE" in block, (
        "ingestion_sp_silver_schema grant must include CREATE_TABLE for dbt to "
        "materialize staging-layer views and seed tables"
    )
    assert "MODIFY" in block, "ingestion_sp_silver_schema grant must include MODIFY for dbt staging refreshes"


# ---------------------------------------------------------------------------
# dbt_build task in workflows module (D59-9)
# ---------------------------------------------------------------------------


def test_dbt_build_task_exists() -> None:
    src = _read_workflows_main_tf()
    assert 'task_key        = "dbt_build"' in src or 'task_key = "dbt_build"' in src, (
        "workflows module must contain a task with task_key = 'dbt_build'"
    )


def test_dbt_build_task_uses_correct_entry_point_and_environment() -> None:
    src = _read_workflows_main_tf()
    idx = src.find('"dbt_build"')
    assert idx != -1
    # Window expanded from 1500 to 2500 chars after PR-Cycle-B (2026-05-01)
    # added 4 missing depends_on edges (compute_pausa, compute_elastic_sync,
    # backfill_statsbomb_360, compute_embeddings_360) — the 1500-char window
    # was no longer wide enough to reach the environment_key declaration.
    window = src[idx : idx + 2500]
    assert "python_wheel_task" in window, "dbt_build task must use python_wheel_task"
    assert 'entry_point  = "dbt_build"' in window or 'entry_point = "dbt_build"' in window, (
        "dbt_build task entry_point must be 'dbt_build'"
    )
    assert 'environment_key = "dbt"' in window, "dbt_build task must use the dbt environment_key"


def test_dbt_build_task_depends_on_twelve_leaf_compute_tasks() -> None:
    """PR-Cycle-B (2026-05-01): the leaf-fan-in grew from 8 to 12 after
    adding the 4 previously-missing edges (compute_pausa,
    compute_elastic_sync, backfill_statsbomb_360, compute_embeddings_360).
    Without these, dbt_build silently built today's gold marts from
    yesterday's bronze for those 4 sources (1-day lag class)."""
    src = _read_workflows_main_tf()
    idx = src.find('"dbt_build"')
    assert idx != -1
    window = src[idx : idx + 2500]
    expected_deps = [
        "backfill_statsbomb_360",
        "compute_defcon_lite",
        "compute_elastic_sync",
        "compute_embeddings_v1",
        "compute_embeddings_360",
        "compute_formations_shape_graph",
        "compute_line_breaking",
        "compute_off_ball_xt",
        "compute_pausa",
        "compute_xg_model_v2",
        "extract_tracking_metadata",
        "hf_sync",
    ]
    missing = [d for d in expected_deps if d not in window]
    assert not missing, f"dbt_build task missing depends_on entries: {missing}"


def test_refresh_synced_tables_depends_only_on_dbt_build() -> None:
    """After D59, refresh_synced_tables collapses its previous 9-way fan-in
    into a single edge through dbt_build."""
    src = _read_workflows_main_tf()
    idx = src.find('"refresh_synced_tables"')
    assert idx != -1
    window = src[idx : idx + 2000]
    assert 'task_key = "dbt_build"' in window, "refresh_synced_tables must depend on dbt_build"
    # Verify the old direct deps are NOT in this window (only the dbt_build edge)
    old_direct_deps = [
        'task_key = "run_model_validation"',
        'task_key = "compute_off_ball_xt"',
        'task_key = "compute_xg_model_v2"',
    ]
    for dep in old_direct_deps:
        assert dep not in window, (
            f"refresh_synced_tables should no longer depend directly on {dep} — "
            f"the dependency now flows through dbt_build."
        )


def test_dbt_environment_block_exists() -> None:
    src = _read_workflows_main_tf()
    # The env block defines `environment_key = "dbt"` AND must contain dbt-databricks dep
    assert 'environment_key = "dbt"' in src, "workflows module must declare a 'dbt' environment_key"
    # Find the actual environment block (not the task usage at task-block scope)
    env_block_idx = src.find('environment_key = "dbt"')
    while env_block_idx != -1:
        preceding = src[max(0, env_block_idx - 200) : env_block_idx]
        if "environment {" in preceding and "spec {" in src[env_block_idx : env_block_idx + 800]:
            break
        env_block_idx = src.find('environment_key = "dbt"', env_block_idx + 1)
    assert env_block_idx != -1, (
        'Could not locate the `environment { ... environment_key = "dbt" ... spec { ... } }` block'
    )

    block = src[env_block_idx - 200 : env_block_idx + 800]
    assert "dbt-databricks" in block, "dbt environment must include dbt-databricks dependency"
    assert "dbt-core" in block, "dbt environment must include dbt-core dependency"
