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
# Three-stage dbt task topology (PR-Cycle-C PR-β, ADR-019)
# ---------------------------------------------------------------------------

_THREE_STAGE_TASKS = ("dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts")


def _find_task_block_start(src: str, task_key: str) -> int:
    """Locate the index of a task block's `task_key = "<key>"` declaration.

    The task_key string also appears inside `depends_on { task_key = "..." }`
    references in earlier tasks (alphabetical depends-on ordering means
    the depends_on reference is hit first by `str.find`). Anchor on the
    declaration line, which is indented exactly 4 spaces and uses the
    aligned `task_key        = "..."` form.

    Returns the index of the opening quote so callers can take a forward
    window over the task body.
    """
    needle_aligned = f'    task_key        = "{task_key}"'
    needle_simple = f'    task_key = "{task_key}"'
    for needle in (needle_aligned, needle_simple):
        idx = src.find(needle)
        if idx != -1:
            return idx
    return -1


def _task_body(src: str, task_key: str) -> str:
    """Return a task block's body from its ``task_key`` declaration up to the next ``task {`` block.

    Robust to task-block length: a fixed-size forward window silently truncates a task's tail (e.g. its
    ``environment_key`` / ``run_if``) when the task grows, so anchor on the declaration and slice to the
    next ``\\n  task {`` (or module end). Returns "" when the task is absent.
    """
    idx = _find_task_block_start(src, task_key)
    if idx == -1:
        return ""
    next_task = src.find("\n  task {", idx)
    return src[idx : next_task if next_task != -1 else len(src)]


def test_three_stage_dbt_tasks_exist() -> None:
    """ADR-019: single `dbt_build` is replaced with three sequential
    invocations driven by mart classification tags."""
    src = _read_workflows_main_tf()
    for task_key in _THREE_STAGE_TASKS:
        assert f'task_key        = "{task_key}"' in src or f'task_key = "{task_key}"' in src, (
            f"workflows module must contain a task with task_key = {task_key!r}"
        )
    # The legacy single-task name must NOT exist anymore.
    assert 'task_key        = "dbt_build"' not in src and 'task_key = "dbt_build"' not in src, (
        "Legacy single `dbt_build` task is replaced by the three-stage topology in PR-Cycle-C PR-β. Remove it."
    )


def test_three_stage_dbt_tasks_use_correct_entry_point_and_environment() -> None:
    """All three stages run the same `dbt_build` entry point; differentiation
    is by the `--select` parameter, not by entry point."""
    src = _read_workflows_main_tf()
    for task_key in _THREE_STAGE_TASKS:
        window = _task_body(src, task_key)
        assert window, f"task_key declaration for {task_key!r} not found in TF"
        assert "python_wheel_task" in window, f"{task_key} must use python_wheel_task"
        assert 'entry_point  = "dbt_build"' in window or 'entry_point = "dbt_build"' in window, (
            f"{task_key} entry_point must be 'dbt_build' (selector differentiates stages)"
        )
        assert 'environment_key = "dbt"' in window, f"{task_key} must use the dbt environment_key"


def test_three_stage_dbt_tasks_use_distinct_select_parameters() -> None:
    """Stage 1 selects input_mart + dimension (with ancestors), stage 2
    selects intermediate_mart (with ancestors), stage 3 selects output_mart
    WITH ancestors (`+tag:output_mart`) and ``--exclude``s everything stages
    1+2 already built — so each output mart's own staging/intermediate
    ancestors are rebuilt (ADR-019 amended; full model coverage is enforced by
    ``test_dbt_stage_selector_coverage``)."""
    src = _read_workflows_main_tf()
    expected = {
        "dbt_build_input_marts": ["+tag:input_mart", "+tag:dimension"],
        "dbt_build_intermediate_marts": ["+tag:intermediate_mart"],
        "dbt_build_output_marts": ["+tag:output_mart", "path:models/staging", "path:models/intermediate"],
    }
    for task_key, selectors in expected.items():
        window = _task_body(src, task_key)
        assert window, f"task_key declaration for {task_key!r} not found in TF"
        for sel in selectors:
            assert f'"{sel}"' in window, (
                f"{task_key} must pass --select with selector {sel!r}; window=\n{window[:600]}..."
            )
    # Stage 3 must --exclude what stages 1+2 already built so it rebuilds ONLY the
    # output marts + their not-yet-built ancestors (no redundant mart rebuilds).
    window3 = _task_body(src, "dbt_build_output_marts")
    assert '"--exclude"' in window3, "dbt_build_output_marts must --exclude stage-1/2 models"
    for ex in ("+tag:input_mart", "+tag:dimension", "+tag:intermediate_mart"):
        assert f'"{ex}"' in window3, f"dbt_build_output_marts --exclude must contain {ex!r}"


def test_dbt_build_input_marts_depends_on_all_ingest_helpers() -> None:
    """Stage 1 runs after all bronze-writer ingest tasks (including the
    ingest-helper `compute_*` tasks per ADR-019: `extract_tracking_metadata`,
    backfills, `resolve_players`)."""
    src = _read_workflows_main_tf()
    window = _task_body(src, "dbt_build_input_marts")
    assert window, "task_key declaration for 'dbt_build_input_marts' not found in TF"
    expected_deps = [
        "backfill_statsbomb_360",
        "backfill_statsbomb_extra",
        "extract_tracking_metadata",
        "ingest_idsse",
        "ingest_idsse_events",
        "ingest_metrica",
        "ingest_skillcorner",
        "ingest_statsbomb",
        "ingest_wyscout",
        "resolve_players",
    ]
    missing = [d for d in expected_deps if f'task_key = "{d}"' not in window]
    assert not missing, f"dbt_build_input_marts task missing depends_on entries: {missing}"


def test_dbt_build_intermediate_marts_depends_on_stage1_and_spadl_vaep() -> None:
    """Stage 2 must run after stage 1 (`dbt_build_input_marts`) AND after
    `compute_spadl_vaep` which writes the SPADL/VAEP bronze that
    `fct_action_values` (the only intermediate_mart) consumes."""
    src = _read_workflows_main_tf()
    window = _task_body(src, "dbt_build_intermediate_marts")
    assert window, "task_key declaration for 'dbt_build_intermediate_marts' not found in TF"
    for dep in ("compute_spadl_vaep", "dbt_build_input_marts"):
        assert f'task_key = "{dep}"' in window, f"dbt_build_intermediate_marts must depend on {dep!r}"


def test_dbt_build_output_marts_depends_on_stage2_and_phase2_compute() -> None:
    """Stage 3 must run after stage 2 (`dbt_build_intermediate_marts`)
    AND after every phase-2 compute task that writes bronze read by an
    output_mart."""
    src = _read_workflows_main_tf()
    window = _task_body(src, "dbt_build_output_marts")
    assert window, "task_key declaration for 'dbt_build_output_marts' not found in TF"
    expected_deps = [
        "compute_defcon_lite",
        "compute_elastic_sync",
        "compute_embeddings_360",
        # compute_embeddings_v1 removed — v1 Doc2Vec deprecated 2026-05-07.
        "compute_embeddings_v2",
        "compute_expected_threat",
        "compute_formations_efpi",
        "compute_formations_shape_graph",
        "compute_line_breaking",
        "compute_off_ball_xt",
        "compute_pausa",
        "compute_pitch_control",
        # compute_xg_model (v1) retired SK3-MIG-B 2026-05-03; compute_xg_model_v2
        # retired 2026-07-10 (v2 producer chain — ADR-066).
        # compute_xg_shot_scores writes bronze.xg_shot_predictions read by the
        # fct_shot_xg output_mart (ADR-066) — the mart is promoted into the
        # daily build via `--vars xg_v3_enabled=true`.
        "compute_xg_shot_scores",
        "dbt_build_intermediate_marts",
        # ADR-074/SEC7 (2026-08-08): was "hf_sync". Stage 3 needed exactly ONE leg of it
        # — bronze.psxg_predictions via stg_psxg__predictions — while the other eight
        # sub-operations are HF Hub exports that gate nothing. Since ADR-073 made hf_sync
        # fail its task on any sub-op failure, that edge let an HF outage block this build.
        "import_psxg_predictions",
    ]
    missing = [d for d in expected_deps if f'task_key = "{d}"' not in window]
    assert not missing, f"dbt_build_output_marts task missing depends_on entries: {missing}"


def test_refresh_synced_tables_depends_only_on_dbt_build_output_marts() -> None:
    """After PR-β, `refresh_synced_tables` waits on stage 3 (the final
    dbt invocation), NOT on the legacy single `dbt_build` task."""
    src = _read_workflows_main_tf()
    idx = src.find('"refresh_synced_tables"')
    assert idx != -1
    window = src[idx : idx + 2000]
    assert 'task_key = "dbt_build_output_marts"' in window, (
        "refresh_synced_tables must depend on dbt_build_output_marts (stage 3)"
    )
    # Verify the legacy dep on single `dbt_build` is gone.
    assert 'task_key = "dbt_build"\n' not in window, (
        "refresh_synced_tables must NOT depend on the legacy single `dbt_build` task — "
        "it now depends on `dbt_build_output_marts` (stage 3)."
    )


def test_run_model_validation_depends_on_dbt_build_output_marts() -> None:
    """ADR-019: `run_model_validation` runs as a SIBLING of
    `refresh_synced_tables` (both depend on `dbt_build_output_marts`).
    This supplants ADR-017's yesterday-gold workaround — validation reads
    today's gold but cannot block today's mart refresh."""
    src = _read_workflows_main_tf()
    idx = src.find('"run_model_validation"')
    assert idx != -1
    window = src[idx : idx + 2000]
    assert 'task_key = "dbt_build_output_marts"' in window, (
        "run_model_validation must depend on dbt_build_output_marts (sibling of refresh_synced_tables)"
    )
    # The legacy dep on `compute_pausa` is gone.
    assert 'task_key = "compute_pausa"' not in window, (
        "run_model_validation must NOT depend on compute_pausa anymore — "
        "the new topology covers this transitively via dbt_build_output_marts."
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
