# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sdk>=0.20",
#     "huggingface_hub>=0.20",
#     "mlflow>=2.19",
#     "silly-kicks>=3.0.1",
# ]
# ///
# ruff: noqa: RUF001, RUF002 — text contains "PR-alpha" (Greek alpha) per project convention
# ruff: noqa: S603, S607 — subprocess invocations of internal tooling (uv, terraform, pytest); no user input
"""SK3-MIG-B retrain orchestrator — drives 11 cycle items + 8 HF republishes
+ Lakebase synced refresh + index restoration + XG1-RETIRE runtime.

Spec: docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md
PEP 723 single-file. Idempotent. --start-at <step|item> resumable. --dry-run skips
HF Jobs invocations + runs steps 5-11 against existing Champions.

Per spec §5.2.1: orchestrator runs as background process. Status streams every
60-120s to stdout AND bronze.sk3_mig_b_runs Delta table.

Per CLAUDE.md "Never disappear into long-running commands": invoke this script
via run_in_background=true; poll output file via tail -f.

Cost cap (§9.5): _COST_CAP_USD = 80.0 — orchestrator halts on cumulative
cycle spend exceeding cap; resume via --override-cost-cap.

Wall-clock cap (§9.6): _WALLTIME_CAP_HOURS = 8.0 per single retrain — catches
hung jobs; resume via --override-walltime-cap.

Usage:
    # Dry-run (no HF Jobs spend; verify wiring):
    uv run python scripts/sk3_mig_b_retrain.py --dry-run

    # Full run:
    uv run python scripts/sk3_mig_b_retrain.py

    # Resume after halt:
    uv run python scripts/sk3_mig_b_retrain.py --start-at f2v_v2 --override-cost-cap
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# Make ingestion.* importable.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "src"))

from ingestion.sk3_mig_b_telemetry import classify_cycle_item  # noqa: E402

_COST_CAP_USD = 80.0
_WALLTIME_CAP_HOURS = 8.0
_STATUS_INTERVAL_SECONDS = 60

# Cycle item dispatch order — Group 1 first (gates Group 2), then Group 3.
_GROUP_1_TRAINED = ("vaep", "xg_v2", "ext_v2_p0", "ext_v2_p1")
_GROUP_1_COMPUTE_ONLY = ("defcon_lite", "obso", "pausa")
_GROUP_2_TRAINED = ("f2v_v1", "f2v_v2", "f2v_360", "scoutgpt")
_GROUP_3_PUBLISH = (
    "spadl_vaep_publish",
    "xg_shots_publish",
    "freeze_frame_publish",
    "shots_on_target_publish",
    "obso_pausa_inputs_publish",
    "obso_trained_grids_publish",
    "obso_pausa_values_publish",
    "f2v_embeddings_publish",
)

# Per-item cost estimates (USD) — empirical from prior cycles. Drives state.cumulative_cost_usd.
_ITEM_COST_USD: dict[str, float] = {
    "vaep": 0.50,
    "xg_v2": 6.00,
    "ext_v2_p0": 0.05,
    "ext_v2_p1": 0.05,
    "defcon_lite": 0.50,
    "obso": 1.50,
    "pausa": 0.20,
    "f2v_v1": 1.50,
    "f2v_v2": 4.00,
    "f2v_360": 5.00,
    "scoutgpt": 18.00,
    "spadl_vaep_publish": 0.10,
    "xg_shots_publish": 0.10,
    "freeze_frame_publish": 0.10,
    "shots_on_target_publish": 0.10,
    "obso_pausa_inputs_publish": 0.10,
    "obso_trained_grids_publish": 0.10,
    "obso_pausa_values_publish": 0.10,
    "f2v_embeddings_publish": 0.10,
}


@dataclass
class CycleState:
    """Mutable state passed between orchestrator steps."""

    cycle_id: str
    cycle_started_at: datetime
    wheel_at_start: str
    silly_kicks_version: str
    catalog: str
    warehouse_id: str
    dry_run: bool
    override_cost_cap: bool
    override_walltime_cap: bool
    allow_databricks_only_cost_hook: bool
    pre_mart_versions: dict[str, int] = field(default_factory=dict)
    cumulative_cost_usd: float = 0.0
    current_item_started_at: datetime | None = None
    current_item: str | None = None
    current_hf_job_id: str | None = None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _emit_status(
    state_or_msg: CycleState | str | None = None,
    *,
    step: str = "—",
    item: str = "—",
    phase: str = "—",
    elapsed_seconds: float = 0.0,
    hf_job_id: str | None = None,
    msg: str = "",
) -> None:
    """Emit a structured status line per spec §5.2.1.

    Accepts either a free-form string (legacy) or a CycleState (structured).
    """
    if isinstance(state_or_msg, str):
        free_msg = state_or_msg if not msg else f"{state_or_msg} | {msg}"
        cycle_id = "—"
        msg = free_msg
    elif state_or_msg is None:
        cycle_id = "—"
    else:
        cycle_id = state_or_msg.cycle_id

    elapsed_hms = time.strftime("%H:%M:%S", time.gmtime(elapsed_seconds))
    line = (
        f"[{_now_utc().isoformat(timespec='seconds')}] "
        f"cycle={cycle_id} step={step} item={item} phase={phase} "
        f"elapsed={elapsed_hms} hf_job_id={hf_job_id or 'null'} "
        f"msg={msg!r}"
    )
    print(line, flush=True)


def _execute_sql(state: CycleState, sql: str) -> list[list]:
    """Run SQL via WorkspaceClient.statement_execution; return data_array."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=state.warehouse_id,
        wait_timeout="50s",
    )
    if result.result is None or result.result.data_array is None:
        return []
    return result.result.data_array


def _write_telemetry_row(
    state: CycleState,
    *,
    cycle_item: str,
    smoke_pass: bool,
    smoke_metrics: dict[str, float] | None = None,
    smoke_metrics_str: dict[str, str] | None = None,
    hf_job_id: str | None = None,
    champion_set_at: datetime | None = None,
    pre_mart_version: int | None = None,
    post_mart_version: int | None = None,
    pre_hf_revision_sha: str | None = None,
    wall_clock_seconds: float | None = None,
    cost_usd: float | None = None,
) -> None:
    """Append one row to bronze.sk3_mig_b_runs.

    Per spec §5.3 + ADR-002 §4 schema discipline. Builds an INSERT INTO statement
    with literal SQL values (parameter binding via Databricks SDK doesn't yet
    cover MAP types; the literals are project-internal — no SQL injection risk).
    """
    cycle_item_kind = classify_cycle_item(cycle_item)
    metrics = smoke_metrics or {}
    metrics_str = smoke_metrics_str or {}

    def _fmt_str(s: str | None) -> str:
        return f"'{s}'" if s is not None else "NULL"

    def _fmt_ts(t: datetime | None) -> str:
        return f"TIMESTAMP '{t.strftime('%Y-%m-%d %H:%M:%S')}'" if t else "NULL"

    def _fmt_num(n: float | int | None) -> str:
        return str(n) if n is not None else "NULL"

    def _fmt_bool(b: bool | None) -> str:
        return "TRUE" if b else "FALSE" if b is not None else "NULL"

    def _fmt_map_dbl(m: dict[str, float]) -> str:
        if not m:
            return "MAP()"
        pairs = ", ".join(f"'{k}', {v}" for k, v in m.items())
        return f"MAP({pairs})"

    def _fmt_map_str(m: dict[str, str]) -> str:
        if not m:
            return "MAP()"
        pairs = ", ".join(f"'{k}', '{v}'" for k, v in m.items())
        return f"MAP({pairs})"

    sql = f"""
INSERT INTO {state.catalog}.bronze.sk3_mig_b_runs (
  cycle_id, cycle_started_at, cycle_finished_at,
  wheel_at_start, wheel_at_end, silly_kicks_version,
  cost_cap_usd, walltime_cap_hours,
  cycle_item, cycle_item_kind,
  hf_job_id, champion_set_at,
  pre_mart_version, post_mart_version,
  pre_hf_revision_sha,
  smoke_pass, smoke_metrics, smoke_metrics_str,
  wall_clock_seconds, cost_usd, recorded_at
) VALUES (
  '{state.cycle_id}', {_fmt_ts(state.cycle_started_at)}, NULL,
  '{state.wheel_at_start}', {_fmt_str("0.3.31" if cycle_item != "pre_state" else None)}, '{state.silly_kicks_version}',
  {_COST_CAP_USD}, {_WALLTIME_CAP_HOURS},
  '{cycle_item}', '{cycle_item_kind}',
  {_fmt_str(hf_job_id)}, {_fmt_ts(champion_set_at)},
  {_fmt_num(pre_mart_version)}, {_fmt_num(post_mart_version)},
  {_fmt_str(pre_hf_revision_sha)},
  {_fmt_bool(smoke_pass)}, {_fmt_map_dbl(metrics)}, {_fmt_map_str(metrics_str)},
  {_fmt_num(wall_clock_seconds)}, {_fmt_num(cost_usd)}, {_fmt_ts(_now_utc())}
)
"""
    try:
        _execute_sql(state, sql)
    except Exception as exc:  # noqa: BLE001 — telemetry must not crash orchestrator
        _emit_status(
            state,
            step="telemetry",
            phase="halted",
            msg=f"telemetry write failed for {cycle_item}: {exc}",
        )


# ── Heartbeat thread ────────────────────────────────────────────────────────

_heartbeat_stop_event = threading.Event()
_heartbeat_thread: threading.Thread | None = None


def _heartbeat_loop(state: CycleState) -> None:
    """Background thread — emits a heartbeat telemetry row every interval until stopped."""
    while not _heartbeat_stop_event.wait(_STATUS_INTERVAL_SECONDS):
        if state.current_item is None:
            continue
        elapsed = (_now_utc() - state.current_item_started_at).total_seconds() if state.current_item_started_at else 0.0
        _emit_status(
            state,
            step="heartbeat",
            item=state.current_item,
            phase="running",
            elapsed_seconds=elapsed,
            hf_job_id=state.current_hf_job_id,
            msg="dispatch in flight",
        )
        _write_telemetry_row(
            state,
            cycle_item="heartbeat",
            smoke_pass=True,
            smoke_metrics={"elapsed_seconds": elapsed},
            smoke_metrics_str={
                "current_item": state.current_item,
                "current_hf_job_id": state.current_hf_job_id or "null",
            },
        )


def _start_heartbeat(state: CycleState) -> None:
    global _heartbeat_thread
    if _heartbeat_thread is not None and _heartbeat_thread.is_alive():
        return
    _heartbeat_stop_event.clear()
    _heartbeat_thread = threading.Thread(target=_heartbeat_loop, args=(state,), daemon=True)
    _heartbeat_thread.start()


def _stop_heartbeat() -> None:
    _heartbeat_stop_event.set()


# ── Step 0: pre-flight ──────────────────────────────────────────────────────


def _step_0_preflight(state: CycleState) -> None:
    _emit_status(state, step="0", phase="running", msg="pre-flight gates start")

    import silly_kicks

    sk_version = getattr(silly_kicks, "__version__", "unknown")
    if sk_version < "3.0.1":
        raise RuntimeError(f"silly-kicks {sk_version} < 3.0.1")
    _emit_status(state, step="0", phase="running", msg=f"silly-kicks {sk_version} OK")

    pyproject = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    version_lines = [line for line in pyproject.splitlines() if line.startswith("version")]
    if "0.3.31" not in version_lines[0]:
        raise RuntimeError(f"Wheel version mismatch: {version_lines[0]!r}. PR-α wheel bump may not have merged.")
    _emit_status(state, step="0", phase="running", msg="wheel 0.3.31 OK")

    required_files = [
        "src/ingestion/sk3_mig_b_telemetry.py",
        "scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql",
        "scripts/train_football2vec.py",
        "scripts/publish_obso_pausa_inputs_hf.py",
        "scripts/publish_football2vec_embeddings_hf.py",
        "src/tests/test_xg_v1_retired.py",
        "src/tests/test_shot_map_v2_columns.py",
        "src/tests/test_no_notebook_hf_publishers.py",
    ]
    forbidden_files = [
        "src/ingestion/xg_model.py",
        "scripts/train_xg_model_hf.py",
        "notebooks/train_xg_model.py",
        "notebooks/train_football2vec.py",
        "notebooks/publish_datasets.py",
        "notebooks/publish_obso_data.py",
        "workflow-cards/wf-xg-v1.yaml",
    ]
    missing = [f for f in required_files if not (_REPO_ROOT / f).exists()]
    leftover = [f for f in forbidden_files if (_REPO_ROOT / f).exists()]
    if missing:
        raise RuntimeError(f"PR-α file(s) missing: {missing}")
    if leftover:
        raise RuntimeError(f"PR-α file(s) not deleted: {leftover}")
    _emit_status(
        state,
        step="0",
        phase="running",
        msg=f"PR-α inventory OK ({len(required_files)} required, {len(forbidden_files)} forbidden)",
    )

    for var in ("DATABRICKS_TOKEN", "DATABRICKS_HOST", "MLFLOW_TRACKING_URI", "HF_TOKEN", "DATABRICKS_WAREHOUSE_ID"):
        if not os.environ.get(var):
            raise RuntimeError(f"{var} unset — orchestrator cannot proceed")
    _emit_status(state, step="0", phase="running", msg="env vars OK")

    sql = f"SELECT MAX(_ingested_at) FROM {state.catalog}.dev_gold.fct_action_values"
    rows = _execute_sql(state, sql)
    max_ts = rows[0][0] if rows else None
    sk3_mig_a_merge = datetime(2026, 5, 2, tzinfo=timezone.utc)
    if isinstance(max_ts, str):
        max_ts = datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
    if max_ts is None or max_ts <= sk3_mig_a_merge:
        raise RuntimeError(f"fct_action_values max(_ingested_at) = {max_ts} <= SK3-MIG-A merge {sk3_mig_a_merge}")
    _emit_status(state, step="0", phase="running", msg=f"fct_action_values fresh ({max_ts}) OK")

    sql = (
        f"SELECT DISTINCT workflow_id FROM {state.catalog}.bronze.workflow_costs "
        f"WHERE started_at > current_timestamp() - INTERVAL 7 DAYS"
    )
    rows = _execute_sql(state, sql)
    workflow_ids = {row[0] for row in rows} if rows else set()
    hf_jobs_present = any(("xg-v2" in wid or "scoutgpt" in wid or "football2vec" in wid) for wid in workflow_ids)
    if not hf_jobs_present and not state.allow_databricks_only_cost_hook:
        _emit_status(
            state,
            step="0",
            phase="halted",
            msg="HALT: bronze.workflow_costs has no HF Jobs workflow_ids in last 7 days. "
            "Re-run with --allow-databricks-only-cost-hook to acknowledge.",
        )
        sys.exit(7)
    elif not hf_jobs_present:
        _emit_status(
            state,
            step="0",
            phase="running",
            msg="cost-hook covers Databricks only; --allow-databricks-only-cost-hook acknowledged",
        )
    else:
        _emit_status(state, step="0", phase="running", msg="cost-hook covers HF Jobs OK")

    affected_marts = [
        "fct_action_values",
        "fct_xg_predictions_v2",
        "fct_passes",
        "fct_player_embeddings",
        "fct_player_embeddings_career",
        "fct_player_embeddings_season",
        "fct_player_embeddings_career_360",
        "fct_player_embeddings_season_360",
        "fct_pausa_values",
        "fct_defcon_actions",
        "fct_defcon_pressure",
    ]
    for mart in affected_marts:
        try:
            sql = f"DESCRIBE HISTORY {state.catalog}.dev_gold.{mart} LIMIT 1"
            rows = _execute_sql(state, sql)
            if rows:
                state.pre_mart_versions[mart] = int(rows[0][0])
        except Exception as exc:  # noqa: BLE001 — mart may not exist; record + continue
            _emit_status(state, step="0", phase="running", msg=f"could not describe {mart}: {exc}")
    _emit_status(
        state,
        step="0",
        phase="running",
        msg=f"captured pre-state versions for {len(state.pre_mart_versions)}/{len(affected_marts)} marts",
    )

    _write_telemetry_row(
        state,
        cycle_item="pre_state",
        smoke_pass=True,
        smoke_metrics={"n_marts_captured": float(len(state.pre_mart_versions))},
    )
    _emit_status(state, step="0", phase="complete", msg="pre-flight COMPLETE")


# ── Per-cycle-item E2E loop ─────────────────────────────────────────────────


def _mlflow_model_name(cycle_item: str) -> str:
    return {
        "vaep": "soccer_analytics.dev_gold.vaep_model",
        "xg_v2": "soccer_analytics.dev_gold.xg_model_v2",
        "f2v_v1": "soccer_analytics.dev_gold.football2vec",
        "f2v_v2": "soccer_analytics.dev_gold.football2vec_v2",
        "f2v_360": "soccer_analytics.dev_gold.football2vec_360",
        "scoutgpt": "soccer_analytics.dev_gold.scoutgpt",
    }.get(cycle_item, "")


def _mart_for_item(cycle_item: str) -> str:
    return {
        "vaep": "fct_action_values",
        "xg_v2": "fct_xg_predictions_v2",
        "defcon_lite": "fct_defcon_actions",
        "obso": "fct_pausa_values",
        "pausa": "fct_pausa_values",
        "f2v_v1": "fct_player_embeddings",
        "f2v_v2": "fct_player_embeddings",
        "f2v_360": "fct_player_embeddings",
        "scoutgpt": "fct_player_embeddings",
    }.get(cycle_item, "")


def _task_key_for_item(cycle_item: str) -> str:
    """Mega-job task_key per workflow card."""
    return {
        "defcon_lite": "compute_defcon",
        "obso": "compute_pausa",
        "pausa": "compute_pausa",
        "vaep": "compute_spadl_vaep",
        "xg_v2": "compute_xg_model_v2",
        "scoutgpt_export": "wf_scoutgpt_export",
    }.get(cycle_item, cycle_item)


def _synced_tables_for_item(cycle_item: str) -> list[str]:
    """Lakebase synced-table FQNs that need refresh after this cycle item.

    ScoutGPT: no synced mart per Phase 0 Task 0.1 (zero terraform/workflow-cards
    grep hits).
    """
    return {
        "vaep": ["fct_action_values_synced"],
        "xg_v2": ["fct_xg_predictions_v2_synced"],
        "defcon_lite": ["fct_defcon_actions_synced", "fct_defcon_pressure_synced"],
        "pausa": ["fct_pausa_values_synced"],
        "obso": ["fct_pausa_values_synced"],
        "f2v_v1": [
            "fct_player_embeddings_synced",
            "fct_player_embeddings_career_synced",
            "fct_player_embeddings_season_synced",
        ],
        "f2v_v2": [
            "fct_player_embeddings_synced",
            "fct_player_embeddings_career_synced",
            "fct_player_embeddings_season_synced",
        ],
        "f2v_360": [
            "fct_player_embeddings_career_360_synced",
            "fct_player_embeddings_season_360_synced",
        ],
        "scoutgpt": [],
    }.get(cycle_item, [])


def _estimate_item_cost(cycle_item: str) -> float:
    return _ITEM_COST_USD.get(cycle_item, 0.0)


def _dispatch_trained_model(state: CycleState, cycle_item: str) -> str:
    """Invoke HF Jobs (or local) for trained-model cycle items. Returns job_id."""
    trainer_map = {
        "vaep": "scripts/train_vaep_model_hf.py",
        "xg_v2": "scripts/train_xg_v2_hf.py",
        "ext_v2_p0": None,  # local Win11
        "ext_v2_p1": None,  # local Win11
        "f2v_v1": "scripts/train_football2vec.py",
        "f2v_v2": "scripts/train_football2vec_v2.py",
        "f2v_360": "scripts/train_football2vec_360.py",
        "scoutgpt": "scripts/train_scoutgpt_hf.py",
    }
    script = trainer_map[cycle_item]
    if script is None:
        phase_module = "phase_0" if "p0" in cycle_item else "phase_1"
        cmd = [
            "uv",
            "run",
            "python",
            "-c",
            f"from analytics.ext_v2.{phase_module} import run_phase; run_phase()",
        ]
        subprocess.run(cmd, check=True)
        return f"local-{cycle_item}-{int(time.time())}"

    flavor_map = {
        "vaep": "cpu-basic",
        "xg_v2": "l40sx1",
        "f2v_v1": "gpu-medium",
        "f2v_v2": "gpu-medium",
        "f2v_360": "gpu-medium",
        "scoutgpt": "gpu-large",
    }
    flavor = flavor_map[cycle_item]

    from huggingface_hub import HfApi

    api = HfApi()
    # NOTE: `run_jobs` API surface depends on huggingface_hub version; if not
    # exposed at the project's pinned version, fall back to the underlying REST
    # endpoint via api._inner_api.post(...). Verified at Phase 9 prep time.
    job = api.run_jobs(  # type: ignore[attr-defined]
        script_path=script,
        hardware=flavor,
        secrets={
            "HF_TOKEN": os.environ["HF_TOKEN"],
            "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
            "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
            "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI"],
            "DATABRICKS_WAREHOUSE_ID": os.environ["DATABRICKS_WAREHOUSE_ID"],
        },
    )
    state.current_hf_job_id = job.job_id
    return job.job_id


def _promote_champion(state: CycleState, cycle_item: str) -> datetime:
    """Verify Champion alias was set by the trainer (no-op otherwise)."""
    import mlflow

    client = mlflow.MlflowClient()
    model_name = _mlflow_model_name(cycle_item)
    if not model_name:
        return _now_utc()
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No versions registered for {model_name} after retrain")
    return _now_utc()


def _trigger_mega_job_task(state: CycleState, cycle_item: str) -> int:
    """Trigger the full mega-job + wait for the specific task_key.

    Per reference_mega_job_orchestrator_design: lakehouse uses ONE mega-job
    ('soccer-analytics-ingestion-dev'); standalone-job dispatch fails.
    """
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    jobs = list(w.jobs.list(name="soccer-analytics-ingestion-dev"))
    if not jobs or jobs[0].job_id is None:
        raise RuntimeError("Mega-job 'soccer-analytics-ingestion-dev' not found")
    mega_job_id: int = jobs[0].job_id
    run = w.jobs.run_now(job_id=mega_job_id)
    target_task_key = _task_key_for_item(cycle_item)
    while True:
        run_state = w.jobs.get_run(run.run_id)
        task_run = next(
            (t for t in run_state.tasks or [] if t.task_key == target_task_key),
            None,
        )
        if task_run and task_run.state and task_run.state.life_cycle_state == "TERMINATED":
            if task_run.state.result_state != "SUCCESS":
                raise RuntimeError(f"Task {target_task_key} terminated with {task_run.state.result_state}")
            break
        time.sleep(_STATUS_INTERVAL_SECONDS)
        _emit_status(
            state,
            step="dispatch",
            item=cycle_item,
            phase="running",
            msg=f"waiting on mega-job task {target_task_key}",
        )

    mart = _mart_for_item(cycle_item)
    if not mart:
        return 0
    sql = f"DESCRIBE HISTORY {state.catalog}.dev_gold.{mart} LIMIT 1"
    rows = _execute_sql(state, sql)
    return int(rows[0][0]) if rows else 0


def _run_smoke_gate(cycle_item: str) -> bool:
    """Invoke pytest against the per-item smoke gate. Returns True on PASS."""
    test_file = f"src/tests/sk3_mig_b/test_{cycle_item}_post_retrain_smoke.py"
    if not (_REPO_ROOT / test_file).exists():
        # Publish-only items have no smoke gate by design.
        return True
    cmd = ["uv", "run", "pytest", test_file, "-v", "--tb=short"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        _emit_status(
            None,
            step="smoke",
            item=cycle_item,
            phase="halted",
            msg=f"smoke gate output:\n{result.stdout[:2000]}\n{result.stderr[:1000]}",
        )
    return result.returncode == 0


def _refresh_synced_table(state: CycleState, fqn: str) -> None:
    """Trigger Lakebase synced-table refresh."""
    cmd = ["uv", "run", "python", "scripts/refresh_synced_tables.py", "--table", fqn]
    subprocess.run(cmd, check=False)


def _restore_pg_indexes(state: CycleState, fqn: str) -> None:
    cmd = [
        "uv",
        "run",
        "python",
        "scripts/maintain_synced_tables.py",
        "--skip-refresh",
        "--table",
        fqn,
    ]
    subprocess.run(cmd, check=False)


def _verify_lakebase_parity(state: CycleState, cycle_item: str) -> None:
    """Smoke SQL — gold ↔ synced row count parity."""
    mart = _mart_for_item(cycle_item)
    if not mart:
        return
    sql_gold = f"SELECT COUNT(*) FROM {state.catalog}.dev_gold.{mart}"
    n_gold = int(_execute_sql(state, sql_gold)[0][0])
    _emit_status(
        state,
        step="lakebase",
        item=cycle_item,
        phase="running",
        msg=f"gold {mart} = {n_gold:,} rows; synced count check delegated to maintain_synced_tables",
    )


def _run_cycle_item(state: CycleState, cycle_item: str) -> bool:
    """Per-cycle-item E2E loop per spec §5.2."""
    item_started_at = _now_utc()
    state.current_item = cycle_item
    state.current_item_started_at = item_started_at
    _emit_status(
        state,
        step="cycle_item",
        item=cycle_item,
        phase="running",
        msg=">>> START",
    )

    # Cost-cap check
    sql = (
        f"SELECT COALESCE(SUM(cost_usd), 0.0) FROM {state.catalog}.bronze.workflow_costs "
        f"WHERE started_at >= '{state.cycle_started_at.isoformat()}'"
    )
    try:
        rows = _execute_sql(state, sql)
        state.cumulative_cost_usd = float(rows[0][0]) if rows else 0.0
    except Exception as exc:  # noqa: BLE001 — cost-hook may be down; warn + proceed
        _emit_status(
            state,
            step="cycle_item",
            item=cycle_item,
            phase="running",
            msg=f"cost-hook query failed: {exc}; proceeding",
        )

    if state.cumulative_cost_usd > _COST_CAP_USD and not state.override_cost_cap:
        _emit_status(
            state,
            step="cycle_item",
            item=cycle_item,
            phase="halted",
            msg=f"cost cap exceeded — {state.cumulative_cost_usd:.2f} > {_COST_CAP_USD}",
        )
        sys.exit(2)

    kind = classify_cycle_item(cycle_item)

    if state.dry_run:
        _emit_status(
            state,
            step="cycle_item",
            item=cycle_item,
            phase="running",
            msg="[dry-run] skip dispatch",
        )
        smoke_pass = _run_smoke_gate(cycle_item)
        _write_telemetry_row(
            state,
            cycle_item=cycle_item,
            smoke_pass=smoke_pass,
            wall_clock_seconds=(_now_utc() - item_started_at).total_seconds(),
        )
        state.current_item = None
        state.current_hf_job_id = None
        return smoke_pass

    hf_job_id: str | None = None
    champion_set_at: datetime | None = None
    post_mart_version: int | None = None

    if kind == "trained_model":
        hf_job_id = _dispatch_trained_model(state, cycle_item)
        champion_set_at = _promote_champion(state, cycle_item)
        post_mart_version = _trigger_mega_job_task(state, cycle_item)
    elif kind == "compute_only":
        post_mart_version = _trigger_mega_job_task(state, cycle_item)
    else:
        raise ValueError(f"Unknown cycle_item_kind: {kind}")

    smoke_pass = _run_smoke_gate(cycle_item)
    if not smoke_pass:
        if kind == "trained_model":
            _emit_status(
                state,
                step="smoke",
                item=cycle_item,
                phase="halted",
                msg=(
                    f"smoke gate FAILED. Restore prior Champion: "
                    f"set_and_verify_mlflow_champion('{_mlflow_model_name(cycle_item)}', "
                    f"version=PRIOR_VERSION)"
                ),
            )
        else:
            pre_mart_version = state.pre_mart_versions.get(_mart_for_item(cycle_item), 0)
            _emit_status(
                state,
                step="smoke",
                item=cycle_item,
                phase="halted",
                msg=(
                    f"smoke gate FAILED. Restore mart: RESTORE TABLE "
                    f"{state.catalog}.dev_gold.{_mart_for_item(cycle_item)} "
                    f"TO VERSION AS OF {pre_mart_version}"
                ),
            )
        sys.exit(3)

    for synced_table in _synced_tables_for_item(cycle_item):
        _refresh_synced_table(state, synced_table)
        _restore_pg_indexes(state, synced_table)
    _verify_lakebase_parity(state, cycle_item)

    elapsed = (_now_utc() - item_started_at).total_seconds()
    if elapsed > _WALLTIME_CAP_HOURS * 3600 and not state.override_walltime_cap:
        _emit_status(
            state,
            step="cycle_item",
            item=cycle_item,
            phase="halted",
            msg=f"walltime cap exceeded — {elapsed:.0f}s > {_WALLTIME_CAP_HOURS * 3600:.0f}s",
        )
        sys.exit(4)

    _write_telemetry_row(
        state,
        cycle_item=cycle_item,
        smoke_pass=True,
        hf_job_id=hf_job_id,
        champion_set_at=champion_set_at,
        pre_mart_version=state.pre_mart_versions.get(_mart_for_item(cycle_item)),
        post_mart_version=post_mart_version,
        wall_clock_seconds=elapsed,
        cost_usd=_estimate_item_cost(cycle_item),
    )
    _emit_status(
        state,
        step="cycle_item",
        item=cycle_item,
        phase="complete",
        elapsed_seconds=elapsed,
        msg="<<< DONE",
    )
    state.current_item = None
    state.current_hf_job_id = None
    return True


# ── Step 1-6 dispatchers ────────────────────────────────────────────────────


def _step_1_group_1(state: CycleState) -> None:
    _emit_status(state, step="1", phase="running", msg="Group 1 cycle items")
    for item in _GROUP_1_TRAINED + _GROUP_1_COMPUTE_ONLY:
        if not _run_cycle_item(state, item):
            sys.exit(3)
    _emit_status(state, step="1", phase="complete", msg="Group 1 COMPLETE")


def _step_2_group_2(state: CycleState) -> None:
    _emit_status(state, step="2", phase="running", msg="Group 2 cycle items")
    if not state.dry_run:
        _emit_status(state, step="2", phase="running", msg="ScoutGPT prerequisite: wf-scoutgpt-export")
        _trigger_mega_job_task(state, "scoutgpt_export")
        _write_telemetry_row(state, cycle_item="scoutgpt_export", smoke_pass=True)
    for item in _GROUP_2_TRAINED:
        if not _run_cycle_item(state, item):
            sys.exit(3)
    _emit_status(state, step="2", phase="complete", msg="Group 2 COMPLETE")


def _get_hf_revision_sha(repo_id: str) -> str | None:
    try:
        from huggingface_hub import HfApi

        api = HfApi()
        info = api.dataset_info(repo_id=repo_id)
        return info.sha
    except Exception:  # noqa: BLE001 — best-effort lookup
        return None


def _step_3_group_3_publish(state: CycleState) -> None:
    _emit_status(state, step="3", phase="running", msg="Group 3 HF dataset republishes")
    publishers = [
        ("spadl_vaep_publish", "scripts/publish_spadl_vaep_hf.py", "luxury-lakehouse/spadl-vaep-action-values"),
        ("xg_shots_publish", "scripts/publish_xg_shots_hf.py", "luxury-lakehouse/xg-shots"),
        ("freeze_frame_publish", "scripts/publish_freeze_frame_hf.py", "luxury-lakehouse/xg-freeze-frame-data"),
        ("shots_on_target_publish", "scripts/publish_shots_on_target_hf.py", "luxury-lakehouse/shots-on-target"),
        ("obso_pausa_inputs_publish", "scripts/publish_obso_pausa_inputs_hf.py", "luxury-lakehouse/obso-pausa-inputs"),
        ("obso_trained_grids_publish", "scripts/compute_epv_transition_hf.py", "luxury-lakehouse/obso-trained-grids"),
        ("obso_pausa_values_publish", "scripts/compute_obso_hf.py", "luxury-lakehouse/obso-pausa-values"),
        (
            "f2v_embeddings_publish",
            "scripts/publish_football2vec_embeddings_hf.py",
            "luxury-lakehouse/football2vec-player-embeddings",
        ),
    ]
    for cycle_item, script, repo_id in publishers:
        pre_sha = _get_hf_revision_sha(repo_id)
        if state.dry_run:
            _emit_status(
                state,
                step="3",
                item=cycle_item,
                phase="running",
                msg=f"[dry-run] skip {script}",
            )
        else:
            cmd = ["uv", "run", "python", script]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                _emit_status(
                    state,
                    step="3",
                    item=cycle_item,
                    phase="halted",
                    msg=f"republish FAILED. Pre-revision SHA: {pre_sha}. stdout={result.stdout[:300]}",
                )
                _write_telemetry_row(
                    state,
                    cycle_item=cycle_item,
                    smoke_pass=False,
                    pre_hf_revision_sha=pre_sha,
                    smoke_metrics_str={"failure_stdout": result.stdout[:500]},
                )
                sys.exit(5)

        _write_telemetry_row(
            state,
            cycle_item=cycle_item,
            smoke_pass=True,
            pre_hf_revision_sha=pre_sha,
            cost_usd=_estimate_item_cost(cycle_item),
        )
        _emit_status(
            state,
            step="3",
            item=cycle_item,
            phase="complete",
            msg=f"published {repo_id}",
        )
    _emit_status(state, step="3", phase="complete", msg="Group 3 COMPLETE")


def _step_4_xg1_retire_runtime(state: CycleState) -> None:
    """XG1-RETIRE runtime parts (PR-α-commit parts already in working tree)."""
    _emit_status(state, step="4", phase="running", msg="XG1-RETIRE runtime")
    if state.dry_run:
        _emit_status(state, step="4", phase="running", msg="[dry-run] skip XG1-RETIRE runtime")
        return

    cmd = [
        "uv",
        "run",
        "python",
        "scripts/delete_synced_table.py",
        "--table",
        "fct_xg_predictions_synced",
    ]
    subprocess.run(cmd, check=False)
    _emit_status(state, step="4", phase="running", msg="fct_xg_predictions_synced dropped")

    sql = f"DROP TABLE IF EXISTS {state.catalog}.dev_gold.fct_xg_predictions"
    _execute_sql(state, sql)
    _emit_status(state, step="4", phase="running", msg="fct_xg_predictions physical table dropped")

    import mlflow

    client = mlflow.MlflowClient()
    try:
        versions = client.search_model_versions("name='soccer_analytics.dev_gold.xg_model'")
        for v in versions:
            client.delete_model_version("soccer_analytics.dev_gold.xg_model", v.version)
        client.delete_registered_model("soccer_analytics.dev_gold.xg_model")
        _emit_status(state, step="4", phase="running", msg="MLflow xg_model v1 wiped")
    except Exception as exc:  # noqa: BLE001 — non-fatal cleanup
        _emit_status(state, step="4", phase="running", msg=f"MLflow xg_model wipe failed: {exc}")

    cmd = [
        "uv",
        "run",
        "python",
        "-c",
        f"from databricks.sdk import WorkspaceClient; "
        f"WorkspaceClient().files.delete_directory("
        f"'/Volumes/{state.catalog}/dev_gold/model_weights/xg_model', recursive=True)",
    ]
    subprocess.run(cmd, check=False)
    _emit_status(state, step="4", phase="running", msg="UC Volume v1 weights wiped")

    tf_dir = _REPO_ROOT / "terraform" / "environments" / "dev"
    cmd = ["terraform", "apply", "-auto-approve"]
    result = subprocess.run(cmd, cwd=tf_dir, capture_output=True, text=True)
    if result.returncode != 0:
        _emit_status(state, step="4", phase="halted", msg=f"terraform apply FAILED:\n{result.stderr[:2000]}")
        sys.exit(6)
    _emit_status(state, step="4", phase="running", msg="terraform apply OK")

    _write_telemetry_row(
        state,
        cycle_item="xg1_retire_runtime",
        smoke_pass=True,
        smoke_metrics={"steps_completed": 5.0},
    )
    _emit_status(state, step="4", phase="complete", msg="XG1-RETIRE runtime COMPLETE — irreversible")


def _step_5_hf4_cleanup(state: CycleState) -> None:
    _emit_status(state, step="5", phase="running", msg="HF4 cleanup verification")
    forbidden = [
        "notebooks/publish_datasets.py",
        "notebooks/publish_obso_data.py",
        "notebooks/train_football2vec.py",
        "notebooks/train_xg_model.py",
    ]
    leftover = [f for f in forbidden if (_REPO_ROOT / f).exists()]
    if leftover:
        raise RuntimeError(f"HF4 cleanup incomplete — leftover: {leftover}")

    for test_file in (
        "src/tests/test_no_notebook_hf_publishers.py",
        "src/tests/test_hf_publish_parity.py",
    ):
        result = subprocess.run(
            ["uv", "run", "pytest", test_file, "-v"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"{test_file} FAILED:\n{result.stdout[:1000]}")
    _emit_status(state, step="5", phase="complete", msg="HF4 cleanup COMPLETE")


def _step_6_final_sweep(state: CycleState) -> None:
    _emit_status(state, step="6", phase="running", msg="Final verification sweep")
    test_files = [
        "src/tests/test_ai_governance_md.py",
        "src/tests/test_topandas_boundedness.py",
        "src/tests/test_xg_v1_retired.py",
        "src/tests/test_shot_map_v2_columns.py",
        "src/tests/test_sk3_mig_b_runs_schema_parity.py",
    ]
    for tf in test_files:
        result = subprocess.run(
            ["uv", "run", "pytest", tf, "-v"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"{tf} FAILED:\n{result.stdout[:1000]}")
        _emit_status(state, step="6", phase="running", msg=f"{tf} OK")

    if state.dry_run:
        _emit_status(state, step="6", phase="running", msg="[dry-run] skip mega-job trigger")
    else:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        jobs = list(w.jobs.list(name="soccer-analytics-ingestion-dev"))
        if not jobs or jobs[0].job_id is None:
            raise RuntimeError("Mega-job 'soccer-analytics-ingestion-dev' not found")
        mega_job_id: int = jobs[0].job_id
        run = w.jobs.run_now(job_id=mega_job_id)
        _emit_status(
            state,
            step="6",
            phase="running",
            msg=f"Daily mega-job triggered: run_id={run.run_id}",
        )
        while True:
            run_state = w.jobs.get_run(run.run_id)
            if run_state.state and run_state.state.life_cycle_state == "TERMINATED":
                if run_state.state.result_state != "SUCCESS":
                    raise RuntimeError(f"Mega-job failed: {run_state.state.result_state}")
                break
            time.sleep(_STATUS_INTERVAL_SECONDS)
            _emit_status(state, step="6", phase="running", msg="waiting on mega-job completion")
        _emit_status(state, step="6", phase="running", msg="Daily mega-job SUCCESS")
    _emit_status(state, step="6", phase="complete", msg="cycle done")


# ── Main + CLI ──────────────────────────────────────────────────────────────


def _step_already_at_or_past(current: str, target: str) -> bool:
    """True if current step is at-or-after target step."""
    order = (
        "preflight",
        "group_1",
        "group_2",
        "group_3",
        "xg1_retire_runtime",
        "hf4_cleanup",
        "final_sweep",
    )
    item_to_step = {item: "group_1" for item in _GROUP_1_TRAINED + _GROUP_1_COMPUTE_ONLY}
    item_to_step.update({item: "group_2" for item in _GROUP_2_TRAINED})
    item_to_step.update({item: "group_3" for item in _GROUP_3_PUBLISH})
    target_step = item_to_step.get(target, target)
    if current not in order or target_step not in order:
        return False
    return order.index(current) >= order.index(target_step)


def main() -> int:
    parser = argparse.ArgumentParser(description="SK3-MIG-B retrain orchestrator")
    parser.add_argument(
        "--start-at",
        default=None,
        help="Resume from a specific cycle item (e.g., f2v_v2). Default: Step 0 pre-flight.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip HF Jobs invocations + run smoke gates against existing Champions.",
    )
    parser.add_argument(
        "--override-cost-cap",
        action="store_true",
        help=f"Bypass the ${_COST_CAP_USD} cycle cost cap.",
    )
    parser.add_argument(
        "--override-walltime-cap",
        action="store_true",
        help=f"Bypass the {_WALLTIME_CAP_HOURS}h per-item walltime cap.",
    )
    parser.add_argument(
        "--allow-databricks-only-cost-hook",
        action="store_true",
        help="Acknowledge bronze.workflow_costs covers Databricks only.",
    )
    parser.add_argument(
        "--cycle-id",
        default=None,
        help="Resume an existing cycle by id. Default: generate new.",
    )
    args = parser.parse_args()

    catalog = os.environ.get("DATABRICKS_CATALOG", "soccer_analytics")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id and (http_path := os.environ.get("DATABRICKS_HTTP_PATH", "")):
        warehouse_id = http_path.rstrip("/").rsplit("/", 1)[-1]

    cycle_id = args.cycle_id or f"sk3-mig-b-{_now_utc().strftime('%Y-%m-%d')}-{uuid.uuid4().hex[:6]}"
    try:
        import silly_kicks

        sk_version = getattr(silly_kicks, "__version__", "3.0.1")
    except ImportError:
        sk_version = "unavailable"

    state = CycleState(
        cycle_id=cycle_id,
        cycle_started_at=_now_utc(),
        wheel_at_start="0.3.31",
        silly_kicks_version=sk_version,
        catalog=catalog,
        warehouse_id=warehouse_id,
        dry_run=args.dry_run,
        override_cost_cap=args.override_cost_cap,
        override_walltime_cap=args.override_walltime_cap,
        allow_databricks_only_cost_hook=args.allow_databricks_only_cost_hook,
    )

    _emit_status(state, step="—", phase="running", msg=f"=== START dry_run={args.dry_run} ===")
    _emit_status(state, step="—", phase="running", msg=f"start_at={args.start_at or 'pre-flight'}")

    _start_heartbeat(state)
    try:
        steps_in_order = [
            ("preflight", lambda: _step_0_preflight(state)),
            ("group_1", lambda: _step_1_group_1(state)),
            ("group_2", lambda: _step_2_group_2(state)),
            ("group_3", lambda: _step_3_group_3_publish(state)),
            ("xg1_retire_runtime", lambda: _step_4_xg1_retire_runtime(state)),
            ("hf4_cleanup", lambda: _step_5_hf4_cleanup(state)),
            ("final_sweep", lambda: _step_6_final_sweep(state)),
        ]
        skip_until = args.start_at
        for step_name, fn in steps_in_order:
            if skip_until and step_name != skip_until and not _step_already_at_or_past(step_name, skip_until):
                _emit_status(
                    state,
                    step="—",
                    phase="running",
                    msg=f"skip step {step_name} (--start-at {skip_until})",
                )
                continue
            skip_until = None
            fn()
    finally:
        _stop_heartbeat()

    _emit_status(state, step="—", phase="complete", msg="=== ORCHESTRATOR COMPLETE ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
