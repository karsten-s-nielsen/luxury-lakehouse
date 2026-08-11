"""SK3-MIG Group A rebuild orchestrator — 11-step force-rebuild of bronze.spadl_actions
+ all coord-dependent downstream marts under silly-kicks 3.0.1.

Run modes:
    --start-at N         Resume from step N (idempotent, each step re-runnable)
    --dry-run            Print steps without executing
    --confirm-deletes    Required for steps that DELETE production data

Steps:
    0  Pre-flight: silly-kicks 3.0.1+ active locally
    1  Capture pre-rebuild Delta versions + Gate A snapshot to JSON sidecar
    2  DELETE bronze.spadl_actions + bronze.vaep_action_values (full)  [requires --confirm-deletes]
    3  Trigger compute_spadl_vaep job; wait for completion
    4  Gate A: provider-coverage verification (all 4 sources, ±0.5% drift)
    5  Trigger 3-stage dbt build (input → intermediate → output marts)
    6  DELETE dev_gold.expected_threat_grids → trigger compute_expected_threat
    7  xG v1/v2 dimension pre-flight (verify gate before any inference triggers)
    8  Trigger coord-dependent inference workflows in dependency order
    9  Final 3-stage dbt build (refresh marts dependent on Step 8 bronze writes)
    10 Refresh Lakebase synced tables + restore custom indexes
    11 Final coord-correctness gate B + xT sanity probe + Markdown report

Spec: docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md
Plan: docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md

⚠️ Per CLAUDE.md "Never disappear into long-running commands": invoke this with
``run_in_background: true`` and poll the output file every 30 seconds.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ingestion.databricks_auth import workspace_client
from shared.constants import DEFAULT_CATALOG

logger = logging.getLogger(__name__)

_CATALOG: str = os.environ.get("DATABRICKS_CATALOG", DEFAULT_CATALOG)
_ROLLBACK_SIDECAR = Path("sk3_mig_rollback.json")
_PRE_COUNTS_SIDECAR = Path("sk3_mig_pre.json")

_DELTA_TABLES_TO_VERSION: list[str] = [
    "bronze.spadl_actions",
    "bronze.vaep_action_values",
    "dev_gold.expected_threat_grids",
]

# Step 8: coord-dependent inference jobs in dependency order. Names must match
# the Databricks job names in terraform/modules/workflows/main.tf.
_STEP_8_JOBS_IN_ORDER: list[str] = [
    "compute_xg_predictions",
    "compute_xg_predictions_v2",
    "compute_defcon_lite",
    "compute_pausa",
    "import_obso_results",
    "compute_player_embeddings_v1",
    "compute_player_embeddings_v2",
    "compute_player_embeddings_360",
]


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _execute_query(client: Any, sql: str, *, expect_rows: bool = True) -> list[dict[str, Any]]:
    """Run SQL via WorkspaceClient.statement_execution and return list of dict rows."""
    warehouse_id = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if not warehouse_id:
        for w in client.warehouses.list():
            warehouse_id = w.id
            break
    if not warehouse_id:
        msg = "No warehouse found. Set DATABRICKS_SQL_WAREHOUSE_ID."
        raise RuntimeError(msg)

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        catalog=_CATALOG,
        wait_timeout="50s",
    )
    statement_id = response.statement_id
    deadline = time.time() + 1200
    while True:
        state = response.status.state.value if response.status and response.status.state else "UNKNOWN"
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELED", "CLOSED"):
            err = response.status.error.message if response.status and response.status.error else ""
            msg = f"SQL {state}: {err}\nSQL: {sql}"
            raise RuntimeError(msg)
        if time.time() > deadline:
            msg = f"SQL timed out after 1200s. statement_id={statement_id}"
            raise RuntimeError(msg)
        time.sleep(2)
        response = client.statement_execution.get_statement(statement_id=statement_id)

    rows: list[dict[str, Any]] = []
    if expect_rows and response.result and response.result.data_array:
        columns = [c.name for c in response.manifest.schema.columns]
        for row in response.result.data_array:
            rows.append(dict(zip(columns, row, strict=True)))
    return rows


def _resolve_job_id(client: Any, job_name: str) -> int:
    """Find the Databricks job id by exact name match."""
    for job in client.jobs.list(name=job_name):
        if job.settings and job.settings.name == job_name:
            return int(job.job_id)
    msg = f"No Databricks job named {job_name!r} found in workspace."
    raise RuntimeError(msg)


def _wait_for_run(client: Any, run_id: int, *, name: str, deadline_seconds: int = 7200) -> None:
    """Poll job run until terminal. Raises if not SUCCESS."""
    deadline = time.time() + deadline_seconds
    last_state = ""
    while time.time() < deadline:
        info = client.jobs.get_run(run_id=run_id)
        state = info.state
        life = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
        result = state.result_state.value if state and state.result_state else None
        if life != last_state:
            logger.info("  job=%s run_id=%d life=%s result=%s", name, run_id, life, result)
            last_state = life
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result == "SUCCESS":
                return
            msg = f"Job {name} (run_id={run_id}) terminal life={life} result={result}"
            raise RuntimeError(msg)
        time.sleep(30)
    msg = f"Job {name} (run_id={run_id}) timed out after {deadline_seconds}s"
    raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------


def step_0_preflight(client: Any, args: argparse.Namespace) -> None:
    import silly_kicks

    version = silly_kicks.__version__
    logger.info("silly-kicks version: %s", version)
    if not (version.startswith("3.") and version >= "3.0.1"):
        msg = f"silly-kicks 3.0.1+ required, got {version}"
        raise RuntimeError(msg)


def step_1_capture_baseline(client: Any, args: argparse.Namespace) -> None:
    versions: dict[str, int | None] = {}
    for table in _DELTA_TABLES_TO_VERSION:
        try:
            rows = _execute_query(client, f"DESCRIBE HISTORY {table} LIMIT 1")
            versions[table] = int(rows[0]["version"]) if rows else None
        except Exception as exc:  # noqa: BLE001 -- pre-flight capture; continue with None
            logger.warning("Could not get version for %s: %r", table, exc)
            versions[table] = None
    sidecar = {"delta_versions": versions, "captured_at": _now_iso()}
    _ROLLBACK_SIDECAR.write_text(json.dumps(sidecar, indent=2))
    logger.info("Wrote %s: %s", _ROLLBACK_SIDECAR, versions)

    pre_counts: dict[str, dict[str, int]] = {}
    for table in ("bronze.spadl_actions", "bronze.vaep_action_values", "dev_gold.fct_action_values"):
        try:
            sql = f"SELECT data_source, COUNT(*) AS rows FROM {table} GROUP BY data_source ORDER BY data_source"
            rows = _execute_query(client, sql)
            pre_counts[table] = {str(r["data_source"]): int(r["rows"]) for r in rows if r.get("data_source")}
        except Exception as exc:  # noqa: BLE001 -- table may be missing on first ever run
            logger.warning("Could not capture pre-counts for %s: %r", table, exc)
            pre_counts[table] = {}
    _PRE_COUNTS_SIDECAR.write_text(json.dumps(pre_counts, indent=2))
    logger.info("Wrote %s with per-source pre-rebuild counts.", _PRE_COUNTS_SIDECAR)


def step_2_delete_bronze(client: Any, args: argparse.Namespace) -> None:
    if not args.confirm_deletes:
        msg = "Step 2 requires --confirm-deletes flag."
        raise RuntimeError(msg)
    for table in ("bronze.spadl_actions", "bronze.vaep_action_values"):
        logger.info("DELETE FROM %s", table)

        _execute_query(client, f"DELETE FROM {table}", expect_rows=False)
        rows = _execute_query(client, f"SELECT COUNT(*) AS n FROM {table}")
        n = int(rows[0]["n"]) if rows else -1
        if n != 0:
            msg = f"DELETE failed: {table} still has {n} rows"
            raise RuntimeError(msg)
        logger.info("  %s: 0 rows confirmed", table)


def step_3_trigger_spadl_vaep(client: Any, args: argparse.Namespace) -> None:
    job_id = _resolve_job_id(client, "compute_spadl_vaep")
    logger.info("Triggering compute_spadl_vaep (job_id=%d)", job_id)
    run = client.jobs.run_now(job_id=job_id)
    _wait_for_run(client, int(run.run_id), name="compute_spadl_vaep")


def step_4_gate_a(client: Any, args: argparse.Namespace) -> None:
    from sk3_mig_verify import gate_a_provider_coverage

    pre = json.loads(_PRE_COUNTS_SIDECAR.read_text()) if _PRE_COUNTS_SIDECAR.exists() else {}
    expected = ["statsbomb", "wyscout", "idsse", "metrica"]
    failures: list[str] = []
    for table in ("bronze.spadl_actions", "bronze.vaep_action_values", "dev_gold.fct_action_values"):
        passed, diagnostic, _counts = gate_a_provider_coverage(
            client, table=table, expected_sources=expected, pre_counts=pre.get(table)
        )
        logger.info(diagnostic)
        if not passed:
            failures.append(table)
    if failures:
        msg = f"Gate A FAILED for: {failures}"
        raise RuntimeError(msg)


def step_5_dbt_build(client: Any, args: argparse.Namespace) -> None:
    for stage_job in ("dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts"):
        job_id = _resolve_job_id(client, stage_job)
        logger.info("Triggering %s (job_id=%d)", stage_job, job_id)
        run = client.jobs.run_now(job_id=job_id)
        _wait_for_run(client, int(run.run_id), name=stage_job, deadline_seconds=5400)


def step_6_wipe_xt_grids(client: Any, args: argparse.Namespace) -> None:
    if not args.confirm_deletes:
        msg = "Step 6 requires --confirm-deletes flag."
        raise RuntimeError(msg)
    logger.info("DELETE FROM dev_gold.expected_threat_grids")
    _execute_query(client, "DELETE FROM dev_gold.expected_threat_grids", expect_rows=False)
    job_id = _resolve_job_id(client, "compute_expected_threat")
    logger.info("Triggering compute_expected_threat (need_global=True path) (job_id=%d)", job_id)
    run = client.jobs.run_now(job_id=job_id)
    _wait_for_run(client, int(run.run_id), name="compute_expected_threat", deadline_seconds=3600)


def step_7_xg_preflight(client: Any, args: argparse.Namespace) -> None:
    from sk3_mig_verify import xg_dimension_preflight

    passed, diagnostic = xg_dimension_preflight()
    logger.info(diagnostic)
    if not passed:
        msg = "xG v1/v2 pre-flight FAILED — see diagnostic above"
        raise RuntimeError(msg)


def step_8_trigger_inference(client: Any, args: argparse.Namespace) -> None:
    skipped: list[str] = []
    for job_name in _STEP_8_JOBS_IN_ORDER:
        try:
            job_id = _resolve_job_id(client, job_name)
        except RuntimeError:
            logger.warning("Job %s not found in workspace; skipping", job_name)
            skipped.append(job_name)
            continue
        logger.info("Triggering %s (job_id=%d)", job_name, job_id)
        run = client.jobs.run_now(job_id=job_id)
        _wait_for_run(client, int(run.run_id), name=job_name, deadline_seconds=5400)
    if skipped:
        logger.warning("Step 8 skipped jobs not present in workspace: %s", skipped)


def step_9_final_dbt(client: Any, args: argparse.Namespace) -> None:
    return step_5_dbt_build(client, args)


def step_10_refresh_lakebase(client: Any, args: argparse.Namespace) -> None:
    for cmd in (
        ["uv", "run", "python", "scripts/maintain_synced_tables.py"],
        ["uv", "run", "python", "scripts/run_lakebase_grants.py"],
    ):
        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)  # noqa: S603 -- internal cmd list, not user input
        if result.returncode != 0:
            msg = f"Step 10 subprocess failed ({cmd}): stdout={result.stdout!r} stderr={result.stderr!r}"
            raise RuntimeError(msg)


def step_11_final_verify(client: Any, args: argparse.Namespace) -> None:
    from sk3_mig_verify import gate_b_coord_correctness, xt_sanity_probe

    failures: list[str] = []
    passed, diagnostic = gate_b_coord_correctness(client)
    logger.info(diagnostic)
    if not passed:
        failures.append("Gate B")
    passed, diagnostic = xt_sanity_probe(client)
    logger.info(diagnostic)
    if failures:
        msg = f"Step 11 FAILED: {failures}"
        raise RuntimeError(msg)


_STEPS: list[tuple[int, str, Any]] = [
    (0, "preflight", step_0_preflight),
    (1, "capture_baseline", step_1_capture_baseline),
    (2, "delete_bronze", step_2_delete_bronze),
    (3, "trigger_spadl_vaep", step_3_trigger_spadl_vaep),
    (4, "gate_a", step_4_gate_a),
    (5, "dbt_build", step_5_dbt_build),
    (6, "wipe_xt_grids", step_6_wipe_xt_grids),
    (7, "xg_preflight", step_7_xg_preflight),
    (8, "trigger_inference", step_8_trigger_inference),
    (9, "final_dbt", step_9_final_dbt),
    (10, "refresh_lakebase", step_10_refresh_lakebase),
    (11, "final_verify", step_11_final_verify),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-at", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-deletes", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    if args.dry_run:
        for n, name, _ in _STEPS:
            print(f"Step {n}: {name}")
        return 0

    client = workspace_client()

    for n, name, fn in _STEPS:
        if n < args.start_at:
            logger.info("--- Step %d: %s SKIPPED (--start-at %d) ---", n, name, args.start_at)
            continue
        logger.info("=== Step %d: %s ===", n, name)
        try:
            fn(client, args)
        except Exception as exc:
            logger.exception("!!! Step %d (%s) FAILED: %r", n, name, exc)
            logger.error("!!! Resume with: --start-at %d --confirm-deletes", n)
            return 1

    logger.info("=== ALL STEPS COMPLETE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
