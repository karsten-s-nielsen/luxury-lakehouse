"""ExT v2 Phase 1 -- KDE-smoothed Singh under Optuna against real fct_action_values.

Mirrors ``scripts/run_ext_v2_phase0.py``: pulls 8.8M SPADL action rows from
Databricks via the SQL Connector, dispatches the Phase 1 harness with the
three KDE Optuna axes active, persists the Optuna study to SQLite, logs
per-trial params + NLL via MLflow callback, and writes the best trial's
metadata as JSON.

Per spec section 10.3:

- Library: sklearn.KernelDensity (Q1)
- Per-source-zone destination KDE, point evaluation (Q2)
- Per-row Silverman with global multiplier when adaptive=True (Q3)
- nll_primary at eps=1e-10, nll_floorless at eps=1e-300 logged per trial (Q4)
- n_trials default 500 (Q5)
- Holdout unchanged from Phase 0 (Q6)
- Local Win11 venue (Q7)

Run from project venv with ad-hoc connector::

    uv run --with databricks-sql-connector python scripts/run_ext_v2_phase1.py \\
        --output docs/evolve/ext-v2-phase-1/phase1_baseline.json \\
        --n-trials 500 \\
        --study-db docs/evolve/ext-v2-phase-1/optuna.db \\
        --mlflow-uri file:./mlruns \\
        --best-producer docs/evolve/ext-v2-phase-1/best_producer.joblib

Stop condition disposition (per spec section 10.3):
- nll_primary < 3.7513 -> PASS, file Phase 1 success in SUMMARY.md.
- nll_primary >= 3.7513 -> FAIL, file finding, plan Phase 2.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import joblib
import mlflow
import pandas as pd

from ingestion.databricks_auth import workspace_client

# PR-Cycle-B (2026-05-01): databricks-sdk + databricks-sql-connector are in
# the [sdk] optional extra. Lazy-import keeps this module importable
# without those extras installed.
if TYPE_CHECKING:
    from databricks import sql
    from databricks.sdk import WorkspaceClient
else:
    try:
        from databricks import sql
        from databricks.sdk import WorkspaceClient
    except ImportError:
        sql = None  # type: ignore[assignment, misc]
        WorkspaceClient = None  # type: ignore[assignment, misc]


def _mlflow_trial_callback(tracking_uri: str | None, metric_name: str):
    """Per-trial MLflow logger — the mlflow-skinny replacement for Optuna's
    optuna-integration ``MLflowCallback``.

    ``optuna-integration[mlflow]`` was dropped because its ``[mlflow]`` extra
    hard-requires full mlflow, which carries CVE-2026-71211 (ADR-027). This logs one
    MLflow run per Optuna trial (params + scored metric + numeric user_attrs) via
    mlflow-skinny's tracking API. This is the repo's only optuna-integration use.
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    def _callback(study: Any, trial: Any) -> None:
        with mlflow.start_run(run_name=f"trial-{trial.number}"):
            mlflow.log_params(trial.params)
            if trial.value is not None:
                mlflow.log_metric(metric_name, float(trial.value))
            for key, val in trial.user_attrs.items():
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    mlflow.log_metric(key, float(val))

    return _callback


WAREHOUSE_ID = "6c3b36ca64d183fe"
"""soccer-analytics-warehouse-dev (2X-Small serverless) -- same as Phase 0."""

XT_RELEVANT_TYPES = (
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "dribble",
    "goalkick",
    "clearance",
    "shot",
    "shot_penalty",
    "shot_freekick",
)

PHASE1_STOP_THRESHOLD = 3.7513
PHASE0_BASELINE_NLL = 3.78924


def load_actions(host: str, http_path: str) -> pd.DataFrame:
    """Pull xT-relevant SPADL actions from gold mart via SQL Connector."""
    types_sql = ", ".join(f"'{t}'" for t in XT_RELEVANT_TYPES)
    statement = f"""
        SELECT
            CAST(competition_id AS STRING) AS competition_id,
            match_key,
            action_type AS type_name,
            action_result AS result_name,
            action_type,
            start_x, start_y, end_x, end_y
        FROM soccer_analytics.dev_gold.fct_action_values
        WHERE action_type IN ({types_sql})
    """  # noqa: S608 -- controlled types list, no user input
    print(f"[query] pulling actions from gold mart (warehouse {WAREHOUSE_ID})", flush=True)
    t0 = time.perf_counter()
    with (
        sql.connect(server_hostname=host, http_path=http_path, auth_type="databricks-cli") as conn,
        conn.cursor() as cur,
    ):
        cur.execute(statement)
        arrow_table = cur.fetchall_arrow()
    df = arrow_table.to_pandas()
    elapsed = time.perf_counter() - t0
    print(f"[load] {len(df):,} actions in {elapsed:.1f}s", flush=True)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-zones-x", type=int, default=12)
    parser.add_argument("--n-zones-y", type=int, default=8)
    parser.add_argument("--n-trials", type=int, default=500)
    parser.add_argument(
        "--study-db",
        type=Path,
        default=None,
        help="Optuna SQLite storage path (e.g. docs/evolve/ext-v2-phase-1/optuna.db). "
        "If omitted, study is in-memory (not resumable).",
    )
    parser.add_argument(
        "--study-name",
        type=str,
        default="ext-v2-phase-1-kde-smoothed",
    )
    parser.add_argument(
        "--mlflow-uri",
        type=str,
        default="file:./mlruns",
        help="MLflow tracking URI. Default: local file backend at ./mlruns",
    )
    parser.add_argument(
        "--mlflow-experiment",
        type=str,
        default="ext-v2-phase-1",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path (best trial summary).",
    )
    parser.add_argument(
        "--best-producer",
        type=Path,
        default=None,
        help="Optional joblib path to dump the best fitted KDESmoothedProducer.",
    )
    args = parser.parse_args(argv)

    from analytics.ext_v2.fitness import compute_holdout_nll_per_competition
    from analytics.ext_v2.harness import run_phase1_harness
    from analytics.ext_v2.holdout import holdout_split
    from analytics.ext_v2.transition import GridSpec

    # Resolve warehouse host + http_path via the SDK (uses CLI profile auth).
    client = workspace_client()
    warehouse = client.warehouses.get(WAREHOUSE_ID)
    if warehouse.odbc_params is None or warehouse.odbc_params.hostname is None:
        msg = f"Warehouse {WAREHOUSE_ID} has no odbc_params"
        raise RuntimeError(msg)
    host = warehouse.odbc_params.hostname
    http_path = warehouse.odbc_params.path
    if http_path is None:
        msg = f"Warehouse {WAREHOUSE_ID} has no http_path"
        raise RuntimeError(msg)

    actions = load_actions(host, http_path)
    n_matches = int(actions["match_key"].nunique())
    n_comps = int(actions["competition_id"].nunique())
    print(f"[load] {n_matches} matches, {n_comps} competitions", flush=True)

    grid = GridSpec(n_zones_x=args.n_zones_x, n_zones_y=args.n_zones_y)
    print(
        f"[fit] running Phase 1 harness (grid {grid.n_zones_x}x{grid.n_zones_y}, n_trials={args.n_trials})",
        flush=True,
    )

    storage = f"sqlite:///{args.study_db}" if args.study_db else None
    if args.study_db is not None:
        args.study_db.parent.mkdir(parents=True, exist_ok=True)

    mlflow_callback = _mlflow_trial_callback(args.mlflow_uri, "nll_primary")

    t1 = time.perf_counter()
    result = run_phase1_harness(
        actions,
        grid=grid,
        n_trials=args.n_trials,
        study_name=args.study_name,
        study_storage=storage,
        callbacks=[mlflow_callback],
    )
    elapsed_fit = time.perf_counter() - t1
    print(
        f"[fit] complete in {elapsed_fit:.1f}s; "
        f"best nll_primary = {result.best_nll:.5f}; "
        f"best nll_floorless = {result.best_nll_floorless:.5f}",
        flush=True,
    )

    # Per-competition NLL via re-split + filter (matches harness internal split exactly).
    _train_actions, holdout_actions = holdout_split(actions)
    holdout_passes = holdout_actions[holdout_actions["action_type"] == "pass"].copy()
    per_comp = compute_holdout_nll_per_competition(result.producer, holdout_passes, grid=grid)  # type: ignore[arg-type]

    # Stop condition disposition.
    stop_disposition = "PASS" if result.best_nll < PHASE1_STOP_THRESHOLD else "FAIL"
    print(f"[stop] disposition: {stop_disposition} (threshold {PHASE1_STOP_THRESHOLD})", flush=True)

    # Plateau check note.
    n_done = len(result.study.trials)
    plateau_warning = result.best_trial.number >= n_done - 50 if n_done > 50 else False
    if plateau_warning:
        print(
            f"[plateau] best trial #{result.best_trial.number} is in last 50 of "
            f"{n_done} trials -- consider extending by 200 (per spec section 10.3 Q5 escape hatch).",
            flush=True,
        )

    summary: dict[str, Any] = {
        "phase": 1,
        "n_zones_x": args.n_zones_x,
        "n_zones_y": args.n_zones_y,
        "n_zones": grid.n_zones,
        "n_trials": args.n_trials,
        "n_total_actions": len(actions),
        "n_train_actions": result.n_train_actions,
        "n_holdout_passes": result.n_holdout_passes,
        "n_total_matches": n_matches,
        "n_competitions": n_comps,
        "best_trial_number": result.best_trial.number,
        "best_params": dict(result.best_trial.params),
        "nll_primary": result.best_nll,
        "nll_floorless": result.best_nll_floorless,
        "phase0_baseline_nll": PHASE0_BASELINE_NLL,
        "stop_threshold": PHASE1_STOP_THRESHOLD,
        "stop_disposition": stop_disposition,
        "relative_improvement_pct": (PHASE0_BASELINE_NLL - result.best_nll) / PHASE0_BASELINE_NLL * 100,
        "plateau_warning": plateau_warning,
        "per_competition_nll": dict(sorted(per_comp.items())),
        "wall_clock_fit_seconds": elapsed_fit,
    }

    payload = json.dumps(summary, indent=2, default=str)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload)
        print(f"[out] wrote {args.output}", flush=True)
    else:
        print("---SUMMARY-JSON---")
        print(payload)

    if args.best_producer is not None:
        args.best_producer.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(result.producer, args.best_producer)
        print(f"[out] dumped best producer to {args.best_producer}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
