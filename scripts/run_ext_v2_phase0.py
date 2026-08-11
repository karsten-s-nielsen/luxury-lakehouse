"""ExT v2 Phase 0 — single-trial Optuna harness against real fct_action_values.

Loads SPADL action data from Databricks via the SQL Connector (handles
8.8M-row pagination natively; the SDK Statements API's INLINE disposition
caps at 25 MiB and would truncate). Runs the Phase 0 harness, prints
global + per-competition NLL as JSON, intended to be invoked once to
populate ``docs/evolve/ext-v2-phase-0/SUMMARY.md``.

Run from the project venv with ``databricks-sql-connector`` ad-hoc::

    uv run --with databricks-sql-connector python scripts/run_ext_v2_phase0.py \
        --output docs/evolve/ext-v2-phase-0/phase0_baseline.json

(The connector isn't a project dep because Phase 0 is the only consumer
and the script is a one-off; promote to ``[sdk]`` extra if a second
consumer materializes.)

Locked design decisions (per docs/superpowers/specs/2026-04-25-ext-v2-
reproduction-design.md §10.2):

- Source: ``soccer_analytics.dev_gold.fct_action_values`` filtered to xT-
  relevant SPADL types (matches v1's
  ``ingestion.expected_threat._RELEVANT_TYPES``).
- Hash key: ``match_key`` (BIGINT, present on both ``fct_action_values``
  and ``fct_passes``).
- Holdout: 15% of matches per (competition, match_key) hash bucket; NLL
  evaluated on the ``action_type='pass'`` subset of holdout.
- Small-comp handling: per-comp NLL skips comps with empty holdout
  gracefully (no exclusion).

Auth via local Databricks CLI profile (auth_type='databricks-cli'). The
script auto-starts ``soccer-analytics-warehouse-dev`` if stopped.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

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

WAREHOUSE_ID = "6c3b36ca64d183fe"
"""soccer-analytics-warehouse-dev (2X-Small serverless)."""

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
    """  # noqa: S608 — controlled types list, no user input
    print(f"[query] pulling actions from gold mart (warehouse {WAREHOUSE_ID})", flush=True)
    t0 = time.perf_counter()
    with (
        sql.connect(
            server_hostname=host,
            http_path=http_path,
            auth_type="databricks-cli",
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(statement)
        # fetchall_arrow is significantly faster than fetchall + pandas conversion
        # for million-row result sets.
        arrow_table = cur.fetchall_arrow()
    df = arrow_table.to_pandas()
    elapsed = time.perf_counter() - t0
    print(f"[load] {len(df):,} actions in {elapsed:.1f}s", flush=True)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-zones-x", type=int, default=12)
    parser.add_argument("--n-zones-y", type=int, default=8)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path. If omitted, prints to stdout.",
    )
    args = parser.parse_args(argv)

    from analytics.ext_v2.fitness import compute_holdout_nll_per_competition
    from analytics.ext_v2.harness import run_phase0_harness
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
    print(f"[fit] running Phase 0 harness (grid {grid.n_zones_x}x{grid.n_zones_y})", flush=True)
    t1 = time.perf_counter()
    result = run_phase0_harness(actions, grid=grid)
    elapsed_fit = time.perf_counter() - t1
    print(f"[fit] complete in {elapsed_fit:.1f}s; global NLL = {result.best_nll:.5f}", flush=True)

    # Per-competition NLL via re-split + filter (matches harness internal split exactly).
    _train_actions, holdout_actions = holdout_split(actions)
    holdout_passes = holdout_actions[holdout_actions["action_type"] == "pass"].copy()
    per_comp = compute_holdout_nll_per_competition(result.producer, holdout_passes, grid=grid)

    summary: dict[str, Any] = {
        "n_zones_x": args.n_zones_x,
        "n_zones_y": args.n_zones_y,
        "n_zones": grid.n_zones,
        "n_total_actions": len(actions),
        "n_train_actions": result.n_train_actions,
        "n_holdout_passes": result.n_holdout_passes,
        "n_total_matches": n_matches,
        "n_competitions": n_comps,
        "global_nll": result.best_nll,
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
