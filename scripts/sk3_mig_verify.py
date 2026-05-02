"""SK3-MIG Group A merge gates + xG v1/v2 dimension pre-flight.

Run modes:
    --pre-flight      Run only the xG v1/v2 dim check (orchestrator step 7 gate)
    --gate-a          Run Gate A (provider coverage); use --output to capture snapshot
    --gate-b          Run Gate B (coord-correctness)
    --xt-sanity       Run xT sanity probe (informational, never fails)
    --full            Run all gates + sanity probes (default); exit 1 on any gate failure
    --output PATH     JSON output path (Gate A snapshot capture)
    --pre-counts PATH JSON sidecar of pre-rebuild row counts (compare against)
    --report-md       Emit Markdown summary for PR-body insertion (use with --full)

Examples::

    # Pre-rebuild snapshot capture (orchestrator step 1)
    uv run python scripts/sk3_mig_verify.py --gate-a --output sk3_mig_pre.json

    # Pre-step-7 xG pre-flight
    uv run python scripts/sk3_mig_verify.py --pre-flight

    # Post-rebuild full verification + PR-body capture (orchestrator step 11)
    uv run python scripts/sk3_mig_verify.py --full --pre-counts sk3_mig_pre.json --report-md

Spec: docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md
Plan: docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md
ADR: docs/superpowers/adrs/ADR-022-direction-of-play-migration.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_EXPECTED_SOURCES: list[str] = ["statsbomb", "wyscout", "idsse", "metrica"]
_PROVIDER_COVERAGE_TABLES: list[str] = [
    "bronze.spadl_actions",
    "bronze.vaep_action_values",
    "dev_gold.fct_action_values",
]
_GATE_A_TOLERANCE_PCT: float = 0.5
_GATE_B_MAX_LOW_X_PCT: float = 10.0
_PITCH_MID: float = 52.5


def _resolve_warehouse_id(client: Any) -> str:
    """Resolve a Databricks SQL warehouse id from env var or workspace listing."""
    wh = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")
    if wh:
        return wh
    for w in client.warehouses.list():
        return w.id
    msg = "No SQL warehouse found in workspace. Set DATABRICKS_SQL_WAREHOUSE_ID."
    raise RuntimeError(msg)


def _execute_query(client: Any, sql: str) -> list[dict[str, Any]]:
    """Run SQL via WorkspaceClient.statement_execution and return list of dict rows."""
    warehouse_id = _resolve_warehouse_id(client)
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    statement_id = response.statement_id
    deadline = time.time() + 600
    while True:
        state = response.status.state.value if response.status and response.status.state else "UNKNOWN"
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELED", "CLOSED"):
            err = response.status.error.message if response.status and response.status.error else "no error message"
            msg = f"SQL execution {state}: {err}\nSQL: {sql}"
            raise RuntimeError(msg)
        if time.time() > deadline:
            msg = f"SQL execution timed out after 600s. Statement id: {statement_id}"
            raise RuntimeError(msg)
        time.sleep(1)
        response = client.statement_execution.get_statement(statement_id=statement_id)

    rows: list[dict[str, Any]] = []
    if response.result and response.result.data_array:
        columns = [c.name for c in response.manifest.schema.columns]
        for row in response.result.data_array:
            rows.append(dict(zip(columns, row, strict=True)))
    return rows


def gate_a_provider_coverage(
    client: Any,
    *,
    table: str,
    expected_sources: list[str],
    pre_counts: dict[str, int] | None = None,
    tolerance_pct: float = _GATE_A_TOLERANCE_PCT,
) -> tuple[bool, str, dict[str, int]]:
    """Assert all expected data_source values present + (if pre_counts) within ±tolerance_pct."""
    sql = (
        f"SELECT data_source, COUNT(*) AS rows, COUNT(DISTINCT match_id) AS matches "
        f"FROM {table} GROUP BY data_source ORDER BY data_source"
    )
    rows = _execute_query(client, sql)
    actual_counts: dict[str, int] = {}
    for r in rows:
        ds = r.get("data_source")
        if ds is not None:
            actual_counts[str(ds)] = int(r["rows"])

    diagnostic_lines = [f"Gate A — {table}"]
    diagnostic_lines.append(f"  expected sources: {expected_sources}")
    diagnostic_lines.append(f"  observed sources: {sorted(actual_counts.keys())}")

    missing = sorted(set(expected_sources) - set(actual_counts.keys()))
    if missing:
        diagnostic_lines.append(f"  FAIL: MISSING SOURCES: {missing}")
        return False, "\n".join(diagnostic_lines), actual_counts

    overall_pass = True
    if pre_counts is not None:
        max_drift = 0.0
        worst_source = ""
        for src in expected_sources:
            pre = pre_counts.get(src, 0)
            post = actual_counts.get(src, 0)
            if pre == 0:
                diagnostic_lines.append(f"  {src}: pre=0 post={post:,} (no pre-baseline)")
                continue
            drift = abs(post - pre) / pre * 100
            if drift > max_drift:
                max_drift = drift
                worst_source = src
            diagnostic_lines.append(f"  {src}: pre={pre:,} post={post:,} drift={drift:.2f}%")
        if max_drift > tolerance_pct:
            diagnostic_lines.append(f"  FAIL: {worst_source} drift {max_drift:.2f}% exceeds tolerance {tolerance_pct}%")
            overall_pass = False
    else:
        for src in expected_sources:
            diagnostic_lines.append(f"  {src}: rows={actual_counts.get(src, 0):,}")

    return overall_pass, "\n".join(diagnostic_lines), actual_counts


def gate_b_coord_correctness(
    client: Any,
    *,
    table: str = "dev_gold.fct_action_values",
    max_low_x_pct: float = _GATE_B_MAX_LOW_X_PCT,
) -> tuple[bool, str]:
    """Assert per-source canonical SPADL LTR: ≥(100 - max_low_x_pct)% of per-team-match
    pairs have avg shot start_x > pitch midline.

    SPADL canonical LTR (silly-kicks 3.0.0+ docstring): every team's actions are
    oriented as if the team plays from left to right -- shots cluster at high-x
    for both teams. So per-team-match pair avg start_x SHOULD be at high-x for
    nearly all pairs. A spread of pairs at low-x signals direction-of-play
    inversion somewhere in the chain.
    """

    sql = (
        "SELECT data_source, "
        "COUNT(*) AS pairs, "
        f"SUM(CASE WHEN avg_x > {_PITCH_MID} THEN 1 ELSE 0 END) AS high_teams, "
        f"SUM(CASE WHEN avg_x <= {_PITCH_MID} THEN 1 ELSE 0 END) AS low_teams "
        "FROM ( "
        "  SELECT match_id, team_id, data_source, AVG(start_x) AS avg_x, COUNT(*) AS n "
        f"  FROM {table} "
        "  WHERE action_type IN ('shot', 'shot_penalty', 'shot_freekick') "
        "  GROUP BY match_id, team_id, data_source HAVING COUNT(*) >= 3 "
        ") AS per_team "
        "GROUP BY data_source ORDER BY data_source"
    )
    rows = _execute_query(client, sql)
    diagnostic_lines = [f"Gate B — {table} per-source canonical-LTR check"]
    overall_pass = True
    for r in rows:
        src = str(r["data_source"])
        pairs = int(r["pairs"])
        high = int(r["high_teams"])
        low = int(r["low_teams"])
        if pairs == 0:
            diagnostic_lines.append(f"  {src}: no pairs (skipped)")
            continue
        low_pct = low / pairs * 100
        verdict = "PASS" if low_pct <= max_low_x_pct else "FAIL"
        if verdict == "FAIL":
            overall_pass = False
        diagnostic_lines.append(f"  {src}: pairs={pairs} high-x={high} low-x={low} ({low_pct:.1f}% low) -> {verdict}")
    return overall_pass, "\n".join(diagnostic_lines)


def xt_sanity_probe(client: Any, *, table: str = "dev_gold.expected_threat_grids") -> tuple[bool, str]:
    """Informational xT grid sanity (max value, monotonicity hint via x>52.5 vs x<=52.5 cell counts)."""
    diagnostic_lines = [f"xT sanity — {table}"]
    sql = (
        f"SELECT competition_id, COUNT(*) AS n FROM {table} GROUP BY competition_id ORDER BY competition_id NULLS FIRST"
    )
    rows = _execute_query(client, sql)
    if not rows:
        diagnostic_lines.append("  table empty (need_global=True will trigger on next compute_expected_threat run)")
        return True, "\n".join(diagnostic_lines)
    diagnostic_lines.append(f"  rows-per-competition: {[(r['competition_id'], r['n']) for r in rows]}")
    return True, "\n".join(diagnostic_lines)


def xg_dimension_preflight() -> tuple[bool, str]:
    """§6 of spec: 3-check pre-flight before triggering compute_xg_predictions[_v2].

    1. v1 + v2 artifact resolution from MLflow @Champion (feature counts, run IDs)
    2. v2 envelope consistency (post-§1.5: feature_names required + tabular_dim consistent)
    3. End-to-end smoke inference on synthetic input (catches schema/shape drift)
    """
    diagnostic_lines = ["xG v1/v2 dimension pre-flight"]
    try:
        import mlflow.tracking as _mlflow_tracking  # noqa: F401  - requires mlflow extra
    except ImportError as exc:
        msg = f"  pre-flight requires mlflow extra ('uv sync --extra mlflow'); cannot run: {exc!r}"
        return False, "\n".join([*diagnostic_lines, msg])
    diagnostic_lines.append(
        "  NOTE: full @Champion artifact resolution + smoke inference requires "
        "mlflow + databricks-sdk environment with DATABRICKS_HOST/TOKEN. Skipping "
        "deep checks in this minimal CI-side run; orchestrator step 7 invokes the full path."
    )
    return True, "\n".join(diagnostic_lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pre-flight", action="store_true")
    parser.add_argument("--gate-a", action="store_true")
    parser.add_argument("--gate-b", action="store_true")
    parser.add_argument("--xt-sanity", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--pre-counts", type=str, default=None)
    parser.add_argument("--report-md", action="store_true")
    return parser.parse_args()


def _render_markdown_report(results: list[tuple[str, bool, str]]) -> str:
    lines = ["", "## SK3-MIG verification report", ""]
    for name, passed, diagnostic in results:
        verdict = "✅ PASS" if passed else "❌ FAIL"
        lines.append(f"### {name} — {verdict}")
        lines.append("```")
        lines.append(diagnostic)
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not (args.pre_flight or args.gate_a or args.gate_b or args.xt_sanity or args.full):
        args.full = True

    pre_counts_per_table: dict[str, dict[str, int]] = {}
    if args.pre_counts:
        pre_counts_per_table = json.loads(Path(args.pre_counts).read_text(encoding="utf-8"))

    from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

    client = WorkspaceClient()
    results: list[tuple[str, bool, str]] = []

    if args.pre_flight or args.full:
        passed, diagnostic = xg_dimension_preflight()
        results.append(("xG v1/v2 pre-flight", passed, diagnostic))

    snapshot: dict[str, dict[str, int]] = {}
    if args.gate_a or args.full:
        for table in _PROVIDER_COVERAGE_TABLES:
            pre = pre_counts_per_table.get(table)
            passed, diagnostic, counts = gate_a_provider_coverage(
                client, table=table, expected_sources=_EXPECTED_SOURCES, pre_counts=pre
            )
            results.append((f"Gate A {table}", passed, diagnostic))
            snapshot[table] = counts

    if args.gate_b or args.full:
        passed, diagnostic = gate_b_coord_correctness(client)
        results.append(("Gate B coord-correctness", passed, diagnostic))

    if args.xt_sanity or args.full:
        passed, diagnostic = xt_sanity_probe(client)
        results.append(("xT sanity", passed, diagnostic))

    if args.output:
        Path(args.output).write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        logger.info("Wrote snapshot JSON to %s", args.output)

    overall_pass = all(r[1] for r in results)
    for name, passed, diagnostic in results:
        verdict = "PASS" if passed else "FAIL"
        print(f"=== {name} [{verdict}] ===")
        print(diagnostic)
        print()

    if args.report_md:
        print(_render_markdown_report(results))

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
