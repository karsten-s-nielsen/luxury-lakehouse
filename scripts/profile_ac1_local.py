"""Profile the action-context enrichment chain LOCALLY against a committed fixture.

No Spark, no Databricks, no env build, no log-bleed: runs the REAL
``run_work_unit`` -> ``enrich_batch`` frame-batch loop in-process under cProfile and
prints the per-stage cumulative-time breakdown. This is the observable, seconds-
to-minutes feedback loop for the "which enrichment stage dominates per-match
wall-clock" question (e.g. ghost-GK vs DAS) — the serverless profiler answered
the same question but behind a 25-40 min black box.

The stage share + per-call ``predict_density`` time are CPU-bound and portable in
DIRECTION across machines; absolute seconds are machine-specific. Run the same
fixture under different silly-kicks versions for an apples-to-apples A/B::

    # current env
    uv run python scripts/profile_ac1_local.py

    # pin a specific silly-kicks for the A/B (uv resolves an ephemeral env)
    uv run --with silly-kicks==4.1.1 python scripts/profile_ac1_local.py
    uv run --with silly-kicks==4.2.0 python scripts/profile_ac1_local.py
"""

from __future__ import annotations

import argparse
import importlib.metadata as md

# Stage functions worth surfacing in the rollup (the enrichment chain + the
# silly-kicks hot leaves). Matched by substring on the cProfile function name.
_STAGE_NAMES: tuple[str, ...] = (
    "add_ghost_gk",
    "compute_ghost_gk",
    "predict_density",
    "add_das",
    "get_dangerous_accessible_space",
    "add_elastic_sync",
    "add_cover_shadows",
    "add_obso",
    "add_team_shape",
    "add_off_ball_context",
    "pitch_control_at_action",
    "infer_ball_carrier",
    "add_gk_influence",
    "add_defensive_line",
    "add_pre_shot_gk_context",
    "add_pressure_on_actor",
    "add_space_creation",
    "add_xt_gk",
    "compute_xt_gk",
    "add_gk_completion",
    "add_shape_graph",
    "link_actions_to_frames",
    "simulate_passes",
    "add_line_break",
    "add_action_context",
    "add_actor_pre_window",
    "add_pausa",
    "derive_team_in_possession",
)


def _print_versions() -> None:
    """Self-certify the analytics libs actually resolved in THIS process."""
    parts = []
    for pkg in ("silly-kicks", "accessible-space", "numba", "numpy", "scipy"):
        try:
            parts.append(f"{pkg}={md.version(pkg)}")
        except md.PackageNotFoundError:
            parts.append(f"{pkg}=<absent>")
    print("env_versions  " + "  ".join(parts))


def main() -> int:
    p = argparse.ArgumentParser(description="Local cProfile of the action-context enrichment chain.")
    p.add_argument("--provider", default="idsse")
    p.add_argument("--match-id", default="J03WMX")
    p.add_argument("--period", type=int, default=1)
    p.add_argument("--root", default="src/tests/fixtures/action_context")
    p.add_argument("--top", type=int, default=40, help="Top callees by cumulative time to print.")
    args = p.parse_args()

    import pandas as pd

    from analytics.action_context.local.parquet_sources import (
        ParquetActionsSource,
        ParquetFrameSource,
        ParquetMatchMetadataSource,
        ParquetXtSource,
    )
    from analytics.action_context.pipeline import run_work_unit
    from analytics.action_context.profiling import profile_callable
    from analytics.action_context.work_unit import WorkUnit

    _print_versions()

    class _Collect:
        """In-memory ResultSink — keeps the enriched row count, drops the frame."""

        rows = 0

        def write(self, wu: WorkUnit, result_df: pd.DataFrame) -> int:
            self.rows = len(result_df)
            return self.rows

    sink = _Collect()

    def _run() -> None:
        run_work_unit(
            WorkUnit(provider=args.provider, match_id=args.match_id, period=args.period),
            frames=ParquetFrameSource(args.root),
            actions=ParquetActionsSource(args.root),
            xt=ParquetXtSource(args.root),
            meta=ParquetMatchMetadataSource(args.root),
            sink=sink,
        )

    print(f"profiling {args.provider}/{args.match_id} p{args.period} ...")
    wall_s, timings = profile_callable(_run, top=400)

    # Map each tracked stage to its cumulative time (first match wins — the
    # outermost call in the cumulative sort).
    by_stage: dict[str, tuple[float, int]] = {}
    for t in timings:
        for stage in _STAGE_NAMES:
            if t.label.startswith(stage + " ") and stage not in by_stage:
                by_stage[stage] = (t.cumulative_s, t.ncalls)

    print(f"\nwall_s={wall_s:.1f}  rows_enriched={sink.rows}")
    print("=== STAGE ROLLUP (cumtime, % of wall, ncalls, per-call) ===")
    ordered = sorted(by_stage.items(), key=lambda kv: kv[1][0], reverse=True)
    for stage, (cum, n) in ordered:
        pct = 100.0 * cum / wall_s if wall_s else 0.0
        percall = cum / n if n else 0.0
        print(f"  {cum:8.1f}s {pct:5.1f}%  n={n:<5d} {percall:7.3f}s/call  {stage}")

    print(f"\n=== TOP {args.top} BY CUMULATIVE (raw) ===")
    for t in timings[: args.top]:
        print(f"  {t.cumulative_s:8.1f}s  n={t.ncalls:<6d} {t.label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
