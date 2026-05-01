"""Conformance test: every today's-gold mart read by a Databricks compute
task must have a transitive ``depends_on`` path to the dbt stage that
builds it.

Peer to ``test_workflow_dag_bronze_reads.py`` (PR-Cycle-B, PR #242).
ADR-019's "compute reads today's gold" principle: any compute task that
reads ``gold.fct_*`` must wait on the dbt stage that produced that mart,
otherwise the compute output silently uses yesterday's gold.

Curated rather than auto-discovered — same rationale as the bronze-read
peer (string-literal false positives, write-vs-read fingerprinting).

Pure parse of ``terraform/modules/workflows/main.tf``. No Databricks
connection, no module imports. Reuses the parser from the bronze-read
peer.

References:
- ADR-019 — Three-Stage dbt_build for Same-Day Gold-Reader Compute
- src/tests/test_workflow_dag_bronze_reads.py — bronze-read peer
- docs/superpowers/specs/2026-05-01-option-b-three-stage-dbt-build-design.md §6.2
"""

from __future__ import annotations

from pathlib import Path

from src.tests.test_workflow_dag_bronze_reads import (  # type: ignore[import-not-found]
    _parse_task_depends_on,
    _transitive_closure,
)

_REPO = Path(__file__).resolve().parents[2]
_TF_FILE = _REPO / "terraform" / "modules" / "workflows" / "main.tf"

# ──────────────────────────────────────────────────────────────────────────────
# Curated gold-read requirements.
#
# Format: (consumer_task, gold_mart, expected_dbt_stage)
#
# Each entry asserts that ``consumer_task`` has a transitive ``depends_on``
# path to ``expected_dbt_stage``. ``gold_mart`` is documentary — it identifies
# WHICH read motivates the dependency.
#
# When a new compute task starts reading a gold mart, add an entry here.
# When a read is removed, remove the entry. The test fails loudly either way.
#
# Stage assignments per ADR-019 mart classification tags:
# - dbt_build_input_marts: dim_*, fct_tracking_frames, fct_shots, fct_discipline_events
# - dbt_build_intermediate_marts: fct_action_values
# - dbt_build_output_marts: every other mart
# ──────────────────────────────────────────────────────────────────────────────

_GOLD_READ_REQUIREMENTS: list[tuple[str, str, str]] = [
    # ── compute_pitch_control: reads input_mart fct_tracking_frames ────────
    # Spearman 2017 pitch-control surfaces over fct_tracking_frames frames.
    ("compute_pitch_control", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_off_ball_xt: reads input_mart fct_tracking_frames ──────────
    # Off-ball xT computed over tracking frames.
    ("compute_off_ball_xt", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_xg_model + compute_xg_model_v2: read input_mart fct_shots ──
    # Both v1 (XGBoost) and v2 (Deep Sets) score from fct_shots gold.
    ("compute_xg_model", "fct_shots", "dbt_build_input_marts"),
    ("compute_xg_model_v2", "fct_shots", "dbt_build_input_marts"),
    # ── compute_formations_efpi: reads input_mart fct_tracking_frames ──────
    # EFPI template matching reads tracking frames.
    ("compute_formations_efpi", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_formations_shape_graph: reads input_mart fct_tracking_frames
    # Sotudeh 2026 shape-graph detector reads tracking frames.
    ("compute_formations_shape_graph", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_line_breaking: gold-side reads stay input_mart-only ────────
    # Path A reads bronze.statsbomb_360 (covered by bronze-read peer);
    # gold-side queries against fct_tracking_frames keep the dependency
    # on stage 1.
    ("compute_line_breaking", "fct_tracking_frames", "dbt_build_input_marts"),
    # ── compute_embeddings_v2: reads intermediate_mart fct_action_values ───
    # ML inference reads SPADL/VAEP action values from gold (intermediate stage).
    ("compute_embeddings_v2", "fct_action_values", "dbt_build_intermediate_marts"),
    # ── run_model_validation: reads output_marts ──────────────────────────
    # ADR-019 supplants ADR-017's yesterday-gold carve-out: validation now
    # reads TODAY's gold, but its sibling-of-refresh_synced_tables position
    # under dbt_build_output_marts means a validation regression cannot
    # block today's mart refresh. The "signal not gate" guarantee is
    # preserved by topology, not by stale reads.
    ("run_model_validation", "fct_xg_predictions_v2", "dbt_build_output_marts"),
    ("run_model_validation", "fct_pausa_values", "dbt_build_output_marts"),
]


# ──────────────────────────────────────────────────────────────────────────────
# Tests.
# ──────────────────────────────────────────────────────────────────────────────


def test_every_gold_read_has_transitive_depends_on_path() -> None:
    """For each curated (consumer, gold_mart, expected_dbt_stage) requirement,
    the consumer task's transitive ``depends_on`` closure must contain the
    expected dbt stage. Catches the same-day-gold-reader-edge class going
    forward (peer to PR-Cycle-B's bronze-read conformance test)."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    errors: list[str] = []
    for consumer, gold_mart, expected_stage in _GOLD_READ_REQUIREMENTS:
        if consumer not in deps:
            errors.append(
                f"{consumer!r} not found in TF data_ingestion job — "
                f"requirement (consumer={consumer!r}, gold_mart={gold_mart!r}, "
                f"stage={expected_stage!r}) is unsatisfiable."
            )
            continue
        closure = _transitive_closure(deps, consumer)
        if expected_stage not in closure:
            errors.append(
                f"{consumer!r} reads gold.{gold_mart} (built by {expected_stage!r}) "
                f"but has no transitive depends_on path to {expected_stage!r}. "
                f"Closure: {sorted(closure)}. "
                f"Add `depends_on {{ task_key = {expected_stage!r} }}` to {consumer!r} "
                f"in terraform/modules/workflows/main.tf."
            )
    assert not errors, "\n\n".join(errors)


def test_gold_read_consumers_present_in_tf() -> None:
    """Anchor: every consumer in ``_GOLD_READ_REQUIREMENTS`` must be parseable
    from the TF file. Guards against a parser regression silently producing
    an empty deps dict."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    consumers = {c for c, _m, _s in _GOLD_READ_REQUIREMENTS}
    consumer_missing = consumers - set(deps.keys())
    assert not consumer_missing, (
        f"Consumer tasks missing from TF parse output: {sorted(consumer_missing)}. "
        f"Either the TF lost the task or the parser has a regression. "
        f"Parsed tasks: {sorted(deps.keys())}"
    )


def test_three_stage_dbt_tasks_present_in_tf() -> None:
    """Anchor: the three dbt stage tasks must exist in TF. Without these,
    every gold-read requirement is unsatisfiable."""
    deps = _parse_task_depends_on(_TF_FILE.read_text(encoding="utf-8"))
    expected_stages = {"dbt_build_input_marts", "dbt_build_intermediate_marts", "dbt_build_output_marts"}
    parsed_keys = set(deps.keys())
    # Stages may appear as deps without having their own deps entries — that's fine.
    # We ALSO accept them appearing as consumer keys. A stage missing from BOTH is a bug.
    stages_seen_as_consumer_or_dep: set[str] = set(deps.keys())
    for dep_set in deps.values():
        stages_seen_as_consumer_or_dep |= dep_set
    missing_stages = expected_stages - stages_seen_as_consumer_or_dep
    assert not missing_stages, (
        f"Three-stage dbt tasks missing from TF: {sorted(missing_stages)}. Parsed tasks: {sorted(parsed_keys)}"
    )
