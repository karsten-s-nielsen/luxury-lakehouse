"""Pure planner for strand-safe synced-table re-derives (ADR-043).

Zero IO: classifies a set of selected dbt model names into ordered re-derive
PlanSteps using only SYNCED_TABLES + the declared registries. The thin executor
(scripts/rederive_synced_marts.py) resolves the selection + match ids and runs the
plan. This split makes all classification logic unit-testable offline.

Three actions: D (MERGE-reprocess, incremental + match-filter), T (plain rebuild of
the 2 `table` marts — zero downtime), B (delete->full-refresh->recreate, merge-all
incremental). No enable-var injection: dbt_project.yml already enables every gated mart
that should be enabled (pausa_enabled / embeddings_enabled / defcon_enabled = true) and
intentionally leaves fct_space_creation 0-row (no node-level enabled=; only a body gate)
— so the re-derive reproduces the daily build's state by passing NO enable vars.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from ingestion.refresh_synced_tables import SYNCED_TABLES, SyncedTableConfig

# The 7 TRIGGERED + incremental + match_id-filtered marts re-derived via the
# CDF-preserving D path (MERGE + reprocess macros). Exhaustive D/T/B partition over
# the TRIGGERED set is enforced by src/tests/test_strand_safe_rederive.py.
D_REPROCESS_MODELS: frozenset[str] = frozenset(
    {
        "fct_action_values",
        "fct_defcon_actions",
        "fct_defcon_pressure",
        "fct_defensive_values",
        "fct_off_ball_xt",
        "fct_tracking_frames",
        "fct_tracking_shape_timeline",
    }
)

# The TRIGGERED `table`-materialized marts. Re-derived via the T (plain rebuild) action:
# a plain `dbt build` is an atomic create-or-replace (count-safe, same id, strand-free —
# it is exactly what the daily stage-3 does) so no synced delete/recreate and no
# --full-refresh is needed. Zero downtime. Verified table-materialized by the partition test.
_TABLE_MARTS: frozenset[str] = frozenset({"fct_pausa_values", "fct_space_creation"})


@dataclass(frozen=True)
class PlanStep:
    """One mart's re-derive instruction. ``dbt_vars`` is passed verbatim to ``dbt build --vars``."""

    model: str
    synced_table: str
    action: Literal["D", "T", "B"]
    full_refresh: bool
    dbt_vars: dict[str, object]


def _triggered_configs() -> dict[str, SyncedTableConfig]:
    return {c.source_table: c for c in SYNCED_TABLES if c.scheduling_policy == "TRIGGERED"}


def plan_rederive(selected_models: Iterable[str], match_ids: Sequence[int], *, rebuild: bool = False) -> list[PlanStep]:
    """Classify selected models into ordered (D, then T, then B) re-derive steps.

    SNAPSHOT / non-synced models are skipped (immune). D = MERGE-reprocess (``reprocess_match_ids``,
    no overwrite). T = plain rebuild of a `table` mart (atomic create-or-replace, zero downtime).
    B = delete synced -> ``--full-refresh`` (``allow_triggered_full_refresh``) -> recreate, for
    merge-all incremental marts. ``rebuild=True`` forces EVERY selected mart through B — the
    sanctioned full-rebuild for a D mart's schema/contract change, or to refresh a T mart's synced
    schema (the tripwire blocks a bare ``dbt --full-refresh``).
    """
    triggered = _triggered_configs()
    steps: list[PlanStep] = []
    for model in set(selected_models):
        cfg = triggered.get(model)
        if cfg is None:
            continue
        if rebuild:
            steps.append(PlanStep(model, cfg.name, "B", True, {"allow_triggered_full_refresh": True}))
        elif model in D_REPROCESS_MODELS:
            steps.append(PlanStep(model, cfg.name, "D", False, {"reprocess_match_ids": list(match_ids)}))
        elif model in _TABLE_MARTS:
            steps.append(PlanStep(model, cfg.name, "T", False, {}))
        else:
            steps.append(PlanStep(model, cfg.name, "B", True, {"allow_triggered_full_refresh": True}))
    _order = {"D": 0, "T": 1, "B": 2}
    return sorted(steps, key=lambda s: (_order[s.action], s.model))
