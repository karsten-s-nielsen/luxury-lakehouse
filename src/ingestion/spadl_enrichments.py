"""SPADL post-conversion enrichment stage.

Provider-agnostic helpers from silly-kicks applied to the canonical SPADL
output of any per-provider converter (StatsBomb, Wyscout, IDSSE, Metrica,
SkillCorner, Gradient Sports). Establishes the named architectural home
for these enrichments — see ADR-016 for the design rationale.

First occupants (LL2):
    - silly_kicks.spadl.utils.add_possessions      → possession_id_heuristic
    - silly_kicks.spadl.utils.add_gk_role          → gk_role
    - silly_kicks.spadl.utils.add_pre_shot_gk_context
                                                   → gk_was_distributing,
                                                     gk_was_engaged,
                                                     gk_actions_in_possession,
                                                     defending_gk_player_id

    - silly_kicks.spadl.add_gk_distribution_metrics → gk_pass_length_m,
                                                       gk_pass_length_class,
                                                       is_launch
                                                       (gk_xt_delta dropped —
                                                       derived in dbt from the
                                                       canonical xT grid)

Future helpers plug in by extending ``apply_spadl_enrichments`` and adding
their column declarations to the schema constants in ``ingestion.spadl_vaep``.
"""

from __future__ import annotations

from typing import Final

import pandas as pd

_VALID_SOURCES: Final[frozenset[str]] = frozenset(
    {"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "gradientsports"}
)


def apply_spadl_enrichments(
    actions: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Apply provider-agnostic SPADL post-conversion enrichments.

    Runs silly-kicks's ``add_possessions``, ``add_gk_role``,
    ``add_gk_distribution_metrics``, ``add_pre_shot_gk_context``, and
    ``add_restart_coordinates`` in order. The helpers themselves are
    provider-agnostic — they read the canonical SPADL columns and don't
    branch on ``source``. The parameter is kept here for telemetry and
    for any future helper that needs source-specific behavior.

    Args:
        actions: SPADL action DataFrame from any provider's
            ``convert_to_actions``. Must include ``action_id`` (silly-kicks's
            helpers require it as input — silly-kicks's converters emit it
            automatically; luxury-lakehouse's UDFs surface it through the
            output StructType).
        source: One of ``"statsbomb"``, ``"wyscout"``, ``"idsse"``,
            ``"metrica"``.

    Returns:
        A copy of ``actions`` with 17 new columns appended:
        ``possession_id_heuristic``, ``gk_role``, the 3 GVM distribution
        columns ``gk_pass_length_m``, ``gk_pass_length_class``, ``is_launch``,
        ``gk_was_distributing``, ``gk_was_engaged``,
        ``gk_actions_in_possession``, ``defending_gk_player_id``, plus the
        4.22.0 restart-coordinate enrichment: ``enriched_start_x``,
        ``enriched_start_y``, ``enriched_end_x``, ``enriched_end_y``,
        ``start_coord_source``, ``end_coord_source``,
        ``start_coord_confidence``, ``end_coord_confidence``.

    Raises:
        ValueError: If ``source`` is not in the valid set.
    """
    if source not in _VALID_SOURCES:
        msg = f"apply_spadl_enrichments: unknown source {source!r}. Valid sources: {sorted(_VALID_SOURCES)}"
        raise ValueError(msg)

    # Imports inside function — silly-kicks is a heavy dep we don't want at
    # module-import time for tests of unrelated modules.
    from silly_kicks.spadl import add_gk_distribution_metrics, add_restart_coordinates
    from silly_kicks.spadl.utils import (
        add_gk_role,
        add_possessions,
        add_pre_shot_gk_context,
    )

    enriched = add_possessions(actions)
    enriched = add_gk_role(enriched)

    # silly-kicks 4.31.0 (Lamberts 2025, "Goalkeeper Value Model"): GK distribution length
    # classification. Consumes ``gk_role`` (must run AFTER add_gk_role). 3 grid-free columns kept
    # at bronze: ``gk_pass_length_m`` (Euclidean start->end, NaN off-distribution),
    # ``gk_pass_length_class`` (short/medium/long category, default 32m/60m thresholds), ``is_launch``
    # (bool, False off-distribution). The function also emits ``gk_xt_delta``, but that requires a
    # silly-kicks-convention (12x8) xT grid; we DROP it here and instead derive the xT delta in
    # ``fct_action_values`` from the lakehouse's canonical ``bronze.expected_threat_grids`` (single
    # xT source of truth — avoids a competing silly-kicks xT grid; same join pattern as
    # fct_goalkeeper_stats). See ADR-056.
    enriched = add_gk_distribution_metrics(enriched)
    enriched = enriched.drop(columns=["gk_xt_delta"])
    # category dtype is not Arrow/Spark-StringType-safe on createDataFrame — coerce to object.
    enriched["gk_pass_length_class"] = enriched["gk_pass_length_class"].astype("object")

    enriched = add_pre_shot_gk_context(enriched)

    # silly-kicks 4.22.0 (upstream ADR-025): Law-fixed-spot restart coordinate imputation —
    # 8 ADDITIVE provenance-tagged columns (enriched_start_x/_y, enriched_end_x/_y,
    # start/end_coord_source, start/end_coord_confidence); canonical start_x/.../end_y are
    # NEVER mutated (a canonical-coordinate change would be a VAEP/xT retrain trigger).
    # Events-only mode here (frames=None): the bronze SPADL writer has no tracking frames in
    # scope — native / rule-point / next-event tiers apply. NaN coords are a GS set-piece
    # phenomenon (other providers ~0%), but the columns are emitted for every provider so the
    # bronze schema is uniform (parity-tested via test_spadl_vaep_writer_parity).
    enriched = add_restart_coordinates(enriched)

    # silly-kicks emits ``possession_id`` from add_possessions. Rename to
    # ``possession_id_heuristic`` to make provenance explicit at the bronze
    # layer. The mart-level canonical ``possession_id`` is sourced from this
    # column via a SELECT alias (see fct_action_values.sql).
    enriched = enriched.rename(columns={"possession_id": "possession_id_heuristic"})

    return enriched
