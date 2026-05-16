"""SPADL post-conversion enrichment stage.

Provider-agnostic helpers from silly-kicks applied to the canonical SPADL
output of any per-provider converter. Establishes the named architectural
home for these enrichments — see ADR-016 for the design rationale.

First occupants (LL2):
    - silly_kicks.spadl.utils.add_possessions      → possession_id_heuristic
    - silly_kicks.spadl.utils.add_gk_role          → gk_role
    - silly_kicks.spadl.utils.add_pre_shot_gk_context
                                                   → gk_was_distributing,
                                                     gk_was_engaged,
                                                     gk_actions_in_possession,
                                                     defending_gk_player_id

Future helpers (e.g., add_gk_distribution_metrics) plug in by extending
``apply_spadl_enrichments`` and adding their column declarations to the
schema constants in ``ingestion.spadl_vaep``.
"""

from __future__ import annotations

from typing import Final

import pandas as pd

_VALID_SOURCES: Final[frozenset[str]] = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner"})


def apply_spadl_enrichments(
    actions: pd.DataFrame,
    *,
    source: str,
) -> pd.DataFrame:
    """Apply provider-agnostic SPADL post-conversion enrichments.

    Runs silly-kicks's ``add_possessions``, ``add_gk_role``, and
    ``add_pre_shot_gk_context`` in order. The helpers themselves are
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
        A copy of ``actions`` with 6 new columns appended:
        ``possession_id_heuristic``, ``gk_role``, ``gk_was_distributing``,
        ``gk_was_engaged``, ``gk_actions_in_possession``,
        ``defending_gk_player_id``.

    Raises:
        ValueError: If ``source`` is not in the valid set.
    """
    if source not in _VALID_SOURCES:
        msg = f"apply_spadl_enrichments: unknown source {source!r}. Valid sources: {sorted(_VALID_SOURCES)}"
        raise ValueError(msg)

    # Imports inside function — silly-kicks is a heavy dep we don't want at
    # module-import time for tests of unrelated modules.
    from silly_kicks.spadl.utils import (
        add_gk_role,
        add_possessions,
        add_pre_shot_gk_context,
    )

    enriched = add_possessions(actions)
    enriched = add_gk_role(enriched)
    enriched = add_pre_shot_gk_context(enriched)

    # silly-kicks emits ``possession_id`` from add_possessions. Rename to
    # ``possession_id_heuristic`` to make provenance explicit at the bronze
    # layer. The mart-level canonical ``possession_id`` is sourced from this
    # column via a SELECT alias (see fct_action_values.sql).
    enriched = enriched.rename(columns={"possession_id": "possession_id_heuristic"})

    return enriched
