"""Conversion rate funnel — mart-only Lakebase queries + app-side rollups.

Replaces the earlier raw-event scan of fct_action_values (9.5 M rows → Parallel
Seq Scan, 37.8 s with game-state filter, exceeding app statement_timeout) with
a read of the pre-aggregated fct_funnel_stages_agg mart (~12,145 rows → composite
index lookup, <100 ms).  Simultaneously closes a silent LIMIT 500000 truncation
that under-reported A3 entries / shots / goals by >50 % for prolific teams.

LL2 Path B (2026-04-29): Wyscout-synthetic-possession compensation removed.
Possession IDs are now sourced from silly-kicks's heuristic add_possessions
and populated for ALL 4 sources (StatsBomb / Wyscout / IDSSE / Metrica).
Straddler semantics still handled via the two possession-count columns
(pos_in_gs, pos_in_match) — see fct_funnel_stages_agg.sql header for the
mart-side derivation. The wy_match_flag column was retired in LL2.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache

_STAGE_KEYS = ("possessions", "a3_entries", "shots", "goals")


@ttl_cache()
def fetch_funnel_agg(
    comp_id: int,
    team_id: int,
    match_id: int | None = None,
    game_state: str | None = None,
) -> pd.DataFrame:
    """Pre-aggregated mart read — single path for single-match and season modes.

    Single-match mode: WHERE match_id = %s (idx_funnel_agg_match).
    Season mode:       WHERE competition_id = %s AND (team_id = %s OR opponent_team_id = %s)
                       (BitmapOr of idx_funnel_agg_comp_team_gs + idx_funnel_agg_comp_opp_gs).
    Game-state filter appends AND game_state = %s (lowercased).

    No LIMIT clause — mart is bounded to ~12,145 rows total. Adding a LIMIT
    would reintroduce the silent-truncation bug the mart was built to fix.
    """
    tbl = t("fct_funnel_stages_agg_synced")
    cols = (
        "match_id, competition_id, team_id, opponent_team_id, game_state,"
        " pos_in_gs, pos_in_match, a3_entries, shots, goals"
    )
    where: list[str]
    params: list[Any]
    if match_id is not None:
        where = ["match_id = %s"]
        params = [int(match_id)]
    else:
        where = ["competition_id = %s", "(team_id = %s OR opponent_team_id = %s)"]
        params = [int(comp_id), int(team_id), int(team_id)]
    if game_state and game_state != "All":
        where.append("game_state = %s")
        params.append(game_state.lower())
    return execute_query(
        f"SELECT {cols} FROM {tbl} WHERE {' AND '.join(where)}",  # noqa: S608
        tuple(params),
    )


@ttl_cache()
def fetch_match_meta(comp_id: int, match_key: int) -> pd.DataFrame:
    """Single-match home/away name lookup — used only in single-match mode.

    Post-PR 2 (ADR-011): fct_match_summary_synced is keyed on match_key;
    match_id was removed from its final SELECT. Filter on match_key.
    """
    ms_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT home_team_id, away_team_id, home_team_name, away_team_name"  # noqa: S608
        f" FROM {ms_tbl}"
        f" WHERE competition_id = %s AND match_key = %s"
        f" LIMIT 1",
        (int(comp_id), int(match_key)),
    )


def rollup_stages(rows: pd.DataFrame, *, gs_filtered: bool) -> dict[str, int]:
    """Collapse mart rows into funnel totals, honoring V01 straddler semantics.

    rows must be pre-filtered to a single side (selected team OR opponent — the
    caller splits the mart df on team_id vs opponent_team_id).

    gs_filtered = True  → use pos_in_gs (straddlers count once per gs they touched)
    gs_filtered = False → dedup pos_in_match across (match_id, team_id) then sum
                          (handles the per-match replication across gs rows)

    LL2 Path B: possession_id is canonical heuristic across all 4 sources, so
    pos_in_gs / pos_in_match capture all possessions natively. The pre-LL2
    wy_match_flag synthetic-possession compensation has been removed.
    """
    if rows.empty:
        return dict.fromkeys(_STAGE_KEYS, 0)
    if gs_filtered:
        possessions = int(rows["pos_in_gs"].sum())
    else:
        possessions = int(rows.groupby(["match_id", "team_id"])["pos_in_match"].first().sum())
    return {
        "possessions": possessions,
        "a3_entries": int(rows["a3_entries"].sum()),
        "shots": int(rows["shots"].sum()),
        "goals": int(rows["goals"].sum()),
    }


def compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]:
    """Compute step-wise and end-to-end conversion rates (percentages 0-100)."""

    def _pct(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0

    return {
        "poss_to_a3": _pct(stages["a3_entries"], stages["possessions"]),
        "a3_to_shot": _pct(stages["shots"], stages["a3_entries"]),
        "shot_to_goal": _pct(stages["goals"], stages["shots"]),
        "end_to_end": _pct(stages["goals"], stages["possessions"]),
    }
