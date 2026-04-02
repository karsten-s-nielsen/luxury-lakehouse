"""Shared filter query functions — all return (label, id) tuples.

Every function returns list[tuple[str, id_type]] with human-readable labels.
No raw IDs ever reach the user. SQL uses parameterized %s placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

from cache import ttl_cache
from db import execute_query, t

logger = logging.getLogger(__name__)

# Security allowlists for column-name interpolation (not parameterizable via %s)
_ALLOWED_EMBEDDING_TABLES = frozenset(
    {
        "fct_player_embeddings_career_synced",
        "fct_player_embeddings_season_synced",
    }
)
_ALLOWED_COUNT_COLUMNS = frozenset({"total_matches", "matches_in_sample"})


def _validate_column(col: str, allowlist: frozenset[str], label: str) -> str:
    """Validate a column name against an allowlist. Raises ValueError if invalid."""
    if col not in allowlist:
        msg = f"Invalid {label}: {col!r}. Allowed: {allowlist}"
        raise ValueError(msg)
    return col


# ---------------------------------------------------------------------------
# Standard filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_competitions() -> list[tuple[str, int]]:
    """Competitions with human-readable 'country — name' labels."""
    tbl = t("dim_competitions_synced")
    df = execute_query(
        f"SELECT DISTINCT competition_id, competition_name, country "  # noqa: S608
        f"FROM {tbl} ORDER BY country, competition_name LIMIT 50",
    )
    if df.empty:
        return []
    return [
        (
            f"{r['country']} \u2014 {r['competition_name']}" if r.get("country") else str(r["competition_name"]),
            int(r["competition_id"]),
        )
        for _, r in df.iterrows()
    ]


@ttl_cache()
def fetch_teams(competition_id: int) -> list[tuple[str, int]]:
    """Teams that appear in matches for this competition."""
    df = execute_query(
        f"SELECT DISTINCT t.team_id, t.team_name "  # noqa: S608
        f"FROM {t('dim_teams_synced')} t "
        f"WHERE t.team_id IN ("
        f"  SELECT m.home_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_id = %s "
        f"  UNION "
        f"  SELECT m.away_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_id = %s"
        f") ORDER BY t.team_name",
        (int(competition_id), int(competition_id)),
    )
    if df.empty:
        return []
    return [(str(r["team_name"]), int(r["team_id"])) for _, r in df.iterrows()]


@ttl_cache()
def fetch_matches(competition_id: int, team_id: int | None) -> list[tuple[str, int]]:
    """Matches for a competition, optionally filtered by team.

    When team_id is None, returns all matches (supports Heat Map allow_all mode).
    """
    conditions = ["competition_id = %s"]
    params: list[Any] = [int(competition_id)]
    if team_id is not None:
        conditions.append("(home_team_id = %s OR away_team_id = %s)")
        params.extend([int(team_id), int(team_id)])
    where = " AND ".join(conditions)
    tbl = t("fct_match_summary_synced")
    df = execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_score, away_score "
        f"FROM {tbl} WHERE {where} "
        f"ORDER BY match_date DESC LIMIT 200",
        tuple(params),
    )
    if df.empty:
        return []
    return [
        (
            f"{r.get('match_date', '')} \u2014 {r['home_team_name']} "
            f"{int(r.get('home_score', 0) or 0)}-{int(r.get('away_score', 0) or 0)} "
            f"{r['away_team_name']}",
            int(r["match_id"]),
        )
        for _, r in df.iterrows()
    ]


@ttl_cache()
def fetch_players(competition_id: int, team_id: int | None) -> list[tuple[str, int]]:
    """Players with stats in this competition, optionally filtered by team.

    Team filtering uses EXISTS subqueries against fct_shots and fct_passes
    (which have team_id) rather than fct_player_stats (which does not).
    """
    conditions = ["ps.competition_id = %s"]
    params: list[Any] = [int(competition_id)]
    if team_id is not None:
        shots_tbl = t("fct_shots_synced")
        passes_tbl = t("fct_passes_synced")
        conditions.append(
            f"(EXISTS (SELECT 1 FROM {shots_tbl} sh"  # noqa: S608
            f"         WHERE sh.player_id = ps.player_id AND sh.team_id = %s)"
            f" OR EXISTS (SELECT 1 FROM {passes_tbl} pa"
            f"            WHERE pa.player_id = ps.player_id AND pa.team_id = %s))"
        )
        params.extend([int(team_id), int(team_id)])
    where = " AND ".join(conditions)
    df = execute_query(
        f"SELECT DISTINCT ps.player_id, p.player_display_name "  # noqa: S608
        f"FROM {t('fct_player_stats_synced')} ps "
        f"JOIN {t('dim_players_synced')} p ON ps.player_id = p.player_id "
        f"WHERE {where} "
        f"ORDER BY p.player_display_name LIMIT 500",
        tuple(params),
    )
    if df.empty:
        return []
    return [(str(r["player_display_name"]), int(r["player_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# Tracking filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_tracking_matches(provider: str | None) -> list[tuple[str, str]]:
    """Tracking matches with labels from match summary.

    provider: 'metrica', 'idsse', 'skillcorner', or None for all.
    """
    tracking_tbl = t("fct_tracking_frames_synced")
    match_tbl = t("fct_match_summary_synced")
    provider_clause = ""
    params: tuple[Any, ...] = ()
    if provider and provider != "All":
        provider_clause = "WHERE source_provider = %s"
        params = (provider,)

    # Recursive CTE loose index scan for distinct match_ids
    df = execute_query(
        f"WITH RECURSIVE dm AS ("  # noqa: S608
        f"  SELECT MIN(match_id) AS match_id FROM {tracking_tbl} {provider_clause}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_id) FROM {tracking_tbl} "
        f"    {'WHERE source_provider = %s AND' if provider and provider != 'All' else 'WHERE'} "
        f"    match_id > dm.match_id)"
        f"  FROM dm WHERE dm.match_id IS NOT NULL"
        f") SELECT dm.match_id, "
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    'Match ' || dm.match_id) AS match_label "
        f"FROM dm "
        f"LEFT JOIN {match_tbl} ms ON dm.match_id::text = ms.match_id::text "
        f"WHERE dm.match_id IS NOT NULL "
        f"ORDER BY match_label LIMIT 100",
        params * 2 if params else (),
    )
    if df.empty:
        return []
    return [(str(r["match_label"]), str(r["match_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# DEFCON-specific filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_defcon_competitions() -> list[tuple[str, int]]:
    """Competitions that have DEFCON pressure data."""
    defcon_tbl = t("fct_defcon_pressure_synced")
    comp_tbl = t("dim_competitions_synced")
    df = execute_query(
        f"WITH RECURSIVE dc AS ("  # noqa: S608
        f"  SELECT MIN(competition_id) AS competition_id FROM {defcon_tbl}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(competition_id) FROM {defcon_tbl} WHERE competition_id > dc.competition_id)"
        f"  FROM dc WHERE dc.competition_id IS NOT NULL"
        f") SELECT dc.competition_id, c.competition_name, c.country "
        f"FROM dc "
        f"JOIN {comp_tbl} c ON dc.competition_id = c.competition_id "
        f"WHERE dc.competition_id IS NOT NULL "
        f"ORDER BY c.competition_name",
    )
    if df.empty:
        return []
    return [
        (
            f"{r['country']} \u2014 {r['competition_name']}" if r.get("country") else str(r["competition_name"]),
            int(r["competition_id"]),
        )
        for _, r in df.iterrows()
    ]


@ttl_cache()
def fetch_defcon_teams(competition_id: int) -> list[tuple[str, int]]:
    """Teams with DEFCON pressure data in this competition."""
    defcon_tbl = t("fct_defcon_pressure_synced")
    match_tbl = t("fct_match_summary_synced")
    teams_tbl = t("dim_teams_synced")
    df = execute_query(
        f"SELECT DISTINCT t.team_id, t.team_name "  # noqa: S608
        f"FROM {defcon_tbl} dp "
        f"JOIN {match_tbl} ms ON dp.match_id::text = ms.match_id::text "
        f"JOIN {teams_tbl} t ON ms.home_team_id = t.team_id OR ms.away_team_id = t.team_id "
        f"WHERE dp.competition_id = %s "
        f"ORDER BY t.team_name",
        (int(competition_id),),
    )
    if df.empty:
        return []
    return [(str(r["team_name"]), int(r["team_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# PAUSA-specific filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_pausa_matches() -> list[tuple[str, str]]:
    """Matches with PAUSA data, labeled from match summary."""
    pausa_tbl = t("fct_pausa_values_synced")
    match_tbl = t("fct_match_summary_synced")
    df = execute_query(
        f"WITH RECURSIVE dm AS ("  # noqa: S608
        f"  SELECT MIN(match_id) AS match_id FROM {pausa_tbl}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_id) FROM {pausa_tbl} WHERE match_id > dm.match_id)"
        f"  FROM dm WHERE dm.match_id IS NOT NULL"
        f") SELECT dm.match_id, "
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    'Match ' || dm.match_id) AS match_label "
        f"FROM dm "
        f"LEFT JOIN {match_tbl} ms ON dm.match_id::text = ms.match_id::text "
        f"WHERE dm.match_id IS NOT NULL "
        f"ORDER BY match_label LIMIT 100",
    )
    if df.empty:
        return []
    return [(str(r["match_label"]), str(r["match_id"])) for _, r in df.iterrows()]


@ttl_cache()
def fetch_pausa_teams(match_id: str) -> list[tuple[str, str]]:
    """Teams in a PAUSA match (raw tracking team identifiers)."""
    pausa_tbl = t("fct_pausa_values_synced")
    df = execute_query(
        f"WITH RECURSIVE dt AS ("  # noqa: S608
        f"  SELECT MIN(team) AS team FROM {pausa_tbl} WHERE match_id = %s AND team IS NOT NULL"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(team) FROM {pausa_tbl} WHERE match_id = %s AND team IS NOT NULL AND team > dt.team)"
        f"  FROM dt WHERE dt.team IS NOT NULL"
        f") SELECT team FROM dt WHERE team IS NOT NULL ORDER BY team LIMIT 50",
        (match_id, match_id),
    )
    if df.empty:
        return []
    return [(str(r["team"]), str(r["team"])) for _, r in df.iterrows()]


@ttl_cache()
def fetch_pausa_players(match_id: str, team: str | None) -> list[tuple[str, str]]:
    """Players in a PAUSA match, optionally filtered by team."""
    pausa_tbl = t("fct_pausa_values_synced")
    dim_tbl = t("dim_players_synced")
    if team:
        df = execute_query(
            f"WITH RECURSIVE dp AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {pausa_tbl}"
            f"  WHERE match_id = %s AND team = %s AND player_id IS NOT NULL"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {pausa_tbl}"
            f"          WHERE match_id = %s AND team = %s AND player_id IS NOT NULL"
            f"          AND player_id > dp.player_id)"
            f"  FROM dp WHERE dp.player_id IS NOT NULL"
            f") SELECT dp.player_id, COALESCE(dim.player_display_name, dp.player_id) AS player_display_name "
            f"FROM dp "
            f"LEFT JOIN {dim_tbl} dim ON dp.player_id::text = dim.player_id::text "
            f"WHERE dp.player_id IS NOT NULL "
            f"ORDER BY player_display_name LIMIT 50",
            (match_id, team, match_id, team),
        )
    else:
        df = execute_query(
            f"WITH RECURSIVE dp AS ("  # noqa: S608
            f"  SELECT MIN(player_id) AS player_id FROM {pausa_tbl}"
            f"  WHERE match_id = %s AND player_id IS NOT NULL"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(player_id) FROM {pausa_tbl}"
            f"          WHERE match_id = %s AND player_id IS NOT NULL"
            f"          AND player_id > dp.player_id)"
            f"  FROM dp WHERE dp.player_id IS NOT NULL"
            f") SELECT dp.player_id, COALESCE(dim.player_display_name, dp.player_id) AS player_display_name "
            f"FROM dp "
            f"LEFT JOIN {dim_tbl} dim ON dp.player_id::text = dim.player_id::text "
            f"WHERE dp.player_id IS NOT NULL "
            f"ORDER BY player_display_name LIMIT 50",
            (match_id, match_id),
        )
    if df.empty:
        return []
    return [(str(r["player_display_name"]), str(r["player_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# Embedding-specific filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_embedding_players(
    competition_id: int | None,
    min_matches: int,
    table: str,
    count_col: str,
) -> list[tuple[str, str]]:
    """Players from embedding table, filtered by min matches.

    table and count_col are validated against allowlists before interpolation.
    """
    _validate_column(table, _ALLOWED_EMBEDDING_TABLES, "embedding table")
    _validate_column(count_col, _ALLOWED_COUNT_COLUMNS, "count column")
    emb_tbl = t(table)
    dim_tbl = t("dim_players_synced")

    conditions = [f"{count_col} >= %s"]
    params: list[Any] = [min_matches]
    if competition_id is not None:
        conditions.append("competition_id = %s")
        params.append(int(competition_id))
    where = " AND ".join(conditions)

    df = execute_query(
        f"WITH RECURSIVE ep AS ("  # noqa: S608
        f"  SELECT MIN(canonical_player_id) AS canonical_player_id FROM {emb_tbl} WHERE {where}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(canonical_player_id) FROM {emb_tbl} WHERE {where}"
        f"          AND canonical_player_id > ep.canonical_player_id)"
        f"  FROM ep WHERE ep.canonical_player_id IS NOT NULL"
        f") SELECT ep.canonical_player_id, p.player_display_name "
        f"FROM ep "
        f"JOIN {dim_tbl} p ON ep.canonical_player_id = p.canonical_player_id "
        f"WHERE ep.canonical_player_id IS NOT NULL "
        f"ORDER BY p.player_display_name LIMIT 2000",
        tuple(params * 2),
    )
    if df.empty:
        return []
    return [(str(r["player_display_name"]), str(r["canonical_player_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# Action Values player filter
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_action_value_players(competition_id: int, team_id: int | None) -> list[tuple[str, int]]:
    """Players from action values table (for Breakdown sub-view inline dropdown)."""
    av_tbl = t("fct_action_values_synced")
    dim_tbl = t("dim_players_synced")
    conditions = ["av.competition_id = %s"]
    params: list[Any] = [int(competition_id)]
    if team_id is not None:
        conditions.append("av.team_id = %s")
        params.append(int(team_id))
    where = " AND ".join(conditions)

    df = execute_query(
        f"WITH RECURSIVE ap AS ("  # noqa: S608
        f"  SELECT MIN(player_id) AS player_id FROM {av_tbl} WHERE {where}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(player_id) FROM {av_tbl} WHERE {where} AND player_id > ap.player_id)"
        f"  FROM ap WHERE ap.player_id IS NOT NULL"
        f") SELECT ap.player_id, p.player_display_name "
        f"FROM ap "
        f"JOIN {dim_tbl} p ON ap.player_id = p.player_id "
        f"WHERE ap.player_id IS NOT NULL "
        f"ORDER BY p.player_display_name LIMIT 200",
        tuple(params * 2),
    )
    if df.empty:
        return []
    return [(str(r["player_display_name"]), int(r["player_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# Scope label & data freshness (shared across all pages)
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_scope_label(comp_id: int, team_id: int | None) -> str:
    """Build scope label string: 'Showing: Country — Competition · Team'."""
    try:
        comp_df = execute_query(
            f"SELECT country, competition_name FROM {t('dim_competitions_synced')} "  # noqa: S608
            f"WHERE competition_id = %s LIMIT 1",
            (int(comp_id),),
        )
        if comp_df.empty:
            return ""
        country = comp_df.iloc[0]["country"]
        comp_name = comp_df.iloc[0]["competition_name"]
        label = f"Showing: {country} \u2014 {comp_name}" if country else f"Showing: {comp_name}"
        if team_id is not None:
            team_df = execute_query(
                f"SELECT team_name FROM {t('dim_teams_synced')} "  # noqa: S608
                f"WHERE team_id = %s LIMIT 1",
                (int(team_id),),
            )
            if not team_df.empty:
                label += f" \u00b7 {team_df.iloc[0]['team_name']}"
        return label
    except Exception:
        return ""


@ttl_cache()
def fetch_data_freshness() -> str:
    """Latest match date from fct_match_summary_synced."""
    try:
        df = execute_query(
            f"SELECT MAX(match_date) AS latest_match FROM {t('fct_match_summary_synced')} LIMIT 1",  # noqa: S608
        )
        if not df.empty and df.iloc[0]["latest_match"] is not None:
            return f"Latest match data: {df.iloc[0]['latest_match']}"
    except Exception:
        return "Data freshness unavailable."
    return ""
