"""Shared filter query functions — all return (label, id) tuples.

Every function returns list[tuple[str, id_type]] with human-readable labels.
No raw IDs ever reach the user. SQL uses parameterized %s placeholders.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
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
# Server-driven autocomplete search — shared infrastructure
#
# Library-style helper for backend substring search across any (name, id) lookup.
# Each per-entity search_* function below builds its own SELECT/FROM/WHERE
# fragment and delegates to _execute_search_query, which appends the substring
# predicate, optional GROUP BY, ORDER BY, LIMIT, executes the query, and
# normalises the result tuples.
#
# Empty query returns top_n_when_empty alphabetical rows (so the dropdown
# is usable without typing). Non-empty query returns up to `limit` substring
# matches.
#
# LIKE wildcards (%, _, \) in user input are escaped with a single backslash
# and the SQL uses `ESCAPE '\'`, so user input cannot inject wildcards.
# ---------------------------------------------------------------------------

# When a search-callback's filter (SQL or in-memory) returns zero rows, callers
# substitute this sentinel string into the LOV. It surfaces "no matches" in the
# dropdown AND forces Taipy/MUI to re-render — empirically, setting `state.lov = []`
# is treated as a no-op by the Taipy state diff, so the previous (stale) LOV
# remains visible. The sentinel is a non-resolving label: every map lookup
# (_player_map.get, etc.) returns None for it, so a user clicking it has the
# same effect as clearing the filter.
NO_MATCHES_SENTINEL = "(no matches)"


def _escape_like(s: str) -> str:
    """Escape LIKE wildcard metacharacters so user input is treated literally.

    Order matters: escape the escape char first, then the wildcards.
    Use with `LIKE %s ESCAPE '\\'` in the SQL.
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _execute_search_query(
    *,
    select_from_where: str,
    name_column: str,
    query: str,
    base_params: tuple[Any, ...],
    group_by: str = "",
    limit: int = 500,
    top_n_when_empty: int = 50,
    return_id_type: type = int,
) -> list[tuple[str, Any]]:
    """Execute a substring-search SQL fragment and return (label, id) tuples.

    Args:
        select_from_where: SQL fragment ending in a WHERE clause. The first
            selected column MUST be aliased ``AS name`` (the display label),
            the second ``AS id`` (the lookup key). Must end with WHERE 1=1
            or another non-empty WHERE so the substring predicate can be
            appended with ' AND '.
        name_column: Fully-qualified column to substring-match on (e.g.
            ``p.player_display_name``). Used in the ``LOWER(...) LIKE LOWER(%s)``
            predicate appended to the WHERE.
        query: The user's search input. Empty/whitespace returns the top-N
            alphabetical rows; non-empty appends a case-insensitive substring
            match (``%query%``).
        base_params: Tuple of params bound to the WHERE clause already in
            ``select_from_where``. New params for the substring + limit are
            appended.
        group_by: Optional GROUP BY clause (e.g. ``p.player_id, name``).
            Inserted between the WHERE and ORDER BY when non-empty.
        limit: Max rows for non-empty queries.
        top_n_when_empty: Max rows when query is empty (initial dropdown load).
        return_id_type: Constructor for the second tuple element (e.g.
            ``int``, ``str``). Preserves the original lookup-key type.

    Returns:
        List of (display_name, id) tuples. Empty list if no rows match.
    """
    q = (query or "").strip()
    pieces: list[str] = [select_from_where]
    params: list[Any] = list(base_params)
    if q:
        pieces.append(f"AND LOWER({name_column}) LIKE LOWER(%s) ESCAPE '\\'")
        params.append(f"%{_escape_like(q)}%")
    if group_by:
        pieces.append(f"GROUP BY {group_by}")
    pieces.append("ORDER BY name LIMIT %s")
    params.append(int(limit if q else top_n_when_empty))
    sql = " ".join(pieces)
    df = execute_query(sql, tuple(params))
    if df.empty:
        return []
    return [(str(r["name"]), return_id_type(r["id"])) for _, r in df.iterrows()]


def search_players(
    query: str,
    competition_id: int | None,
    team_id: int | None = None,
    *,
    limit: int = 500,
    top_n_when_empty: int = 50,
) -> list[tuple[str, int]]:
    """Backend substring-search for players by display name.

    Mirrors fetch_players' "has stats in comp AND (shot for team OR pass for
    team)" semantics so a search returns the same player set the page would
    otherwise filter by, just narrowed by the typed query.

    Args:
        query: Substring to match against player_display_name (case-insensitive).
            Empty returns top_n_when_empty alphabetical players in scope.
        competition_id: Required scope; None falls back to cross-competition
            (any player with stats in any synced competition).
        team_id: Optional team narrowing. Only applied when competition_id
            is also set.
        limit: Max non-empty-query results.
        top_n_when_empty: Max empty-query results.

    Returns:
        list[(player_display_name, player_id)] — int IDs.
    """
    stats_tbl = t("fct_player_stats_synced")
    players_tbl = t("dim_players_synced")

    if team_id is not None and competition_id is not None:
        shots_tbl = t("fct_shots_synced")
        passes_tbl = t("fct_passes_synced")
        select_from_where = (
            f"WITH team_players AS ("  # noqa: S608
            f"  SELECT DISTINCT player_id FROM {shots_tbl}"
            f"  WHERE competition_id = %s AND team_id = %s"
            f"  UNION"
            f"  SELECT DISTINCT player_id FROM {passes_tbl}"
            f"  WHERE competition_id = %s AND team_id = %s"
            f") "
            f"SELECT p.player_display_name AS name, p.player_id AS id "
            f"FROM {stats_tbl} ps "
            f"JOIN {players_tbl} p ON ps.player_id = p.player_id "
            f"JOIN team_players tp ON tp.player_id = ps.player_id "
            f"WHERE ps.competition_id = %s"
        )
        base_params: tuple[Any, ...] = (
            int(competition_id),
            int(team_id),
            int(competition_id),
            int(team_id),
            int(competition_id),
        )
    elif competition_id is not None:
        select_from_where = (
            f"SELECT p.player_display_name AS name, p.player_id AS id "  # noqa: S608
            f"FROM {stats_tbl} ps "
            f"JOIN {players_tbl} p ON ps.player_id = p.player_id "
            f"WHERE ps.competition_id = %s"
        )
        base_params = (int(competition_id),)
    else:
        select_from_where = (
            f"SELECT p.player_display_name AS name, p.player_id AS id "  # noqa: S608
            f"FROM {stats_tbl} ps "
            f"JOIN {players_tbl} p ON ps.player_id = p.player_id "
            f"WHERE 1=1"
        )
        base_params = ()

    return _execute_search_query(
        select_from_where=select_from_where,
        name_column="p.player_display_name",
        query=query,
        base_params=base_params,
        group_by="p.player_display_name, p.player_id",
        limit=limit,
        top_n_when_empty=top_n_when_empty,
        return_id_type=int,
    )


def search_goalkeepers(
    query: str,
    competition_id: int,
    team_id: int | None = None,
    *,
    limit: int = 500,
    top_n_when_empty: int = 50,
) -> list[tuple[str, int]]:
    """Backend substring-search for goalkeepers (GK-only) by display name.

    Mirrors queries.goalkeepers.fetch_gk_player_lov semantics: only players with
    rows in fct_goalkeeper_stats for the selected competition (+optional team).
    Orders by display name (alpha) rather than minutes DESC so search results
    are predictable — user types "van der", sees matches in alpha order.
    """
    gk_tbl = t("fct_goalkeeper_stats_synced")
    players_tbl = t("dim_players_synced")
    base_predicates = ["gk.competition_id = %s"]
    base_params: list[Any] = [int(competition_id)]
    if team_id is not None:
        base_predicates.append("gk.team_id = %s")
        base_params.append(int(team_id))
    where = " AND ".join(base_predicates)
    select_from_where = (
        f"SELECT p.player_display_name AS name, p.player_id AS id "  # noqa: S608
        f"FROM {gk_tbl} gk "
        f"JOIN {players_tbl} p ON gk.player_id = p.player_id "
        f"WHERE {where}"
    )
    return _execute_search_query(
        select_from_where=select_from_where,
        name_column="p.player_display_name",
        query=query,
        base_params=tuple(base_params),
        group_by="p.player_display_name, p.player_id",
        limit=limit,
        top_n_when_empty=top_n_when_empty,
        return_id_type=int,
    )


def search_pausa_players(
    query: str,
    match_id: str,
    team: str | None = None,
    *,
    limit: int = 500,
    top_n_when_empty: int = 50,
) -> list[tuple[str, str]]:
    """Backend substring-search for PAUSA players (string player_id) in a match.

    Mirrors fetch_pausa_players: player must have rows in fct_pausa_values for
    this match (+optional team). String IDs because PAUSA uses IDSSE tracking
    identifiers, not StatsBomb int IDs.
    """
    pausa_tbl = t("fct_pausa_values_synced")
    dim_tbl = t("dim_players_synced")
    base_predicates = ["pv.match_id = %s", "pv.player_id IS NOT NULL"]
    base_params: list[Any] = [str(match_id)]
    if team:
        base_predicates.append("pv.team = %s")
        base_params.append(str(team))
    where = " AND ".join(base_predicates)
    select_from_where = (
        f"SELECT COALESCE(dim.player_display_name, pv.player_id) AS name, pv.player_id AS id "  # noqa: S608
        f"FROM {pausa_tbl} pv "
        f"LEFT JOIN {dim_tbl} dim ON pv.player_id::text = dim.player_id::text "
        f"WHERE {where}"
    )
    return _execute_search_query(
        select_from_where=select_from_where,
        name_column="COALESCE(dim.player_display_name, pv.player_id)",
        query=query,
        base_params=tuple(base_params),
        group_by="pv.player_id, dim.player_display_name",
        limit=limit,
        top_n_when_empty=top_n_when_empty,
        return_id_type=str,
    )


def search_embedding_players(
    query: str,
    competition_id: int | None,
    min_matches: int,
    table: str,
    count_col: str,
    *,
    limit: int = 500,
    top_n_when_empty: int = 50,
) -> list[tuple[str, str]]:
    """Backend substring-search for players in an embedding table (canonical_player_id, str).

    Mirrors fetch_embedding_players semantics: scope = (table * min_matches * optional comp).
    The `table` + `count_col` inputs are validated against their allowlists (SQL-injection-safe).
    Cross-competition when competition_id is None — this is THE critical case because
    the full embedding set is ~9000 players; client-side filter was loading the whole
    list into the browser for the Player-Similarity page.
    """
    _validate_column(table, _ALLOWED_EMBEDDING_TABLES, "embedding table")
    _validate_column(count_col, _ALLOWED_COUNT_COLUMNS, "count column")
    emb_tbl = t(table)
    dim_tbl = t("dim_players_synced")
    base_predicates = [f"emb.{count_col} >= %s"]
    base_params: list[Any] = [int(min_matches)]
    if competition_id is not None:
        base_predicates.append("emb.competition_id = %s")
        base_params.append(int(competition_id))
    where = " AND ".join(base_predicates)
    select_from_where = (
        f"SELECT p.player_display_name AS name, emb.canonical_player_id AS id "  # noqa: S608
        f"FROM {emb_tbl} emb "
        f"JOIN {dim_tbl} p ON emb.canonical_player_id = p.canonical_player_id "
        f"WHERE {where}"
    )
    return _execute_search_query(
        select_from_where=select_from_where,
        name_column="p.player_display_name",
        query=query,
        base_params=tuple(base_params),
        group_by="p.player_display_name, emb.canonical_player_id",
        limit=limit,
        top_n_when_empty=top_n_when_empty,
        return_id_type=str,
    )


# ---------------------------------------------------------------------------
# Standard filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_competitions() -> list[tuple[str, int, int]]:
    """Competitions with human-readable 'country -- name' labels.

    Post-PR 2 (ADR-011): dim_competitions_synced is keyed on
    `competition_key` (BIGINT surrogate). We also surface the legacy
    INT `competition_id` so callers can route to the right fact table —
    `competition_key` for Kimball-migrated facts (fct_passes,
    fct_match_summary), legacy `competition_id` for still-legacy facts
    (fct_shots, fct_action_values, fct_defcon_*) until their own
    migration PRs. For IDSSE rows the legacy `competition_id` is 0
    (sentinel — non-numeric native IDs); IDSSE competitions don't
    appear in legacy fact tables anyway, so the 0 sentinel is safe.

    Returns: list of (label, competition_key, competition_id_legacy_int).
    """
    # PR 5a (ADR-011) extended dim_competitions to 4 providers (StatsBomb,
    # Wyscout, IDSSE, Metrica). Same display name (e.g. "England — Premier
    # League") can appear under both StatsBomb and Wyscout — different
    # competition_keys, identical label. Dedupe per (country,
    # competition_name) with provider preference (SB > WS > IDSSE > Metrica)
    # so the user sees one row per competition. Downstream fact queries are
    # competition_key-keyed; the chosen key picks the provider whose data
    # populates the page. Long-term: introduce canonical_competition_key
    # similar to canonical_team_key / canonical_player_key from PR 5a's
    # entity resolution work.
    tbl = t("dim_competitions_synced")
    df = execute_query(
        f"SELECT DISTINCT ON (c.country, c.competition_name) "  # noqa: S608
        f"  c.competition_key, c.competition_id, c.competition_name, c.country "
        f"FROM {tbl} c "
        f"ORDER BY c.country, c.competition_name, "
        f"  CASE c.provider "
        f"    WHEN 'statsbomb' THEN 1 "
        f"    WHEN 'wyscout' THEN 2 "
        f"    WHEN 'idsse' THEN 3 "
        f"    WHEN 'metrica' THEN 4 "
        f"    ELSE 5 "
        f"  END "
        f"LIMIT 100",
    )
    if df.empty:
        return []
    return [
        (
            f"{r['country']} \u2014 {r['competition_name']}" if r.get("country") else str(r["competition_name"]),
            int(r["competition_key"]),
            int(r["competition_id"]) if pd.notna(r.get("competition_id")) else 0,
        )
        for _, r in df.iterrows()
    ]


@ttl_cache()
def fetch_teams(competition_key: int) -> list[tuple[str, int]]:
    """Teams that appear in matches for this competition.

    Post-PR 2 (ADR-011): filters on fct_match_summary_synced.competition_key.
    IDSSE rows in fct_match_summary have NULL team_id (DFL team IDs are
    strings, not covered by dim_teams yet) so IDSSE competitions return
    empty team lists — Pass Map's Team=All selector still works to show
    all passes in those matches.
    """
    df = execute_query(
        f"SELECT t.team_id, t.team_name "  # noqa: S608
        f"FROM {t('dim_teams_synced')} t "
        f"WHERE t.team_id IN ("
        f"  SELECT m.home_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_key = %s "
        f"  UNION "
        f"  SELECT m.away_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_key = %s"
        f") ORDER BY t.team_name",
        (int(competition_key), int(competition_key)),
    )
    if df.empty:
        return []
    return [(str(r["team_name"]), int(r["team_id"])) for _, r in df.iterrows()]


def _format_match_label(r: Any) -> str:
    """Build a match dropdown label, suppressing NULL date and NULL scores.

    StatsBomb/Wyscout rows have all fields populated. IDSSE rows come from
    DFL open data, which ships position XML but not scoreboard data, so
    match_date / home_score / away_score are NULL. Rather than surface
    "None — Team 0-0 Team" (which misleadingly implies a 0-0 result),
    the label gracefully degrades to "Team vs Team" for score-less rows
    and prepends the date only when it exists.
    """
    home = r["home_team_name"]
    away = r["away_team_name"]
    has_date = pd.notna(r.get("match_date"))
    has_score = pd.notna(r.get("home_score")) and pd.notna(r.get("away_score"))
    date_prefix = f"{r['match_date']} \u2014 " if has_date else ""
    if has_score:
        hs = int(r["home_score"])
        as_ = int(r["away_score"])
        return f"{date_prefix}{home} {hs}-{as_} {away}"
    return f"{date_prefix}{home} vs {away}"


@ttl_cache()
def fetch_matches(competition_key: int, team_id: int | None) -> list[tuple[str, int, int]]:
    """Matches for a competition, optionally filtered by team.

    Post-PR 2 (ADR-011): fct_match_summary_synced is keyed on `match_key`
    (Kimball surrogate BIGINT). We also surface the native `match_id`
    (INT, via dim_matches JOIN) so callers can route to the right
    fact table — `match_key` for Kimball-migrated facts (fct_passes,
    fct_match_summary, fct_line_breaking_results), native `match_id`
    for still-legacy facts (fct_shots, fct_action_values,
    fct_funnel_stages_agg, etc.) until their own migration PRs.

    Returns `list[tuple[str, int, int]]` — (label, match_key, match_id).
    For IDSSE/Metrica rows (native match_id is non-numeric), match_id
    is 0; those matches do not appear in legacy match_id-keyed facts
    so the 0 sentinel is safe.

    When team_id is None, returns all matches (Heat Map allow_all mode).
    """
    conditions = ["ms.competition_key = %s"]
    params: list[Any] = [int(competition_key)]
    if team_id is not None:
        conditions.append("(ms.home_team_id = %s OR ms.away_team_id = %s)")
        params.extend([int(team_id), int(team_id)])
    where = " AND ".join(conditions)
    ms_tbl = t("fct_match_summary_synced")
    dm_tbl = t("dim_matches_synced")
    df = execute_query(
        f"SELECT ms.match_key, "  # noqa: S608
        f"  CASE WHEN dm.native_match_id ~ '^[0-9]+$' "
        f"    THEN CAST(dm.native_match_id AS BIGINT) ELSE 0 END AS native_match_id_int, "
        f"  ms.match_date, ms.home_team_name, ms.away_team_name, "
        f"  ms.home_score, ms.away_score "
        f"FROM {ms_tbl} ms "
        f"LEFT JOIN {dm_tbl} dm ON ms.match_key = dm.match_key "
        f"WHERE {where} "
        f"ORDER BY ms.match_date DESC LIMIT 200",
        tuple(params),
    )
    if df.empty:
        return []
    return [
        (
            _format_match_label(r),
            int(r["match_key"]),
            int(r["native_match_id_int"]) if pd.notna(r.get("native_match_id_int")) else 0,
        )
        for _, r in df.iterrows()
    ]


@ttl_cache()
def fetch_players(competition_id: int, team_id: int | None) -> list[tuple[str, int]]:
    """Players with stats in this competition, optionally filtered by team.

    Team filtering uses a pre-computed team_players CTE (UNION of distinct
    player_ids from fct_shots and fct_passes filtered by team) rather than
    correlated EXISTS subqueries, because the prior implementation evaluated
    two EXISTS per candidate player — O(N_players * 2 subquery lookups) —
    and measured 168 ms for comp=11, team=552. The CTE shape filters by
    (competition_id, team_id) so it uses the idx_shots_comp_team_player
    and idx_passes_comp_team_match leading-column composites to build the
    team set once, then JOINs it with fct_player_stats and dim_players.

    Verified against live Lakebase 2026-04-16:
    - CURRENT (2x EXISTS): 168.6 ms, 35 players
    - NEW (CTE UNION):     64.2 ms, 35 players — diff = 0 (semantic equiv)

    Kept semantics identical: "has stats in comp AND (shot for team OR pass
    for team)". NOT switched to fct_action_values because that would include
    tackles/interceptions/carries — a superset — and we want to preserve
    shots-OR-passes as the definition of "played for this team offensively".
    """
    stats_tbl = t("fct_player_stats_synced")
    players_tbl = t("dim_players_synced")

    if team_id is not None:
        shots_tbl = t("fct_shots_synced")
        passes_tbl = t("fct_passes_synced")
        df = execute_query(
            f"WITH team_players AS ("  # noqa: S608
            f"  SELECT DISTINCT player_id FROM {shots_tbl}"
            f"  WHERE competition_id = %s AND team_id = %s"
            f"  UNION"
            f"  SELECT DISTINCT player_id FROM {passes_tbl}"
            f"  WHERE competition_id = %s AND team_id = %s"
            f") "
            # NO LIMIT — a single competition is bounded by the data schema (one season ≈ 500-2000 players max).
            # The combobox virtualizes rendering, payload size is negligible. LIMIT silently hides data, which
            # is worse than the alternative; the composite index on (competition_id, team_id, player_id) keeps
            # the range scan sub-100ms even for the largest competition.
            f"SELECT ps.player_id, p.player_display_name "
            f"FROM {stats_tbl} ps "
            f"JOIN {players_tbl} p ON ps.player_id = p.player_id "
            f"JOIN team_players tp ON tp.player_id = ps.player_id "
            f"WHERE ps.competition_id = %s "
            f"GROUP BY ps.player_id, p.player_display_name "
            f"ORDER BY p.player_display_name",
            (int(competition_id), int(team_id), int(competition_id), int(team_id), int(competition_id)),
        )
    else:
        # NO LIMIT — see comment above. competition_id is indexed; payload is bounded by the season schema.
        df = execute_query(
            f"SELECT ps.player_id, p.player_display_name "  # noqa: S608
            f"FROM {stats_tbl} ps "
            f"JOIN {players_tbl} p ON ps.player_id = p.player_id "
            f"WHERE ps.competition_id = %s "
            f"GROUP BY ps.player_id, p.player_display_name "
            f"ORDER BY p.player_display_name",
            (int(competition_id),),
        )

    if df.empty:
        return []
    return [(str(r["player_display_name"]), int(r["player_id"])) for _, r in df.iterrows()]


# ---------------------------------------------------------------------------
# Tracking filters
# ---------------------------------------------------------------------------


@ttl_cache()
def fetch_tracking_matches(provider: str | None) -> list[tuple[str, str]]:
    """Tracking matches with human-readable labels.

    Post-PR 2 (ADR-011): labels sourced from dim_matches_synced (team
    names populated for IDSSE/Metrica/StatsBomb). The legacy
    dim_tracking_matches table was deleted in PR 2; its data is
    subsumed by dim_matches.

    Returns match_ids from fct_tracking_frames_synced (still the native
    identifier; fct_tracking_frames is not yet Kimball-migrated, so
    IDSSE match_ids remain prefixed with 'idsse_' here). Consumers
    (pitch_control / team_shape / tactical_positions) accept the
    prefixed form and query fct_tracking_frames_synced directly.

    dim_matches.native_match_id is unprefixed for IDSSE, so the JOIN
    strips the 'idsse_' prefix before matching.

    provider: 'metrica', 'idsse', 'skillcorner', or None for all.
    """
    tracking_tbl = t("fct_tracking_frames_synced")
    match_tbl = t("fct_match_summary_synced")
    dim_tbl = t("dim_matches_synced")
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
        f"  COALESCE("
        f"    ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    dim_m.home_team_name || ' v ' || dim_m.away_team_name, "
        f"    'Match ' || dm.match_id"
        f"  ) AS match_label "
        f"FROM dm "
        f"LEFT JOIN {dim_tbl} dim_m "
        f"  ON dim_m.native_match_id = regexp_replace(dm.match_id::text, '^idsse_', '') "
        f"LEFT JOIN {match_tbl} ms ON dim_m.match_key = ms.match_key "
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
    """Teams with DEFCON pressure data in this competition.

    Uses GROUP BY instead of SELECT DISTINCT to leverage index-based grouping.
    """
    defcon_tbl = t("fct_defcon_pressure_synced")
    match_tbl = t("fct_match_summary_synced")
    teams_tbl = t("dim_teams_synced")
    df = execute_query(
        f"SELECT t.team_id, t.team_name "  # noqa: S608
        f"FROM {defcon_tbl} dp "
        f"JOIN {match_tbl} ms ON dp.match_id::text = ms.match_id::text "
        f"JOIN {teams_tbl} t ON ms.home_team_id = t.team_id OR ms.away_team_id = t.team_id "
        f"WHERE dp.competition_id = %s "
        f"GROUP BY t.team_id, t.team_name "
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
    """Matches with PAUSA data, labeled from match summary.

    Iterates on match_key (Kimball FK) and joins fct_match_summary_synced
    on match_key. A scalar subquery retrieves match_id for downstream
    filter compatibility (fetch_pausa_teams / fetch_pausa_players filter
    on fct_pausa_values_synced.match_id).
    """
    pausa_tbl = t("fct_pausa_values_synced")
    match_tbl = t("fct_match_summary_synced")
    df = execute_query(
        f"WITH RECURSIVE dm AS ("  # noqa: S608
        f"  SELECT MIN(match_key) AS match_key FROM {pausa_tbl}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_key) FROM {pausa_tbl} WHERE match_key > dm.match_key)"
        f"  FROM dm WHERE dm.match_key IS NOT NULL"
        f") SELECT dm.match_key, "
        f"  (SELECT pv.match_id FROM {pausa_tbl} pv WHERE pv.match_key = dm.match_key LIMIT 1) AS match_id, "
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    'Match ' || dm.match_key) AS match_label "
        f"FROM dm "
        f"LEFT JOIN {match_tbl} ms ON dm.match_key = ms.match_key "
        f"WHERE dm.match_key IS NOT NULL "
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
    """Players from action values table (for Breakdown sub-view inline dropdown).

    Access path rationale (verified against live Lakebase 2026-04-16 audit):

    - comp-only path: subquery DISTINCT on fct_action_values_synced then
      JOIN dim_players. Measured 409 ms for comp=11 (~2,131 distinct players
      out of 9.5M rows). The prior recursive-CTE implementation timed out at
      >90 s on this path — the recursive MIN(player_id) step could not use
      the idx_action_values_comp_team_player composite because team_id is
      not in the filter, so each recursive probe scanned competition-wide
      action rows. This rewrite replaces N recursive scans with one DISTINCT
      pass; the Unique node deduplicates in memory.

    - comp+team path: same subquery-DISTINCT shape, adds `team_id = %s`.
      Measured 8 ms for (comp=11, team=552). Here the existing
      idx_action_values_comp_team_player composite does reduce scan rows by
      team before DISTINCT, so the plan is a bounded index range scan.

    We keep fct_action_values as the source (NOT fct_player_stats) because
    fct_player_stats is missing 8 action-having players for comp=11
    (verified via anti-join probe), and silently dropping them from the
    dropdown is unacceptable. If a future `fct_action_player_index` mart
    is built, this function can be re-pointed at it for <20 ms response.
    """
    av_tbl = t("fct_action_values_synced")
    dim_tbl = t("dim_players_synced")
    conditions = ["competition_id = %s"]
    params: list[Any] = [int(competition_id)]
    if team_id is not None:
        conditions.append("team_id = %s")
        params.append(int(team_id))
    where = " AND ".join(conditions)

    df = execute_query(
        f"SELECT ids.player_id, p.player_display_name "  # noqa: S608
        f"FROM (SELECT DISTINCT player_id FROM {av_tbl} WHERE {where}) ids "
        f"JOIN {dim_tbl} p ON ids.player_id = p.player_id "
        f"ORDER BY p.player_display_name LIMIT 200",
        tuple(params),
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


def build_scope_label_plain(pairs: list[tuple[str, str]]) -> str:
    """Render a plain-text scope label for alt attributes and screen readers.

    HTML rendering of the scope line is handled by the page_template — each
    dimension renders via static Taipy markdown with per-dim state vars,
    because Taipy's `|text|raw|` flag escapes HTML tags. See PageConfig.scope_dims
    and _build_standard_page.

    Args:
        pairs: List of (dimension_label, value) tuples in display order.

    Returns:
        'Competition: {value} \u00b7 Team: {value}' — no HTML, safe for alt text.
        Empty list returns empty string.
    """
    if not pairs:
        return ""
    parts = [f"{label}: {value}" for label, value in pairs]
    return " \u00b7 ".join(parts)


def build_warning(domain: str, suggestions: list[str]) -> str:
    """Render a canonical 'no data' warning message.

    Args:
        domain: Plural domain noun (e.g., 'actions', 'match data', 'passes').
        suggestions: 0-3 short human-phrased next steps.
            Example: ['removing the team filter', 'choosing a different player']

    Returns:
        'No {domain} found for this selection. Try {joined}.'
        Joining: single -> bare; two -> 'a or b'; three+ -> 'a, b or c'.
        Empty suggestions -> 'Try adjusting your filters.'
    """
    if not domain or not domain.strip():
        msg = "domain must be non-empty"
        raise ValueError(msg)
    if not suggestions:
        tail = "Try adjusting your filters."
    elif len(suggestions) == 1:
        tail = f"Try {suggestions[0]}."
    elif len(suggestions) == 2:
        tail = f"Try {suggestions[0]} or {suggestions[1]}."
    else:
        head = ", ".join(suggestions[:-1])
        tail = f"Try {head} or {suggestions[-1]}."
    return f"No {domain} found for this selection. {tail}"


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
