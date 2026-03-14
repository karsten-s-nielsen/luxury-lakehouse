"""Player Similarity page — find similar players via pgvector cosine distance."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_player_radar
from streamlit_app.components.filters import render_competition_filter
from streamlit_app.db import execute_query, t

# Default metrics with display labels and reasonable ranges (mirrors player_radar.py)
_DEFAULT_METRICS: list[tuple[str, str, tuple[float, float]]] = [
    ("goals_per_90", "Goals/90", (0, 1.5)),
    ("xg_per_90", "xG/90", (0, 1.5)),
    ("passes_per_90", "Passes/90", (0, 80)),
    ("progressive_passes_per_90", "Prog. Passes/90", (0, 12)),
    ("pass_completion_pct", "Pass %", (40, 100)),
    ("xg_overperformance", "xG Over-perf", (-5, 5)),
    ("line_breaking_per_90", "LB Passes/90", (0, 5)),
    ("vaep_per_90", "VAEP/90", (-0.5, 1.5)),
    ("offensive_vaep_per_90", "Off. VAEP/90", (-0.5, 1.5)),
    ("defensive_vaep_per_90", "Def. VAEP/90", (-0.5, 1.0)),
    ("defcon_per_90", "DEFCON/90", (-0.5, 2.0)),
]


# Allowlists for column names interpolated into SQL (defence-in-depth)
_ALLOWED_VECTOR_COLUMNS: frozenset[str] = frozenset({"behavioral_vector", "stat_vector"})
_ALLOWED_COUNT_COLUMNS: frozenset[str] = frozenset({"total_matches", "matches_in_sample"})


def _format_vector_literal(vector: list[float]) -> str:
    """Convert a Python list of floats to a pgvector literal string.

    Example: [0.1, 0.2, 0.3] -> "[0.1,0.2,0.3]"
    """
    return "[" + ",".join(str(v) for v in vector) + "]"


def _get_vector_column(search_type: str) -> str:
    """Return the vector column name based on the search type radio selection."""
    if search_type == "Playing style":
        return "behavioral_vector"
    return "stat_vector"


def _get_vector_dimension(search_type: str) -> int:
    """Return the vector dimension based on the search type radio selection."""
    if search_type == "Playing style":
        return 32
    return 13


def _get_table_and_columns(competition_id: int | None) -> tuple[str, str]:
    """Return the raw synced table name (without schema prefix) and total column.

    No competition (or "All") -> career table with total_matches.
    Specific competition -> season table with matches_in_sample.

    Callers should wrap the table name with ``t()`` before use in queries.
    """
    if competition_id is None:
        return "fct_player_embeddings_career_synced", "total_matches"
    return "fct_player_embeddings_season_synced", "matches_in_sample"


@st.cache_data(ttl=600, show_spinner="Loading player vector...")
def _fetch_player_embedding_vector(tbl: str, pid: str, comp_id: int | None) -> Any:
    if comp_id is not None:
        return execute_query(
            f"SELECT behavioral_vector, stat_vector "  # noqa: S608
            f"FROM {tbl} WHERE canonical_player_id = %s "
            f"AND competition_id = %s",
            (pid, comp_id),
        )
    return execute_query(
        f"SELECT behavioral_vector, stat_vector "  # noqa: S608
        f"FROM {tbl} WHERE canonical_player_id = %s",
        (pid,),
    )


def _fetch_target_vector(
    table: str,
    player_id: str,
    competition_id: int | None,
) -> Any:
    """Fetch the target player's embedding vectors."""
    return _fetch_player_embedding_vector(
        table, str(player_id), int(competition_id) if competition_id is not None else None
    )


@st.cache_data(ttl=600, show_spinner="Finding similar players...")
def _fetch_similar_players(
    tbl: str,
    dim_players_tbl: str,
    vec_str: str,
    vec_col: str,
    vec_dim: int,
    tot_col: str,
    pid: str,
    min_m: int,
    lim: int,
    comp_id: int | None,
) -> Any:
    comp_filter = ""
    params: list[Any] = [vec_str, min_m, pid, lim]
    if comp_id is not None:
        comp_filter = "AND e.competition_id = %s "
        params = [vec_str, min_m, comp_id, pid, lim]

    return execute_query(
        f"SELECT e.canonical_player_id, p.player_display_name, "  # noqa: S608
        f"  p.data_sources, "
        f"  e.{tot_col}, "
        f"  e.{vec_col}::text::vector({vec_dim}) <=> %s::vector({vec_dim}) AS distance "
        f"FROM {tbl} e "
        f"JOIN {dim_players_tbl} p "
        f"  ON e.canonical_player_id = p.canonical_player_id "
        f"WHERE e.{tot_col} >= %s " + comp_filter + "  AND e.canonical_player_id != %s "
        "ORDER BY distance LIMIT %s",
        tuple(params),
    )


def _search_similar_players(
    table: str,
    vector_str: str,
    vector_col: str,
    vector_dim: int,
    total_col: str,
    player_id: str,
    min_matches: int,
    limit: int,
    competition_id: int | None,
) -> Any:
    """Run pgvector cosine distance query to find similar players."""
    if vector_col not in _ALLOWED_VECTOR_COLUMNS:
        msg = f"Invalid vector column: {vector_col}"
        raise ValueError(msg)
    if total_col not in _ALLOWED_COUNT_COLUMNS:
        msg = f"Invalid count column: {total_col}"
        raise ValueError(msg)

    return _fetch_similar_players(
        table,
        t("dim_players_synced"),
        vector_str,
        vector_col,
        vector_dim,
        total_col,
        str(player_id),
        min_matches,
        limit,
        int(competition_id) if competition_id is not None else None,
    )


@st.cache_data(ttl=600, show_spinner="Loading radar stats...")
def _fetch_similarity_radar_stats(
    stats_tbl: str, players_tbl: str, placeholders: str, pids: tuple[str, ...], comp_id: int | None
) -> Any:
    comp_clause = ""
    params: list[Any] = list(pids)
    if comp_id is not None:
        comp_clause = "AND ps.competition_id = %s "
        params.append(comp_id)

    return execute_query(
        f"SELECT sub.canonical_player_id, sub.player_display_name, "  # noqa: S608
        f"  sub.minutes_played, sub.goals_per_90, sub.xg_per_90, "
        f"  sub.passes_per_90, sub.progressive_passes_per_90, "
        f"  sub.pass_completion_pct, sub.xg_overperformance, "
        f"  sub.line_breaking_per_90, "
        f"  sub.vaep_per_90, sub.offensive_vaep_per_90, sub.defensive_vaep_per_90, "
        f"  sub.defcon_per_90 "
        f"FROM ("
        f"  SELECT p.canonical_player_id, p.player_display_name, "
        f"    ps.minutes_played, ps.goals_per_90, ps.xg_per_90, "
        f"    ps.passes_per_90, ps.progressive_passes_per_90, "
        f"    ps.pass_completion_pct, ps.xg_overperformance, "
        f"    ps.line_breaking_per_90, "
        f"    ps.vaep_per_90, ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
        f"    ps.defcon_per_90, "
        f"    ROW_NUMBER() OVER (PARTITION BY p.canonical_player_id ORDER BY ps.minutes_played DESC) AS rn "
        f"  FROM {stats_tbl} ps "
        f"  JOIN {players_tbl} p ON ps.player_id = p.player_id "
        f"  WHERE p.canonical_player_id IN ({placeholders}) " + comp_clause + ") sub "
        "WHERE sub.rn = 1",
        tuple(params),
    )


def _load_radar_stats(canonical_player_ids: list[str], competition_id: int | None) -> Any:
    """Load per-90 stats for radar comparison of two players.

    Accepts canonical_player_id values (from embedding tables) and maps them
    to player_id via dim_players for the fct_player_stats join.
    """
    pids = tuple(str(pid) for pid in canonical_player_ids)
    placeholders = ", ".join(["%s"] * len(pids))
    return _fetch_similarity_radar_stats(
        t("fct_player_stats_synced"),
        t("dim_players_synced"),
        placeholders,
        pids,
        int(competition_id) if competition_id is not None else None,
    )


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_embedding_players(tbl: str, dim_players_tbl: str, tot_col: str, min_m: int, comp_id: int | None) -> Any:
    comp_filter = ""
    params: list[Any] = [min_m]
    if comp_id is not None:
        comp_filter = "AND e.competition_id = %s "
        params.append(comp_id)

    return execute_query(
        f"SELECT DISTINCT e.canonical_player_id, p.player_display_name "  # noqa: S608
        f"FROM {tbl} e "
        f"JOIN {dim_players_tbl} p "
        f"  ON e.canonical_player_id = p.canonical_player_id "
        f"WHERE e.{tot_col} >= %s " + comp_filter + "ORDER BY p.player_display_name",
        tuple(params),
    )


def _render_player_select_for_embeddings(
    table: str,
    total_col: str,
    min_matches: int,
    competition_id: int | None,
) -> str | None:
    """Render a player selectbox filtered to those with embedding vectors."""
    if total_col not in _ALLOWED_COUNT_COLUMNS:
        msg = f"Invalid count column: {total_col}"
        raise ValueError(msg)

    df = _fetch_embedding_players(
        table,
        t("dim_players_synced"),
        total_col,
        min_matches,
        int(competition_id) if competition_id is not None else None,
    )
    if df is None or len(df) == 0:
        st.info("No players with embeddings found for the selected filters.")
        return None

    options = df.to_dict("records")
    label_to_id = {r["player_display_name"]: str(r["canonical_player_id"]) for r in options}
    labels = sorted(label_to_id.keys())

    selected = st.selectbox("Player", labels, index=None, placeholder="Type to search...")
    if selected is None:
        return None
    return label_to_id[selected]


def page() -> None:
    """Render the Player Similarity page."""
    st.header(":material/search: Player Similarity")
    st.caption(
        "Find similar players using pgvector cosine distance on behavioral (32-d) or statistical (13-d) "
        "embedding vectors. Behavioral embeddings via "
        "[Theiner et al. (2022)](https://doi.org/10.1007/978-3-031-02044-5_2) football2vec "
        "with [Doc2Vec (Le & Mikolov 2014)](https://arxiv.org/abs/1405.4053)."
    )

    with st.sidebar:
        search_type: str = st.radio(
            "Search by",
            ["Playing style", "Statistical output"],
            index=0,
        )  # type: ignore[assignment]

        filter_by_competition = st.checkbox("Filter by competition", value=False)
        competition_id: int | None = None
        if filter_by_competition:
            competition_id = render_competition_filter()

        min_matches: int = st.slider(
            "Min. matches",
            min_value=1,
            max_value=50,
            value=5,
        )  # type: ignore[assignment]

        raw_table, total_col = _get_table_and_columns(competition_id)
        table = t(raw_table)

        player_id = _render_player_select_for_embeddings(
            table,
            total_col,
            min_matches,
            competition_id,
        )

        limit: int = st.selectbox("Results", [5, 10, 20], index=1)  # type: ignore[assignment]

    if player_id is None:
        st.info("Select a player to find similar profiles.")
        return

    # Fetch target player's vector
    vector_col = _get_vector_column(search_type)
    vector_dim = _get_vector_dimension(search_type)
    target_result = _fetch_target_vector(table, player_id, competition_id)

    if target_result is None or len(target_result) == 0:
        st.warning("No embedding vector found for the selected player.")
        return

    raw_vector = target_result.iloc[0][vector_col]
    if raw_vector is None:
        st.warning(f"Selected player has no {search_type.lower()} vector.")
        return

    # Convert to list if needed (PG may return string or list)
    if isinstance(raw_vector, str):
        # Parse pgvector string "[0.1,0.2,...]" -> list of floats
        cleaned = raw_vector.strip("[]")
        vector: list[float] = [float(x) for x in cleaned.split(",")]
    else:
        vector = [float(x) for x in raw_vector]

    if len(vector) != vector_dim:
        st.error(f"Vector dimension mismatch: expected {vector_dim}, got {len(vector)}.")
        return

    vector_str = _format_vector_literal(vector)

    # Run similarity search
    results = _search_similar_players(
        table=table,
        vector_str=vector_str,
        vector_col=vector_col,
        vector_dim=vector_dim,
        total_col=total_col,
        player_id=player_id,
        min_matches=min_matches,
        limit=limit,
        competition_id=competition_id,
    )

    if results is None or len(results) == 0:
        st.info("No similar players found. Try lowering the minimum matches threshold.")
        return

    st.subheader("Similar Players")
    display_cols = ["player_display_name", "distance", total_col, "data_sources"]
    available_cols = [c for c in display_cols if c in results.columns]
    st.dataframe(
        results[available_cols].rename(
            columns={
                "player_display_name": "Player",
                "distance": "Cosine Distance",
                total_col: "Matches",
                "data_sources": "Sources",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    # Radar comparison
    st.subheader("Radar Comparison")
    compare_map = dict(zip(results["player_display_name"], results["canonical_player_id"], strict=True))
    compare_selected = st.selectbox("Compare with", list(compare_map.keys()))

    if compare_selected is not None:
        compare_player_id = str(compare_map[compare_selected])
        radar_data = _load_radar_stats([player_id, compare_player_id], competition_id)

        if radar_data is None or len(radar_data) == 0:
            st.info("No per-90 stats available for radar comparison.")
            return

        metric_keys = [m[0] for m in _DEFAULT_METRICS]
        labels = [m[1] for m in _DEFAULT_METRICS]
        ranges = [m[2] for m in _DEFAULT_METRICS]

        players_data: list[dict[str, float]] = []
        player_names: list[str] = []
        for _, row in radar_data.iterrows():
            players_data.append({k: float(row.get(k, 0) or 0) for k in metric_keys})
            player_names.append(str(row["player_display_name"]))

        if len(players_data) < 1:
            st.info("Not enough data for radar comparison.")
            return

        title = " vs ".join(player_names)
        fig = plot_player_radar(
            players_data,
            metric_keys,
            labels,
            ranges,
            title=title,
            player_names=player_names,
        )

        _, col_radar, _ = st.columns([1, 2, 1])
        with col_radar:
            st.pyplot(fig)
