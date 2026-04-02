"""Player Similarity state module — all variables prefixed with ps_.

pgvector cosine distance search on behavioral (128-d) or statistical (13-d)
embedding vectors. Custom in-page filters (not sidebar). Career vs season
table routing based on competition filter toggle.

Registered as the Player-Similarity page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
from filters import fetch_data_freshness, fetch_embedding_players
from mplsoccer import Radar
from queries.players import fetch_player_embedding_vector, fetch_similarity_radar_stats, search_similar_players
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, PLAYER_COLORS, chart_to_file

from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Allowlists for column names interpolated into SQL (defence-in-depth)
_ALLOWED_VECTOR_COLUMNS: frozenset[str] = frozenset({"behavioral_vector", "stat_vector"})
_ALLOWED_COUNT_COLUMNS: frozenset[str] = frozenset({"total_matches", "matches_in_sample"})

# Default metrics for radar comparison (mirrors player_radar.py)
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

# Distance threshold labels
_DISTANCE_THRESHOLDS_CAPTION = "< 0.20 Very Similar | < 0.35 Similar | < 0.50 Moderate | >= 0.50 Different"


def _interpret_distance(d: float) -> str:
    """Classify cosine distance into a similarity label."""
    if d < 0.20:
        return "Very Similar"
    if d < 0.35:
        return "Similar"
    if d < 0.50:
        return "Moderately Similar"
    return "Different"


# ── Exported state variables (all ps_ prefixed) ─────────────────────────────

# In-page filter state
ps_search_mode: str = "Playing style"
ps_search_mode_lov: list[str] = ["Playing style", "Statistical output"]
ps_filter_by_competition: bool = False
ps_competition_id: int | None = None
ps_competition_lov: list[str] = []
ps_selected_competition: str | None = None
ps_min_matches: int = 5
ps_player_lov: list[str] = []
ps_selected_player: str | None = None
ps_result_count: int = 10
ps_result_count_lov: list[str] = ["5", "10", "20"]

# Results state
_PS_RESULTS_COLS = ["Player", "Cosine Distance", "Similarity", "Matches", "Sources"]
ps_results_data: pd.DataFrame = pd.DataFrame(columns=_PS_RESULTS_COLS)
ps_radar_image: str = ""
ps_spoke_caption: str = ""
ps_compare_lov: list[str] = []
ps_selected_compare: str | None = None
ps_threshold_caption: str = _DISTANCE_THRESHOLDS_CAPTION
ps_status_message: str = ""
ps_warning_text: str = ""

ps_data_freshness: str = ""

__all__ = [
    "ps_data_freshness",
    "on_ps_filter_by_competition_change",
    "on_ps_min_matches_change",
    "on_ps_result_count_change",
    "on_ps_search_mode_change",
    "on_ps_selected_compare_change",
    "on_ps_selected_competition_change",
    "on_ps_selected_player_change",
    "ps_compare_lov",
    "ps_competition_id",
    "ps_competition_lov",
    "ps_filter_by_competition",
    "ps_min_matches",
    "ps_player_lov",
    "ps_radar_image",
    "ps_result_count",
    "ps_spoke_caption",
    "ps_result_count_lov",
    "ps_results_data",
    "ps_search_mode",
    "ps_search_mode_lov",
    "ps_selected_compare",
    "ps_selected_competition",
    "ps_selected_player",
    "ps_status_message",
    "ps_threshold_caption",
    "ps_warning_text",
]


# ── Internal lookup maps (NOT exported) ──────────────────────────────────────

_ps_comp_map: dict[str, int] = {}
_ps_player_map: dict[str, str] = {}  # label -> canonical_player_id
_ps_compare_map: dict[str, str] = {}  # label -> canonical_player_id for results
_ps_results_df: pd.DataFrame = pd.DataFrame()  # cached results for compare selection


# ── Helper functions ─────────────────────────────────────────────────────────


def _get_vector_column(search_mode: str) -> str:
    """Return the vector column name based on search mode."""
    if search_mode == "Playing style":
        return "behavioral_vector"
    return "stat_vector"


def _get_vector_dimension(search_mode: str) -> int:
    """Return the vector dimension based on search mode."""
    if search_mode == "Playing style":
        return 128
    return 13


def _get_table_and_columns(competition_id: int | None) -> tuple[str, str]:
    """Return the raw table name and count column.

    No competition -> career table with total_matches.
    Specific competition -> season table with matches_in_sample.
    """
    if competition_id is None:
        return "fct_player_embeddings_career_synced", "total_matches"
    return "fct_player_embeddings_season_synced", "matches_in_sample"


def _format_vector_literal(vector: list[float]) -> str:
    """Convert a Python list of floats to a pgvector literal string."""
    return "[" + ",".join(str(v) for v in vector) + "]"


# ── Rendering ────────────────────────────────────────────────────────────────


def _render_comparison_radar(
    players_data: list[dict[str, float]],
    player_names: list[str],
) -> str:
    """Render a comparison radar chart for target + similar player."""
    metric_keys = [m[0] for m in _DEFAULT_METRICS]
    labels = [m[1] for m in _DEFAULT_METRICS]
    ranges = [m[2] for m in _DEFAULT_METRICS]
    low = [r[0] for r in ranges]
    high = [r[1] for r in ranges]

    radar = Radar(labels, low, high, round_int=[False] * len(labels), num_rings=4)
    result = radar.setup_axis(figsize=(6, 6), facecolor=PITCH_BG_COLOR)
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    radar.draw_circles(ax=ax, facecolor=PITCH_BG_COLOR, edgecolor="#333355")
    radar.draw_param_labels(ax=ax, color=PITCH_LINE_COLOR, fontsize=8)

    for i, player in enumerate(players_data[:2]):
        values = [player.get(m, 0.0) for m in metric_keys]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": color, "alpha": 0.2},
            kwargs_rings={"facecolor": color, "alpha": 0.1},
        )
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": "none", "edgecolor": color, "linewidth": 2},
            kwargs_rings={"facecolor": "none"},
        )

    if player_names:
        handles = [
            mpatches.Patch(color=PLAYER_COLORS[i % len(PLAYER_COLORS)], alpha=0.6, label=name)
            for i, name in enumerate(player_names[: len(players_data)])
        ]
        ax.legend(
            handles=handles,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.06),
            ncol=len(handles),
            fontsize=8,
            frameon=False,
            labelcolor=PITCH_LINE_COLOR,
        )

    title = " vs ".join(player_names)
    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=11, pad=15, fontweight="bold")
    plt.close(fig)

    return chart_to_file(fig, "ps_comparison_radar")


# ── Callbacks ────────────────────────────────────────────────────────────────


def _clear_results(state: Any) -> None:
    """Reset result-related state variables."""
    state.ps_results_data = pd.DataFrame(columns=_PS_RESULTS_COLS)
    state.ps_radar_image = ""
    state.ps_spoke_caption = ""
    state.ps_compare_lov = []
    state.ps_selected_compare = None
    state.ps_status_message = ""
    state.ps_warning_text = ""


def _clear_all(state: Any) -> None:
    """Reset all ps_ state variables."""
    _clear_results(state)
    state.ps_player_lov = []
    state.ps_selected_player = None


def _resolve_competition_id(state: Any) -> int | None:
    """Resolve current competition selection to an ID, or None."""
    if not state.ps_filter_by_competition or not state.ps_selected_competition:
        return None
    return _ps_comp_map.get(state.ps_selected_competition)


def _load_player_list(state: Any) -> None:
    """Reload the player dropdown from embedding table based on current filters."""
    global _ps_player_map
    comp_id = _resolve_competition_id(state)
    raw_table, count_col = _get_table_and_columns(comp_id)
    min_matches = int(state.ps_min_matches)

    try:
        players = fetch_embedding_players(comp_id, min_matches, raw_table, count_col)
        if not players:
            state.ps_warning_text = "No players with embeddings found."
            return
        _ps_player_map = {label: pid for label, pid in players}
        state.ps_player_lov = [label for label, _ in players]
    except Exception:
        logger.exception("Failed to load embedding players")
        _ps_player_map = {}
        state.ps_player_lov = []


def on_ps_search_mode_change(state: Any, var_name: str, var_value: Any) -> None:
    """Search mode changed — clear results, re-run if player selected."""
    _clear_results(state)
    if state.ps_selected_player:
        _run_similarity_search(state)


def on_ps_filter_by_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    """Competition toggle changed — reload player list."""
    _clear_results(state)
    state.ps_selected_player = None
    _load_player_list(state)


def on_ps_selected_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    """Competition selection changed — reload player list."""
    _clear_results(state)
    state.ps_selected_player = None
    _load_player_list(state)


def on_ps_min_matches_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min matches slider changed — reload player list."""
    _clear_results(state)
    state.ps_selected_player = None
    _load_player_list(state)


def on_ps_selected_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Player selection changed — run similarity search."""
    _clear_results(state)
    if var_value:
        _run_similarity_search(state)


def on_ps_result_count_change(state: Any, var_name: str, var_value: Any) -> None:
    """Result count changed — re-run if player selected."""
    _clear_results(state)
    if state.ps_selected_player:
        _run_similarity_search(state)


def on_ps_selected_compare_change(state: Any, var_name: str, var_value: Any) -> None:
    """Compare player selection changed — render radar."""
    if not var_value:
        state.ps_radar_image = ""
        state.ps_spoke_caption = ""
        return

    player_id = _ps_player_map.get(state.ps_selected_player, "")
    compare_id = _ps_compare_map.get(var_value, "")
    if not player_id or not compare_id:
        state.ps_radar_image = ""
        state.ps_spoke_caption = ""
        return

    comp_id = _resolve_competition_id(state)

    try:
        radar_data = fetch_similarity_radar_stats([player_id, compare_id], comp_id)
        if radar_data.empty:
            state.ps_radar_image = ""
            state.ps_spoke_caption = ""
            return

        metric_keys = [m[0] for m in _DEFAULT_METRICS]
        players_data: list[dict[str, float]] = []
        player_names: list[str] = []
        for _, row in radar_data.iterrows():
            players_data.append({k: float(row.get(k, 0) or 0) for k in metric_keys})
            player_names.append(str(row["player_display_name"]))

        if len(players_data) < 1:
            state.ps_radar_image = ""
            state.ps_spoke_caption = ""
            return

        state.ps_radar_image = _render_comparison_radar(players_data, player_names)

        # Spoke caption — glossary covers metric definitions, show scale context
        state.ps_spoke_caption = "Raw value scaling. See Glossary for metric definitions."
    except Exception:
        logger.exception("Failed to render comparison radar")
        state.ps_radar_image = ""
        state.ps_spoke_caption = ""


def _run_similarity_search(state: Any) -> None:
    """Execute the pgvector similarity search and populate results state."""
    global _ps_compare_map, _ps_results_df

    player_label = state.ps_selected_player
    if not player_label:
        _clear_results(state)
        return

    player_id = _ps_player_map.get(player_label, "")
    if not player_id:
        _clear_results(state)
        return

    comp_id = _resolve_competition_id(state)
    raw_table, total_col = _get_table_and_columns(comp_id)
    search_mode = state.ps_search_mode or "Playing style"
    vector_col = _get_vector_column(search_mode)
    vector_dim = _get_vector_dimension(search_mode)
    limit = int(state.ps_result_count)

    # Show loading feedback during pgvector search (CHI-AUDIT: Gergle feedback)
    state.ps_status_message = f"Searching for similar players ({search_mode})..."

    try:
        # Fetch target vector
        target_result = fetch_player_embedding_vector(raw_table, player_id, comp_id)
        if target_result.empty:
            state.ps_warning_text = "No embedding vector for this player for the selected filters."
            state.ps_status_message = ""
            return

        raw_vector = target_result.iloc[0][vector_col]
        if raw_vector is None:
            state.ps_warning_text = f"No {search_mode.lower()} vector for this player for the selected filters."
            state.ps_status_message = ""
            return

        # Parse vector
        if isinstance(raw_vector, str):
            cleaned = raw_vector.strip("[]")
            vector: list[float] = [float(x) for x in cleaned.split(",")]
        else:
            vector = [float(x) for x in raw_vector]

        if len(vector) != vector_dim:
            other = "Statistical output" if search_mode == "Playing style" else "Playing style"
            state.ps_warning_text = (
                f"This player doesn't have a {search_mode.lower()} embedding. Try switching to {other} search instead."
            )
            state.ps_status_message = ""
            return

        vector_str = _format_vector_literal(vector)

        # Run similarity search
        results = search_similar_players(
            table=raw_table,
            vector_str=vector_str,
            vector_col=vector_col,
            vector_dim=vector_dim,
            total_col=total_col,
            player_id=player_id,
            min_matches=int(state.ps_min_matches),
            limit=limit,
            competition_id=comp_id,
        )

        if results.empty:
            state.ps_warning_text = (
                "No similar players for the selected filters. Try lowering the minimum matches threshold."
            )
            state.ps_status_message = ""
            return

        # Add interpretation column
        results["interpretation"] = results["distance"].apply(_interpret_distance)

        # Format data_sources for display
        if "data_sources" in results.columns:
            results["data_sources"] = results["data_sources"].str.replace(",", " · ")

        # Build compare map
        _ps_compare_map = dict(zip(results["player_display_name"], results["canonical_player_id"], strict=True))
        _ps_results_df = results

        # Build display table (stringified for Taipy)
        display_cols = ["player_display_name", "distance", "interpretation", total_col, "data_sources"]
        available_cols = [c for c in display_cols if c in results.columns]
        display_df = results[available_cols].rename(
            columns={
                "player_display_name": "Player",
                "distance": "Cosine Distance",
                "interpretation": "Similarity",
                total_col: "Matches",
                "data_sources": "Sources",
            }
        )

        # Format distance to 4 decimal places
        if "Cosine Distance" in display_df.columns:
            display_df["Cosine Distance"] = display_df["Cosine Distance"].apply(lambda x: f"{x:.4f}")

        state.ps_results_data = display_df
        state.ps_compare_lov = list(_ps_compare_map.keys())
        state.ps_selected_compare = None
        state.ps_status_message = f"Found {len(results)} similar players."
        state.ps_warning_text = ""

        logger.info("Similarity search: %d results for player %s", len(results), player_id)

    except Exception:
        logger.exception("Similarity search failed")
        _clear_results(state)
        state.ps_warning_text = "Search failed. Please try again."


# ── Page refresh (called on filter cascade) ──────────────────────────────────


def ps_refresh(state: Any) -> None:
    """Refresh player similarity page — reload competitions, player list."""
    global _ps_comp_map

    # Load competitions for the in-page filter
    from filters import fetch_competitions

    try:
        comps = fetch_competitions()
        _ps_comp_map = {label: cid for label, cid in comps}
        state.ps_competition_lov = [label for label, _ in comps]
    except Exception:
        logger.exception("Failed to load competitions for similarity page")

    # Reload player list based on current filter state
    _load_player_list(state)

    # If a player is already selected, re-run the search
    if state.ps_selected_player:
        _run_similarity_search(state)

    state.ps_data_freshness = fetch_data_freshness()


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Player-Similarity", ps_refresh)
