"""Shared filter state variables + cascade callbacks.

All variables here are unprefixed — they're the only unprefixed state.
Per-page state modules use mandatory prefixes (sm_, ms_, pm_, etc.).
"""

from __future__ import annotations

import logging
from typing import Any

from filters import (
    NO_MATCHES_SENTINEL,
    fetch_competitions,
    fetch_matches,
    fetch_players,
    fetch_teams,
    fetch_tracking_matches,
    search_players,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exported state variables
# ---------------------------------------------------------------------------
current_page: str = "Shot-Map"

# Standard filter state
selected_competition: str | None = None
selected_team: str | None = None
selected_match: str | None = None
selected_player: str | None = None
selected_players_multi: list[str] = []

# Server-driven autocomplete query — bound to the search <|input|> above the
# Player dropdown. Empty string means "show top-50 alphabetical"; non-empty
# fires a backend SQL substring search via filters.search_players.
player_search_query: str = ""

# Filter option lists (list of values for Taipy selectors)
competition_lov: list[str] = []
team_lov: list[str] = []
match_lov: list[str] = []
player_lov: list[str] = []
player_lov_multi: list[str] = []  # same as player_lov but without "All" (for multi-select)

# Tracking filters
selected_provider: str | None = "All"
provider_lov: list[str] = ["All", "metrica", "idsse", "skillcorner"]
selected_tracking_match: str | None = None
tracking_match_lov: list[str] = []

# xG model filter
selected_xg_model: str | None = "StatsBomb"
xg_model_lov: list[str] = ["StatsBomb", "Custom (Logistic)", "Custom (XGBoost)"]

# Sub-view selectors
selected_sub_view: str | None = None
sub_view_lov: list[str] = []

# Min filter sliders
min_passes: int = 3
min_minutes: int = 90

# Header panel toggles
show_getting_started: bool = False
show_glossary: bool = False

# Footer control — dashboard pages set to False and render the footer
# inside their scroll wrapper.  Other pages use the site-wide footer.
show_site_footer: bool = True

# Loading state — bound to a spinner overlay in the template
is_loading: bool = False
loading_text: str = "Loading..."

__all__ = [
    # State variables
    "current_page",
    "selected_competition",
    "selected_team",
    "selected_match",
    "selected_player",
    "selected_players_multi",
    "player_search_query",
    "competition_lov",
    "team_lov",
    "match_lov",
    "player_lov",
    "player_lov_multi",
    "selected_provider",
    "provider_lov",
    "selected_tracking_match",
    "tracking_match_lov",
    "selected_xg_model",
    "xg_model_lov",
    "selected_sub_view",
    "sub_view_lov",
    "min_passes",
    "min_minutes",
    "show_getting_started",
    "show_glossary",
    "show_site_footer",
    "is_loading",
    "loading_text",
    "toggle_getting_started",
    "toggle_glossary",
    # Callbacks (must be in main.py namespace for Taipy template resolution)
    "on_init",
    "on_navigate",
    "on_competition_change",
    "on_team_change",
    "on_match_change",
    "on_player_change",
    "on_player_search_change",
    "on_provider_change",
    "on_tracking_match_change",
    "on_xg_model_change",
    "on_min_passes_change",
    "on_min_minutes_change",
    "on_sub_view_change",
]

# ---------------------------------------------------------------------------
# Internal lookup maps (NOT exported — not bound to UI)
# ---------------------------------------------------------------------------
_comp_map: dict[str, int] = {}
_team_map: dict[str, int] = {}
_match_map: dict[str, int] = {}
_player_map: dict[str, int] = {}
_tracking_match_map: dict[str, str] = {}

# Page refresh registry — pages register their refresh functions here
_page_refreshers: dict[str, Any] = {}

# Dashboard pages render the footer inside their scroll wrapper, so the
# site-wide footer must be hidden.  Pages self-declare via is_dashboard=True
# at registration time.  _refresh_current_page derives show_site_footer
# from this set, removing the need for imperative state.show_site_footer
# assignments in individual page refresh functions.
_dashboard_pages: set[str] = set()
_page_teardowns: dict[str, Any] = {}  # page_name -> teardown callback (no args)


def register_page_refresher(page_name: str, fn: Any, *, is_dashboard: bool = False) -> None:
    """Register a page-specific refresh function called on filter changes.

    Args:
        page_name: Route key (e.g., "AI-ML-Workflows").
        fn: Refresh callback accepting (state).
        is_dashboard: True for dashboard-layout pages that render their own
            footer inside the scroll wrapper.  The site-wide footer is
            hidden automatically for these pages.
    """
    _page_refreshers[page_name] = fn
    if is_dashboard:
        _dashboard_pages.add(page_name)


def register_page_teardown(page_name: str, fn: Any) -> None:
    """Register a teardown callback invoked when the user navigates away from *page_name*.

    Teardowns run before the new page's refresher. Use for stopping timers,
    cancelling background tasks, or releasing resources tied to a page.
    """
    _page_teardowns[page_name] = fn


_LOADING_TEXTS: dict[str, str] = {
    "Shot-Map": "Loading shots...",
    "Pass-Map": "Loading passes...",
    "Heat-Map": "Loading actions...",
    "Pass-Network": "Loading passes...",
    "Match-Summary": "Loading match data...",
    "Player-Impact": "Loading VAEP data...",
    "Player-Comparison": "Loading player stats...",
    "Player-Similarity": "Finding similar players...",
    "Movement-Pressing": "Loading movement data...",
    "Pitch-Control": "Computing pitch control...",
    "Team-Shape": "Loading team shape...",
    "Pass-Timing": "Loading PAUSA data...",
    "Defensive-Impact": "Loading defensive data...",
}


def _refresh_current_page(state: Any) -> None:
    """Call the current page's refresh function if registered."""
    # Dashboard pages render the footer inside their scroll wrapper,
    # so the site-wide footer must be hidden.  Derived from the
    # _dashboard_pages set (populated at registration time).
    state.show_site_footer = state.current_page not in _dashboard_pages

    # Run teardown for any page the user is navigating away from.
    for page_name, teardown in _page_teardowns.items():
        if page_name != state.current_page:
            try:
                teardown()
            except Exception:
                logger.debug("Teardown failed for %s", page_name, exc_info=True)

    fn = _page_refreshers.get(state.current_page)
    if fn:
        state.loading_text = _LOADING_TEXTS.get(state.current_page, "Loading...")
        state.is_loading = True
        try:
            fn(state)
        except Exception:
            logger.exception("Failed to refresh page %s", state.current_page)
        finally:
            state.is_loading = False


def get_comp_id(label: str | None) -> int | None:
    """Resolve competition label to ID."""
    return _comp_map.get(label) if label else None  # type: ignore[arg-type]


_ALL_LABEL = "All"


def get_team_id(label: str | None) -> int | None:
    """Resolve team label to ID. Returns None for 'All' or empty."""
    if not label or label == _ALL_LABEL:
        return None
    return _team_map.get(label)  # type: ignore[arg-type]


def get_match_id(label: str | None) -> int | None:
    """Resolve match label to ID. Returns None for 'All' or empty."""
    if not label or label == _ALL_LABEL:
        return None
    return _match_map.get(label)  # type: ignore[arg-type]


def get_player_id(label: str | None) -> int | None:
    """Resolve player label to ID. Returns None for 'All' or empty."""
    if not label or label == _ALL_LABEL:
        return None
    return _player_map.get(label)  # type: ignore[arg-type]


def get_tracking_match_id(label: str | None) -> str | None:
    """Resolve tracking match label to ID."""
    return _tracking_match_map.get(label) if label else None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def on_init(state: Any) -> None:
    """Load competitions and tracking matches on app start."""
    global _comp_map, _tracking_match_map
    try:
        comps = fetch_competitions()
        _comp_map = {label: cid for label, cid in comps}
        state.competition_lov = [label for label, _ in comps]
        logger.info("Loaded %d competitions", len(comps))
    except Exception:
        logger.exception("Failed to load competitions")

    # Pre-load tracking matches for Movement/Pitch Control pages
    try:
        matches = fetch_tracking_matches(None)  # All providers
        _tracking_match_map = {label: mid for label, mid in matches}
        state.tracking_match_lov = [label for label, _ in matches]
        logger.info("Loaded %d tracking matches", len(matches))
    except Exception:
        logger.exception("Failed to load tracking matches")


def on_navigate(state: Any, page_name: str, *_args: Any) -> None:
    """Track current page for conditional sidebar rendering + refresh page data."""
    state.current_page = page_name
    logger.info("Navigated to %s", page_name)
    # Refresh the target page's data if a competition is already selected
    _refresh_current_page(state)


def on_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    """Competition changed — reload dependents, reset selections."""
    global _team_map, _match_map, _player_map
    comp_id = get_comp_id(var_value)
    if comp_id is None:
        return
    logger.info("Competition: %r (id=%d)", var_value, comp_id)

    # NOTE (2026-04-16 audit): we deliberately do NOT call clear_cache() here.
    # The 2026-04-16 EXPLAIN-ANALYZE audit showed that wiping every per-function
    # cache on comp switch was a net regression — 13 zero-arg cached functions
    # (fetch_competitions, fetch_data_freshness, fetch_cold_costs, etc.) and
    # ~25 match-keyed functions are all comp-independent, so clearing them on
    # every comp change is pure cache waste.  The bounded TTLCache + LRU
    # eviction in cache.py already handles the two concerns the clear was
    # trying to address: (a) unbounded growth is bounded by maxsize=2000,
    # (b) stale per-comp entries naturally evict under LRU pressure.

    # Reset dependents to "All" (clearable)
    state.selected_team = _ALL_LABEL
    state.selected_match = _ALL_LABEL
    state.selected_player = _ALL_LABEL
    state.selected_players_multi = []
    # Reset the player-search input — competition change implies a fresh scope.
    state.player_search_query = ""

    try:
        teams = fetch_teams(comp_id)
        _team_map = {label: tid for label, tid in teams}
        state.team_lov = [_ALL_LABEL] + [label for label, _ in teams]

        matches = fetch_matches(comp_id, None)
        _match_map = {label: mid for label, mid in matches}
        state.match_lov = [_ALL_LABEL] + [label for label, _ in matches]

        # Full player list still drives _player_map (so any search-result selection
        # resolves to the right ID) and player_lov_multi (the dropdown_multi widget
        # on Player Comparison still shows every player). The single-select
        # player_lov shows only top-50 alphabetical to keep the dropdown payload
        # bounded; user types in the search input to find anyone outside that set.
        # NOTE: we slice fetch_players' result directly rather than firing a
        # second search_players("") query — see TODO.md #30 for the shared-state
        # race window this keeps from widening.
        players = fetch_players(comp_id, None)
        _player_map = {label: pid for label, pid in players}
        state.player_lov_multi = [label for label, _ in players]
        state.player_lov = [_ALL_LABEL] + [label for label, _ in players[:50]]

        _refresh_current_page(state)
    except Exception:
        logger.exception("Failed on competition change")


def on_team_change(state: Any, var_name: str, var_value: Any) -> None:
    """Team changed — reload matches and players for this team."""
    global _match_map, _player_map
    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(var_value)
    if comp_id is None:
        return

    state.selected_match = _ALL_LABEL
    state.selected_player = _ALL_LABEL
    state.selected_players_multi = []
    # Reset the player-search input — team narrows scope, so any prior typed
    # query may now match a different set; clearing keeps semantics predictable.
    state.player_search_query = ""

    try:
        matches = fetch_matches(comp_id, team_id)
        _match_map = {label: mid for label, mid in matches}
        state.match_lov = [_ALL_LABEL] + [label for label, _ in matches]

        # Full list -> _player_map + player_lov_multi (see on_competition_change comment).
        # Single-select player_lov is the top-50 slice of the same full list —
        # NOT a second search_players("") call, per TODO.md #30 race-window concern.
        players = fetch_players(comp_id, team_id)
        _player_map = {label: pid for label, pid in players}
        state.player_lov_multi = [label for label, _ in players]
        state.player_lov = [_ALL_LABEL] + [label for label, _ in players[:50]]

        _refresh_current_page(state)
    except Exception:
        logger.exception("Failed on team change")


def on_match_change(state: Any, var_name: str, var_value: Any) -> None:
    """Match changed — refresh current page."""
    _refresh_current_page(state)


def on_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """Player changed — refresh current page.

    Handles both single-select (selected_player) and multi-select
    (selected_players_multi). Taipy auto-binds the value before calling.
    """
    logger.info("Player change: var_name=%s, var_value=%r", var_name, var_value)
    _refresh_current_page(state)


def on_player_search_change(state: Any, var_name: str, var_value: Any) -> None:
    """Server-driven autocomplete: refresh player_lov from a backend SQL substring search.

    Fired by the <|input|> above the Player dropdown after a 300 ms debounce.
    Empty query returns top-50 alphabetical (in scope); non-empty returns up to
    500 substring matches. Scope is the current selected_competition + selected_team.
    The shared _player_map already has every player from fetch_players, so any
    selection from a search result resolves to the right ID even after the user
    keeps typing and the original entry leaves the visible LOV.
    """
    global _player_map
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        return
    team_id = get_team_id(state.selected_team)
    query = var_value or ""
    try:
        results = search_players(query, comp_id, team_id, top_n_when_empty=50)
    except Exception:
        logger.exception("Player search failed for query=%r comp=%s team=%s", query, comp_id, team_id)
        return
    # Merge new ID mappings (results are a subset of fetch_players already, but
    # cross-comp searches and search-only paths still benefit from the merge).
    _player_map.update(dict(results))
    if results:
        state.player_lov = [_ALL_LABEL] + [label for label, _ in results]
    else:
        # Empty result: surface the sentinel so the dropdown visibly updates
        # (a bare [] is treated as no-op by the Taipy state diff).
        state.player_lov = [_ALL_LABEL, NO_MATCHES_SENTINEL]
    logger.info("Player search: query=%r -> %d results", query, len(results))


def on_provider_change(state: Any, var_name: str, var_value: Any) -> None:
    """Provider changed — reload tracking matches and refresh page.

    Clearing selected_tracking_match without refreshing leaves stale chart
    data from the previous provider visible until a new match is selected.
    """
    global _tracking_match_map

    provider = var_value if var_value != "All" else None
    state.selected_tracking_match = None

    try:
        matches = fetch_tracking_matches(provider)
        _tracking_match_map = {label: mid for label, mid in matches}
        state.tracking_match_lov = [label for label, _ in matches]
    except Exception:
        logger.exception("Failed on provider change")

    _refresh_current_page(state)


def on_tracking_match_change(state: Any, var_name: str, var_value: Any) -> None:
    """Tracking match changed — refresh current page."""
    _refresh_current_page(state)


def on_xg_model_change(state: Any, var_name: str, var_value: Any) -> None:
    """xG model changed — refresh current page."""
    _refresh_current_page(state)


def on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min passes slider changed — refresh current page."""
    _refresh_current_page(state)


def on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Min minutes slider changed — refresh current page."""
    _refresh_current_page(state)


def on_sub_view_change(state: Any, var_name: str, var_value: Any) -> None:
    """Sub-view selector changed — refresh current page."""
    _refresh_current_page(state)


def toggle_getting_started(state: Any) -> None:
    """Toggle Getting Started panel visibility."""
    state.show_getting_started = not state.show_getting_started
    state.show_glossary = False


def toggle_glossary(state: Any) -> None:
    """Toggle Glossary panel visibility."""
    state.show_glossary = not state.show_glossary
    state.show_getting_started = False


# ---------------------------------------------------------------------------
