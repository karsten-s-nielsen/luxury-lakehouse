"""(Right! Luxury!) Lakehouse — Streamlit analytics dashboard entry point."""

from __future__ import annotations

import streamlit as st

from streamlit_app.components.glossary import render_glossary_sidebar, render_onboarding_sidebar
from streamlit_app.pages.action_values import page as action_values_page
from streamlit_app.pages.defensive_valuation import page as defensive_valuation_page
from streamlit_app.pages.heat_map import page as heat_map_page
from streamlit_app.pages.match_summary import page as match_summary_page
from streamlit_app.pages.movement_analysis import page as movement_analysis_page
from streamlit_app.pages.pass_map import page as pass_map_page
from streamlit_app.pages.pass_network import page as pass_network_page
from streamlit_app.pages.pass_timing import page as pass_timing_page
from streamlit_app.pages.pitch_control import page as pitch_control_page
from streamlit_app.pages.player_radar import page as player_radar_page
from streamlit_app.pages.player_similarity import page as player_similarity_page
from streamlit_app.pages.shot_map import page as shot_map_page


def main() -> None:
    """Application entry point with st.navigation page routing."""
    st.set_page_config(
        page_title="(Right! Luxury!) Lakehouse",
        page_icon=":material/sports_soccer:",
        layout="wide",
    )

    # Expand sidebar nav + amber accent for visual identity bridge with HF Space (F39)
    st.markdown(
        "<style>"
        "section[data-testid='stSidebar'] nav > ul { max-height: none !important; }"
        "section[data-testid='stSidebar'] { border-top: 3px solid #f59e0b; }"
        "h1 { background: linear-gradient(90deg, #f59e0b 0%, transparent 60%); "
        "-webkit-background-clip: text; -webkit-text-fill-color: transparent; "
        "background-clip: text; }"
        "</style>",
        unsafe_allow_html=True,
    )

    st.title(":material/sports_soccer: (Right! Luxury!) Lakehouse")

    pages: dict[str, list[st.Page]] = {  # type: ignore[type-arg]
        "Match Analysis": [
            st.Page(shot_map_page, title="Shot Map", icon=":material/target:", url_path="shot-map"),
            st.Page(pass_map_page, title="Pass Map", icon=":material/arrow_forward:", url_path="pass-map"),
            st.Page(heat_map_page, title="Heat Map", icon=":material/local_fire_department:", url_path="heat-map"),
            st.Page(pass_network_page, title="Pass Network", icon=":material/hub:", url_path="pass-network"),
            st.Page(match_summary_page, title="Match Summary", icon=":material/scoreboard:", url_path="match-summary"),
        ],
        "Player Analysis": [
            st.Page(action_values_page, title="Player Impact", icon=":material/trending_up:", url_path="action-values"),
            st.Page(player_radar_page, title="Player Comparison", icon=":material/radar:", url_path="player-radar"),
            st.Page(
                player_similarity_page,
                title="Player Similarity",
                icon=":material/search:",
                url_path="player-similarity",
            ),
        ],
        "Advanced": [
            st.Page(
                movement_analysis_page,
                title="Movement & Pressing",
                icon=":material/directions_run:",
                url_path="movement-analysis",
            ),
            st.Page(pitch_control_page, title="Pitch Control", icon=":material/grid_on:", url_path="pitch-control"),
            st.Page(pass_timing_page, title="Pass Timing", icon=":material/timer:", url_path="pass-timing"),
            st.Page(
                defensive_valuation_page,
                title="Defensive Impact",
                icon=":material/shield:",
                url_path="defensive-valuation",
            ),
        ],
    }

    nav = st.navigation(pages)

    # Extract current page URL path for context-sensitive glossary.
    # st.Page stores url_path; try multiple access patterns for compatibility.
    current_page = getattr(nav, "url_path", None) or getattr(nav, "_url_path", None) or ""
    # Last resort: derive from page title → url_path mapping
    if not current_page:
        title = getattr(nav, "title", "") or ""
        title_to_path = {
            "Shot Map": "shot-map",
            "Pass Map": "pass-map",
            "Heat Map": "heat-map",
            "Pass Network": "pass-network",
            "Match Summary": "match-summary",
            "Player Impact": "action-values",
            "Player Comparison": "player-radar",
            "Player Similarity": "player-similarity",
            "Movement & Pressing": "movement-analysis",
            "Pitch Control": "pitch-control",
            "Pass Timing": "pass-timing",
            "Defensive Impact": "defensive-valuation",
        }
        current_page = title_to_path.get(title, "")

    with st.sidebar:
        render_onboarding_sidebar()
        render_glossary_sidebar(page_url_path=current_page)
        st.caption("Soccer analytics powered by StatsBomb, Metrica Sports & Wyscout open data.")
        st.markdown(
            ":material/open_in_new: [Interactive Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)"
            " · [Published Datasets](https://huggingface.co/luxury-lakehouse)",
        )
    nav.run()


if __name__ == "__main__":
    main()
