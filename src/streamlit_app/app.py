"""(Right! Luxury!) Lakehouse — Streamlit analytics dashboard entry point."""

from __future__ import annotations

import streamlit as st

from streamlit_app.pages.heat_map import page as heat_map_page
from streamlit_app.pages.match_summary import page as match_summary_page
from streamlit_app.pages.pass_map import page as pass_map_page
from streamlit_app.pages.pass_network import page as pass_network_page
from streamlit_app.pages.pitch_control import page as pitch_control_page
from streamlit_app.pages.player_radar import page as player_radar_page
from streamlit_app.pages.shot_map import page as shot_map_page


def main() -> None:
    """Application entry point with st.navigation page routing."""
    st.set_page_config(
        page_title="(Right! Luxury!) Lakehouse",
        page_icon=":material/sports_soccer:",
        layout="wide",
    )

    st.title(":material/sports_soccer: (Right! Luxury!) Lakehouse")

    pages = [
        st.Page(shot_map_page, title="Shot Map", icon=":material/target:", url_path="shot-map"),
        st.Page(pass_map_page, title="Pass Map", icon=":material/arrow_forward:", url_path="pass-map"),
        st.Page(heat_map_page, title="Heat Map", icon=":material/local_fire_department:", url_path="heat-map"),
        st.Page(pass_network_page, title="Pass Network", icon=":material/hub:", url_path="pass-network"),
        st.Page(player_radar_page, title="Player Radar", icon=":material/radar:", url_path="player-radar"),
        st.Page(match_summary_page, title="Match Summary", icon=":material/scoreboard:", url_path="match-summary"),
        st.Page(pitch_control_page, title="Pitch Control", icon=":material/grid_on:", url_path="pitch-control"),
    ]

    nav = st.navigation({"Analysis": pages})

    with st.sidebar:
        st.caption("Soccer analytics powered by StatsBomb, Metrica Sports & Wyscout open data.")

    nav.run()


if __name__ == "__main__":
    main()
