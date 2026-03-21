"""Taipy app — thin orchestrator. Imports state + pages, runs gui."""

from __future__ import annotations

import logging

from pages.action_values import page_md as action_values_page
from pages.defensive_valuation import page_md as defensive_impact_page
from pages.heat_map import page_md as heat_map_page
from pages.match_summary import page_md as match_summary_page
from pages.movement_analysis import page_md as movement_page
from pages.pass_map import page_md as pass_map_page
from pages.pass_network import page_md as pass_network_page
from pages.pass_timing import page_md as pass_timing_page
from pages.pitch_control import page_md as pitch_control_page
from pages.player_radar import page_md as player_radar_page
from pages.player_similarity import page_md as player_similarity_page

# --- Page layouts ---
from pages.shot_map import page_md as shot_map_page
from pages.widget_spacing_test import *  # noqa: F403
from pages.widget_spacing_test import page_md as spacing_test_page
from state.action_values import *  # noqa: F403
from state.defensive_valuation import *  # noqa: F403
from state.heat_map import *  # noqa: F403
from state.match_summary import *  # noqa: F403
from state.movement_analysis import *  # noqa: F403
from state.pass_map import *  # noqa: F403
from state.pass_network import *  # noqa: F403
from state.pass_timing import *  # noqa: F403
from state.pitch_control import *  # noqa: F403
from state.player_radar import *  # noqa: F403
from state.player_similarity import *  # noqa: F403

# --- State imports (star import required for Taipy module-level binding) ---
from state.shared import *  # noqa: F403
from state.shot_map import *  # noqa: F403
from taipy.gui import Gui
from template import root_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# --- Page registry ---
# Keys = page names shown in navbar. Must match Streamlit CHI-audited titles.
# The on_navigate callback receives these keys as page_name.
pages = {
    "/": root_page,
    "Shot-Map": shot_map_page,
    "Pass-Map": pass_map_page,
    "Heat-Map": heat_map_page,
    "Pass-Network": pass_network_page,
    "Match-Summary": match_summary_page,
    "Player-Impact": action_values_page,
    "Player-Comparison": player_radar_page,
    "Player-Similarity": player_similarity_page,
    "Movement-Pressing": movement_page,
    "Pitch-Control": pitch_control_page,
    "Pass-Timing": pass_timing_page,
    "Defensive-Impact": defensive_impact_page,
    "Widget-Spacing-Test": spacing_test_page,
}

if __name__ == "__main__":
    gui = Gui(pages=pages, css_file="style_v2.css")
    gui.run(
        host="0.0.0.0",
        port=7860,
        title="(Right! Luxury!) Lakehouse",
        dark_mode=True,
        use_reloader=False,
        on_init=on_init,
        on_navigate=on_navigate,
        stylekit={
            "color_primary": "#f59e0b",
            "color_secondary": "#d97706",
            "color_paper": "#1a1a2e",
            "color_background": "#0e1117",
            "color_background_dark": "#0a0d12",
            "color_background_light": "#1a1d23",
            "font_family": "Source Sans Pro, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
        },
    )
