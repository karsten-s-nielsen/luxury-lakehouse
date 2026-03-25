"""Taipy app — thin orchestrator. Imports state + pages, runs gui."""

from __future__ import annotations

import logging

from page_template import PageEntry, build_nav
from pages.action_values import page_config as action_values_config
from pages.action_values import page_md as action_values_page
from pages.defensive_valuation import page_config as defensive_impact_config
from pages.defensive_valuation import page_md as defensive_impact_page
from pages.heat_map import page_config as heat_map_config
from pages.heat_map import page_md as heat_map_page
from pages.match_summary import page_config as match_summary_config
from pages.match_summary import page_md as match_summary_page
from pages.movement_analysis import page_config as movement_config
from pages.movement_analysis import page_md as movement_page
from pages.pass_map import page_config as pass_map_config
from pages.pass_map import page_md as pass_map_page
from pages.pass_network import page_config as pass_network_config
from pages.pass_network import page_md as pass_network_page
from pages.pass_timing import page_config as pass_timing_config
from pages.pass_timing import page_md as pass_timing_page
from pages.pitch_control import page_config as pitch_control_config
from pages.pitch_control import page_md as pitch_control_page
from pages.player_radar import page_config as player_radar_config
from pages.player_radar import page_md as player_radar_page
from pages.player_similarity import page_config as player_similarity_config
from pages.player_similarity import page_md as player_similarity_page

# --- Page layouts ---
from pages.shot_map import page_config as shot_map_config
from pages.shot_map import page_md as shot_map_page
from pages.workflows import page_config as workflows_config
from pages.workflows import page_md as workflows_page
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
from state.workflows import *  # noqa: F403
from state.workflows import RawHtml
from taipy.gui import Gui
from template import build_root_page

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


# --- RawHtml content provider ---
# Taipy's <|{var}|text|raw|> escapes HTML. Register a content provider so
# <|part|content={var}|> renders RawHtml objects as actual HTML.
def _raw_html_provider(content: RawHtml) -> str:
    """Taipy content provider: renders RawHtml as actual HTML."""
    return content.html


Gui.register_content_provider(RawHtml, _raw_html_provider)

# --- Page registry (ordered) ---
# List order = nav display order. Section headers appear in order of first occurrence.
# The on_navigate callback receives route keys as page_name.
PAGE_REGISTRY: list[PageEntry] = [
    # Match Analysis
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    PageEntry("Heat-Map", heat_map_config, heat_map_page),
    PageEntry("Pass-Network", pass_network_config, pass_network_page),
    PageEntry("Match-Summary", match_summary_config, match_summary_page),
    # Player Analysis
    PageEntry("Player-Impact", action_values_config, action_values_page),
    PageEntry("Player-Comparison", player_radar_config, player_radar_page),
    PageEntry("Player-Similarity", player_similarity_config, player_similarity_page),
    # Advanced
    PageEntry("Movement-Pressing", movement_config, movement_page),
    PageEntry("Pitch-Control", pitch_control_config, pitch_control_page),
    PageEntry("Pass-Timing", pass_timing_config, pass_timing_page),
    PageEntry("Defensive-Impact", defensive_impact_config, defensive_impact_page),
    # Operations
    PageEntry("AI-ML-Workflows", workflows_config, workflows_page),
]

# Generate nav and root page
_nav_md = build_nav(PAGE_REGISTRY)
root_page = build_root_page(_nav_md)

# Build Taipy pages dict
pages: dict[str, str] = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})

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
