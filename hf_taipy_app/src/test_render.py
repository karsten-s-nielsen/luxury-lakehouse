"""Minimal Taipy render test — no database, just template verification."""
# ruff: noqa: E402 — mock state vars must be defined before page imports (Taipy binding)

# Mock all state variables that the template and pages reference
current_page = "Shot-Map"
selected_competition = None
competition_lov = []
selected_team = None
team_lov = []
selected_match = None
match_lov = []
selected_player = None
player_lov = []
selected_players_multi = []
player_lov_multi = []
selected_xg_model = None
xg_model_lov = []
min_passes = 3
min_minutes = 0
pm_show_progressive = False
pm_show_line_breaking = False
selected_provider = "All"
provider_lov = [("All", "All")]
selected_tracking_match = None
tracking_match_lov = []
selected_sub_view = "Physical Performance"
sub_view_lov = [
    ("Physical Performance", "Physical Performance"),
    ("PPDA / Pressing Intensity", "PPDA / Pressing Intensity"),
    ("Off-Ball xT", "Off-Ball xT"),
]
show_glossary = False
show_getting_started = False
is_loading = False
loading_text = "Loading..."
pr_selected_metrics = []
pr_metric_lov = []
pr_comp_selected = False
pr_player_count = 0
pr_no_data_warning = ""
pr_radar_image = ""
pr_no_physical_note = ""
pr_low_minute_warning = ""
pr_spoke_caption = ""
pr_metrics_hint = ""
pr_stats_table = []
pr_data_freshness = ""

# Shot Map state
sm_pitch_image = ""
sm_scope_label = ""
sm_data_scope_note = ""
sm_nan_fallback_note = ""
sm_empty_message = "Select a competition to begin."
sm_data_freshness = ""
sm_total_shots = "--"
sm_goals = "--"
sm_total_xg = "--"
sm_xg_delta = ""
sm_conversion = "--"
sm_xg_per_shot = "--"
sm_xg_per_delta = ""
sm_brier = "--"
sm_brier_delta = ""

# Player Similarity state
ps_search_mode = None
ps_search_mode_lov = [("Playing style", "Playing style"), ("Statistical profile", "Statistical profile")]
ps_selected_player = None
ps_player_lov = []
ps_result_count = "10"
ps_result_count_lov = [("5", "5"), ("10", "10"), ("25", "25")]
ps_filter_by_competition = False
ps_selected_competition = None
ps_competition_lov = []
ps_results_data = []
ps_selected_compare = None
ps_compare_lov = []
ps_status_message = ""
ps_radar_image = ""
ps_spoke_caption = ""

# Movement state
ma_physical_metric = "Total Distance (km)"
ma_physical_metric_lov = ["Total Distance (km)", "HSR Distance (m)", "Sprint Distance (m)"]
ma_physical_image = ""
ma_phys_players = "--"
ma_phys_avg_dist = "--"
ma_phys_max_speed_kmh = "--"
ma_phys_max_speed_ms = "--"
ma_ppda_image = ""
ma_ppda_avg_home = "--"
ma_ppda_avg_away = "--"
ma_ppda_matches = "--"
ma_oxt_image = ""
ma_oxt_players = "--"
ma_oxt_avg = "--"
ma_oxt_max = "--"

# Pass Timing state
pt_selected_match = None
pt_match_lov = []
pt_selected_team = None
pt_team_lov = []
pt_selected_player = None
pt_player_lov = []
pt_avg_pausa = ""
pt_avg_temporal = ""
pt_avg_spatial = ""
pt_pass_count = ""
pt_scatter_figure = None
pt_heatmap_figure = None
pt_rankings_data = []
pt_show_dfl_caption = False

# Defensive Impact state
dv_selected_comp = None
dv_comp_lov = []
dv_selected_team = None
dv_team_lov = []
dv_current_view = "Rankings"
dv_view_lov = [("Rankings", "Rankings"), ("Breakdown", "Breakdown"), ("Timeline", "Timeline")]
dv_rankings_data = []
dv_breakdown_player_lov = []
dv_selected_breakdown_player = None
dv_intercept = "--"
dv_concede = "--"
dv_disturb = "--"
dv_deter = "--"
dv_breakdown_figure = None
dv_breakdown_caption = ""
dv_timeline_player_lov = []
dv_selected_timeline_player = None
dv_timeline_match_lov = []
dv_selected_timeline_match = None
dv_timeline_data = []

# Pitch Control state
pc_half = None
pc_half_lov = [("1st Half", "1"), ("2nd Half", "2")]
pc_model = None
pc_model_lov = [("Physics-based", "physics"), ("Voronoi", "voronoi")]
pc_show_velocity = False
pc_elapsed_seconds = 0
pc_min_seconds = 0
pc_max_seconds = 2700
pc_time_display = "0:00"
pc_pitch_image = ""
pc_status = ""
pc_player_count = "--"
pc_home_control = "--"
pc_away_control = "--"
pc_control_at_ball = "--"
pc_avg_speed = "--"
pc_max_speed = "--"
pc_avg_dist_to_ball = "--"

# Action Values state
av_rankings_data = []
av_rankings_empty_msg = ""
av_total_vaep = "--"
av_total_actions = "--"
av_top_action = "--"
av_breakdown_image = ""
av_positive = "--"
av_negative = "--"
av_net_vaep = "--"
av_most_valuable = "--"
av_timeline_image = ""
av_timeline_data = []

# Pass Map state
pm_pitch_image = ""
pm_scope_label = ""
pm_data_freshness = ""
pm_total = "--"
pm_completed = "--"
pm_completion_pct = "--"
pm_progressive = "--"
pm_line_breaking = "--"

# Heat Map state
hm_pass_bubbles = ""
hm_shot_bubbles = ""
hm_pass_focus = ""
hm_shot_focus = ""
hm_scope_label = ""
hm_data_freshness = ""
hm_total = "--"
hm_passes = "--"
hm_shots = "--"

# Pass Network state
pn_chart_figure = None
pn_scope_label = ""
pn_data_freshness = ""
pn_total_passes = "--"
pn_unique_connections = "--"
pn_top_pair_count = "--"
pn_top_pair_names = ""

# Match Summary state
ms_home_name = ""
ms_scope_label = ""
ms_data_freshness = ""
ms_shooting_chart = ""
ms_passing_chart = ""
ms_possession_chart = ""
ms_ppda_chart = ""
ms_home_score = "--"
ms_away_score = "--"
ms_home_xg = "--"
ms_home_xg_delta = ""
ms_away_xg = "--"
ms_away_xg_delta = ""


# Dummy callbacks
def on_init(state):
    pass


def on_navigate(state, page_name):
    state.current_page = page_name


def on_competition_change(state, var, val):
    pass


def on_team_change(state, var, val):
    pass


def on_match_change(state, var, val):
    pass


def on_player_change(state, var, val):
    pass


def on_xg_model_change(state, var, val):
    pass


def on_min_passes_change(state, var, val):
    pass


def on_min_minutes_change(state, var, val):
    pass


def on_pm_toggle_change(state, var, val):
    pass


def on_provider_change(state, var, val):
    pass


def on_tracking_match_change(state, var, val):
    pass


def on_sub_view_change(state, var, val):
    pass


def on_ma_physical_metric_change(state, var_name, var_value):
    pass


def on_pr_metric_change(state, var, val):
    pass


def on_ps_search_mode_change(state, var, val):
    pass


def on_ps_selected_player_change(state, var, val):
    pass


def on_ps_result_count_change(state, var, val):
    pass


def on_ps_filter_by_competition_change(state, var, val):
    pass


def on_ps_selected_competition_change(state, var, val):
    pass


def on_ps_selected_compare_change(state, var, val):
    pass


def toggle_glossary(state):
    pass


def toggle_getting_started(state):
    pass


def pt_on_match_change(state, var, val):
    pass


def pt_on_team_change(state, var, val):
    pass


def pt_on_player_change(state, var, val):
    pass


def dv_on_comp_change(state, var, val):
    pass


def dv_on_team_change(state, var, val):
    pass


def dv_on_view_change(state, var, val):
    pass


def dv_on_breakdown_player_change(state, var, val):
    pass


def dv_on_timeline_player_change(state, var, val):
    pass


def dv_on_timeline_match_change(state, var, val):
    pass


def pc_on_half_change(state, var, val):
    pass


def pc_on_model_change(state, var, val):
    pass


def pc_on_velocity_change(state, var, val):
    pass


def pc_on_seconds_change(state, var, val):
    pass


# Import ALL pages
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
from pages.shot_map import page_config as shot_map_config
from pages.shot_map import page_md as shot_map_page
from taipy.gui import Gui
from template import build_root_page

PAGE_REGISTRY: list[PageEntry] = [
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    PageEntry("Heat-Map", heat_map_config, heat_map_page),
    PageEntry("Pass-Network", pass_network_config, pass_network_page),
    PageEntry("Match-Summary", match_summary_config, match_summary_page),
    PageEntry("Player-Impact", action_values_config, action_values_page),
    PageEntry("Player-Comparison", player_radar_config, player_radar_page),
    PageEntry("Player-Similarity", player_similarity_config, player_similarity_page),
    PageEntry("Movement-Pressing", movement_config, movement_page),
    PageEntry("Pitch-Control", pitch_control_config, pitch_control_page),
    PageEntry("Pass-Timing", pass_timing_config, pass_timing_page),
    PageEntry("Defensive-Impact", defensive_impact_config, defensive_impact_page),
]

root_page = build_root_page(build_nav(PAGE_REGISTRY))
pages: dict[str, str] = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})

gui = Gui(pages=pages, css_file="style_v2.css")
# Taipy 4.1 stubs miss on_init/on_navigate kwargs; both are valid at runtime.
gui.run(
    host="0.0.0.0",
    port=7861,
    title="(Right! Luxury!) Lakehouse",
    dark_mode=True,
    use_reloader=False,
    on_init=on_init,  # pyright: ignore[reportCallIssue]
    on_navigate=on_navigate,  # pyright: ignore[reportCallIssue]
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
