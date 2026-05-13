"""Movement & Pressing page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_ADVANCED, Citation, ContentBlock, ContentRow, Metric, PageConfig, SubView, build_page

page_config = PageConfig(
    title="Movement & Pressing",
    icon="directions_run",
    nav_section=NAV_ADVANCED,
    description=(
        'Off-Ball xT combines pitch control (Spearman 2017, "Physics-Based Modeling of '
        'Pass Probabilities in Soccer") with Expected Threat zones (Karun Singh 2018). '
        "Physical metrics from tracking data."
    ),
    freshness_var="ma_data_freshness",
    citations=[
        Citation(
            "Spearman (2017) — Physics-Based Modeling of Pass Probabilities in Soccer "
            "(MIT Sloan Sports Analytics Conference)"
        ),
        Citation("Karun Singh (2018)", "https://karun.in/blog/expected-threat.html"),
    ],
    empty_message="",
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Physical Performance"',
            content=[
                ContentRow([ContentBlock("image", "ma_physical_image")]),
                ContentRow([ContentBlock("expandable_table", "ma_physical_table", header="Full Stats")]),
            ],
            warning_var="ma_warning_text",
            empty_message="Select a match to begin.",
            empty_condition="len(ma_physical_image) == 0 and len(tracking_match_lov) > 0",
            fallback_empty_message="No physical stats for the selected filters. This page requires tracking data (available for ~20 matches).",
            fallback_empty_condition="len(ma_physical_image) == 0 and len(tracking_match_lov) == 0",
            metrics=[
                Metric("Players", "ma_phys_players", "Number of players visible in the current data scope."),
                Metric(
                    "Avg Distance (km)",
                    "ma_phys_avg_dist",
                    "Average total distance covered per player in kilometers.",
                ),
                Metric(
                    "Max Speed (km/h)",
                    "ma_phys_max_speed_kmh",
                    "Maximum player speed in km/h. Elite sprints reach ~35 km/h.",
                ),
                Metric(
                    "Max Speed (m/s)",
                    "ma_phys_max_speed_ms",
                    "Maximum player speed in m/s. Elite sprints reach 9-10 m/s (~35 km/h).",
                ),
            ],
        ),
        SubView(
            condition='selected_sub_view == "PPDA / Pressing Intensity"',
            content=[
                ContentRow([ContentBlock("image", "ma_ppda_image")]),
                ContentRow([ContentBlock("expandable_table", "ma_ppda_table", header="PPDA Data")]),
            ],
            scope_vars=["ma_ppda_scope_label"],
            warning_var="ma_warning_text",
            empty_message="Select a competition to begin.",
            empty_condition="len(ma_ppda_image) == 0 and len(competition_lov) > 0",
            metrics=[
                Metric(
                    "Avg Home PPDA",
                    "ma_ppda_avg_home",
                    "Passes Per Defensive Action. Lower = more aggressive pressing.",
                ),
                Metric(
                    "Avg Away PPDA",
                    "ma_ppda_avg_away",
                    "Passes Per Defensive Action. Lower = more aggressive pressing.",
                ),
                Metric("Matches", "ma_ppda_matches", "Number of matches included in the current analysis."),
            ],
        ),
        SubView(
            condition='selected_sub_view == "Off-Ball xT"',
            content=[
                ContentRow([ContentBlock("image", "ma_oxt_image")]),
                ContentRow([ContentBlock("expandable_table", "ma_oxt_table", header="Off-Ball xT Data")]),
            ],
            warning_var="ma_warning_text",
            empty_message="Select a match to begin.",
            empty_condition="len(ma_oxt_image) == 0 and len(tracking_match_lov) > 0",
            fallback_empty_message="No tracking data for the selected filters. This page requires tracking data (available for ~20 matches).",
            fallback_empty_condition="len(ma_oxt_image) == 0 and len(tracking_match_lov) == 0",
            metrics=[
                Metric("Players", "ma_oxt_players", "Number of players visible in the current data scope."),
                Metric(
                    "Avg Off-Ball xT",
                    "ma_oxt_avg",
                    "Average expected threat from off-ball movement. Measures Progression — "
                    "how much players' movement improved territorial position. Typical range: 0.001-0.01.",
                ),
                Metric(
                    "Max Off-Ball xT",
                    "ma_oxt_max",
                    "Highest cumulative off-ball xT by any single player. Measures Progression. "
                    "Typical range: 0.001-0.01.",
                ),
            ],
        ),
    ],
)
page_md = build_page(page_config)
