"""Movement & Pressing page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, Metric, PageConfig, SubView, build_page

page_md = build_page(
    PageConfig(
        title="Movement & Pressing",
        icon="directions_run",
        description="Off-Ball xT combines pitch control with Expected Threat zones. Physical metrics from tracking data.",
        citations=[
            Citation("Spearman (2017)", "https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals"),
            Citation("Karun Singh (2018)", "https://karun.in/blog/expected-threat.html"),
        ],
        image_var="",  # uses sub_views
        empty_message="",
        empty_condition="",
        sub_views=[
            SubView(
                condition='ma_active_view == "Physical Performance"',
                image_var="ma_physical_image",
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
                condition='ma_active_view == "PPDA / Pressing Intensity"',
                image_var="ma_ppda_image",
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
                condition='ma_active_view == "Off-Ball xT"',
                image_var="ma_oxt_image",
                empty_message="Select a match to begin.",
                empty_condition="len(ma_oxt_image) == 0 and len(tracking_match_lov) > 0",
                fallback_empty_message="No tracking data for the selected filters. This page requires tracking data (available for ~20 matches).",
                fallback_empty_condition="len(ma_oxt_image) == 0 and len(tracking_match_lov) == 0",
                metrics=[
                    Metric("Players", "ma_oxt_players", "Number of players visible in the current data scope."),
                    Metric(
                        "Avg Off-Ball xT",
                        "ma_oxt_avg",
                        "Cumulative expected threat from off-ball movement. Typical range: 0.001-0.01 per match.",
                    ),
                    Metric(
                        "Max Off-Ball xT",
                        "ma_oxt_max",
                        "Cumulative expected threat from off-ball movement. Typical range: 0.001-0.01 per match.",
                    ),
                ],
            ),
        ],
    )
)
