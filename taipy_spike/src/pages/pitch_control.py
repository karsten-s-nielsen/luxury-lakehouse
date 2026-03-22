"""Pitch Control page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Pitch Control",
    icon="grid_on",
    nav_section="Advanced",
    freshness_var="pc_data_freshness",
    description=(
        "Physics-based pitch control model. Voronoi baseline also available. "
        "Tracking data available for ~20 matches from Metrica, IDSSE, and SkillCorner."
    ),
    citations=[
        Citation("Spearman (2017)", "https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals"),
    ],
    content=[ContentRow([ContentBlock("image", "pc_pitch_image")])],
    empty_message="Select a tracking match and half to begin.",
    empty_condition="len(pc_pitch_image) == 0 and len(tracking_match_lov) > 0",
    warning_var="pc_warning_text",
    scope_vars=["pc_status"],
    metrics=[
        Metric("Players", "pc_player_count", "Number of players visible in the current data scope."),
        Metric(
            "Home Control",
            "pc_home_control",
            "Percentage of pitch area controlled by the home team at this moment.",
        ),
        Metric(
            "Away Control",
            "pc_away_control",
            "Percentage of pitch area controlled by the away team at this moment.",
        ),
        Metric(
            "Control at Ball",
            "pc_control_at_ball",
            "Pitch control at the ball's location. 0.0 = full away control, 1.0 = full home control. Values near 0.5 indicate contested space.",
        ),
        Metric("Avg Speed (m/s)", "pc_avg_speed", "Average player speed in m/s. Typical match average: 1.5-2.5 m/s."),
        Metric(
            "Max Speed (m/s)",
            "pc_max_speed",
            "Maximum player speed in m/s. Elite sprints reach 9-10 m/s (~35 km/h).",
        ),
        Metric("Avg Dist to Ball (m)", "pc_avg_dist_to_ball", "Average distance from players to the ball in meters."),
    ],
)
page_md = build_page(page_config)
