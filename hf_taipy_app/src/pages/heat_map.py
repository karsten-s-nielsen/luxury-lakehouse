"""Heat Map page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_MATCH_ANALYSIS, Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Heat Map",
    icon="local_fire_department",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Action density visualization using bin statistics. "
        "Spatial analysis approach per Anzer & Bauer (2021) "
        '"A goal scoring probability model based on tracking data." '
        "Rendered via mplsoccer."
    ),
    citations=[
        Citation("Anzer & Bauer (2021)", "https://doi.org/10.1007/s10994-021-06011-5"),
        Citation("mplsoccer", "https://mplsoccer.readthedocs.io/"),
    ],
    content=[ContentRow([ContentBlock("image", "hm_pitch_image")])],
    empty_message="Select a competition to begin.",
    empty_condition="len(hm_pitch_image) == 0 and len(competition_lov) > 0",
    warning_var="hm_warning_text",
    scope_vars=["hm_scope_label"],
    freshness_var="hm_data_freshness",
    metrics=[
        Metric(
            "Total Actions",
            "hm_total",
            "Total number of on-ball actions (passes, shots, dribbles, etc.) in the selected scope.",
        ),
        Metric("Passes", "hm_passes", "Number of pass actions in the selected scope."),
        Metric("Shots", "hm_shots", "Number of shot actions in the selected scope."),
        Metric("Most Active Zone", "hm_most_active_zone", "The pitch zone with the highest action density."),
    ],
)
page_md = build_page(page_config)
