"""Heat Map page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    ScopeDim,
    build_page,
)

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
        Citation("Anzer & Bauer (2021)", "https://doi.org/10.3389/fspor.2021.624475"),
        Citation("mplsoccer", "https://mplsoccer.readthedocs.io/"),
    ],
    content=[
        ContentRow(
            [
                ContentBlock("image", "hm_pass_bubbles", alt_var="hm_pass_bubbles_alt"),
                ContentBlock("image", "hm_shot_bubbles", alt_var="hm_shot_bubbles_alt"),
            ],
            columns=2,
            condition="len(hm_pass_bubbles) > 0",
        ),
        ContentRow(
            [
                ContentBlock("image", "hm_pass_focus", alt_var="hm_pass_focus_alt"),
                ContentBlock("image", "hm_shot_focus", alt_var="hm_shot_focus_alt"),
            ],
            columns=2,
            condition="len(hm_pass_focus) > 0",
        ),
    ],
    empty_message="Select a competition to begin.",
    empty_condition="len(hm_pass_bubbles) == 0 and len(competition_lov) > 0",
    warning_var="hm_warning_text",
    scope_dims=[
        ScopeDim("Competition", "hm_scope_comp"),
        ScopeDim("Team", "hm_scope_team"),
        ScopeDim("Player", "hm_scope_player"),
    ],
    scope_vars=["hm_scope_coverage"],
    freshness_var="hm_data_freshness",
    metrics=[
        Metric(
            "Total Actions",
            "hm_total",
            "Total number of on-ball actions (passes, shots, dribbles, etc.) in the selected scope.",
        ),
        Metric("Passes", "hm_passes", "Number of pass actions in the selected scope."),
        Metric("Shots", "hm_shots", "Number of shot actions in the selected scope."),
    ],
)
page_md = build_page(page_config)
