"""Match Summary page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_MATCH_ANALYSIS, Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Match Summary",
    icon="scoreboard",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Match scorecard with Expected Goals (xG) per Rathke (2017). "
        "Pressing intensity via PPDA (Trainor & Chassy 2021)."
    ),
    citations=[
        Citation("Rathke (2017)", "https://doi.org/10.1515/jqas-2019-0044"),
        Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688"),
    ],
    content=[
        ContentRow(
            [
                ContentBlock("image", "ms_shooting_chart"),
                ContentBlock("image", "ms_passing_chart"),
            ],
            columns=2,
            condition="len(ms_home_name) > 0",
        ),
        ContentRow(
            [
                ContentBlock("image", "ms_possession_chart"),
                ContentBlock(
                    "image",
                    "ms_ppda_chart",
                    caption="PPDA: Passes Per Defensive Action. Under 10 = aggressive pressing, over 15 = passive.",
                ),
            ],
            columns=2,
            condition="len(ms_home_name) > 0",
        ),
    ],
    empty_message="Select a competition and match to begin.",
    empty_condition="len(ms_home_name) == 0",
    warning_var="ms_warning_text",
    scope_vars=["ms_scope_label"],
    freshness_var="ms_data_freshness",
    metrics=[
        Metric("Home Score", "ms_home_score", "Match score."),
        Metric("Away Score", "ms_away_score", "Match score."),
        Metric(
            "Home xG",
            "ms_home_xg",
            "Probability of scoring from each shot's location and context. Higher = better chance. Sum over a match = team's expected output.",
            delta_var="ms_home_xg_delta",
        ),
        Metric(
            "Away xG",
            "ms_away_xg",
            "Probability of scoring from each shot's location and context. Higher = better chance. Sum over a match = team's expected output.",
            delta_var="ms_away_xg_delta",
        ),
    ],
)
page_md = build_page(page_config)
