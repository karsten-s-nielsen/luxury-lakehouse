"""Match Summary page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Match Summary",
    icon="scoreboard",
    nav_section="Match Analysis",
    description="Match scorecard with Expected Goals (xG) and pressing intensity via PPDA.",
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
    scope_vars=["ms_scope_label"],
    freshness_var="ms_data_freshness",
    metrics=[
        Metric("Home Score", "ms_home_score"),
        Metric("Away Score", "ms_away_score"),
        Metric(
            "Home xG",
            "ms_home_xg",
            "Expected goals for the home team based on shot quality.",
            delta_var="ms_home_xg_delta",
        ),
        Metric(
            "Away xG",
            "ms_away_xg",
            "Expected goals for the away team based on shot quality.",
            delta_var="ms_away_xg_delta",
        ),
    ],
)
page_md = build_page(page_config)
