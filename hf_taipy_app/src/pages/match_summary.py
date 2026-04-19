"""Match Summary page — config only, layout from page_template."""

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
    title="Match Summary",
    icon="scoreboard",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Match scorecard with Expected Goals (xG) per Robberechts & Davis (2020). "
        "Pressing intensity via PPDA (Trainor & Chassy 2021)."
    ),
    citations=[
        Citation(
            "Robberechts & Davis (2020) — How Data Availability Affects the Ability to Learn Good xG Models",
            "https://dtai.cs.kuleuven.be/sports/blog/how-data-availability-affects-the-ability-to-learn-good-xg-models",
        ),
        Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688"),
    ],
    content=[
        ContentRow(
            [
                ContentBlock("image", "ms_shooting_chart", alt_var="ms_shooting_chart_alt"),
                ContentBlock("image", "ms_passing_chart", alt_var="ms_passing_chart_alt"),
            ],
            columns=2,
            condition="len(ms_home_name) > 0",
        ),
        ContentRow(
            [
                ContentBlock("image", "ms_possession_chart", alt_var="ms_possession_chart_alt"),
                ContentBlock(
                    "image",
                    "ms_ppda_chart",
                    alt_var="ms_ppda_chart_alt",
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
    scope_dims=[
        ScopeDim("Competition", "ms_scope_comp"),
        ScopeDim("Team", "ms_scope_team"),
        ScopeDim("Match", "ms_scope_match"),
    ],
    scope_vars=["ms_league_averages"],
    freshness_var="ms_data_freshness",
    metrics=[
        Metric("Home Score", "ms_home_score", "Match score."),
        Metric("Away Score", "ms_away_score", "Match score."),
        Metric(
            "Home xG",
            "ms_home_xg",
            "Expected goals from shot locations and context. Delta shows goals minus xG: positive = overperformed, negative = underperformed.",
            delta_var="ms_home_xg_delta",
        ),
        Metric(
            "Away xG",
            "ms_away_xg",
            "Expected goals from shot locations and context. Delta shows goals minus xG: positive = overperformed, negative = underperformed.",
            delta_var="ms_away_xg_delta",
        ),
    ],
)
page_md = build_page(page_config)
