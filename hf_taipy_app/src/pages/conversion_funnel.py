"""Conversion Rate Funnel — page configuration."""

from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    ScopeDim,
    StatCard,
    build_page,
)

page_config = PageConfig(
    title="Conversion Funnel",
    icon="filter_alt",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Possessions → Attacking Third Entries → Shots → Goals. "
        "Conversion rates at each stage reveal where a team's attack "
        "breaks down or excels. Mirror bars compare home vs away."
    ),
    citations=[
        Citation(
            "Donnelly (2024) — Systematic Approach to Performance Analysis (course materials)",
        ),
    ],
    empty_message=(
        "Select a competition and team to see the conversion funnel. "
        "Optionally select a match for single-game analysis."
    ),
    empty_condition="len(cf_possessions) == 0",
    warning_var="cf_warning_text",
    scope_dims=[
        ScopeDim("Competition", "cf_scope_comp"),
        ScopeDim("Team", "cf_scope_team"),
        ScopeDim("Match", "cf_scope_match"),
        ScopeDim("Game State", "cf_scope_game_state"),
    ],
    stats=[
        StatCard(
            label="Possessions",
            var="cf_possessions",
            detail_var="cf_possessions_detail",
            help_text=("Total team possessions — a continuous sequence of actions by one team. StatsBomb data only."),
        ),
        StatCard(
            label="A3 Entries",
            var="cf_a3_entries",
            detail_var="cf_a3_detail",
            help_text=(
                "Successful actions crossing into the attacking third "
                "(final 35m of the pitch). Higher = better territorial "
                "penetration. Includes passes, dribbles, and carries."
            ),
        ),
        StatCard(
            label="Shots",
            var="cf_shots",
            detail_var="cf_shots_detail",
            help_text=(
                "Total shots attempted from attacking positions. "
                "Conversion from A3 entries measures chance creation rate."
            ),
        ),
        StatCard(
            label="Goals",
            var="cf_goals",
            detail_var="cf_goals_detail",
            help_text=(
                "Goals scored (excludes own goals). Conversion from shots "
                "measures finishing efficiency. 0-100%, higher = better."
            ),
        ),
    ],
    content=[
        ContentRow(
            blocks=[
                ContentBlock(
                    "chart",
                    "cf_funnel_chart",
                    header="Conversion Funnel — Home vs Away",
                    chart_height="350px",
                    condition="cf_funnel_chart is not None",
                ),
            ],
        ),
    ],
)

page_md = build_page(page_config)
