"""Team KPIs page — config only, layout from page_template.

Dashboard layout (StatCards + full-width wide table) over the team-match grain mart fct_team_metrics.
Presented BY GRAIN: one row per team-match, all 49 KPI columns; headline StatCards summarise the scope.
"""

from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    RequiredFilter,
    ScopeDim,
    StatCard,
    build_page,
)

_SCALE_NOTE = (
    "One row per team per match (the mart grain). Probabilities (Win/Draw/Loss) are 0-100% and sum to 1; "
    "xPoints is 0-3 per match; percentages are 0-100. Direction varies by metric — PPDA and the two Time-to "
    "columns are LOWER = better; Win Prob, xPoints, xG, Field Tilt and the output/buildup counts are HIGHER = "
    "better. See the Glossary for each family."
)

page_config = PageConfig(
    title="Team KPIs",
    icon="groups",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Team performance profile at the team-match grain — 44 team KPIs (pressing, buildup, progression, "
        "attacking output) plus model win/draw/loss probabilities and expected points. Team aggregates, "
        "not individual-player evaluations."
    ),
    citations=[
        Citation("silly-kicks — team KPIs (TF-52) & match-outcome win probability (TF-53)"),
        Citation("Field tilt / PPDA — pressing & territory analytics (course materials)"),
    ],
    scope_dims=[
        ScopeDim("Competition", "tk_scope_comp"),
        ScopeDim("Team", "tk_scope_team"),
        ScopeDim("Match", "tk_scope_match"),
    ],
    required_filters=[RequiredFilter("selected_competition", "competition")],
    freshness_var="tk_data_freshness",
    warning_var="tk_warning_text",
    stats=[
        StatCard(
            "Matches",
            "tk_matches",
            help_text="Number of team-match rows in the current scope (competition / team / match filters).",
        ),
        StatCard(
            "Win Probability",
            "tk_win_prob",
            detail_var="tk_win_prob_detail",
            help_text="Model win probability (match_outcome, per-shot xG integrated), averaged over the "
            "scope. 0-100%, higher = better.",
        ),
        StatCard(
            "xPoints",
            "tk_xpoints",
            detail_var="tk_xpoints_detail",
            help_text="Expected league points from the win/draw/loss probabilities (3·p_win + 1·p_draw). "
            "0-3 per match, higher = better.",
        ),
        StatCard(
            "Expected Goals",
            "tk_xg",
            detail_var="tk_xg_detail",
            help_text="Expected goals per match (match_outcome integration of per-shot xG). Higher = better.",
        ),
        StatCard(
            "Field Tilt",
            "tk_field_tilt",
            detail_var="tk_field_tilt_detail",
            help_text="Share of final-third possession (%). Higher = more territorial dominance.",
        ),
        StatCard(
            "PPDA",
            "tk_ppda",
            detail_var="tk_ppda_detail",
            help_text="Passes allowed Per Defensive Action — pressing intensity. LOWER = more intense press "
            "(typical range 5-15).",
        ),
    ],
    content=[
        ContentRow(
            [
                ContentBlock(
                    "table",
                    "tk_metrics_table",
                    header="Team-match KPIs (wide, by grain)",
                    table_page_size=25,
                    caption=_SCALE_NOTE,
                )
            ]
        ),
    ],
)
page_md = build_page(page_config)
