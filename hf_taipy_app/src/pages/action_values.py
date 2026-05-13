"""Player Impact (VAEP) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    ScopeDim,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Player Impact",
    icon="trending_up",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Valuing Actions by Estimating Probabilities (VAEP) \u2014 Decroos et al. (2019). Implemented via silly-kicks."
    ),
    freshness_var="av_data_freshness",
    citations=[
        Citation("Decroos et al. (2019)", "https://doi.org/10.1145/3292500.3330758"),
        Citation("silly-kicks", "https://github.com/karsten-s-nielsen/silly-kicks"),
    ],
    empty_message="",
    empty_condition="",
    scope_dims=[
        ScopeDim("Competition", "av_scope_comp"),
        ScopeDim("Team", "av_scope_team"),
        ScopeDim("Match", "av_scope_match"),
        ScopeDim("Player", "av_scope_player"),
    ],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Rankings"',
            content=[ContentRow([ContentBlock("table", "av_rankings_data")])],
            scale_notes=[
                "VAEP/90: higher = more impactful (typical range 0.01\u20131.0).",
                "Off. VAEP/90: offensive contribution per 90 min. Def. VAEP/90: defensive contribution per 90 min.",
            ],
            empty_message="Select a competition to begin.",
            empty_condition="len(av_rankings_data) == 0 and selected_competition is None",
            warning_var="av_warning_text",
        ),
        SubView(
            condition='selected_sub_view == "Breakdown"',
            content=[ContentRow([ContentBlock("image", "av_breakdown_image", alt_var="av_breakdown_image_alt")])],
            empty_message="Select a competition to see action breakdown.",
            empty_condition="len(av_breakdown_image) == 0 and selected_competition is None",
            warning_var="av_warning_text",
            metrics=[
                Metric(
                    "Total VAEP",
                    "av_total_vaep",
                    "Valuing Actions by Estimating Probabilities — how much each on-ball action changed "
                    "the probability of scoring. Positive = helped, negative = hurt. "
                    "Measures Decision Value (net of Survival and Progression).",
                ),
                Metric(
                    "Total Actions",
                    "av_total_actions",
                    "Number of on-ball actions (passes, shots, dribbles, etc.) in the selected scope.",
                ),
                Metric(
                    "Top Action Type",
                    "av_top_action",
                    "The SPADL action type with the highest total VAEP contribution.",
                ),
            ],
        ),
        SubView(
            condition='selected_sub_view == "Timeline"',
            content=[
                ContentRow([ContentBlock("image", "av_timeline_image", alt_var="av_timeline_image_alt")]),
                ContentRow([ContentBlock("expandable_table", "av_timeline_data", header="Action Details")]),
            ],
            empty_message="Select a match to see action timeline.",
            empty_condition="len(av_timeline_image) == 0 and selected_match is None",
            warning_var="av_warning_text",
            metrics=[
                Metric(
                    "Positive Actions",
                    "av_positive",
                    "Actions with positive VAEP — contributed to scoring probability. "
                    "Reflects net Decision Value: Survival gain + Progression gain.",
                ),
                Metric(
                    "Negative Actions",
                    "av_negative",
                    "Actions with negative VAEP — reduced scoring probability. "
                    "Reflects net Decision Value: Survival cost exceeded Progression gain.",
                ),
                Metric(
                    "Net Match VAEP",
                    "av_net_vaep",
                    "Sum of all VAEP values in a match — positive = team created more than conceded. "
                    "Aggregate Decision Value across all on-ball actions.",
                ),
                Metric(
                    "Most Valuable Action",
                    "av_most_valuable",
                    "The single action that contributed most to scoring probability in this match. "
                    "Highest absolute Decision Value.",
                ),
            ],
        ),
    ],
)
page_md = build_page(page_config)
