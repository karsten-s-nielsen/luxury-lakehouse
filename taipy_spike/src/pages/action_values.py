"""Player Impact (VAEP) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, SubView, build_page

page_config = PageConfig(
    title="Player Impact",
    icon="trending_up",
    nav_section="Player Analysis",
    description="Valuing Actions by Estimating Probabilities (VAEP).",
    freshness_var="av_data_freshness",
    citations=[
        Citation("Decroos et al. (2019)", "https://doi.org/10.1007/s10994-021-05989-6"),
        Citation("socceraction", "https://github.com/ML-KULeuven/socceraction"),
    ],
    empty_message="",
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Rankings"',
            content=[ContentRow([ContentBlock("table", "av_rankings_data")])],
            scale_notes=["VAEP/90: higher = more impactful (typical range 0.01-1.0)"],
            empty_message="Select a competition to begin.",
            empty_condition="len(av_rankings_data) == 0 and selected_competition is not None",
        ),
        SubView(
            condition='selected_sub_view == "Breakdown"',
            content=[ContentRow([ContentBlock("image", "av_breakdown_image")])],
            empty_message="Select a team to see action breakdown.",
            empty_condition="len(av_breakdown_image) == 0 and selected_competition is not None",
            metrics=[
                Metric(
                    "Total VAEP",
                    "av_total_vaep",
                    "Valuing Actions by Estimating Probabilities — how much each on-ball action changed the probability of scoring. Positive = helped, negative = hurt.",
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
                ContentRow([ContentBlock("image", "av_timeline_image")]),
                ContentRow([ContentBlock("expandable_table", "av_timeline_data", header="Action Details")]),
            ],
            empty_message="Select a match to see action timeline.",
            empty_condition="len(av_timeline_image) == 0 and selected_match is not None",
            metrics=[
                Metric(
                    "Positive Actions",
                    "av_positive",
                    "Actions with positive VAEP — contributed to scoring probability.",
                ),
                Metric("Negative Actions", "av_negative", "Actions with negative VAEP — reduced scoring probability."),
                Metric(
                    "Net Match VAEP",
                    "av_net_vaep",
                    "Sum of all VAEP values in a match — positive = team created more than conceded.",
                ),
                Metric(
                    "Most Valuable Action",
                    "av_most_valuable",
                    "The single action that contributed most to scoring probability in this match.",
                ),
            ],
        ),
    ],
)
page_md = build_page(page_config)
