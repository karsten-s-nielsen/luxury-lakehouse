"""Player Impact (VAEP) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, Metric, PageConfig, SubView, build_page

_TIMELINE_POST = """\
<|part|render={len(av_timeline_data) > 0}|
<|part|class_name=ll-subtitle|
Action Details
|>
<|{av_timeline_data}|table|page_size=50|>
|>"""

page_md = build_page(
    PageConfig(
        title="Player Impact",
        icon="trending_up",
        description="Valuing Actions by Estimating Probabilities (VAEP).",
        citations=[
            Citation("Decroos et al. (2019)", "https://doi.org/10.1007/s10994-021-05989-6"),
            Citation("socceraction", "https://github.com/ML-KULeuven/socceraction"),
        ],
        image_var="",
        empty_message="",
        empty_condition="",
        sub_views=[
            SubView(
                condition='selected_sub_view == "Rankings"',
                table_var="av_rankings_data",
                pre_content=(
                    "<|part|class_name=ll-reference|\nVAEP/90: higher = more impactful (typical range 0.01-1.0)\n|>"
                ),
                empty_message="Select a competition to begin.",
                empty_condition="len(av_rankings_data) == 0 and selected_competition is not None",
            ),
            SubView(
                condition='selected_sub_view == "Breakdown"',
                image_var="av_breakdown_image",
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
                image_var="av_timeline_image",
                empty_message="Select a match to see action timeline.",
                empty_condition="len(av_timeline_image) == 0 and selected_match is not None",
                post_content=_TIMELINE_POST,
                metrics=[
                    Metric(
                        "Positive Actions",
                        "av_positive",
                        "Actions with positive VAEP — contributed to scoring probability.",
                    ),
                    Metric(
                        "Negative Actions", "av_negative", "Actions with negative VAEP — reduced scoring probability."
                    ),
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
)
