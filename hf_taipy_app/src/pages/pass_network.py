"""Pass Network page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    RequiredFilter,
    ScopeDim,
    build_page,
)

page_config = PageConfig(
    title="Pass Network",
    icon="hub",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Network analysis of passing connections per Pena & Touchette (2012) "
        '"A network theory analysis of football strategies." '
        "Wyscout matches do not include pass recipient data."
    ),
    citations=[
        Citation("Pena & Touchette (2012)", "https://doi.org/10.48550/arXiv.1206.6904"),
    ],
    content=[
        ContentRow(
            [ContentBlock("chart", "pn_chart_figure", condition="pn_total_passes != '--'", chart_height="650px")]
        )
    ],
    warning_var="pn_warning_text",
    required_filters=[
        RequiredFilter("selected_competition", "competition"),
        RequiredFilter("selected_team", "team"),
        RequiredFilter("selected_match", "match"),
    ],
    scope_dims=[
        ScopeDim("Competition", "pn_scope_comp"),
        ScopeDim("Team", "pn_scope_team"),
        ScopeDim("Match", "pn_scope_match"),
    ],
    freshness_var="pn_data_freshness",
    metrics=[
        Metric("Completed Passes", "pn_total_passes", "Number of passes that reached the intended recipient."),
        Metric(
            "Unique Connections",
            "pn_unique_connections",
            "Number of distinct passer-receiver pairs above the minimum threshold.",
        ),
        Metric(
            "Top Pair Count",
            "pn_top_pair_count",
            "Number of passes between the most frequent passer-receiver pair.",
            delta_var="pn_top_pair_names",
        ),
    ],
)
page_md = build_page(page_config)
