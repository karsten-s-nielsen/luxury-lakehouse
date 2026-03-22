"""Pass Network page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Pass Network",
    icon="hub",
    nav_section="Match Analysis",
    description="Network analysis of passing connections. Wyscout matches do not include pass recipient data.",
    citations=[
        Citation("Pena & Touchette (2012)", "https://doi.org/10.48550/arXiv.1206.6904"),
    ],
    content=[ContentRow([ContentBlock("image", "pn_pitch_image")])],
    empty_message="Select a competition, team, and match to begin.",
    empty_condition="selected_competition is None",
    warning_var="pn_warning_text",
    scope_vars=["pn_scope_label"],
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
