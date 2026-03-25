"""Player Comparison page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_PLAYER_ANALYSIS, Citation, ContentBlock, ContentRow, PageConfig, build_page

page_config = PageConfig(
    title="Player Comparison",
    icon="radar",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Multi-metric player comparison using mplsoccer radar chart. "
        "Metrics from VAEP (Decroos et al. 2019) and tracking data."
    ),
    citations=[
        Citation("mplsoccer", "https://mplsoccer.readthedocs.io/"),
        Citation("Decroos et al. (2019)", "https://doi.org/10.1007/s10994-021-05989-6"),
    ],
    scope_vars=["pr_scope_label"],
    warning_var="pr_warning_text",
    content=[
        ContentRow(
            [
                ContentBlock(
                    "text",
                    "pr_select_hint",
                    condition="pr_comp_selected and pr_player_count == 0 and len(pr_no_data_warning) == 0",
                )
            ]
        ),
        ContentRow([ContentBlock("text", "pr_no_data_warning")]),
        ContentRow([ContentBlock("text", "pr_no_physical_note")]),
        ContentRow([ContentBlock("text", "pr_low_minute_warning")]),
        ContentRow([ContentBlock("text", "pr_metrics_hint", condition="len(pr_metrics_hint) > 0")]),
        ContentRow([ContentBlock("image", "pr_radar_image")]),
        ContentRow([ContentBlock("expandable_table", "pr_stats_table", header="Full Stats", table_page_size=25)]),
    ],
    empty_message="Select a competition to begin.",
    empty_condition="not pr_comp_selected",
    freshness_var="pr_data_freshness",
)
page_md = build_page(page_config)
