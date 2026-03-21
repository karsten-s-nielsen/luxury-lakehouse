"""Pass Timing (PAUSA) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Pass Timing",
    icon="timer",
    nav_section="Advanced",
    freshness_var="pt_data_freshness",
    description=(
        "PAUSA: Passing Ability Under Spatiotemporal Awareness. "
        "Composite of temporal judgment (when) x spatial selection (where)."
    ),
    citations=[
        Citation("Lee, Jo, Hong, Bauer & Ko (2026)", "https://github.com/leemingo/mitssac-pausa"),
        Citation("Spearman (2018)", "https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals"),
        Citation("Kim et al. (2025) ELASTIC", "https://arxiv.org/abs/2508.09238"),
    ],
    content=[
        ContentRow([ContentBlock("image", "pt_scatter_image")]),
        ContentRow([ContentBlock("image", "pt_heatmap_image")]),
        ContentRow(
            [
                ContentBlock(
                    "table",
                    "pt_rankings_data",
                    header="Player Rankings",
                    caption_var="pt_dfl_caption",
                    caption_condition="pt_show_dfl_caption",
                )
            ]
        ),
    ],
    empty_message="Select a match to begin. PAUSA data available for 7 IDSSE Bundesliga matches.",
    empty_condition="len(pt_avg_pausa) == 0 and len(pt_match_lov) > 0",
    metrics=[
        Metric(
            "Avg PAUSA",
            "pt_avg_pausa",
            "Passing Ability Under Spatiotemporal Awareness. Composite of temporal judgment and spatial selection. Higher = better pass timing and target choice. (Lee et al., MIT Sloan 2026)",
        ),
        Metric(
            "Avg Temporal Judgment",
            "pt_avg_temporal",
            "Was the pass released at the optimal moment? Ratio of actual OBSO at release to peak OBSO in the window. 1.0 = perfect timing.",
        ),
        Metric(
            "Avg Spatial Selection",
            "pt_avg_spatial",
            "Was the target location the best available? Ratio of actual OBSO at target to maximum OBSO across all receivers. 1.0 = optimal target.",
        ),
        Metric("Pass Count", "pt_pass_count", "Number of passes evaluated for PAUSA scoring."),
    ],
)
page_md = build_page(page_config)
