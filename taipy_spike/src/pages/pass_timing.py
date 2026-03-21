"""Pass Timing (PAUSA) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, Metric, PageConfig, build_page

_CHARTS_CONTENT = """\
<|part|render={len(pt_scatter_image) > 0}|

<|layout|columns=1 1|gap=1rem|

<|{pt_scatter_image}|image|width=100%|>

<|{pt_heatmap_image}|image|width=100%|>

|>

|>

<|part|render={len(pt_rankings_data) > 0}|
<|part|class_name=ll-subtitle|
Player Rankings
|>
<|{pt_rankings_data}|table|>
|>

<|part|render={pt_show_dfl_caption}|
*Player names shown as DFL identifiers — IDSSE tracking data does not include player names. \
Human-readable names require a DFL roster lookup (not yet available).*
|>"""

page_config = PageConfig(
    title="Pass Timing",
    icon="timer",
    nav_section="Advanced",
    description=(
        "PAUSA: Passing Ability Under Spatiotemporal Awareness. "
        "Composite of temporal judgment (when) x spatial selection (where)."
    ),
    citations=[
        Citation("Lee, Jo, Hong, Bauer & Ko (2026)", "https://github.com/leemingo/mitssac-pausa"),
        Citation("Spearman (2018)", "https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals"),
        Citation("Kim et al. (2025) ELASTIC", "https://arxiv.org/abs/2508.09238"),
    ],
    image_var="",  # uses pre_image_content for paired charts
    empty_message="Select a match to begin. PAUSA data available for 7 IDSSE Bundesliga matches.",
    empty_condition="len(pt_avg_pausa) == 0 and len(pt_match_lov) > 0",
    pre_image_content=_CHARTS_CONTENT,
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
