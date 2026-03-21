"""Defensive Impact (DEFCON) page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, SubView, build_page

page_config = PageConfig(
    title="Defensive Impact",
    icon="shield",
    nav_section="Advanced",
    freshness_var="dv_data_freshness",
    description=(
        "Tier 3 (tabular heuristic, no GNN) approximation of the DEFCON framework. "
        "Credits: Intercept, Concede, Disturb, Deter. "
        "Tiers: 1 = full GNN, 2 = simplified GNN, 3 = tabular heuristic (this implementation)."
    ),
    citations=[
        Citation("Kim et al. (2025)", "https://github.com/hyunsungkim-ds/defcon"),
    ],
    empty_message="No competitions with defensive pressure data available.",
    empty_condition="len(dv_comp_lov) == 0",
    sub_views=[
        SubView(
            condition='dv_current_view == "Rankings"',
            content=[ContentRow([ContentBlock("table", "dv_rankings_data")])],
            scale_notes=[
                "Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).",
                "Total Pressure: higher = more defensive attention attracted (typical range 1-50 per competition)",
            ],
            empty_message="No defensive pressure data for the selected filters.",
            empty_condition="len(dv_rankings_data) == 0 and len(dv_comp_lov) > 0",
        ),
        SubView(
            condition='dv_current_view == "Breakdown"',
            content=[ContentRow([ContentBlock("image", "dv_breakdown_image")])],
            scale_notes=[
                "Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).",
            ],
            empty_message="No breakdown chart data for the selected player.",
            empty_condition="len(dv_breakdown_image) == 0 and dv_selected_breakdown_player is not None",
            metrics=[
                Metric(
                    "Intercept",
                    "dv_intercept",
                    "DEFCON credit for winning the ball. Higher = more successful interceptions. Typical match total per player: 0.1-1.5 credits.",
                ),
                Metric(
                    "Concede",
                    "dv_concede",
                    "DEFCON credit charged when a shot or goal occurs despite pressure. Lower is better. Typical match total per player: 0.0-0.5 credits.",
                ),
                Metric(
                    "Disturb",
                    "dv_disturb",
                    "DEFCON credit for disrupting possession without winning the ball. Higher = more effective disruption. Typical match total: 0.1-1.0 credits.",
                ),
                Metric(
                    "Deter",
                    "dv_deter",
                    "DEFCON credit for preventing attacker progression through positioning. Higher = more effective deterrence. Typical match total: 0.1-2.0 credits.",
                ),
            ],
        ),
        SubView(
            condition='dv_current_view == "Timeline"',
            content=[ContentRow([ContentBlock("table", "dv_timeline_data")])],
            scale_notes=[
                "Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).",
                "DEFCON Value: positive = effective defensive contribution. Confidence: 0-1, higher = more certain.",
            ],
            empty_message="No defensive actions for this match.",
            empty_condition="len(dv_timeline_data) == 0 and dv_selected_timeline_player is not None and dv_selected_timeline_match is not None",
        ),
    ],
)
page_md = build_page(page_config)
