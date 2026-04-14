"""Tactical Positions page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_ADVANCED,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Tactical Positions",
    icon="grid_view",
    nav_section=NAV_ADVANCED,
    description=(
        "Where does each player actually play? Per-frame tactical role detection reveals "
        "positional structure, formation changes over time, and player versatility. "
        "Compares two formation detectors (EFPI template-matching vs Shape Graph geometry). "
        "Sotudeh (2026), ETH Zurich PhD thesis."
    ),
    freshness_var="tac_data_freshness",
    citations=[
        Citation(
            "Sotudeh (2026) — Identification of Team Tactical Formations and Player Positions in Association Football, ETH Zurich (DISS. ETH NO. 31732)",
            "https://doi.org/10.1038/s44260-025-00047-x",
        ),
    ],
    empty_message="",
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Position Plots"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart", "tac_position_plot_figure", condition="tac_position_plot_figure is not None"
                        )
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "Most Common Formation",
                    "tac_most_common_formation",
                    "The formation label (e.g., 2-1-2-1-1) appearing in the most frames. "
                    "Derived from per-frame vertical level counts of all outfield players.",
                ),
                Metric(
                    "Formation Stability",
                    "tac_formation_stability",
                    "Percentage of frames where the most common formation holds. "
                    "Higher = more consistent shape (100% = never changed).",
                ),
            ],
            scope_vars=["tac_scope_label"],
            warning_var="tac_warning_text",
            empty_message="Select a tracking match to see position plots.",
            empty_condition="tac_position_plot_figure is None and selected_tracking_match is None",
        ),
        SubView(
            condition='selected_sub_view == "Formation Comparison"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "tac_formation_comparison_figure",
                            condition="tac_formation_comparison_figure is not None",
                        )
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "Agreement Rate",
                    "tac_agreement_rate",
                    "Duration-weighted percentage of match time where EFPI and Shape Graph assign "
                    "the same formation label. Higher = more detector consensus. "
                    "Typical range: 40\u201370% (detectors use different methods, so perfect agreement is rare).",
                ),
                Metric(
                    "Formation Changes (EFPI)",
                    "tac_efpi_changes",
                    "Number of formation transitions detected by EFPI (template-matching). "
                    "More changes = less tactical stability.",
                ),
                Metric(
                    "Formation Changes (Shape Graph)",
                    "tac_sg_changes",
                    "Number of formation transitions detected by Shape Graph (Delaunay-based). "
                    "More changes = less tactical stability.",
                ),
                Metric(
                    "Dominant (EFPI)",
                    "tac_efpi_dominant",
                    "Most frequent formation label assigned by the EFPI detector across all windows.",
                ),
                Metric(
                    "Dominant (Shape Graph)",
                    "tac_sg_dominant",
                    "Most frequent formation label assigned by the Shape Graph detector across all windows.",
                ),
            ],
            scope_vars=["tac_scope_label"],
            warning_var="tac_warning_text",
            empty_message="Select a tracking match to see formation comparison.",
            empty_condition=("tac_formation_comparison_figure is None and selected_tracking_match is None"),
        ),
        SubView(
            condition='selected_sub_view == "Position Maps"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart", "tac_position_map_figure", condition="tac_position_map_figure is not None"
                        ),
                        ContentBlock(
                            "chart",
                            "tac_position_map_compare_figure",
                            condition="tac_position_map_compare_figure is not None",
                        ),
                    ],
                    columns=2,
                ),
            ],
            metrics=[
                Metric(
                    "Primary Position",
                    "tac_primary_position",
                    "The vertical/horizontal level combination where the player spends the most time. "
                    "Format: Level/Side (pct). E.g., B/C (45.2%) = center-back 45% of the match.",
                ),
                Metric(
                    "Versatility",
                    "tac_position_versatility",
                    "Number of 5x5 grid cells where the player spends more than 10% of match time. "
                    "Higher = more positionally flexible (1 = specialist, 4+ = versatile).",
                ),
                Metric(
                    "Vertical Range",
                    "tac_vertical_range",
                    "Span of vertical levels occupied more than 5% of the time. "
                    "E.g., B-M (3 levels) = moves between back line and midfield.",
                ),
            ],
            scope_vars=["tac_scope_label"],
            warning_var="tac_warning_text",
            empty_message="Select a tracking match and player to see position maps.",
            empty_condition=("tac_position_map_figure is None and selected_tracking_match is None"),
        ),
    ],
)
page_md = build_page(page_config)
