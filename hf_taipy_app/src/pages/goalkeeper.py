"""Goalkeeper Analytics page — config only, layout from page_template."""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Goalkeeper Analytics",
    icon="sports_handball",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Four-pillar GK evaluation: shot stopping (PSxG goals prevented), "
        "distribution (pass distance, xT progression), claiming, and sweeping. "
        "Butcher et al. (2025) + Lamberts (2025)."
    ),
    freshness_var="gk_data_freshness",
    citations=[
        Citation("Butcher et al. (2025) — GK Shot Stopping", "https://doi.org/10.1515/jqas-2024-0091"),
        Citation("Lamberts (2025) — GK Distribution", "https://doi.org/10.1007/978-3-031-31772-9_19"),
    ],
    empty_message="",
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Rankings"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "table",
                            "gk_rankings_df",
                            on_action="gk_on_rankings_action",
                            header="GK Leaderboard (click a row to find similar players)",
                        ),
                    ]
                ),
            ],
            scope_vars=["gk_scope_label"],
            warning_var="gk_warning_text",
            empty_message="Select a competition to see GK rankings.",
            empty_condition="len(gk_rankings_df) == 0 and selected_competition is None",
        ),
        SubView(
            condition='selected_sub_view == "Shot Stopping"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gk_goalmouth_figure",
                            header="Goalmouth Shot Map",
                            condition="gk_goalmouth_figure is not None",
                        )
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gk_goals_prevented_figure",
                            header="Goals Prevented",
                            condition="gk_goals_prevented_figure is not None",
                        )
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "PSxG Faced",
                    "gk_psxg_faced",
                    "Post-Shot Expected Goals faced — probability of each shot being scored given its end "
                    "location in the goal frame (0-1 per shot, summed). Higher = harder shots faced.",
                ),
                Metric(
                    "Goals Prevented",
                    "gk_goals_prevented_val",
                    "PSxG faced minus goals conceded. Positive = GK saved more than expected "
                    "(outperforming the model). Negative = conceded more than expected.",
                ),
                Metric(
                    "Save %",
                    "gk_save_pct_val",
                    "Percentage of on-target shots saved. Typical elite GK range: 65-75%.",
                ),
            ],
            scope_vars=["gk_scope_label"],
            warning_var="gk_warning_text",
            empty_message="Select a competition to see shot stopping data.",
            empty_condition="gk_goalmouth_figure is None and selected_competition is None",
        ),
        SubView(
            condition='selected_sub_view == "Distribution"',
            content=[
                ContentRow(
                    [
                        ContentBlock("image", "gk_distribution_image", header="Pass Distribution"),
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "Short %",
                    "gk_short_pct",
                    "Percentage of GK distribution passes under 32m. Typically goal kicks and short build-up.",
                ),
                Metric(
                    "Medium %",
                    "gk_medium_pct",
                    "Percentage of GK distribution passes between 32-60m. Intermediate build-up.",
                ),
                Metric(
                    "Long %",
                    "gk_long_pct",
                    "Percentage of GK distribution passes over 60m. Long balls and launches.",
                ),
                Metric(
                    "Launch Rate",
                    "gk_launch_rate_val",
                    "Percentage of GK passes over 60m — higher = more direct style. Typical range: 15-40%.",
                ),
                Metric(
                    "xT / Pass",
                    "gk_xt_per_pass_val",
                    "Average Expected Threat gained per GK distribution pass. "
                    "Higher = more progressive distribution (typical: 0.001-0.005).",
                ),
                Metric(
                    "Total xT",
                    "gk_xt_total_val",
                    "Sum of Expected Threat gained from all GK distribution passes. "
                    "Higher = greater total contribution to attacking progression.",
                ),
            ],
            scope_vars=["gk_scope_label"],
            warning_var="gk_warning_text",
            empty_message="Select a competition to see distribution data.",
            empty_condition="len(gk_distribution_image) == 0 and selected_competition is None",
        ),
    ],
)
page_md = build_page(page_config)
