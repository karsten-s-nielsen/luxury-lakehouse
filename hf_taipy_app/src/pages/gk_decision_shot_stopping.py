"""Goalkeeper Decisions & Shot-Stopping page — config only, layout from page_template.

Two views (tile-top, full-width, no right rail): Decision-Making (fct_gk_decision, cohort positioning)
and Shot-Stopping (fct_gk_shot_stopping_pooled, goals prevented + CI band). Neither is a ranked
leaderboard — decision-making is not ranking-licensed and shot-stopping defers ranking upstream.
"""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    ScopeDim,
    StatCard,
    SubView,
    build_page,
)

_COHORT_NOTE = (
    "Reconstruction-tier evaluation. Both views position the keeper WITHIN the competition cohort — no "
    "cross-keeper ranking is shown (decision-making is not ranking-licensed; shot-stopping sample sizes "
    "are too small for a percentile leaderboard)."
)

page_config = PageConfig(
    title="Goalkeeper Decisions & Shot-Stopping",
    icon="sports",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Two goalkeeper evaluations — Decision-Making (was each build-up distribution the best available "
        "option?) and Shot-Stopping (goals prevented vs post-shot xG). " + _COHORT_NOTE
    ),
    freshness_var="gkd_data_freshness",
    citations=[
        Citation("silly-kicks — GK decision value (TF-62, reconstruction tier)"),
        Citation("PSxG — ADR-060 (4-feature logistic; Butcher et al. 2025 / Anzer & Bauer 2021)"),
    ],
    scope_dims=[
        ScopeDim("Competition", "gkd_scope_comp"),
        ScopeDim("Keeper", "gkd_keeper_label"),
    ],
    empty_message=_COHORT_NOTE,
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Decision-Making"',
            stats=[
                StatCard(
                    "Decisions",
                    "gkd_dec_n",
                    help_text="Number of reconstructed build-up distribution decisions for this keeper in "
                    "the competition (floored at 10 for a stable profile).",
                ),
                StatCard(
                    "Decision Quality",
                    "gkd_dec_quality",
                    detail_var="gkd_dec_quality_detail",
                    help_text="Selection efficiency = chosen option's EV ÷ best available EV, averaged. "
                    "0-100%, higher = better (100% = always picked the best option).",
                ),
                StatCard(
                    "Optimal-Choice Rate",
                    "gkd_dec_optimal",
                    detail_var="gkd_dec_optimal_detail",
                    help_text="Average percentile of the chosen option among those available (0-100%). "
                    "~50% = no better than random; higher = consistently picks stronger options.",
                ),
                StatCard(
                    "Value Left on Table",
                    "gkd_dec_value_left",
                    detail_var="gkd_dec_value_left_detail",
                    help_text="Best available EV minus chosen EV (threat units). LOWER = less value "
                    "forgone. This is the comparison affordance: chosen vs best.",
                ),
            ],
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkd_decision_figure",
                            condition="gkd_decision_figure is not None",
                            header="Decision profile vs cohort — optimal-choice percentile x selection efficiency",
                            plot_config_var="gkd_plot_config",
                            caption="One point per keeper; the gold star is the selected keeper. Position in "
                            "the spread, never a rank.",
                        )
                    ]
                ),
            ],
            warning_var="gkd_warning_text",
            empty_message="Pick a competition and keeper to see the decision profile.",
            empty_condition="gkd_decision_figure is None and gkd_selected_keeper is None",
        ),
        SubView(
            condition='selected_sub_view == "Shot-Stopping"',
            stats=[
                StatCard(
                    "Goals Prevented",
                    "gkd_ss_gp",
                    detail_var="gkd_ss_gp_detail",
                    help_text="Post-shot xG faced minus goals conceded. Positive = saved more than a model "
                    "keeper would; 0 = as expected. Shown with a 95% band; small samples flagged.",
                ),
                StatCard(
                    "PSxG Faced",
                    "gkd_ss_psxg",
                    help_text="Sum of post-shot expected goals faced (on-target shots). Higher = a heavier "
                    "shot-stopping workload.",
                ),
                StatCard(
                    "Shots Faced",
                    "gkd_ss_shots",
                    help_text="On-target shots faced in the competition. Small counts make goals-prevented "
                    "noisy — see the band.",
                ),
                StatCard(
                    "Goals Conceded",
                    "gkd_ss_conceded",
                    help_text="Goals conceded on the shots faced. Compared against PSxG to derive goals prevented.",
                ),
            ],
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkd_ss_figure",
                            condition="gkd_ss_figure is not None",
                            header="Shot-stopping vs cohort — goals prevented by workload",
                            plot_config_var="gkd_plot_config",
                            caption="One point per keeper; the gold star is the selected keeper. Position in "
                            "the spread, never a rank.",
                        )
                    ]
                ),
            ],
            warning_var="gkd_warning_text",
            empty_message="Pick a competition and keeper to review shot-stopping.",
            empty_condition="gkd_ss_figure is None and gkd_selected_keeper is None",
        ),
    ],
)
page_md = build_page(page_config)
