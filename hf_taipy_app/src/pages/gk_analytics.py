"""Goalkeeper Analytics — insight-first two-view page (config only; layout from page_template).

Each view = top KPI tiles (ll-stats-bar, like Conversion Funnel) -> ★ BIG STORY -> hero chart(s) ->
table / honest-secondary. Full-width content (no right rail). Competition-filtered tracking cohort.
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
    "Tracking-data cohort only — competitions from GradientSports, SkillCorner, IDSSE. "
    "StatsBomb / Wyscout keepers do not appear here. Comparisons are within the selected competition; "
    "no cross-keeper ranking is shown (cohort too small to rank)."
)

page_config = PageConfig(
    title="Goalkeeper Analytics",
    icon="sports_handball",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Goalkeeper insight in two views — Distribution Value (how he distributes: the share that adds "
        "threat, by style, positioned in his competition cohort) and Shot Review (space-command / sweeper "
        "profile + honest shot-facing). " + _COHORT_NOTE
    ),
    freshness_var="gka_data_freshness",
    citations=[
        Citation("Eyestone — xT-GK: Expected Threat for Goalkeepers (course materials)"),
        Citation("Spearman (2018) — Beyond Expected Goals", "https://www.researchgate.net/publication/327139841"),
        Citation("PSxG — ADR-060 (4-feature logistic; Butcher et al. 2025 / Anzer & Bauer 2021)"),
    ],
    scope_dims=[
        ScopeDim("Competition", "gka_scope_comp"),
        ScopeDim("Keeper", "gka_keeper_label"),
    ],
    empty_message=_COHORT_NOTE,
    empty_condition="",
    sub_views=[
        SubView(
            condition='selected_sub_view == "Distribution Value"',
            stats=[
                StatCard(
                    "Adds threat",
                    "gka_threat_val",
                    detail_var="gka_threat_detail",
                    help_text="Share of his distributions that ADD threat (xT-GK v2 above 0). The headline "
                    "signal: xT-GK v2 is signed and mostly negative across keepers, so the share that adds "
                    "threat varies meaningfully while the average barely does.",
                ),
                StatCard(
                    "Value / distribution",
                    "gka_value_val",
                    detail_var="gka_value_detail",
                    help_text="Mean xT-GK v2 composite per distribution (signed xG units, higher = better; the "
                    "keeper norm is mildly negative). The detail line breaks out its 4 terms — position, "
                    "pressure, retention, deep-zone. Shown for completeness — the % that adds threat is the "
                    "better headline.",
                ),
                StatCard(
                    "Distribution style",
                    "gka_style_val",
                    detail_var="gka_style_detail",
                    help_text="Average forward progression (m) with completion %. One axis: short-safe "
                    "(short, high completion) ↔ long-direct (long, lower completion).",
                ),
                StatCard(
                    "Our verdict",
                    "gka_off_verdict_val",
                    detail_var="gka_off_verdict_detail",
                    help_text="Threat-by-style quadrant read vs the competition cohort (proactive / secure "
                    "recycler / risk-without-reward). Cohort positioning, never a rank.",
                ),
            ],
            content=[
                ContentRow([ContentBlock("html", "gka_dist_story", height_var="gka_dist_story_height")]),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gka_dist_figure",
                            condition="gka_dist_figure is not None",
                            header="Distribution profile vs cohort — threat-by-style",
                            plot_config_var="gka_plot_config",
                        ),
                    ]
                ),
            ],
            warning_var="gka_warning_text",
            empty_message="Pick a competition and keeper to see the distribution profile.",
            empty_condition="gka_dist_figure is None and gka_selected_keeper is None",
        ),
        SubView(
            condition='selected_sub_view == "Shot Review"',
            stats=[
                StatCard(
                    "Reachable area",
                    "gka_reach_val",
                    detail_var="gka_reach_detail",
                    help_text="Mean ground he reaches behind the line, over every defended action "
                    "(m²; higher = more space commanded). Robust — ~150+ actions/keeper.",
                ),
                StatCard(
                    "Pitch-control share",
                    "gka_pc_val",
                    detail_var="gka_pc_detail",
                    help_text="Share of the pitch he controls on defended actions (%; higher = more command).",
                ),
                StatCard(
                    "Closing time · 6-yd",
                    "gka_closing_val",
                    detail_var="gka_closing_detail",
                    help_text="Time to reach the six-yard box (s; LOWER is better).",
                ),
                StatCard(
                    "Our verdict",
                    "gka_def_verdict_val",
                    detail_var="gka_def_verdict_detail",
                    help_text="Spatial-capacity read: upper-cohort command behind a deep line = capacity "
                    "unused. Not a formation recommendation.",
                ),
            ],
            content=[
                ContentRow([ContentBlock("html", "gka_def_story", height_var="gka_def_story_height")]),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gka_sweeper_figure",
                            condition="gka_sweeper_figure is not None",
                            header="Sweeper profile vs cohort (position in the spread, not a rank)",
                            plot_config_var="gka_plot_config",
                        ),
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gka_line_height_figure",
                            condition="gka_line_height_figure is not None",
                            header="Defensive line height vs cohort",
                            plot_config_var="gka_plot_config",
                        ),
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "html",
                            "gka_honest_secondary",
                            height_var="gka_honest_secondary_height",
                            header="Shot-facing — honest, thin sample",
                        )
                    ]
                ),
            ],
            warning_var="gka_warning_text",
            empty_message="Pick a competition and keeper to review the defensive profile.",
            empty_condition="gka_sweeper_figure is None and gka_selected_keeper is None",
        ),
    ],
)
page_md = build_page(page_config)
