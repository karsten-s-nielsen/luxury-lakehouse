"""Goalkeeper Tracking Analytics page — config only, layout from page_template (ADR-051)."""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    ScopeDim,
    SubView,
    build_page,
)

page_config = PageConfig(
    title="Goalkeeper Tracking",
    icon="sports_handball",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Tracking-data goalkeeper analytics: distribution value under six switchable game-model "
        "presets (xT-GK), model-optimal positioning (Ghost GK), box command, and pre-shot "
        "geometry. Tracking providers only (GradientSports, IDSSE, SkillCorner)."
    ),
    freshness_var="gkt_data_freshness",
    citations=[
        Citation("Eyestone — xT-GK: Expected Threat for Goalkeepers (course materials)"),
        Citation("Poole (2022) — USWNT Goalkeeper Profile, IGCC (course materials)"),
        Citation("Spearman (2018) — Beyond Expected Goals", "https://www.researchgate.net/publication/327139841"),
    ],
    empty_message="",
    empty_condition="",
    scope_dims=[
        ScopeDim("Goalkeeper", "gkt_scope_player"),
        ScopeDim("Preset", "gkt_scope_preset"),
    ],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Distribution Value"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkt_bump_figure",
                            condition="gkt_bump_figure is not None",
                            header="Rank under every game-model preset",
                        )
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkt_map_selected_figure",
                            condition="gkt_map_selected_figure is not None",
                            header="Distributions valued under the selected preset",
                        ),
                        ContentBlock(
                            "chart",
                            "gkt_map_compare_figure",
                            condition="gkt_map_compare_figure is not None",
                            header="The same passes under the comparison preset",
                        ),
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "xT-GK / pass",
                    "gkt_xtgk_mean_val",
                    "Mean xT-GK per distribution under the selected preset. Scale roughly "
                    "-0.05 to +0.10; higher = more attacking value created per pass. "
                    "Shown against the sample mean.",
                ),
                Metric(
                    "Completion",
                    "gkt_completion_val",
                    "Mean model probability that his attempted distributions succeed "
                    "(0-1; lower = riskier pass selection, not worse passing).",
                ),
                Metric(
                    "n",
                    "gkt_n_dist_val",
                    "Number of distributions behind these values. Small n = treat as noisy.",
                ),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see distribution value.",
            empty_condition="gkt_bump_figure is None and gkt_selected_player is None",
        ),
        SubView(
            condition='selected_sub_view == "Defensive Positioning"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkt_scene_figure",
                            condition="gkt_scene_figure is not None",
                            header="Actual vs model-optimal position (Ghost GK)",
                        )
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkt_context_figure",
                            condition="gkt_context_figure is not None",
                            header="When does he leave the model line?",
                        ),
                        ContentBlock(
                            "chart",
                            "gkt_closing_figure",
                            condition="gkt_closing_figure is not None",
                            header="Command of the box",
                        ),
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "Deviation",
                    "gkt_deviation_val",
                    "Mean distance from the ghost-model optimum on shots faced (meters; "
                    "lower = more orthodox positioning). Shown against the sample mean.",
                ),
                Metric(
                    "Closing (6yd)",
                    "gkt_closing_val",
                    "Mean minimum time to reach the six-yard box (seconds; lower = better).",
                ),
                Metric(
                    "Reach",
                    "gkt_reach_val",
                    "Mean reachable area around his position (m²; higher = better).",
                ),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see positioning.",
            empty_condition="gkt_scene_figure is None and gkt_selected_player is None",
        ),
        SubView(
            condition='selected_sub_view == "Shot Stopping"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "gkt_cone_figure",
                            condition="gkt_cone_figure is not None",
                            header="Pre-shot geometry",
                        ),
                        ContentBlock(
                            "chart",
                            "gkt_shotmap_figure",
                            condition="gkt_shotmap_figure is not None",
                            header="Every shot faced — where was he standing?",
                        ),
                    ]
                ),
            ],
            metrics=[
                Metric(
                    "Shots faced",
                    "gkt_shots_val",
                    "On-target-linked shots with tracked GK geometry in scope.",
                ),
                Metric(
                    "Goals",
                    "gkt_goals_val",
                    "Goals conceded on those shots (Goals Prevented arrives with PSxG/TF-48).",
                ),
                Metric(
                    "Off line",
                    "gkt_offline_val",
                    "Mean distance off the goal line at the shot (meters; context, not a "
                    "grade — high values can be sweeping duty).",
                ),
            ],
            warning_var="gkt_warning_text",
            empty_message="Select a goalkeeper to see shot geometry.",
            empty_condition="gkt_cone_figure is None and gkt_selected_player is None",
        ),
    ],
)
page_md = build_page(page_config)
