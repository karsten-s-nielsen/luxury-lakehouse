"""Team Shape page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_ADVANCED, Citation, ContentBlock, ContentRow, Metric, PageConfig, SubView, build_page

page_config = PageConfig(
    title="Team Shape",
    icon="polyline",
    nav_section=NAV_ADVANCED,
    freshness_var="ts_data_freshness",
    description=(
        "Team shape analysis using EFPI formation detection (Bekkers & Dabadghao / UnravelSports), "
        "convex hull geometry, and spatial stretch/compactness metrics. Tracking data available "
        "for ~20 matches from Metrica, IDSSE, and SkillCorner."
    ),
    citations=[
        Citation("Bekkers & Dabadghao (2025)", "https://arxiv.org/abs/2506.23843"),
        Citation("Frencken et al. (2011)", "https://doi.org/10.1080/02640414.2011.594963"),
        Citation("Bourbousson et al. (2010)", "https://doi.org/10.1080/02640410903503632"),
    ],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Snapshot"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "ts_snapshot_figure",
                            condition="ts_snapshot_figure is not None",
                            chart_height="540px",
                        )
                    ]
                ),
            ],
            warning_var="ts_warning_text",
            empty_message="Select a tracking match to begin.",
            empty_condition="ts_snapshot_figure is None and len(tracking_match_lov) > 0",
            fallback_empty_message=(
                "No tracking data available. This page requires tracking data (available for ~20 matches)."
            ),
            fallback_empty_condition="ts_snapshot_figure is None and len(tracking_match_lov) == 0",
            scope_vars=["ts_snapshot_caption"],
            metrics=[
                Metric(
                    "Team Length",
                    "ts_team_length",
                    "Distance from deepest defender to highest attacker in meters. Under 30m = compact, over 40m = stretched.",
                    delta_var="ts_length_delta",
                ),
                Metric(
                    "Team Width",
                    "ts_team_width",
                    "Distance between widest players in meters. Over 38m = good width creation.",
                    delta_var="ts_width_delta",
                ),
                Metric(
                    "Defensive Line",
                    "ts_def_line_height",
                    "Defensive line height as pitch percentage. 0% = own goal, 100% = opponent goal. "
                    "Over 50% = high press, under 35% = deep block.",
                ),
                Metric(
                    "Hull Area",
                    "ts_hull_area",
                    "Convex hull area enclosing outfield players in m². "
                    "~1000m² when defending compactly, ~1500m² when attacking.",
                ),
                Metric(
                    "Stretch Index",
                    "ts_stretch_index",
                    "Mean distance of players from the team centroid in meters. "
                    "Under 10m = compact defense, over 16m = stretched attack. Lower = more compact shape.",
                ),
                Metric(
                    "Inter-Line Gaps",
                    "ts_inter_line_gaps",
                    "Average vertical gap between defensive lines in meters. "
                    "Under 12m = compact block, over 18m = exposed gaps.",
                ),
            ],
        ),
        SubView(
            condition='selected_sub_view == "Timeline"',
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "ts_timeline_figure",
                            header="Shape Metrics Over Time",
                            condition="ts_timeline_figure is not None",
                            chart_height="420px",
                        )
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "chart",
                            "ts_formation_figure",
                            header="Formation Labels (5-min windows)",
                            condition="ts_formation_figure is not None",
                            chart_height="300px",
                        )
                    ]
                ),
                ContentRow(
                    [
                        ContentBlock(
                            "table",
                            "ts_phase_comparison",
                            header="Phase Comparison",
                            condition="len(ts_phase_comparison) > 0",
                        )
                    ]
                ),
            ],
            warning_var="ts_warning_text",
            empty_message="Select a tracking match to begin.",
            empty_condition="ts_timeline_figure is None and len(tracking_match_lov) > 0",
            fallback_empty_message=(
                "No tracking data available. This page requires tracking data (available for ~20 matches)."
            ),
            fallback_empty_condition="ts_timeline_figure is None and len(tracking_match_lov) == 0",
            metrics=[
                Metric(
                    "Avg Length",
                    "ts_avg_length",
                    "Average team length across the match in meters. Under 30m = compact, over 40m = stretched.",
                ),
                Metric(
                    "Avg Width",
                    "ts_avg_width",
                    "Average team width across the match in meters. Over 38m = good width creation.",
                ),
                Metric(
                    "Formation Changes",
                    "ts_formation_changes",
                    "Number of formation transitions detected across 5-minute windows.",
                ),
                Metric(
                    "Compactness Δ",
                    "ts_compactness_delta",
                    "Change in average hull area from 1st half to 2nd half. "
                    "Positive = team spread wider in the 2nd half.",
                    delta_var="ts_compactness_delta_fmt",
                ),
            ],
        ),
    ],
)
page_md = build_page(page_config)
