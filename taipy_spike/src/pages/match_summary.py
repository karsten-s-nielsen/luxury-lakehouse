"""Match Summary page — config only, layout from page_template.

Note: Match Summary uses a custom left column layout (2x2 chart grid)
instead of a single image, so it uses pre_image_content for the charts
and a dummy image_var.
"""

from __future__ import annotations

from page_template import Citation, Metric, PageConfig, build_page

_CHARTS_CONTENT = """\
<|part|render={len(ms_home_name) > 0}|

<|layout|columns=1 1|gap=1rem|

<|{ms_shooting_chart}|image|width=100%|>

<|{ms_passing_chart}|image|width=100%|>

|>

<|layout|columns=1 1|gap=1rem|

<|{ms_possession_chart}|image|width=100%|>

<|{ms_ppda_chart}|image|width=100%|>

|>

*PPDA: Passes Per Defensive Action. <10 = aggressive pressing, >15 = passive.*
|>"""

page_md = build_page(
    PageConfig(
        title="Match Summary",
        icon="scoreboard",
        description="Match scorecard with Expected Goals (xG) and pressing intensity via PPDA.",
        citations=[
            Citation("Rathke (2017)", "https://doi.org/10.1515/jqas-2019-0044"),
            Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688"),
        ],
        image_var="",  # uses pre_image_content for 2x2 chart grid
        empty_message="Select a competition and match to begin.",
        empty_condition="len(ms_home_name) == 0",
        scope_vars=["ms_scope_label"],
        freshness_var="ms_data_freshness",
        pre_image_content=_CHARTS_CONTENT,
        metrics=[
            Metric("Home Score", "ms_home_score"),
            Metric("Away Score", "ms_away_score"),
            Metric(
                "Home xG",
                "ms_home_xg",
                "Expected goals for the home team based on shot quality.",
                delta_var="ms_home_xg_delta",
            ),
            Metric(
                "Away xG",
                "ms_away_xg",
                "Expected goals for the away team based on shot quality.",
                delta_var="ms_away_xg_delta",
            ),
        ],
    )
)
