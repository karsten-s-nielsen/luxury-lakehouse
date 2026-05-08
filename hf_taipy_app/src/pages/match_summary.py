"""Match Summary page — dashboard layout per 2026-04-19 redesign spec.

PageConfig only; all rendering flows through ``page_template.build_page``.
State variables, callbacks, and query orchestration live in
``state/match_summary.py``.
"""

from __future__ import annotations

from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    RequiredFilter,
    ScopeDim,
    StatCard,
    build_page,
)

page_config = PageConfig(
    title="Match Summary",
    icon="scoreboard",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Match scorecard with editorial verdict and decisive-action narrative. "
        "xG per Robberechts & Davis (2020). VAEP per Decroos et al. (2019), computed via silly-kicks. "
        "Pressing intensity (PPDA) per Trainor & Chassy (2021)."
    ),
    citations=[
        Citation(
            "Robberechts & Davis (2020) — How Data Availability Affects the Ability to Learn Good xG Models",
            "https://dtai.cs.kuleuven.be/sports/blog/how-data-availability-affects-the-ability-to-learn-good-xg-models",
        ),
        Citation(
            "Decroos et al. (2019) — Actions Speak Louder than Goals: Valuing Player Actions in Soccer",
            "https://doi.org/10.1145/3292500.3330758",
        ),
        Citation("silly-kicks", "https://github.com/karsten-s-nielsen/silly-kicks"),
        Citation("Trainor & Chassy (2021)", "https://doi.org/10.3389/fpsyg.2020.531688"),
    ],
    scope_dims=[
        ScopeDim("Competition", "ms_scope_comp"),
        ScopeDim("Team", "ms_scope_team"),
        ScopeDim("Match", "ms_scope_match"),
    ],
    stats=[
        StatCard(
            label="Final",
            var="ms_final_score",
            help_text="Full-time score. Home team listed first.",
        ),
        StatCard(
            label="Home xG",
            var="ms_home_xg",
            detail_var="ms_home_xg_delta",
            help_text=(
                "Expected goals from shot locations and context. Delta = goals minus xG; positive = overperformed."
            ),
        ),
        StatCard(
            label="Away xG",
            var="ms_away_xg",
            detail_var="ms_away_xg_delta",
            help_text=(
                "Expected goals from shot locations and context. Delta = goals minus xG; positive = overperformed."
            ),
        ),
        StatCard(
            label="Our Verdict",
            var="ms_verdict_phrase",
            detail_var="ms_verdict_detail",
            help_text=(
                "Editorial interpretation of whether the scoreline reflected the run of play, "
                "by xG margin. Phrase set: Fully merited / Fair result / Fortunate / "
                "Smash & grab / Flattered by scoreline."
            ),
        ),
    ],
    content=[
        ContentRow(
            [
                ContentBlock(
                    "html",
                    "ms_moments_html",
                    height_var="ms_moments_height",
                    container_class="ll-match-moments-wrap",
                ),
            ],
            condition="len(ms_home_name) > 0",
        ),
        ContentRow(
            [
                ContentBlock(
                    "chart",
                    "ms_xg_race_fig",
                    chart_height="320px",
                    caption_var="ms_xg_race_alt",
                    condition="ms_xg_race_fig is not None",
                ),
            ],
            condition="len(ms_home_name) > 0",
        ),
        ContentRow(
            [
                ContentBlock(
                    "html",
                    "ms_delta_table_html",
                    height_var="ms_delta_table_height",
                    container_class="ll-delta-table-wrap",
                ),
            ],
            condition="len(ms_home_name) > 0",
        ),
    ],
    warning_var="ms_warning_text",
    required_filters=[
        RequiredFilter("selected_competition", "competition"),
        RequiredFilter("selected_match", "match"),
    ],
    # League averages live per-row in the Row 3 delta table (spec §5.5), not as
    # a standalone scope line — removed 2026-04-19 after post-deploy UX review.
    freshness_var="ms_data_freshness",
)
page_md = build_page(page_config)
