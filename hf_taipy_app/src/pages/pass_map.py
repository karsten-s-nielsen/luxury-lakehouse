"""Pass Map page — config only, layout from page_template."""

from __future__ import annotations

from page_template import NAV_MATCH_ANALYSIS, Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Pass Map",
    icon="arrow_forward",
    nav_section=NAV_MATCH_ANALYSIS,
    description=(
        "Progressive passes and line-breaking detection via Ward clustering "
        "on StatsBomb 360 freeze-frame positions. "
        "Line-breaking detection requires StatsBomb 360 data (~323 of 380+ matches)."
    ),
    citations=[
        Citation("Suzuki et al. (2019)", "https://doi.org/10.1515/jqas-2019-0060"),
        Citation("Parma Calcio 1913", "https://github.com/parmacalcio1913/line-breaking-passes"),
    ],
    content=[ContentRow([ContentBlock("image", "pm_pitch_image")])],
    empty_message="Select a competition, team, and match to begin.",
    empty_condition='len(pm_pitch_image) == 0 and pm_total == "--"',
    warning_var="pm_warning_text",
    scope_vars=["pm_scope_label"],
    freshness_var="pm_data_freshness",
    metrics=[
        Metric("Total Passes", "pm_total", "Number of passes attempted by the selected player or team."),
        Metric("Completed", "pm_completed", "Number of passes that reached the intended recipient."),
        Metric("Completion %", "pm_completion_pct", "Percentage of attempted passes successfully completed."),
        Metric("Progressive", "pm_progressive", "Passes that move the ball significantly toward the opponent's goal."),
        Metric(
            "Line-Breaking",
            "pm_line_breaking",
            "A pass that penetrates at least one defensive line, detected via "
            "Ward clustering on StatsBomb 360 freeze-frame defender positions.",
        ),
    ],
)
page_md = build_page(page_config)
