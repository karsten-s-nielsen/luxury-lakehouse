"""Shot Map page — config only, layout from page_template."""

from __future__ import annotations

from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    title="Shot Map",
    icon="target",
    nav_section="Match Analysis",
    description="Shot locations sized by xG with isotonic calibration.",
    citations=[
        Citation("Rathke (2017)", "https://doi.org/10.1515/jqas-2019-0044"),
        Citation("XGBoost", "https://xgboost.readthedocs.io/"),
    ],
    content=[ContentRow([ContentBlock("image", "sm_pitch_image")])],
    empty_message="Select a competition to begin.",
    empty_condition="len(sm_pitch_image) == 0 and len(sm_empty_message) > 0",
    warning_var="sm_warning_text",
    scope_vars=["sm_scope_label", "sm_data_scope_note", "sm_nan_fallback_note"],
    freshness_var="sm_data_freshness",
    metrics=[
        Metric("Total Shots", "sm_total_shots", "Total number of shots attempted."),
        Metric("Goals", "sm_goals", "Total number of goals scored."),
        Metric(
            "Total xG",
            "sm_total_xg",
            "Probability of scoring from each shot's location and context. "
            "Higher = better chance. Sum over a match = team's expected output.",
            delta_var="sm_xg_delta",
        ),
        Metric("Conversion Rate", "sm_conversion", "Goals / Total Shots — percentage of shots that resulted in goals."),
        Metric(
            "xG / Shot",
            "sm_xg_per_shot",
            "Average expected goals per shot — higher indicates better shot quality.",
            delta_var="sm_xg_per_delta",
        ),
        Metric(
            "Brier Score",
            "sm_brier",
            "Prediction calibration metric — lower is better. "
            "0.0 = perfect predictions, 0.25 = coin flip. Good models score under 0.10.",
            delta_var="sm_brier_delta",
            lower_is_better=True,
        ),
    ],
)
page_md = build_page(page_config)
