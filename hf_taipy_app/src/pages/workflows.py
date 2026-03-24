"""AI/ML Workflows page — DAG, table, cost transparency, detail drilldown.

Dashboard mode uses the template (PageConfig + build_page).
Detail mode is hand-crafted (future: separate detail template).
"""

from __future__ import annotations

from page_template import (
    NAV_OPERATIONS,
    ContentBlock,
    ContentRow,
    PageConfig,
    StatCard,
    build_page,
)

page_config = PageConfig(
    title="AI/ML Workflows",
    icon="account_tree",
    nav_section=NAV_OPERATIONS,
    description=(
        "Interactive dependency graph and operational dashboard for all AI/ML workflows. "
        "16 workflow cards covering training (HF Jobs) and inference (Databricks) pipelines. "
        "Cost transparency across three tiers: actual (billing), estimated (live), and projected (YAML)."
    ),
    citations=[],
    stats=[
        StatCard("Workflows", "wf_total_workflows", "wf_workflows_detail"),
        StatCard("Freshness", "wf_freshness_summary", "wf_freshness_detail"),
        StatCard("Total Cost (30 Days)", "wf_total_cost_30d", "wf_cost_detail"),
        StatCard("Run Volume (30 Days)", "wf_run_volume", "wf_run_volume_detail"),
    ],
    content=[
        ContentRow(
            [
                ContentBlock(
                    "html",
                    "wf_dag_html",
                    height_var="wf_dag_height",
                    container_class="ll-dag-container",
                )
            ]
        ),
        ContentRow(
            [
                ContentBlock(
                    "table",
                    "wf_table_data",
                    table_page_size=20,
                )
            ]
        ),
    ],
    empty_message="No workflows match the selected filters.",
    empty_condition="len(wf_table_data) == 0 and wf_cards_loaded",
    warning_var="wf_no_cards_warning",
)

# ---------------------------------------------------------------------------
# Dashboard markdown (generated from template)
# ---------------------------------------------------------------------------
_dashboard_md = build_page(page_config)

# ---------------------------------------------------------------------------
# Detail drilldown — hand-crafted (future: separate detail template)
# Detail navigation disabled for now — table and DAG clicks do not navigate.
# TODO: Replace with template-driven detail layout when detail navigation is
# enabled. The detail view should use a DetailPageConfig (or similar) that
# feeds into build_page(), matching the dashboard/standard/sub-view pattern.
# ---------------------------------------------------------------------------
_detail_md = """
<|Back to Workflows|button|on_action=wf_on_back_click|class_name=ll-header-btn text-no-transform|>

<|part|class_name=ll-detail-header|
## <|{wf_detail_title}|text|>

<|part|content={wf_detail_badges_html}|height=50px|>

<|part|render={len(wf_detail_meta) > 0}|class_name=ll-detail-meta|
<|{wf_detail_meta}|text|>
|>
|>

<|part|render={len(wf_detail_overview) > 0}|class_name=ll-detail-section|
### Overview

<|{wf_detail_overview}|text|mode=md|>
|>

<|part|render={len(wf_detail_data_flow_html) > 0}|class_name=ll-detail-section|
### Data Flow

<|part|content={wf_detail_data_flow_html}|height=300px|>
|>

<|part|render={len(wf_detail_exec_html) > 0}|class_name=ll-detail-section|
### Execution

<|part|content={wf_detail_exec_html}|height=350px|>
|>

<|part|render={len(wf_detail_monitoring_html) > 0}|class_name=ll-detail-section|
### Monitoring

<|part|content={wf_detail_monitoring_html}|height=300px|>
|>

<|part|render={len(wf_detail_cost_html) > 0}|class_name=ll-detail-section|
### Cost Transparency

<|part|content={wf_detail_cost_html}|height=300px|>
|>

<|part|render={len(wf_detail_references_html) > 0}|class_name=ll-detail-section|
### Academic Provenance

<|part|content={wf_detail_references_html}|height=200px|>
|>

<|part|render={len(wf_detail_deps_html) > 0}|class_name=ll-detail-section|
### Dependencies

<|part|content={wf_detail_deps_html}|height=200px|>
|>

<|part|render={len(wf_detail_idempotency_html) > 0 or len(wf_detail_source_html) > 0}|class_name=ll-detail-section|
### Idempotency & Source Code

<|part|render={len(wf_detail_idempotency_html) > 0}|
<|part|content={wf_detail_idempotency_html}|height=200px|>
|>

<|part|render={len(wf_detail_source_html) > 0}|
<|part|content={wf_detail_source_html}|height=250px|>
|>
|>
"""

# ---------------------------------------------------------------------------
# Combined page — two-mode switch (dashboard vs detail)
# Uses string concatenation, NOT f-string, because build_page() output
# contains bare single-brace Taipy bindings that would fail as f-string lookups.
# ---------------------------------------------------------------------------
page_md = (
    "<|part|render={wf_selected_workflow is None}|\n"
    + _dashboard_md
    + "\n|>\n\n"
    + "<|part|render={wf_selected_workflow is not None}|\n"
    + _detail_md
    + "\n|>\n"
)
