"""AI/ML Workflows — DAG rendering + content providers.

SVG/Cytoscape.js DAG generation, dagre layout, legend, click handlers,
and the RawHtml wrapper class for Taipy content providers.

State prefix: wf_
"""

from __future__ import annotations

import json
import logging
from html import escape as html_escape
from typing import TYPE_CHECKING, Any

from db import validate_param_id

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RawHtml wrapper — renders actual HTML via Taipy content provider
# ---------------------------------------------------------------------------


class RawHtml:
    """Wrapper for raw HTML content rendered via Taipy content provider.

    Taipy's <|{var}|text|raw|> escapes HTML. Use <|part|content={var}|>
    with a registered content provider instead.

    For auto-sizing iframes, append AUTOSIZE_JS to the HTML string.
    It measures body.scrollHeight after load and resizes the iframe
    and up to 4 parent wrappers to match.
    """

    __slots__ = ("html",)

    # Generic iframe auto-height script. Appended to RawHtml content by
    # content generators that need the iframe to match its content height.
    # Cytoscape DAGs use their own sizing (fit + zoom cap) but call the
    # same _resizeFrame pattern inline. Non-graph HTML content (future
    # observability pages) should append this constant.
    #
    # 4-level parent walk: iframe -> content provider div -> render wrapper
    # -> layout cell.  This is sufficient for simple HTML content that only
    # needs height propagation (no min-width for horizontal scrolling).
    # The DAG's _fitAndResize walks 8 levels because it also propagates
    # min-width up to the ll-dashboard-scroll wrapper, which sits higher
    # in the DOM tree.
    AUTOSIZE_JS = (
        "<script>\n"
        "(function() {\n"
        "    function _resizeFrame() {\n"
        "        if (!window.frameElement) return;\n"
        "        var h = document.body.scrollHeight;\n"
        "        var el = window.frameElement;\n"
        "        for (var i = 0; i < 4 && el; i++) {\n"
        "            el.style.setProperty('height', h + 'px', 'important');\n"
        "            el.style.setProperty('min-height', 'auto', 'important');\n"
        "            el = el.parentElement;\n"
        "        }\n"
        "    }\n"
        "    if (document.readyState === 'complete') _resizeFrame();\n"
        "    else window.addEventListener('load', _resizeFrame);\n"
        "})();\n"
        "</script>"
    )

    def __init__(self, html: str = "") -> None:
        self.html = html

    def __len__(self) -> int:
        return len(self.html)

    def __bool__(self) -> bool:
        return bool(self.html)

    def __str__(self) -> str:
        return self.html


# ---------------------------------------------------------------------------
# Color + type constants (shared with workflows_stats via import)
# ---------------------------------------------------------------------------

# Single source of truth for color-name -> hex mapping.
# Used by DAG nodes, DAG legend, table cell styles, and stat card detail HTML.
# Keep in sync with ll-cell-type-* rules in style_v2.css.
COLOR_HEX: dict[str, str] = {
    "blue": "#58a6ff",
    "purple": "#bc8cff",
    "teal": "#3fb9a0",
    "amber": "#e3b341",
    "gray": "#6e7681",
}

TYPE_COLORS: dict[str, str] = {
    "training-and-inference": "blue",
    "training": "blue",
    "inference": "blue",
    "grid-computation": "purple",
    "heuristic": "teal",
    "validation": "amber",
}

TYPE_LABELS: dict[str, str] = {
    "training-and-inference": "Train+Infer",
    "training": "Training",
    "inference": "Inference",
    "grid-computation": "Grid Compute",
    "heuristic": "Heuristic",
    "validation": "Validation",
}

STATUS_COLORS: dict[str, str] = {
    "production": "green",
    "active": "green",
    "draft": "gray",
    "deprecated": "red",
}

# Upper bound for DAG container height (JS + Python).
# Lives in the state module (not page_template.py) because it is used
# in DAG HTML generation (_build_dag_html) and dynamic height computation
# (_refresh_table), both of which are state-module responsibilities.
DAG_MAX_HEIGHT_PX = 700

# Runtime and freshness hex colors for stat card detail HTML.
# Keep in sync with ll-cell-rt-* and ll-cell-fresh-* rules in style_v2.css.
RUNTIME_HEX: dict[str, str] = {"db": "#ff6347", "hf": "#ffd500"}
FRESHNESS_HEX: dict[str, str] = {"warning": "#d29922", "stale": "#f85149"}


# ---------------------------------------------------------------------------
# DAG HTML generation (Cytoscape.js + dagre)
# ---------------------------------------------------------------------------


def build_dag_html(
    cards: dict[str, dict[str, Any]],
    highlight_ids: set[str] | None = None,
) -> RawHtml:
    """Generate Cytoscape.js DAG visualization as embeddable HTML.

    Uses dagre layout for automatic left-to-right tier placement.
    Nodes colored by workflow type. Edges from depends_on.
    Click events call back to Python via taipy.gui.invoke_callback.

    When highlight_ids is set, matched nodes render at full opacity and
    context nodes (upstream/downstream neighbors) render dimmed.
    """
    # Compute vertical ordering weights to minimize edge-node overlaps.
    # Each node gets a weight = average of its predecessors' weights.
    # Roots (no predecessors) get evenly spaced weights [0..1].
    # This propagates vertical affinity through the graph so nodes
    # connected to "lower" sources are placed lower in their rank.
    dep_map: dict[str, list[str]] = {}
    for card_id, card in cards.items():
        dep_map[card_id] = [d for d in card.get("depends_on", []) if d in cards]

    roots = [cid for cid, deps in dep_map.items() if not deps]
    roots.sort()  # deterministic order
    weights: dict[str, float] = {}
    for i, r in enumerate(roots):
        weights[r] = i / max(1, len(roots) - 1) if len(roots) > 1 else 0.5

    # BFS propagation: each non-root node = average of predecessors' weights
    remaining = {cid for cid in cards if cid not in weights}
    for _ in range(len(cards)):  # max iterations = graph depth
        if not remaining:
            break
        for cid in list(remaining):
            pred_weights = [weights[d] for d in dep_map[cid] if d in weights]
            if pred_weights:
                weights[cid] = sum(pred_weights) / len(pred_weights)
                remaining.discard(cid)

    # Fallback for any unresolved (cycles, shouldn't happen in a DAG)
    for cid in remaining:
        weights[cid] = 0.5

    # Build nodes with vertical weight for dagre sort
    nodes = []
    for card_id, card in cards.items():
        wf_type = card.get("type", "inference")
        color = TYPE_COLORS.get(wf_type, "gray")
        full_name = card.get("name", card_id)
        label = full_name
        # Truncate long names for display
        if len(label) > 30:
            label = label[:28] + "..."
        # Mark whether this node is a primary match or context
        is_context = highlight_ids is not None and card_id not in highlight_ids
        nodes.append(
            {
                "data": {
                    "id": card_id,
                    "label": label,
                    "fullName": full_name,
                    "type": wf_type,
                    "color": color,
                    "status": card.get("status", "draft"),
                    "context": "yes" if is_context else "no",
                    "vWeight": round(weights.get(card_id, 0.5), 4),
                },
            }
        )

    # Build edges from depends_on
    edges = []
    for card_id, card in cards.items():
        for dep_id in card.get("depends_on", []):
            if dep_id in cards:
                edges.append(
                    {
                        "data": {"source": dep_id, "target": card_id},
                    }
                )

    elements_json = json.dumps(nodes + edges)

    # Color map for Cytoscape styles
    color_styles = "\n".join(
        f"        {{ selector: 'node[color = \"{k}\"]', style: {{ "
        f"'background-color': '{v}', 'border-color': '{v}' }} }},"
        for k, v in COLOR_HEX.items()
    )

    # Legend items — shapes match table column ::before markers (WCAG 1.4.1).
    # circle (train), diamond (grid), triangle (heuristic), square (validation).
    _legend_shapes: dict[str, str] = {
        "training-and-inference": "width:8px;height:8px;border-radius:50%;background:{c};",
        "grid-computation": "width:7px;height:7px;transform:rotate(45deg);background:{c};",
        "heuristic": (
            "width:0;height:0;background:transparent;"
            "border-left:4px solid transparent;border-right:4px solid transparent;"
            "border-bottom:8px solid {c};"
        ),
        "validation": "width:8px;height:8px;border-radius:1px;background:{c};",
    }
    legend_items = "".join(
        f'<span style="display:inline-flex;align-items:center;margin-right:12px;">'
        f'<span style="{_legend_shapes[type_key].format(c=COLOR_HEX[color_name])}'
        f'margin-right:4px;display:inline-block;"></span>'
        f'<span style="font-size:0.75rem;color:{COLOR_HEX[color_name]};">{TYPE_LABELS.get(type_key, type_key)}</span>'
        f"</span>"
        for type_key, color_name in [
            ("training-and-inference", "blue"),
            ("grid-computation", "purple"),
            ("heuristic", "teal"),
            ("validation", "amber"),
        ]
    )

    return RawHtml(
        f'<div id="wf-cy" style="width:100%; background:rgba(0,0,0,0.15);'
        f' border-radius:8px;"></div>\n'
        f'<div style="padding:6px 0;text-align:center;">{legend_items}</div>\n'
        f'<script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"'
        f' integrity="sha384-H3uzGzTfGHUAumB8+s4GEdfFwzAceN9wCCndN8AXubWKFIPuBSWKKtWDx7RhSf/z"'  # pragma: allowlist secret
        f' crossorigin="anonymous"></script>\n'
        f'<script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"'
        f' integrity="sha384-2IH3T69EIKYC4c+RXZifZRvaH5SRUdacJW7j6HtE5rQbvLhKKdawxq6vpIzJ7j9M"'  # pragma: allowlist secret
        f' crossorigin="anonymous"></script>\n'
        f'<script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"'
        f' integrity="sha384-u69h9ebXeSjlg6q/rb1zKTRAGu/h8deCl0409xpS/QJctMKnc4M9Fzkm01VOQdeF"'  # pragma: allowlist secret
        f' crossorigin="anonymous"></script>\n'
        f"<script>\n"
        f"(function() {{\n"
        f"    if (typeof cytoscape === 'undefined') return;\n"
        f"    var container = document.getElementById('wf-cy');\n"
        f"    container.style.height = '{DAG_MAX_HEIGHT_PX}px';\n"
        f"    var cy = cytoscape({{\n"
        f"        container: container,\n"
        f"        elements: {elements_json},\n"
        f"        // Layout run separately below so layoutstop handler\n"
        f"        // is registered before the (synchronous) dagre fires.\n"
        f"        layout: {{ name: 'preset' }},\n"
        f"        style: [\n"
        f"            {{ selector: 'node', style: {{\n"
        f"                'label': 'data(label)',\n"
        f"                'text-valign': 'center',\n"
        f"                'text-halign': 'center',\n"
        f"                'font-size': '11px',\n"
        f"                'color': '#e6edf3',\n"
        f"                'text-outline-color': '#1a1d24',\n"
        f"                'text-outline-width': 2,\n"
        f"                'width': 210,\n"
        f"                'height': 40,\n"
        f"                'shape': 'roundrectangle',\n"
        f"                'border-width': 2,\n"
        f"                'background-opacity': 0.2,\n"
        f"            }} }},\n"
        f"            {{ selector: 'node[status = \"deprecated\"]', style: {{\n"
        f"                'border-style': 'dashed',\n"
        f"                'opacity': 0.6,\n"
        f"            }} }},\n"
        f"            {{ selector: 'node[status = \"draft\"]', style: {{\n"
        f"                'border-style': 'dotted',\n"
        f"                'opacity': 0.5,\n"
        f"            }} }},\n"
        f"            {{ selector: 'node[context = \"yes\"]', style: {{\n"
        f"                'opacity': 0.3,\n"
        f"                'border-style': 'dashed',\n"
        f"            }} }},\n"
        f"{color_styles}\n"
        f"            {{ selector: 'edge', style: {{\n"
        f"                'width': 1.5,\n"
        f"                'line-color': '#6e7681',\n"
        f"                'target-arrow-color': '#6e7681',\n"
        f"                'target-arrow-shape': 'triangle',\n"
        f"                'curve-style': 'bezier',\n"
        f"                'arrow-scale': 0.8,\n"
        f"            }} }},\n"
        f"        ],\n"
        f"        userZoomingEnabled: false,\n"
        f"        userPanningEnabled: false,\n"
        f"        boxSelectionEnabled: false,\n"
        f"    }});\n"
        f"\n"
        f"    // After layout: render at 1:1 (no zoom scaling).  Set container\n"
        f"    // to the graph's natural width.  Propagate min-width through the\n"
        f"    // iframe parent chain up to the ll-dashboard-scroll wrapper so\n"
        f"    // the scroll wrapper triggers a horizontal scrollbar on narrow\n"
        f"    // viewports instead of crushing the graph.\n"
        f"    function _fitAndResize() {{\n"
        f"        var pad = 30;\n"
        f"\n"
        f"        if (cy.elements().length === 0) {{\n"
        f"            // Empty graph — collapse container\n"
        f"            container.style.height = '0px';\n"
        f"        }} else {{\n"
        f"            var bb = cy.elements().boundingBox();\n"
        f"            var naturalW = Math.ceil(bb.w) + pad * 2;\n"
        f"            var naturalH = Math.ceil(bb.h) + pad * 2;\n"
        f"            container.style.height = Math.max(120, naturalH) + 'px';\n"
        f"            cy.resize();\n"
        f"            cy.zoom(1);\n"
        f"            // Pin graph left-top to (pad, pad) so scroll position 0\n"
        f"            // shows the leftmost nodes — no left-side clipping.\n"
        f"            cy.pan({{x: pad - bb.x1, y: pad - bb.y1}});\n"
        f"        }}\n"
        f"\n"
        f"        // Propagate height and min-width through iframe parent chain.\n"
        f"        // Walk up to the ll-dashboard-scroll wrapper (max 8 as safety cap).\n"
        f"        // Expected DOM depth: iframe(0) -> content div(1) -> ll-dag-container(2)\n"
        f"        // -> render wrapper(3) -> content row(4) -> dashboard body(5)\n"
        f"        // -> ll-dashboard-scroll(6, break target). 8 is a safety margin.\n"
        f"        // Height: exact content size.  Min-width: natural graph width so\n"
        f"        // the scroll wrapper knows when content exceeds the viewport.\n"
        f"        // The pastDag flag distinguishes the DAG container (needs height)\n"
        f"        // from outer wrappers (only need min-width for scroll triggering).\n"
        f"        var legendEl = container.nextElementSibling;\n"
        f"        var legendH = legendEl ? legendEl.offsetHeight : 0;\n"
        f"        var bs = getComputedStyle(document.body);\n"
        f"        var bodyM = parseInt(bs.marginTop) + parseInt(bs.marginBottom);\n"
        f"        var totalH = container.offsetHeight + legendH + bodyM;\n"
        f"        var bb2 = cy.elements().length > 0 ? cy.elements().boundingBox() : null;\n"
        f"        var minW = bb2 ? Math.ceil(bb2.w) + pad * 2 : 0;\n"
        f"        if (window.frameElement) {{\n"
        f"            var el = window.frameElement;\n"
        f"            var pastDag = false;\n"
        f"            for (var i = 0; i < 8 && el; i++) {{\n"
        f"                // Stop BEFORE the scroll wrapper — it must stay at\n"
        f"                // viewport width so overflow-x: auto triggers the\n"
        f"                // scrollbar.  Its children having min-width is what\n"
        f"                // makes the content wider than the wrapper.\n"
        f"                if (el.classList && el.classList.contains('ll-dashboard-scroll')) break;\n"
        f"                // Height only on iframe + wrappers up to the DAG container.\n"
        f"                if (!pastDag) {{\n"
        f"                    el.style.setProperty('height', totalH + 'px', 'important');\n"
        f"                    el.style.setProperty('min-height', 'auto', 'important');\n"
        f"                }}\n"
        f"                // min-width on children inside the scroll wrapper.\n"
        f"                if (minW > 0) {{\n"
        f"                    el.style.setProperty('min-width', minW + 'px', 'important');\n"
        f"                }}\n"
        f"                if (el.classList && el.classList.contains('ll-dag-container')) pastDag = true;\n"
        f"                el = el.parentElement;\n"
        f"            }}\n"
        f"        }}\n"
        f"    }}\n"
        f"    cy.one('layoutstop', _fitAndResize);\n"
        f"\n"
        f"    // Run dagre layout — handler above fires on completion.\n"
        f"    // For 0 elements, dagre won't fire layoutstop — call directly.\n"
        f"    if (cy.elements().length === 0) {{\n"
        f"        _fitAndResize();\n"
        f"    }} else {{\n"
        f"        cy.layout({{\n"
        f"            name: 'dagre',\n"
        f"            rankDir: 'LR',\n"
        f"            nodeSep: 40,\n"
        f"            rankSep: 80,\n"
        f"            padding: 30,\n"
        f"            fit: false,\n"
        f"            sort: function(a, b) {{\n"
        f"                // Order nodes within each rank by propagated vertical\n"
        f"                // weight (average of predecessors). Nodes connected to\n"
        f"                // lower sources are placed lower, reducing edge-node overlaps.\n"
        f"                return (a.data('vWeight') || 0.5) - (b.data('vWeight') || 0.5);\n"
        f"            }},\n"
        f"        }}).run();\n"
        f"    }}\n"
        f"}})();\n"
        f"</script>"
    )


# ---------------------------------------------------------------------------
# DAG click callback
# ---------------------------------------------------------------------------


def wf_on_dag_click(state: Any, id: str, payload: dict[str, Any]) -> None:
    """DAG node clicked — switch to detail view for that workflow."""
    # Import here to avoid circular dependency (workflows imports from workflows_dag)
    from state.workflows import _cards, _show_detail

    workflow_id = payload.get("args", [""])[0] if isinstance(payload, dict) else str(id)
    if not workflow_id:
        return
    try:
        validate_param_id(workflow_id)
    except ValueError:
        logger.warning("Invalid workflow ID from DAG click: %r", workflow_id)
        return
    if workflow_id in _cards:
        _show_detail(state, workflow_id)


# ---------------------------------------------------------------------------
# Detail section builders (HTML rendering for workflow card drilldown)
# ---------------------------------------------------------------------------


def build_badges_html(card: dict[str, Any]) -> RawHtml:
    """Status + type badges HTML."""
    status: str = card.get("status") or "draft"
    wf_type: str = card.get("type") or ""
    type_label = TYPE_LABELS.get(wf_type, wf_type)
    type_color = TYPE_COLORS.get(wf_type, "gray")
    status_color = STATUS_COLORS.get(status, "gray")

    return RawHtml(
        f'<span class="ll-badge ll-badge-{status_color}">{html_escape(status)}</span> '
        f'<span class="ll-badge ll-badge-{type_color}">{html_escape(type_label)}</span>'
    )


def build_data_flow_html(card: dict[str, Any]) -> RawHtml:
    """Three-column INPUTS -> WORKFLOW -> OUTPUTS layout."""
    inputs = card.get("inputs", {})
    outputs = card.get("outputs", {})

    # Input chips
    input_chips: list[str] = []
    for ds in inputs.get("datasets", []):
        ds_id = ds.get("id", "")
        desc = ds.get("description", "")
        tooltip = f' title="{html_escape(desc)}"' if desc else ""
        input_chips.append(f'<div class="ll-data-chip"{tooltip}>{html_escape(ds_id)}</div>')
    for mdl in inputs.get("models", []):
        mdl_id = mdl.get("id", "")
        input_chips.append(f'<div class="ll-data-chip">{html_escape(mdl_id)}</div>')

    # Output chips
    output_chips: list[str] = []
    for tbl_out in outputs.get("tables", []):
        tbl_id = tbl_out.get("id", "")
        output_chips.append(f'<div class="ll-data-chip">{html_escape(tbl_id)}</div>')
    for mdl_out in outputs.get("models", []):
        mdl_id = mdl_out.get("id", "")
        dest = mdl_out.get("destination", "")
        label = f"{mdl_id} ({dest})" if dest else mdl_id
        output_chips.append(f'<div class="ll-data-chip">{html_escape(label)}</div>')

    if not input_chips and not output_chips:
        return RawHtml("")

    inputs_html = "\n".join(input_chips) if input_chips else '<div class="ll-data-chip">None</div>'
    outputs_html = "\n".join(output_chips) if output_chips else '<div class="ll-data-chip">None</div>'

    return RawHtml(
        f'<div class="ll-data-flow">'
        f"<div>{inputs_html}</div>"
        f'<div class="ll-data-flow-center">\u2192</div>'
        f"<div>{outputs_html}</div>"
        f"</div>"
    )


def build_exec_html(card: dict[str, Any]) -> RawHtml:
    """Training/inference execution cards."""
    exec_cfg = card.get("execution") or {}
    if not exec_cfg:
        return RawHtml("")

    cards_html: list[str] = []
    for phase in ("training", "inference"):
        phase_cfg = exec_cfg.get(phase)
        if not phase_cfg:
            continue

        rows: list[str] = []
        for key in ("runtime", "trigger", "entry_point", "module", "distribution", "schedule", "timeout", "flavor"):
            val = phase_cfg.get(key)
            if val:
                label = key.replace("_", " ").title()
                rows.append(
                    f'<div class="ll-exec-row">'
                    f'<span class="ll-exec-label">{html_escape(label)}</span>'
                    f"<span>{html_escape(str(val))}</span>"
                    f"</div>"
                )

        rows_html = "\n".join(rows)
        cards_html.append(f'<div class="ll-exec-card"><h4>{phase.title()}</h4>{rows_html}</div>')

    if not cards_html:
        return RawHtml("")
    return RawHtml(f'<div class="ll-exec-grid">{"".join(cards_html)}</div>')


def build_monitoring_html(card: dict[str, Any]) -> RawHtml:
    """Monitoring metrics with bar indicators."""
    monitoring = card.get("monitoring") or {}
    if not monitoring:
        return RawHtml("")

    rows: list[str] = []

    # Freshness SLA
    sla = monitoring.get("freshness_sla_hours")
    if sla is not None:
        rows.append(
            f'<div class="ll-exec-row"><span class="ll-exec-label">Freshness SLA</span><span>{sla}h</span></div>'
        )

    # Validator
    validator = monitoring.get("validator")
    if validator:
        rows.append(
            f'<div class="ll-exec-row">'
            f'<span class="ll-exec-label">Validator</span>'
            f"<span>{html_escape(validator)}</span>"
            f"</div>"
        )

    # Performance thresholds from performance section
    perf = card.get("performance") or {}
    for key in ("inference_timeout", "training_time", "memory_ceiling"):
        val = perf.get(key)
        if val:
            label = key.replace("_", " ").title()
            rows.append(
                f'<div class="ll-exec-row">'
                f'<span class="ll-exec-label">{html_escape(label)}</span>'
                f"<span>{html_escape(str(val))}</span>"
                f"</div>"
            )

    if not rows:
        return RawHtml("")
    return RawHtml("\n".join(rows))


def build_cost_html(
    card: dict[str, Any],
    cold_costs: pd.DataFrame,
    warm_costs: pd.DataFrame,
) -> RawHtml:
    """Cost transparency section — per-phase actual/estimated/projected."""
    cost_cfg = card.get("cost") or {}
    exec_cfg = card.get("execution") or {}
    entry_point = (exec_cfg.get("inference") or {}).get("entry_point", "")

    cards_html: list[str] = []

    for phase in ("training", "inference"):
        phase_cost = cost_cfg.get(phase)
        if not phase_cost:
            continue

        runtime = phase_cost.get("runtime", "\u2014")
        typical = phase_cost.get("typical_cost_usd")
        typical_str = f"${float(typical):.2f}" if typical else "\u2014"

        # Check for actual cost from cold tier
        actual_str = "\u2014"
        source_class = "ll-cost-projected"
        source_label = "Projected"
        if phase == "inference" and entry_point and not cold_costs.empty:
            match_rows = cold_costs[cold_costs["task_key"] == entry_point]
            if not match_rows.empty:
                actual_val = float(match_rows.iloc[0]["total_cost_usd"] or 0)
                if actual_val > 0:
                    actual_str = f"${actual_val:.2f}"
                    source_class = "ll-cost-actual"
                    source_label = "Actual (30d)"

        display_val = actual_str if actual_str != "\u2014" else typical_str
        if display_val == "\u2014" and source_label == "Projected":
            source_class = "ll-cost-estimated"
            source_label = "Estimated"

        cards_html.append(
            f'<div class="ll-cost-card">'
            f"<h4>{phase.title()}</h4>"
            f'<div class="ll-cost-big">{display_val}</div>'
            f'<span class="ll-cost-source {source_class}">{source_label}</span>'
            f'<div style="font-size:0.8rem;color:rgba(255,255,255,0.5);margin-top:0.5rem;">'
            f"Runtime: {html_escape(runtime)}"
            f"</div>"
            f"</div>"
        )

    if not cards_html:
        return RawHtml("")
    return RawHtml(f'<div class="ll-cost-grid">{"".join(cards_html)}</div>')


def build_references_html(card: dict[str, Any]) -> RawHtml:
    """Academic provenance with role badges."""
    refs = card.get("references", [])
    if not refs:
        return RawHtml("")
    items: list[str] = []
    for ref in refs:
        role = ref.get("role", "methodology")
        citation = ref.get("citation", "")
        if citation:
            items.append(
                f'<div class="ll-ref-item">'
                f'<span class="ll-ref-role">{html_escape(role)}</span>'
                f"{html_escape(citation)}</div>"
            )
    return RawHtml("\n".join(items)) if items else RawHtml("")


def build_deps_html(card: dict[str, Any], all_cards: dict[str, dict[str, Any]]) -> RawHtml:
    """Mini dependency graph — immediate upstream/downstream neighbors."""
    deps = card.get("depends_on", [])
    card_id = card.get("id", "")

    # Find downstream (cards that depend on this one)
    downstream: list[str] = []
    for other_id, other_card in all_cards.items():
        if card_id in (other_card.get("depends_on") or []):
            downstream.append(other_card.get("name", other_id))

    if not deps and not downstream:
        return RawHtml("")

    parts: list[str] = []
    if deps:
        dep_names = [all_cards.get(d, {}).get("name", d) for d in deps if d in all_cards]
        if dep_names:
            items = ", ".join(html_escape(n) for n in dep_names)
            parts.append(
                f'<div class="ll-exec-row"><span class="ll-exec-label">Depends on</span><span>{items}</span></div>'
            )

    if downstream:
        items = ", ".join(html_escape(n) for n in downstream)
        parts.append(
            f'<div class="ll-exec-row"><span class="ll-exec-label">Required by</span><span>{items}</span></div>'
        )

    return RawHtml("\n".join(parts))


def build_idempotency_html(card: dict[str, Any]) -> RawHtml:
    """Idempotency strategy display."""
    idem = card.get("idempotency") or {}
    if not idem:
        return RawHtml("")

    strategy = idem.get("strategy", "\u2014")
    key = idem.get("key", "")
    desc = idem.get("description", "")

    # Key can be a string or list
    if isinstance(key, list):
        key_str = ", ".join(str(k) for k in key)
    else:
        key_str = str(key) if key else "\u2014"

    rows: list[str] = [
        f'<div class="ll-exec-row">'
        f'<span class="ll-exec-label">Strategy</span>'
        f"<span>{html_escape(strategy)}</span>"
        f"</div>",
        f'<div class="ll-exec-row"><span class="ll-exec-label">Key</span><span>{html_escape(key_str)}</span></div>',
    ]
    if desc:
        rows.append(
            f'<div style="font-size:0.85rem;color:rgba(255,255,255,0.6);margin-top:0.5rem;">{html_escape(desc)}</div>'
        )

    return RawHtml("\n".join(rows))


def build_source_html(card: dict[str, Any]) -> RawHtml:
    """Source code links + HF Hub links."""
    links = card.get("links") or {}
    if not links:
        return RawHtml("")

    parts: list[str] = []

    # Source code paths
    for src_path in links.get("source_code", []):
        parts.append(f'<div class="ll-source-link">{html_escape(src_path)}</div>')

    # HF links
    for key in ("hf_model", "hf_dataset"):
        url = links.get(key)
        if url:
            label = "Model" if "model" in key else "Dataset"
            parts.append(
                f'<div class="ll-source-link">'
                f'<a href="{html_escape(url)}" target="_blank" '
                f'style="color:var(--color-primary);text-decoration:none;">'
                f"HF {label}: {html_escape(url)}</a>"
                f"</div>"
            )

    return RawHtml("\n".join(parts)) if parts else RawHtml("")


__all__ = [
    "RawHtml",
    "COLOR_HEX",
    "TYPE_COLORS",
    "TYPE_LABELS",
    "STATUS_COLORS",
    "DAG_MAX_HEIGHT_PX",
    "RUNTIME_HEX",
    "FRESHNESS_HEX",
    "build_badges_html",
    "build_cost_html",
    "build_dag_html",
    "build_data_flow_html",
    "build_deps_html",
    "build_exec_html",
    "build_idempotency_html",
    "build_monitoring_html",
    "build_references_html",
    "build_source_html",
    "wf_on_dag_click",
]
