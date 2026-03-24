"""AI/ML Workflows page state — DAG, table, detail drilldown, cost queries.

All variables prefixed with wf_. Manages workflow card loading from YAML,
Cytoscape.js DAG rendering, dashboard table with cost data from Lakebase,
and 8-section detail drilldown.

State prefix: wf_
Route key: AI-ML-Workflows
"""

from __future__ import annotations

import json
import logging
import re
from html import escape as html_escape
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from cache import ttl_cache
from config import get_settings
from db import execute_query, t, validate_param_id

from state.shared import register_page_refresher

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
# Constants
# ---------------------------------------------------------------------------
_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)", re.DOTALL)

# Stat card detail: base CSS for content-provider iframes (dark theme, no margin).
# Content provider iframes are sandboxed documents — they do NOT inherit the app theme.
# Keep font-family and color in sync with the app's dark theme if it changes.
_STAT_DETAIL_STYLE = (
    "margin:0;padding:0;background:transparent;"
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
    "font-size:0.8rem;color:rgba(255,255,255,0.6);line-height:1.4;"
)


def _stat_detail_html(inner: str) -> RawHtml:
    """Wrap colored HTML in a dark-themed body for stat card content provider."""
    if not inner:
        return RawHtml("")
    return RawHtml(f'<body style="{_STAT_DETAIL_STYLE}">{inner}</body>')


# Single source of truth for color-name → hex mapping.
# Used by DAG nodes, DAG legend, table cell styles, and stat card detail HTML.
# Keep in sync with ll-cell-type-* rules in style_v2.css.
_COLOR_HEX: dict[str, str] = {
    "blue": "#58a6ff",
    "purple": "#bc8cff",
    "teal": "#3fb9a0",
    "amber": "#e3b341",
    "gray": "#6e7681",
}

# Runtime and freshness hex colors for stat card detail HTML.
# Keep in sync with ll-cell-rt-* and ll-cell-fresh-* rules in style_v2.css.
_RUNTIME_HEX: dict[str, str] = {"db": "#ff6347", "hf": "#ffd500"}
_FRESHNESS_HEX: dict[str, str] = {"warning": "#d29922", "stale": "#f85149"}

_TYPE_COLORS: dict[str, str] = {
    "training-and-inference": "blue",
    "training": "blue",
    "inference": "blue",
    "grid-computation": "purple",
    "heuristic": "teal",
    "validation": "amber",
    "augmentation": "gray",
}

# Upper bound for DAG container height (JS + Python).
# Lives in the state module (not page_template.py) because it is used
# in DAG HTML generation (_build_dag_html) and dynamic height computation
# (_refresh_table), both of which are state-module responsibilities.
_DAG_MAX_HEIGHT_PX = 700

_TYPE_LABELS: dict[str, str] = {
    "training-and-inference": "Train+Infer",
    "training": "Training",
    "inference": "Inference",
    "grid-computation": "Grid Compute",
    "heuristic": "Heuristic",
    "validation": "Validation",
    "augmentation": "Augmentation",
}


def _classify_freshness(age_hours: float, sla_hours: float) -> str:
    """Classify workflow freshness against its SLA threshold.

    Three tiers: OK (within 75% of SLA), Warning (within SLA), Stale (beyond SLA).
    Used by table rendering, filter matching, and stats computation.
    """
    if age_hours <= sla_hours * 0.75:
        return "OK"
    if age_hours <= sla_hours:
        return "Warning"
    return "Stale"


def _classify_runtime(exec_cfg: dict[str, Any]) -> str:
    """Classify workflow runtime from execution config phases.

    Returns human-readable label: 'DB', 'HF', 'DB + HF', or em-dash.
    """
    rts: list[str] = []
    for phase in ("training", "inference"):
        rt = ((exec_cfg.get(phase) or {}).get("runtime") or "").lower()
        if "hf" in rt:
            if "HF" not in rts:
                rts.append("HF")
        elif "databricks" in rt:
            if "DB" not in rts:
                rts.append("DB")
    return " + ".join(rts) if rts else "\u2014"


_STATUS_COLORS: dict[str, str] = {
    "production": "green",
    "active": "green",
    "draft": "gray",
    "deprecated": "red",
}

# ---------------------------------------------------------------------------
# Table cell style callbacks (resolved by name via Taipy style[column])
# ---------------------------------------------------------------------------

# Type label → hex color (derived from _TYPE_COLORS + _COLOR_HEX + _TYPE_LABELS)
_TYPE_LABEL_COLORS: dict[str, str] = {
    _TYPE_LABELS[k]: _COLOR_HEX[v] for k, v in _TYPE_COLORS.items() if k in _TYPE_LABELS
}

# Type label → CSS class (matches DAG node colors)
_TYPE_CELL_STYLES: dict[str, str] = {
    "Train+Infer": "ll-cell-type-train",
    "Training": "ll-cell-type-train",
    "Inference": "ll-cell-type-train",
    "Grid Compute": "ll-cell-type-grid",
    "Heuristic": "ll-cell-type-heuristic",
    "Validation": "ll-cell-type-validation",
    "Augmentation": "ll-cell-type-augmentation",
}


def wf_style_type(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Type column cells."""
    return _TYPE_CELL_STYLES.get(str(value), "")


def wf_style_runtime(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Runtime column cells."""
    s = str(value)
    if "+" in s:
        return "ll-cell-rt-both"
    if s == "DB":
        return "ll-cell-rt-db"
    if s == "HF":
        return "ll-cell-rt-hf"
    return ""


def wf_style_freshness(state: Any, value: Any, index: int, row: int, column_name: str) -> str:
    """Return CSS class for Freshness column cells."""
    s = str(value)
    if s == "OK":
        return "ll-cell-fresh-ok"
    if s == "Warning":
        return "ll-cell-fresh-warning"
    if s == "Stale":
        return "ll-cell-fresh-stale"
    return ""


# ---------------------------------------------------------------------------
# Dashboard state
# ---------------------------------------------------------------------------
wf_selected_workflow: str | None = None  # None = dashboard, set = detail view

wf_dag_html: RawHtml = RawHtml("")
wf_dag_height: str = "700px"  # Dynamic height for DAG container — computed from node count
wf_cards_loaded: bool = False  # True after YAML cards loaded successfully
wf_no_cards_warning: str = ""  # Non-empty when YAML cards fail to load (warning_var target)

wf_total_workflows: str = "0"
wf_workflows_detail: RawHtml = RawHtml("")
wf_freshness_summary: str = "\u2014"
wf_freshness_detail: RawHtml = RawHtml("")
wf_total_cost_30d: str = "$0.00"
wf_cost_detail: RawHtml = RawHtml("")
wf_run_volume: str = "0"
wf_run_volume_detail: str = ""
_WF_TABLE_COLS = [
    "Name",
    "Type",
    "Runtime",
    "Last Run",
    "Last Duration",
    "Cost (30d)",
    "Avg/Run",
    "Freshness",
]
wf_table_data: pd.DataFrame = pd.DataFrame(columns=pd.Index(_WF_TABLE_COLS))

wf_type_filter: str | None = "All"
wf_type_lov: list[str] = ["All"]
wf_runtime_filter: str | None = "All"
wf_runtime_lov: list[str] = ["All"]
wf_freshness_filter: str | None = "All"
wf_freshness_lov: list[str] = ["All"]

# ---------------------------------------------------------------------------
# Detail state
# ---------------------------------------------------------------------------
wf_detail_title: str = ""
wf_detail_badges_html: RawHtml = RawHtml("")
wf_detail_meta: str = ""
wf_detail_overview: str = ""
wf_detail_data_flow_html: RawHtml = RawHtml("")
wf_detail_exec_html: RawHtml = RawHtml("")
wf_detail_monitoring_html: RawHtml = RawHtml("")
wf_detail_cost_html: RawHtml = RawHtml("")
wf_detail_references_html: RawHtml = RawHtml("")
wf_detail_deps_html: RawHtml = RawHtml("")
wf_detail_idempotency_html: RawHtml = RawHtml("")
wf_detail_source_html: RawHtml = RawHtml("")

# Admin (Phase 2 foundation)
wf_is_admin: bool = False

# ---------------------------------------------------------------------------
# Internal (NOT exported — not bound to UI)
# ---------------------------------------------------------------------------
_cards: dict[str, dict[str, Any]] = {}
_cost_by_task: dict[str, float] = {}  # task_key -> 30d cost USD
_wf_card_ids: list[str] = []  # Parallel to wf_table_data rows — maps row index to card ID

__all__ = [
    # RawHtml wrapper (used by main.py content provider)
    "RawHtml",
    # Dashboard
    "wf_selected_workflow",
    "wf_dag_html",
    "wf_dag_height",
    "wf_cards_loaded",
    "wf_no_cards_warning",
    "wf_total_workflows",
    "wf_workflows_detail",
    "wf_freshness_summary",
    "wf_freshness_detail",
    "wf_total_cost_30d",
    "wf_cost_detail",
    "wf_run_volume",
    "wf_run_volume_detail",
    "wf_table_data",
    "wf_type_filter",
    "wf_type_lov",
    "wf_runtime_filter",
    "wf_runtime_lov",
    "wf_freshness_filter",
    "wf_freshness_lov",
    # Detail
    "wf_detail_title",
    "wf_detail_badges_html",
    "wf_detail_meta",
    "wf_detail_overview",
    "wf_detail_data_flow_html",
    "wf_detail_exec_html",
    "wf_detail_monitoring_html",
    "wf_detail_cost_html",
    "wf_detail_references_html",
    "wf_detail_deps_html",
    "wf_detail_idempotency_html",
    "wf_detail_source_html",
    # Admin
    "wf_is_admin",
    # Callbacks
    "wf_on_dag_click",
    "wf_on_back_click",
    "wf_on_type_filter",
    "wf_on_runtime_filter",
    "wf_on_freshness_filter",
    "wf_on_table_action",
    "wf_refresh",
    # Table cell style callbacks
    "wf_style_type",
    "wf_style_runtime",
    "wf_style_freshness",
]


# ---------------------------------------------------------------------------
# Card loading from YAML
# ---------------------------------------------------------------------------


def _load_cards_from_yaml() -> dict[str, dict[str, Any]]:
    """Load workflow card YAML files from workflow-cards/ directory.

    Searches relative to app root (works both locally and on HF Spaces).
    Returns dict keyed by card 'id' field.
    """
    # Try multiple paths: HF Space root, local dev
    candidates = [
        Path("workflow-cards"),
        Path(__file__).parent.parent.parent / "workflow-cards",  # hf_taipy_app/../workflow-cards
        Path(__file__).parent.parent.parent.parent / "workflow-cards",  # repo root
    ]
    cards_dir: Path | None = None
    for p in candidates:
        if p.is_dir() and list(p.glob("*.yaml")):
            cards_dir = p
            break

    if cards_dir is None:
        logger.warning("No workflow-cards directory found")
        return {}

    cards: dict[str, dict[str, Any]] = {}
    for yaml_path in sorted(cards_dir.glob("*.yaml")):
        try:
            text = yaml_path.read_text(encoding="utf-8")
            match = _FRONTMATTER_RE.match(text)
            if not match:
                logger.warning("No frontmatter in %s", yaml_path.name)
                continue
            data: dict[str, Any] = yaml.safe_load(match.group(1)) or {}
            data["body"] = match.group(2)
            data["_file"] = yaml_path.name
            card_id = data.get("id", "")
            if card_id:
                cards[card_id] = data
        except Exception:
            logger.exception("Failed to parse %s", yaml_path.name)

    logger.info("Loaded %d workflow cards", len(cards))
    return cards


# ---------------------------------------------------------------------------
# DAG HTML generation (Cytoscape.js + dagre)
# ---------------------------------------------------------------------------


def _build_dag_html(
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
        color = _TYPE_COLORS.get(wf_type, "gray")
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
        for k, v in _COLOR_HEX.items()
    )

    # Legend items
    legend_items = "".join(
        f'<span style="display:inline-flex;align-items:center;margin-right:12px;">'
        f'<span style="width:10px;height:10px;border-radius:2px;background:{_COLOR_HEX[color_name]};'
        f'margin-right:4px;"></span>'
        f'<span style="font-size:0.75rem;color:#8b949e;">{_TYPE_LABELS.get(type_key, type_key)}</span>'
        f"</span>"
        for type_key, color_name in [
            ("training-and-inference", "blue"),
            ("grid-computation", "purple"),
            ("heuristic", "teal"),
            ("validation", "amber"),
            ("augmentation", "gray"),
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
        f"    container.style.height = '{_DAG_MAX_HEIGHT_PX}px';\n"
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
        f"\n"
        f"    // Click handler \u2014 find and click the matching table row in parent\n"
        f"    cy.on('tap', 'node', function(evt) {{\n"
        f"        var fullName = evt.target.data('fullName');\n"
        f"        if (!fullName) return;\n"
        f"        try {{\n"
        f"            var rows = window.parent.document.querySelectorAll('.MuiTableBody-root tr');\n"
        f"            for (var i = 0; i < rows.length; i++) {{\n"
        f"                var cell = rows[i].querySelector('td');\n"
        f"                if (cell && cell.textContent.trim() === fullName) {{\n"
        f"                    rows[i].click();\n"
        f"                    break;\n"
        f"                }}\n"
        f"            }}\n"
        f"        }} catch(e) {{ console.warn('DAG click handler:', e); }}\n"
        f"    }});\n"
        f"\n"
        f"    // Visual click feedback: cursor pointer on nodes\n"
        f"    cy.on('mouseover', 'node', function() {{\n"
        f"        container.style.cursor = 'pointer';\n"
        f"    }});\n"
        f"    cy.on('mouseout', 'node', function() {{\n"
        f"        container.style.cursor = 'default';\n"
        f"    }});\n"
        f"}})();\n"
        f"</script>"
    )


# ---------------------------------------------------------------------------
# Cost query functions (Lakebase)
# ---------------------------------------------------------------------------


@ttl_cache()
def _fetch_cold_costs() -> pd.DataFrame:
    """30-day aggregated costs from fct_workflow_costs_synced (cold tier).

    Returns DataFrame with columns: task_key, total_cost_usd, total_dbu, run_count.
    """
    _empty = pd.DataFrame(columns=pd.Index(["task_key", "total_cost_usd", "total_dbu", "run_count"]))
    try:
        tbl = t("fct_workflow_costs_synced")
        return execute_query(
            f"SELECT task_key, "  # noqa: S608
            f"  SUM(attributed_cost_usd) AS total_cost_usd, "
            f"  SUM(attributed_dbu) AS total_dbu, "
            f"  COUNT(DISTINCT job_run_id) AS run_count "
            f"FROM {tbl} "
            f"WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days' "
            f"GROUP BY task_key "
            f"ORDER BY total_cost_usd DESC "
            f"LIMIT 100",
        )
    except Exception:
        logger.warning("Cold cost query failed — costs unavailable", exc_info=True)
        return _empty


@ttl_cache()
def _fetch_warm_costs() -> pd.DataFrame:
    """Recent cost estimates from workflow_cost_live_synced (warm tier).

    Returns DataFrame with columns: workflow_id, state, duration_seconds,
    estimated_cost_usd, started_at, ended_at, task_key.
    """
    try:
        settings = get_settings()
        tbl = t("workflow_cost_live_synced", schema=settings.observability_schema)
        return execute_query(
            f"SELECT workflow_id, phase, state, task_key, "  # noqa: S608
            f"  duration_seconds, estimated_cost_usd, "
            f"  started_at, ended_at, rate_usd_per_hour "
            f"FROM {tbl} "
            f"WHERE started_at >= NOW() - INTERVAL '30 days' "
            f"ORDER BY started_at DESC "
            f"LIMIT 500",
        )
    except Exception:
        logger.warning("Warm cost query failed — costs unavailable", exc_info=True)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Jobs API — last run + duration + freshness
# ---------------------------------------------------------------------------


@ttl_cache()
def _fetch_job_runs() -> dict[str, dict[str, Any]]:
    """Fetch recent job runs from Databricks Jobs API.

    Returns dict keyed by task_key with latest run info:
    {task_key: {"last_run": datetime, "duration_seconds": int, "state": str}}
    """
    try:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
        # Find runs from the last 30 days
        runs: dict[str, dict[str, Any]] = {}
        for run in ws.jobs.list_runs(
            expand_tasks=True,
            start_time_from=int(pd.Timestamp.now(tz="UTC").timestamp() * 1000 - 30 * 86_400_000),
            limit=25,
        ):
            if not run.tasks:
                continue
            for task in run.tasks:
                key = task.task_key or ""
                if not key:
                    continue
                end_time = task.end_time or 0
                if key not in runs or end_time > runs[key].get("end_time_ms", 0):
                    duration = (task.execution_duration or 0) // 1000  # ms -> seconds
                    runs[key] = {
                        "last_run": (pd.Timestamp(end_time, unit="ms", tz="UTC") if end_time else None),
                        "duration_seconds": duration,
                        "state": (
                            task.state.result_state.value if task.state and task.state.result_state else "UNKNOWN"
                        ),
                        "end_time_ms": end_time,
                    }
        logger.info("Fetched run data for %d task keys from Jobs API", len(runs))
        return runs
    except Exception:
        logger.warning("Jobs API query failed \u2014 run data unavailable", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# Table data builder
# ---------------------------------------------------------------------------


def _build_table_data(
    cards: dict[str, dict[str, Any]],
    cold_costs: pd.DataFrame,
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    runtime_filter: str | None = "All",
    freshness_filter: str | None = "All",
) -> pd.DataFrame:
    """Build dashboard table DataFrame from cards + cost data."""
    global _wf_card_ids
    card_ids: list[str] = []

    # Build cost lookups: entry_point -> 30d USD and run count
    cost_lookup: dict[str, float] = {}
    run_count_lookup: dict[str, int] = {}
    if not cold_costs.empty:
        cost_lookup = cold_costs.set_index("task_key")["total_cost_usd"].apply(lambda x: float(x or 0)).to_dict()
        run_count_lookup = cold_costs.set_index("task_key")["run_count"].apply(lambda x: int(x or 0)).to_dict()

    rows = []
    for card_id, card in cards.items():
        wf_type = card.get("type", "")

        # Apply filters
        if type_filter and type_filter != "All" and _TYPE_LABELS.get(wf_type, wf_type) != type_filter:
            continue

        # Determine runtime(s)
        exec_cfg = card.get("execution") or {}
        runtime_str = _classify_runtime(exec_cfg)

        # Apply runtime filter
        if runtime_filter and runtime_filter != "All" and runtime_str != runtime_filter:
            continue

        # Cost: actual from cold tier, formatted for display + sort
        # Zero-padded $XXX.XX format sorts correctly as strings
        entry_point = (exec_cfg.get("inference") or {}).get("entry_point", "")
        actual_cost = cost_lookup.get(entry_point)
        runs = run_count_lookup.get(entry_point, 0)
        if actual_cost is not None and actual_cost > 0:
            cost_val = f"${actual_cost:7.2f}"
            avg_run_val = f"${actual_cost / runs:7.2f}" if runs > 0 else "\u2014"
        else:
            cost_val = "\u2014"
            avg_run_val = "\u2014"

        # Last Run + Duration + Freshness from Jobs API
        job_run = job_runs.get(entry_point, {})
        last_run_ts = job_run.get("last_run")
        duration_secs = job_run.get("duration_seconds", 0)

        last_run_str = "\u2014"
        duration_str = "\u2014"
        if last_run_ts is not None:
            last_run_str = last_run_ts.strftime("%Y-%m-%d %H:%M")
            if duration_secs > 0:
                mins, secs = divmod(duration_secs, 60)
                duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        # Freshness: from Jobs API last run time vs SLA
        freshness_str = "\u2014"
        sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours and last_run_ts is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
            freshness_str = _classify_freshness(age_hours, sla_hours)
        elif sla_hours is None:
            freshness_str = "\u2014"  # Manual-trigger, no SLA

        # Apply freshness filter
        if freshness_filter and freshness_filter != "All" and freshness_str != freshness_filter:
            continue

        rows.append(
            {
                "Name": card.get("name", card_id),
                "Type": _TYPE_LABELS.get(wf_type, wf_type),
                "Runtime": runtime_str,
                "Last Run": last_run_str,
                "Last Duration": duration_str,
                "Cost (30d)": cost_val,
                "Avg/Run": avg_run_val,
                "Freshness": freshness_str,
            }
        )
        card_ids.append(card_id)

    # Store parallel card ID list for row click mapping (not in the DataFrame)
    _wf_card_ids = card_ids

    if not rows:
        return pd.DataFrame(columns=pd.Index(_WF_TABLE_COLS))
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Detail section builders
# ---------------------------------------------------------------------------


def _build_badges_html(card: dict[str, Any]) -> RawHtml:
    """Status + type badges HTML."""
    status: str = card.get("status") or "draft"
    wf_type: str = card.get("type") or ""
    type_label = _TYPE_LABELS.get(wf_type, wf_type)
    type_color = _TYPE_COLORS.get(wf_type, "gray")
    status_color = _STATUS_COLORS.get(status, "gray")

    return RawHtml(
        f'<span class="ll-badge ll-badge-{status_color}">{html_escape(status)}</span> '
        f'<span class="ll-badge ll-badge-{type_color}">{html_escape(type_label)}</span>'
    )


def _build_data_flow_html(card: dict[str, Any]) -> RawHtml:
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


def _build_exec_html(card: dict[str, Any]) -> RawHtml:
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


def _build_monitoring_html(card: dict[str, Any]) -> RawHtml:
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


def _build_cost_html(
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


def _build_references_html(card: dict[str, Any]) -> RawHtml:
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


def _build_deps_html(card: dict[str, Any], all_cards: dict[str, dict[str, Any]]) -> RawHtml:
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


def _build_idempotency_html(card: dict[str, Any]) -> RawHtml:
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


def _build_source_html(card: dict[str, Any]) -> RawHtml:
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


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def wf_on_dag_click(state: Any, id: str, payload: dict[str, Any]) -> None:
    """DAG node clicked — switch to detail view for that workflow."""
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


def wf_on_table_action(state: Any, var_name: str, payload: dict[str, Any]) -> None:
    """Table row clicked — switch to detail view.

    Uses a hidden _wf_card_ids list (parallel to table rows) to map
    row index to card ID, avoiding a visible _card_id column in the table.
    """
    idx = payload.get("index", 0) if isinstance(payload, dict) else 0
    if 0 <= idx < len(_wf_card_ids):
        card_id = _wf_card_ids[idx]
        if card_id in _cards:
            _show_detail(state, card_id)


def wf_on_back_click(state: Any, id: str, payload: dict[str, Any]) -> None:
    """Back to dashboard. Signature matches Taipy on_action (state, id, payload)."""
    state.wf_selected_workflow = None


def wf_on_type_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Type filter changed — rebuild table."""
    _refresh_table(state)


def wf_on_runtime_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Runtime filter changed — rebuild table."""
    _refresh_table(state)


def wf_on_freshness_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Freshness filter changed — rebuild table."""
    _refresh_table(state)


# ---------------------------------------------------------------------------
# Detail population
# ---------------------------------------------------------------------------


def _show_detail(state: Any, workflow_id: str) -> None:
    """Populate all detail state variables for a workflow."""
    card = _cards.get(workflow_id, {})
    cold = _fetch_cold_costs()
    warm = _fetch_warm_costs()

    state.wf_selected_workflow = workflow_id
    state.wf_detail_title = card.get("name", workflow_id)
    state.wf_detail_badges_html = _build_badges_html(card)
    _dash = "\u2014"
    domain = card.get("domain", _dash)
    owners = ", ".join(card.get("owners", []))
    version = card.get("version", "?")
    state.wf_detail_meta = f"Domain: {domain} | Owner: {owners} | v{version}"
    state.wf_detail_overview = card.get("body", "").strip()
    state.wf_detail_data_flow_html = _build_data_flow_html(card)
    state.wf_detail_exec_html = _build_exec_html(card)
    state.wf_detail_monitoring_html = _build_monitoring_html(card)
    state.wf_detail_cost_html = _build_cost_html(card, cold, warm)
    state.wf_detail_references_html = _build_references_html(card)
    state.wf_detail_deps_html = _build_deps_html(card, _cards)
    state.wf_detail_idempotency_html = _build_idempotency_html(card)
    state.wf_detail_source_html = _build_source_html(card)


# ---------------------------------------------------------------------------
# Table refresh (used by filter callbacks)
# ---------------------------------------------------------------------------


def _filter_card_ids(
    cards: dict[str, dict[str, Any]],
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    runtime_filter: str | None,
    freshness_filter: str | None,
) -> set[str]:
    """Return card IDs that match all active filters."""
    matched: set[str] = set()
    for card_id, card in cards.items():
        wf_type = card.get("type", "")

        if type_filter and type_filter != "All" and _TYPE_LABELS.get(wf_type, wf_type) != type_filter:
            continue

        # Runtime
        exec_cfg = card.get("execution") or {}
        runtime_str = _classify_runtime(exec_cfg)
        if runtime_filter and runtime_filter != "All" and runtime_str != runtime_filter:
            continue

        # Freshness (needs job_runs)
        if freshness_filter and freshness_filter != "All":
            sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
            entry_point = (exec_cfg.get("inference") or {}).get("entry_point", "")
            job_run = job_runs.get(entry_point, {})
            last_run_ts = job_run.get("last_run")
            freshness = "\u2014"
            if sla_hours and last_run_ts is not None:
                age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
                freshness = _classify_freshness(age_hours, sla_hours)
            if freshness != freshness_filter:
                continue

        matched.add(card_id)
    return matched


def _refresh_table(state: Any) -> None:
    """Rebuild dashboard table AND DAG with current filters."""
    cold = _fetch_cold_costs()
    jobs = _fetch_job_runs()

    # Filter cards
    matched_ids = _filter_card_ids(
        _cards,
        jobs,
        state.wf_type_filter,
        state.wf_runtime_filter,
        state.wf_freshness_filter,
    )

    # Check if any filter is active
    all_filters_default = all(
        f in (None, "All") for f in (state.wf_type_filter, state.wf_runtime_filter, state.wf_freshness_filter)
    )

    if all_filters_default:
        # No filters — show full DAG
        state.wf_dag_html = _build_dag_html(_cards)
        state.wf_dag_height = f"{max(200, min(_DAG_MAX_HEIGHT_PX, len(_cards) * 50 + 80))}px"
    else:
        # Build filtered DAG: matched cards + their immediate neighbors for context
        dag_cards: dict[str, dict[str, Any]] = {}
        for card_id in matched_ids:
            dag_cards[card_id] = _cards[card_id]
            # Add upstream dependencies
            for dep_id in _cards[card_id].get("depends_on", []):
                if dep_id in _cards:
                    dag_cards[dep_id] = _cards[dep_id]
            # Add downstream dependents
            for other_id, other_card in _cards.items():
                if card_id in other_card.get("depends_on", []):
                    dag_cards[other_id] = other_card
        if dag_cards:
            state.wf_dag_html = _build_dag_html(dag_cards, highlight_ids=matched_ids)
            state.wf_dag_height = f"{max(200, min(_DAG_MAX_HEIGHT_PX, len(dag_cards) * 50 + 80))}px"
        else:
            state.wf_dag_html = RawHtml("")
            state.wf_dag_height = "0px"

    state.wf_table_data = _build_table_data(
        _cards,
        cold,
        jobs,
        state.wf_type_filter,
        state.wf_runtime_filter,
        state.wf_freshness_filter,
    )

    # Recompute stats for the filtered subset
    warm = _fetch_warm_costs()
    _compute_stats(state, cold, warm, jobs, visible_card_ids=matched_ids if not all_filters_default else None)


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------


def _compute_stats(
    state: Any,
    cold: pd.DataFrame,
    warm: pd.DataFrame,
    jobs: dict[str, dict[str, Any]],
    visible_card_ids: set[str] | None = None,
) -> None:
    """Compute stats bar metrics.

    When visible_card_ids is set, stats reflect only the filtered subset.
    """
    cards_subset = (
        {k: v for k, v in _cards.items() if k in visible_card_ids} if visible_card_ids is not None else _cards
    )
    state.wf_total_workflows = str(len(cards_subset))

    # Workflow type breakdown for detail line
    type_counts: dict[str, int] = {}
    for card in cards_subset.values():
        wf_type: str = card.get("type") or ""
        label: str = _TYPE_LABELS.get(wf_type, wf_type)
        type_counts[label] = type_counts.get(label, 0) + 1
    # Sort by count descending, format as colored HTML spans
    sorted_types = sorted(type_counts.items(), key=lambda x: (-x[1], x[0]))
    colored_parts = [
        f'<span style="color:{_TYPE_LABEL_COLORS.get(t, "#8b949e")}">{n} {html_escape(t)}</span>'
        for t, n in sorted_types
    ]
    state.wf_workflows_detail = _stat_detail_html(", ".join(colored_parts))

    # Total 30d cost — always scope to workflow card entry points.
    # The cold table includes all Databricks tasks (ingestion, etc.)
    # but the dashboard should only show workflow card costs.
    all_entry_points = {
        ((c.get("execution") or {}).get("inference") or {}).get("entry_point", "") for c in cards_subset.values()
    }
    all_entry_points.discard("")
    if not cold.empty and all_entry_points:
        cost_df = cold[cold["task_key"].isin(list(all_entry_points))]
    else:
        cost_df = pd.DataFrame()

    # Cost breakdown by runtime: Databricks (actual/cold) vs HF Jobs (estimated/projected)
    dbx_cost = float(cost_df["total_cost_usd"].sum()) if not cost_df.empty else 0.0

    # HF Jobs costs: warm tier (estimated from CostEstimateHook) or YAML projected
    hf_cost = 0.0
    # Collect HF Jobs entry points
    hf_entry_points: set[str] = set()
    for card in cards_subset.values():
        cost_cfg = card.get("cost") or {}
        for phase in ("training", "inference"):
            phase_cost = cost_cfg.get(phase)
            if phase_cost and phase_cost.get("runtime", "").lower() in ("hf-jobs", "hf_jobs", "hf jobs"):
                ep = ((card.get("execution") or {}).get(phase) or {}).get("entry_point", "")
                if ep:
                    hf_entry_points.add(ep)
    # Try warm tier first
    hf_warm_cost = 0.0
    if not warm.empty and hf_entry_points:
        hf_df = warm[warm["task_key"].isin(list(hf_entry_points))]
        if not hf_df.empty:
            hf_warm_cost = float(hf_df["estimated_cost_usd"].sum())
    # Fallback: YAML projected costs when warm tier has no data
    if hf_warm_cost > 0:
        hf_cost = hf_warm_cost
        hf_tier = "estimated"
    else:
        hf_tier = "projected"
        for card in cards_subset.values():
            cost_cfg = card.get("cost") or {}
            for phase in ("training", "inference"):
                phase_cost = cost_cfg.get(phase)
                if phase_cost and phase_cost.get("runtime", "").lower() in ("hf-jobs", "hf_jobs", "hf jobs"):
                    hf_cost += float(phase_cost.get("typical_cost_usd") or 0)

    total = dbx_cost + hf_cost
    state.wf_total_cost_30d = f"${total:.2f}"

    # Cost detail: breakdown by runtime with colored labels
    cost_parts: list[str] = []
    if dbx_cost > 0:
        cost_parts.append(f'${dbx_cost:.2f} <span style="color:{_RUNTIME_HEX["db"]}">DB</span> (actual)')
    if hf_cost > 0:
        cost_parts.append(f'${hf_cost:.2f} <span style="color:{_RUNTIME_HEX["hf"]}">HF</span> ({hf_tier})')
    state.wf_cost_detail = _stat_detail_html(" + ".join(cost_parts))

    # Freshness summary with status breakdown (matching mockup format)
    monitored = 0
    fresh_count = 0
    warning_count = 0
    stale_count = 0
    for _card_id, card in cards_subset.items():
        sla = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla is None:
            continue  # No SLA = not monitored, skip
        monitored += 1
        entry_point = ((card.get("execution") or {}).get("inference") or {}).get("entry_point", "")
        run = jobs.get(entry_point, {})
        last_run = run.get("last_run")
        if last_run is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run).total_seconds() / 3600
            status = _classify_freshness(age_hours, sla)
            if status == "OK":
                fresh_count += 1
            elif status == "Warning":
                warning_count += 1
            else:
                stale_count += 1
        else:
            stale_count += 1  # Never run = stale
    if monitored > 0:
        state.wf_freshness_summary = f"{fresh_count}/{monitored} within SLA"
        # Detail: breakdown of non-OK statuses with colored labels
        detail_parts: list[str] = []
        if warning_count:
            detail_parts.append(f'<span style="color:{_FRESHNESS_HEX["warning"]}">{warning_count} warning</span>')
        if stale_count:
            detail_parts.append(f'<span style="color:{_FRESHNESS_HEX["stale"]}">{stale_count} stale</span>')
        state.wf_freshness_detail = _stat_detail_html(" \u2014 ".join(detail_parts))
    else:
        state.wf_freshness_summary = "No SLAs configured"
        state.wf_freshness_detail = RawHtml("")

    # Run volume: total runs from cold costs (Databricks billing)
    num_runs = int(cost_df["run_count"].sum()) if not cost_df.empty and "run_count" in cost_df.columns else 0
    state.wf_run_volume = str(num_runs)
    if num_runs > 0:
        daily_rate = num_runs / 30
        avg_cost = total / num_runs
        detail_parts = []
        if daily_rate >= 1:
            detail_parts.append(f"~{daily_rate:.0f}/day")
        else:
            detail_parts.append(f"~{daily_rate:.1f}/day")
        detail_parts.append(f"${avg_cost:.2f} avg/run")
        state.wf_run_volume_detail = " \u00b7 ".join(detail_parts)
    else:
        state.wf_run_volume_detail = ""


# ---------------------------------------------------------------------------
# Main refresh (page entry point)
# ---------------------------------------------------------------------------


def wf_refresh(state: Any) -> None:
    """Page entry point — loads cards, queries costs, builds dashboard."""
    global _cards

    _cards = _load_cards_from_yaml()
    if not _cards:
        logger.warning("No workflow cards loaded")
        state.wf_no_cards_warning = "No workflow cards loaded. Check that the workflow-cards/ directory is available."
        return
    state.wf_no_cards_warning = ""  # Clear on successful load
    state.wf_cards_loaded = True

    # Build filter LOVs from card metadata
    types = sorted({c.get("type", "") for c in _cards.values()})
    state.wf_type_lov = ["All"] + [_TYPE_LABELS.get(tp, tp) for tp in types]
    state.wf_type_filter = "All"

    # Build runtime LOV from card execution config
    runtime_values: set[str] = set()
    for c in _cards.values():
        exec_cfg = c.get("execution") or {}
        rt_str = _classify_runtime(exec_cfg)
        if rt_str != "\u2014":
            runtime_values.add(rt_str)
    state.wf_runtime_lov = ["All"] + sorted(runtime_values)
    state.wf_runtime_filter = "All"

    # Build DAG
    state.wf_dag_html = _build_dag_html(_cards)

    # Query costs + job runs
    cold = _fetch_cold_costs()
    warm = _fetch_warm_costs()
    jobs = _fetch_job_runs()

    # Build freshness LOV from computed freshness values (needs jobs data)
    freshness_values: set[str] = set()
    for c in _cards.values():
        sla_hours = (c.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours is None:
            continue
        ep = ((c.get("execution") or {}).get("inference") or {}).get("entry_point", "")
        run = jobs.get(ep, {})
        last_run = run.get("last_run")
        if last_run is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run).total_seconds() / 3600
            freshness_values.add(_classify_freshness(age_hours, sla_hours))
    state.wf_freshness_lov = ["All"] + sorted(freshness_values)
    state.wf_freshness_filter = "All"

    # Build table
    state.wf_table_data = _build_table_data(_cards, cold, jobs, "All", "All", "All")

    # Stats (uses jobs for freshness, cold for cost, warm for live runs)
    _compute_stats(state, cold, warm, jobs)

    # Clear detail state (dashboard mode)
    state.wf_selected_workflow = None

    logger.info("Workflows page loaded: %d cards, %d cost rows", len(_cards), len(cold))


register_page_refresher("AI-ML-Workflows", wf_refresh, is_dashboard=True)
