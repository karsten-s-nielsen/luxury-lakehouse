# AI/ML Workflows Page Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Taipy "AI/ML Workflows" page — Cytoscape.js DAG, sortable workflow table, cost transparency, 8-section detail drilldown — under a new "Operations" nav section with role-based access foundations. Includes Lakebase synced table infrastructure for cost data.

**Architecture:** Hand-crafted Taipy markdown (not `build_page()`) for a layout that differs fundamentally from analytics pages. Card metadata loaded from YAML files at app startup. Cost data queried from Lakebase synced tables (cold tier: `fct_workflow_costs_synced`, warm tier: `workflow_cost_live_synced`). Cytoscape.js with dagre layout embedded via Taipy `<|{var}|text|raw|>` block (Taipy's syntax for raw HTML rendering). `NavSection` dataclass replaces `nav_section` strings across all pages, carrying a `role` field for future admin gating.

**Tech Stack:** Taipy 4.0, Cytoscape.js 3.x + cytoscape-dagre 2.x (CDN), PyYAML (already a Taipy dep), psycopg2 (existing `db.py`), Terraform (Databricks synced tables), PostgreSQL (Lakebase)

**Key design decisions:**
- "Operations" nav section (extensible for future Observability pages)
- All workflows + ingestion pipelines unified — the 16 workflow cards already cover both
- Phase 1: read-only, `role="viewer"` for all. Phase 2: `role="admin"` for trigger/configure actions
- Backend enforces role independently of UI (never trust client state)
- No ops template yet (YAGNI) — extract when second ops page is added
- Cards parsed from YAML at startup, not from wheel import (no heavy dependencies on HF Spaces)
- Cost section included in scope; auto-refresh + visual state indicators deferred to Task 15D

**Commit strategy:** No commits without explicit user approval AND successful E2E test. Local server + Puppeteer verification is expected before any commit request. Logical commit points: (1) NavSection refactor, (2) all workflows page code + infrastructure.

---

## File Map

### New Files
| File | Responsibility |
|------|---------------|
| `hf_taipy_app/src/state/workflows.py` | State vars (`wf_` prefix), card loading, DAG HTML, table data, detail data, cost SQL, callbacks |
| `hf_taipy_app/src/pages/workflows.py` | `PageConfig` + hand-crafted Taipy markdown (dashboard + detail views) |

### Modified Files
| File | Changes |
|------|---------|
| `hf_taipy_app/src/page_template.py` | Add `NavSection` dataclass + 4 constants, update `PageConfig.nav_section` type, update `build_nav()` |
| `hf_taipy_app/src/pages/*.py` (12 files) | Replace `nav_section="..."` with `NavSection` constant references |
| `hf_taipy_app/src/main.py` | Import + register workflows page in `PAGE_REGISTRY` |
| `hf_taipy_app/src/template.py` | `GLOSSARY` additions, `PAGE_TERMS["AI-ML-Workflows"]`, `_WF_PAGES` visibility tuple |
| `hf_taipy_app/src/style_v2.css` | Ops page CSS: stats bar, type/status badges, DAG container, detail panel, data flow, exec cards, monitoring bars, cost cards, reference badges |
| `hf_taipy_app/src/db.py` | Extend `t()` with optional `schema` parameter |
| `hf_taipy_app/src/config.py` | Add `observability_schema` setting |
| `scripts/deploy_taipy.py` | Pre-upload step: copy `workflow-cards/` into app staging |
| `terraform/modules/synced_tables/main.tf` | 2 new synced table resources |
| `terraform/modules/synced_tables/variables.tf` | Add `observability_schema` variable |
| `terraform/environments/dev/main.tf` | Pass `observability_schema` to module |
| `scripts/create_indexes.py` | Indexes for `fct_workflow_costs_synced` + `workflow_cost_live_synced` |
| `scripts/lakebase_grants.sql` | Grants for `observability` PG schema |

---

## Task 1: NavSection Dataclass + Page Migrations

**Files:**
- Modify: `hf_taipy_app/src/page_template.py`
- Modify: `hf_taipy_app/src/pages/shot_map.py`, `pass_map.py`, `heat_map.py`, `pass_network.py`, `match_summary.py`, `action_values.py`, `player_radar.py`, `player_similarity.py`, `movement_analysis.py`, `pitch_control.py`, `pass_timing.py`, `defensive_valuation.py`

This task implements Deferred Decision #1 from `project_taipy_deferred_design.md`: replace `PageConfig.nav_section: str` with a `NavSection` dataclass carrying a `role` field.

- [ ] **Step 1: Add NavSection dataclass to page_template.py**

Add after the `SidebarWidget` dataclass, before `PageConfig`:

```python
@dataclass(frozen=True)
class NavSection:
    """Navigation section grouping with role-based visibility.

    Phase 1: all sections use role="viewer" (visible to everyone).
    Phase 2: admin-only sections use role="admin" and build_nav()
    filters by authenticated user role.
    """

    name: str
    icon: str = ""
    role: str = "viewer"


# -- Section constants (imported by page files) ----------------------------
NAV_MATCH_ANALYSIS = NavSection("Match Analysis")
NAV_PLAYER_ANALYSIS = NavSection("Player Analysis")
NAV_ADVANCED = NavSection("Advanced")
NAV_OPERATIONS = NavSection("Operations")
```

- [ ] **Step 2: Update PageConfig.nav_section type**

In the `PageConfig` dataclass, change:

```python
# Before:
nav_section: str
# After:
nav_section: NavSection
```

- [ ] **Step 3: Update build_nav() to use NavSection.name**

In `build_nav()`, the section grouping currently uses the raw string. Change to use `.name`:

```python
# Before (in build_nav):
sections.setdefault(entry.config.nav_section, []).append(entry)
# After:
sections.setdefault(entry.config.nav_section.name, []).append(entry)
```

- [ ] **Step 4: Export NavSection and constants from page_template.py**

Ensure `NavSection`, `NAV_MATCH_ANALYSIS`, `NAV_PLAYER_ANALYSIS`, `NAV_ADVANCED`, `NAV_OPERATIONS` are importable. They are already at module level — no `__all__` in this file.

- [ ] **Step 5: Migrate all 12 page files**

Each page file changes its import and `nav_section` value. The pattern:

```python
# Before (e.g., shot_map.py):
from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

page_config = PageConfig(
    ...
    nav_section="Match Analysis",
    ...
)

# After:
from page_template import (
    NAV_MATCH_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    Metric,
    PageConfig,
    build_page,
)

page_config = PageConfig(
    ...
    nav_section=NAV_MATCH_ANALYSIS,
    ...
)
```

Mapping:
| Section | Pages | Constant |
|---------|-------|----------|
| Match Analysis | shot_map, pass_map, heat_map, pass_network, match_summary | `NAV_MATCH_ANALYSIS` |
| Player Analysis | action_values, player_radar, player_similarity | `NAV_PLAYER_ANALYSIS` |
| Advanced | movement_analysis, pitch_control, pass_timing, defensive_valuation | `NAV_ADVANCED` |

- [ ] **Step 6: Verify — lint + type check + local run**

```bash
cd hf_taipy_app && uv run ruff check src/ && uv run ruff format --check src/
```

Visually confirm nav sections render correctly (section headers unchanged).

---

## Task 2: Infrastructure — Synced Tables + Grants + Indexes

**Files:**
- Modify: `terraform/modules/synced_tables/main.tf`
- Modify: `terraform/modules/synced_tables/variables.tf`
- Modify: `terraform/environments/dev/main.tf`
- Modify: `scripts/create_indexes.py`
- Modify: `scripts/lakebase_grants.sql`

**Prerequisites:** `fct_workflow_costs` must exist in `soccer_analytics.dev_gold` (built by dbt). `workflow_cost_live` must exist in `soccer_analytics.observability` (created by `scripts/create_cost_table.sql`).

- [ ] **Step 1: Add observability_schema variable**

In `terraform/modules/synced_tables/variables.tf`, add:

```hcl
variable "observability_schema" {
  description = "Observability schema name in the catalog (for workflow_cost_live)"
  type        = string
  default     = "observability"
}
```

- [ ] **Step 2: Add synced table resources**

In `terraform/modules/synced_tables/main.tf`, add at the end. **Follow the existing resource format exactly** (`name`, `database_instance_name`, `logical_database_name`, `spec` block with `source_table_full_name`, `primary_key_columns`, `scheduling_policy`):

```hcl
# ── Cost / Observability tables ──────────────────────────────────────────

resource "databricks_database_synced_database_table" "fct_workflow_costs" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_workflow_costs_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_workflow_costs"
    primary_key_columns    = ["task_key", "usage_date", "job_run_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "workflow_cost_live" {
  name                   = "${var.catalog_name}.${var.observability_schema}.workflow_cost_live_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.observability_schema}.workflow_cost_live"
    primary_key_columns    = ["run_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}
```

**Important:** As noted in the module header, synced tables for Lakebase Autoscaling must be created via the Databricks UI first, then imported into Terraform state:
```bash
terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_workflow_costs' \
  'soccer_analytics.dev_gold.fct_workflow_costs_synced'
terraform import 'module.synced_tables.databricks_database_synced_database_table.workflow_cost_live' \
  'soccer_analytics.observability.workflow_cost_live_synced'
```

- [ ] **Step 3: Pass observability_schema to module**

In `terraform/environments/dev/main.tf`, update the `synced_tables` module call:

```hcl
module "synced_tables" {
  source = "../../modules/synced_tables"

  catalog_name           = module.workspace.catalog_name
  database_instance_name = module.lakebase.instance_name
  environment            = var.environment
  gold_schema            = "${var.environment}_gold"
  observability_schema   = "observability"
}
```

- [ ] **Step 4: Apply Terraform**

```bash
cd terraform/environments/dev
terraform plan -target=module.synced_tables
terraform apply -target=module.synced_tables
```

Wait for synced tables to reach ACTIVE status (check Databricks UI → Catalog → synced tables).

- [ ] **Step 5: Add observability schema grants**

Append to `scripts/lakebase_grants.sql`:

```sql
-- ── Observability schema grants ──────────────────────────────────────────
GRANT USAGE ON SCHEMA observability TO :app_sp_uuid;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO :app_sp_uuid;
ALTER DEFAULT PRIVILEGES IN SCHEMA observability
    GRANT SELECT ON TABLES TO :app_sp_uuid;
```

Run the grants:
```bash
psql -v app_sp_uuid="'<sp-uuid>'" -h <lakebase-host> -U <admin> -d databricks_postgres -f scripts/lakebase_grants.sql
```

- [ ] **Step 6: Add indexes for cost tables**

In `scripts/create_indexes.py`:

**a) Add to `INDEXES` list** (uses `dev_gold` schema — same as existing):
```python
# -- fct_workflow_costs_synced — ~1K rows (Workflows page cost column) ──
("idx_wf_costs_task_key", "fct_workflow_costs_synced", "task_key"),
("idx_wf_costs_task_date", "fct_workflow_costs_synced", "task_key, usage_date"),
```

**b) Add new `OBSERVABILITY_INDEXES` list + loop** for `observability` PG schema:
```python
OBSERVABILITY_SCHEMA = "observability"

OBSERVABILITY_INDEXES: list[tuple[str, str, str]] = [
    ("idx_wf_live_wf_id", "workflow_cost_live_synced", "workflow_id"),
    ("idx_wf_live_wf_state", "workflow_cost_live_synced", "workflow_id, state"),
]
```

In the `_create_indexes()` function, after the existing loop over `INDEXES`, add a second loop:
```python
# Observability schema indexes
for idx_name, table, columns in OBSERVABILITY_INDEXES:
    fqn = f"{OBSERVABILITY_SCHEMA}.{table}"
    sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {fqn} ({columns})"
    _execute(conn, sql)
```

Note: The tuple format is `(name: str, table: str, columns: str)` — columns is a comma-separated string, **not a list**. This matches the existing `INDEXES` entries (e.g., `"task_key, usage_date"`).

- [ ] **Step 7: Run indexes + verify**

```bash
python scripts/create_indexes.py
python scripts/create_indexes.py --verify
```

Add a verify query for the cost tables:
```sql
EXPLAIN ANALYZE SELECT task_key, SUM(attributed_cost_usd) FROM dev_gold.fct_workflow_costs_synced WHERE task_key = 'compute_pitch_control' GROUP BY task_key;
```

Confirm Index Scan (not Seq Scan).

---

## Task 3: App Foundation — Config + DB + Deploy

**Files:**
- Modify: `hf_taipy_app/src/config.py`
- Modify: `hf_taipy_app/src/db.py`
- Modify: `scripts/deploy_taipy.py`

- [ ] **Step 1: Add observability_schema to AppSettings**

In `config.py`, add field to `AppSettings`:

```python
observability_schema: str = "observability"
```

Add to the identifier validator (same pattern as `unity_catalog` and `gold_schema`).

- [ ] **Step 2: Extend t() with optional schema parameter**

In `db.py`, update `t()`:

```python
def t(name: str, schema: str | None = None) -> str:
    """Build qualified table reference: {schema}.{name}.

    Args:
        name: Table name (validated as safe identifier).
        schema: PG schema override. Defaults to gold_schema from settings.
    """
    settings = get_settings()
    prefix = schema if schema is not None else settings.pg_schema_prefix
    validate_table_name(name)
    if schema is not None:
        validate_table_name(schema)
    return f"{prefix}.{name}"
```

Existing callers (`t("fct_shots_synced")`) are unchanged — `schema=None` falls through to the default.

- [ ] **Step 3: Modify deploy script to bundle workflow-cards**

In `scripts/deploy_taipy.py`, add a pre-upload step. In the `_deploy()` function, after the pre-flight checks but before `upload_folder()`:

```python
import shutil

# Bundle workflow-cards for the HF Space (card YAML files needed at runtime)
cards_src = Path(__file__).parent.parent / "workflow-cards"
cards_dst = Path(__file__).parent.parent / "hf_taipy_app" / "workflow-cards"
if cards_src.is_dir():
    if cards_dst.exists():
        shutil.rmtree(cards_dst)
    shutil.copytree(cards_src, cards_dst)
    logger.info("Bundled %d workflow cards", len(list(cards_dst.glob("*.yaml"))))
```

Wrap the copy + upload + cleanup in a `try/finally` to ensure cleanup even on crash:

```python
try:
    # ... existing upload_folder() call ...
    pass
finally:
    # Clean up bundled workflow-cards (not committed to hf_taipy_app/)
    if cards_dst.exists():
        shutil.rmtree(cards_dst)
        logger.info("Cleaned up bundled workflow-cards")
```

Add `workflow-cards/` to `hf_taipy_app/.gitignore` (create if needed) so the temp copy is never committed.

- [ ] **Step 4: Verify deploy dry-run**

```bash
python scripts/deploy_taipy.py staging --dry-run
```

Confirm workflow-cards are listed in the upload manifest.

---

## Task 4: CSS — Operations Page Styles

**Files:**
- Modify: `hf_taipy_app/src/style_v2.css`

Add a new section at the end of `style_v2.css`. These classes are used by the Taipy markdown in the page file.

- [ ] **Step 1: Add operations page CSS**

```css
/* ===== 9. Operations pages (AI/ML Workflows) ===== */

/* Stats bar — horizontal row of summary cards */
.ll-stats-bar {
    display: flex !important;
    gap: 1rem !important;
    margin-bottom: 1.5rem !important;
    flex-wrap: wrap !important;
}

.ll-stat-card {
    flex: 1 1 200px !important;
    background: rgba(255, 255, 255, 0.04) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 8px !important;
    padding: 1rem !important;
}

.ll-stat-card h3 {
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    color: #ffffff !important;
    margin: 0 0 0.25rem 0 !important;
}

.ll-stat-label {
    font-size: 0.75rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
    color: rgba(255, 255, 255, 0.5) !important;
    margin: 0 !important;
}

.ll-stat-detail {
    font-size: 0.8rem !important;
    color: rgba(255, 255, 255, 0.6) !important;
    margin-top: 0.25rem !important;
}

/* DAG container */
.ll-dag-container {
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 8px !important;
    margin-bottom: 1.5rem !important;
    overflow: hidden !important;
    background: rgba(255, 255, 255, 0.02) !important;
}

/* Type badges — pill-shaped colored labels */
.ll-badge {
    display: inline-block !important;
    padding: 2px 8px !important;
    border-radius: 12px !important;
    font-size: 0.7rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.03em !important;
    white-space: nowrap !important;
}

.ll-badge-blue { background: rgba(88, 166, 255, 0.15) !important; color: #58a6ff !important; }
.ll-badge-purple { background: rgba(188, 140, 255, 0.15) !important; color: #bc8cff !important; }
.ll-badge-teal { background: rgba(63, 185, 160, 0.15) !important; color: #3fb9a0 !important; }
.ll-badge-amber { background: rgba(227, 179, 65, 0.15) !important; color: #e3b341 !important; }
.ll-badge-gray { background: rgba(110, 118, 129, 0.15) !important; color: #8b949e !important; }
.ll-badge-green { background: rgba(63, 185, 80, 0.15) !important; color: #3fb950 !important; }
.ll-badge-red { background: rgba(248, 81, 73, 0.15) !important; color: #f85149 !important; }

/* Status dots (freshness indicators) */
.ll-status-dot {
    display: inline-block !important;
    width: 8px !important;
    height: 8px !important;
    border-radius: 50% !important;
    margin-right: 0.4rem !important;
    vertical-align: middle !important;
}

.ll-dot-green { background: #3fb950 !important; }
.ll-dot-yellow { background: #d29922 !important; }
.ll-dot-red { background: #f85149 !important; }
.ll-dot-gray { background: #6e7681 !important; }

/* Runtime icons */
.ll-runtime {
    display: inline-block !important;
    font-size: 0.7rem !important;
    padding: 1px 6px !important;
    border-radius: 3px !important;
    font-weight: 600 !important;
    margin-right: 0.25rem !important;
}

.ll-runtime-dbx { background: rgba(255, 63, 52, 0.15) !important; color: #ff6b6b !important; }
.ll-runtime-hf { background: rgba(255, 208, 55, 0.15) !important; color: #ffd037 !important; }

/* Detail panel */
.ll-detail-header {
    margin-bottom: 1.5rem !important;
}

.ll-back-link {
    font-size: 0.85rem !important;
    color: var(--color-primary) !important;
    cursor: pointer !important;
    margin-bottom: 0.5rem !important;
}

.ll-detail-meta {
    font-size: 0.85rem !important;
    color: rgba(255, 255, 255, 0.6) !important;
    margin-top: 0.25rem !important;
}

/* Detail sections */
.ll-detail-section {
    margin-bottom: 2rem !important;
    padding-bottom: 1.5rem !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.06) !important;
}

.ll-detail-section:last-child {
    border-bottom: none !important;
}

.ll-section-title {
    font-size: 1.1rem !important;
    font-weight: 600 !important;
    color: rgba(255, 255, 255, 0.9) !important;
    margin-bottom: 0.75rem !important;
}

/* Data flow — three-column layout */
.ll-data-flow {
    display: grid !important;
    grid-template-columns: 1fr auto 1fr !important;
    gap: 1rem !important;
    align-items: start !important;
}

.ll-data-flow-center {
    display: flex !important;
    flex-direction: column !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 1rem !important;
    color: var(--color-primary) !important;
    font-size: 0.85rem !important;
    font-weight: 600 !important;
}

.ll-data-chip {
    display: inline-block !important;
    padding: 4px 10px !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    font-size: 0.8rem !important;
    margin: 2px !important;
    color: rgba(255, 255, 255, 0.8) !important;
}

.ll-data-chip a {
    color: var(--color-primary) !important;
    text-decoration: none !important;
}

/* Execution cards — side-by-side */
.ll-exec-grid {
    display: grid !important;
    grid-template-columns: 1fr 1fr !important;
    gap: 1rem !important;
}

.ll-exec-card {
    background: rgba(255, 255, 255, 0.03) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 8px !important;
    padding: 1rem !important;
}

.ll-exec-card h4 {
    font-size: 0.95rem !important;
    font-weight: 600 !important;
    margin-bottom: 0.5rem !important;
}

.ll-exec-row {
    display: flex !important;
    justify-content: space-between !important;
    font-size: 0.85rem !important;
    padding: 0.25rem 0 !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.04) !important;
}

.ll-exec-label {
    color: rgba(255, 255, 255, 0.5) !important;
}

/* Cost cards */
.ll-cost-grid {
    display: grid !important;
    grid-template-columns: 1fr 1fr !important;
    gap: 1rem !important;
}

.ll-cost-card {
    background: rgba(255, 255, 255, 0.03) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 8px !important;
    padding: 1rem !important;
}

.ll-cost-big {
    font-size: 1.5rem !important;
    font-weight: 700 !important;
    font-family: monospace !important;
}

.ll-cost-source {
    display: inline-block !important;
    font-size: 0.65rem !important;
    padding: 1px 6px !important;
    border-radius: 3px !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
}

.ll-cost-actual { background: rgba(63, 185, 80, 0.15) !important; color: #3fb950 !important; }
.ll-cost-estimated { background: rgba(227, 179, 65, 0.15) !important; color: #e3b341 !important; }
.ll-cost-projected { background: rgba(110, 118, 129, 0.15) !important; color: #8b949e !important; }

/* Reference list */
.ll-ref-item {
    padding: 0.5rem 0 !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.04) !important;
}

.ll-ref-role {
    display: inline-block !important;
    font-size: 0.65rem !important;
    padding: 1px 6px !important;
    border-radius: 3px !important;
    background: rgba(188, 140, 255, 0.15) !important;
    color: #bc8cff !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    margin-right: 0.5rem !important;
}

/* Monitoring bars (inline progress indicators) */
.ll-mon-bar {
    height: 6px !important;
    background: rgba(255, 255, 255, 0.1) !important;
    border-radius: 3px !important;
    overflow: hidden !important;
    width: 100px !important;
    display: inline-block !important;
    vertical-align: middle !important;
    margin-left: 0.5rem !important;
}

.ll-mon-bar-fill {
    height: 100% !important;
    border-radius: 3px !important;
    transition: width 0.3s ease !important;
}

/* Source code links */
.ll-source-link {
    font-family: monospace !important;
    font-size: 0.85rem !important;
    color: rgba(255, 255, 255, 0.7) !important;
    display: block !important;
    padding: 0.2rem 0 !important;
}

/* Clickable table rows */
.ll-clickable-row {
    cursor: pointer !important;
}

/* Dashboard filter bar */
.ll-filter-bar {
    display: flex !important;
    gap: 1rem !important;
    margin-bottom: 1rem !important;
    align-items: center !important;
}
```

---

## Task 5: Workflows State Module

**Files:**
- Create: `hf_taipy_app/src/state/workflows.py`

This is the core logic. The module follows the same pattern as `state/defensive_valuation.py`: prefixed variables, `__all__` exports, `register_page_refresher`, `@ttl_cache` queries, internal lookup maps.

- [ ] **Step 1: Create state module scaffold with all state variables**

```python
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
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from cache import ttl_cache
from config import get_settings
from db import execute_query, t

from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n?(.*)", re.DOTALL)

_TYPE_COLORS: dict[str, str] = {
    "training-and-inference": "blue",
    "training": "blue",
    "inference": "blue",
    "grid-computation": "purple",
    "heuristic": "teal",
    "validation": "amber",
    "augmentation": "gray",
}

_TYPE_LABELS: dict[str, str] = {
    "training-and-inference": "Train+Infer",
    "training": "Training",
    "inference": "Inference",
    "grid-computation": "Grid Compute",
    "heuristic": "Heuristic",
    "validation": "Validation",
    "augmentation": "Augmentation",
}

# ---------------------------------------------------------------------------
# Dashboard state
# ---------------------------------------------------------------------------
wf_selected_workflow: str | None = None  # None = dashboard, set = detail view

wf_dag_html: str = ""

wf_total_workflows: str = "0"
wf_freshness_summary: str = "—"
wf_total_cost_30d: str = "$0.00"
wf_cost_detail: str = ""
wf_last_full_run: str = "—"
wf_last_full_run_detail: str = ""

_WF_TABLE_COLS = [
    "Name", "Type", "Status", "Domain", "Runtime",
    "Last Run", "Duration", "Cost (30d)", "Freshness",
]
wf_table_data: pd.DataFrame = pd.DataFrame(columns=_WF_TABLE_COLS)

wf_type_filter: str | None = "All"
wf_type_lov: list[str] = ["All"]
wf_status_filter: str | None = "All"
wf_status_lov: list[str] = ["All"]

# ---------------------------------------------------------------------------
# Detail state
# ---------------------------------------------------------------------------
wf_detail_title: str = ""
wf_detail_badges_html: str = ""
wf_detail_meta: str = ""
wf_detail_overview: str = ""
wf_detail_data_flow_html: str = ""
wf_detail_exec_html: str = ""
wf_detail_monitoring_html: str = ""
wf_detail_cost_html: str = ""
wf_detail_references_html: str = ""
wf_detail_deps_html: str = ""
wf_detail_idempotency_html: str = ""
wf_detail_source_html: str = ""

# Admin (Phase 2 foundation)
wf_is_admin: bool = False

# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------
_cards: dict[str, dict[str, Any]] = {}
_cost_by_task: dict[str, float] = {}  # task_key -> 30d cost USD
_wf_card_ids: list[str] = []  # Parallel to wf_table_data rows — maps row index to card ID

__all__ = [
    # Dashboard
    "wf_selected_workflow",
    "wf_dag_html",
    "wf_total_workflows",
    "wf_freshness_summary",
    "wf_total_cost_30d",
    "wf_cost_detail",
    "wf_last_full_run",
    "wf_last_full_run_detail",
    "wf_table_data",
    "wf_type_filter",
    "wf_type_lov",
    "wf_status_filter",
    "wf_status_lov",
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
    "wf_on_status_filter",
    "wf_on_table_action",
    "wf_refresh",
]
```

- [ ] **Step 2: Card loading function**

```python
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
```

- [ ] **Step 3: DAG HTML generation with Cytoscape.js**

```python
def _build_dag_html(cards: dict[str, dict[str, Any]]) -> str:
    """Generate Cytoscape.js DAG visualization as embeddable HTML.

    Uses dagre layout for automatic left-to-right tier placement.
    Nodes colored by workflow type. Edges from depends_on.
    Click events call back to Python via taipy.gui.invoke_callback.
    """
    # Build nodes
    nodes = []
    for card_id, card in cards.items():
        wf_type = card.get("type", "inference")
        color = _TYPE_COLORS.get(wf_type, "gray")
        label = card.get("name", card_id)
        # Truncate long names for display
        if len(label) > 20:
            label = label[:18] + "..."
        nodes.append({
            "data": {
                "id": card_id,
                "label": label,
                "type": wf_type,
                "color": color,
                "status": card.get("status", "draft"),
            },
        })

    # Build edges from depends_on
    edges = []
    for card_id, card in cards.items():
        for dep_id in card.get("depends_on", []):
            if dep_id in cards:
                edges.append({
                    "data": {"source": dep_id, "target": card_id},
                })

    elements_json = json.dumps(nodes + edges)

    # Color map for Cytoscape styles
    color_map = {
        "blue": "#58a6ff",
        "purple": "#bc8cff",
        "teal": "#3fb9a0",
        "amber": "#e3b341",
        "gray": "#6e7681",
    }
    color_styles = "\n".join(
        f"        {{ selector: 'node[color = \"{k}\"]', style: {{ "
        f"'background-color': '{v}', 'border-color': '{v}' }} }},"
        for k, v in color_map.items()
    )

    return f"""<div id="wf-cy" style="width:100%; height:400px; background:rgba(0,0,0,0.15); border-radius:8px;"></div>
<script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>
<script src="https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"></script>
<script src="https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"></script>
<script>
(function() {{
    if (typeof cytoscape === 'undefined') return;
    var cy = cytoscape({{
        container: document.getElementById('wf-cy'),
        elements: {elements_json},
        layout: {{
            name: 'dagre',
            rankDir: 'LR',
            nodeSep: 40,
            rankSep: 80,
            padding: 30,
        }},
        style: [
            {{ selector: 'node', style: {{
                'label': 'data(label)',
                'text-valign': 'center',
                'text-halign': 'center',
                'font-size': '11px',
                'color': '#e6edf3',
                'text-outline-color': '#1a1d24',
                'text-outline-width': 2,
                'width': 140,
                'height': 40,
                'shape': 'roundrectangle',
                'border-width': 2,
                'background-opacity': 0.2,
            }} }},
            {{ selector: 'node[status = "deprecated"]', style: {{
                'border-style': 'dashed',
                'opacity': 0.6,
            }} }},
            {{ selector: 'node[status = "draft"]', style: {{
                'border-style': 'dotted',
                'opacity': 0.5,
            }} }},
{color_styles}
            {{ selector: 'edge', style: {{
                'width': 1.5,
                'line-color': '#6e7681',
                'target-arrow-color': '#6e7681',
                'target-arrow-shape': 'triangle',
                'curve-style': 'bezier',
                'arrow-scale': 0.8,
            }} }},
        ],
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false,
    }});

    // Click handler — calls Python callback
    cy.on('tap', 'node', function(evt) {{
        var nodeId = evt.target.id();
        // Taipy callback mechanism
        if (window.taipy && window.taipy.socket) {{
            window.taipy.socket.emit('message', {{
                type: 'A',
                name: 'on_action',
                payload: {{ action: 'wf_on_dag_click', args: [nodeId] }},
            }});
        }}
    }});
}})();
</script>"""
```

**CRITICAL: Do NOT abandon Cytoscape.js without explicit user approval.** Exhaust all embedding approaches (inline `<script>`, iframe with `srcdoc`, Taipy `page` property, static HTML file served via Taipy) before escalating.

**Test the JS→Python callback early.** The `window.taipy.socket.emit` approach is undocumented internal API. If it doesn't work, use this **guaranteed fallback** — a hidden Taipy selector bound to a Python callback:

```python
# In state module: hidden state var for DAG click
wf_dag_selected: str | None = None  # Hidden selector, updated by JS

# In page markdown:
# <|{wf_dag_selected}|selector|lov={wf_dag_node_lov}|dropdown|on_change=wf_on_dag_select|class_name=ll-hidden|>
# CSS: .ll-hidden { display: none !important; }
```

The JS in the DAG HTML can set a hidden `<select>` element's value, which triggers the Taipy callback. This avoids needing internal Taipy socket API.

**Fallback if `<|text|raw|>` doesn't execute `<script>` tags:** Convert to a Plotly network graph using `go.Scatter` for nodes (mode="markers+text") and edges (mode="lines"), rendered with `<|{wf_dag_figure}|chart|>`. Use dagre-style manual layout: topological sort → assign layers → spread within layer. Less polished but guaranteed to work with Taipy's native Plotly support.

**Security:** Always call `validate_param_id(workflow_id)` from `db.py` before using a workflow ID received from JavaScript callbacks. Even though it's only used as a dict key, defense-in-depth is required per CLAUDE.md.

- [ ] **Step 4: Cost query functions**

```python
@ttl_cache()
def _fetch_cold_costs() -> pd.DataFrame:
    """30-day aggregated costs from fct_workflow_costs_synced (cold tier).

    Returns DataFrame with columns: task_key, total_cost_usd, total_dbu.
    """
    tbl = t("fct_workflow_costs_synced")
    try:
        return execute_query(
            f"SELECT task_key, "  # noqa: S608
            f"  SUM(attributed_cost_usd) AS total_cost_usd, "
            f"  SUM(attributed_dbu) AS total_dbu "
            f"FROM {tbl} "
            f"WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days' "
            f"GROUP BY task_key "
            f"ORDER BY total_cost_usd DESC "
            f"LIMIT 100",
        )
    except Exception:
        logger.warning("Cold cost query failed — table may not be synced yet")
        return pd.DataFrame(columns=["task_key", "total_cost_usd", "total_dbu"])


@ttl_cache()
def _fetch_warm_costs() -> pd.DataFrame:
    """Recent cost estimates from workflow_cost_live_synced (warm tier).

    Returns DataFrame with columns: workflow_id, state, duration_seconds,
    estimated_cost_usd, started_at, ended_at, task_key.
    """
    settings = get_settings()
    tbl = t("workflow_cost_live_synced", schema=settings.observability_schema)
    try:
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
        logger.warning("Warm cost query failed — table may not be synced yet")
        return pd.DataFrame()
```

- [ ] **Step 4b: Jobs API — last run + duration + freshness**

The HF Space has `WorkspaceClient()` already configured (same credentials used for Lakebase auth at `db.py:114`). Use it to fetch job run history for precise freshness, last run, and duration data.

```python
from databricks.sdk import WorkspaceClient


@ttl_cache()
def _fetch_job_runs() -> dict[str, dict[str, Any]]:
    """Fetch recent job runs from Databricks Jobs API.

    Returns dict keyed by task_key with latest run info:
    {task_key: {"last_run": datetime, "duration_seconds": int, "state": str}}
    """
    try:
        ws = WorkspaceClient()
        # Get the workflow job — all pipelines run as tasks in one job
        # Find runs from the last 30 days
        runs: dict[str, dict[str, Any]] = {}
        for run in ws.jobs.list_runs(
            expand_tasks=True,
            start_time_from=int((pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)).timestamp() * 1000),
            limit=100,
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
                        "last_run": pd.Timestamp(end_time, unit="ms", tz="UTC") if end_time else None,
                        "duration_seconds": duration,
                        "state": (task.state.result_state.value if task.state and task.state.result_state else "UNKNOWN"),
                        "end_time_ms": end_time,
                    }
        logger.info("Fetched run data for %d task keys from Jobs API", len(runs))
        return runs
    except Exception:
        logger.warning("Jobs API query failed — run data unavailable", exc_info=True)
        return {}
```

- [ ] **Step 5: Table data builder**

```python
def _build_table_data(
    cards: dict[str, dict[str, Any]],
    cold_costs: pd.DataFrame,
    job_runs: dict[str, dict[str, Any]],
    type_filter: str | None,
    status_filter: str | None,
) -> pd.DataFrame:
    """Build dashboard table DataFrame from cards + cost data."""
    global _wf_card_ids
    card_ids: list[str] = []

    # Build cost lookup: entry_point -> 30d USD (set_index is faster than iterrows)
    cost_lookup: dict[str, float] = {}
    if not cold_costs.empty:
        cost_lookup = cold_costs.set_index("task_key")["total_cost_usd"].apply(
            lambda x: float(x or 0)
        ).to_dict()

    rows = []
    for card_id, card in cards.items():
        wf_type = card.get("type", "")
        status = card.get("status", "")

        # Apply filters
        if type_filter and type_filter != "All" and wf_type != type_filter:
            continue
        if status_filter and status_filter != "All" and status != status_filter:
            continue

        # Determine runtime(s)
        exec_cfg = card.get("execution") or {}
        runtimes = []
        if exec_cfg.get("training"):
            rt = exec_cfg["training"].get("runtime", "")
            if "hf" in rt:
                runtimes.append("HF")
            elif "databricks" in rt:
                runtimes.append("DBX")
        if exec_cfg.get("inference"):
            rt = exec_cfg["inference"].get("runtime", "")
            if "databricks" in rt:
                runtimes.append("DBX")
        runtime_str = " + ".join(sorted(set(runtimes))) or "—"

        # Cost: actual from cold tier, or projected from YAML
        entry_point = (exec_cfg.get("inference") or {}).get("entry_point", "")
        actual_cost = cost_lookup.get(entry_point)
        if actual_cost is not None and actual_cost > 0:
            cost_str = f"${actual_cost:.2f}"
        else:
            # Fall back to projected from YAML
            cost_cfg = card.get("cost") or {}
            projected = 0.0
            if cost_cfg.get("training"):
                projected += float(cost_cfg["training"].get("typical_cost_usd", 0) or 0)
            if cost_cfg.get("inference"):
                projected += float(cost_cfg["inference"].get("typical_cost_usd", 0) or 0)
            cost_str = f"~${projected:.2f}" if projected > 0 else "—"

        # Last Run + Duration + Freshness from Jobs API
        entry_point = (exec_cfg.get("inference") or {}).get("entry_point", "")
        job_run = job_runs.get(entry_point, {})
        last_run_ts = job_run.get("last_run")
        duration_secs = job_run.get("duration_seconds", 0)

        last_run_str = "—"
        duration_str = "—"
        if last_run_ts is not None:
            last_run_str = last_run_ts.strftime("%Y-%m-%d %H:%M")
            if duration_secs > 0:
                mins, secs = divmod(duration_secs, 60)
                duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

        # Freshness: from Jobs API last run time vs SLA
        freshness_str = "—"
        sla_hours = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla_hours and last_run_ts is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run_ts).total_seconds() / 3600
            if age_hours <= sla_hours * 0.75:
                freshness_str = "OK"
            elif age_hours <= sla_hours:
                freshness_str = "Warning"
            else:
                freshness_str = "Stale"
        elif sla_hours is None:
            freshness_str = "—"  # Manual-trigger, no SLA

        rows.append({
            "Name": card.get("name", card_id),
            "Type": _TYPE_LABELS.get(wf_type, wf_type),
            "Status": status.capitalize(),
            "Domain": card.get("domain", ""),
            "Runtime": runtime_str,
            "Last Run": last_run_str,
            "Duration": duration_str,
            "Cost (30d)": cost_str,
            "Freshness": freshness_str,
        })
        card_ids.append(card_id)

    # Store parallel card ID list for row click mapping (not in the DataFrame)
    _wf_card_ids = card_ids

    if not rows:
        return pd.DataFrame(columns=_WF_TABLE_COLS)
    return pd.DataFrame(rows)
```

- [ ] **Step 6: Detail section builders**

Build all 8 detail sections as HTML strings. Each function takes a card dict and returns an HTML string rendered inside a Taipy `<|{var}|html|>` block.

Key functions (implement each):

```python
def _build_badges_html(card: dict) -> str:
    """Status + type badges HTML."""
    ...

def _build_data_flow_html(card: dict) -> str:
    """Three-column INPUTS → WORKFLOW → OUTPUTS layout."""
    ...

def _build_exec_html(card: dict) -> str:
    """Training/inference execution cards."""
    ...

def _build_monitoring_html(card: dict) -> str:
    """Monitoring metrics table with bar indicators."""
    ...

def _build_cost_html(card: dict, cold_costs: pd.DataFrame, warm_costs: pd.DataFrame) -> str:
    """Cost transparency section — per-phase actual/estimated/projected."""
    ...

def _build_references_html(card: dict) -> str:
    """Academic provenance with role badges."""
    ...

def _build_deps_html(card: dict, all_cards: dict) -> str:
    """Mini dependency graph — immediate upstream/downstream neighbors."""
    ...

def _build_idempotency_html(card: dict) -> str:
    """Idempotency strategy display."""
    ...

def _build_source_html(card: dict) -> str:
    """Source code links + HF Hub link."""
    ...
```

Each function follows the same pattern:
1. Extract relevant fields from the card dict
2. Build HTML using the CSS classes from Task 4
3. Return empty string if section has no data (section hidden via Taipy `render=` condition)

Example — references:
```python
def _build_references_html(card: dict[str, Any]) -> str:
    refs = card.get("references", [])
    if not refs:
        return ""
    items = []
    for ref in refs:
        role = ref.get("role", "methodology")
        citation = ref.get("citation", "")
        items.append(
            f'<div class="ll-ref-item">'
            f'<span class="ll-ref-role">{role}</span>'
            f'{citation}</div>'
        )
    return "\n".join(items)
```

- [ ] **Step 7: Callbacks + refresh**

```python
def wf_on_dag_click(state: Any, id: str, payload: dict) -> None:
    """DAG node clicked — switch to detail view for that workflow."""
    workflow_id = payload.get("args", [""])[0] if isinstance(payload, dict) else str(id)
    if workflow_id in _cards:
        _show_detail(state, workflow_id)


def wf_on_table_action(state: Any, var_name: str, payload: dict) -> None:
    """Table row clicked — switch to detail view.

    Uses a hidden _wf_card_ids list (parallel to table rows) to map
    row index to card ID, avoiding a visible _card_id column in the table.
    """
    idx = payload.get("index", 0) if isinstance(payload, dict) else 0
    if 0 <= idx < len(_wf_card_ids):
        card_id = _wf_card_ids[idx]
        if card_id in _cards:
            _show_detail(state, card_id)


def wf_on_back_click(state: Any, id: str, payload: dict) -> None:
    """Back to dashboard. Signature matches Taipy on_action (state, id, payload)."""
    state.wf_selected_workflow = None


def wf_on_type_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Type filter changed — rebuild table."""
    _refresh_table(state)


def wf_on_status_filter(state: Any, var_name: str, var_value: Any) -> None:
    """Status filter changed — rebuild table."""
    _refresh_table(state)


def _show_detail(state: Any, workflow_id: str) -> None:
    """Populate all detail state variables for a workflow."""
    card = _cards.get(workflow_id, {})
    cold = _fetch_cold_costs()
    warm = _fetch_warm_costs()

    state.wf_selected_workflow = workflow_id
    state.wf_detail_title = card.get("name", workflow_id)
    state.wf_detail_badges_html = _build_badges_html(card)
    state.wf_detail_meta = (
        f"Domain: {card.get('domain', '—')} | "
        f"Owner: {', '.join(card.get('owners', []))} | "
        f"v{card.get('version', '?')}"
    )
    state.wf_detail_overview = card.get("body", "").strip()
    state.wf_detail_data_flow_html = _build_data_flow_html(card)
    state.wf_detail_exec_html = _build_exec_html(card)
    state.wf_detail_monitoring_html = _build_monitoring_html(card)
    state.wf_detail_cost_html = _build_cost_html(card, cold, warm)
    state.wf_detail_references_html = _build_references_html(card)
    state.wf_detail_deps_html = _build_deps_html(card, _cards)
    state.wf_detail_idempotency_html = _build_idempotency_html(card)
    state.wf_detail_source_html = _build_source_html(card)


def _refresh_table(state: Any) -> None:
    """Rebuild dashboard table with current filters."""
    cold = _fetch_cold_costs()
    jobs = _fetch_job_runs()
    state.wf_table_data = _build_table_data(
        _cards, cold, jobs,
        state.wf_type_filter, state.wf_status_filter,
    )


def _compute_stats(
    state: Any, cold: pd.DataFrame, warm: pd.DataFrame, jobs: dict[str, dict[str, Any]]
) -> None:
    """Compute stats bar metrics."""
    state.wf_total_workflows = str(len(_cards))

    # Total 30d cost
    total = float(cold["total_cost_usd"].sum()) if not cold.empty else 0.0
    state.wf_total_cost_30d = f"${total:.2f}"

    # Freshness summary: count workflows WITH SLA that are within SLA (from Jobs API)
    monitored = 0
    fresh_count = 0
    for card_id, card in _cards.items():
        sla = (card.get("monitoring") or {}).get("freshness_sla_hours")
        if sla is None:
            continue  # No SLA = not monitored, skip
        monitored += 1
        entry_point = ((card.get("execution") or {}).get("inference") or {}).get("entry_point", "")
        run = jobs.get(entry_point, {})
        last_run = run.get("last_run")
        if last_run is not None:
            age_hours = (pd.Timestamp.now(tz="UTC") - last_run).total_seconds() / 3600
            if age_hours <= sla:
                fresh_count += 1
    if monitored > 0:
        state.wf_freshness_summary = f"{fresh_count}/{monitored} within SLA"
    else:
        state.wf_freshness_summary = "No SLAs configured"

    # Last full run: most recent completed run across all tasks
    latest_ts = None
    latest_duration = 0
    for run_info in jobs.values():
        ts = run_info.get("last_run")
        if ts is not None and (latest_ts is None or ts > latest_ts):
            latest_ts = ts
            latest_duration = run_info.get("duration_seconds", 0)
    if latest_ts:
        state.wf_last_full_run = latest_ts.strftime("%Y-%m-%d %H:%M UTC")
        mins, secs = divmod(latest_duration, 60)
        state.wf_last_full_run_detail = f"Duration: {mins}m {secs}s" if mins else f"Duration: {secs}s"
    else:
        state.wf_last_full_run = "—"
        state.wf_last_full_run_detail = ""


def wf_refresh(state: Any) -> None:
    """Page entry point — loads cards, queries costs, builds dashboard."""
    global _cards

    _cards = _load_cards_from_yaml()
    if not _cards:
        logger.warning("No workflow cards loaded")
        return

    # Build filter LOVs from card metadata
    types = sorted({c.get("type", "") for c in _cards.values()})
    statuses = sorted({c.get("status", "") for c in _cards.values()})
    state.wf_type_lov = ["All"] + [_TYPE_LABELS.get(tp, tp) for tp in types]
    state.wf_status_lov = ["All"] + [s.capitalize() for s in statuses]
    state.wf_type_filter = "All"
    state.wf_status_filter = "All"

    # Build DAG
    state.wf_dag_html = _build_dag_html(_cards)

    # Query costs + job runs
    cold = _fetch_cold_costs()
    warm = _fetch_warm_costs()
    jobs = _fetch_job_runs()

    # Build table
    state.wf_table_data = _build_table_data(_cards, cold, jobs, "All", "All")

    # Stats (uses jobs for freshness, cold for cost, warm for live runs)
    _compute_stats(state, cold, warm, jobs)

    # Clear detail state (dashboard mode)
    state.wf_selected_workflow = None

    logger.info("Workflows page loaded: %d cards, %d cost rows", len(_cards), len(cold))


register_page_refresher("AI-ML-Workflows", wf_refresh)
```

---

## Task 6: Page Config + Taipy Markdown

**Files:**
- Create: `hf_taipy_app/src/pages/workflows.py`

The page markdown is hand-crafted (not generated by `build_page()`) because the layout differs fundamentally from analytics pages. It uses `PageConfig` only for nav/glossary metadata.

- [ ] **Step 1: Create page config**

```python
"""AI/ML Workflows page — DAG, table, cost transparency, detail drilldown.

Hand-crafted Taipy markdown. Uses PageConfig for nav registration only.
"""

from __future__ import annotations

from page_template import NAV_OPERATIONS, Citation, PageConfig

page_config = PageConfig(
    title="AI/ML Workflows",
    icon="account_tree",
    nav_section=NAV_OPERATIONS,
    description=(
        "Interactive dependency graph and operational dashboard for all AI/ML workflows. "
        "16 workflow cards covering training (HF Jobs) and inference (Databricks) pipelines. "
        "Cost transparency across three tiers: actual (billing), estimated (live), and projected (YAML)."
    ),
    citations=[],  # No single citation — individual workflows cite their own papers
)
```

- [ ] **Step 2: Write Taipy markdown**

The page has two modes controlled by `wf_selected_workflow`.

**IMPORTANT:** Taipy uses `<|{var}|text|raw|>` for raw HTML rendering, NOT `<|{var}|html|>`. The `|html|` syntax does not exist in the Taipy codebase. Every raw HTML block must use `|text|raw|`.

```python
page_md = """
<|part|class_name=ll-page-header|
## <span class="material-symbols-outlined">account_tree</span> AI/ML Workflows
|>

<|part|render={wf_selected_workflow is None}|

<|part|class_name=ll-stats-bar|
<|part|class_name=ll-stat-card|
<|{wf_total_workflows}|text|class_name=ll-stat-card h3|>

Total Workflows
{: .ll-stat-label}
|>
<|part|class_name=ll-stat-card|
<|{wf_freshness_summary}|text|class_name=ll-stat-card h3|>

Freshness
{: .ll-stat-label}
|>
<|part|class_name=ll-stat-card|
<|{wf_total_cost_30d}|text|class_name=ll-stat-card h3|>

Cost (30 days)
{: .ll-stat-label}
<|{wf_cost_detail}|text|class_name=ll-stat-detail|>
|>
<|part|class_name=ll-stat-card|
<|{wf_last_full_run}|text|class_name=ll-stat-card h3|>

Last Full Run
{: .ll-stat-label}
<|{wf_last_full_run_detail}|text|class_name=ll-stat-detail|>
|>
|>

<|part|class_name=ll-filter-bar|
<|{wf_type_filter}|selector|lov={wf_type_lov}|dropdown|on_change=wf_on_type_filter|label=Type|>
<|{wf_status_filter}|selector|lov={wf_status_lov}|dropdown|on_change=wf_on_status_filter|label=Status|>
|>

<|part|class_name=ll-dag-container|
<|{wf_dag_html}|text|raw|>
|>

<|{wf_table_data}|table|page_size=20|class_name=ll-clickable-row|on_action=wf_on_table_action|>

|>

<|part|render={wf_selected_workflow is not None}|

<|Back to Workflows|button|on_action=wf_on_back_click|class_name=ll-header-btn text-no-transform|>

<|part|class_name=ll-detail-header|
### {wf_detail_title}

<|{wf_detail_badges_html}|text|raw|>

{wf_detail_meta}
{: .ll-detail-meta}
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_overview) > 0}|
**Overview**
{: .ll-section-title}

{wf_detail_overview}
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_data_flow_html) > 0}|
**Data Flow**
{: .ll-section-title}

<|{wf_detail_data_flow_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_exec_html) > 0}|
**Execution**
{: .ll-section-title}

<|{wf_detail_exec_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_monitoring_html) > 0}|
**Monitoring**
{: .ll-section-title}

<|{wf_detail_monitoring_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_cost_html) > 0}|
**Cost Transparency**
{: .ll-section-title}

<|{wf_detail_cost_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_references_html) > 0}|
**Academic Provenance**
{: .ll-section-title}

<|{wf_detail_references_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_deps_html) > 0}|
**Dependencies**
{: .ll-section-title}

<|{wf_detail_deps_html}|text|raw|>
|>

<|part|class_name=ll-detail-section|render={len(wf_detail_idempotency_html) > 0}|
**Idempotency & Source Code**
{: .ll-section-title}

<|{wf_detail_idempotency_html}|text|raw|>

<|{wf_detail_source_html}|text|raw|>
|>

|>
"""
```

**Notes for the implementing agent:**
- The exact Taipy markdown syntax may need adjustment — test each section incrementally
- All raw HTML blocks use `<|{var}|text|raw|>` (verified pattern from `template.py:464,567`)
- The table `on_action` callback fires `wf_on_table_action` with row info
- Filter dropdowns are inline in the page markdown (not in the sidebar)
- If `<|text|raw|>` doesn't execute `<script>` tags (Cytoscape.js), fall back to Plotly network graph with `go.Scatter` nodes/edges and `<|{fig}|chart|>` component
- The stats bar uses nested `<|part|>` blocks — verify Taipy renders them correctly
- Add an empty-state info box: `<|part|render={len(wf_table_data) == 0}|class_name=ll-info-box|>No workflow cards found. Ensure workflow-cards/ directory is bundled in deploy.<|part|>`

---

## Task 7: Integration — main.py + template.py + Glossary

**Files:**
- Modify: `hf_taipy_app/src/main.py`
- Modify: `hf_taipy_app/src/template.py`

- [ ] **Step 1: Register page in main.py**

Add imports:
```python
from pages.workflows import page_config as workflows_config
from pages.workflows import page_md as workflows_page
from state.workflows import *  # noqa: F403
```

Add to `PAGE_REGISTRY` (after the Advanced section):
```python
    # Operations
    PageEntry("AI-ML-Workflows", workflows_config, workflows_page),
```

- [ ] **Step 2: Add glossary terms to template.py**

Add to `GLOSSARY` dict:
```python
    "Workflow Card": "A YAML manifest describing an AI/ML workflow — its inputs, outputs, execution config, cost estimates, and academic provenance. Each of the 16 workflows has a card in workflow-cards/.",
    "Skip Guard": "Idempotency pattern that checks for already-processed results before expensive computation. Prevents duplicate work on retry.",
    "Freshness SLA": "Maximum acceptable age (in hours) for a workflow's output data. Green = within SLA, yellow = 75-100%, red = exceeded.",
    "applyInPandas": "Spark distributed execution pattern that runs a Python function on each group of a grouped DataFrame in parallel across executors.",
    "Cost Tier": "How cost data was sourced: Actual (from billing), Estimated (from pipeline timing), or Projected (from YAML card estimates).",
```

- [ ] **Step 3: Add PAGE_TERMS entry**

```python
PAGE_TERMS["AI-ML-Workflows"] = [
    "Workflow Card",
    "Skip Guard",
    "Freshness SLA",
    "applyInPandas",
    "Cost Tier",
]
```

- [ ] **Step 4: Update sidebar visibility**

The Workflows page has no analytics filters (no competition/team/match cascade). Add `"AI-ML-Workflows"` to the pages that should NOT show the filter header. In `template.py`, the `_FILTER_HEADER_PAGES` tuple controls this — ensure `"AI-ML-Workflows"` is NOT in this tuple (it shouldn't be, since it's not listed by default).

If the workflows page needs its own sidebar widgets (type/status filter dropdowns), add them as page-specific `SidebarWidget` entries with `condition=f"current_page == 'AI-ML-Workflows'"`. Or handle filtering inline in the page markdown (simpler for Phase 1).

- [ ] **Step 5: Lint + type check**

```bash
cd hf_taipy_app && uv run ruff check src/ && uv run ruff format --check src/
```

---

## Task 8: Verification

- [ ] **Step 1: Local smoke test**

Run the Taipy app locally (requires Lakebase credentials):
```bash
cd hf_taipy_app
LAKEBASE_HOST=<host> LAKEBASE_ENDPOINT_NAME=<endpoint> python src/main.py
```

Verify:
- "Operations" nav section appears with "AI/ML Workflows" page link
- Existing 12 pages still work (nav sections unchanged)
- Clicking "AI/ML Workflows" shows the dashboard (stats bar, DAG, table)
- DAG renders with 16 nodes and dependency edges
- Table shows 16 rows with cost data
- Clicking a table row or DAG node navigates to detail view
- Detail view shows all 8 sections with real data
- "Back to Workflows" returns to dashboard
- No console errors

- [ ] **Step 2: Deploy to staging**

```bash
python scripts/deploy_taipy.py staging
```

Verify:
- Deployment succeeds
- workflow-cards/ bundled and uploaded
- staging Space starts without errors

- [ ] **Step 3: Puppeteer verification on staging**

Navigate to staging URL, take screenshots of:
1. Dashboard view (stats bar + DAG + table)
2. Detail view for a representative workflow (e.g., wf-pitch-control)
3. Academic provenance section
4. Cost transparency section

Verify no missing data, no layout breaks, no raw IDs visible.

- [ ] **Step 4: Verify all existing pages unaffected**

Click through all 12 existing pages on staging. Confirm:
- Nav section headers render correctly
- Filter cascade works
- No regressions

---

## Risk Register

| Risk | Mitigation |
|------|-----------|
| Taipy `<\|text\|raw\|>` strips `<script>` tags | Try iframe embed, inline `<script>` placement, or Taipy `page` property. **Do NOT abandon Cytoscape.js without explicit user approval** — exhaust all embedding approaches first |
| Taipy callback from JS doesn't work | Use hidden selector dropdown bound to Python callback instead of direct JS→Python |
| Synced table latency for cost data | Show "Cost data syncing..." info box; data appears within minutes |
| DAG too wide for mobile/small screens | CSS `overflow: auto` on container + zoom/pan via Cytoscape |
| HF Space doesn't have workflow-cards/ | Deploy script pre-flight check verifies card count |

## Security Considerations

- **Phase 1 (this plan):** Read-only. No authentication required. All data queries use parameterized SQL via existing `execute_query()`.
- **Phase 2 (future):** Admin actions (trigger run, update config) require:
  1. `NavSection(role="admin")` filtering in `build_nav()`
  2. `wf_is_admin` state from JWT/OAuth subject verification via `db._extract_jwt_subject()`
  3. Server-side role check in every write callback (never trust client `wf_is_admin`)
  4. Separate `_write_*()` functions gated on role, distinct from `_read_*()` functions
- **Card YAML safety:** Cards are developer-authored, committed to the repo, and validated in CI. The `body` field (Markdown) is rendered by Taipy's Markdown parser which sanitizes HTML. No `eval()` or dynamic execution of card content.
