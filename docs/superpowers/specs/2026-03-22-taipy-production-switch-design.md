# Taipy Production Switch

**Date:** 2026-03-22
**Branch:** `spike/taipy-proof-of-concept`
**Goal:** Switch the production HF Space from Streamlit to Taipy and update all documentation.

## Context

The Taipy spike achieved full content parity with the Streamlit app (12 pages, CHI audit,
Plotly interactivity). A reliable deploy pipeline (`scripts/deploy_taipy.py`) is in place.
The production HF Space (`luxury-lakehouse/soccer-analytics-app`) currently runs Streamlit.

## Scope

### 1. Production Deployment

Add `production` target to `scripts/deploy_taipy.py` TARGETS dict:

```python
TARGETS: dict[str, str] = {
    "staging": "luxury-lakehouse/staging",
    "production": "luxury-lakehouse/soccer-analytics-app",
}
```

Deploy `taipy_spike/` to `luxury-lakehouse/soccer-analytics-app`. Secrets already configured
on that space (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `LAKEBASE_HOST`,
`LAKEBASE_ENDPOINT_NAME`, `GOLD_SCHEMA`).

**`taipy_spike/README.md` frontmatter** must be updated to match production metadata:

```yaml
---
title: (Right! Luxury!) Lakehouse
emoji: "\u26BD"
colorFrom: yellow
colorTo: gray
sdk: docker
app_port: 7860
pinned: true
license: apache-2.0
tags: [soccer, football, analytics, taipy, databricks, lakebase]
short_description: 12-page soccer analytics dashboard on Lakebase
---
```

Changes from current staging README: `title` (was "Luxury Lakehouse Staging"),
`pinned: true` (was false), add `short_description`, keep `taipy` tag (replaces `streamlit`).

### 2. Streamlit Preservation

Keep `src/streamlit_app/` and `hf_streamlit_app/` untouched in the repo. No code changes,
no deprecation markers. Runnable locally via `uv run streamlit run src/streamlit_app/app.py`
or deployable to `luxury-lakehouse/staging` if needed for comparison.

Target deprecation: ~2026-03-29 (1 week).

### 3. Documentation Updates

#### CLAUDE.md

- **Line 9** ("presentation (Streamlit)"): Change to "presentation (Taipy)".
- **Add note** after the architecture principles section: Streamlit code is retained in
  `src/streamlit_app/` and `hf_streamlit_app/` for reference during the transition period.
- **`## Streamlit Performance` section**: Rename to `## App Performance` — the rules
  (module-level cache, LIMIT clauses, recursive CTE) apply equally to Taipy.
- **`## Streamlit UX Standards` section**: Keep heading as-is. These rules already document
  Taipy equivalents inline (added in CHI-AUDIT-190). The `st.*` API references stay valid
  since the Streamlit code is preserved.
- **Performance budgets**: `Streamlit page load` budget stays — same target applies to Taipy.

#### README.md

- **Pipeline diagrams** (lines 22, 30): `Streamlit` → `Taipy`
- **C4 reference** (line 37): Update "Streamlit Component" to match new C4 level names
- **App URL** (line 46): Keep URL, update description from "Streamlit" to "Taipy"
- **Directory structure** (line 89): Add `taipy_spike/` entry alongside existing
  `src/streamlit_app/` (kept for reference)
- **Phase completion** (line 129): Add note about Taipy switch
- **Tech stack** (line 157): `Streamlit` → `Taipy 4.1`

#### docs/huggingface/org-card.md

Update "Streamlit" → "Taipy" in app descriptions. Keep data source references unchanged.

#### docs/c4/architecture.dsl + .html

Replace with the Taipy architecture. Copy `taipy_spike/architecture.dsl` verbatim to
`docs/c4/architecture.dsl` — the DSL already models the production topology (Deploy Pipeline,
HF Spaces, Lakebase, all internal containers). Re-render `architecture.html` via the
Structurizr pipeline.

### 4. Directory Cleanup (post-merge)

The `taipy_spike/` directory name stays as-is for this merge. Immediately after merging to
`main`, a follow-up branch renames/restructures the Taipy source into a proper location.

### 5. Merge

After all changes committed and production deploy verified via Puppeteer, merge
`spike/taipy-proof-of-concept` into `main`.

## Rollback Procedure

If the Taipy app fails in production after deploy:

```bash
# Re-deploy Streamlit to production (manual, from repo root):
python -c "
from huggingface_hub import HfApi
api = HfApi()
api.upload_folder(folder_path='hf_streamlit_app', repo_id='luxury-lakehouse/soccer-analytics-app', repo_type='space')
"
```

The `hf_streamlit_app/` directory contains the full Streamlit deployment package
(Dockerfile, source, config) ready to upload without modification.

## Puppeteer Verification

After deploying to production, verify via Puppeteer:

1. Navigate to `https://luxury-lakehouse-soccer-analytics-app.hf.space`
2. Screenshot the landing page — confirm nav sidebar renders with all 12 page links
3. Confirm "Select a competition to begin" info message is visible (DB connection working)

This matches the staging verification already performed in this session.

## Success Criteria

1. `https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app` serves the Taipy app
2. Landing page loads with nav, sidebar filters, and DB-connected info message (Puppeteer)
3. Streamlit code compiles and runs locally without modification
4. All documentation references updated consistently
5. C4 architecture diagram reflects Taipy production topology
6. Tests pass (807+), lint clean, pyright clean

## Out of Scope

- Renaming `taipy_spike/` (deferred to post-merge cleanup)
- Streamlit code deprecation (1-week grace period)
- New features or UX changes
