# Taipy Production Switch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Switch the production HF Space from Streamlit to Taipy and update all documentation.

**Architecture:** Deploy `taipy_spike/` to the production HF Space (`luxury-lakehouse/soccer-analytics-app`) using the existing `deploy_taipy.py` script. Update CLAUDE.md, README.md, org-card, and C4 diagrams. Streamlit code stays in repo for reference.

**Tech Stack:** Python, huggingface_hub, Structurizr DSL, Puppeteer (verification)

**Spec:** `docs/superpowers/specs/2026-03-22-taipy-production-switch-design.md`

---

### Task 1: Update deploy script and README frontmatter, deploy to production

**Files:**
- Modify: `scripts/deploy_taipy.py:33-35` (TARGETS dict)
- Modify: `taipy_spike/README.md` (rewrite for production)

- [ ] **Step 1: Add production target to deploy script**

In `scripts/deploy_taipy.py`, update the TARGETS dict:

```python
TARGETS: dict[str, str] = {
    "staging": "luxury-lakehouse/staging",
    "production": "luxury-lakehouse/soccer-analytics-app",
}
```

- [ ] **Step 2: Update taipy_spike/README.md for production**

Replace the full file content with production frontmatter and the descriptive body
from `hf_streamlit_app/README.md` (lines 20-33), updating "Streamlit" references:

```markdown
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

# (Right! Luxury!) Lakehouse

Interactive soccer analytics dashboard powered by [Databricks Lakebase](https://www.databricks.com/product/lakebase) PostgreSQL.
12 analysis pages covering 380+ matches across 5 data providers.

**Data:** [StatsBomb](https://github.com/statsbomb/open-data) (CC-BY 4.0) ·
[Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) (CC-BY-NC 4.0) ·
[Metrica](https://github.com/metrica-sports/sample-data) (CC-BY 4.0) ·
[IDSSE](https://doi.org/10.6084/m9.figshare.c.5727542) (CC-BY 4.0) ·
[SkillCorner](https://github.com/SkillCorner/opendata) (MIT)

**Platform:** [luxury-lakehouse](https://huggingface.co/luxury-lakehouse) ·
[Datasets](https://huggingface.co/luxury-lakehouse) ·
[Models](https://huggingface.co/luxury-lakehouse)
```

- [ ] **Step 3: Verify dry-run works for production target**

Run: `python scripts/deploy_taipy.py production --dry-run`
Expected: Pre-flight passes, file listing appears with no errors.

- [ ] **Step 4: Deploy to production**

Run: `python scripts/deploy_taipy.py production`
Expected: `VERIFIED: last_modified advanced`, commit URL logged.

- [ ] **Step 5: Wait for build and verify via Puppeteer**

Poll `space_info` until stage is `RUNNING` (requirements.txt unchanged from staging,
so Docker layer cache should make this fast — ~1 min). Then:
1. Navigate to `https://luxury-lakehouse-soccer-analytics-app.hf.space`
2. Screenshot landing page
3. Confirm: nav sidebar with 12 pages, "Select a competition to begin" info message

---

### Task 2: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md:9` (architecture principles)
- Modify: `CLAUDE.md:118` (performance budget label)
- Modify: `CLAUDE.md:123` (section heading)

- [ ] **Step 1: Update separation of concerns (line 9)**

Change `presentation (Streamlit)` to `presentation (Taipy)`.

- [ ] **Step 2: Add Streamlit retention note (after line 11)**

After the architecture principles bullet list (line 11), add:

```
- **Streamlit retained for reference**: `src/streamlit_app/` and `hf_streamlit_app/` are preserved during the Taipy transition period (~1 week). No changes needed to this code.
```

- [ ] **Step 3: Update performance budget label (line 118)**

Change `**Streamlit page load**` to `**App page load**`.

- [ ] **Step 4: Rename Streamlit Performance section (line 123)**

Change `## Streamlit Performance` to `## App Performance`.

The three rules under this section (module-level cache, LIMIT clauses, recursive CTE) are
framework-agnostic and apply to Taipy equally. Leave rule text unchanged.

- [ ] **Step 5: Leave Streamlit UX Standards section as-is**

No changes. Already documents Taipy equivalents inline. `st.*` references remain valid
since Streamlit code is preserved.

---

### Task 3: Update README.md

**Files:**
- Modify: `README.md` (lines 22, 30, 37, 46, 77, 89, 120, 129, 145, 157)

- [ ] **Step 1: Update pipeline diagrams**

Line 22 — change `→ Streamlit` to `→ Taipy`
Line 30 — change `→ Streamlit` to `→ Taipy`

- [ ] **Step 2: Update C4 reference (line 37)**

Change:
`System Context, Container, Ingestion Component, dbt Component, Streamlit Component, and Data Flow levels`
to:
`System Context, Container, Deployment, and Filter Cascade levels`

- [ ] **Step 3: Update application layer (line 46)**

Change `Streamlit on HuggingFace Spaces` to `Taipy on HuggingFace Spaces`.
URL stays the same.

- [ ] **Step 4: Update player similarity reference (line 77)**

Change `interactive Streamlit page` to `interactive dashboard page`.

- [ ] **Step 5: Update directory structure (line 89)**

Replace:
```
│   └── streamlit_app/  # Interactive analytics dashboard
├── notebooks/          # Databricks notebooks
```
with:
```
│   └── streamlit_app/  # Streamlit dashboard (retained for reference)
├── taipy_spike/        # Taipy production dashboard (deployed to HF Spaces)
├── notebooks/          # Databricks notebooks
```

- [ ] **Step 6: Update status and phase table**

Line 120 — change `12 Streamlit pages` to `12 Taipy pages`

Line 129 — change `Application Deployment (Streamlit)` to
`Application Deployment (Streamlit → Taipy)`

After line 145, add:
```
| 20 | Taipy Migration (12 pages, full content parity) | Complete |
```

- [ ] **Step 7: Update tech stack (line 157)**

Change `Streamlit + mplsoccer + Plotly` to `Taipy 4.1 + mplsoccer + Plotly`

- [ ] **Step 8: Verify README updates**

Run: `grep -n "Streamlit" README.md` — confirm only intentional references remain
(e.g., Phase 5 label, directory structure note, analytics section references to
Streamlit pages that still exist in code).

---

### Task 4: Update org-card.md

**Files:**
- Modify: `docs/huggingface/org-card.md:20,30`

- [ ] **Step 1: Update dashboard description (line 20)**

Change `12-page Streamlit app` to `12-page Taipy app`.

- [ ] **Step 2: Update platform scale bullet (line 30)**

Change `12 Streamlit dashboard pages` to `12 Taipy dashboard pages`.

---

### Task 5: Update C4 architecture diagrams

**Files:**
- Overwrite: `docs/c4/architecture.dsl`
- Overwrite: `docs/c4/architecture.html`

- [ ] **Step 1: Copy Taipy files to production C4 location**

Copy `taipy_spike/architecture.dsl` → `docs/c4/architecture.dsl` (overwrite).
Copy `taipy_spike/architecture.html` → `docs/c4/architecture.html` (overwrite).

Both files were regenerated earlier this session and are up to date with the
Taipy production architecture (4 views: System Context, Containers, Deployment,
Filter Cascade).

- [ ] **Step 2: Verify diagram renders in browser**

Open `docs/c4/architecture.html`. Confirm all 4 tabs render correctly.

---

### Task 6: Verification suite

- [ ] **Step 1: Lint and type-check**

Run: `uv run ruff check scripts/deploy_taipy.py && uv run pyright scripts/deploy_taipy.py`
Expected: All checks passed, 0 errors.

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest src/tests/`
Expected: 807+ passed, 0 failed.

- [ ] **Step 3: Puppeteer verify production site**

Navigate to `https://luxury-lakehouse-soccer-analytics-app.hf.space`.
Screenshot. Confirm Taipy app renders with all 12 nav entries.

---

### Task 7: Commit, push, and merge

- [ ] **Step 1: Stage and commit all changes**

```bash
git add scripts/deploy_taipy.py taipy_spike/README.md CLAUDE.md README.md \
  docs/huggingface/org-card.md docs/c4/architecture.dsl docs/c4/architecture.html \
  docs/superpowers/specs/2026-03-22-taipy-production-switch-design.md \
  docs/superpowers/plans/2026-03-22-taipy-production-switch.md
git commit -m "feat: switch production HF Space from Streamlit to Taipy

Deploy Taipy to luxury-lakehouse/soccer-analytics-app. Update CLAUDE.md,
README.md, org-card, and C4 architecture. Streamlit code retained in
src/streamlit_app/ and hf_streamlit_app/ for reference (~1 week).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 2: Push**

Run: `git push`

- [ ] **Step 3: Merge to main**

```bash
git checkout main
git pull
git merge --no-ff spike/taipy-proof-of-concept -m "Merge spike/taipy-proof-of-concept: Taipy production switch"
git push
```

Use `--no-ff` to preserve the branch boundary in history.

- [ ] **Step 4: Update memory**

Update `project_taipy_spike_status.md` to reflect production deployment.
Update MEMORY.md "Latest State" section.
