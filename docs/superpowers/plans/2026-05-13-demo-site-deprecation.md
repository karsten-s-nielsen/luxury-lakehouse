# Demo Site Deprecation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the Gradio demo Space, its source code, supporting scripts, and all references across the codebase. Replace live documentation links with the production Taipy app.

**Architecture:** Pure deletion + text replacement across ~35 files. No new code. Execution order: code edits first, file deletions second, CI verification third, external resource cleanup last (irreversible).

**Tech Stack:** Git, bash, `huggingface_hub` (for post-CI archive + delete of HF Space and Bucket)

**Spec:** `docs/superpowers/specs/2026-05-13-demo-site-deprecation-design.md`

---

## File Map

All changes are edits (E) or deletions (D):

| File | Action | What changes |
|------|--------|-------------|
| `hf_taipy_app/src/page_template.py` | E | Remove demo link from `_FOOTER_CONTENT` |
| `README.md` | E | Remove demo badge + demo link in ML Artifacts row |
| `ARCHITECTURE.md` | E | Remove 3 entries (export_demo_data, setup_hf_buckets, demo_space/) |
| `CLAUDE.md` | E | Remove "or Gradio" + delete multi-surface parity bullet |
| `AI_GOVERNANCE.md` | E | Update 3 demo references |
| `docs/huggingface/org-card.md` | E | Remove demo from intro + Spaces table |
| `ROADMAP.md` | E | Remove 2 demo line items |
| `docs/getting-started.md` | E | Relabel "live demo" → "Interactive dashboard" |
| `pyproject.toml` | E | Remove `"demo_space"` from ruff + pyright excludes |
| `.gitignore` | E | Remove demo parquet exclusion |
| `src/tests/test_hf_publish_parity.py` | E | Remove demo from docstring exclusion list |
| 6 model cards in `docs/huggingface/model-cards/` | E | Replace demo links → production app links |
| 18 dataset cards in `docs/huggingface/dataset-cards/` | E | Replace demo links → production app links |
| `demo_space/` | D | Entire directory |
| `scripts/setup_hf_buckets.py` | D | Demo bucket provisioning script |
| `notebooks/export_demo_data.py` | D | Demo data export notebook |

---

### Task 1: Python and config file edits

**Files:**
- Modify: `hf_taipy_app/src/page_template.py:466-469`
- Modify: `pyproject.toml:151,252`
- Modify: `.gitignore:123-124`
- Modify: `src/tests/test_hf_publish_parity.py:22-25`

- [ ] **Step 1: Edit `_FOOTER_CONTENT` in page_template.py**

Replace the multi-line `_FOOTER_CONTENT` constant. Remove the demo link and the ` · ` separator.

```
old:
_FOOTER_CONTENT = (
    "[Interactive Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)"
    " · [Published Datasets](https://huggingface.co/luxury-lakehouse)"
)

new:
_FOOTER_CONTENT = "[Published Datasets](https://huggingface.co/luxury-lakehouse)"
```

- [ ] **Step 2: Remove `"demo_space"` from ruff exclude in pyproject.toml**

Line 151:
```
old: exclude = ["demo_space", "notebooks"]
new: exclude = ["notebooks"]
```

- [ ] **Step 3: Remove `"demo_space"` from pyright exclude in pyproject.toml**

Line 252 — remove the `"demo_space",` line from the `exclude` list:
```
old:
exclude = [
    "demo_space",
    "notebooks",

new:
exclude = [
    "notebooks",
```

- [ ] **Step 4: Remove demo parquet exclusion from .gitignore**

Remove lines 123-124:
```
old:
# Demo Space sample data (generated from Databricks, uploaded to HF directly)
demo_space/data/*.parquet

new:
(delete both lines)
```

- [ ] **Step 5: Remove demo from test_hf_publish_parity.py docstring**

Lines 22-25 — remove the demo Space exclusion from the docstring:
```
old:
- Private / app Spaces (``soccer-analytics-app``, ``staging``,
  ``soccer-analytics-demo``) — their README lives with the deployable
  app source (``hf_taipy_app/`` or ``demo_space/``), not under
  ``docs/huggingface/``.

new:
- Private / app Spaces (``soccer-analytics-app``, ``staging``) — their
  README lives with the deployable app source (``hf_taipy_app/``), not
  under ``docs/huggingface/``.
```

---

### Task 2: README.md and ARCHITECTURE.md

**Files:**
- Modify: `README.md:11,46` (spec mentioned ~line 132 — verified clean; lines 125-140 are setup instructions with no demo references)
- Modify: `ARCHITECTURE.md:683,692,731-733`

- [ ] **Step 1: Remove demo badge from README.md line 11**

Remove the demo badge from the badges line. The badge to remove (including the leading space):
```
old: ` [![Try the Demo](https://img.shields.io/badge/%F0%9F%A4%97%20HuggingFace-Live%20Demo-yellow?style=flat-square)](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)`

new: (delete — empty string)
```

- [ ] **Step 2: Remove demo link from README.md ML Artifacts row (line 46)**

Remove `, and [interactive demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)` from the sentence:
```
old: GPU training on HF Jobs, and [interactive demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo). Every artifact's

new: GPU training on HF Jobs. Every artifact's
```

- [ ] **Step 3: Remove export_demo_data.py from ARCHITECTURE.md (line 683)**

Delete the entire line:
```
│   ├── export_demo_data.py           # Export demo data for Gradio Space
```

- [ ] **Step 4: Remove setup_hf_buckets.py from ARCHITECTURE.md (line 692)**

Delete the entire line:
```
│   ├── setup_hf_buckets.py           # Initialize HF Buckets (demo-data) with versioned Parquet uploads
```

- [ ] **Step 5: Remove demo_space/ block from ARCHITECTURE.md (lines 731-733)**

Delete all three lines:
```
├── demo_space/                      # Hugging Face Gradio demo Space (6 tabs: pass quality, pitch control, player similarity, shot map, DEFCON pressure, pass timing)
│   ├── app.py                       # Gradio app with luxury flagship theme (dark surfaces, gold accents)
│   └── pitch_control.py             # Pure NumPy pitch control (Spearman 2017) — no Spark dependency
```

---

### Task 3: CLAUDE.md and AI_GOVERNANCE.md

**Files:**
- Modify: `CLAUDE.md:176,183`
- Modify: `AI_GOVERNANCE.md:58,188,245`

- [ ] **Step 1: Remove "or Gradio" from CLAUDE.md line 176**

```
old: Every Taipy or Gradio code change must satisfy all of these.
new: Every Taipy code change must satisfy all of these.
```

- [ ] **Step 2: Delete multi-surface UX parity bullet from CLAUDE.md (line 183)**

Delete the entire bullet (single line):
```
- **Multi-surface UX parity**: When a Taipy page has glossary terms, help tooltips, scale references, or academic citations, the corresponding Gradio demo tab must have equivalents (e.g., `gr.Accordion("Glossary")` with per-tab filtered terms, axis labels with range/direction, `gr.Markdown` citations). A feature on one surface without its UX scaffolding on the other is incomplete.
```

**No change: CLAUDE.md line ~185** — "HF artifact link completeness" bullet references `HF Space header, HF Space footer` generically (production app). No demo-specific language present; no edit needed.

- [ ] **Step 3: Update AI_GOVERNANCE.md line 58 — footer text reference**

```
old: labelled an "Interactive Demo · Published Datasets" in its site footer
new: labelled "Published Datasets" in its site footer
```

- [ ] **Step 4: Update AI_GOVERNANCE.md line 188 — Football2Vec v2 classification**

```
old: **Not high-risk** (dashboard is a research demo)
new: **Not high-risk** (dashboard is a research tool)
```

- [ ] **Step 5: Update AI_GOVERNANCE.md line 245 — human oversight**

```
old: Adequate for a research demo.
new: Adequate for a research tool.
```

---

### Task 4: Org card, ROADMAP, and getting-started

**Files:**
- Modify: `docs/huggingface/org-card.md:20,91`
- Modify: `ROADMAP.md:838,911`
- Modify: `docs/getting-started.md:137`

- [ ] **Step 1: Remove demo from org-card.md intro (line 20)**

```
old: > **Try it now:** [Full Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; 16-page Taipy app with live data from 380+ matches across 5 providers. Or explore the [Gradio Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) for a quick look.

new: > **Try it now:** [Full Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; 16-page Taipy app with live data from 380+ matches across 5 providers.
```

- [ ] **Step 2: Remove demo row from org-card.md Spaces table (line 91)**

Delete the entire row:
```
| [**Soccer Analytics Demo**](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) | Lightweight 6-tab Gradio explorer with pre-cached Parquet data. No database dependency — instant load for quick exploration. |
```

- [ ] **Step 3: Remove demo bucket line and fix intro from ROADMAP.md (lines 835, 838)**

Delete item 2 and fix the intro text (it says "two things" but only item 1 remains):
```
old: Even before the Polars branch merges, two things are actionable today:
new: Even before the Polars branch merges, one thing is actionable today:
```

Then delete item 2:
```
2. **Storage Buckets for demo data** — **DONE.** Demo data migrated to HF Bucket; `demo_space/app.py` reads from `hf://buckets/luxury-lakehouse/demo-data/`.
```

- [ ] **Step 4: Remove multi-surface UX parity row from ROADMAP.md (line 911)**

Delete the entire row:
```
| **Multi-surface UX parity** | Taipy glossary and Gradio demo terms should derive from a single source |
```

- [ ] **Step 5: Relabel "live demo" in docs/getting-started.md (line 137)**

```
old: - **Try the live demo:** [Soccer Analytics Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
new: - **Interactive dashboard:** [Soccer Analytics Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
```

---

### Task 5: Model cards (6 files)

**Files (all under `docs/huggingface/model-cards/`):**
- Modify: `xg-v2-model-card.md` (lines 327, 329)
- Modify: `vaep-model.md` (lines 272, 274)
- Modify: `football2vec-v2-model-card.md` (line 219)
- Modify: `football2vec-360-model-card.md` (line 243)
- Modify: `psxg-model.md` (line 185)
- Modify: `pitch-control.md` (line 130)

Two replacement patterns cover all occurrences. Apply both to every file using `replace_all=true` (pattern will no-op if absent in a given file):

- [ ] **Step 1: Replace blockquote demo links (pattern 1)**

Apply to all 6 model card files:
```
old: [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)
new: [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
```

Files with this pattern: all 6.

- [ ] **Step 2: Replace descriptive paragraph demo links (pattern 2)**

Apply to files that have descriptive paragraphs:
```
old: [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)
new: [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
```

Files with this pattern: `xg-v2-model-card.md`, `vaep-model.md`, `pitch-control.md`.

- [ ] **Step 3: Verify no demo links remain in model cards**

Run: `grep -r "soccer-analytics-demo" docs/huggingface/model-cards/`
Expected: zero hits.

---

### Task 6: Dataset cards (18 files)

**Files (all under `docs/huggingface/dataset-cards/`):**
- `xg-shot-data.md` (lines 52, 206, 208)
- `xg-freeze-frame-data.md` (line 41)
- `statsbomb-shots-on-target.md` (lines 57, 161)
- `spadl-vaep-action-values.md` (lines 43, 145)
- `space-creation-values.md` (lines 42, 134)
- `scoutgpt-training-data.md` (line 41)
- `psxg-predictions.md` (lines 49, 137)
- `pitch-control-tracking.md` (lines 33, 140)
- `obso-trained-grids.md` (lines 49, 170)
- `obso-pausa-values.md` (lines 45, 167)
- `obso-pausa-inputs.md` (lines 51, 143)
- `line-breaking-passes.md` (lines 33, 129)
- `football2vec-360-embeddings.md` (lines 44, 146)
- `football2vec-training-data.md` (lines 37, 158)
- `football2vec-statsbomb-wyscout.md` (line 45)
- `football2vec-player-embeddings.md` (lines 43, 165)
- `football2vec-360-training-data.md` (lines 40, 192)
- `expected-threat-grids.md` (lines 42, 103)

Same two replacement patterns as Task 5.

- [ ] **Step 1: Replace blockquote demo links (pattern 1)**

Apply to all 18 dataset card files using `replace_all=true`:
```
old: [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)
new: [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
```

- [ ] **Step 2: Replace descriptive paragraph demo links (pattern 2)**

Apply to files that have descriptive paragraphs:
```
old: [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)
new: [Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)
```

Files with this pattern: `xg-shot-data.md`.

- [ ] **Step 3: Verify no demo links remain in dataset cards**

Run: `grep -r "soccer-analytics-demo" docs/huggingface/dataset-cards/`
Expected: zero hits.

---

### Task 7: File and directory deletions

**Files:**
- Delete: `demo_space/` (entire directory — `app.py`, `README.md`, `requirements.txt`, `thumbnail.png`, `pitch_control.py`, `data/.gitkeep`, `__pycache__/`)
- Delete: `scripts/setup_hf_buckets.py`
- Delete: `notebooks/export_demo_data.py`

- [ ] **Step 1: Delete demo_space/ directory**

```bash
rm -rf demo_space/
```

- [ ] **Step 2: Delete scripts/setup_hf_buckets.py**

```bash
rm scripts/setup_hf_buckets.py
```

- [ ] **Step 3: Delete notebooks/export_demo_data.py**

```bash
rm notebooks/export_demo_data.py
```

---

### Task 8: Verification

- [ ] **Step 1: Run ruff check**

```bash
uv run ruff check src/ scripts/
```
Expected: no new errors.

- [ ] **Step 2: Run ruff format check**

```bash
uv run ruff format --check src/ scripts/
```
Expected: clean.

- [ ] **Step 3: Run pyright**

```bash
uv run pyright src/
```
Expected: no new type errors.

- [ ] **Step 4: Run parity test**

```bash
uv run pytest src/tests/test_hf_publish_parity.py -v
```
Expected: PASS (this test runs online when `HF_TOKEN` is set; skips gracefully otherwise).

- [ ] **Step 5: Verify no demo references remain outside historical docs**

```bash
grep -ri "soccer-analytics-demo" --include="*.py" --include="*.md" --include="*.toml" --include="*.yml" --include="*.json" . | grep -v "docs/superpowers/specs/" | grep -v "docs/superpowers/plans/" | grep -v "docs/decisions/" | grep -v "docs/evolve/"
```
Expected: zero hits.

- [ ] **Step 6: Verify no orphaned Gradio references in active code/docs**

```bash
grep -ri "gradio" --include="*.py" --include="*.toml" . | grep -v "docs/superpowers/" | grep -v "docs/decisions/" | grep -v "docs/evolve/"
```
Expected: zero hits (all Gradio code is deleted; only historical docs should mention it).

---

### Task 9: Commit

- [ ] **Step 1: Stage all changes**

Use `git add -u` (tracked files only — avoids accidentally staging untracked editor configs, `.DS_Store`, or local scratch) plus explicit `git rm` for deleted files:

```bash
git rm -r demo_space/
git rm scripts/setup_hf_buckets.py
git rm notebooks/export_demo_data.py
git add -u
git status
```

Review the staged changes. Verify:
- ~35 modified files (edits)
- ~8 deleted files (demo_space/ contents + 2 scripts)
- No unintended additions (check "new file" entries — there should be none)

- [ ] **Step 2: Commit**

```bash
git commit -m "chore: deprecate Gradio demo site — remove source, scripts, and all references

Remove demo_space/ directory, scripts/setup_hf_buckets.py, and
notebooks/export_demo_data.py. Update 24 model/dataset cards to point
to the production Taipy app. Clean up README, ARCHITECTURE, ROADMAP,
CLAUDE.md, AI_GOVERNANCE.md, org-card, pyproject.toml, .gitignore, and
test docstrings. Footer now shows only 'Published Datasets'.

Spec: docs/superpowers/specs/2026-05-13-demo-site-deprecation-design.md

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 10: External resource cleanup (post-CI, operator action)

**Prerequisites:** CI must be green on the merged commit. This task is manual — run interactively.

- [ ] **Step 1: Archive HF Space locally**

```python
from pathlib import Path
from huggingface_hub import snapshot_download

archive_root = Path.home() / "hf-archive"
snapshot_download(
    repo_id="luxury-lakehouse/soccer-analytics-demo",
    repo_type="space",
    local_dir=str(archive_root / "soccer-analytics-demo"),
)
```

- [ ] **Step 2: Archive HF Bucket locally**

```python
snapshot_download(
    repo_id="luxury-lakehouse/demo-data",
    repo_type="dataset",
    local_dir=str(archive_root / "demo-data"),
)
```

- [ ] **Step 3: Verify archives exist and are non-empty**

```python
for name in ("soccer-analytics-demo", "demo-data"):
    p = archive_root / name
    files = list(p.rglob("*"))
    print(f"{name}: {len(files)} files")
    assert len(files) > 0, f"archive {name} is empty!"
```

- [ ] **Step 4: Delete HF Space**

```python
from huggingface_hub import delete_repo
delete_repo(repo_id="luxury-lakehouse/soccer-analytics-demo", repo_type="space")
```

- [ ] **Step 5: Delete HF Bucket**

```python
from huggingface_hub import delete_repo
delete_repo(repo_id="luxury-lakehouse/demo-data", repo_type="dataset")
```

- [ ] **Step 6: Verify 404**

```bash
curl -sL -o /dev/null -w "%{http_code}" https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo
# Expected: 404 (use -L to follow HF redirects)
```
