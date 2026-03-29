# Documentation Audit Remediation — Design Spec

> **Date**: 2026-03-29
> **Audit source**: `LAKEHOUSE-DOC-AUDIT-1_12_0.md` (mad-scientist-skills:documentation-audit v1.12.0)
> **Scope**: Fix all 42 findings from the documentation audit
> **Constraints**: Documentation only — no code changes. No commits without explicit user approval.

---

## Verified Facts (Ground Truth)

These facts were verified from source code before designing fixes:

| Item | Correct Value | Evidence |
|---|---|---|
| StatsBomb coordinate origin | **Bottom-left** | `xg-model-card.md:86`, `pitch-control.md:65`, mplsoccer `origin="lower"` |
| `distance_to_goal` unit | **Yards** | `dbt_project/macros/distance_to_goal.sql:17`, `scripts/publish_xg_shots_hf.py:42` |
| xG v2 repo ID | **`xg-v2-model-set-encoder`** | `scripts/train_xg_v2_hf.py:84` |
| Page count | **14** | `hf_taipy_app/src/pages/*.py` minus `widget_spacing_test.py` |
| StatsBomb shot count | **~95K** | `xg-model-card.md:57` |

---

## Group 1: Critical Spatial & Identity Fixes

**Findings**: #1, #2, #3, #4, #11
**Files modified**: `xg-v2-model-card.md`, `xg-model-card.md`
**Files created**: 0

| Finding | File | Change |
|---|---|---|
| #1 Coordinate origin | `xg-v2-model-card.md:157-158` | "top-left" → "bottom-left" |
| #2 Distance unit | `xg-v2-model-card.md:113` | "(meters)" → "(yards)" |
| #3 Wrong v1 link | `xg-v2-model-card.md:310` | Link target `football2vec-statsbomb-wyscout` → `xg-model-statsbomb-wyscout`, label "Football2Vec" → "xG v1 (XGBoost)" |
| #4 Dual repo ID | `xg-v2-model-card.md` + `xg-model-card.md` inference code | `xg-v2-set-encoder` → `xg-v2-model-set-encoder` in all code examples |
| #11 License conflict | `xg-model-card.md` YAML frontmatter | `license: mit` → `license: cc-by-nc-4.0` |

---

## Group 2: HF Artifact Card Completeness

**Findings**: #9, #10, #19, #34, #35, #41
**Files modified**: `xg-freeze-frame.md`, `spadl-vaep.md`, `pitch-control.md`, `xg-shot-data.md`, all model cards (heading standardization)
**Files created**: 5 new dataset cards

### xg-freeze-frame.md rewrite (Finding #9)

Expand from 37-line stub to full card. Sections: YAML frontmatter (with `language: [en]`), description ("What is this dataset?"), Quick Start, schema table, coordinate system, data sources, companion resources, limitations, citation.

Source for schema: `scripts/publish_xg_shots_hf.py` and xG v2 model card.

### 5 new dataset cards (Finding #10)

Each follows the established template:

| Dataset | Schema source | Key content |
|---|---|---|
| `expected-threat-grids.md` | `src/analytics/expected_threat.py`, `scripts/publish_expected_threat_hf.py` | Markov chain xT grids, grid dimensions, transition/scoring matrices |
| `obso-pausa-inputs.md` | `src/analytics/obso.py`, `scripts/publish_obso_hf.py` | Input features for OBSO/PAUSA computation |
| `obso-pausa-values.md` | Same pipeline (output values) | Per-pass OBSO/PAUSA scores, timing decomposition |
| `obso-trained-grids.md` | Same pipeline (trained artifacts) | Trained transition/EPV/OBSO grid arrays |
| `space-creation-values.md` | `src/analytics/space_creation.py`, `scripts/publish_space_creation_hf.py` | ELASTIC space creation values per frame |

Template per card (~120-200 lines):
```
---
YAML frontmatter (title, emoji, license, size_categories, task_categories, tags, language)
---
# Dataset Name
Description paragraph.
## Quick Start (3-5 line load example)
## Schema (pipe table with column, type, description)
## Coordinate System (where applicable)
## Data Sources (table with source, license, notes)
## Companion Resources (cross-links to related models/datasets)
## Limitations
## Citation
```

### Consistency fixes bundled in G2

- Finding #19: Fix `xg-shot-data.md` StatsBomb count from ~88K to ~95K (verified ground truth)
- Finding #34: Standardize all dataset cards to `## Quick Start` (not `## Usage`)
- Finding #35: Standardize all cards to `## Companion Resources` (covers both models and datasets)
- Finding #41: Add cross-links: `spadl-vaep.md` → VAEP model card, `pitch-control.md` → pitch control module/related datasets

---

## Group 3: README + ARCHITECTURE.md

**Findings**: #6, #8, #14, #23, #25, #29, #37, #38, #40
**Files modified**: `README.md`, `ARCHITECTURE.md`, `hf_taipy_app/README.md`, `demo_space/README.md`
**Files created**: 0

### README.md

- **#6**: Add "Getting Started" section (3 lines — link to `docs/getting-started.md` + live demo badge)
- **#8**: Fix page count to 14
- **#23**: Update status blurb from "Phase 19 complete" to "Phase 20 complete"
- **#25**: Fix test count to 807 consistently
- **#40**: Add GitHub Actions CI badge (shield.io badge linking to `python-ci.yml` workflow)

### ARCHITECTURE.md

- **#14**: Add `hf_taipy_app/` to directory tree; annotate `src/streamlit_app/` as deprecated
- **#25**: Fix test count from "704 passed" to 807
- **#37**: Fix dataset card count from "5 datasets" to 6
- **#38**: Add `xg-v2-model-card.md` and `model-cards/vaep-model.md` to docs tree

### hf_taipy_app/README.md

- **#8**: Fix YAML `short_description` to "14-page soccer analytics dashboard" and body to "14 analysis pages"

### demo_space/README.md

- **#29**: Update published artifact list to include all 11 datasets and 4 models (matching org-card)

---

## Group 4: huggingface-setup.md Overhaul + Getting Started Tutorial

**Findings**: #5, #7, #13, #15, #16, #27, #28, #32
**Files modified**: `docs/huggingface-setup.md`
**Files created**: `docs/getting-started.md`

### huggingface-setup.md (deep rewrite)

1. **#27 — Measurable objective**: Add opening block: "After this guide you will have: (1) loaded a pre-trained football2vec embedding, (2) retrained on your data, (3) verified the output on HF Hub"
2. **#15 — Prerequisites block**: Front-load definitions/links for: UC Volume, Databricks Connect, SPADL, per-90 stats, Doc2Vec, PV-DM, gensim, secret scope, canonical_player_id, HF write token, z-score normalization
3. **#7 — Verification commands**: After each procedural step — `print(vector.shape)` → expect `(32,)`, row count check after `compute_embeddings`, `huggingface_hub.list_repo_files()` after publish
4. **#16 — Python version**: "Python 3.10+" → "Python >=3.10, <3.11 (strict — Databricks serverless constraint)"
5. **#13 — Dependency version**: `huggingface_hub>=0.20` → `huggingface_hub>=1.5.0`
6. **#28 — Error recovery**: 3 blocks covering missing HF token, stopped warehouse, missing UC Volume files
7. **#32 — Condition ordering**: Rewrite condition-after-instruction sentences to lead with the condition

### docs/getting-started.md (new Tutorial)

Tutorial-quadrant document for fork users. Structure:

```markdown
# Getting Started

> After completing this guide you will have: a working local environment,
> passing tests, and the ability to run linting and type checks.

## Prerequisites
- Git, Python 3.10 (strict), uv

## 1. Clone and Install
Steps + verification: `uv sync` → `uv run python --version` → expect 3.10.x

## 2. Verify the Environment
`uv run ruff check src/` → expect 0 violations
`uv run pyright src/` → expect 0 errors
`uv run pytest src/tests/ -x --count=5` → expect passes

## 3. Explore the Project
Pointers to README (overview), CLAUDE.md (engineering standards),
ARCHITECTURE.md (platform architecture), docs/c4/architecture.html (interactive diagrams)

## 4. Next Steps
- Try the live demo (link)
- Use pre-trained models: docs/huggingface-setup.md
- Understand the architecture: ARCHITECTURE.md
- Contribute: CONTRIBUTING.md

## Common Issues
Error-recovery table: Python version mismatch, uv not found, test failures on Windows paths
```

Not Databricks-specific — a fork user can verify the local environment without cloud credentials.

---

## Group 5: Linguistic & Style Sweep

**Findings**: #12, #17, #18, #21, #22, #30, #31, #42
**Files modified**: ~8 files (README.md, ROADMAP.md, ARCHITECTURE.md, org-card.md, org-interests.md, vaep-model.md, xg-shot-data.md, xg-v2-model-card.md)
**Files created**: `docs/glossary.md`

### Temporal language (Finding #12)

| File:Line | Before | After |
|---|---|---|
| `ROADMAP.md:14` | "currently has minimal observability" | "has minimal observability" (or date-stamp) |
| `ROADMAP.md:622` | "Currently the platform has a single `dev` environment" | "The platform has a single `dev` environment" |
| `ARCHITECTURE.md:599` | "Currently 41 btree indexes" | "The platform has 41 btree indexes" |
| `org-interests.md:7` | "No API endpoint currently exists" | "No API endpoint exists for this field" |

### Non-inclusive language (Finding #17)

- `ROADMAP.md:261`: "dummy tensors" → "placeholder tensors"

### Brand name (Finding #18)

- "HuggingFace" → "Hugging Face" in user-facing prose across README.md, ARCHITECTURE.md, huggingface-setup.md
- Keep `huggingface_hub`, `huggingface.co`, `huggingface-cli` as-is (package/URL identifiers)

### Domain acronym expansions (Finding #21)

In `README.md` analytics section, expand on first use:
- VAEP (Valuing Actions by Estimating Probabilities)
- SPADL (Simplified Player Action Description Language)
- PPDA (Passes Per Defensive Action)
- HSR (High-Speed Running)
- OBSO (Off-Ball Scoring Opportunities)
- PAUSA (Pitch control Analysis Using a Set-piece Approach)
- EPTS (Electronic Performance and Tracking Systems)

### Repo-level glossary (Finding #22)

Create `docs/glossary.md` — all domain terms with definitions and scale/direction where applicable. Mirrors the in-app Taipy glossary for GitHub readers. Cross-linked from README and getting-started.md.

### 52-word sentence (Finding #30)

`org-card.md:113` — split into two sentences.

### Right Is Right (Finding #31)

| File | Fix |
|---|---|
| `README.md:123` | Add brief interpretive context after metrics |
| `vaep-model.md:198` | Add: "For ranking applications this is less consequential; for absolute probabilities, validate with a calibration curve" |
| `xg-shot-data.md:143` | Add: "Use `scale_pos_weight` in XGBoost or stratified sampling" |
| `xg-v2-model-card.md:148` | Add baseline context for the +0.090 ROC-AUC improvement |

### Passive voice (Finding #42)

- `README.md:171`: "is maintained through" → "Claude Code skills enforce"

---

## Group 6: SECURITY.md + Repo Housekeeping

**Findings**: #20, #26, #33, #36
**Files modified**: `SECURITY.md`
**Files created**: `CONTRIBUTING.md`

### SECURITY.md (Findings #20, #26, #36)

1. **#20**: Add "Reporting a Vulnerability" section at the top: GitHub private vulnerability reporting as the primary channel, response time expectation (~7 days for solo maintainer), supported versions (current `main` only)
2. **#36**: Fix stale Streamlit references: "Streamlit UI" → "Taipy dashboard", update or remove "XSRF protection enabled in Streamlit config"
3. **#26**: Add note to Finding I-4: "Runbooks remain planned — see TODO.md for current operational procedures"

### CONTRIBUTING.md (Finding #33)

Minimal contributing guide for a solo project:
- Points to CLAUDE.md for engineering standards
- Fork workflow (fork → branch → PR)
- Required checks: `uv run ruff check src/`, `uv run ruff format --check src/`, `uv run pyright src/`, `uv run pytest src/tests/ -v`
- PR expectations: descriptive title, test coverage for new code

### Skipped (by design)

- `CODE_OF_CONDUCT.md` — boilerplate for a solo project with no community interaction
- `CHANGELOG.md` — project uses git history + phase tables, not semantic versioning

---

## Execution Order

G1 → G2 → G3 → G4 → G5 → G6

Dependencies:
- G1 spatial facts feed G2 (new cards use verified coordinate/unit values)
- G2 card creation informs G3 (artifact lists in README/demo_space)
- G4 getting-started.md cross-links to G5 glossary.md

## Parallelization Opportunities

- G1 (2 files, light edits) can run as a single focused pass
- G2 new cards can be written in parallel (5 independent files)
- G3 and G5 both touch README.md — must be sequenced (G3 first, G5 second)
- G6 is fully independent of all other groups
