# Demo Site Deprecation — Design Spec

**Date**: 2026-05-13
**Status**: Approved
**Scope**: Remove the Gradio demo Space (`luxury-lakehouse/soccer-analytics-demo`), its source code, supporting scripts, and all references across the codebase. Replace live documentation links with the production Taipy app where a matching page exists.

## Context

The Gradio demo Space was the original interactive UI, predating the production Taipy app. It serves 6 tabs (Shot Map, Pass Quality, Pitch Control, Player Similarity, DEFCON Pressure, Pass Timing) using pre-cached Parquet files via HF Buckets — no live database connection.

The production Taipy app now covers all 6 demo tabs with live Lakebase data, richer interactivity, and 10 additional pages. The demo is outdated, unmaintained, and no longer representative of the platform. Keeping it creates user confusion and documentation maintenance burden.

## Decision

Delete the demo Space entirely (clean slate, hard 404 — no tombstone/redirect Space). Replace documentation links with the production Taipy app URL. Leave historical specs/plans untouched.

## Execution Order

The safe sequence is:

1. **Code edits** — remove all references, update cards and docs
2. **File/directory deletions** — `demo_space/`, `scripts/setup_hf_buckets.py`, `notebooks/export_demo_data.py`
3. **Commit + CI verification** — all checks green before touching external resources
4. **Archive external resources** — archive both before deleting either:
   - `huggingface_hub.snapshot_download(repo_id="luxury-lakehouse/soccer-analytics-demo", repo_type="space", local_dir="...")`
   - `huggingface_hub.snapshot_download(repo_id="luxury-lakehouse/demo-data", repo_type="dataset", local_dir="...")`
5. **Delete external resources** — HF Space + HF Bucket via `delete_repo()`

Steps 4-5 happen only after CI is green. Archive before delete — irreversible operations get a safety net.

## Deletions

### Directories and files removed from the repository

| Path | Reason |
|------|--------|
| `demo_space/` (entire directory) | Demo app source, README, thumbnail, requirements, pitch_control module |
| `scripts/setup_hf_buckets.py` | Sole purpose is uploading demo Parquet to `luxury-lakehouse/demo-data` bucket |
| `notebooks/export_demo_data.py` | Sole purpose is exporting sample data from Databricks for the demo |

### External resources deleted

| Resource | Action |
|----------|--------|
| HF Space `luxury-lakehouse/soccer-analytics-demo` | Archive locally via `snapshot_download`, then delete via `huggingface_hub.delete_repo(repo_id="luxury-lakehouse/soccer-analytics-demo", repo_type="space")` |
| HF Bucket `luxury-lakehouse/demo-data` | Delete via `huggingface_hub.delete_repo(repo_id="luxury-lakehouse/demo-data", repo_type="dataset")` (created by `setup_hf_buckets.py` via `HfApi.create_bucket`) |

## Edits — Live References

### Taipy app footer

**File**: `hf_taipy_app/src/page_template.py`
**Change**: Remove the `[Interactive Demo](...soccer-analytics-demo)` link and the ` · ` separator from `_FOOTER_CONTENT`. Target value: `'[Published Datasets](https://huggingface.co/luxury-lakehouse)'`. The user is already on the production app — a self-referential demo link adds no value.

### README.md

**File**: `README.md`
**Change**: Remove the demo badge (line ~11) and any demo section/links (~line 47, ~132). Keep the production app badge and links.

### ARCHITECTURE.md

**File**: `ARCHITECTURE.md`
**Change**: Remove demo references at lines ~683 (`export_demo_data.py` entry), ~692 (`setup_hf_buckets.py` entry), and ~731 (`demo_space/` directory entry). Update to reflect only the Taipy app as the presentation layer.

### Org card

**File**: `docs/huggingface/org-card.md`
**Change**: Remove the demo row from the Spaces table (~line 20, ~91). Keep the production app row.

### ROADMAP.md

**File**: `ROADMAP.md`
**Change**: Remove demo-specific line items (HF Bucket migration note ~line 838, UX parity note ~line 911). These refer to demo infrastructure that no longer exists.

### pyproject.toml

**File**: `pyproject.toml`
**Change**: Remove `"demo_space"` from ruff exclude list (~line 151) and pyright exclude list (~line 247).

### .gitignore

**File**: `.gitignore`
**Change**: Remove the `demo_space/data/*.parquet` exclusion line (~line 124).

### test_hf_publish_parity.py

**File**: `src/tests/test_hf_publish_parity.py`
**Change**: Docstring-only edit. Remove `soccer-analytics-demo` and `demo_space/` from the module docstring's Space exclusion list (lines 22-24). No code constants reference the demo — `_MODEL_CARD_EXEMPT` and `_DATASET_CARD_EXEMPT` are unaffected.

### docs/getting-started.md

**File**: `docs/getting-started.md`
**Change**: Line 137 says `"Try the live demo:"` with a link to the production app. The URL is correct (already points to `soccer-analytics-app`), but the label "live demo" is confusing post-deprecation. Change to `"Try the app:"` or `"Interactive dashboard:"`.

### AI_GOVERNANCE.md

**File**: `AI_GOVERNANCE.md`
**Change**: Three references to update:

1. **Line ~58** — `"Interactive Demo · Published Datasets" in its site footer`: Update to `"Published Datasets" in its site footer` (matching the new `_FOOTER_CONTENT` value after demo link removal).
2. **Line ~186** — `"dashboard is a research demo"`: Rephrase to reflect that the platform is a single Taipy dashboard for internal analytics research (the Gradio demo no longer exists).
3. **Line ~243** — `"Adequate for a research demo"`: Same — rephrase to remove demo characterization.

### CLAUDE.md

**File**: `CLAUDE.md`
**Change**: Three edits:

1. **Line ~176** — `"Every Taipy or Gradio code change must satisfy all of these"`: Change to `"Every Taipy code change must satisfy all of these"`. Gradio code no longer exists.
2. **Line ~183** — `"Multi-surface UX parity"` bullet: Delete entirely. With the demo removed, there is only one surface (Taipy). The rule is obsolete.
3. **Line ~185** — `"HF artifact link completeness"` bullet: Keep the bullet (it refers to the production Taipy app's header/footer, not the demo). Remove only the word "Gradio" if it appears in the surrounding text. The checklist of locations to update when publishing a new HF artifact remains valid for the single-surface (Taipy) world.

### Model cards (6 files)

All in `docs/huggingface/model-cards/`. Replace demo links with the production Taipy app link, pointing to the relevant page. **Some cards have multiple demo links** (e.g., `vaep-model.md` lines 272 and 274, `xg-v2-model-card.md` lines 327 and 329) — grep within each file to catch all occurrences.

| Model Card | Demo Tab Referenced | Taipy Replacement |
|------------|-------------------|-------------------|
| `xg-v2-model-card.md` | Shot Map | Soccer Analytics App (Shot Map) |
| `vaep-model.md` | Pass Quality | Soccer Analytics App (AI/ML Workflows) |
| `football2vec-v2-model-card.md` | Player Similarity | Soccer Analytics App (Player Similarity) |
| `football2vec-360-model-card.md` | Player Similarity | Soccer Analytics App (Player Similarity) |
| `psxg-model.md` | Shot Map | Soccer Analytics App (Shot Map) |
| `pitch-control.md` | Pitch Control | Soccer Analytics App (Pitch Control) |

**Link format**: `[Soccer Analytics App](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)` with the page name in surrounding context.

### Dataset cards (18 files)

All in `docs/huggingface/dataset-cards/`. Same treatment — replace demo links with the matching Taipy page. **Some cards have multiple demo links** — grep within each file to catch all occurrences.

| Dataset Card | Taipy Page |
|-------------|-----------|
| `xg-shot-data.md` | Shot Map |
| `xg-freeze-frame-data.md` | Shot Map |
| `statsbomb-shots-on-target.md` | Shot Map |
| `spadl-vaep-action-values.md` | AI/ML Workflows |
| `space-creation-values.md` | AI/ML Workflows |
| `scoutgpt-training-data.md` | Player Similarity |
| `psxg-predictions.md` | Shot Map |
| `pitch-control-tracking.md` | Pitch Control |
| `obso-trained-grids.md` | AI/ML Workflows |
| `obso-pausa-values.md` | Pass Timing |
| `obso-pausa-inputs.md` | Pass Timing |
| `line-breaking-passes.md` | Pass Map |
| `football2vec-360-embeddings.md` | Player Similarity |
| `football2vec-training-data.md` | Player Similarity |
| `football2vec-statsbomb-wyscout.md` | Player Similarity |
| `football2vec-player-embeddings.md` | Player Similarity |
| `football2vec-360-training-data.md` | Player Similarity |
| `expected-threat-grids.md` | AI/ML Workflows |

## Untouched — Historical documents

The following files mention "demo" in the context of historical plans and specs. These are records of past decisions and are **not edited**:

- `docs/superpowers/specs/2026-03-15-model-ops-and-event-sync-design.md`
- `docs/superpowers/specs/2026-03-18-taipy-spike-design.md`
- `docs/superpowers/specs/2026-03-22-taipy-parity-sweep-design.md`
- `docs/superpowers/specs/2026-03-26-hf-buckets-auto-refresh-design.md`
- `docs/superpowers/specs/2026-03-29-documentation-audit-remediation-design.md`
- `docs/superpowers/plans/2026-03-11-hf-hub-expansion.md`
- `docs/superpowers/plans/2026-03-12-interactive-analytics.md`
- `docs/superpowers/plans/2026-03-15-model-ops-and-event-sync.md`
- `docs/superpowers/plans/2026-03-22-taipy-parity-sweep.md`
- `docs/superpowers/plans/2026-03-24-analytics-quality-cycle-q1.md`
- `docs/superpowers/plans/2026-03-26-hf-buckets-auto-refresh.md`
- `docs/superpowers/plans/2026-03-29-documentation-audit-remediation.md`
- `docs/superpowers/plans/2026-04-09-kirk-voss-audit-fixes.md`
- `docs/superpowers/plans/2026-04-29-pr-ll2-path-b-close-out.md`
- `docs/superpowers/specs/2026-04-07-evolve-level2-code-evolution-design.md`
- `docs/superpowers/specs/2026-04-23-ev2-football2vec-l2-adversarial-design.md`
- `docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md`
- `docs/superpowers/specs/2026-05-10-ll3-co-design.md`
- `docs/decisions/taipy-selection.md`
- `docs/evolve/scoutgpt-l2-harvest/SUMMARY.md`
- `docs/evolve/ev1-football2vec/SUMMARY.md`

## Verification

After all changes:

1. `ruff check src/ scripts/` — no new lint errors
2. `ruff format --check src/ scripts/` — formatting clean
3. `pyright src/` — no new type errors
4. `uv run pytest src/tests/test_hf_publish_parity.py -v` — parity test passes without the demo exclusion
5. `grep -ri "soccer-analytics-demo" --include="*.py" --include="*.md" --include="*.toml" --include="*.yml" --include="*.json"` — zero hits outside historical specs/plans
6. Confirm HF Space returns 404 after deletion

## Review feedback incorporated

| ID | Concern | Resolution |
|----|---------|------------|
| H1 | ARCHITECTURE.md line ~47 wrong | Fixed — actual hits are lines ~683, ~692, ~731 |
| H2 | "Gradio or Streamlit?" | Verified Gradio — `app.py` imports `gradio as gr`, README `sdk: gradio` |
| H3 | docs/getting-started.md has "live demo" label | Moved from untouched to edited — relabel to "Interactive dashboard" |
| M1 | No archive before irreversible deletion | Added `snapshot_download` archive step before `delete_repo` |
| M2 | No tombstone/redirect for bookmarked URLs | Rejected — tombstone Space requires ongoing maintenance for zero value. Hard 404 is intentional. |
| M3 | CLAUDE.md line ~176 missed | Added — "Taipy or Gradio" → "Taipy" |
| M4 | AI_GOVERNANCE.md / CLAUDE.md not in edits table format | Promoted to `### File / **Change**:` format |
| M5 | No CHANGELOG.md entry | Rejected — project has no CHANGELOG.md; uses git history + PR descriptions |
| M6 | Execution ordering unspecified | Added "Execution Order" section with 5-step safe sequence |
| L1 | Multiple demo links per card | Added note to grep within each file |
| L2 | .yaml in verification grep | Rejected — all workflow files use `.yml` |
| L3 | pyproject.toml formatting after removal | Verified clean — no formatting risk |
| L4 | test_hf_publish_parity.py edit scope vague | Clarified — docstring-only edit (lines 22-24), no code constants affected |
| M1v2 | HF Bucket not archived before deletion | Added `snapshot_download` for `demo-data` bucket alongside Space archive in step 4 |
| M2v2 | CLAUDE.md line ~185 conditional unresolved | Resolved — keep the bullet (refers to production app), remove only "Gradio" text |
| M3v2 | Footer target value unspecified | Added explicit target: `'[Published Datasets](https://huggingface.co/luxury-lakehouse)'` |
| M4v2 | AI_GOVERNANCE line ~58 edit non-deterministic | Made deterministic — quotes exact new text `"Published Datasets" in its site footer` |
| L1v2 | `line-breaking-passes.md` → Pass Map mapping unverified | Verified — `hf_taipy_app/src/pages/pass_map.py` exists with `title="Pass Map"` |
| L2v2 | `.json` files not in verification grep | Added `--include="*.json"` to grep. Verified zero `.json` hits for `soccer-analytics-demo` |
