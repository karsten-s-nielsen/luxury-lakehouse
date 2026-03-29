# Documentation Audit Remediation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 42 findings from the documentation audit (LAKEHOUSE-DOC-AUDIT-1_12_0.md)

**Architecture:** Six thematic groups executed sequentially (G1→G6). Documentation-only changes — no source code modifications. ~15 existing files modified, 8 new files created.

**Tech Stack:** Markdown, YAML frontmatter (HuggingFace Hub format)

**Spec:** `docs/superpowers/specs/2026-03-29-documentation-audit-remediation-design.md`

---

## Verified Ground Truth

| Item | Correct Value |
|---|---|
| StatsBomb coordinate origin | **Bottom-left** |
| `distance_to_goal` unit | **Yards** |
| xG v2 repo ID | **`xg-v2-model-set-encoder`** |
| Page count | **14** |
| StatsBomb shot count | **~95K** |

---

## Task 1: G1 — Critical Spatial & Identity Fixes

**Findings:** #1, #2, #3, #4, #11
**Files:**
- Modify: `docs/huggingface/xg-v2-model-card.md`
- Modify: `docs/huggingface/xg-model-card.md`

- [ ] **Step 1: Fix coordinate origin in xg-v2-model-card.md (Finding #1)**

In `docs/huggingface/xg-v2-model-card.md`, change line 157:

```
Old: - Origin: top-left corner of the pitch
New: - Origin: bottom-left corner of the pitch
```

- [ ] **Step 2: Fix distance unit in xg-v2-model-card.md (Finding #2)**

In `docs/huggingface/xg-v2-model-card.md`, change line 113:

```
Old: | `distance_to_goal` | Numeric | Euclidean distance from shot location to goal center (meters) |
New: | `distance_to_goal` | Numeric | Euclidean distance from shot location to goal center (yards) |
```

- [ ] **Step 3: Fix wrong v1 link in xg-v2-model-card.md (Finding #3)**

In `docs/huggingface/xg-v2-model-card.md`, change line 310:

```
Old: - **v1 baseline model**: [Football2Vec](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout)
New: - **v1 baseline model**: [xG v1 (XGBoost)](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout)
```

- [ ] **Step 4: Fix repo ID in xg-v2-model-card.md inference code (Finding #4)**

In `docs/huggingface/xg-v2-model-card.md`, change line 180:

```
Old:     repo_id="luxury-lakehouse/xg-v2-set-encoder",
New:     repo_id="luxury-lakehouse/xg-v2-model-set-encoder",
```

- [ ] **Step 5: Fix license in xg-model-card.md (Finding #11)**

In `docs/huggingface/xg-model-card.md`, change YAML frontmatter line 4:

```
Old: license: mit
New: license: cc-by-nc-4.0
```

Also update the "More Information" section at line 232:

```
Old: - **License**: [MIT](https://opensource.org/licenses/MIT)
New: - **License**: [CC-BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) (inherited from Wyscout training data)
```

- [ ] **Step 6: Verify all G1 changes**

Read both files and confirm:
- xg-v2-model-card.md: "bottom-left" at line 157, "(yards)" at line 113, correct v1 link at line 310, correct repo ID at line 180
- xg-model-card.md: `license: cc-by-nc-4.0` in YAML, CC-BY-NC 4.0 in More Information

---

## Task 2: G2 — Rewrite xg-freeze-frame.md

**Finding:** #9
**Files:**
- Modify: `docs/huggingface/dataset-cards/xg-freeze-frame.md` (rewrite from 37-line stub to full card)

- [ ] **Step 1: Rewrite xg-freeze-frame.md**

Replace the entire file with:

```markdown
---
language: [en]
license: cc-by-4.0
task_categories:
  - tabular-classification
tags:
  - sports-analytics
  - soccer
  - football
  - expected-goals
  - freeze-frames
  - statsbomb
  - deep-sets
size_categories:
  - 10M<n<100M
configs:
  - config_name: default
    data_files:
      - split: train
        path: "data/**/*.parquet"
---

# xG Freeze-Frame Data &mdash; StatsBomb 360

**~15.58M freeze-frame rows** from 323 StatsBomb 360 matches, capturing player positions at the moment of each shot. Each row represents one visible player in one shot event.

Part of the (Right! Luxury!) Lakehouse soccer analytics platform.

## Quick Start

```python
from datasets import load_dataset

ds = load_dataset("luxury-lakehouse/xg-freeze-frame-data")
df = ds["train"].to_pandas()

# Average number of visible players per shot
df.groupby("event_id").size().describe()
```

> **Explore interactively:** [HF Space demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

## What Is This Dataset?

StatsBomb 360 data includes inline freeze frames for each event &mdash; a snapshot of every visible player's position at the instant of the event. This dataset extracts freeze-frame rows specifically for shot events, providing the spatial context that the [xG v2 set encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) uses to condition expected goals predictions on defensive positioning.

Each row represents one player visible in one shot. A single shot typically has 10&ndash;22 freeze-frame rows (one per visible player). The set encoder aggregates these into a fixed-length context vector using permutation-invariant sum pooling.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | `string` | Shot event ID (FK to xG Shot Data) |
| `match_id` | `bigint` | Match identifier |
| `competition_id` | `bigint` | Competition identifier |
| `season_id` | `bigint` | Season identifier |
| `player_x_norm` | `double` | Player x position normalized to [0, 1] from StatsBomb 120-yard pitch |
| `player_y_norm` | `double` | Player y position normalized to [0, 1] from StatsBomb 80-yard pitch |
| `is_keeper` | `boolean` | Whether the player is the goalkeeper |
| `is_teammate` | `boolean` | Whether the player is on the shooting team |

### Coordinate System

Raw positions are in the **StatsBomb coordinate system** (120 &times; 80 yards, origin at bottom-left, attacking direction left to right). The `player_x_norm` and `player_y_norm` columns are normalized:

```
x_norm = location_x / 120.0
y_norm = location_y / 80.0
```

## Data Sources

| Source | Coverage | License |
|--------|----------|---------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) (360 subset) | ~323 matches with inline freeze frames | CC-BY 4.0 |

Only StatsBomb 360 matches include freeze-frame data. Non-360 StatsBomb matches and all Wyscout matches do not contribute to this dataset. The xG v2 model handles missing freeze frames by falling back to a zero context vector.

## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [xG v2 Set Encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Model | Deep Sets encoder + MLP that consumes this dataset as spatial context |
| [xG Shot Data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | Dataset | Tabular shot features (joins on `event_id`) |
| [xG Model v1](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Model | Tabular-only XGBoost baseline (no freeze-frame context) |

## Limitations

- **Partial visibility**: StatsBomb 360 captures only *visible* players. Players behind the camera or in crowded areas may be absent. The set encoder handles this gracefully (fewer rows per shot), but predictions may underestimate defensive pressure when defenders are occluded.
- **StatsBomb 360 only**: Covers ~323 of ~3,000 StatsBomb matches. The majority of shots in the xG Shot Data dataset do not have corresponding freeze-frame rows.
- **Shot events only**: This dataset extracts freeze frames for shots specifically. Freeze frames for other event types (passes, tackles) are not included.
- **No player identity**: The dataset includes spatial position and role flags only. Player name, jersey number, height, and other attributes are not captured.

## Citation

If you use this dataset, please cite StatsBomb and the Deep Sets architecture:

```bibtex
@misc{statsbomb2024opendata,
  title={StatsBomb Open Data},
  author={{StatsBomb}},
  year={2024},
  url={https://github.com/statsbomb/open-data},
  note={CC-BY 4.0}
}
```

```bibtex
@inproceedings{zaheer2017deep,
  title={Deep Sets},
  author={Zaheer, Manzil and Kottur, Satwik and Ravanbakhsh, Siamak
          and P{\'o}czos, Barnab{\'a}s and Salakhutdinov, Ruslan
          and Smola, Alexander J.},
  booktitle={Advances in Neural Information Processing Systems (NeurIPS)},
  volume={30},
  year={2017}
}
```

## More Information

- **License**: [CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Publishing script**: `scripts/publish_xg_shots_hf.py`
- **Platform**: [Luxury Lakehouse Soccer Analytics](https://github.com/karsten-s-nielsen/luxury-lakehouse)
```

- [ ] **Step 2: Verify the rewrite**

Read the file and confirm: YAML frontmatter has `language: [en]`, all 7 sections present (Quick Start, What Is, Schema, Data Sources, Companion Resources, Limitations, Citation), coordinate system documented as bottom-left.

---

## Task 3: G2 — New Dataset Card: expected-threat-grids

**Finding:** #10
**Files:**
- Create: `docs/huggingface/dataset-cards/expected-threat-grids.md`

- [ ] **Step 1: Create expected-threat-grids.md**

Write the full file. Source data: `scripts/compute_xt_grid_hf.py` (lines 369-421), `src/analytics/expected_threat.py`. Grid shape: 12x8 (96 cells per competition). SPADL 105x68m coordinate system. License: MIT. Citation: Karun Singh (2018).

The card must follow the established template: YAML frontmatter, description, Quick Start, Schema, Coordinate System, Data Sources, Companion Resources, Limitations, Citation, More Information.

Schema columns from the publish script: `zone_x` (int, 0-11), `zone_y` (int, 0-7), `xt_value` (float), `competition_id` (string, or "global").

- [ ] **Step 2: Verify the card**

Read the file and confirm all sections present, schema matches source code, coordinate system documented.

---

## Task 4: G2 — New Dataset Card: obso-pausa-inputs

**Finding:** #10
**Files:**
- Create: `docs/huggingface/dataset-cards/obso-pausa-inputs.md`

- [ ] **Step 1: Create obso-pausa-inputs.md**

Source data: `notebooks/publish_obso_data.py` (lines 117-216). Two configs: `events` and `elastic_sync`. License: MIT (computed from IDSSE CC-BY 4.0). Citations: Bassek et al. (2025) for IDSSE, Kim et al. (2025) for ELASTIC.

Events schema: `match_id`, `event_id`, `event_type`, `timestamp_seconds`, `period`, `player_id`, `team`, `x`, `y` (DFL 105x68m coordinates).

ELASTIC sync schema: `match_id`, `event_id`, `frame_id`, `alignment_confidence`, `alignment_error_seconds`.

- [ ] **Step 2: Verify the card**

---

## Task 5: G2 — New Dataset Card: obso-pausa-values

**Finding:** #10
**Files:**
- Create: `docs/huggingface/dataset-cards/obso-pausa-values.md`

- [ ] **Step 1: Create obso-pausa-values.md**

Source data: `scripts/compute_obso_hf.py` (lines 677-752). License: MIT (computed from IDSSE CC-BY 4.0). Citations: Spearman (2018), Fernandez & Bornn (2018), Lee et al. (2026) for PAUSA, Kim et al. (2025), Bassek et al. (2025).

Schema from lines 558-579: `match_id`, `pass_id`, `event_id`, `player_id`, `team`, `period`, `timestamp_seconds`, `frame_id`, `ball_x`, `ball_y`, `receiver_x`, `receiver_y`, `actual_obso`, `peak_obso`, `optimal_obso`, `temporal_judgment` (0-1, higher = better timing), `spatial_selection` (0-1, higher = better target), `alignment_confidence`.

Grid: 104x68 cells, StatsBomb 120x80 coordinate system. Ghost window: 3.0s before to 1.0s after at 25fps.

- [ ] **Step 2: Verify the card**

---

## Task 6: G2 — New Dataset Card: obso-trained-grids

**Finding:** #10
**Files:**
- Create: `docs/huggingface/dataset-cards/obso-trained-grids.md`

- [ ] **Step 1: Create obso-trained-grids.md**

Source data: `scripts/compute_epv_transition_hf.py` (lines 960-1091). License: MIT (derived from StatsBomb + Wyscout via SPADL). Citations: Singh (2018) for xT, Spearman (2018), Fernandez & Bornn (2018), Lee et al. (2026).

Three data files with different schemas:
1. `reachability_grid_global.parquet`: `zone_y` (0-99), `zone_x` (0-63), `reachability` (float 0-1). Grid: 100x64.
2. `epv_grid_global.parquet`: `zone_y` (0-49), `zone_x` (0-31), `epv_value` (float 0-1). Grid: 50x32.
3. `completion_matrix_global.parquet`: `origin_zone` (int), `target_zone` (int), `probability` (float). 25x16=400 zones.

Per-competition variants also included (`*_all.parquet` with `competition_id` column).

- [ ] **Step 2: Verify the card**

---

## Task 7: G2 — New Dataset Card: space-creation-values

**Finding:** #10
**Files:**
- Create: `docs/huggingface/dataset-cards/space-creation-values.md`

- [ ] **Step 1: Create space-creation-values.md**

Source data: `scripts/compute_space_creation_hf.py` (lines 814-887). License: MIT (computed from IDSSE CC-BY 4.0). Citations: Fernandez & Bornn (2018), Spearman (2018), Bassek et al. (2025).

Schema from lines 665-676: `match_id`, `frame_id`, `player_id`, `team`, `period`, `space_created_m2` (>= 0), `space_destroyed_m2` (<= 0), `net_space_m2` (positive = beneficial).

Grid: 52x34, cell area ~4.04 m². Frame sampling: every 25th frame (1fps from 25fps source).

- [ ] **Step 2: Verify the card**

---

## Task 8: G2 — Consistency Fixes Across Existing Cards

**Findings:** #19, #34, #35, #41
**Files:**
- Modify: `docs/huggingface/dataset-cards/xg-shot-data.md`
- Modify: `docs/huggingface/dataset-cards/spadl-vaep.md`
- Modify: `docs/huggingface/dataset-cards/pitch-control.md`
- Modify: `docs/huggingface/xg-v2-model-card.md`
- Modify: `docs/huggingface/xg-model-card.md`
- Modify: `docs/huggingface/model-cards/vaep-model.md`
- Modify: `docs/huggingface/model-card.md`

- [ ] **Step 1: Fix coordinate origin in xg-shot-data.md (Finding #1 propagation)**

In `docs/huggingface/dataset-cards/xg-shot-data.md`, change line 85:

```
Old: - Origin: top-left corner of the pitch
New: - Origin: bottom-left corner of the pitch
```

- [ ] **Step 2: Fix StatsBomb shot count in xg-shot-data.md (Finding #19)**

In `docs/huggingface/dataset-cards/xg-shot-data.md`, change line 25:

```
Old: (~88K)
New: (~95K)
```

Also fix the Data Sources table at line 94:

```
Old: | [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~88K | ~3,000 | CC-BY 4.0 |
New: | [StatsBomb Open Data](https://github.com/statsbomb/open-data) | ~95K | ~3,000 | CC-BY 4.0 |
```

- [ ] **Step 3: Standardize heading "Companion Datasets" → "Companion Resources" (Finding #35)**

In all 4 model cards, rename:

```
Old: ## Companion Datasets
New: ## Companion Resources
```

Files: `docs/huggingface/xg-v2-model-card.md`, `docs/huggingface/xg-model-card.md`, `docs/huggingface/model-cards/vaep-model.md`, `docs/huggingface/model-card.md`.

- [ ] **Step 4: Add cross-links in spadl-vaep.md (Finding #41)**

In `docs/huggingface/dataset-cards/spadl-vaep.md`, in the "More Information" section, add a Companion Resources section (or append to existing) linking to the VAEP model card:

```markdown
## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [VAEP Model](https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout) | Model | P(scores) + P(concedes) XGBClassifiers trained on this dataset |
| [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | Dataset | Behavioral + statistical vectors derived from SPADL actions |
```

- [ ] **Step 5: Add cross-links in pitch-control.md (Finding #41)**

In `docs/huggingface/dataset-cards/pitch-control.md`, in the "More Information" section, add a Companion Resources section:

```markdown
## Companion Resources

| Resource | Type | Description |
|----------|------|-------------|
| [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | Dataset | Off-ball scoring opportunities computed from pitch control surfaces |
| [Space Creation Values](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values) | Dataset | Per-player space creation using differential pitch control |
```

- [ ] **Step 6: Verify all G2 consistency changes**

Grep for "Companion Datasets" across all cards — should return 0 matches (all renamed to "Companion Resources"). Grep for "~88K" — should return 0 matches.

---

## Task 9: G3 — README.md Fixes

**Findings:** #6, #8, #23, #25, #40
**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add CI badge (Finding #40)**

Add a GitHub Actions CI badge after the existing badges in the header area. Find the badges section and append:

```markdown
[![CI](https://github.com/karsten-s-nielsen/luxury-lakehouse/actions/workflows/python-ci.yml/badge.svg)](https://github.com/karsten-s-nielsen/luxury-lakehouse/actions/workflows/python-ci.yml)
```

- [ ] **Step 2: Fix page count to 14 (Finding #8)**

Search for "14-page" — should already be present in some places. Search for any references to "12-page" or "13-page" and fix to "14-page". Also fix the test count and status blurb.

- [ ] **Step 3: Update status blurb (Finding #23)**

Change the status blurb from "Phase 19 complete" to "Phase 20 complete":

```
Old: Phase 19 complete + GPU models v2
New: Phase 20 complete (Taipy Migration) + GPU models v2
```

- [ ] **Step 4: Fix test count (Finding #25)**

Ensure all references to test counts say 807 (not 704 or any other number).

- [ ] **Step 5: Add Getting Started section (Finding #6)**

Add a new section after the appropriate location in README.md:

```markdown
## Getting Started

See the [Getting Started guide](docs/getting-started.md) for local setup (clone, install, verify), or try the [live demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) immediately. For pre-trained model usage, see the [Hugging Face setup guide](docs/huggingface-setup.md).
```

- [ ] **Step 6: Verify README changes**

Read README.md and confirm: CI badge present, page count is 14 everywhere, status says "Phase 20 complete", test count is 807, Getting Started section links to docs/getting-started.md.

---

## Task 10: G3 — ARCHITECTURE.md Fixes

**Findings:** #14, #25, #37, #38
**Files:**
- Modify: `ARCHITECTURE.md`

- [ ] **Step 1: Add hf_taipy_app/ to directory tree (Finding #14)**

In the "Repository Structure" section, add `hf_taipy_app/` to the directory tree and annotate `src/streamlit_app/` as deprecated. The exact edits depend on the current tree structure — find the app-related entries and add:

```
hf_taipy_app/             # Production Taipy dashboard (14 pages, HF Spaces)
│   ├── src/pages/        # Page state modules
│   ├── src/state/        # State variables and callbacks
│   ├── src/template.py   # Page template builder
│   └── Dockerfile        # HF Spaces Docker deployment
```

And annotate the existing streamlit entry:

```
Old: src/streamlit_app/         # Streamlit dashboard
New: src/streamlit_app/         # [DEPRECATED] Streamlit dashboard — retained during Taipy transition
```

- [ ] **Step 2: Fix dataset card count (Finding #37)**

```
Old: dataset-cards/ # HF Hub dataset cards (5 datasets)
New: dataset-cards/ # HF Hub dataset cards (11 datasets)
```

- [ ] **Step 3: Add missing model card entries to docs tree (Finding #38)**

Add `xg-v2-model-card.md` and `model-cards/vaep-model.md` to the docs section of the directory tree.

- [ ] **Step 4: Fix test count (Finding #25)**

```
Old: pytest (704 passed)
New: pytest (807 passed)
```

- [ ] **Step 5: Verify ARCHITECTURE.md changes**

Read the directory tree section and test count section. Confirm all changes are applied.

---

## Task 11: G3 — HF Space READMEs

**Findings:** #8, #29
**Files:**
- Modify: `hf_taipy_app/README.md`
- Modify: `demo_space/README.md`

- [ ] **Step 1: Fix hf_taipy_app/README.md page count (Finding #8)**

```
Old: short_description: 13-page soccer analytics dashboard on Lakebase
New: short_description: 14-page soccer analytics dashboard on Lakebase
```

```
Old: 12 analysis pages covering 380+ matches across 5 data providers.
New: 14 analysis pages covering 380+ matches across 5 data providers.
```

- [ ] **Step 2: Update demo_space/README.md artifact list (Finding #29)**

Read the current published artifacts section in `demo_space/README.md` and update it to include all 11 datasets and 4 models matching `docs/huggingface/org-card.md`. Add the missing items: `xg-shot-data`, `xg-freeze-frame-data`, `xg-v2-model-set-encoder`, `vaep-model-statsbomb-wyscout`, `expected-threat-grids`, `obso-pausa-inputs`, `obso-pausa-values`, `obso-trained-grids`, `space-creation-values`.

- [ ] **Step 3: Verify both files**

---

## Task 12: G4 — huggingface-setup.md Overhaul

**Findings:** #7, #13, #15, #16, #27, #28, #32
**Files:**
- Modify: `docs/huggingface-setup.md`

- [ ] **Step 1: Read current file**

Read the full file to understand current structure before rewriting.

- [ ] **Step 2: Add measurable objective (Finding #27)**

Add after the title:

```markdown
> **After this guide you will have:** (1) loaded a pre-trained football2vec embedding and verified its shape, (2) retrained on your own data and confirmed the output, (3) published artifacts to HuggingFace Hub.
```

- [ ] **Step 3: Add prerequisites block (Finding #15)**

Add a new section before the first procedural section:

```markdown
## Prerequisites

| Term | Definition |
|------|-----------|
| **UC Volume** | Databricks Unity Catalog storage volume — a managed cloud storage path for files |
| **Databricks Connect** | SDK for connecting local Python to a remote Databricks cluster |
| **SPADL** | Simplified Player Action Description Language — a unified event format (Decroos et al. 2019) |
| **Per-90 stats** | Player statistics normalized to 90-minute match equivalents |
| **Doc2Vec / PV-DM** | Paragraph Vector Distributed Memory — a document embedding algorithm (Le & Mikolov 2014) |
| **gensim** | Python library for topic modeling and document similarity |
| **Secret scope** | Databricks-managed key vault for storing credentials (e.g., HF tokens) |
| **canonical_player_id** | Platform's deduplicated player identifier across data sources |
| **HF write token** | HuggingFace Hub token with write permission for publishing artifacts |
| **z-score normalization** | Statistical standardization: (value - mean) / std_dev |

**Required tools:**
- Python >=3.10, <3.11 (strict — Databricks serverless constraint; 3.11+ will fail)
- `gensim>=4.3.0`, `huggingface_hub>=1.5.0` (included in project dependencies)
```

- [ ] **Step 4: Fix Python version (Finding #16)**

Replace all instances of "Python 3.10+" with "Python >=3.10, <3.11 (strict — Databricks serverless constraint)".

- [ ] **Step 5: Fix huggingface_hub version (Finding #13)**

```
Old: huggingface_hub>=0.20
New: huggingface_hub>=1.5.0
```

- [ ] **Step 6: Add verification commands (Finding #7)**

After each procedural step in the guide, add verification. For example, after "1. Use the Pre-Trained Model":

```markdown
**Verify:** Run the Quick Start code above. You should see:
```python
print(f"Vector shape: {vector.shape}")  # Expected: (32,)
print(f"Top 3 similar: {[p for p, _ in similar[:3]]}")  # Expected: 3 player names
```

After "2. Retrain on Your Data" steps, add:

```markdown
**Verify:** Check the embedding table was populated:
```python
# In Databricks notebook
display(spark.table("soccer_analytics.dev_gold.fct_player_embeddings").count())
# Expected: >0 rows
```

After "3. Publish to HuggingFace Hub":

```markdown
**Verify:** Confirm the upload:
```python
from huggingface_hub import list_repo_files
files = list_repo_files("your-org/football2vec-statsbomb-wyscout")
print(files)  # Expected: ['model', 'z_score_params.json', ...]
```

- [ ] **Step 7: Add error recovery (Finding #28)**

Add a new section at the end:

```markdown
## Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `AuthenticationError` on HF Hub push | Missing or invalid HF token | Run `huggingface-cli login` with a write token, or set the Databricks secret: `databricks secrets put-secret hf token` |  <!-- pragma: allowlist secret -->
| `WAREHOUSE_NOT_RUNNING` or timeout on SQL queries | SQL warehouse auto-stopped after 10 min idle | Run `python scripts/ensure_warehouse.py` before any Databricks operation |
| `FileNotFoundError` on UC Volume path | Model weights not yet written to the Volume | Run the training notebook first (`notebooks/train_football2vec.py`), or download from HF Hub with `snapshot_download` |
```

- [ ] **Step 8: Fix condition-after-instruction patterns (Finding #32)**

Find instances where conditions follow instructions and reorder. For example:

```
Old: The training notebook automatically publishes to HF Hub if the Databricks secret scope `hf` / key `token` is configured.
New: If you have configured the Databricks secret scope `hf` / key `token`, the training notebook automatically publishes to HF Hub.
```

- [ ] **Step 9: Verify the overhaul**

Read the full file. Confirm: objective block at top, prerequisites table, verification after each section, error recovery table, Python version correct, huggingface_hub version correct, no condition-after-instruction patterns remain.

---

## Task 13: G4 — New Getting Started Tutorial

**Finding:** #5
**Files:**
- Create: `docs/getting-started.md`

- [ ] **Step 1: Create docs/getting-started.md**

Write the full Tutorial-quadrant document. Structure: measurable objective, prerequisites, clone + install with verification, verify environment (ruff, pyright, pytest), explore the project (pointers to key docs), next steps, common issues table.

The guide must be Databricks-independent — a fork user can verify the local environment without cloud credentials.

Key verification commands:
- `uv run python --version` → expect 3.10.x
- `uv run ruff check src/` → expect 0 violations
- `uv run pyright src/` → expect 0 errors
- `uv run pytest src/tests/ -x -q` → expect passes

- [ ] **Step 2: Verify the tutorial**

Read the file. Confirm: measurable objective stated, prerequisites listed, verification after every 2-3 steps, error recovery table for common issues, no Databricks dependency.

---

## Task 14: G5 — Linguistic & Style Sweep

**Findings:** #12, #17, #18, #21, #30, #31, #42
**Files:**
- Modify: `ROADMAP.md`
- Modify: `ARCHITECTURE.md`
- Modify: `docs/huggingface/org-interests.md`
- Modify: `docs/huggingface/org-card.md`
- Modify: `README.md`
- Modify: `docs/huggingface/model-cards/vaep-model.md`
- Modify: `docs/huggingface/dataset-cards/xg-shot-data.md`
- Modify: `docs/huggingface/xg-v2-model-card.md`
- Modify: `docs/huggingface-setup.md`

- [ ] **Step 1: Fix temporal language (Finding #12)**

In `ROADMAP.md` line ~14:
```
Old: currently has minimal observability
New: has minimal observability
```

In `ROADMAP.md` line ~622:
```
Old: Currently the platform has a single `dev` environment
New: The platform has a single `dev` environment
```

In `ARCHITECTURE.md` line ~599:
```
Old: Currently 41 btree indexes
New: The platform has 41 btree indexes
```

In `docs/huggingface/org-interests.md` line ~7:
```
Old: No API endpoint currently exists
New: No API endpoint exists
```

- [ ] **Step 2: Fix non-inclusive language (Finding #17)**

In `ROADMAP.md` line ~261:
```
Old: dummy tensors
New: placeholder tensors
```

- [ ] **Step 3: Standardize brand name (Finding #18)**

Use Grep to find all instances of "HuggingFace" (single word) in user-facing `.md` files and replace with "Hugging Face" (two words). Keep package identifiers (`huggingface_hub`, `huggingface.co`, `huggingface-cli`) unchanged.

Files to check: `README.md`, `ARCHITECTURE.md`, `docs/huggingface-setup.md`.

- [ ] **Step 4: Expand domain acronyms in README (Finding #21)**

In the analytics section of `README.md`, expand acronyms on first use:

- "VAEP" → "VAEP (Valuing Actions by Estimating Probabilities)"
- "SPADL" → "SPADL (Simplified Player Action Description Language)"
- "PPDA" → "PPDA (Passes Per Defensive Action)"
- "HSR" → "HSR (High-Speed Running)"
- "OBSO" → "OBSO (Off-Ball Scoring Opportunities)"
- "PAUSA" → "PAUSA (Passing Ability Under Spatiotemporal Awareness)"
- "EPTS" → "EPTS (Electronic Performance and Tracking Systems)"

- [ ] **Step 5: Fix 52-word sentence in org-card (Finding #30)**

In `docs/huggingface/org-card.md` line ~113, split:

```
Old: 71 of 78 findings resolved across two cognitive interface audits (CHI-AUDIT-180, CHI-AUDIT-190), grounded in 15 HCI frameworks (Norman, Sweller, Gergle, Kahneman, Cleveland & McGill, and others) — every metric has a help tooltip, every page has academic citations, every analytics term is defined in a context-sensitive glossary (Streamlit and HF Space)
New: 71 of 78 findings resolved across two cognitive interface audits (CHI-AUDIT-180, CHI-AUDIT-190), grounded in 15 HCI frameworks including Norman, Sweller, Gergle, Kahneman, and Cleveland & McGill. Every metric has a help tooltip, every page has academic citations, and every analytics term is defined in a context-sensitive glossary.
```

- [ ] **Step 6: Fix Right Is Right violations (Finding #31)**

In `README.md` status blurb (~line 123), add brief context after the metrics. For example:
```
Old: 26 synced tables, 45 PG indexes, 807 unit tests
New: 26 synced tables (Lakebase reverse-ETL), 45 PG indexes (sub-second dashboard queries), 807 unit tests
```

In `docs/huggingface/model-cards/vaep-model.md` line ~197:
```
Old: Predicted probabilities may not be perfectly calibrated in absolute terms.
New: Predicted probabilities may not be perfectly calibrated in absolute terms. For ranking players or actions, this is less consequential; for applications requiring absolute probability values, validate with a reliability diagram.
```

In `docs/huggingface/dataset-cards/xg-shot-data.md` line ~134:
```
Old: Models should account for this imbalance during training.
New: Account for this imbalance during training using class weights (`scale_pos_weight` in XGBoost) or stratified sampling.
```

In `docs/huggingface/xg-v2-model-card.md` line ~146:
```
Old: ROC-AUC improved by **+0.090** over the v1 XGBoost baseline. Isotonic calibration closed the Brier score gap to 0.003 while substantially improving log loss (1.212 → 0.200).
New: ROC-AUC improved by **+0.090** over the v1 XGBoost baseline (0.825 → 0.915) — a large gain in discrimination for xG models, where +0.02 is typically meaningful. Isotonic calibration closed the Brier score gap to 0.003 while reducing log loss sixfold (1.212 → 0.200).
```

- [ ] **Step 7: Fix passive voice (Finding #42)**

In `README.md` line ~171:
```
Old: architectural quality is maintained through Claude Code skills
New: Claude Code skills enforce architectural quality
```

- [ ] **Step 8: Verify all linguistic changes**

Grep for "currently" in user-facing docs — should return 0 in the fixed locations. Grep for "dummy tensors" — 0 matches. Grep for "HuggingFace" (single word, excluding URLs/packages) — 0 matches.

---

## Task 15: G5 — New Glossary

**Finding:** #22
**Files:**
- Create: `docs/glossary.md`

- [ ] **Step 1: Create docs/glossary.md**

Write a comprehensive glossary of all domain terms used in the documentation. Include: term name, definition, scale/direction where applicable, and a "First used in" column pointing to the doc where the term first appears.

Terms to include (at minimum): xG, xT, VAEP, SPADL, PPDA, HSR, OBSO, PAUSA, DEFCON, ELASTIC, EPTS, Pitch Control, Medallion Architecture, Lakebase, Synced Table, UC Volume, Bronze/Silver/Gold, Delta Lake, dbt, applyInPandas.

Cross-link from README.md (Getting Started section) and `docs/getting-started.md`.

- [ ] **Step 2: Add glossary link to README.md**

In the Getting Started section of README.md, add:
```markdown
For domain terminology, see the [Glossary](docs/glossary.md).
```

- [ ] **Step 3: Verify the glossary**

Read the file. Confirm all terms from Finding #21 are covered with definitions, scale/direction info where applicable.

---

## Task 16: G6 — SECURITY.md + CONTRIBUTING.md

**Findings:** #20, #26, #33, #36
**Files:**
- Modify: `SECURITY.md`
- Create: `CONTRIBUTING.md`

- [ ] **Step 1: Add vulnerability disclosure section to SECURITY.md (Finding #20)**

Add at the top of SECURITY.md, before the existing audit report content:

```markdown
## Reporting a Vulnerability

If you discover a security vulnerability, please report it through [GitHub's private vulnerability reporting](https://github.com/karsten-s-nielsen/luxury-lakehouse/security/advisories/new). Do not open a public issue.

**Response time:** This is a solo-maintained project. Expect an initial response within 7 days.

**Supported versions:** Only the current `main` branch is supported.

---

```

- [ ] **Step 2: Fix stale Streamlit references (Finding #36)**

In SECURITY.md, find and fix:
```
Old: Clear entry points: Streamlit UI, CLI ingestion, Terraform IaC
New: Clear entry points: Taipy dashboard, CLI ingestion, Terraform IaC
```

Find and fix the XSRF reference:
```
Old: XSRF protection enabled in Streamlit config
New: Taipy server-side state management (no client-side session tokens requiring XSRF protection)
```

- [ ] **Step 3: Add note to Finding I-4 about runbooks (Finding #26)**

In the existing Finding I-4 entry, append:
```
Old: Referenced runbooks (docs/runbooks/) do not exist in repo.
New: Referenced runbooks (docs/runbooks/) do not exist in repo. Status: deferred — see TODO.md for current operational procedures.
```

- [ ] **Step 4: Create CONTRIBUTING.md (Finding #33)**

Write a minimal contributing guide:

```markdown
# Contributing to (Right! Luxury!) Lakehouse

Thank you for your interest in contributing!

## Engineering Standards

All contributions must follow the engineering standards documented in [CLAUDE.md](CLAUDE.md). Key requirements:

- **Python 3.10** (strict: >=3.10, <3.11 — Databricks serverless constraint)
- **Line length**: 120 characters maximum
- **Type annotations**: All public function signatures

## Development Setup

See the [Getting Started guide](docs/getting-started.md) for local environment setup.

## Required Checks

All of these must pass before submitting a PR:

```bash
uv run ruff check src/           # Lint
uv run ruff format --check src/  # Format
uv run pyright src/              # Type check
uv run pytest src/tests/ -v      # Unit tests
```

## Pull Request Process

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all checks pass
3. Write descriptive commit messages (see git history for style)
4. Open a PR with a clear title and description of what and why

## Questions?

Open a [GitHub Discussion](https://github.com/karsten-s-nielsen/luxury-lakehouse/discussions) or reach out via the project's HuggingFace community.
```

- [ ] **Step 5: Verify G6 changes**

Read SECURITY.md and confirm: vulnerability disclosure section at top, no Streamlit references remain, Finding I-4 has status note. Read CONTRIBUTING.md and confirm: links to CLAUDE.md and getting-started.md, required checks listed, no placeholder content.

---

## Parallelization Guide

| Tasks | Can Parallelize? | Notes |
|-------|-----------------|-------|
| Tasks 3, 4, 5, 6, 7 | **Yes** | 5 independent new dataset cards |
| Task 1, Task 2 | **Yes** | Different files, no overlap |
| Tasks 9, 10, 11 | Partially | All in G3, but touch different files |
| Task 8 | After Tasks 1-2 | Depends on G1 spatial fixes being done first |
| Tasks 14, 15 | Partially | Task 15 (glossary) independent; Task 14 touches README which Task 9 also touches |
| Task 16 | **Yes** | Fully independent of all other tasks |
| Task 12, 13 | **Yes** | Different files (setup.md vs getting-started.md) |

**Recommended parallel batches:**
1. Tasks 1 + 2 + 3 + 4 + 5 + 6 + 7 + 16 (all independent)
2. Tasks 8 + 9 + 10 + 11 + 12 + 13 (after batch 1, G2 consistency needs G1 done)
3. Tasks 14 + 15 (after batch 2, linguistic sweep needs README structure finalized)

---

## Commit Strategy

No commits during implementation — all changes staged for user review. When approved:

```bash
git add docs/ README.md ARCHITECTURE.md ROADMAP.md SECURITY.md CONTRIBUTING.md \
       hf_taipy_app/README.md demo_space/README.md
git commit -m "docs: fix all 42 findings from documentation audit v1.12.0

Remediate all findings from LAKEHOUSE-DOC-AUDIT-1_12_0.md:
- Fix Critical spatial metadata (coordinate origin, distance units, repo IDs, license)
- Write 5 new dataset cards + rewrite xg-freeze-frame stub
- Add Getting Started tutorial, glossary, and CONTRIBUTING.md
- Overhaul huggingface-setup.md with verification commands and error recovery
- Fix temporal language, brand name, acronym expansions, Right Is Right violations
- Add vulnerability disclosure policy to SECURITY.md

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```
