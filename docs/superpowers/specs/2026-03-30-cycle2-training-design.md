# Cycle 2 — "Training" Design Spec

**Date**: 2026-03-30
**Scope**: D18 + D30 + D36 + D37
**Branch**: Single feature branch from `main`
**Execution order**: Track B first (D36 → D37), then Track A (D18 → D30)
**Commits**: Pending user approval — one or two commits (possibly one per track)

---

## Track B — Shape Graphs (D36 + D37)

### D36 — Shape Graph Algorithm

**Source**: Sotudeh, H. (2026). *Identification of Team Tactical Formations and Player Positions in Association Football.* PhD thesis, ETH Zurich (DISS. ETH NO. 31732). Published: [npj Complexity, DOI: 10.1038/s44260-025-00047-x](https://doi.org/10.1038/s44260-025-00047-x).

#### New module: `src/analytics/shape_graph.py`

Core functions:

1. **`compute_shape_graph(positions: np.ndarray) -> ShapeGraph`** — Sotudeh Algorithm 1. Takes (n, 2) outfield player coordinates. Computes Delaunay triangulation, calculates angular stability for each edge (angle between circumcenters of incident triangles), iteratively removes the least stable edge (threshold < 45°) and merges incident faces, recomputes stabilities, repeats until all edges are stable. Returns the sparse, stable Delaunay subgraph.

2. **`infer_positions(shape_graph: ShapeGraph, positions: np.ndarray, attacking_direction: float) -> list[PositionLabel]`** — Level decomposition. Computes vertical levels (B/DM/M/AM/F) and horizontal levels (L/LC/C/RC/R) from internal face centers of the shape graph. Maps each player's (vertical, horizontal) pair to one of 25 tactical position labels.

3. **`angular_stability(edge, triangulation) -> float`** — Stability metric for a single edge. Internal but separately testable.

#### Data types

Frozen dataclasses (or Pydantic):

- `ShapeGraph` — edges, faces, stability values, Delaunay reference
- `PositionLabel` — vertical_level (str), horizontal_level (str), label (str, e.g., "DM-RC")

#### Tests: `src/tests/test_shape_graph.py`

- Known formation arrangements from thesis Figure 4.7 (4-4-2, 4-3-3, 3-5-2) — verify correct level decomposition and position labels
- Edge cases: collinear players, minimum player count (< 4), asymmetric formations
- `pytest-benchmark`: target sub-millisecond for 10 outfield players per frame

#### Integration: `src/ingestion/formations.py`

- Run shape graphs alongside EFPI in the same pipeline
- Shape graph detector writes window-level formation labels to the unified `fct_formation_labels` table
- New frame-level output written to `fct_player_positions` (grain: match_id, frame_id, player_id)

#### Workflow card: `workflow-cards/wf-shape-graphs.yaml`

Sotudeh citations, scipy/numpy dependency, no GPU, executes alongside `wf-formations`.

### D37 — Position Maps (5×5 Time-in-Position Matrix)

**Dependency**: D36

#### New dbt mart: `fct_position_maps`

- **Grain**: (player_id, match_id, position_label, phase)
- **Columns**: `player_id`, `match_id`, `team`, `position_label`, `vertical_level`, `horizontal_level`, `pct_time`, `phase` (all / in_possession / out_of_possession). Note: `team` is the tracking-data side label (home/away), not `team_id`, because position inference operates on tracking frames which use side labels.
- **Source**: `fct_player_positions` aggregated by player × match × position × phase
- **Contract**: enforced, with explicit `data_type` on every column
- **Clustering**: liquid clustered by `(match_id, player_id)`

#### Synced table

Added to `scripts/refresh_synced_tables.py`. No pgvector index needed (categorical data, not vectors).

#### E2E validation

Query synced table from Lakebase:
- Position distributions sum to 100% per player-match-phase
- Spot-check against matches with known formations
- Verify all 20 tracking matches have position map data

---

## Track A — Transformer Embeddings (D18 + D30)

### D18 — Football2vec v2 Transformer

**Source**: Replaces Doc2Vec (gensim) with a small transformer. Prerequisites D28 (position-group z-scoring) and D29 (23-type SPADL vocabulary) landed in Cycle 1.

#### Training data export

New Databricks task in `src/ingestion/`:
- Exports training sequences from `fct_action_values` to Parquet on UC Volume
- Publishes to HF Hub as `luxury-lakehouse/football2vec-training-data`
- Each row: player_id, match_id, ordered SPADL action sequence with (action_type, x, y, result) per action. `result` is the SPADL binary success/failure flag.
- Position-group labels included for stratified evaluation (from D28)

#### Model architecture: `src/analytics/football2vec_transformer.py`

- **Type**: Tiny encoder-only transformer
- **Layers**: 4 transformer encoder layers
- **Hidden dim**: 128
- **Attention heads**: 4
- **Token embedding**: 23-type SPADL vocabulary → 128d learned embedding
- **Spatial encoding**: Two small MLPs projecting normalized (x, y) → 128d each, summed with token embedding
- **Training objective**: Masked action prediction (15% random mask, predict action type)
- **Output embedding**: Mean pooling over unmasked tokens → 128d player-match embedding
- **Config**: `Football2VecConfig` (Pydantic model) for reproducibility

#### HF Jobs training script: `hf_jobs_scripts/train_football2vec_v2.py`

- PEP 723 script, wheel dependency first line
- `--stage 1` (D18): MLM training from scratch
- `--stage 2` (D30): Load Stage 1 checkpoint, add adversarial head
- **Hardware**: A10G large (24GB VRAM, 46GB RAM)
- **Data source**: reads from HF dataset `luxury-lakehouse/football2vec-training-data`
- **Output**: checkpoints + final model to HF Hub
- **Logging**: MLflow (follows existing VAEP pattern)
- **Hyperparameters**: batch_size=256, lr=1e-4, cosine schedule, 20-30 epochs, early stopping on val loss
- **Split**: train/val/test stratified by competition_id to avoid leakage

#### Inference and publish

- Run inference on all 87K player-match documents → 128d embeddings. Inference runs as part of the HF Jobs script (reads from HF dataset, writes embeddings to HF Hub as a Parquet dataset). A separate Databricks import task reads the HF dataset and writes to Delta.
- Aggregate to season-level and career-level (mean pooling, matching current Football2vec pattern)
- Write to Delta: overwrite existing `player_embeddings_raw` at new dimensionality
- Downstream dbt models (`stg_player_embeddings`, `fct_player_embeddings_season`, `fct_player_embeddings_career`) — update column contracts for 128d

#### pgvector migration

- Recreate 4 HNSW indexes at dim=128 (were dim=32)
- Update `scripts/create_indexes.py` with new dimension
- Synced tables: user recreates `embeddings_season` and `embeddings_career`

#### HF Hub publish

- Publish final model to `luxury-lakehouse/football2vec-v2`
- Update existing `luxury-lakehouse/football2vec-statsbomb-wyscout` dataset with v2 embeddings (v1 archived as a dataset version)

#### Taipy app

- Update `hf_taipy_app/src/analytics/` embedding utilities to handle 128d
- Player Similarity page: cosine distance is dimension-agnostic, should be transparent
- Player Comparison page: radar chart stat vectors are separate, unaffected

### D30 — Adversarial Team Debiasing

**Source**: Danesi (2025), "The Imposter on the Pitch" (HPI/Hudl). Technical basis: Ganin et al. (2016) DANN gradient reversal layer.

#### Architecture addition

- **Team classifier head**: Linear layer (128d → num_teams)
- **Gradient reversal layer (GRL)**: Identity forward, negated gradient backward (scaled by lambda)
- **Loss**: `L_total = L_mlm - lambda * L_team_ce`
- **Lambda warmup**: 0 → 0.2 over first 5 epochs of Stage 2

#### Training: same HF Jobs script, `--stage 2`

- Load Stage 1 checkpoint
- Add team classifier head, full fine-tune with adversarial objective (nothing frozen)
- Hard negative mining: within each batch, ensure pairs from same position_group + same team_id
- Validation metric: team classification accuracy should *decrease* (debiasing working) while MLM loss stays stable
- Early stopping: monitor combined loss, stop when team accuracy plateaus at chance level

#### Output

- "v2-debiased" 128d embeddings — same aggregation pipeline as D18
- Write to same Delta tables (debiased version is the final product, replacing Stage 1 embeddings)
- Publish model to HF Hub as `luxury-lakehouse/football2vec-v2` (debiased checkpoint is released model; Stage 1 checkpoint archived as a tagged version)

#### Evaluation (both stages)

- **Nearest-neighbor retrieval**: Do similar players cluster by position group? (should improve over Doc2Vec baseline)
- **Cross-source validation**: 11,918 players in both StatsBomb + Wyscout — do embeddings from different sources for the same player converge?
- **Team debiasing metric**: Train a held-out team classifier on frozen embeddings — accuracy near chance for v2-debiased vs high for v2 Stage 1

---

## Cross-Cutting Concerns

### Formation Table Migration

- Rename `formation_labels` → `fct_formation_labels` in dbt
- Add `detector` column (string: `efpi` | `shape_graph`)
- Backfill existing EFPI rows: dbt model sets `'efpi'` as default in the `detector` column expression; ingestion code writes `detector` explicitly for both detectors going forward
- Update `src/ingestion/formations.py` to write the `detector` column for both detectors
- Update Taipy app queries that read `formation_labels` to use new table name
- Synced table: user recreates after rename

### dbt Model Changes

| Model | Change |
|-------|--------|
| `formation_labels` staging/mart | Rename to `fct_formation_labels`, add `detector` column, update contract |
| `fct_player_positions` (new) | Frame-level shape graph positions, contract enforced, liquid clustered by `match_id` |
| `fct_position_maps` (new) | Aggregated position maps, contract enforced, liquid clustered by `(match_id, player_id)` |
| `fct_player_embeddings_season` | Update contract: 128d columns replace 32d |
| `fct_player_embeddings_career` | Update contract: 128d columns replace 32d |

All mart models: `on_schema_change: fail`, `contract: {enforced: true}`.

### Workflow Cards

| Card | Action |
|------|--------|
| `wf-shape-graphs.yaml` (new) | Sotudeh citations, scipy dependency, no GPU |
| `wf-football2vec-v2.yaml` (new) | Replaces `wf-football2vec.yaml`, transformer architecture, HF Jobs GPU |
| `wf-formations.yaml` | Update to reference both EFPI and shape graph detectors |

### HF Hub Artifacts

| Artifact | Action |
|----------|--------|
| `luxury-lakehouse/football2vec-training-data` (new dataset) | Training sequences exported from Delta |
| `luxury-lakehouse/football2vec-v2` (new model) | Transformer model with Stage 1 + Stage 2 tagged versions |
| `luxury-lakehouse/football2vec-statsbomb-wyscout` (update) | v2 embeddings, v1 archived as dataset version |
| Org card, model card, README | Update artifact lists per HF artifact link completeness rule |

### Terraform

- New Databricks task for training data export
- Shape graph compute task alongside formations

### Test Budget

| Test file | Scope |
|-----------|-------|
| `test_shape_graph.py` (new) | Unit tests + benchmarks for core algorithm, position inference, edge cases |
| `test_football2vec_transformer.py` (new) | Model construction, forward pass shapes, tokenization, config validation (CPU, no GPU) |
| Existing embedding tests | Update expected dimensionality 32 → 128 |
| Existing formation/line-breaking/team-shape tests | Unaffected |

### Not in Scope

- **TD#9** (3-cluster fix in line_breaking) — stays in deferred tech debt, orthogonal to D36
- **D31/D32/D33** (360 context, ScoutGPT, ScoutGPT integration) — future cycles
- **Taipy position map visualization** — future UI cycle (D37 is data-only)
- **Dual-detector comparison UX in Taipy** — future UI cycle (queryable via Lakebase now)
- **D35** (workflow drilldown) — separate UI work, not part of this cycle
