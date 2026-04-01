# Cycle 3 — GK Analytics + 360-Enriched Context + Infrastructure

**Date:** 2026-03-31
**Scope:** D38, D39, D31, TD#29, TD#2

---

## Cycle Composition

| # | Task | Size | Dependencies |
|---|------|------|-------------|
| D38 | GK Event Metrics | Dunkin' | None |
| D39 | GK Post-Shot Model & Sweeper Metrics | Wicked | D38 |
| D31 | 360-Enriched Situational Context | Wicked | D30 (Cycle 2, done) |
| TD#29 | Space Creation Import Pipeline | Dunkin' | None |
| TD#2 | Synced Table Maintenance Wrapper | Dunkin' | None |

**Parallelism:** D38→D39 is sequential. D31, TD#29, and TD#2 are all independent of each other and of D38/D39.

**Dropped:** E7 (embeddings incremental) — source table is ~87K rows, full rebuild is ~3s. Break-even at >500K rows, likely Cycle 4 (ScoutGPT).

---

## D38: GK Event Metrics

### New Files

- `src/analytics/goalkeeper.py` — pure analytics, no Spark imports
- `src/tests/test_goalkeeper.py`
- `dbt_project/models/marts/fct_goalkeeper_stats.sql`
- `dbt_project/models/marts/_marts__models.yml` — contract addition

### Analytics Module (`src/analytics/goalkeeper.py`)

Follows the established analytics module pattern: frozen `@dataclass` for params, named `@dataclass` for results, pure NumPy/pandas, no Spark imports.

**Functions:**

1. **`compute_gk_distribution_xt(passes_df: pd.DataFrame, xt_grid: np.ndarray) -> pd.DataFrame`**
   - Input: GK-initiated passes (goalkick + passes where player is a GK). Columns: `player_id, match_id, start_x, start_y, end_x, end_y, action_result`.
   - xT lookup: zone indices via `min(int(x / (105 / 12)), 11)`, `min(int(y / (68 / 8)), 7)` (SPADL 105x68m coordinate system). Pattern from `src/analytics/off_ball_xt.py:35-60`.
   - `xt_delta = xt_grid[end_zone] - xt_grid[start_zone]` per pass.
   - Pass length classification: Euclidean distance — short (<32m), medium (32-60m), long (>60m).
   - Output grain: one row per `player_id x match_id`. Columns: `total_xt_added, xt_per_pass, pass_count, short_pct, medium_pct, long_pct, launch_rate` (long / total).

2. **`compute_gk_collection_stats(actions_df: pd.DataFrame) -> pd.DataFrame`**
   - Input: all actions for GK players. Filters to `action_type IN ('keeper_claim', 'keeper_punch')`.
   - Output grain: one row per `player_id x match_id`. Columns: `claims, claim_success_rate` (successful claims / total claims), `punches`.

3. **`compute_gk_action_summary(passes_df: pd.DataFrame, actions_df: pd.DataFrame, xt_grid: np.ndarray) -> pd.DataFrame`**
   - Combines distribution xT + collection stats + `keeper_save` / `keeper_pick_up` counts.
   - Output grain: one row per `player_id x match_id`. Full column set for `fct_goalkeeper_stats`.

### dbt Model (`fct_goalkeeper_stats.sql`)

- Sources: `fct_action_values` joined with `dim_players` on `position_group = 'Goalkeeper'`
- Grain: one row per `player_id x match_id`
- Config: `materialized='table'`, `liquid_clustered_by: ['player_id']`, `contract: {enforced: true}`, `enabled=var('goalkeeper_enabled', false)`
- Columns: `player_id, match_id, competition_id, season_id, minutes_played, saves, save_pct, claims, claim_success_rate, punches, distribution_passes, gk_xt_delta_total, gk_xt_per_pass, launch_rate, keeper_pick_ups`

### Stat Vector Refactor (Position-Group Split)

`STAT_FEATURES` in `src/ingestion/player_embeddings.py` becomes `STAT_FEATURES_BY_GROUP: dict[str, list[str]]`:

```python
STAT_FEATURES_BY_GROUP: dict[str, list[str]] = {
    "Goalkeeper": [
        "save_pct",
        "gk_xt_per_pass",
        "launch_rate",
        "claim_success_rate",
        # extend as GK analytics matures
    ],
    "Defender": [
        "goals_per_90", "xg_per_90", "passes_per_90", "pass_completion_pct",
        "progressive_passes_per_90", "line_breaking_per_90", "vaep_per_90",
        "offensive_vaep_per_90", "defensive_vaep_per_90", "defcon_per_90",
        "intercept_per_90", "deter_per_90", "xg_overperformance",
    ],
    "Midfielder": [
        # same 13 features as Defender for now
    ],
    "Forward": [
        # same 13 features as Defender for now
    ],
}
```

The z-score normalization query branches by position group. Each group's stat vector is dense and semantically correct. GK stat features must exist in `fct_goalkeeper_stats` (joined via `player_id + match_id`). Outfield features continue sourcing from `fct_player_stats`.

**Downstream impact:** The `fct_player_embeddings_career` and `fct_player_embeddings_season` dbt models aggregate vectors with `collect_list` + element-wise mean. Since GK and outfield vectors have different lengths, the aggregation naturally partitions — GK vectors are only averaged with other GK vectors (same player = same position group). No dbt changes needed for this.

**pgvector impact:** GK embeddings (behavioral + stat) live in the same `fct_player_embeddings` table but with different stat vector dimensions. The HNSW indexes are on the behavioral vector (128d for v2, 144d for 360), not the stat vector. No index changes needed for the stat vector refactor.

---

## D39: GK Post-Shot Model & Sweeper Metrics

### dbt Prerequisite: Promote `end_location_z` to Gold

**Investigation result:** `stg_statsbomb__shots.sql` correctly parses `end_location_z` (goalmouth height in yards, 0-8 range) from `shot_end_location` JSON array index 2. But `int_unified_shots.sql` and `fct_shots.sql` drop it — never promoted to gold.

**Fix:**
- `int_unified_shots.sql`: StatsBomb branch adds `end_location_z`. Wyscout branch adds `cast(null as double) as end_location_z`.
- `fct_shots.sql`: Passes through `end_location_z`. Contract updated in `_marts__models.yml`.

**Coordinate system:** StatsBomb `end_location` uses 120x80 pitch coordinates. For on-target shots, `x` is always 120 (goal line). Goalmouth coordinates: `end_location_y` (cross-goal position, ~36-44 range for 8-yard goal centered at y=40) and `end_location_z` (height, 0-8 yards). Off-target shots have `end_location_z = NULL` (2-element array).

### PSxG Model (`src/analytics/goalkeeper.py`)

Following Butcher et al. (2025), "An Expected Goals On Target (xGOT) Model" (MDPI, open access).

**Functions:**

4. **`train_psxg_model(shots_df: pd.DataFrame) -> PSxGModel`**
   - Logistic regression (scikit-learn) on on-target shots only (`end_location_z IS NOT NULL`).
   - Features: `end_location_y` (horizontal goalmouth position, normalized to 0-8 yard goal width), `end_location_z` (height in yards, 0-8).
   - Returns frozen `PSxGModel` dataclass wrapping fitted sklearn model + feature scaler.
   - Training data: all StatsBomb on-target shots (~15K events across 3,000+ matches).

5. **`predict_psxg(model: PSxGModel, shots_df: pd.DataFrame) -> pd.DataFrame`**
   - Per-shot PSxG probability. Only scores on-target shots; off-target shots get `psxg = NULL`.
   - Output: input DataFrame with `psxg` column added.

6. **`compute_goals_prevented(gk_stats_df: pd.DataFrame) -> pd.DataFrame`**
   - Per-GK per-match: `psxg_faced = sum(psxg)` for on-target shots faced, `goals_conceded`, `goals_prevented = psxg_faced - goals_conceded`.
   - Positive = outperforming expectations.

### Sweeper-Keeper Metrics (`src/analytics/goalkeeper.py`)

7. **`compute_sweeper_metrics(events_df: pd.DataFrame, players_df: pd.DataFrame) -> pd.DataFrame`**
   - For tracking matches only (~7 IDSSE with event-tracking alignment).
   - Per-GK per-match: `avg_defensive_action_distance` (mean distance from own goal line), `actions_outside_box_per_90`.
   - Limited scope — 7 matches. Metrics are NULL for non-tracking matches.

### HF Jobs Training Script (`scripts/train_psxg_hf.py`)

Follows established pattern (`train_vaep_model_hf.py`, `train_xg_v2_hf.py`):

**Prerequisite:** On-target shots must be exported to HF Hub as a dataset (`luxury-lakehouse/statsbomb-shots-on-target`) before training. This is a Databricks job task: query `fct_shots WHERE end_location_z IS NOT NULL`, write Parquet to UC Volume, upload to HF Hub. Columns: `event_id, match_id, end_location_y, end_location_z, shot_outcome` (goal=1, saved/post=0).

1. Download on-target shots dataset from HF Hub
2. Train logistic regression
3. Evaluate: log-loss, Brier score, calibration curve
4. Publish model weights + config to `luxury-lakehouse/psxg-model` on HF Hub
5. Generate predictions for all on-target shots, publish to `luxury-lakehouse/psxg-predictions`
6. Import predictions to bronze Delta via import script (same pattern as OBSO/VAEP)

**HF Jobs config:** `a10g-small` (logistic regression is trivial), <5 min runtime.

### Extended `fct_goalkeeper_stats.sql`

Additional columns from D39: `psxg_faced, goals_conceded, goals_prevented, avg_defensive_action_distance, actions_outside_box_per_90`.

PSxG columns sourced from imported predictions (bronze Delta, following OBSO pattern). Sweeper columns sourced from tracking-matched events.

### `fct_player_percentiles.sql` — Position-Group Guard

Change `PERCENT_RANK() OVER (PARTITION BY competition_id, season_id ORDER BY ...)` to `PERCENT_RANK() OVER (PARTITION BY competition_id, season_id, position_group ORDER BY ...)` on ALL metrics.

Requires joining `dim_players` in the base CTE to propagate `position_group`. Players with `position_group IS NULL` are excluded from percentile computation (consistent with embedding pipeline behavior).

### Workflow Card (`workflow-cards/wf-goalkeeper.yaml`)

Four-pillar GK evaluation taxonomy:
- Shot stopping: PSxG model (Butcher et al. 2025), goals prevented
- Distribution: xT delta on GK passes, launch rate
- Cross collection: claim/punch success rates
- Defensive activity: sweeper-keeper positioning

References: Butcher et al. (2025) MDPI, Lamberts (2025) Substack, Yam (MIT Sloan), Stats Perform xGOT whitepaper.

---

## D31: 360-Enriched Situational Context

### Decision Record

- **Embedding model:** Separate 360-enriched model (not merged with v2). Own embedding space.
- **Output dimension:** 144d (128d transformer + 16d Deep Sets context, no projection). Plans for more 360 data justify preserving full representational capacity.
- **Architecture:** Reuse Football2vec v2 transformer encoder + Deep Sets branch from `set_encoder.py`.

### New Files

- `src/analytics/football2vec_360.py` — 360-enriched encoder
- `src/tests/test_football2vec_360.py`
- `scripts/prepare_360_training_data.py` — builds training dataset with 360 context (Databricks)
- `scripts/train_football2vec_360.py` — HF Jobs training script
- `workflow-cards/wf-football2vec-360.yaml`
- `dbt_project/models/marts/fct_player_embeddings_career_360.sql`
- `dbt_project/models/marts/fct_player_embeddings_season_360.sql`

### Architecture

```
SPADL actions ──> Token Embed + Spatial MLPs ──> Transformer Encoder ──> Mean Pool ──> 128d
                                                                                       |
360 freeze frame ──> Deep Sets (encode_player_set) ──> 16d ────────────────────────────|
                                                                                       v
                                                                              Concat ──> 144d output
```

### Model (`src/analytics/football2vec_360.py`)

- **`Football2Vec360Config`** — extends `Football2VecConfig` with `context_dim: int = 16`, `use_pretrained_encoder: bool = True`.
- **`Football2Vec360Encoder(nn.Module)`** — loads pretrained v2 transformer weights (frozen or fine-tunable), adds Deep Sets branch using `set_encoder.py`'s architecture (`Linear(4->32) -> ReLU -> Linear(32->16) -> ReLU -> sum aggregation`), concatenates 128d + 16d = 144d output.
- **Training Stage 1:** MLM with 360 context. Same masked language modeling objective as v2. 360 context vector is concatenated to the transformer output before the MLM head projection.
- **Training Stage 2:** Adversarial team debiasing. Reuses D30's gradient reversal layer (GRL). The team classifier head receives the 144d embedding.

### Training Data Preparation (`scripts/prepare_360_training_data.py`)

Runs on Databricks (needs Spark for the 15.58M row join):
1. Join `fct_action_values` with `stg_statsbomb__360` on `event_id = event_uuid`
2. For each action: gather all freeze frame players into a `(N_players, 4)` matrix — `[x_norm, y_norm, is_keeper, is_teammate]` (same features as `set_encoder.py`)
3. Serialize as nested Parquet column alongside existing action fields (`action_type, x, y, result`)
4. Write to HF Hub: `luxury-lakehouse/football2vec-360-training-data`
5. Scope: 323 StatsBomb 360 matches, ~2M actions with 360 context

### Training Script (`scripts/train_football2vec_360.py`)

HF Jobs GPU (`a10g-small` — 323 matches is modest):
1. Download training data from HF Hub
2. Optionally load pretrained v2 weights from `luxury-lakehouse/football2vec-v2`
3. Stage 1: MLM training with 360 context
4. Stage 2: Adversarial debiasing with gradient reversal
5. Publish model weights to `luxury-lakehouse/football2vec-360`
6. Generate 144d embeddings for all players in 360 matches
7. Publish embeddings to `luxury-lakehouse/football2vec-360-embeddings`

### dbt Models

- `fct_player_embeddings.sql`: Already accepts multiple `data_source` values. 360 embeddings imported with `data_source = 'football2vec_360'`. No structural changes needed — the `behavioral_vector` column is `array<double>` with no hardcoded length.
- `fct_player_embeddings_career_360.sql`: Same pattern as `fct_player_embeddings_career.sql` but:
  - Filtered to `data_source = 'football2vec_360'`
  - `sequence(0, 143)` for 144d vector aggregation
  - Only players in the 323 StatsBomb 360 matches (~2,500-4,000 unique players)
- `fct_player_embeddings_season_360.sql`: Same pattern, grain: `canonical_player_id x competition_id x season_id`.

### Scope Limitation

323 matches → ~2,500-4,000 unique players with 360-enriched embeddings. The Player Similarity page model selector shows "Football2vec v2" (all players) vs "Football2vec v2+360" (360 subset). If a player has no 360 embedding, the 360 option is disabled.

---

## TD#29: Space Creation Import Pipeline

### New Files

- `scripts/import_space_creation.py`
- `dbt_project/models/staging/space_creation/_space_creation__sources.yml`
- `dbt_project/models/staging/space_creation/stg_space_creation__values.sql`
- `dbt_project/models/marts/fct_space_creation.sql`

### Import Script (`scripts/import_space_creation.py`)

Follows `import_obso_results.py` pattern:

1. `huggingface_hub.hf_hub_download(repo_id="luxury-lakehouse/space-creation-values", repo_type="dataset")` → local cache
2. Upload Parquet to UC Volume staging path: `/Volumes/soccer_analytics/dev_gold/model_weights/space_creation/`
3. `spark.read.parquet(volume_path)` → add `_ingested_at = current_timestamp()` → `replaceWhere` on `match_id` → `saveAsTable("{catalog}.{schema}.space_creation_values")`

Arguments: `--catalog` (default `soccer_analytics`), `--schema` (default `bronze`), `--volume-path`. Regex-validated SQL identifiers.

### dbt Staging (`stg_space_creation__values.sql`)

- `enabled=var('space_creation_enabled', false)`
- Source: `{{ source('space_creation', 'space_creation_values') }}`
- Dedup: `ROW_NUMBER() OVER (PARTITION BY match_id, frame_id, player_id ORDER BY _ingested_at DESC)`
- Explicit casts for all columns: `match_id (string), frame_id (int), player_id (string), team (string), period (int), space_created_m2 (double), space_destroyed_m2 (double), net_space_m2 (double)`
- Freshness: warn 24h, error 72h

### dbt Mart (`fct_space_creation.sql`)

- `materialized='table'`, `liquid_clustered_by: ['match_id']`, `contract: {enforced: true}`
- Grain: one row per `player_id x match_id x frame_id`
- Joins `dim_players` for `canonical_player_id` resolution (IDSSE DFL PersonId)
- Passes through all value columns + `period`, `team`

### Post-Pipeline

- Synced table created via Databricks UI (TD#1 constraint)
- Add entry to `refresh_synced_tables.py` SYNCED_TABLES list
- Add btree indexes to `create_indexes.py`: `(match_id)`, `(player_id)`, `(match_id, frame_id)`

~875K rows — no special performance considerations.

---

## TD#2: Synced Table Maintenance Wrapper

### New File

- `scripts/maintain_synced_tables.py`

### Design

Thin orchestration script encoding the operational procedure as code:

```
Step 1: python scripts/refresh_synced_tables.py --wait [--catalog X --schema Y]
        (REST API — triggers DLT pipelines, polls to IDLE)

Step 2: python scripts/create_indexes.py [--catalog X --schema Y]
        (psycopg2 — creates indexes on all synced tables, idempotent)

Step 3: python scripts/create_indexes.py --verify [--catalog X --schema Y]
        (EXPLAIN ANALYZE — confirms Index Scan on fact tables)
```

### Arguments

- `--catalog`, `--schema` — passed through to both child scripts
- `--dry-run` — prints commands without executing
- `--skip-refresh` — runs only index creation + verify (for post-recreation scenarios)
- `--skip-verify` — skips step 3

### Error Handling

- Step 1 failure (refresh timeout, pipeline error): exits with error, does not proceed
- Step 2 failure (PG connection, DDL error): exits with error, does not verify
- Step 3 Seq Scan warnings: logged, exit 0 (soft warnings — planner may legitimately choose Seq Scan on small result sets)

### Auth

No new auth mechanisms. Subprocess inheritance:
- Step 1 needs Databricks CLI auth (REST API)
- Steps 2-3 need Databricks workspace access (PG JWT from `/api/2.0/postgres/credentials`)

Documented in `--help` output.

### Implementation

~80 lines of orchestration. Structured JSON logging to stdout. Start/end timestamps and subprocess exit codes per step.

---

## Cross-Cutting Concerns

### New Synced Tables (4)

| Table | Source | Indexes |
|-------|--------|---------|
| `fct_goalkeeper_stats_synced` | D38 | btree: `(player_id)`, `(match_id)`, `(competition_id, season_id)` |
| `fct_player_embeddings_career_360_synced` | D31 | HNSW 144d on `behavioral_vector` |
| `fct_player_embeddings_season_360_synced` | D31 | HNSW 144d on `behavioral_vector` |
| `fct_space_creation_synced` | TD#29 | btree: `(match_id)`, `(player_id)`, `(match_id, frame_id)` |

Total synced tables: 25 existing + 4 = 29.
Total new indexes: 5 btree + 2 HNSW + 1 composite = 8.

### New Workflow Cards (2)

- `workflow-cards/wf-goalkeeper.yaml` (D39) — four-pillar GK evaluation
- `workflow-cards/wf-football2vec-360.yaml` (D31) — 360-enriched embeddings

### New HF Hub Artifacts

| Type | Repo | Source |
|------|------|--------|
| Model | `luxury-lakehouse/psxg-model` | D39 |
| Model | `luxury-lakehouse/football2vec-360` | D31 |
| Dataset | `luxury-lakehouse/statsbomb-shots-on-target` | D39 (PSxG training data) |
| Dataset | `luxury-lakehouse/psxg-predictions` | D39 (per-shot PSxG scores) |
| Dataset | `luxury-lakehouse/football2vec-360-training-data` | D31 |
| Dataset | `luxury-lakehouse/football2vec-360-embeddings` | D31 |

### New Model Cards (2)

Source of truth in `docs/huggingface/model-cards/`, pushed to HF Hub as repo README.md:

- `docs/huggingface/model-cards/psxg-model.md` — logistic regression on goalmouth coordinates, Butcher et al. (2025) reference, training data scale, evaluation metrics (log-loss, Brier score, calibration)
- `docs/huggingface/model-cards/football2vec-360-model-card.md` — 144d transformer + Deep Sets architecture, 323 StatsBomb 360 matches, adversarial debiasing, relationship to v2 base model

### New Dataset Cards (4)

Source of truth in `docs/huggingface/dataset-cards/`, pushed to HF Hub as repo README.md:

- `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md` — on-target shots with goalmouth coordinates (end_location_y, end_location_z), StatsBomb 120x80 coordinate system, ~15K rows
- `docs/huggingface/dataset-cards/psxg-predictions.md` — per-shot PSxG probability for all on-target shots, model version reference
- `docs/huggingface/dataset-cards/football2vec-360-training-data.md` — SPADL action sequences with nested 360 freeze frame player matrices, 323 matches, ~2M actions
- `docs/huggingface/dataset-cards/football2vec-360-embeddings.md` — 144d player embeddings from 360-enriched model, ~2,500-4,000 players

### HF Artifact Link Completeness

Per CLAUDE.md, when publishing new artifacts ALL references must be updated in one commit:

- `docs/huggingface/org-card.md` — add 2 new models to Models table, 4 new datasets to Datasets table
- `README.md` — update artifact counts if referenced
- Taipy app header/footer — update if artifact list is displayed (check `hf_taipy_app/src/template.py` `_FOOTER_CONTENT`)

### dbt Impact

- +6 new models: `stg_space_creation__values`, `fct_space_creation`, `fct_goalkeeper_stats`, `fct_player_embeddings_career_360`, `fct_player_embeddings_season_360`, plus modified `fct_player_percentiles`
- Contract updates: `fct_shots` (add `end_location_z`), `int_unified_shots` (add `end_location_z`)
- New feature flags: `goalkeeper_enabled`, `space_creation_enabled`

### Databricks Workflow Tasks

+3 new tasks: GK metrics compute, PSxG predictions import, space creation import.
360 training/embedding generation runs on HF Jobs, not Databricks.

### Test Coverage

| Test file | Coverage |
|-----------|----------|
| `test_goalkeeper.py` | PSxG model accuracy (synthetic), distribution xT computation, collection stats, sweeper metrics, edge cases (no on-target shots, GK with zero passes) |
| `test_football2vec_360.py` | 360 context encoding, model forward pass with/without context, embedding dimension = 144, training loop smoke test |
| dbt data tests | not_null, unique, accepted_values, relationships on all new models |

### Benchmarks

- 360 Deep Sets encoding: <1ms per event for `encode_player_set` call (target)
- PSxG inference: trivial (logistic regression), no benchmark needed

### Full New File Inventory

| Category | Files | Count |
|----------|-------|-------|
| Analytics modules | `src/analytics/goalkeeper.py`, `src/analytics/football2vec_360.py` | 2 |
| Tests | `src/tests/test_goalkeeper.py`, `src/tests/test_football2vec_360.py` | 2 |
| Scripts | `scripts/train_psxg_hf.py`, `scripts/prepare_360_training_data.py`, `scripts/train_football2vec_360.py`, `scripts/import_space_creation.py`, `scripts/maintain_synced_tables.py` | 5 |
| dbt staging | `stg_space_creation__values.sql`, `_space_creation__sources.yml` | 2 |
| dbt marts | `fct_goalkeeper_stats.sql`, `fct_space_creation.sql`, `fct_player_embeddings_career_360.sql`, `fct_player_embeddings_season_360.sql` | 4 |
| Workflow cards | `wf-goalkeeper.yaml`, `wf-football2vec-360.yaml` | 2 |
| Model cards | `psxg-model.md`, `football2vec-360-model-card.md` | 2 |
| Dataset cards | `statsbomb-shots-on-target.md`, `psxg-predictions.md`, `football2vec-360-training-data.md`, `football2vec-360-embeddings.md` | 4 |
| **Total new files** | | **23** |

Modified files: `int_unified_shots.sql`, `fct_shots.sql`, `fct_player_percentiles.sql`, `_marts__models.yml`, `player_embeddings.py`, `create_indexes.py`, `refresh_synced_tables.py`, `org-card.md`, `README.md` (9 files).

### Cycle Output

- GK evaluation across four pillars (shot stopping, distribution, collection, defensive activity)
- Position-group percentiles (all positions, not just GK)
- Position-group stat vectors (separate embedding feature sets per position)
- 360-enriched embeddings for 323 StatsBomb matches (144d, own embedding space)
- Space creation data fully in platform (bronze → gold → synced → indexed)
- Automated synced table maintenance (refresh → indexes → verify in one command)
- 2 new workflow cards, 2 new model cards, 4 new dataset cards, org-card updated
- 7 HF models (5 existing + 2 new), 16 HF datasets (12 existing + 4 new)
