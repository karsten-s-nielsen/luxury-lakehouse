# Cycle 4 Remaining: Pipeline Fixes, Detected Issues & Embedding Explorer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the data pipeline gaps that leave GK Analytics and Tactical Positions pages with incomplete data, resolve 5 issues discovered during Puppeteer testing, build the Embedding Explorer HF Space, and polish with workflow card updates.

**Architecture:** Three cycles with two merge points. Cycle A fixes data pipelines (OBSO import, dbt PSxG staging, GK stats NULL stubs, GK stat vector expansion) and the GK team resolution data model gap. Cycle B resolves Tactical Positions issues (raw player IDs, team names, EFPI coverage). Cycle C builds the Embedding Explorer Space and Atlas widget for Player Similarity. Each cycle produces one commit.

**Tech Stack:** Python 3.10, PySpark, dbt (Databricks), Terraform HCL, Taipy, Plotly, Embedding Atlas (JS), DuckDB-WASM, huggingface_hub, UMAP.

**Prior plan:** `docs/superpowers/plans/2026-04-03-cycle4-gk-embeddings-viz.md` — Phase 2 (Tasks 7-14) is complete. This plan covers the remaining Phase 1 (Tasks 1-6), Phase 3 (Tasks 15-17), Phase 4 (Tasks 18-19), plus 5 newly discovered issues.

**Detected issues from Puppeteer testing (2026-04-04):**

| # | Issue | Root Cause | Cycle |
|---|-------|-----------|-------|
| D1 | Raw tracking player IDs in Tactical Positions charts | `fct_player_positions` uses tracking-provider string IDs (e.g., "Home_1", "DFL-OBJ-0012X") that don't join to `dim_players` (StatsBomb integer IDs). Needs a tracking player mapping table or enriched position data with resolved names. | B |
| D2 | Team shows "Home"/"Away" without team names for Metrica/SkillCorner | `fetch_match_events()` returns empty for tracking-only matches (no StatsBomb event data). Team names need to come from tracking metadata or a match info lookup table. | B |
| D3 | EFPI formation labels only available for IDSSE provider | `compute_formations_efpi` pipeline only runs on IDSSE matches. Needs to be extended to Metrica and SkillCorner. | B |
| D4 | EFPI vs Shape Graph use different label formats | By design — EFPI uses template names ("metodo", "2422"), Shape Graph uses level counts ("2-1-2-1-3"). Not a bug — document in glossary. | — |
| D5 | Agreement Rate "—" when EFPI and Shape Graph cover different halves | The two detectors compute on non-overlapping time windows for the same match. Pipeline configuration issue. | B |

---

## Cycle A: Data Pipeline Fixes (GK Data Completeness)

Resolves: GK distribution data gap, rate columns blank, GK team resolution (from Phase 2 open issues), plus original plan Tasks 1-6.

### Task A1: Fix `import_obso_results.py` — Add HF Hub Download Bridge

**Files:**
- Modify: `src/ingestion/import_obso_results.py`

The script reads directly from a UC Volume path. The HF Jobs script publishes to HF Hub, not the Volume. Add a download step following the pattern in `import_psxg_predictions.py` (lines 81-93).

- [ ] **Step 1: Read both import scripts to understand patterns**

Read `src/ingestion/import_obso_results.py` and `src/ingestion/import_psxg_predictions.py`. Key difference: PSxG has `HF_REPO` constant and uses `hf_hub_download` + `shutil.copy2` to stage from HF Hub to Volume.

- [ ] **Step 2: Add HF Hub download constants and `_download_from_hf` helper**

Add `HF_REPO = "luxury-lakehouse/obso-pausa-values"` and the `_download_from_hf` helper function matching the PSxG pattern. Add `--hf-repo` CLI argument to `main()`.

- [ ] **Step 3: Add download step at the start of `run_pipeline`**

Before reading from Volume, call `_download_from_hf(hf_repo, "data/pausa_raw_scores.parquet", volume_path)`. Update `run_pipeline` signature to accept `hf_repo: str`.

- [ ] **Step 4: Run linter and type checker**

```bash
uv run ruff check src/ingestion/import_obso_results.py
uv run pyright src/ingestion/import_obso_results.py
```

---

### Task A2: Fix Terraform `depends_on` for Import Tasks

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Add `depends_on` to `import_obso_results` task**

After line 673, add `depends_on { task_key = "compute_elastic_sync" }`.

- [ ] **Step 2: Add explicit `--volume-path` to `import_obso_results` parameters**

Replace parameters block with catalog, schema, and volume-path args.

- [ ] **Step 3: Add `depends_on` to `import_psxg_predictions` task**

After line 712, add `depends_on { task_key = "export_shots_on_target" }`.

- [ ] **Step 4: Verify Terraform validates**

```bash
cd terraform/environments/dev && terraform validate
```

---

### Task A3: Create dbt Source + Staging for PSxG Predictions

**Files:**
- Create: `dbt_project/models/staging/psxg/_psxg__sources.yml`
- Create: `dbt_project/models/staging/psxg/stg_psxg__predictions.sql`

The `bronze.psxg_predictions` table exists but has no dbt source or staging model. `fct_goalkeeper_stats.sql` needs this to JOIN PSxG data.

- [ ] **Step 1: Create source YAML** with `psxg_predictions` table definition (columns: `event_id`, `match_id`, `player_id`, `psxg`, `_ingested_at`).

- [ ] **Step 2: Create staging model** with ROW_NUMBER dedup partitioned by `event_id`, latest `_ingested_at` wins.

- [ ] **Step 3: Verify dbt compiles**

```bash
cd dbt_project && dbt compile --select stg_psxg__predictions
```

---

### Task A4: Update `fct_goalkeeper_stats.sql` — Replace NULL Stubs + Add `team_id`

**Files:**
- Modify: `dbt_project/models/marts/fct_goalkeeper_stats.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml` (contract update)

Replace 5 `cast(null as ...)` stubs with actual data. Also add `team_id` column to resolve the GK team resolution issue (detected issue from Phase 2 testing — rankings table can't filter by team because `fct_goalkeeper_stats` has no `team_id`).

- [ ] **Step 1: Add `sweeper_stats` CTE**

Compute `avg_defensive_action_distance` and `actions_outside_box_per_90` from the existing `gk_actions` CTE. Reference: `src/analytics/goalkeeper.py` lines 395-432.

- [ ] **Step 2: Add `psxg_agg` CTE**

Aggregate PSxG predictions per GK per match. Join through shots — `psxg_predictions.event_id` matches shot events. Resolve opposing GK (shots AGAINST the GK's team).

- [ ] **Step 3: Add `team_id` resolution**

Add a CTE that determines each GK's team_id per match from the match_summary (home/away). The GK's team is the one that appears in ALL their matches — use the mode approach (most frequent team_id). This enables direct team filtering in the rankings query without the current workaround via match_summary JOINs at query time.

- [ ] **Step 4: Replace NULL stubs in final SELECT**

Replace the 5 NULL stubs with JOINs to `sweeper_stats` and `psxg_agg`. Add `team_id` column.

- [ ] **Step 5: Update model contract in `_marts__models.yml`**

Add `team_id` column with `data_type: string` to the `fct_goalkeeper_stats` contract.

- [ ] **Step 6: Verify dbt builds**

```bash
python scripts/ensure_warehouse.py -- dbt build --select fct_goalkeeper_stats
```

---

### Task A5: Update GK Rankings Query to Use `team_id`

**Files:**
- Modify: `hf_taipy_app/src/queries/goalkeepers.py`

Now that `fct_goalkeeper_stats` has `team_id`, simplify the GK team resolution.

- [ ] **Step 1: Add `team_id` filter to `fetch_gk_rankings`**

Add `WHERE` clause: `if team_id is not None: where_parts.append("gk.team_id = %s")`. This replaces the current behavior where team selection doesn't filter the rankings table.

- [ ] **Step 2: Simplify `fetch_gk_player_lov`**

Replace the complex CTE-based mode approach with a direct `WHERE gk.team_id = %s` filter. The `team_id` column in `fct_goalkeeper_stats` now handles the resolution.

- [ ] **Step 3: Simplify `resolve_gk_team_id`**

Can now query `team_id` directly from `fct_goalkeeper_stats` instead of the match_summary mode approach. Keep the function signature for backward compatibility but simplify internals.

- [ ] **Step 4: Run linter**

```bash
uv run ruff check hf_taipy_app/src/queries/goalkeepers.py
```

---

### Task A6: Extend GK Stat Vector to 13 Features

**Files:**
- Modify: `src/ingestion/player_embeddings_common.py`
- Test: `src/tests/test_player_embeddings.py`

Extend `STAT_FEATURES_BY_GROUP["Goalkeeper"]` from 4 to 13 features, matching outfield dimensionality. All 13 features are now available from `fct_goalkeeper_stats` (populated after Task A4).

- [ ] **Step 1: Write failing test** — assert `len(STAT_FEATURES_BY_GROUP["Goalkeeper"]) == 13`
- [ ] **Step 2: Run test to verify it fails** (currently 4)
- [ ] **Step 3: Update feature tuple** to 13 features (see original plan Task 5 for full list)
- [ ] **Step 4: Update `_load_goalkeeper_stats` query** to SELECT new columns with per-90 derivations
- [ ] **Step 5: Run test to verify it passes**
- [ ] **Step 6: Run full suite**

```bash
uv run ruff check src/ingestion/player_embeddings_common.py && uv run pytest src/tests/test_player_embeddings.py -v
```

---

### Task A7: Cycle A Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full lint + type check + test suite**

```bash
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/ && uv run pytest src/tests/ -v
```

- [ ] **Step 2: User triggers dbt build + synced table refresh**

User runs `dbt build --select stg_psxg__predictions fct_goalkeeper_stats` and `python scripts/refresh_synced_tables.py --table fct_goalkeeper_stats`.

- [ ] **Step 3: Verify GK rankings filter by team on staging**

Puppeteer: select competition → select team → verify table shows only that team's GKs.

- [ ] **Step 4: Verify GK distribution data on staging**

Puppeteer: select competition with distribution data → verify pitch image renders.

- [ ] **Step 5: Verify non-NULL rate columns on staging**

Puppeteer: verify Save %, Launch Rate, xT/Pass have values (not "—") for competitions with GK stats.

**COMMIT POINT:** `feat: cycle A — GK pipeline fixes, team_id resolution, 13d stat vector`

---

## Cycle B: Tactical Positions Data Quality (Detected Issues D1-D3, D5)

### Task B1: Add Tracking Player Mapping Table

**Files:**
- Create: `dbt_project/models/staging/tracking/stg_tracking__player_mapping.sql`
- Create: `dbt_project/models/staging/tracking/_tracking__sources.yml` (if not exists)

Create a mapping from tracking provider player IDs to human-readable names. Sources: IDSSE XML has player names in the DFL position data, SkillCorner JSONL has player metadata, Metrica CSVs have "Home_1" format (no names available).

- [ ] **Step 1: Investigate available player metadata per provider**

Read the ingestion scripts (`idsse.py`, `skillcorner.py`, `metrica.py`) to determine what player name data exists in bronze tables per provider.

- [ ] **Step 2: Create staging model** that maps `(provider, tracking_player_id)` to `player_display_name` from whatever source data is available.

- [ ] **Step 3: Update `fct_player_positions` and `fct_position_maps`** to JOIN the mapping table and populate `player_display_name` directly, rather than relying on the Lakebase-side `LEFT JOIN dim_players`.

---

### Task B2: Add Match Info for Tracking Matches

**Files:**
- Investigate: `src/ingestion/idsse.py`, `src/ingestion/skillcorner.py`, `src/ingestion/metrica.py`

Team names for tracking matches come from `fetch_match_events()` which queries StatsBomb event data. Tracking-only matches have no StatsBomb events. The fix is to populate team names from the tracking provider's own metadata.

- [ ] **Step 1: Investigate each provider's metadata** for team name availability (IDSSE XML headers, SkillCorner match JSON, Metrica CSV headers).

- [ ] **Step 2: Create or extend a match metadata table** (`dim_tracking_matches` or similar) with `match_id`, `home_team_name`, `away_team_name`, `provider`.

- [ ] **Step 3: Update `_init_team_lov` in `state/tactical_positions.py`** to query the new table as fallback when `fetch_match_events()` returns empty.

---

### Task B3: Extend EFPI Formation Detection to All Providers

**Files:**
- Modify: `src/ingestion/formations_efpi.py`
- Modify: Terraform workflow task configuration (if provider-scoped)

- [ ] **Step 1: Investigate why EFPI only runs on IDSSE**

Read `formations_efpi.py` and the Terraform workflow definition. Determine if the scope restriction is in the pipeline code (provider filter) or the Terraform task configuration (only scheduled for IDSSE matches).

- [ ] **Step 2: Extend to Metrica and SkillCorner**

Remove provider restriction. Ensure the EFPI algorithm handles different FPS rates (25fps IDSSE/Metrica vs 10fps SkillCorner).

- [ ] **Step 3: Ensure EFPI and Shape Graph cover the same time windows**

Investigate why the detectors produce non-overlapping windows (detected issue D5). Both should cover the full match. This may be a window configuration issue in one or both detectors.

---

### Task B4: Cycle B Verification

- [ ] **Step 1: Re-run formations pipeline for Metrica + SkillCorner matches**
- [ ] **Step 2: Verify EFPI + Shape Graph both have data for same time windows**
- [ ] **Step 3: Verify Agreement Rate computes (no longer "—")**
- [ ] **Step 4: Verify player names appear instead of raw IDs (where mapping data exists)**
- [ ] **Step 5: Verify team names appear for IDSSE/SkillCorner matches**

**COMMIT POINT:** `feat: cycle B — tactical positions data quality (player names, team names, EFPI coverage)`

---

## Cycle C: Embedding Explorer & Player Similarity Enhancement

### Task C1: Embedding Export Script

**Files:**
- Create: `scripts/export_embedding_atlas_data.py`

Export season-level and career-level embedding data from Lakebase to Parquet on HF Hub with pre-computed UMAP 2D projections. See original plan Task 15 for full implementation.

- [ ] **Step 1: Create export script** with `export_embeddings()` function (Lakebase query, UMAP computation, Parquet output)
- [ ] **Step 2: Run linter**

---

### Task C2: Embedding Explorer Space

**Files:**
- Create: `embedding-explorer/README.md`
- Create: `embedding-explorer/index.html`

Pure HTML/JS static Space using Embedding Atlas widget + DuckDB-WASM for client-side Parquet queries. See original plan Task 16 for full implementation.

- [ ] **Step 1: Create README.md** with HF Space metadata (static SDK)
- [ ] **Step 2: Create index.html** — read Embedding Atlas docs first, then build single-page app with scatter, filters, tooltips
- [ ] **Step 3: Deploy to HF Space**
- [ ] **Step 4: Verify Space is RUNNING**

---

### Task C3: Player Similarity — Embedded Atlas Neighborhood Widget

**Files:**
- Modify: `hf_taipy_app/src/state/player_similarity.py`
- Modify: `hf_taipy_app/src/pages/player_similarity.py`

Add a scoped Atlas widget showing the selected player's ~50 nearest neighbors. Uses `RawHtml` + content provider iframe pattern from `workflows_dag.py`. See original plan Task 17.

- [ ] **Step 1: Add Atlas HTML builder** to state module
- [ ] **Step 2: Add `ps_atlas_neighborhood` state variable**
- [ ] **Step 3: Wire into similarity search flow**
- [ ] **Step 4: Add ContentBlock to page config**
- [ ] **Step 5: Run linter**

---

### Task C4: Workflow Card Updates + Final Verification

**Files:**
- Modify: `workflow-cards/wf-import-obso.yaml`

- [ ] **Step 1: Update wf-import-obso.yaml** — change source from `uc-volume` to `huggingface`
- [ ] **Step 2: Validate workflow cards** — `uv run validate_workflow_cards`
- [ ] **Step 3: Full lint + type check + test suite**
- [ ] **Step 4: Puppeteer verification** of all 16 pages on staging
- [ ] **Step 5: Deploy production**
- [ ] **Step 6: Verify Embedding Explorer Space**

**COMMIT POINT:** `feat: cycle C — embedding explorer, Atlas widget, workflow card updates`

---

## Summary

| Cycle | Tasks | Commit Count | Key Outcome |
|-------|-------|-------------|-------------|
| A | A1-A7 | 1 | GK data completeness: PSxG, sweeper metrics, team_id, 13d stat vector |
| B | B1-B4 | 1 | Tactical Positions data quality: player names, team names, EFPI coverage, agreement rate |
| C | C1-C4 | 1 | Embedding Explorer Space, Atlas widget in Player Similarity |

**Total: 3 commits, 15 tasks.**

### Detected Issues Resolution Map

| Issue | Resolved In |
|-------|------------|
| GK team resolution (rankings don't filter by team) | Task A4 (add team_id) + A5 (query update) |
| GK distribution data gap | Task A4 (PSxG + sweeper stubs populated) |
| GK rate columns blank | Task A4 (NULL stubs replaced with real data) |
| D1: Raw tracking player IDs | Task B1 (player mapping table) |
| D2: Team shows "Home"/"Away" only | Task B2 (match info table) |
| D3: EFPI only for IDSSE | Task B3 (extend to all providers) |
| D4: EFPI vs SG label formats | Not a bug — by design |
| D5: Agreement Rate "—" | Task B3 (ensure overlapping time windows) |
