# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-31

---

## On Deck

Tasks warming up in the on-deck circle.

| Size | What it means |
|------|---------------|
| **Monstah** | Multi-phase epic — clear the wall or go home |
| **Wicked** | Looks small, surprisingly impactful |
| **Dunkin'** | Quick run, keeps things moving |

| # | Task | Size | Source | Notes |
|---|------|------|--------|-------|
| D35 | AI/ML Workflows — Detail Drilldown & Card Validation | Wicked | [2026-03-23-taipy-workflows-page.md](docs/superpowers/plans/2026-03-23-taipy-workflows-page.md) | Enable the 8-section detail drilldown panel (designed but disabled — UX design not settled). Sections: overview, data flow, execution config, monitoring, cost breakdown, academic provenance, dependencies, changelog. Requires design decisions on navigation (slide-in panel vs sub-page vs modal) and information density. Also: validate all 16 workflow card YAML files against reality — the drilldown makes card data visible for the first time, so expect corrections to estimates, dependencies, and monitoring thresholds |
| D31 | 360-Enriched Situational Context for Embeddings | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extend `set_encoder.py` Deep Sets architecture to produce a 16-32d situational context vector from 360 freeze frames (15.58M rows, 323 matches). Concatenate with action token embedding before transformer encoding. Encodes spatial relationships (pressing intensity, passing lanes, defensive shape) around each event. Constraint: 360 frames are anonymous (no player_id), so encodes spatial structure, not player-specific graphs. Follows D30 |
| D32 | ScoutGPT-Style Sequence Model — Training & Evaluation | Wicked | [adversarial-training.md](docs/research/adversarial-training.md), [arXiv:2512.17266](https://arxiv.org/abs/2512.17266) | Player-conditioned GPT transformer over ~9.5M SPADL action sequences (Hong et al. 2025). Architecture: transformer decoder with player ID embedding table (11,918 players), 23-type action embeddings, spatial encodings (x/y), autoregressive next-action prediction. Player ID as conditioning token enables counterfactual substitution (swap ID → "what would Messi do here?"). VAEP as reward signal. Train on HF Jobs GPU (A10G, comparable to ScoutGPT's 5 PL seasons). Evaluate: next-action accuracy, counterfactual ranking correlation, cross-source validation. Publish weights + config to HF Hub. Pre-req: D29 (SPADL vocab). Benefits from D18 (transformer experience) and D30 (adversarial objective can be integrated) |
| D33 | ScoutGPT Integration — Embeddings, pgvector & Taipy | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extract player embeddings from trained D32 model. Write to Delta via new `fct_player_embeddings_sequence` mart (or extend existing). Synced table + pgvector HNSW index. Add model selector to Player Similarity Taipy page ("Football2vec" vs "ScoutGPT"). Counterfactual substitution UI: "what would Player X do in Team Y's possessions?" Side-by-side comparison dashboard between old and new embeddings. Follows D32 |
| D7 | Observability Layer (OTel) | Monstah | [ROADMAP.md](ROADMAP.md) | Research complete, ready for implementation. Instrument once, observe anywhere. ~$1-2/month personal tier |
| U3 | Global player search — search by name across all pages | Monstah | CHI-AUDIT-180-rev-1 #1 | New search component with 11,918-player index + cross-page routing + session state. Needs design decisions |
| U4 | Uncertainty/confidence bounds on model outputs | Monstah | CHI-AUDIT-180-rev-1 #4 | xG v2 now outputs MC dropout 95% CI (`xg_ci_lower`, `xg_ci_upper`). VAEP/pitch control still lack native uncertainty. Partial — xG done, others remain |
| M1 | Rotate Databricks PAT for HF Spaces | Dunkin' | HF-MIGRATION | PAT created 2026-03-16 with 90-day lifetime. **Expires ~2026-06-14.** Generate new PAT in Databricks workspace Settings → Developer → Access tokens, then update `DATABRICKS_TOKEN` secret at huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app/settings |
| M2 | Migrate HF Space auth from PAT to OAuth M2M | Wicked | SEC-AUDIT-200 #2 | Two SPs created with OAuth secrets: `luxury-lakehouse-hf-app-dev` (`330f96b9-...`, orphaned PG role) and `luxury-lakehouse-hf-app-v2-dev` (`1a1dbf08-...`). UC grants in place. **Blocked:** Lakebase Autoscaling does not auto-provision PG roles for SPs authenticated via M2M OAuth — workspace API works but PG credential JWT is rejected at the PG auth layer. The existing working SP (`be66af99-...`) was internally provisioned by Lakebase during synced table creation. Need Databricks support ticket to clarify how to provision SP PG access for Lakebase Autoscaling endpoints |

---

### GK Event Metrics (D38)

**Size:** Dunkin
**What:** Goalkeeper distribution value and collection metrics from existing event data. No new data sources needed.

**Sub-items:**

1. **GK Distribution xT** — For every GK-initiated pass (`goalkick` + passes where `player_id` is a goalkeeper), compute `xT_delta = xT(end_x, end_y) - xT(start_x, start_y)` using the production 12×8 xT grids. Aggregate per GK per match: total xT added, xT per pass, short/medium/long split, launch rate (long pass %).
2. **Cross collection metrics** — Aggregate `keeper_claim` and `keeper_punch` SPADL actions per 90. Compute claim success rate (successful claims / total aerial contests in box). Compare to league average.
3. **GK action summary** — Combine distribution xT + collection rates + existing `keeper_save`/`keeper_pick_up` counts into a `fct_goalkeeper_stats` dbt model (or GK-specific columns in `fct_player_stats`).

**New files:**
- `src/analytics/goalkeeper.py` — xT delta computation, GK action aggregation
- `src/tests/test_goalkeeper.py`
- `dbt_project/models/marts/fct_goalkeeper_stats.sql`

**Dependencies:** None — xT grids (`luxury-lakehouse/expected-threat-grids`), SPADL actions (`bronze.spadl_actions`), and `dim_players.position_group = 'Goalkeeper'` all exist.
**Unlocks:** D39 (post-shot model), GK-specific embeddings, future Taipy GK page.

---

### GK Post-Shot Model & Sweeper Metrics (D39)

**Size:** Wicked
**What:** Post-shot expected goals (PSxG) model for shot-stopping evaluation + tracking-based sweeper-keeper positioning. Requires schema investigation first.

**Sub-items:**

1. **PSxG model** — Logistic regression on StatsBomb on-target shots using goalmouth `end_location` coordinates. Reference methodology: [Butcher et al. (2025), "An Expected Goals On Target (xGOT) Model"](https://www.mdpi.com/2504-2289/9/3/64) (open access). Output: per-shot PSxG value. Derived GK metric: `goals_prevented = PSxG_faced - goals_conceded` (PSxG+/-).
   - **Pre-requisite investigation:** Verify that StatsBomb `shot.end_location` provides goalmouth placement (x, y within goal frame), not just pitch coordinates. Check `stg_statsbomb__shots` schema.
2. **Sweeper-keeper positioning** — For the 20 tracking matches: compute GK average defensive action distance from goal line, actions outside penalty box per 90, using `is_goalkeeper` flag + event coordinates.
   - Limited to ELASTIC-synced matches (~7 IDSSE) for event-tracking alignment.
3. **Fix `fct_player_percentiles`** — Add `position_group` filter so GK percentiles are computed within the Goalkeeper population, not ranked against outfield players (current behavior is meaningless for GK metrics).
4. **Workflow card** `workflow-cards/wf-goalkeeper.yaml` with references to MDPI xGOT paper and the four-pillar GK evaluation taxonomy (shot stopping, distribution, cross collection, defensive activity).

**New/modified files:**
- `src/analytics/goalkeeper.py` — PSxG model training + inference, sweeper metrics
- `src/tests/test_goalkeeper.py` — extend from D38
- `dbt_project/models/marts/fct_goalkeeper_stats.sql` — extend from D38
- `dbt_project/models/marts/fct_player_percentiles.sql` — add position_group guard
- `workflow-cards/wf-goalkeeper.yaml`

**Dependencies:** D38 (GK event metrics provides the foundation). StatsBomb shot schema investigation (blocking for PSxG).
**Unlocks:** GK-specific embedding features (replacing outfield-centric stat vector for Goalkeeper position_group), future Taipy GK performance page.

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. Root cause: the `/api/2.0/postgres/` surface (Autoscaling) has zero synced table endpoints — UI is the only method. The Provisioned API (`/api/2.0/database/synced_tables`) uses `database_instance_name` with no project/branch equivalent. GitHub issue filed: [terraform-provider-databricks#5456](https://github.com/databricks/terraform-provider-databricks/issues/5456). Related: [#5389](https://github.com/databricks/terraform-provider-databricks/issues/5389) (same gap for `databricks_database_database_catalog`). **Update 2026-03-06:** Connected with a Databricks Solution Architect at SSAC26 conference (LinkedIn). Bug report reference being forwarded for internal triage. | Blocked on Databricks API team adding synced table endpoints to `/api/2.0/postgres/`. Provider cannot be fixed until upstream API exists. |
| 2 | PG index recreation after synced table changes | `scripts/create_indexes.py` | Custom indexes dropped on synced table recreation. Must re-run script manually. | Operational procedure; automated via `create_indexes.py --verify`. |
| 6 | Line-breaking SkillCorner not yet wired | `line_breaking.py` | Path A (StatsBomb 360), Path B (Metrica tracking), and Path C (IDSSE tracking) all operational. SkillCorner (10 matches) has tracking but no event data — cannot compute line-breaking without pass events. | SkillCorner: blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions. NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
| 16 | Physical stats tracking-only | `fct_physical_stats.sql` | Only 20 matches (Metrica 3, IDSSE 7, SkillCorner 10) have physical data. ~3,000 event-only matches have none. | Data limitation — no tracking for StatsBomb/Wyscout. |
| 18 | DEFCON-lite anonymous defenders | `ingestion/defcon_lite.py` | StatsBomb 360 freeze frames are anonymous — `defender_player_id` is synthetic. `fct_defensive_values` cannot attribute credit to real defenders. Mitigated: `fct_defcon_pressure` pivots to attacker perspective (real `action_player_id`). | Full fix requires Tier 4 GNN with tracking data (500+ matches needed). |
| 25 | Lakebase CU right-sizing | Terraform | `autoscaling_max_cu = 4` may be overprovisioned for dev. Reduce to 2. | Blocked — Terraform provider cannot update `initial_endpoint_spec` after creation. Needs UI change. |
| 27 | Respo.Vision ingestion architecture | `src/ingestion/` | Respo.Vision 3D pose tracking (50+ keypoints × 22 players × 60fps = ~2.14B floats/match, ~17 GB raw) cannot use current ingestion patterns. Requirements: (1) streaming download (`requests.get(url, stream=True)` + chunked write to UC Volume), (2) Spark-native file reading (`spark.read.parquet()` or `spark.read.json()` — no pandas on driver), (3) incremental skip guard on `match_id`, (4) `applyInPandas` for all per-match analytics (driver must never see raw tracking data), (5) schema decision: narrow `(match_id, frame_id, player_id, keypoint_id, x, y, z)` ~2B rows/match vs semi-narrow `(match_id, frame_id, player_id, keypoints_json)` ~14M rows/match. Design before any data arrives. | Blocked on own-footage recording + Respo.Vision processing. |
| 29 | Space creation import pipeline | `scripts/`, `dbt_project/` | `fct_space_creation` data exists only on HF Hub (`luxury-lakehouse/space-creation-values`). Needs: (1) import script to download Parquet from HF Hub to UC Volume staging path, (2) write to `bronze.space_creation_values` Delta table, (3) dbt staging model (`stg_space_creation__values`), (4) dbt mart model (`fct_space_creation`), (5) synced table creation via UI, (6) PG indexes. Pattern: follow `scripts/import_obso_results.py` for the import step. ~875K rows. | Next data pipeline cycle. |
| 28 | Databricks budget automation | `terraform/` | AWS budget is automated via `aws_budgets_budget` ($100/month, 80%/100% alerts). Databricks spending alerts are manual (UI-only, $250/month set 2026-03-12). Investigate whether Databricks Budgets API or `databricks_budget` Terraform resource can automate this for consistency. | Next infrastructure cycle. |

### Deferred EIP / Optimization Items

Items from the Pipeline Optimization & Scaling (EIP) roadmap section that were evaluated and deferred. Core EIP patterns (Splitter, Aggregator, Router, Pipes & Filters) are already implemented and codified in CLAUDE.md.

| # | Item | Description | When to revisit |
|---|------|-------------|-----------------|
| D22 | NannyML CBPE for Model Monitoring | Evaluated 2026-03-25. All current models have immediate ground truth — CBPE's value (performance estimation without ground truth) does not apply. Existing scipy-based drift detection (PSI, Wasserstein, KS, CUSUM) covers all validation scenarios. | When a real-time inference use case appears where ground truth is delayed. |
| E1 | `for_each_task` fan-out | Databricks workflow `for_each_task` for match-level parallelism. Currently all pipelines use `applyInPandas` within a single job which is sufficient. | When single-job wall clock exceeds 2hr timeout or Respo.Vision data arrives (~7M rows/match). |
| E2 | Change Data Feed (CDF) | Delta `table_changes()` for incremental downstream consumption. No downstream consumer currently needs change tracking. | When a streaming consumer (e.g., real-time dashboard, ML feature store) is added. |
| E3 | Dead Letter Channel | Failed record quarantine to `bronze.dead_letters` table. Current retry logic handles transient errors; no persistent failure pattern observed. | When ingestion sources become unreliable or data volume exceeds manual inspection. |
| E4 | `dbt clone` for staging | Zero-copy table references for pre-production validation. Requires Lakebase branching. | When staging environment (ROADMAP.md) is implemented. |
| E6 | Delta retention policy | Explicit `delta.deletedFileRetentionDuration` (30d gold, 7d bronze) ahead of DBR 18.0. | Before DBR 18.0 upgrade where `RETAIN X HOURS` in manual VACUUM is ignored. |
| E7 | `fct_player_embeddings_career/season` incremental | Both models use `materialized='table'` (full rebuild). Acceptable: ~8,950 rows each, simple `AVG()` over ~87K source rows (~3 seconds), guarded by `enabled=var('embeddings_enabled', false)` so they only run on explicit request. Incremental alternative (track which players have new matches, recompute per-player means, merge) adds state-tracking complexity that exceeds the full-rebuild cost at this scale. Break-even: source table >500K rows or rebuild >30 seconds. Evaluated 2026-03-31 (OPT-AUDIT). | When `fct_player_embeddings` exceeds 500K rows or rebuild time exceeds 30 seconds. |
| U5 | Cross-page contextual links in Taipy | No contextual "see also" links between related pages (e.g., Player Impact → Player Comparison, Defensive Impact → Player Impact). The template architecture supports `ContentBlock("text", ...)` for inline links, but which pages to connect and where to place links requires UX design decisions. CHI-AUDIT C-10 identified this as a navigation design opportunity. Evaluated 2026-03-31. | When the next Taipy UI cycle adds new pages or the shape graph visualization pages are built (ROADMAP.md). |

---

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
- **Shape Graph Visualizations & Tactical Applications** — position plots, dual-detector UX, scouting via position maps (D36/D37 shipped in Cycle 2)
- **Goalkeeper Analytics** — four-pillar GK evaluation taxonomy, key references, embedding gap (D38/D39 are the implementation items)
- **Visual Exploratory Behavior** — partially unblocked: 6 Veo3 recordings + local RTMO pose estimation feasible (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)
- **Hugging Face Hub** — Tier 5 (streaming dataset publishing via XET + Polars) blocked on upstream

## Infrastructure Notes

Infrastructure IDs are environment-specific. Use `terraform output` for current values.

| Resource | Env Var / Source |
|----------|----------------|
| AWS region | `us-east-1` |
| AWS profile | `AWS_PROFILE=devops-agent` |
| Databricks workspace URL | `DATABRICKS_HOST` env var |
| Unity Catalog | `soccer_analytics` |
| SQL Warehouse ID | `terraform output sql_warehouse_id` |
| Lakebase project ID | `terraform output lakebase_project_id` |
| Lakebase endpoint | `terraform output lakebase_endpoint_name` |
| Lakebase DNS (RW) | `terraform output lakebase_read_write_dns` |
| Ingestion job ID | `DATABRICKS_JOB_ID` env var / `terraform output ingestion_job_id` |
| App URL (Taipy) | [huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) |
| GitHub Actions IAM Role | `terraform output github_actions_role_arn` |
| State KMS Key | `terraform output state_kms_key_arn` |
| Terraform CI SP | `terraform output terraform_ci_sp_application_id` |
| GitHub repo | `karsten-s-nielsen/luxury-lakehouse` (private) |
| Monthly budget | Under $100 |
| Terraform state bucket | `karstenskyt-terraform-state` (S3 native locking) |
| Start Claude Code with | `AWS_PROFILE=devops-agent claude` |
