# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-12 (Phase 18 — HF Hub Expansion: 4 datasets, Gradio demo Space, pitch control batch, JAX kernel, symmetry augmentation)

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
| D5 | OpenSTARLab Pre-Trained Models | Wicked | [ROADMAP.md](ROADMAP.md) | Apache 2.0 inference (Seq2Event, LEM, FMS) on existing StatsBomb/Wyscout data. Publish predictions to HF Hub |
| D6 | Custom xG Model | Wicked | [ROADMAP.md](ROADMAP.md) | Train competition-specific xG from ~131K StatsBomb shots. Logistic regression baseline + gradient boosted. Publish to HF Hub |
| D8 | Dynamic xT Grid Seed Replacement | Dunkin' | Tech debt #17 | Module ready (`src/analytics/expected_threat.py`). Run `compute_expected_threat` on Databricks to generate data-driven grid, replace static `expected_threat_grid.csv` seed |
| D9 | ELASTIC Event-Tracking Sync | Wicked | [ROADMAP.md](ROADMAP.md) | Adapt Kim et al. ELASTIC sync engine for IDSSE schema. Aligns discrete events with 25fps tracking (95.5% exact alignment). Independently valuable — any future tracking+event analysis needs this. License: Apache-2.0 (pending, assumed formality) |
| D10 | OBSO + PAUSA Pipeline | Wicked | [ROADMAP.md](ROADMAP.md) | Full OBSO scoring surface (PPCF × Transition × EPV) + PAUSA temporal/spatial decomposition + ghost trajectories + `fct_pausa_values` Delta table + dbt mart + Streamlit page. Builds on D9 |
| D11 | MLflow Model Registry & Experiment Tracking | Dunkin' | [ROADMAP.md](ROADMAP.md) | UC Model Registry with Champion/Challenger aliases for D5/D6 models. `mlflow.evaluate()` for batch validation (Brier score, calibration). Cross-cutting enabler for all ML work |
| D12 | Model Validation & Drift Detection | Dunkin' | [ROADMAP.md](ROADMAP.md) | PSI for xG, Wasserstein for xT/VAEP distributions, CUSUM for sustained drift, range bounds for physical stats. Pure scipy/numpy. Reference baselines as dbt seeds |
| D13 | Physics-Based Tracking Augmentation | Dunkin' | [ROADMAP.md](ROADMAP.md) | Position/velocity jitter within physical constraints. 10× multiplier on 20 tracking matches. Combined with symmetry augmentation (8×) = 80× effective training data. NumPy only |
| D14 | Space Creation (Full Counterfactual) | Wicked | [ROADMAP.md](ROADMAP.md) | Fernandez & Bornn 2018 OBSO counterfactual analysis. Differential pitch control per player per frame. Unblocked by D10 (OBSO + ghost trajectories) + D13 (augmented tracking data). JAX kernel ready (Phase 18) |
| D7 | Observability Layer (OTel) | Monstah | [ROADMAP.md](ROADMAP.md) | Research complete, ready for implementation. Instrument once, observe anywhere. ~$1-2/month personal tier |

---

## Completed Phases

Phases 0–18 are complete. See git history for implementation details.

### Completed On-Deck Items

| # | Task | Resolution |
|---|------|------------|
| D1 | HF Space — Pitch Control + Velocity Arrows | Pitch control tab live with frame slider, physics heatmap (RdBu), velocity arrows. `pitch_control.py` (pure NumPy) + sample tracking Parquet exported from Databricks |
| D2 | HF Space — DEFCON Pressure Breakdown | DEFCON pressure tab live with filterable player dropdown + Plotly grouped bar chart (intercept/concede/disturb/deter). Data aggregated from `fct_defcon_actions` (9,815 rows, 2,394 players, 323 matches), bundled as Parquet in Space |
| D3 | Dynamic xT Grid | `src/analytics/expected_threat.py` + `src/ingestion/expected_threat.py` — Markov chain value iteration replaces static Karun Singh seed. Entry point `compute_expected_threat`, tests in `test_expected_threat.py` |
| ~~D4~~ | ~~Pitch Control Animation~~ | ~~Dropped — per-frame physics computation too expensive for free HF Space tier. Static frame slider (D1) provides equivalent functionality~~ |

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. Root cause: the `/api/2.0/postgres/` surface (Autoscaling) has zero synced table endpoints — UI is the only method. The Provisioned API (`/api/2.0/database/synced_tables`) uses `database_instance_name` with no project/branch equivalent. GitHub issue filed: [terraform-provider-databricks#5456](https://github.com/databricks/terraform-provider-databricks/issues/5456). Related: [#5389](https://github.com/databricks/terraform-provider-databricks/issues/5389) (same gap for `databricks_database_database_catalog`). **Update 2026-03-06:** Connected with a Databricks Solution Architect at SSAC26 conference (LinkedIn). Bug report reference being forwarded for internal triage. | Blocked on Databricks API team adding synced table endpoints to `/api/2.0/postgres/`. Provider cannot be fixed until upstream API exists. |
| 2 | PG index recreation after synced table changes | `scripts/create_indexes.py` | Custom indexes dropped on synced table recreation. Must re-run script manually. | Operational procedure; automated via `create_indexes.py --verify`. |
| 6 | Line-breaking Path B limited to Metrica only | `line_breaking.py` | IDSSE (7 matches) and SkillCorner (10 matches) have tracking but no event data. | Blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions. NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
| 14 | Space creation deferred | ROADMAP.md | Full Fernandez & Bornn 2018 OBSO requires N+1 pitch control computations per frame — too expensive for current compute budget. **Update (Phase 18):** JAX kernel (`compute_pitch_control_grid_fast`) enables `vmap` over grid points, partially unblocking OBSO. | Research direction in ROADMAP.md. JAX kernel available. |
| 16 | Physical stats tracking-only | `fct_physical_stats.sql` | Only 20 matches (Metrica 3, IDSSE 7, SkillCorner 10) have physical data. ~3,000 event-only matches have none. | Data limitation — no tracking for StatsBomb/Wyscout. |
| 18 | DEFCON-lite anonymous defenders | `ingestion/defcon_lite.py` | StatsBomb 360 freeze frames are anonymous — `defender_player_id` is synthetic. `fct_defensive_values` cannot attribute credit to real defenders. Mitigated: `fct_defcon_pressure` pivots to attacker perspective (real `action_player_id`). | Full fix requires Tier 4 GNN with tracking data (500+ matches needed). |
| 25 | Lakebase CU right-sizing | Terraform | `autoscaling_max_cu = 4` may be overprovisioned for dev. Reduce to 2. | Blocked — Terraform provider cannot update `initial_endpoint_spec` after creation. Needs UI change. |
| 26 | IDSSE XML ball-before-player ordering assumption | `src/ingestion/idsse.py` | Single-pass XML merge assumes ball FrameSets precede player FrameSets in DFL position XML. Validated by inspection of current 7 files but not asserted in code. Add a runtime check or unit test that verifies ball coords are available when player frames are processed. If DFL ever delivers files with interleaved ordering, `ball_x`/`ball_y` will silently degrade to NULL. | Low priority — graceful degradation, but should validate. |
| 27 | Respo.Vision ingestion architecture | `src/ingestion/` | Respo.Vision 3D pose tracking (50+ keypoints × 22 players × 60fps = ~2.14B floats/match, ~17 GB raw) cannot use current ingestion patterns. Requirements: (1) streaming download (`requests.get(url, stream=True)` + chunked write to UC Volume), (2) Spark-native file reading (`spark.read.parquet()` or `spark.read.json()` — no pandas on driver), (3) incremental skip guard on `match_id`, (4) `applyInPandas` for all per-match analytics (driver must never see raw tracking data), (5) schema decision: narrow `(match_id, frame_id, player_id, keypoint_id, x, y, z)` ~2B rows/match vs semi-narrow `(match_id, frame_id, player_id, keypoints_json)` ~14M rows/match. Design before any data arrives. | Blocked on own-footage recording + Respo.Vision processing. |
| 28 | Databricks budget automation | `terraform/` | AWS budget is automated via `aws_budgets_budget` ($100/month, 80%/100% alerts). Databricks spending alerts are manual (UI-only, $250/month set 2026-03-12). Investigate whether Databricks Budgets API or `databricks_budget` Terraform resource can automate this for consistency. | Next infrastructure cycle. |

### Deferred EIP / Optimization Items

Items from the Pipeline Optimization & Scaling (EIP) roadmap section that were evaluated and deferred. Core EIP patterns (Splitter, Aggregator, Router, Pipes & Filters) are already implemented and codified in CLAUDE.md.

| # | Item | Description | When to revisit |
|---|------|-------------|-----------------|
| E1 | `for_each_task` fan-out | Databricks workflow `for_each_task` for match-level parallelism. Currently all pipelines use `applyInPandas` within a single job which is sufficient. | When single-job wall clock exceeds 2hr timeout or Respo.Vision data arrives (~7M rows/match). |
| E2 | Change Data Feed (CDF) | Delta `table_changes()` for incremental downstream consumption. No downstream consumer currently needs change tracking. | When a streaming consumer (e.g., real-time dashboard, ML feature store) is added. |
| E3 | Dead Letter Channel | Failed record quarantine to `bronze.dead_letters` table. Current retry logic handles transient errors; no persistent failure pattern observed. | When ingestion sources become unreliable or data volume exceeds manual inspection. |
| E4 | `dbt clone` for staging | Zero-copy table references for pre-production validation. Requires Lakebase branching. | When staging environment (ROADMAP.md) is implemented. |
| E5 | Training data versioning | Delta time travel + MLflow `log_input()` with `delta://table@version` URIs. | When ML model training becomes iterative (DEFCON Tier 4 GNN, football2vec v2). |
| E6 | Delta retention policy | Explicit `delta.deletedFileRetentionDuration` (30d gold, 7d bronze) ahead of DBR 18.0. | Before DBR 18.0 upgrade where `RETAIN X HOURS` in manual VACUUM is ignored. |

### Resolved

Items resolved during phases or the optimization audit (2026-03-11). Details preserved in git history.

| # | Item | Resolution |
|---|------|------------|
| 3 | StatsBomb `backfill_extra_json` N+1 | Each per-match `SELECT *` is bounded by `WHERE match_id`. Delta MERGE replaces read-modify-write. OOM risk is per-match (bounded). |
| 4 | SPADL/VAEP `.toPandas()` OOM | Per-partition Spark pulls replace full-table `.toPandas()`. SB events pulled per `(comp_id, season_id)`, Wyscout events per competition match set. |
| 8 | Line-breaking append duplicates | Delta MERGE on `event_id` key replaces `replaceWhere` — structural deduplication at write time. |
| 12 | Off-Ball xT 1fps sampling | Migrated to `applyInPandas` grouped by `match_id`. 1fps retained as correct accuracy/compute trade-off. |
| 15 | Acceleration noise | Savitzky-Golay smoothing (`window_length=7, polyorder=2`) via `analytics/smoothing.py`. Positions clamped to pitch bounds. |
| 18 | Off-Ball xT NaN values | Batch re-run completed as part of Phase 17. `math.isnan()` guard in place. |
| 19 | No AWS budget alarm | `aws_budgets_budget` with $100/month limit, 80%/100% email alerts. |
| 20 | Action values unbounded query | LIMIT 2000 on timeline, LIMIT 500 on rankings, recursive CTE for DISTINCT. |
| 21 | StatsBomb backfill SELECT * | Delta MERGE replaces read-modify-write — updates only `_raw_extra_json`. |
| 22 | Redundant LAG in fct_physical_stats | Removed duplicate LAG. Displacement derived from upstream columns. |
| 23 | S3 lifecycle rule | `aws_s3_bucket_lifecycle_configuration` expires non-current versions after 90 days. |
| 24 | Metrica tracking reshape iterrows | `pd.melt()` vectorized wide-to-narrow reshape replaces per-row iteration. |
| O1 | Wyscout OOM on ingestion | Per-competition load-release with `replaceWhere`, `del` + `gc.collect()`. |
| O2 | Metrica batch concat OOM | Per-match writes with `replaceWhere` replace `pd.concat()` + `mode="overwrite"`. |
| O3 | DataFrame filter inside iterrows | Pre-built `groupby()` for DEFCON and line-breaking. |
| O4 | Nested iterrows for pseudo-freeze-frames | Pre-extracted arrays + zip comprehension. |
| O5 | Off-ball xT accumulation loop | Vectorized `pd.concat()` + `.groupby().agg()` replaces per-player iterrows. |
| O6 | Player embeddings iterrows/apply | NumPy `.values` + zip replaces `.apply(axis=1)` and iterrows. |
| O7 | Entity resolution iterrows | Zip replaces iterrows for rapidfuzz scoring loops. |
| O8 | Double `df.count()` on merge writes | `write_delta_table()` accepts optional `row_count` to skip redundant DAG. |
| O9 | Shot map unbounded query | `LIMIT 10000` on competition-wide shot query. |
| O10 | Missing PG indexes | Composite `(match_id, action_player_id)` on DEFCON, btree on `canonical_player_id`. |
| O11 | `statsbombpy` in core deps | Moved to optional `[statsbomb]` extra. |
| O12 | No `max_concurrent_runs` | Set `max_concurrent_runs = 1` on ingestion workflow. |
| O13 | Entity resolution Delta schema merge | Explicit `int64`/`float64` dtypes on empty DataFrame code paths. |
| 5 | `pitch_control_value` column NULL | `pitch_control_batch.py` populates `bronze.pitch_control_values` via `applyInPandas`. Staging model `stg_pitch_control__values` exposes values. HF export JOINs at query time. |
| O14 | Off-ball xT missing seed CSV | xT grid CSV uploaded to UC Volume with fallback chain. |
| O15 | IDSSE tracking intermittent OOM | Per-period processing halves peak DataFrame memory. |
| O21 | Incremental skip guards missing | All 5 ingestion modules check existing match IDs before re-processing. |
| O22 | DEFCON type mismatches | `IntegerType`→`LongType` for IDs in `applyInPandas` schemas. |
| O23 | VAEP model distribution broken on serverless | XGBoost models serialized to bytes via closure. |
| O24 | DEFCON timeline Seq Scan timeout | Composite `(competition_id, action_player_id)` index + `ANALYZE` + `LIMIT 2000`. |

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Cognitive Interface Audit** (`cognitive-interface-audit`) — mental model alignment, error tolerance (Wood 7-layer), cognitive load, visual grounding (Gergle); beta complete in mad-scientist-skills v1.7.0
- **Pipeline Optimization & Scaling** — EIP core patterns implemented (Splitter, Aggregator, Router, Pipes & Filters); remaining deferred items below
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
- **Visual Exploratory Behavior** — blocked by own-footage Respo.Vision data (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)
- **Space Creation** — Fernandez & Bornn 2018 OBSO (deferred from Phase 12; JAX `vmap` may unblock)
- **HuggingFace Hub Integration** — Tiers 1-2 and 4 complete (model + 4 datasets published, Gradio demo Space live with luxury flagship theme); remaining: Tier 3 GPU training

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
| Streamlit App URL | `terraform output app_url` |
| GitHub Actions IAM Role | `terraform output github_actions_role_arn` |
| State KMS Key | `terraform output state_kms_key_arn` |
| Terraform CI SP | `terraform output terraform_ci_sp_application_id` |
| GitHub repo | `karsten-s-nielsen/luxury-lakehouse` (private) |
| Monthly budget | Under $100 |
| Terraform state bucket | `karstenskyt-terraform-state` (S3 native locking) |
| Start Claude Code with | `AWS_PROFILE=devops-agent claude` |
