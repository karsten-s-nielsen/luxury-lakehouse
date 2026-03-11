# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-11 (optimization audit complete, documentation cleanup)

---

## Completed Phases

Phases 0–17 are complete. See [PLAN.md §7](PLAN.md#7-completed-phases) for the summary table and git history for implementation details.

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. Root cause: the `/api/2.0/postgres/` surface (Autoscaling) has zero synced table endpoints — UI is the only method. The Provisioned API (`/api/2.0/database/synced_tables`) uses `database_instance_name` with no project/branch equivalent. GitHub issue filed: [terraform-provider-databricks#5456](https://github.com/databricks/terraform-provider-databricks/issues/5456). Related: [#5389](https://github.com/databricks/terraform-provider-databricks/issues/5389) (same gap for `databricks_database_database_catalog`). **Update 2026-03-06:** Connected with a Databricks Solution Architect at SSAC26 conference (LinkedIn). Bug report reference being forwarded for internal triage. | Blocked on Databricks API team adding synced table endpoints to `/api/2.0/postgres/`. Provider cannot be fixed until upstream API exists. |
| 2 | PG index recreation after synced table changes | `scripts/create_indexes.py` | Custom indexes dropped on synced table recreation. Must re-run script manually. | Operational procedure; automated via `create_indexes.py --verify`. |
| 5 | `pitch_control_value` column still NULL | `fct_tracking_frames` | Column provisioned but not populated. Batch computation deferred from Phase 11. | Requires compute-heavy Databricks job across 38M frames. |
| 6 | Line-breaking Path B limited to Metrica only | `line_breaking.py` | IDSSE (7 matches) and SkillCorner (10 matches) have tracking but no event data. | Blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions. NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
| 14 | Space creation deferred | ROADMAP.md | Full Fernandez & Bornn 2018 OBSO requires N+1 pitch control computations per frame — too expensive for current compute budget. | Research direction in ROADMAP.md. |
| 16 | Physical stats tracking-only | `fct_physical_stats.sql` | Only 20 matches (Metrica 3, IDSSE 7, SkillCorner 10) have physical data. ~3,000 event-only matches have none. | Data limitation — no tracking for StatsBomb/Wyscout. |
| 17 | xT grid static | `expected_threat_grid.csv` | Karun Singh standard 12x8 seed. Could be computed dynamically per competition from pass/shot data for more accurate values. | Enhancement — current static grid is standard practice. |
| 18 | DEFCON-lite anonymous defenders | `ingestion/defcon_lite.py` | StatsBomb 360 freeze frames are anonymous — `defender_player_id` is synthetic. `fct_defensive_values` cannot attribute credit to real defenders. Mitigated: `fct_defcon_pressure` pivots to attacker perspective (real `action_player_id`). | Full fix requires Tier 4 GNN with tracking data (500+ matches needed). |
| 25 | Lakebase CU right-sizing | Terraform | `autoscaling_max_cu = 4` may be overprovisioned for dev. Reduce to 2. | Blocked — Terraform provider cannot update `initial_endpoint_spec` after creation. Needs UI change. |
| 26 | IDSSE XML ball-before-player ordering assumption | `src/ingestion/idsse.py` | Single-pass XML merge assumes ball FrameSets precede player FrameSets in DFL position XML. Validated by inspection of current 7 files but not asserted in code. Add a runtime check or unit test that verifies ball coords are available when player frames are processed. If DFL ever delivers files with interleaved ordering, `ball_x`/`ball_y` will silently degrade to NULL. | Low priority — graceful degradation, but should validate. |
| 27 | Respo.Vision ingestion architecture | `src/ingestion/` | Respo.Vision 3D pose tracking (50+ keypoints × 22 players × 60fps = ~2.14B floats/match, ~17 GB raw) cannot use current ingestion patterns. Requirements: (1) streaming download (`requests.get(url, stream=True)` + chunked write to UC Volume), (2) Spark-native file reading (`spark.read.parquet()` or `spark.read.json()` — no pandas on driver), (3) incremental skip guard on `match_id`, (4) `applyInPandas` for all per-match analytics (driver must never see raw tracking data), (5) schema decision: narrow `(match_id, frame_id, player_id, keypoint_id, x, y, z)` ~2B rows/match vs semi-narrow `(match_id, frame_id, player_id, keypoints_json)` ~14M rows/match. Design before any data arrives. | Blocked on own-footage recording + Respo.Vision processing. |

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
| O14 | Off-ball xT missing seed CSV | xT grid CSV uploaded to UC Volume with fallback chain. |
| O15 | IDSSE tracking intermittent OOM | Per-period processing halves peak DataFrame memory. |
| O21 | Incremental skip guards missing | All 5 ingestion modules check existing match IDs before re-processing. |
| O22 | DEFCON type mismatches | `IntegerType`→`LongType` for IDs in `applyInPandas` schemas. |
| O23 | VAEP model distribution broken on serverless | XGBoost models serialized to bytes via closure. |
| O24 | DEFCON timeline Seq Scan timeout | Composite `(competition_id, action_player_id)` index + `ANALYZE` + `LIMIT 2000`. |

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Pipeline Optimization & Scaling (EIP)** — Splitter/Aggregator/Scatter-Gather patterns, caching layers, Respo.Vision scale planning (see TODO #26, #27)
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
- **Visual Exploratory Behavior** — blocked by own-footage Respo.Vision data (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)
- **Space Creation** — Fernandez & Bornn 2018 OBSO (deferred from Phase 12; JAX `vmap` may unblock)
- **HuggingFace Hub Integration** — Tier 1-2 complete (football2vec published to [`luxury-lakehouse/football2vec-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout), model card + org card live); remaining: Tier 3 GPU training, Tier 4 public demo Space

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
