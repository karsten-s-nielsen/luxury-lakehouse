# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-10 (optimization audit epilogue — closed #3, #12, #21, #24)

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
| ~~3~~ | ~~StatsBomb `backfill_extra_json` N+1~~ | ~~`statsbomb.py:438`~~ | ~~Resolved: Each per-match `SELECT *` is bounded by `WHERE match_id = {match_id}` — not a full table scan. The `_raw_extra_json` mapping requires the full row for re-serialization, so column projection provides no benefit here. OOM risk is per-match (bounded), not cumulative. Backfill pattern is intentional.~~ | ~~Resolved~~ |
| ~~4~~ | ~~SPADL/VAEP `.toPandas()` OOM risk~~ | ~~`spadl_vaep.py:170`~~ | ~~Resolved: per-partition Spark pulls replace full-table `.toPandas()`. StatsBomb events pulled per `(competition_id, season_id)`, Wyscout events pulled per competition match set.~~ | ~~Resolved~~ |
| 5 | `pitch_control_value` column still NULL | `fct_tracking_frames` | Column provisioned but not populated. Batch computation deferred from Phase 11. | Requires compute-heavy Databricks job across 38M frames. |
| 6 | Line-breaking Path B limited to Metrica only | `line_breaking.py` | IDSSE (7 matches) and SkillCorner (10 matches) have tracking but no event data. | Blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| ~~8~~ | ~~`line_breaking_results` append duplicates~~ | ~~`ingestion/line_breaking.py`~~ | ~~Resolved: Delta MERGE on `event_id` key eliminates structural duplicates at write time.~~ | ~~Resolved~~ |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. |
| ~~12~~ | ~~Off-Ball xT 1fps sampling~~ | ~~`off_ball_xt.py`~~ | ~~Resolved: Ingestion migrated to `applyInPandas` (grouped by `match_id`). Sequential per-match loop eliminated — Spark distributes across executors. 1fps sampling rate retained as correct accuracy/compute trade-off. GPU batch path available via ROADMAP.md EIP section if higher resolution needed in future.~~ | ~~Resolved~~ |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions. NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
| 14 | Space creation deferred | ROADMAP.md | Full Fernandez & Bornn 2018 OBSO requires N+1 pitch control computations per frame — too expensive for current compute budget. | Research direction in ROADMAP.md. |
| ~~15~~ | ~~Acceleration noise~~ | ~~`fct_tracking_frames.sql`~~ | ~~Resolved: Savitzky-Golay smoothing (window=7, polyorder=2) applied at ingestion in `_smooth_tracking()`. Positions clamped to pitch bounds.~~ | ~~Resolved~~ |
| 16 | Physical stats tracking-only | `fct_physical_stats.sql` | Only 20 matches (Metrica 3, IDSSE 7, SkillCorner 10) have physical data. ~3,000 event-only matches have none. | Data limitation — no tracking for StatsBomb/Wyscout. |
| 17 | xT grid static | `expected_threat_grid.csv` | Karun Singh standard 12x8 seed. Could be computed dynamically per competition from pass/shot data for more accurate values. | Enhancement — current static grid is standard practice. |
| 18 | DEFCON-lite anonymous defenders | `ingestion/defcon_lite.py` | StatsBomb 360 freeze frames are anonymous — `defender_player_id` is synthetic. `fct_defensive_values` cannot attribute credit to real defenders. Mitigated: `fct_defcon_pressure` pivots to attacker perspective (real `action_player_id`). | Full fix requires Tier 4 GNN with tracking data (500+ matches needed). |
| 19 | No AWS budget alarm | Terraform | `$100/month` budget not enforced by AWS. Silent overspend risk. Add `aws_budgets_budget` resource. | Low effort, high value. |
| 20 | Action values unbounded query | `pages/action_values.py:107` | No LIMIT on match action timeline query. Can return 2K+ actions per match. | Add reasonable LIMIT. |
| ~~21~~ | ~~StatsBomb backfill SELECT *~~ | ~~`statsbomb.py:454`~~ | ~~Resolved: Per-match `SELECT *` is bounded by `WHERE match_id = {match_id}`. Full row needed for JSON re-serialization — column projection not applicable. Memory profile is per-match (bounded), not full-table. Backfill pattern confirmed intentional.~~ | ~~Resolved~~ |
| 22 | Redundant LAG windows in fct_physical_stats | `fct_physical_stats.sql:33-34` | Recomputes displacement LAG already available in upstream `fct_tracking_frames`. Add `displacement_m` to upstream model. | Medium effort — dbt model change + synced table recreation. |
| 23 | S3 lifecycle rule for state versions | Terraform | No lifecycle policy for non-current S3 state versions. Storage hygiene. | Blocked on IAM `s3:PutLifecycleConfiguration` for DevOpsAgent role. |
| ~~24~~ | ~~Metrica tracking reshape iterrows~~ | ~~`metrica.py:451`~~ | ~~Resolved: Replaced with `pd.melt()` vectorized wide-to-narrow reshape. Per-match DataFrame never needs row iteration — columnar transformation handles ~9.5M frames efficiently.~~ | ~~Resolved~~ |
| 25 | Lakebase CU right-sizing | Terraform | `autoscaling_max_cu = 4` may be overprovisioned for dev. Reduce to 2. | Blocked — Terraform provider cannot update `initial_endpoint_spec` after creation. Needs UI change. |

### Resolved

| # | Item | Resolution |
|---|------|------------|
| ~~4~~ | SPADL/VAEP `.toPandas()` OOM | Per-partition Spark pulls replace full-table `.toPandas()`. SB events pulled per `(comp_id, season_id)`, Wyscout events per competition match set. |
| ~~8~~ | Line-breaking append duplicates | Delta MERGE on `event_id` key replaces `replaceWhere` — structural deduplication at write time. dbt `ROW_NUMBER()` dedup retained as defense in depth. |
| ~~15~~ | Acceleration noise | Savitzky-Golay smoothing (`window_length=7, polyorder=2`) applied at ingestion via `analytics/smoothing.py`. Positions clamped to pitch bounds after smoothing. SkillCorner restructured to per-match processing (matching IDSSE pattern) for memory efficiency. |
| ~~18~~ | Off-Ball xT NaN values | Batch re-run completed as part of Phase 17. Code fix (`math.isnan()` guard) was already in place. |
| ~~O1~~ | Wyscout OOM on ingestion | Per-competition load-release: load one JSON, write with `replaceWhere`, `del` + `gc.collect()`. Datetime cols cast to string for Delta schema merge. |
| ~~O2~~ | Metrica batch concat OOM risk | Per-match writes with `replaceWhere=f"match_id = '{match_id}'"` replace `pd.concat()` + `mode="overwrite"`. |
| ~~O3~~ | DataFrame filter inside iterrows | Pre-built `groupby()` for DEFCON (`defcon_lite.py:383`) and line-breaking (`line_breaking.py:175`). |
| ~~O4~~ | Nested iterrows for pseudo-freeze-frames | Replaced with pre-extracted arrays + zip comprehension (`ingestion/defcon_lite.py:257`). |
| ~~O5~~ | Off-ball xT accumulation loop | Vectorized `pd.concat()` + `.groupby().agg()` replaces per-player iterrows (`analytics/off_ball_xt.py`). |
| ~~O6~~ | Player embeddings iterrows/apply | NumPy `.values` + zip replaces `.apply(axis=1)` and iterrows for dict building (`player_embeddings.py`). |
| ~~O7~~ | Entity resolution iterrows | Zip replaces iterrows for rapidfuzz scoring loops (`analytics/entity_resolution.py`). |
| ~~O8~~ | Double `df.count()` on merge writes | `write_delta_table()` now accepts optional `row_count` param to skip redundant Spark DAG recomputation. |
| ~~O9~~ | Shot map unbounded query | Added `LIMIT 10000` to competition-wide shot query (`pages/shot_map.py`). |
| ~~O10~~ | Missing PG indexes | Added composite `(match_id, action_player_id)` on `fct_defcon_actions_synced` and btree on `canonical_player_id` for embedding lookups. |
| ~~O11~~ | `statsbombpy` in core deps | Moved to optional `[statsbomb]` extra. Terraform workflow uses dedicated `statsbomb` env. |
| ~~O12~~ | No `max_concurrent_runs` | Set `max_concurrent_runs = 1` on ingestion workflow. |
| ~~O13~~ | Entity resolution Delta schema merge | Explicit `int64`/`float64` dtypes on all empty DataFrame code paths in `entity_resolution.py` (lines 140, 407, 573). Eliminates `DELTA_FAILED_TO_MERGE_FIELDS` on `player_id_a`. |
| ~~O14~~ | Off-ball xT missing seed CSV | xT grid CSV uploaded to UC Volume (`/Volumes/soccer_analytics/bronze/libs/expected_threat_grid.csv`). Fallback chain: dbt seed table → UC Volume → workspace path. |
| ~~O15~~ | IDSSE tracking intermittent OOM | Per-period processing: `_parse_positions_xml()` returns rows bucketed by period, each half processed/written/released independently. Halves peak DataFrame memory. |

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Pipeline Optimization & Scaling (EIP)** — Splitter/Aggregator/Scatter-Gather patterns, caching layers, Respo.Vision scale planning
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
- **Visual Exploratory Behavior** — blocked by pose data procurement (BSD 3-Clause)
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
