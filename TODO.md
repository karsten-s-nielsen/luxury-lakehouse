# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-04

---

## Completed Phases

Phases 0–11, 13 are complete. See [PLAN.md §7](PLAN.md#7-completed-phases) for the summary table and git history for implementation details.

---

## Next Up

### Phase 12 — Movement Analysis (PLAN §8.4)

**Tier 1 — Event-data proxies (all matches):**
- [ ] PPDA (Passes Per Defensive Action) — team pressing intensity
- [ ] Pressure event analysis — StatsBomb `type='Pressure'` density maps
- [ ] Add pressing metrics to `fct_match_summary` or new `fct_pressing_stats`

**Tier 2 — Physical performance (tracking matches):**
- [ ] Physical dashboard — distance, HSR, sprints, accelerations per player per match
- [ ] Use [`floodlight`](https://github.com/floodlight-sports/floodlight) (v1.1+, MIT) for kinematics
- [ ] New Streamlit **Movement Analysis page**

**Tier 3 — Off-ball spatial (tracking + pitch control):**
- [ ] Off-Ball xT — `pitch_control × xT` per frame per player
- [ ] Space creation quantification (Fernandez & Bornn 2018)
- [ ] Depends on Phase 10 + Phase 11

### Phase 14 — Cross-Source Player Entity Resolution (PLAN §8.6)

- [ ] Request license from `parmacalcio1913/players-matcher` (currently unlicensed)
- [ ] Build `int_player_xref` mapping across StatsBomb, Metrica, Wyscout
- [ ] Refactor `dim_players` to merge cross-source records

### Phase 15 — pgvector Player Embeddings (PLAN §8.7)

- [ ] Design feature vector from `fct_player_stats` per-90 metrics
- [ ] Populate `fct_player_embeddings` (0 rows, table provisioned)
- [ ] Depends on Phase 14 for cross-source identity (within-source feasible without it)

### Phase 16 — Player Similarity Streamlit Page (PLAN §8.8)

- [ ] pgvector nearest-neighbor search (`player_search.py`)
- [ ] Depends on Phase 15

### Phase 17 — DEFCON Defensive Valuation (PLAN §8.9)

- [ ] DEFCON repo has no license — must reimplement from paper equations
- [ ] EPV decomposition from VAEP (Phase 9) + pitch control (Phase 11)
- [ ] Credit assignment: Intercept, Disturb, Deter, Concede
- [ ] DEFCON-lite (tabular): feasible with public data
- [ ] Full GNN DEFCON: requires 500+ matches with tracking (may need commercial data)

---

## Technical Debt

- [ ] **Synced tables Terraform workaround** — When the Databricks provider adds `database_project`/`branch` fields to `databricks_database_synced_database_table`, remove the UI+import workflow, drop `lifecycle { ignore_changes = all }`, and retire `scripts/import_synced_tables.sh`. Track: [provider changelog](https://registry.terraform.io/providers/databricks/databricks/latest/docs).
- [ ] **PG index recreation after synced table changes** — Custom indexes on Lakebase synced tables are dropped when a synced table is recreated. Must re-run `scripts/create_indexes.py` after every recreation (alongside `scripts/lakebase_grants.sql` for SP permissions). Lakebase partitions tables internally (`__db_system.partition_*`); indexes must cascade to child partitions (no `ON ONLY`).
- [ ] **Double `df.count()` in ingestion writes** (`utils.py`) — `validate_dataframe()` calls `df.count()`, then `write_delta_table()` calls it again before `saveAsTable()`. Each triggers full DAG recomputation (2x compute cost per write). Fix: pass row count from validation to write, or cache the DataFrame.
- [ ] **IDSSE bronze append duplicates** (`idsse.py:270`) — Uses `mode="append"` after first match. Retry of a partial run causes duplicate rows in bronze. Mitigated by dbt `ROW_NUMBER()` dedup, but bronze is dirty. Fix: use `replaceWhere` keyed on `match_id`.
- [ ] **SPADL/VAEP append without `replaceWhere`** (`spadl_vaep.py:247,686`) — `mode="append"` without partition guards. Partial retry causes duplicates. Mitigated by dbt dedup. Fix: use `replaceWhere` keyed on `match_id` or `competition_id`.
- [ ] **SPADL/VAEP `.toPandas()` OOM risk** (`spadl_vaep.py:170`) — Full bronze tables collected to driver memory. Works at current scale (~3M events) but will OOM at 2x. Fix: Spark-native rewrite or bounded partitioned pulls.
- [ ] **StatsBomb `backfill_extra_json` N+1** (`statsbomb.py:438`) — Runs ~3,500 per-match `SELECT *` queries in a loop. Each is a full table scan. Fix: single `SELECT` grouped by match, or batch processing.
- [ ] **`fct_tracking_frames` missing `CLUSTER BY`** — No Z-ordering on gold Delta table. Pitch control queries scan all files before synced table indexes apply. Fix: add `CLUSTER BY (match_id)` to dbt config.
- [ ] **`pitch_control_value` column still NULL** — `fct_tracking_frames.pitch_control_value` is provisioned but not populated. Batch computation deferred from Phase 11. Requires running `compute_pitch_control_frame()` across all frames and writing back to Delta.
- [ ] **Pitch control `max_speed` unused** — `PitchControlParams.max_speed` is defined but not used in the TTI calculation. The Spearman model uses single-phase acceleration only (no velocity cap). Implement two-phase TTI (acceleration + constant speed) for full model fidelity.
- [ ] **Line-breaking Path B limited to Metrica only** — IDSSE (7 matches) and SkillCorner (10 matches) have tracking but no event data. Line-breaking for these 17 matches requires event data procurement or ball trajectory discontinuity detection.
- [ ] **Single-frame 360 analysis** — Path A uses opponent positions at pass moment only. Dual-frame analysis (start + receipt) would be more robust but 360 freeze frames lack temporal resolution.
- [ ] **`line_breaking_results` append duplicates** — `replaceWhere` on `(data_source, match_id)` prevents full duplicates, but partial retry within a batch can produce duplicates. Mitigated by dbt `ROW_NUMBER()` dedup. Fix: finer-grained `replaceWhere`.
- [ ] **Fixed 3-cluster assumption** — Ward clustering with `n_clusters=3` assumes attack/midfield/defense. Breaks down for 5-depth formations (e.g., 3-1-4-2). Dynamic cluster count via silhouette score would be more robust.
- [ ] **No set-piece exclusion** — Corners, free kicks, throw-ins have non-standard formations. Could filter via `pass_type` or develop set-piece-aware algorithm.
- [ ] **`plot_pass_network` (matplotlib) unused** — `pass_network.py` now uses `plot_pass_network_interactive` (Plotly). The original matplotlib function in `pitch.py` is only referenced by existing tests. Remove after updating tests to use the Plotly version.
- [ ] **Heat Map pre-aggregation lossy** — Server-side `GROUP BY round(x/10)` reduces 500K rows to ~200 but bins coordinates into 10-yard cells before `bin_statistic`. Per-action precision is lost. Acceptable for density visualization; not suitable for action-level drill-down.

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Visual Exploratory Behavior** — blocked by pose data procurement (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)

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
