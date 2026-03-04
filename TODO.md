# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-04

---

## Completed Phases

Phases 0–13 are complete. See [PLAN.md §7](PLAN.md#7-completed-phases) for the summary table and git history for implementation details.

---

## Next Up

### Phase 14 — Cross-Source Player Entity Resolution (PLAN §8.2)

- [ ] Request license from `parmacalcio1913/players-matcher` (currently unlicensed)
- [ ] Build `int_player_xref` mapping across StatsBomb, Metrica, Wyscout
- [ ] Refactor `dim_players` to merge cross-source records

### Phase 15 — pgvector Player Embeddings (PLAN §8.3)

- [ ] Design feature vector from `fct_player_stats` per-90 metrics
- [ ] Populate `fct_player_embeddings` (0 rows, table provisioned)
- [ ] Depends on Phase 14 for cross-source identity (within-source feasible without it)

### Phase 16 — Player Similarity Streamlit Page (PLAN §8.4)

- [ ] pgvector nearest-neighbor search (`player_search.py`)
- [ ] Depends on Phase 15

### Phase 17 — DEFCON Defensive Valuation (PLAN §8.5)

- [ ] DEFCON repo has no license — must reimplement from paper equations
- [ ] EPV decomposition from VAEP (Phase 9) + pitch control (Phase 11)
- [ ] Credit assignment: Intercept, Disturb, Deter, Concede
- [ ] DEFCON-lite (tabular): feasible with public data
- [ ] Full GNN DEFCON: requires 500+ matches with tracking (may need commercial data)

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. | Waiting on Databricks provider to add `database_project`/`branch` fields and pipeline schedule support. |
| 2 | PG index recreation after synced table changes | `scripts/create_indexes.py` | Custom indexes dropped on synced table recreation. Must re-run script manually. | Operational procedure; automated via `create_indexes.py --verify`. |
| 3 | StatsBomb `backfill_extra_json` N+1 | `statsbomb.py:438` | ~3,500 per-match `SELECT * + toPandas()` queries in a loop. Each triggers full DAG plan. | High risk — batch `.toPandas()` on ~10M rows could OOM. Needs careful memory budgeting. |
| 4 | SPADL/VAEP `.toPandas()` OOM risk | `spadl_vaep.py:170` | Full bronze tables collected to driver memory. Works at ~3M events, will OOM at 2x. | Requires Spark-native rewrite of socceraction pipeline. Large effort. |
| 5 | `pitch_control_value` column still NULL | `fct_tracking_frames` | Column provisioned but not populated. Batch computation deferred from Phase 11. | Requires compute-heavy Databricks job across 38M frames. |
| 6 | Line-breaking Path B limited to Metrica only | `line_breaking.py` | IDSSE (7 matches) and SkillCorner (10 matches) have tracking but no event data. | Blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| 8 | `line_breaking_results` append duplicates | `ingestion/line_breaking.py` | `replaceWhere` on `(data_source, match_id)` prevents full duplicates, but partial retry within a batch can duplicate. Mitigated by dbt dedup. | Marginal improvement; already mitigated. |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. Part of Phase 12+. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. Not needed for current use case. |
| 12 | Off-Ball xT 1fps sampling | `off_ball_xt.py` | 1fps sampling (~5,400 frames/match) is a compute-time trade-off. Full 25fps would be 25x slower with marginal accuracy gain. | Performance budget. Consider GPU batch if higher resolution needed. |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions (`Duel`, `Interception`, `Foul Committed`, `Block`). NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
| 14 | Space creation deferred | ROADMAP.md | Full Fernandez & Bornn 2018 OBSO requires N+1 pitch control computations per frame — too expensive for current compute budget. | Move to ROADMAP.md as research direction. |
| 15 | Acceleration noise | `fct_tracking_frames.sql` | Frame-to-frame speed differencing amplifies sensor noise. May show unrealistic acceleration spikes. | Consider Savitzky-Golay smoothing in Python pre-processing if spikes observed in practice. |
| 16 | Physical stats tracking-only | `fct_physical_stats.sql` | Only 20 matches (Metrica 3, IDSSE 7, SkillCorner 10) have physical data. ~3,000 event-only matches have none. | Data limitation — no tracking for StatsBomb/Wyscout. |
| 17 | xT grid static | `expected_threat_grid.csv` | Karun Singh standard 12x8 seed. Could be computed dynamically per competition from pass/shot data for more accurate values. | Enhancement — current static grid is standard practice. |
| 18 | Off-Ball xT NaN values | `off_ball_xt_results` | 470/616 player rows have NULL xT due to NaN propagation from pitch control boundary conditions. Code fixed with `math.isnan()` guard but batch job needs re-run to populate clean data. | Requires ~45 min Databricks job re-run. |

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Pipeline Optimization & Scaling (EIP)** — Splitter/Aggregator/Scatter-Gather patterns, caching layers, Respo.Vision scale planning
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Visual Exploratory Behavior** — blocked by pose data procurement (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)
- **Space Creation** — Fernandez & Bornn 2018 OBSO (deferred from Phase 12; JAX `vmap` may unblock)

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
