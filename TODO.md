# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md).

**Last updated**: 2026-03-03

---

## Completed Phases

Phases 0–10 are complete. See [PLAN.md §7](PLAN.md#7-completed-phases) for the summary table and git history for implementation details.

---

## Next Up

### Phase 11 — Physics-Based Pitch Control Model (PLAN §8.2)

- [ ] Implement Spearman et al. (2017) pitch control — player influence from position, velocity, time-to-intercept
- [ ] Populate `pitch_control_value` in `fct_tracking_frames` (currently NULL)
- [ ] Update Streamlit Pitch Control page with continuous heatmap overlay
- [ ] Depends on tracking data from Phase 7 (Metrica) + Phase 10 (IDSSE/SkillCorner)

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

### Phase 13 — Cross-Source Player Entity Resolution (PLAN §8.5)

- [ ] Request license from `parmacalcio1913/players-matcher` (currently unlicensed)
- [ ] Build `int_player_xref` mapping across StatsBomb, Metrica, Wyscout
- [ ] Refactor `dim_players` to merge cross-source records

### Phase 14 — pgvector Player Embeddings (PLAN §8.6)

- [ ] Design feature vector from `fct_player_stats` per-90 metrics
- [ ] Populate `fct_player_embeddings` (0 rows, table provisioned)
- [ ] Depends on Phase 13 for cross-source identity (within-source feasible without it)

### Phase 15 — Player Similarity Streamlit Page (PLAN §8.7)

- [ ] pgvector nearest-neighbor search (`player_search.py`)
- [ ] Depends on Phase 14

### Phase 16 — DEFCON Defensive Valuation (PLAN §8.8)

- [ ] DEFCON repo has no license — must reimplement from paper equations
- [ ] EPV decomposition from VAEP (Phase 9) + pitch control (Phase 11)
- [ ] Credit assignment: Intercept, Disturb, Deter, Concede
- [ ] DEFCON-lite (tabular): feasible with public data
- [ ] Full GNN DEFCON: requires 500+ matches with tracking (may need commercial data)

---

## Technical Debt

- [ ] **Synced tables Terraform workaround** — When the Databricks provider adds `database_project`/`branch` fields to `databricks_database_synced_database_table`, remove the UI+import workflow, drop `lifecycle { ignore_changes = all }`, and retire `scripts/import_synced_tables.sh`. Track: [provider changelog](https://registry.terraform.io/providers/databricks/databricks/latest/docs).
- [ ] **PG index recreation after synced table changes** — Custom indexes on Lakebase synced tables are dropped when a synced table is recreated. Must re-run `scripts/create_indexes.py` after every recreation (alongside `scripts/lakebase_grants.sql` for SP permissions). Lakebase partitions tables internally (`__db_system.partition_*`); indexes must cascade to child partitions (no `ON ONLY`).

## Future Work (unscheduled)

- [ ] **Lakebase query optimization round** — Thorough review of PG indexes on all synced tables (currently only `fct_tracking_frames_synced` has ad-hoc btree indexes). Audit EXPLAIN plans for all Streamlit queries. Investigate composite indexes, partial indexes, and covering indexes. Consider materializing small lookup tables (match list, competition list) to avoid full-table scans on large tables. Also evaluate parallelizing Databricks ingestion tasks that are currently sequential (multiple serverless instances, fan-out patterns in Workflows).
- [ ] Voronoi area persistence — pre-compute in dbt (lower priority if Phase 11 replaces Voronoi)
- [ ] Pitch Control animation — frame-by-frame playback
- [ ] Event overlay on Pitch Control — render events on pitch control view
- [ ] Respo.Vision 3D pose tracking — skeletal keypoints from broadcast video (user pursuing)
- [ ] Wyscout match metadata — formations, coaches, venue (not in public dataset)

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
| GitHub repo | `karstenskyt/luxury-lakehouse` (private) |
| Monthly budget | Under $100 |
| Terraform state bucket | `karstenskyt-terraform-state` (S3 native locking) |
| Start Claude Code with | `AWS_PROFILE=devops-agent claude` |
