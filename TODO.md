# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-26

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
| D18 | Football2vec v2 — Transformer Embeddings | Wicked | [ROADMAP.md](ROADMAP.md) | Replace Doc2Vec (gensim, CPU) with a small transformer on tokenized match sequences. Train on HF Jobs GPU (A10G). 87K player-match documents in Delta. Better player representations for similarity search. Publish to HF Hub |
| D7 | Observability Layer (OTel) | Monstah | [ROADMAP.md](ROADMAP.md) | Research complete, ready for implementation. Instrument once, observe anywhere. ~$1-2/month personal tier |
| U3 | Global player search — search by name across all pages | Monstah | CHI-AUDIT-180-rev-1 #1 | New search component with 11,918-player index + cross-page routing + session state. Needs design decisions |
| U4 | Uncertainty/confidence bounds on model outputs | Monstah | CHI-AUDIT-180-rev-1 #4 | xG v2 now outputs MC dropout 95% CI (`xg_ci_lower`, `xg_ci_upper`). VAEP/pitch control still lack native uncertainty. Partial — xG done, others remain |
| M1 | Rotate Databricks PAT for HF Spaces | Dunkin' | HF-MIGRATION | PAT created 2026-03-16 with 90-day lifetime. **Expires ~2026-06-14.** Generate new PAT in Databricks workspace Settings → Developer → Access tokens, then update `DATABRICKS_TOKEN` secret at huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app/settings |
| M2 | Migrate HF Space auth from PAT to OAuth M2M | Wicked | SEC-AUDIT-200 #2 | Two SPs created with OAuth secrets: `luxury-lakehouse-hf-app-dev` (`330f96b9-...`, orphaned PG role) and `luxury-lakehouse-hf-app-v2-dev` (`1a1dbf08-...`). UC grants in place. **Blocked:** Lakebase Autoscaling does not auto-provision PG roles for SPs authenticated via M2M OAuth — workspace API works but PG credential JWT is rejected at the PG auth layer. The existing working SP (`be66af99-...`) was internally provisioned by Lakebase during synced table creation. Need Databricks support ticket to clarify how to provision SP PG access for Lakebase Autoscaling endpoints |
| D26 | Formation Detection — GK Metadata Pipeline | Wicked | Session 6 | GK exclusion requires provider metadata (not positional heuristics). See detailed write-up below |

### Formation Detection — GK Metadata Pipeline (D26)

**Status:** Deferred from session 6 (2026-03-26)
**Scope:** Wicked (2-3 sessions, ~4-6 hours) — looks like "just add a column" but touches 3 ingestion pipelines, 4 dbt models, 38M row recompute, synced table recreation, and formation pipeline rerun
**Branch:** Separate feature branch from main

**Problem:** Formation detection (EFPI algorithm) requires excluding the goalkeeper before template matching. Templates exist for 8, 9, 10 outfield players only. Currently, `fct_tracking_frames` has no position/role metadata — all 11 players are passed to detection, `templates.get(11)` returns None, and no formations are detected for any match with full tracking. Only 2 of 20 matches produced results (IDSSE matches where the away team happened to have 10 tracked players).

**Root cause:** The tracking pipeline does not store player position metadata despite all three source providers having it:
- **kloppy** (SkillCorner): `Player.position` → `PositionType` including GK
- **IDSSE XML**: Player roster with roles in match metadata
- **Metrica EPTS**: Player roster with positions in XML metadata

**Industry standard:** Every published method (Bekkers & Dabadghao 2025, Shaw & Glickman 2019, Bialkowski et al. 2014) uses provider metadata for GK exclusion. No published method uses positional heuristics (idxmin/idxmax on x-coordinate). A positional heuristic fails because attacking direction is not normalized between periods in our coordinate system.

**Implementation plan:**
1. Add `is_goalkeeper` boolean column to all three staging models (`stg_metrica__tracking`, `stg_idsse__tracking`, `stg_skillcorner__tracking`) — extract from source metadata
2. Add `is_goalkeeper` to `fct_tracking_frames` mart model (pass-through from staging)
3. Update `_marts__models.yml` contract with new column
4. Update formation pipeline (`src/ingestion/formations.py`) to filter `is_goalkeeper = false` before detection
5. Re-run formation pipeline for all matches (delete existing results, full recompute)
6. Rebuild `fct_formation_labels` via dbt
7. Recreate synced table + indexes
8. Re-enable Formation metric on Team Shape page (Snapshot + Timeline)
9. Verify formations appear for all 20 tracking matches

**Files affected:**
- `src/ingestion/metrica.py` — extract GK from EPTS metadata
- `src/ingestion/idsse.py` — extract GK from XML roster
- `src/ingestion/skillcorner.py` — extract GK from kloppy Player.position
- `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`
- `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`
- `dbt_project/models/staging/skillcorner/stg_skillcorner__tracking.sql`
- `dbt_project/models/marts/fct_tracking_frames.sql`
- `dbt_project/models/marts/_marts__models.yml`
- `src/ingestion/formations.py`
- `hf_taipy_app/src/pages/team_shape.py` (re-enable Formation metric)
- `hf_taipy_app/src/state/team_shape.py` (re-enable formation state)

**Dependencies:** None — can be done independently of other work.
**Unlocks:** Formation metric on Team Shape page, future position-group analytics (defensive line by role, pressing by position, etc.)

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. Root cause: the `/api/2.0/postgres/` surface (Autoscaling) has zero synced table endpoints — UI is the only method. The Provisioned API (`/api/2.0/database/synced_tables`) uses `database_instance_name` with no project/branch equivalent. GitHub issue filed: [terraform-provider-databricks#5456](https://github.com/databricks/terraform-provider-databricks/issues/5456). Related: [#5389](https://github.com/databricks/terraform-provider-databricks/issues/5389) (same gap for `databricks_database_database_catalog`). **Update 2026-03-06:** Connected with a Databricks Solution Architect at SSAC26 conference (LinkedIn). Bug report reference being forwarded for internal triage. | Blocked on Databricks API team adding synced table endpoints to `/api/2.0/postgres/`. Provider cannot be fixed until upstream API exists. |
| 2 | PG index recreation after synced table changes | `scripts/create_indexes.py` | Custom indexes dropped on synced table recreation. Must re-run script manually. | Operational procedure; automated via `create_indexes.py --verify`. |
| 6 | Line-breaking Path B limited to Metrica only | `line_breaking.py` | IDSSE events now ingested (D9), but line-breaking not yet wired to ELASTIC-aligned events. SkillCorner (10 matches) has tracking but no event data. | IDSSE: wire ELASTIC sync to line-breaking. SkillCorner: blocked on event data procurement or ball trajectory detection. |
| 7 | Single-frame 360 analysis | `line_breaking.py` | Path A uses opponent positions at pass moment only. Dual-frame would be more robust. | 360 freeze frames lack temporal resolution. Data limitation. |
| 9 | Fixed 3-cluster assumption | `analytics/line_breaking.py` | Ward clustering with `n_clusters=3` assumes 3 defensive lines. Breaks for 5-depth formations. | Research task — needs silhouette score analysis. |
| 10 | No set-piece exclusion | `analytics/line_breaking.py` | Corners, free kicks, throw-ins have non-standard formations. | Research task — needs `pass_type` filtering or set-piece-aware algorithm. |
| 11 | Heat Map pre-aggregation lossy | `heat_map.py` | Server-side `GROUP BY round(x/10)` bins into 10-yard cells before `bin_statistic`. Per-action precision lost. | Acceptable trade-off for density visualization. |
| 13 | PPDA StatsBomb-only | `fct_match_summary.sql` | PPDA uses StatsBomb defensive actions. NULL for Wyscout-only competitions (different event taxonomy). | Data limitation. Would require event type mapping or different pressing proxy. |
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
| D22 | NannyML CBPE for Model Monitoring | Evaluated 2026-03-25. All current models have immediate ground truth — CBPE's value (performance estimation without ground truth) does not apply. Existing scipy-based drift detection (PSI, Wasserstein, KS, CUSUM) covers all validation scenarios. | When a real-time inference use case appears where ground truth is delayed. |
| E1 | `for_each_task` fan-out | Databricks workflow `for_each_task` for match-level parallelism. Currently all pipelines use `applyInPandas` within a single job which is sufficient. | When single-job wall clock exceeds 2hr timeout or Respo.Vision data arrives (~7M rows/match). |
| E2 | Change Data Feed (CDF) | Delta `table_changes()` for incremental downstream consumption. No downstream consumer currently needs change tracking. | When a streaming consumer (e.g., real-time dashboard, ML feature store) is added. |
| E3 | Dead Letter Channel | Failed record quarantine to `bronze.dead_letters` table. Current retry logic handles transient errors; no persistent failure pattern observed. | When ingestion sources become unreliable or data volume exceeds manual inspection. |
| E4 | `dbt clone` for staging | Zero-copy table references for pre-production validation. Requires Lakebase branching. | When staging environment (ROADMAP.md) is implemented. |
| E6 | Delta retention policy | Explicit `delta.deletedFileRetentionDuration` (30d gold, 7d bronze) ahead of DBR 18.0. | Before DBR 18.0 upgrade where `RETAIN X HOURS` in manual VACUUM is ignored. |

---

## Research & Future Work

See [ROADMAP.md](ROADMAP.md) for research directions, long-horizon features, and unscheduled ideas including:

- **Observability Layer (OpenTelemetry)** — instrument once, observe anywhere; ~$1-2/month personal tier
- **Deep Learning Infrastructure** — hybrid GPU training, pre-trained soccer models, DeepMind-inspired optimization
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
- **Team Shape Analysis** — Stage 2 blocked on SkillCorner DoD
- **Visual Exploratory Behavior** — blocked by own-footage Respo.Vision data (BSD 3-Clause)
- **Staging Environment** — Lakebase branching for pre-production validation
- **Graph-Based Tactical Patterns** — GNN research direction (Raabe et al. 2022)
- **Decision Optimization** — RL-based pass optimization beyond VAEP (Rahimian et al.)
- **HuggingFace Hub** — Tier 5 (streaming dataset publishing via XET + Polars) blocked on upstream

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
