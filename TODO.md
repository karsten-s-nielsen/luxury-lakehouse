# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-04-08 (Guard pipeline hardening: D48 chunked MERGE + exception surfacing, D49 import isolation, D50 schema fixes, D46 ID cleanup, D47 full guards, D40e parallel gate, 11 conformance test classes)

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
| D40c | Terraform `condition_task` Gates (Blocked) | Wicked | D40 follow-up | Dynamic value references (`{{tasks.X.values.Y}}`) are rejected by the Databricks Jobs API in `condition_task` operands. The freshness gate writes `{wf_id}-count` integer task values specifically for this. **Current mitigation (D40e cycle):** `WorkflowSkippedError` skip works correctly — pipelines check `filter_result.count == 0` and skip in <1s after env loads. Freshness gate now parallelized (196s total). The only win from `condition_task` is avoiding ~4s serverless cold-start per skipped task (~60-80s across 15-20 skipped tasks per run). **Investigation avenues:** (1) The `{{tasks.X.values.Y}}` syntax may be wrong — try `tasks.freshness_gate.values["wf-pitch-control-count"] > 0` or other expression formats; check Databricks REST API docs for `condition_task` expression language (may differ from `dbutils.jobs.taskValues` references), (2) test via manual `PUT /api/2.1/jobs/update` with a single task before wiring Terraform, (3) alternative: lightweight Python `condition_task` that reads task value and exits — but adds 27 gate tasks with cold-start overhead, likely worse than current skip. The Terraform module has a TODO(D40c) comment marking where gates should go. **Priority:** Low — functional correctness already achieved via code-level skip. |
| D40d | `for_each_task` Fan-Out for Pitch Control + Off-Ball xT | Wicked | D40 follow-up | Guards compute `FilterResult.chunks` (2 matches/chunk) but Terraform does not wire `for_each_task`. **Steps:** (1) Monitor per-task runtimes from `system.lakeflow.job_task_run_timeline`, (2) verify `for_each_task` Terraform syntax, (3) wire fan-out for `compute_pitch_control` and `compute_off_ball_xt` — each iteration receives a chunk from the gate's task values, (4) test with a manual job run. Respo.Vision will require fan-out from day one when data arrives. |
| D35 | AI/ML Workflows — Detail Drilldown & Card Validation | Wicked | [2026-03-23-taipy-workflows-page.md](docs/superpowers/plans/2026-03-23-taipy-workflows-page.md) | Enable the 8-section detail drilldown panel (designed but disabled — UX design not settled). Sections: overview, data flow, execution config, monitoring, cost breakdown, academic provenance, dependencies, changelog. Requires design decisions on navigation (slide-in panel vs sub-page vs modal) and information density. Also: validate all 16 workflow card YAML files against reality — the drilldown makes card data visible for the first time, so expect corrections to estimates, dependencies, and monitoring thresholds |
| D33 | ScoutGPT Integration — Embeddings, pgvector & Taipy | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extract player embeddings from trained ScoutGPT model. Write to Delta via new `fct_player_embeddings_sequence` mart (or extend existing). Synced table + pgvector HNSW index. Add model selector to Player Similarity Taipy page ("Football2vec" vs "ScoutGPT"). Counterfactual substitution UI: "what would Player X do in Team Y's possessions?" Side-by-side comparison dashboard between old and new embeddings. Follows D32 (complete) |
| D7 | Observability Layer (OTel) | Monstah | [ROADMAP.md](ROADMAP.md) | Research complete, ready for implementation. Instrument once, observe anywhere. ~$1-2/month personal tier |
| U3 | Global player search — search by name across all pages | Monstah | CHI-AUDIT-180-rev-1 #1 | New search component with 11,918-player index + cross-page routing + session state. Needs design decisions |
| U4 | Uncertainty/confidence bounds on model outputs | Monstah | CHI-AUDIT-180-rev-1 #4 | xG v2 now outputs MC dropout 95% CI (`xg_ci_lower`, `xg_ci_upper`). VAEP/pitch control still lack native uncertainty. Partial — xG done, others remain |
| M1 | Rotate Databricks PAT for HF Spaces | Dunkin' | HF-MIGRATION | PAT created 2026-03-16 with 90-day lifetime. **Expires ~2026-06-14.** Generate new PAT in Databricks workspace Settings → Developer → Access tokens, then update `DATABRICKS_TOKEN` secret at huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app/settings |
| D45 | Football2vec v2 — StatsBomb 128d embeddings + helper restructuring | Wicked | Session 29 investigation (2026-04-07) | **Root cause:** v2 model trained + inferred on Wyscout-only snapshot (22,726 rows, 2026-03-31). StatsBomb training data added afterwards but inference never re-run. **Pre-work done (session 29):** safetensors conversion uploaded to HF Hub (both stages), `_load_stage1` code expects `.safetensors` (correct), dbt dynamic-dimension fix in place as safety net. **Remaining steps:** (1) Move `train_football2vec_v2_helpers.py` into wheel package (`src/analytics/` or `src/ingestion/`) so HF Jobs can import it — current multi-file script can't run on HF Jobs, (2) Re-run `--stage 2` inference-only on current dataset (~87K sequences, both StatsBomb + Wyscout) via HF Jobs `l40sx1`, (3) Re-run `compute_embeddings_v2` Databricks task to import fresh Parquet → writes both 128d partitions. **Secondary fix:** v1 skip guard (`player_embeddings_v1.py:192-229`) is global (match-level) — if any new match appears, v1 re-runs for ALL sources and overwrites v2 128d with v1 32d. Must be scoped to only process matches/sources not covered by v2 |
| M2 | Deploy OAuth M2M credentials to HF Spaces | Dunkin' | SEC-AUDIT-200 #2 | All infrastructure ready: PG role created (`scripts/setup_lakebase_roles.py`), `config.py` supports OAuth env vars, `manage_space.py` deploys OAuth secrets. **Steps:** (1) Retrieve OAuth client secret from Databricks workspace (Settings → Service Principals → luxury-lakehouse-hf-app-v2-dev), (2) `export DATABRICKS_CLIENT_ID=1a1dbf08-df56-48de-b97a-276b2a4232d8 DATABRICKS_CLIENT_SECRET=<secret>`, (3) test locally, (4) deploy staging (`python scripts/manage_space.py deploy staging`), (5) verify, (6) deploy production, (7) remove `DATABRICKS_TOKEN` PAT from both Spaces. One-time operation — OAuth M2M credentials don't expire. Completes M2, makes M1 (PAT rotation) unnecessary |
| PA1 | Game State Segmentation | Wicked | Performance analysis courses (all 5) | Cross-cutting enhancement: segment all metrics by game state (winning/losing/level), time periods, and "Big Five Moments" (first/last 5 min of each half + immediately post-goal). Adds the context layer that transforms descriptive stats into coaching-actionable insight. **Implementation:** New shared filter in `state/shared.py` (game_state enum from `fct_match_summary` scoreline + timestamp), wire into all 16 page queries. Each page gains a "Game State" dropdown that re-slices its data. dbt prerequisite: add `game_state` and `match_period` columns to relevant mart models (derivable from existing timestamp + scoreline data). |
| PA2 | Set Piece Effectiveness Page | Wicked | Performance analysis courses (all 5) | Zero set piece analysis across 16 pages, yet set pieces account for ~25% of WC 2022 goals (Power et al., STATS AI Group). New page: corner delivery analysis (in-swing 2.7% vs out-swing 2.2%, flick-on 4.8% vs direct 2.0%), FK crosses vs direct shots (1.1% vs 7.2%), crossing zone heatmap (Gelade 2017 OptaPro: Zone 4 cutbacks 7.2% vs Zone 1 deep crosses 0.2%), throw-in outcomes by zone. StatsBomb event data has set piece type, delivery zone, and outcome metadata. **Template:** `PageConfig` with sidebar filters (set piece type, side, competition) + content blocks (zone heatmap, conversion funnel, outcome breakdown). |
| PA3 | Throw-In Analytics Page | Wicked | Performance analysis courses (L3, Gronnemark Throw-In Academy) | 40-60 throw-ins per match, 15-20 min of play — currently invisible in the platform. Gronnemark's coaching produced +23 percentage point improvement at Liverpool (45.4% → 68.4% possession retention under pressure, EPL rank #18 → #1). **Metrics from StatsBomb events:** per-zone count (defending/middle/attacking third), possession retention rate (sequence analysis: did team retain after throw?), fast throw-in % (time from ball out to throw, where available), throw-in direction (forward/backward/lateral), throw-to-chance/goal sequences. **Tracking data extension (where available):** effective range, opponent defending pattern classification, space created pre-throw. Unique platform differentiator — very few analytics tools cover throw-ins at all. |
| PA4 | Conversion Rate Funnel Page | Wicked | Performance analysis courses (all 5) | The most-requested analytical view across 6 PA courses. Possessions → Attacking 3rd Entries → Chances → Goals with conversion rates at each step. Maps directly to Donnelly's systematic approach ("The What" → "The Outcome" with context in between). **Data:** `fct_action_values` (SPADL actions with start/end coords define A3 entries), `fct_shots` (chances + goals). **Implementation:** Sankey or funnel visualization showing drop-off at each stage. Per-team comparison, per-match drill-down, multi-match trend. Sidebar filters: competition, team, match, game state (if PA1 done first). |
| SEC1 | EU AI Act Gap Analysis | Wicked | SEC-AUDIT-v1.12.0 REG-01 | Document which existing models (xG v1/v2, VAEP, pitch control, DEFCON, similarity search, PSxG) could be classified as high-risk under EU AI Act Annex III Category 4 (Employment) if used by a club for employment decisions (contract renewal, squad selection, transfers). **Deliverables:** (1) Risk classification per model, (2) applicable conformity assessment obligations, (3) required technical documentation, (4) human oversight mechanisms, (5) fairness analysis. **Mitigating context:** personal project, not sold to clubs, all public data — but documentation needed for governance maturity. **Compliance deadline: August 2, 2026.** |
| SEC2 | Model Artifact Integrity Verification | Dunkin' | SEC-AUDIT-v1.12.0 ML-02 (CWE-345) | No checksum or signature verification when loading model weights from MLflow, UC Volume, or HF Hub. Defense-in-depth: verify SHA-256 hash of artifact after download, before inference. **Steps:** (1) Add `_verify_artifact_hash()` helper to `src/ingestion/utils.py`, (2) store expected hashes alongside model artifacts (MLflow tags or sidecar `.sha256` files), (3) wire verification into `defcon_lite.py`, `spadl_vaep.py`, `xg_model.py`, `xg_model_v2.py` model loading paths. |
| SEC3 | HF Jobs Wheel SHA-256 Pinning | Dunkin' | SEC-AUDIT-v1.12.0 ML-03 (CWE-494) | PEP 723 scripts in `scripts/*_hf.py` reference the luxury-lakehouse wheel by URL without explicit SHA-256 pin. Supply chain risk if the artifact is tampered with on HF Hub. **Steps:** (1) CI publishes wheel hash alongside wheel to `build-artifacts`, (2) PEP 723 dependency lines include `--hash=sha256:...`, (3) HF Jobs scripts verify hash before install. |
| SEC4 | CI Service Principal Least-Privilege | Dunkin' | SEC-AUDIT-v1.12.0 INF-01 (CWE-250) | CI service principal has `account_admin` + `ALL_PRIVILEGES` on catalog — broadest possible grant. Justified for `terraform plan` but over-privileged. **Steps:** (1) Audit which specific privileges CI actually needs (likely `USE_CATALOG`, `USE_SCHEMA`, `SELECT` on specific schemas), (2) replace blanket grants in `terraform/modules/service_principals/main.tf`, (3) verify `terraform plan` and `dbt slim CI` still pass with reduced permissions. |

---

## Technical Debt

### Blocked or Deferred

| # | Item | Location | Description | Blocker |
|---|------|----------|-------------|---------|
| 1 | Synced tables Terraform workaround | `terraform/` | Must create synced tables via UI + import due to missing provider fields. `lifecycle { ignore_changes = all }`. No schedule/cron field on resource — SNAPSHOT refresh requires manual trigger or external job. Workaround: `scripts/refresh_synced_tables.py`. Root cause: the `/api/2.0/postgres/` surface (Autoscaling) has zero synced table endpoints — UI is the only method. The Provisioned API (`/api/2.0/database/synced_tables`) uses `database_instance_name` with no project/branch equivalent. GitHub issue filed: [terraform-provider-databricks#5456](https://github.com/databricks/terraform-provider-databricks/issues/5456). Related: [#5389](https://github.com/databricks/terraform-provider-databricks/issues/5389) (same gap for `databricks_database_database_catalog`). **Update 2026-03-06:** Connected with a Databricks Solution Architect at SSAC26 conference (LinkedIn). Bug report reference being forwarded for internal triage. | Blocked on Databricks API team adding synced table endpoints to `/api/2.0/postgres/`. Provider cannot be fixed until upstream API exists. |
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
| 28 | Databricks budget automation | `terraform/` | AWS budget is automated via `aws_budgets_budget` ($100/month, 80%/100% alerts). Databricks spending alerts are manual (UI-only, $250/month set 2026-03-12). Investigate whether Databricks Budgets API or `databricks_budget` Terraform resource can automate this for consistency. | Next infrastructure cycle. |

### Deferred EIP / Optimization Items

Items from the Pipeline Optimization & Scaling (EIP) roadmap section that were evaluated and deferred. Core EIP patterns (Splitter, Aggregator, Router, Pipes & Filters) are already implemented and codified in CLAUDE.md.

| # | Item | Description | When to revisit |
|---|------|-------------|-----------------|
| D22 | NannyML CBPE for Model Monitoring | Evaluated 2026-03-25. All current models have immediate ground truth — CBPE's value (performance estimation without ground truth) does not apply. Existing scipy-based drift detection (PSI, Wasserstein, KS, CUSUM) covers all validation scenarios. | When a real-time inference use case appears where ground truth is delayed. |
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
- **Deep Learning Infrastructure** — GNN training, continual learning, pre-trained model integration
- **Provider Abstraction** — configurable multi-tier ingestion; free/open tiers default, commercial activates via credentials
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
