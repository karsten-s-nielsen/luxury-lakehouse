# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-04-07 (Evolve Stage 3 complete: 114 iterations, best rho 0.5946 (+35% over seed), 3.9M params (-41%), cross_attention seed updated with evolved config)

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
| D40 | Adaptive Pipeline Fan-Out — Scout/Gate + `for_each_task` | Monstah | Session 22 investigation (2026-04-01) | Three-layer architecture to cut job wall-clock from ~16m to ~5m steady-state. **Layer 1 — Freshness Gate:** Single task runs all skip guards, outputs JSON of which pipelines have new work + match counts. Tasks with no work are skipped entirely via `run_if` (saves ~3m overhead per skipped task). **Layer 2 — Scout + Fan-Out:** For pipelines with work, a scout task partitions unscored match IDs into chunks (~500 matches each) and outputs a JSON array. `for_each_task` creates one subtask per chunk, each with its own serverless executor pool. Threshold: <500 matches → 1 chunk (same as today); >=500 → N chunks for parallel scoring. **Layer 3 — Safe `applyInPandas`:** Each subtask uses `groupBy("match_id").applyInPandas()` — groups are always ~1,600 rows (~5 MB), safe by 160x margin. Model cache (`_model_cache`) loads once per executor, reused across groups. **Also includes:** (a) combine HF import/export tasks into single "hf_sync" task (saves 6m overhead), (b) dependency graph audit to identify overly conservative `depends_on` links that inflate the critical path, (c) CLAUDE.md rules for `applyInPandas` group key selection (prefer `match_id` for event data; estimate peak memory at largest expected group, not average). Pre-req: D41 (VAEP scoring fix — DONE, commit `56b8450`) |
| D35 | AI/ML Workflows — Detail Drilldown & Card Validation | Wicked | [2026-03-23-taipy-workflows-page.md](docs/superpowers/plans/2026-03-23-taipy-workflows-page.md) | Enable the 8-section detail drilldown panel (designed but disabled — UX design not settled). Sections: overview, data flow, execution config, monitoring, cost breakdown, academic provenance, dependencies, changelog. Requires design decisions on navigation (slide-in panel vs sub-page vs modal) and information density. Also: validate all 16 workflow card YAML files against reality — the drilldown makes card data visible for the first time, so expect corrections to estimates, dependencies, and monitoring thresholds |
| D33 | ScoutGPT Integration — Embeddings, pgvector & Taipy | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extract player embeddings from trained ScoutGPT model. Write to Delta via new `fct_player_embeddings_sequence` mart (or extend existing). Synced table + pgvector HNSW index. Add model selector to Player Similarity Taipy page ("Football2vec" vs "ScoutGPT"). Counterfactual substitution UI: "what would Player X do in Team Y's possessions?" Side-by-side comparison dashboard between old and new embeddings. Follows D32 (complete) |
| D7 | Observability Layer (OTel) | Monstah | [ROADMAP.md](ROADMAP.md) | Research complete, ready for implementation. Instrument once, observe anywhere. ~$1-2/month personal tier |
| U3 | Global player search — search by name across all pages | Monstah | CHI-AUDIT-180-rev-1 #1 | New search component with 11,918-player index + cross-page routing + session state. Needs design decisions |
| U4 | Uncertainty/confidence bounds on model outputs | Monstah | CHI-AUDIT-180-rev-1 #4 | xG v2 now outputs MC dropout 95% CI (`xg_ci_lower`, `xg_ci_upper`). VAEP/pitch control still lack native uncertainty. Partial — xG done, others remain |
| M1 | Rotate Databricks PAT for HF Spaces | Dunkin' | HF-MIGRATION | PAT created 2026-03-16 with 90-day lifetime. **Expires ~2026-06-14.** Generate new PAT in Databricks workspace Settings → Developer → Access tokens, then update `DATABRICKS_TOKEN` secret at huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app/settings |
| M2 | Deploy OAuth M2M credentials to HF Spaces | Dunkin' | SEC-AUDIT-200 #2 | All infrastructure ready: PG role created (`scripts/setup_lakebase_roles.py`), `config.py` supports OAuth env vars, `manage_space.py` deploys OAuth secrets. **Steps:** (1) Retrieve OAuth client secret from Databricks workspace (Settings → Service Principals → luxury-lakehouse-hf-app-v2-dev), (2) `export DATABRICKS_CLIENT_ID=1a1dbf08-df56-48de-b97a-276b2a4232d8 DATABRICKS_CLIENT_SECRET=<secret>`, (3) test locally, (4) deploy staging (`python scripts/manage_space.py deploy staging`), (5) verify, (6) deploy production, (7) remove `DATABRICKS_TOKEN` PAT from both Spaces. One-time operation — OAuth M2M credentials don't expire. Completes M2, makes M1 (PAT rotation) unnecessary |
| D44 | socceraction PR — Wyscout `keeper_claim` mapping | Dunkin' | GK data quality investigation (2026-04-05) | Upstream fix: `socceraction/spadl/wyscout.py` `determine_type_id()` maps all `type_id=9` to `keeper_save` unconditionally. No `keeper_claim`/`keeper_punch` differentiation. Fix: (a) add sub-type routing for `subtype_id=90/91` under `type_id=9` (need to verify semantics against raw Wyscout data in our bronze tables), (b) intercept GK aerial duels (`type_id=1, subtype_id=10` where player is GK) in `convert_duels()` before they're discarded → map to `keeper_claim`. StatsBomb path already fixed in this repo (`_TYPE_KEY_OVERRIDES`). MIT license, maintained by ML-KULeuven. Small PR (~20 LOC + test). Benefits entire soccer analytics ecosystem |
| D42 | Evolve Engine Level 2 — Code Evolution | Wicked | [evolve-engine-design.md](docs/superpowers/specs/2026-04-04-evolve-engine-design.md) | Upgrade from config-only mutation (Level 1) to code evolution: LLM generates `custom_embed()` functions that are monkey-patched onto `ScoutGPTDecoder._embed()` before training. Enables the LLM to invent entirely novel conditioning mechanisms beyond the 4 pre-implemented types. **Architecture audit note (2026-04-05):** Level 1 replaced `exec_module` with `ast.literal_eval` (no code execution) per CLAUDE.md "no dangerous builtins" rule. Level 2 deliberately breaches this boundary — requires a documented security policy upgrade. **Recommended approach:** AST allowlist + subprocess isolation (defense-in-depth). AST allowlist validates code structure at parse time (allow `torch.*`, arithmetic, control flow; reject imports, I/O, `__import__`, attribute access outside `torch.*`). Subprocess isolation provides runtime containment (no network, restricted env, hard timeout) — the existing `remote_worker.py` pattern is 80% of this. RestrictedPython was considered but fights PyTorch's dynamic dispatch. An ADR must be filed before implementation. Includes: (a) evaluator code-patching logic with AST allowlist validation, (b) prompt engineering for OpenEvolve to generate valid PyTorch code, (c) subprocess sandbox (no network, restricted env, timeout), (d) seed programs with example `custom_embed()` to bootstrap LLM generation. **Update 2026-04-07 (Stage 3 complete):** 114 iterations (early-stopped), best combined_score 0.6622 (+18.8% over seed), spearman_rho 0.5946 (+35.4%), 3.9M params (-41% vs 6.6M seed). All 120 final programs are cross_attention hyperparameter variants — the config-only search space is exhausted. Key discovery: multi-task loss (vaep 0.32 + player_prediction 0.18) is the primary driver, not architecture size. Level 2 code evolution is the logical next step — structural changes (hybrid conditioning, novel attention patterns, learned gating) cannot be expressed as config dicts. Infrastructure fixes applied: per-worker backend claiming, checkpoint_interval=5, domain-specific LLM prompt (0% rejection rate vs 28% before), SSH orphan cleanup on timeout. Pre-req: ADR for security policy |

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
