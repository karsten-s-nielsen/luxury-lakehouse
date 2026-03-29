# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [ARCHITECTURE.md](ARCHITECTURE.md). For research directions and unscheduled ideas, see [ROADMAP.md](ROADMAP.md).

**Last updated**: 2026-03-28

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
| D38 | Extend ruff S (bandit) rules to `scripts/` | Dunkin' | SEC-AUDIT | `scripts/` excluded from ruff at `pyproject.toml:103` — 22 Python files bypass all bandit security linting including PAT/token-handling code. Remove exclusion, triage ~7-8 violations (S603 subprocess, S108 temp files, S607 partial path). Note: `ensure_warehouse.py:150` passes `sys.argv` to `subprocess.run()` unsuppressed — highest priority |
| D39 | Add SAST tooling to CI | Dunkin' | SEC-AUDIT | Defense-in-depth beyond ruff S — no data-flow or taint analysis in CI. Repo is private → Semgrep Community (`p/python` + `p/security-audit` rulesets) is free; CodeQL requires GitHub Advanced Security. Add workflow, ~2 min added to CI. Relevant for EU AI Act Article 9 technical documentation |
| D40 | SQL warehouse ACL grants in Terraform | Dunkin' | SEC-AUDIT | `sql_warehouse/main.tf` creates the warehouse without any `databricks_grants` — access relies on workspace defaults. Add explicit grants scoped to ingestion SP + Terraform CI SP (catalog module has the established pattern). Straightforward Terraform change |
| D41 | Audit + document MLflow pyfunc pickle exposure | Dunkin' | SEC-AUDIT | `mlflow.pyfunc.load_model()` at `defcon_lite.py:73` and `spadl_vaep.py:625` deserializes cloudpickle on the driver (MLflow pyfunc flavor always uses cloudpickle internally). Executors are clean — JSON bytes via `get_booster().save_raw("json")`. Risk bounded by UC ACLs. Document which serializer each registered model uses and whether `weights_only=True` or safetensors alternatives are feasible |
| D42 | Data classification tags on dbt models | Dunkin' | SEC-AUDIT | Add `meta: { data_sensitivity: "public", contains_pii: false }` to all model YAML configs. Zero `meta:` blocks exist today across 13 YAML files (~45 models). Establishes governance metadata pattern for data catalog |
| D43 | Coordinate system normalization layer | Wicked | SEC-AUDIT | 5 providers use 4+ distinct coordinate systems and 3 sampling rates (25fps, 10fps, event-only). Transforms currently duplicated as inline expressions across 7 dbt staging SQL files with no shared macro; `line_breaking.py:241-249` also duplicates the Metrica formula. Design a canonical coordinate transform — shared dbt macro or `src/analytics/` module with documented transforms per provider. Cross-references D28, D29, D30 |
| D35 | AI/ML Workflows — Detail Drilldown & Card Validation | Wicked | [2026-03-23-taipy-workflows-page.md](docs/superpowers/plans/2026-03-23-taipy-workflows-page.md) | Enable the 8-section detail drilldown panel (designed but disabled — UX design not settled). Sections: overview, data flow, execution config, monitoring, cost breakdown, academic provenance, dependencies, changelog. Requires design decisions on navigation (slide-in panel vs sub-page vs modal) and information density. Also: validate all 16 workflow card YAML files against reality — the drilldown makes card data visible for the first time, so expect corrections to estimates, dependencies, and monitoring thresholds |
| D28 | Position-Group Z-Scoring for Stat Vectors | Dunkin' | [adversarial-training.md](docs/research/adversarial-training.md) | Normalize stat vectors within `position_group` (GK, Def, Mid, Fwd) instead of globally. Fixes goalkeeper contamination in similarity search (GKs scored as top passers). Change in `_compute_stat_vectors()` — add groupby on `position_group` before z-score. Trivial but immediately improves embedding quality. Pre-requisite for D18 |
| D29 | SPADL Vocabulary Upgrade for Embeddings | Dunkin' | [adversarial-training.md](docs/research/adversarial-training.md) | Replace 12-13 type action tokenizer in `football2vec.py` with 23-type SPADL taxonomy from `fct_action_values`. Richer behavioral tokens at zero additional data cost — distinguishes tackle/interception, corner_short/corner_crossed, freekick variants. SPADL data already exists (~9.5M actions). Pre-requisite for D18 |
| D18 | Football2vec v2 — Transformer Embeddings | Wicked | [ROADMAP.md](ROADMAP.md) | Replace Doc2Vec (gensim, CPU) with a small transformer on tokenized match sequences. Train on HF Jobs GPU (A10G). 87K player-match documents in Delta. Better player representations for similarity search. Publish to HF Hub. Benefits from D28 (position-group z-scoring) and D29 (SPADL vocabulary) landing first |
| D30 | Adversarial Team Debiasing (Gradient Reversal) | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Add team adversary head with gradient reversal layer (Ganin et al. 2016 DANN, lambda=0.2) to D18's transformer model. Produces team-agnostic player embeddings — answers "who plays like X regardless of system" instead of "who plays in a similar system." Hard negative mining: same position_group, same team_id. Cross-source entity resolution (11,918 players in both StatsBomb + Wyscout) provides natural validation. Train on HF Jobs GPU. Publish debiased embeddings to HF Hub alongside current ones (dual-track). Follows D18 |
| D31 | 360-Enriched Situational Context for Embeddings | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extend `set_encoder.py` Deep Sets architecture to produce a 16-32d situational context vector from 360 freeze frames (15.58M rows, 323 matches). Concatenate with action token embedding before transformer encoding. Encodes spatial relationships (pressing intensity, passing lanes, defensive shape) around each event. Constraint: 360 frames are anonymous (no player_id), so encodes spatial structure, not player-specific graphs. Follows D30 |
| D32 | ScoutGPT-Style Sequence Model — Training & Evaluation | Wicked | [adversarial-training.md](docs/research/adversarial-training.md), [arXiv:2512.17266](https://arxiv.org/abs/2512.17266) | Player-conditioned GPT transformer over ~9.5M SPADL action sequences (Hong et al. 2025). Architecture: transformer decoder with player ID embedding table (11,918 players), 23-type action embeddings, spatial encodings (x/y), autoregressive next-action prediction. Player ID as conditioning token enables counterfactual substitution (swap ID → "what would Messi do here?"). VAEP as reward signal. Train on HF Jobs GPU (A10G, comparable to ScoutGPT's 5 PL seasons). Evaluate: next-action accuracy, counterfactual ranking correlation, cross-source validation. Publish weights + config to HF Hub. Pre-req: D29 (SPADL vocab). Benefits from D18 (transformer experience) and D30 (adversarial objective can be integrated) |
| D33 | ScoutGPT Integration — Embeddings, pgvector & Taipy | Wicked | [adversarial-training.md](docs/research/adversarial-training.md) | Extract player embeddings from trained D32 model. Write to Delta via new `fct_player_embeddings_sequence` mart (or extend existing). Synced table + pgvector HNSW index. Add model selector to Player Similarity Taipy page ("Football2vec" vs "ScoutGPT"). Counterfactual substitution UI: "what would Player X do in Team Y's possessions?" Side-by-side comparison dashboard between old and new embeddings. Follows D32 |
| D36 | Shape Graph Algorithm — Core Implementation | Wicked | [Sotudeh (2026), ETH Zurich DISS. 31732](https://doi.org/10.1038/s44260-025-00047-x) | Implement Sotudeh's shape graph algorithm as a second formation detection method alongside EFPI. Delaunay triangulation → angular stability → iterative edge removal → shape graph. Position inference via 5×5 level decomposition (25 tactical labels). Pure geometry — no ML training, no templates needed. Uses `scipy.spatial.Delaunay` + numpy. See detailed write-up below. **Dependency:** D26 (GK metadata) for full 20-match coverage; can develop/test on IDSSE away teams (10 tracked players) |
| D37 | Position Maps — 5×5 Time-in-Position Matrix | Dunkin' | [Sotudeh (2026), ETH Zurich DISS. 31732](https://doi.org/10.1038/s44260-025-00047-x) | Compute per-player position maps from D36's frame-level position assignments. New mart table `fct_position_maps` (player_id, match_id, position_label, pct_time, phase). Three phase variants: all, in-possession, out-of-possession. Compact tactical player profile — complements spatial heatmaps with positional distributions. **Dependency:** D36 |
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

### Shape Graph Algorithm — Core Implementation (D36)

**Status:** Ready for implementation — algorithm fully specified with pseudocode
**Scope:** Wicked (2-3 sessions) — new analytics module + position inference + tests + benchmarks + workflow card
**Branch:** Separate feature branch from main
**Source:** Sotudeh, H. (2026). *Identification of Team Tactical Formations and Player Positions in Association Football.* PhD thesis, ETH Zurich (DISS. ETH NO. 31732). Published papers: [survey (Frontiers, DOI: 10.3389/fspor.2024.1512386)](https://doi.org/10.3389/fspor.2024.1512386), [shape graphs (npj Complexity, DOI: 10.1038/s44260-025-00047-x)](https://doi.org/10.1038/s44260-025-00047-x).

**What it is:** A bottom-up geometric formation detection method that complements the top-down template-matching approach in EFPI (D20). Instead of matching player positions against predefined templates, shape graphs build a stable subgraph of the Delaunay triangulation and infer positions from the graph's geometric structure. No formation library needed — positions emerge from geometry.

**Algorithm (thesis Algorithm 1, p.26):**
1. Compute Delaunay triangulation of outfield player (x, y) positions
2. Calculate angular stability for each edge (angle between circumcenters of incident triangles)
3. Find the least stable edge; if stability < 45°, remove it and merge the two incident faces
4. Recompute stabilities on the merged face edges
5. Repeat until all remaining edges have stability ≥ 45°
6. Result: the **shape graph** — a sparse, stable subgraph that filters Delaunay flicker noise

**Position inference (thesis Chapter 4):**
1. Decompose shape graph into vertical levels (B/DM/M/AM/F) using internal face centers
2. Decompose into horizontal levels (L/LC/C/RC/R) using face centers
3. Map each player's (vertical, horizontal) pair to one of 25 tactical positions via 5×5 matrix

**Key properties vs EFPI:**

| Dimension | EFPI (current) | Shape Graphs (D36) |
|-----------|---------------|-------------------|
| Approach | Top-down template matching | Bottom-up geometric |
| Templates | 68 from mplsoccer | None needed |
| Normalization | Elastic scaling to bbox | None — scale-invariant |
| Novel formations | Cannot discover | Can discover |
| Position labels | Template slot mapping | 5×5 level decomposition |

**Implementation plan:**
1. New module `src/analytics/shape_graph.py` — shape graph construction (Algorithm 1) + position inference (level decomposition)
2. Unit tests with known formation arrangements — thesis Figure 4.7 provides boundary cases (straight lines, trees, stars, circles, diamonds)
3. `pytest-benchmark` test — O(n²) worst case for n=10, expect sub-millisecond per frame
4. Workflow card `workflow-cards/wf-shape-graphs.yaml` with Sotudeh citations
5. Integration point: `src/ingestion/formations.py` runs both EFPI and shape graphs, stores results in parallel columns or a separate table

**Files affected:**
- `src/analytics/shape_graph.py` (new)
- `src/tests/test_shape_graph.py` (new)
- `workflow-cards/wf-shape-graphs.yaml` (new)
- `src/ingestion/formations.py` — add shape graph detection alongside EFPI

**Dependencies:** D26 (GK metadata) for full tracking coverage. Can develop and test without it — IDSSE away teams have 10 tracked players, plus synthetic test data from thesis boundary cases.
**Unlocks:** D37 (position maps), future Taipy visualizations (position plots, dual-detector comparison — see [ROADMAP.md](ROADMAP.md))
**Data sources validated by thesis:** Metrica Sports open data + IDSSE (Bundesliga) — both already ingested in this project

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
- **Shape Graph Visualizations & Tactical Applications** — position plots, dual-detector UX, scouting via position maps (needs D36/D37 first)
- **Visual Exploratory Behavior** — partially unblocked: 6 Veo3 recordings + local RTMO pose estimation feasible (BSD 3-Clause)
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
