# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md).

**Last updated**: 2026-03-01

---

## Phase 0 — Foundation (Complete)

- [x] ~~**Rotate Databricks token**~~ — Done
- [x] ~~**Rename local folder**~~ — Done
- [x] ~~**Verify Terraform in PATH**~~ — v1.14.6
- [x] ~~**Verify AWS access**~~ — `devops-agent` role working (account 454762693631)
- [x] ~~**Configure Terraform state backend**~~ — S3 bucket `karstenskyt-terraform-state`, native S3 locking
- [x] ~~**Run `terraform init`**~~ — Successful
- [x] ~~**Run `terraform plan`**~~ — 16 resources, 0 errors
- [x] ~~**Initial git commit + push**~~ — Commit `5995bee`, pushed to `karstenskyt/luxury-lakehouse`

## Phase 1 — Serverless Infrastructure (Complete)

- [x] ~~`terraform apply` — workspace module~~ — Unity Catalog `soccer_analytics` (created via SQL + imported; Default Storage workaround)
- [x] ~~`terraform apply` — catalog module~~ — bronze, silver, gold schemas
- [x] ~~`terraform apply` — sql_warehouse module~~ — Serverless SQL, 2X-Small, auto-stop 10min
- [x] ~~`terraform apply` — lakebase module~~ — PostgreSQL 16, CU_1 capacity (stopped for cost savings until Phase 4)
- [x] ~~`terraform apply` — workflows module~~ — Ingestion job (paused in dev)
- [x] ~~`terraform apply` — app module~~ — Streamlit Databricks App
- [x] ~~`terraform apply` — synced_tables module~~ — 8 synced tables (Gold Delta → Lakebase PostgreSQL)
- [x] ~~Verify all resources in Databricks UI~~ — 8/8 confirmed via REST API
- [x] ~~Run `/final-review` + regenerate C4 diagrams~~

## Phase 2 — Data Ingestion (Complete)

- [x] ~~Implement `src/ingestion/utils.py`~~ — shared CLI, logging, HTTP, Delta helpers, validation
- [x] ~~Implement `src/ingestion/statsbomb.py`~~ — fetch from StatsBomb API, write to Bronze (5 tables)
- [x] ~~Implement `src/ingestion/metrica.py`~~ — fetch tracking CSV, write to Bronze (2 tables)
- [x] ~~Implement `src/ingestion/wyscout.py`~~ — fetch event JSON, write to Bronze (2 tables)
- [x] ~~Unit tests~~ — 55 tests passing (utils, statsbomb, metrica, wyscout)
- [x] ~~Quality gates~~ — ruff (0 violations), pyright (0 errors), pytest (55/55 pass), wheel build OK
- [x] ~~Deploy wheel to Databricks and trigger ingestion job~~ — wheel uploaded to UC Volume, job triggered
- [x] ~~Verify Bronze tables populated~~ — 9 tables, 31.4M rows total
- [x] ~~Run `/final-review`~~

## Phase 3 — Transformation (dbt)

- [x] ~~Configure `dbt_project/profiles.yml` with Databricks connection~~
- [x] ~~Run `dbt deps` to install packages~~
- [x] ~~Implement staging models (Silver): flatten nested JSON, parse coordinates~~
- [x] ~~Implement intermediate models: unified shots and passes~~
- [x] ~~Implement mart models (Gold): fct_shots, fct_passes, fct_player_stats, fct_match_summary~~
- [x] ~~Implement tracking models: fct_tracking_frames, fct_player_embeddings~~
- [x] ~~Implement dimensions: dim_players, dim_teams, dim_competitions~~
- [x] ~~**Security:** Add dbt tests (`unique`, `not_null`, `accepted_values`) on all silver/gold models~~
- [x] ~~**Security:** Define Unity Catalog `grants` in dbt for schema-level access control~~
- [x] ~~Run `dbt build` — all models + tests pass~~ (PASS=130 WARN=14 ERROR=0, 8/8 source freshness)
- [x] ~~Migrate `calogica/dbt_expectations` → `metaplane/dbt_expectations`~~
- [x] ~~Move source freshness config to `config` block (dbt 1.11+ deprecation)~~
- [x] ~~**Tech debt:** Extract hardcoded thresholds to dbt vars~~ — 6 new vars, 7 files updated (`a6bdeec`)
- [x] ~~**Tech debt:** Refactor dual `source()`/`ref()` pattern~~ — stg_statsbomb__events expanded, shots/passes/minutes use `ref()` only (`487be82`)
- [x] ~~**Tech debt:** DRY `from_json()` calls~~ — parse-once CTEs in wyscout events and statsbomb lineups (`a532038`)
- [x] ~~**Tech debt:** Enable `use_materialization_v2` flag~~ — (`7c02afc`)
- [x] ~~**Tech debt:** Nest test arguments under `arguments` property~~ — 44 tests migrated across 5 YAML files (`5b229fa`)
- [x] ~~**Tech debt:** Add missing `accepted_values` tests~~ — 7 new tests on categorical columns (`9127ecc`)
- [x] ~~**Tech debt:** Add missing range tests~~ — 13 new range tests on numeric columns (`710a977`)
- [x] ~~**Tech debt:** Document all undocumented YAML columns~~ — ~100 column descriptions added (`048fdbf`)
- [x] ~~**Tech debt:** Integrate `position_mapping.csv` seed into `dim_players`~~ — `position_group` column with accepted_values test (`1d5b868`)
- [x] ~~Run `/final-review`~~

## Phase 4 — Zero-ETL Synchronization (Complete)

- [x] ~~Configure Synced Tables (Gold Delta → Lakebase)~~ — 8 tables synced via `databricks_database_synced_database_table`
- [x] ~~Fix PK mismatches~~ — `fct_player_stats` → `player_stats_id`, `fct_player_embeddings` → `embedding_id`
- [x] ~~Add `gold_schema` variable~~ — handles dbt's `dev_gold` naming (env prefix on custom schemas)
- [x] ~~Wake Lakebase instance~~ — `stopped = false`, CU_1 capacity
- [x] ~~Uncomment synced_tables module~~ — `terraform apply` creates 8 synced table resources
- [x] ~~Run `dbt build`~~ — 19 models materialized (PASS=148 WARN=14 ERROR=0), gold tables populated
- [x] ~~`terraform apply`~~ — 8 synced tables created, all reached `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`
- [x] ~~Verify synced table row counts~~ — fct_passes 5.05M, fct_shots 131K, fct_player_stats 20K, dim_players 11K
- [x] ~~**Security:** Add `resources` block to `databricks_app` for least-privilege SQL warehouse access~~ (`b376289`)
- [x] ~~**Security:** Restrict Lakebase connections to Streamlit app service principal only~~ — done in Phase 5 (SP role creation + PG grants)
- [x] ~~**Security:** Configure connection pooling with 55min recycle~~ — done in Security Audit (L-6)
- [x] ~~Run `/final-review`~~

## Pre-Phase-5 Fixes (Complete)

- [x] ~~Consolidate `_serialize_json_columns` into `utils.py`~~ — single shared function with optional `columns` parameter
- [x] ~~Add `table_name` validation to `write_delta_table`~~ — `_IDENTIFIER_RE` check prevents SQL injection
- [x] ~~Add `validate_dataframe` calls to statsbomb `_write_batch` and matches write~~ — schema + non-empty checks
- [x] ~~Fix `_read_existing_match_ids` silent exception~~ — now logs at DEBUG with `exc_info`
- [x] ~~Fix `_safe_fetch` traceback loss~~ — `logger.exception` replaces `logger.warning`
- [x] ~~Fix wyscout `main()` duplication~~ — uses `parse_ingestion_args` with `extra_args`, eliminates `print()` and private `_IDENTIFIER_RE` import
- [x] ~~Fix `monkeypatch` type annotation~~ — `pytest.MonkeyPatch` replaces `object`
- [x] ~~Fix `known-first-party` in pyproject.toml~~ — `["ingestion", "streamlit_app"]` replaces `["luxury_lakehouse"]`
- [x] ~~Remove unused `warehouse_id` from workflows module~~
- [x] ~~Remove empty `default = ""` on `sql_warehouse_id` in app module~~
- [x] ~~Add composite key tests on intermediate models~~ — `unique_combination_of_columns` on passes, shots, minutes
- [x] ~~Fix redundant `ref()` in `int_minutes_played.sql`~~ — reuse `events` CTE
- [x] ~~Rename terminal CTE in `fct_tracking_frames.sql`~~ — `with_velocity` → `final`
- [x] ~~Delete unused macros~~ — `flatten_json.sql` (4 macros, all dead code)

## Phase 5 — Streamlit Application (Complete)

- [x] ~~Implement `src/streamlit_app/config.py`~~ — Pydantic BaseSettings with env var binding, identifier validation
- [x] ~~Implement `src/streamlit_app/db.py`~~ — OAuth M2M via SDK + REST fallback, JWT subject extraction, parameterized queries
- [x] ~~**Security:** Databricks App auth~~ — OAuth M2M with 55-min token refresh, SSL required, `verify=True`
- [x] ~~**Security:** Parameterized queries only~~ — all queries use `%s` placeholders, table names validated via `_IDENTIFIER_RE`
- [x] ~~Implement components~~ — `filters.py` (5 cascading widgets), `pitch.py` (mplsoccer wrappers), `charts.py` (radar + bar)
- [x] ~~Implement pages~~ — Shot Map, Pass Map, Player Radar, Match Summary (4 pages with `st.navigation`)
- [x] ~~Implement `app.py`~~ — entry point with `st.navigation`, dark theme, sidebar branding
- [x] ~~Unit tests~~ — 28 new tests (config: 6, db: 11, components: 11), total 83/83 passing
- [x] ~~Quality gates~~ — ruff (0 violations), pyright (0 errors), pytest (83/83 pass)
- [x] ~~Deploy as Databricks App~~ — `app.yaml` manifest, port 8000, PYTHONPATH=src
- [x] ~~Fix Lakebase connectivity~~ — PG schema discovery (`dev_gold`), SP role creation + grants (USE CATALOG, USE SCHEMA, SELECT, PG USAGE + SELECT)
- [x] ~~End-to-end smoke test~~ — all 4 pages loading, filters cascading, visualizations rendering
- [x] ~~Run `/final-review` + final C4 diagram update~~

## Security Audit (2026-02-27)

Full report: [SECURITY.md](SECURITY.md) — `mad-skills:security-audit` v1.5.0

- [x] ~~Terraform `count` on unknown SP `application_id`~~ — Fixed: `enable_ingestion_sp_grants` bool
- [x] ~~Deploying user lacks `servicePrincipal.user` role~~ — Fixed: `databricks_access_control_rule_set`
- [x] ~~**High:** Add `detect-secrets` to pre-commit (H-1)~~ — baseline generated
- [x] ~~**Medium:** Catch `psycopg2.Error` in `execute_query()` — sanitize tracebacks (M-2)~~
- [x] ~~**Medium:** Add `statement_timeout=30000` to Lakebase PG connection (M-3)~~
- [x] ~~**Medium:** Remove `unsafe_allow_html=True` in match_summary.py (M-1)~~ — replaced with `st.header` + `int()` casts
- [x] ~~**Medium:** Log auth failures in `_refresh_token()` (M-5)~~
- [x] ~~**Medium:** Add UUID format assertion on JWT `sub` claim (M-4)~~
- [x] ~~**Medium:** Remove `WRITE_VOLUME` from ingestion SP on libs volume (M-8)~~
- [x] ~~**Medium:** Migrate Terraform auth from PAT to OAuth M2M (M-6)~~ — OAuth M2M + GitHub OIDC federation
- [x] ~~**Medium:** Add `databricks_ip_access_list` for workspace API (M-7)~~ — accepted risk (requires static IPs)
- [x] ~~**Medium:** Verify Databricks Apps proxy injects security headers (M-9)~~ — HSTS + nosniff confirmed, app behind OAuth
- [x] ~~**Medium:** Move hardcoded infra IDs to env vars (M-10)~~
- [x] ~~**Low:** Document `WHERE {where}` pattern constraints (L-1)~~
- [x] ~~**Low:** Replace `SELECT *` with explicit column list in match_summary.py (L-2)~~
- [x] ~~**Low:** Implement connection pooling with 55min recycle (L-6)~~
- [x] ~~**Low:** Codify Lakebase PG grants in versioned SQL script (L-7)~~
- [x] ~~**Low:** Add `timeout_seconds` and `max_retries` to ingestion tasks (L-11)~~
- [x] ~~**Low:** Tighten dbt grants to configurable principal (L-15)~~
- [x] ~~**Low:** Add `int()` type assertions on filter IDs (L-3)~~
- [x] ~~**Low:** Make `_token_cache` thread-safe (L-4)~~ — guarded by `_pool_lock`
- [x] ~~**Low:** Document schema-level MODIFY rationale (L-8)~~ — accepted with comment
- [x] ~~**Low:** Make SP role grant principal configurable (L-9)~~ — `var.deployer_user_names`
- [x] ~~**Low:** Fix `uv sync --frozen` in dbt CI (L-12)~~
- [x] ~~**Low:** Collapse Terraform plan output in PR comments (L-13)~~ — `<details>` wrap
- [x] ~~**Low:** Log REST credential HTTP errors (L-14)~~ — `logger.error` before raise
- [x] ~~**Low:** Token memory zeroing (L-5)~~ — accepted risk (Python strings immutable)
- [x] ~~**Low:** S3 state encryption — KMS CMK with automatic rotation (L-10)~~

## Final Review (2026-02-27)

- [x] ~~**Bug:** Fix `required_columns` in statsbomb `_write_batch`~~ — `statsbombpy` returns `id`/`type`, not `event_id`/`type_name`
- [x] ~~**Test:** Split `test_connection_returned_on_error` into two tests~~ — one for `psycopg2.Error` (sanitized), one for `RuntimeError` (propagated)
- [x] ~~**Docs:** Update PLAN.md implementation status~~ — Phase 5 complete, 83 tests, 165 dbt tests
- [x] ~~**Docs:** Update SECURITY.md action plan~~ — mark resolved items in Next sprint and Backlog
- [x] ~~**Docs:** Regenerate C4 architecture diagrams~~

## Final Review (2026-02-28)

- [x] ~~**Bug:** Fix connection leak in `execute_query()`~~ — `getconn()` moved before `try` block to prevent `NameError` masking real errors
- [x] ~~**Bug:** Fix dbt CI missing `--extra dbt`~~ — `uv sync --frozen --extra dbt` ensures dbt packages are installed
- [x] ~~**Docs:** Update PLAN.md security section~~ — `sslmode=verify-full`, connection pooling, `statement_timeout`, SECURITY.md reference
- [x] ~~**Docs:** Regenerate C4 architecture diagrams~~

## Phase 5.5 — Migrate Lakebase to Autoscaling + PG 17 (Complete)

- [x] ~~Terraform module: `databricks_postgres_project` + endpoint (PG 17, autoscaling 0.5–4 CU, scale-to-zero)~~
- [x] ~~Synced tables: re-pointed to Autoscaling project, 8 tables ONLINE with data~~
- [x] ~~Streamlit app: new credential API, `sslmode=require`, retry logic for scale-to-zero~~
- [x] ~~PG grants, smoke test, quality gates (ruff 0, pyright 0, pytest 85/85)~~

## Phase 5.6 — IAM OIDC + OAuth M2M + KMS Hardening (Complete)

- [x] ~~OAuth M2M migration (M-6), KMS CMK for state encryption (L-10), AWS IAM OIDC + Databricks federation for secretless CI~~
- [x] ~~CI green on PR #6, all GitHub secrets deleted, 4 repo variables only~~

## Phase 6 — StatsBomb 360 Freeze Frames (Complete)

- [x] ~~Add `backfill_360()` + `backfill_360_main()` CLI entry point~~ — iterates matches with events but no 360 data
- [x] ~~Fix `statsbomb_360` source definition~~ — actual schema: `id`, `match_id`, `teammate`, `actor`, `keeper`, `location`, `visible_area` (statsbombpy pre-explodes)
- [x] ~~Add `stg_statsbomb__360` dbt staging model~~ — parse location JSON, dedup via ROW_NUMBER(), one row per player per event
- [x] ~~Add dbt tests on 360 staging model~~ — 15 tests: unique, not_null, relationships, accepted_values, range (PASS=16 WARN=0 ERROR=0)
- [x] ~~Run `backfill_statsbomb_360` on Databricks~~ — 15.58M rows, 323 matches, 5 competitions, 335s runtime
- [x] ~~Verify `stg_statsbomb__360` builds successfully via `dbt build`~~ — view created, all 15 tests pass

## Phase 7 — Metrica Game 3 (EPTS) + Pitch Control (Complete)

Games 1–2 were already ingested; Game 3 (FIFA EPTS format) now added with XML metadata, colon-delimited tracking, and JSON events parsers. Ball coordinate propagation fixed in `stg_metrica__tracking.sql`. Pitch Control page with Voronoi tessellation deployed.

- [x] ~~**Fix ball coordinate propagation** — `stg_metrica__tracking.sql` was casting `ball_x`/`ball_y` to NULL; now broadcasts frame-level ball coordinates~~
- [x] ~~**Add Game 3 ingestion** — EPTS parsers: `_parse_epts_metadata()` (XML), `_parse_epts_tracking()`, `_parse_epts_events()` (JSON)~~
- [x] ~~**Fix `_build_player_columns()`** — Metrica CSV header uses player name (not "x"/"y") in column_row; rewrote to track alternating x/y per pair~~
- [x] ~~**Unit tests** — 22 new tests for EPTS metadata, tracking, and events parsers (107 total)~~
- [x] ~~**Pitch Control page** — `plot_pitch_control()` with Voronoi tessellation, `pitch_control.py` page with match/period/frame filters~~
- [x] ~~**Sync fct_tracking_frames** — Terraform resource + Lakebase synced table (9.46M rows)~~
- [x] ~~**Lakebase auth fix** — `databricks_create_role()` required for SP OAuth JWT authentication; REST API fallback for credential generation~~
- [x] ~~Quality gates: ruff 0, pyright 0, pytest 107/107~~

## Next Up

Ordered execution plan for remaining work:

### Phase 8 — Heat Map + Pass Network Streamlit Pages

No cross-source dependency — these use existing synced gold tables (`fct_passes_synced`, `fct_shots_synced`).

- [ ] **Heat Map** — touch/action density maps per player or team
- [ ] **Pass Network** — graph visualization of passing connections between teammates

### Phase 9 — SPADL / VAEP Action Valuation (PLAN 14.7)

Convert event data to the SPADL (Soccer Player Action Description Language) unified format and train VAEP (Valuing Actions by Estimating Probabilities) models. Uses existing bronze event data — no new data sources needed.

- [ ] Add `kloppy` (v3.18+) and `socceraction` (v1.5+) as dependencies
- [ ] Build Kloppy → SPADL conversion pipeline for StatsBomb and Wyscout events (12 columns, 22 action types)
- [ ] Train VAEP gradient-boosted scoring model on SPADL actions (P(scoring within 10 actions) − P(conceding within 10 actions))
- [ ] Create `fct_action_values` gold table — per-action offensive and defensive VAEP scores
- [ ] Create Streamlit page — top players by VAEP/90, action value distributions

### Phase 10 — Additional Public Tracking Datasets (PLAN 14.8)

Ingest new open tracking datasets via Kloppy to expand beyond Metrica's 3 matches.

- [ ] **Bundesliga IDSSE** — 7 Bundesliga matches, TRACAB tracking (25fps) + DFL events, CC-BY 4.0 license (Nature Scientific Data, Feb 2025)
- [ ] **SkillCorner Open Data** — 10 A-League 2024/25 matches, JSONL tracking (10fps) + CSV events
- [ ] Build `src/ingestion/kloppy_loader.py` — generic Kloppy-based ingestion that normalizes any provider to the existing bronze tracking schema
- [ ] Verify `stg_metrica__tracking` and `fct_tracking_frames` handle multi-provider tracking data (or refactor to `stg_tracking`)

### Phase 11 — Physics-Based Pitch Control Model (PLAN 14.9)

Replace the current Voronoi approximation with a physics-based pitch control model using tracking data.

- [ ] Implement Spearman et al. (2017) pitch control model — player influence as a function of position, velocity, and time-to-intercept
- [ ] Populate `pitch_control_value` column in `fct_tracking_frames` (currently NULL) via Python UDF or MLflow model
- [ ] Update Streamlit Pitch Control page with continuous heatmap overlay (replacing Voronoi)
- [ ] Depends on tracking data from Metrica (Phase 7) + IDSSE/SkillCorner (Phase 10)

### Phase 12 — Cross-Source Player Entity Resolution (PLAN 14.2)

- [ ] **Prerequisite:** Request open-source license from `parmacalcio1913/players-matcher` — repo is currently unlicensed (all rights reserved). Cannot legally use without explicit permission.
- [ ] Integrate [`parmacalcio1913/players-matcher`](https://github.com/parmacalcio1913/players-matcher) — fuzzy-match players across StatsBomb, Metrica, and Wyscout into a canonical ID
- [ ] Build `int_player_xref` mapping — dbt intermediate model or seed linking source-specific IDs
- [ ] Refactor `dim_players` — merge cross-source records using the mapping

### Phase 13 — pgvector Player Embeddings (PLAN 14.3)

- [ ] Design feature vector from `fct_player_stats` per-90 metrics
- [ ] Populate `fct_player_embeddings` — currently 0 rows, table and synced table already provisioned
- [ ] Depends on Phase 12 (entity resolution) for unified player identity

### Phase 14 — Player Similarity Streamlit Page (PLAN 14.6)

- [ ] **Player Similarity** — pgvector nearest-neighbor search (`player_search.py`, depends on Phase 13)
- [ ] ~~**Pitch Control** — built in Phase 7~~

### Phase 15 — DEFCON-Inspired Defensive Valuation (PLAN 14.10)

Quantify individual defensive contributions inspired by the DEFCON framework (Kim et al., 2025). See research notes in PLAN.md §14.10.

- [ ] **License status**: DEFCON repo ([`hyunsungkim-ds/defcon`](https://github.com/hyunsungkim-ds/defcon)) has **no LICENSE file** — all rights reserved. Cannot copy code. Implementation must be from paper equations (publicly available) and standard open-source libraries.
- [ ] Build Expected Possession Value (EPV) decomposition from VAEP scores (Phase 9) + pitch control (Phase 11)
- [ ] Implement credit assignment: Intercept, Disturb, Deter, Concede (4 defensive contribution types)
- [ ] **DEFCON-lite (tabular)**: gradient-boosted tree model using VAEP features + tracking-derived spatial features, without GNN
- [ ] **Full DEFCON (GNN)**: Graph Attention Network for action selection/success prediction (requires 500+ matches with tracking — may need commercial data)
- [ ] Depends on: VAEP (Phase 9), additional tracking data (Phase 10), pitch control (Phase 11)

## Technical Debt

- [ ] **Update synced_tables Terraform module for Autoscaling API** — When the Databricks Terraform provider adds `database_project` and `branch` fields to `databricks_database_synced_database_table`, remove the UI+import workaround. Update `main.tf` to pass project/branch instead of `database_instance_name`, remove `lifecycle { ignore_changes = all }`, and retire `scripts/import_synced_tables.sh`. Track: [provider changelog](https://registry.terraform.io/providers/databricks/databricks/latest/docs). Until then, any new synced table (e.g., Phase 6+ gold tables) must be created via Databricks UI and imported.

## Future Work (unscheduled)

- [ ] **Voronoi area persistence** — pre-compute `voronoi_area` column in `fct_tracking_frames` via dbt (currently NULL, computed client-side in Streamlit for visualization only)
- [ ] **Pitch Control animation** — frame-by-frame playback via `st.empty()` loop or JS component; Phase 7 delivers single-frame static views only
- [ ] **Event overlay on Pitch Control** — render event markers (passes, shots) on pitch control view; requires syncing `stg_metrica__events` to Lakebase or creating a joined gold table
- [ ] **Respo.Vision 3D pose tracking** — 3D skeletal data from broadcast video (user pursuing via network); complements Metrica 2D with skeletal keypoints and body orientation
- [ ] **Wyscout match metadata** — deferred (event data ingested, match details not yet in Figshare dataset)
- [ ] **Full GNN DEFCON** — requires 500+ matches with synchronized tracking + event data; current public data (~20 matches) is insufficient for GNN training. May require commercial data (StatsBomb 360 + tracking, Second Spectrum, etc.)

## Infrastructure Notes

Infrastructure IDs are environment-specific. Set these as environment variables
rather than hardcoding in scripts. See `terraform output` for current values.

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
