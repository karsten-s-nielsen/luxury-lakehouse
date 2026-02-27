# (Right! Luxury!) Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md).

**Last updated**: 2026-02-28

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
- [ ] **Security:** Restrict Lakebase connections to Streamlit app service principal only (Phase 5)
- [ ] **Security:** Configure connection pooling with 55min recycle (Phase 5: `psycopg2.pool`)
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

## Phase 5 — Streamlit Application

- [ ] Implement `src/streamlit_app/db.py` — OAuth M2M connection to Lakebase
- [ ] **Security:** Use Databricks App auth — never deploy without authentication
- [ ] **Security:** Parameterized queries only — never concatenate user input into SQL
- [ ] Implement pages: shots, passes, player_radar, match_summary, pitch_control, player_search
- [ ] Implement components: filters, charts (mplsoccer wrappers)
- [ ] Deploy as Databricks App
- [ ] End-to-end smoke test
- [ ] Run `/final-review` + final C4 diagram update

## Future Data Sources

- [ ] **Respo.Vision 3D pose tracking** — 3D skeletal data from broadcast video (user pursuing via network)
- [ ] **Wyscout match metadata** — deferred (event data ingested, match details not yet in Figshare dataset)
- [ ] **StatsBomb 360 frames** — deferred (ingestion scaffolded but most competitions lack 360 data)

## Infrastructure Notes

| Resource | Value |
|----------|-------|
| AWS region | us-east-1 |
| AWS profile | `devops-agent` |
| Databricks workspace URL | `https://dbc-48322be9-16be.cloud.databricks.com` |
| Databricks tier | Premium (14-day free trial) |
| Unity Catalog metastore | `metastore_aws_us_east_1` (auto-created) |
| Unity Catalog | `soccer_analytics` (Default Storage) |
| SQL Warehouse ID | `6c3b36ca64d183fe` |
| Lakebase instance | `soccer-analytics-lakebase-dev` |
| Lakebase DNS (RW) | `instance-f9ffeb4b-6f4b-4fd6-9287-335d745ce173.database.cloud.databricks.com` |
| Ingestion job ID | `240279916175143` |
| Streamlit App URL | `https://soccer-analytics-dashboard-dev-7474660814094441.aws.databricksapps.com` |
| GitHub repo | `karstenskyt/luxury-lakehouse` (private) |
| Monthly budget | Under $100 |
| Terraform state bucket | `karstenskyt-terraform-state` (S3 native locking) |
| Start Claude Code with | `AWS_PROFILE=devops-agent claude` |
