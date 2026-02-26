# Luxury Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md).

**Last updated**: 2026-02-26

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
- [ ] `terraform apply` — synced_tables module — Deferred to Phase 4 (requires gold-layer tables from Phase 3)
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

- [ ] Configure `dbt_project/profiles.yml` with Databricks connection
- [ ] Run `dbt deps` to install packages
- [ ] Implement staging models (Silver): flatten nested JSON, parse coordinates
- [ ] Implement intermediate models: unified shots and passes
- [ ] Implement mart models (Gold): fct_shots, fct_passes, fct_player_stats, fct_match_summary
- [ ] Implement tracking models: fct_tracking_frames, fct_player_embeddings
- [ ] Implement dimensions: dim_players, dim_teams, dim_competitions
- [ ] Run `dbt build` — all models + tests pass
- [ ] Run `/final-review`

## Phase 4 — Zero-ETL Synchronization

- [ ] Configure Synced Tables (Gold Delta > Lakebase)
- [ ] Verify tables queryable via PostgreSQL wire protocol
- [ ] Test sub-10ms query latency
- [ ] Run `/final-review`

## Phase 5 — Streamlit Application

- [ ] Implement `src/streamlit_app/db.py` — OAuth M2M connection to Lakebase
- [ ] Implement pages: shots, passes, player_radar, match_summary, pitch_control, player_search
- [ ] Implement components: filters, charts (mplsoccer wrappers)
- [ ] Deploy as Databricks App
- [ ] End-to-end smoke test
- [ ] Run `/final-review` + final C4 diagram update

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
