# Luxury Lakehouse — TODO

Quick-reference action items. Full details in [PLAN.md](PLAN.md).

**Last updated**: 2026-02-25

---

## Immediate

- [x] ~~**Rotate Databricks token**~~ — Done
- [x] ~~**Rename local folder**~~ — Done
- [x] ~~**Verify Terraform in PATH**~~ — v1.14.6
- [x] ~~**Verify AWS access**~~ — `devops-agent` role working (account 454762693631)
- [x] ~~**Configure Terraform state backend**~~ — S3 bucket `karstenskyt-terraform-state`, native S3 locking
- [x] ~~**Run `terraform init`**~~ — Successful
- [x] ~~**Run `terraform plan`**~~ — 16 resources, 0 errors
- [ ] **Initial git commit + push** to `karstenskyt/luxury-lakehouse`

## Phase 0 — Remaining Items

| Item | Status | Blocker |
|------|--------|---------|
| Databricks workspace | **Done** — `https://dbc-48322be9-16be.cloud.databricks.com` | — |
| Databricks token | Done (needs rotation) | — |
| Unity Catalog metastore | **Done** — auto-created (`metastore_aws_us_east_1`) | — |
| Terraform state backend | **Done** — `karstenskyt-terraform-state` + native S3 locking | — |
| IAM role extension for Databricks | Not needed for Phase 0-1 | Future: Phase 2 if S3 access needed |
| Terraform init + plan | **Done** — 16 resources, 0 errors | — |
| Databricks CLI install | Not started | Can use `pip install databricks-cli` via uv |
| Initial push to GitHub | Not started | Ready now |

## Phase 1 — Serverless Infrastructure (IaC)

- [ ] `terraform apply` — workspace module (Unity Catalog + soccer_analytics catalog)
- [ ] `terraform apply` — catalog module (bronze, silver, gold schemas)
- [ ] `terraform apply` — sql_warehouse module (Serverless SQL, 2X-Small, auto-stop 10min)
- [ ] `terraform apply` — lakebase module (PostgreSQL 17, scale-to-zero)
- [ ] `terraform apply` — workflows module (ingestion job, paused in dev)
- [ ] `terraform apply` — synced_tables module (Gold > Lakebase sync)
- [ ] `terraform apply` — app module (Streamlit Databricks App)
- [ ] Verify all resources in Databricks UI
- [ ] Run `/final-review` + regenerate C4 diagrams

## Phase 2 — Data Ingestion

- [ ] Implement `src/ingestion/statsbomb.py` — fetch from StatsBomb API, write to Bronze
- [ ] Implement `src/ingestion/metrica.py` — fetch tracking CSV, write to Bronze
- [ ] Implement `src/ingestion/wyscout.py` — fetch event JSON, write to Bronze
- [ ] Test ingestion locally against workspace
- [ ] Verify Bronze tables populated in Unity Catalog
- [ ] Run `/final-review`

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
| GitHub repo | `karstenskyt/luxury-lakehouse` (private) |
| Monthly budget | Under $100 |
| Terraform state bucket | `karstenskyt-terraform-state` (S3 native locking) |
| Start Claude Code with | `AWS_PROFILE=devops-agent claude` |
