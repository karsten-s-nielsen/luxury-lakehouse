# PAT Rotation Runbook

**Status:** Deprecated — superseded by OAuth M2M (2026-04-09)
**Date:** 2026-04-02

## Context

The Taipy dashboard originally authenticated to Lakebase via a Databricks Personal Access Token (PAT). This was replaced by OAuth M2M credentials in Cycle 5 Phase 3.

## Current State (as of 2026-04-09)

**OAuth M2M is deployed.** Both Spaces (`luxury-lakehouse/soccer-analytics-app` and `luxury-lakehouse/staging`) authenticate via `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` from the `luxury-lakehouse-hf-app-v2-dev` service principal. The `DATABRICKS_TOKEN` PAT secret has been removed from both Spaces.

**OAuth M2M secret lifetime:** 730 days (expires ~2028-04-09). To regenerate: Databricks → Settings → Service Principals → `luxury-lakehouse-hf-app-v2-dev` → Secrets → Generate.

## Legacy Rotation Procedure (archived)

No longer needed. Retained for historical reference only.

1. ~~Generate new PAT in Databricks workspace settings~~
2. ~~Update `DATABRICKS_TOKEN` secret in both HF Spaces~~
3. ~~Rebuild both Spaces~~
4. ~~Verify dashboard loads~~
