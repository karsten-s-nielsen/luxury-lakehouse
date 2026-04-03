# PAT Rotation Runbook

**Status:** Accepted
**Date:** 2026-04-02

## Context

The Taipy dashboard authenticates to Lakebase via a Databricks Personal Access Token (PAT). This is a temporary measure — OAuth M2M is the target but is partially blocked by a Lakebase autoscaling limitation (TODO M2).

## Decision

Document the manual PAT rotation procedure until OAuth M2M is available.

## Rotation Procedure

1. Generate new PAT in Databricks workspace settings (Settings > Developer > Access tokens)
2. Update `DATABRICKS_TOKEN` secret in HF Space `luxury-lakehouse/soccer-analytics-app`
3. Update `DATABRICKS_TOKEN` secret in HF Space `luxury-lakehouse/staging`
4. Rebuild both Spaces to pick up the new token
5. Verify dashboard loads and queries succeed on both staging and production

## Timeline

- Current PAT expires: ~2026-06-14
- Rotation window: 1 week before expiry (by ~2026-06-07)
- Target: Eliminate PAT dependency via OAuth M2M (TODO M2)

## Consequences

Manual rotation is a known operational burden. The 55-minute token pool age in db.py means a new PAT takes effect within 55 minutes of Space rebuild without any code changes.
