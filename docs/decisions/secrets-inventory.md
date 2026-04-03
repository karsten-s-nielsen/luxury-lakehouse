# Secrets Inventory

**Status:** Accepted
**Date:** 2026-04-02

## Context

The platform uses 5 distinct secret stores across Databricks, HF Spaces, HF Jobs, GitHub Actions, and developer environments. No centralized inventory existed.

## Decision

Document all secret stores, their contents, rotation policies, and owners.

## Inventory

| Store | Secrets | Rotation | Owner |
|-------|---------|----------|-------|
| Databricks ambient OAuth | Workspace access, Unity Catalog | Automatic (platform-managed) | Platform |
| HF Space secrets | DATABRICKS_TOKEN, DATABRICKS_HOST, LAKEBASE_HOST, LAKEBASE_ENDPOINT_NAME, LAKEBASE_DATABASE, GOLD_SCHEMA | Manual — PAT expires ~2026-06-14 (M1) | @karsten |
| HF Jobs secrets | HF_TOKEN | Via HF account settings | @karsten |
| GitHub Actions OIDC | AWS role assumption for Terraform state | Automatic (OIDC federation) | CI |
| Developer env vars | Local .env files (gitignored) | Per-developer | Individual |

## Consequences

PAT rotation (M1) is the only manual rotation required. Completing OAuth M2M for Lakebase (M2) would eliminate the PAT dependency entirely.
