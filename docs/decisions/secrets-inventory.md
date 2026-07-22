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
| HF Space secrets | DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, DATABRICKS_HOST, LAKEBASE_HOST, LAKEBASE_ENDPOINT_NAME, LAKEBASE_DATABASE, GOLD_SCHEMA | OAuth M2M secret: 730-day lifetime (expires ~2028-04-09). Regenerate via Databricks Service Principals UI. | @karsten |
| HF Jobs secrets | HF_TOKEN | Via HF account settings | @karsten |
| GitHub Actions OIDC | AWS role assumption for Terraform state | Automatic (OIDC federation) | CI |
| Developer env vars | Local .env files (gitignored) | Per-developer | Individual |

## Consequences

No manual rotation required for the foreseeable future. OAuth M2M credentials (730-day lifetime) replaced the 90-day PAT as of Cycle 5 Phase 3 (2026-04-09). The `DATABRICKS_TOKEN` PAT secret has been removed from both Spaces.

## History

- **2026-04-02:** Initial inventory. PAT-based auth, M1 rotation planned.
- **2026-04-09:** M2 complete — OAuth M2M deployed to both Spaces (staging + production). PAT removed. M1 superseded.
- **2026-06-08:** Orphaned `DATABRICKS_TOKEN` PAT (comment "HF Spaces", created 2026-03-16, expiry 2026-06-14, last used 2026-04-09 — i.e. the OAuth M2M cutover, never touched since) deleted from Databricks following an expiry-warning email. It was superseded by OAuth M2M but never revoked. Token inventory is now OAuth-M2M-only for the Spaces; dashboards unaffected (service-principal OAuth, expires ~2028-04-09). Not renewed — an idle long-lived PAT on a personal account is a liability, not something to refresh.
- **2026-07-21:** Workspace retired PATs entirely; the CI GitHub secret `DATABRICKS_TOKEN` (admin PAT, separate from the Spaces PAT above) went invalid and broke every live CI test with `Invalid access token (auth_type=pat)`. All GitHub Actions workflows migrated to GitHub OIDC token minting via the repo-wide `terraform_ci` SP (see [ADR-071](../superpowers/adrs/ADR-071-ci-databricks-oidc-auth.md)); the `secrets.DATABRICKS_TOKEN` repo secret is now unreferenced and can be deleted. **Not yet migrated** (different mechanism, not currently failing): off-Actions runtime code that reads `os.environ["DATABRICKS_TOKEN"]` — local dev scripts (OAuth CLI profile) and the HF-Jobs publishers/trainers (would need M2M client-secret).
