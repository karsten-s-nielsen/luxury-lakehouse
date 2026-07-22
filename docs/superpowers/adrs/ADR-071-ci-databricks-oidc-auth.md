# ADR-071: CI authenticates to Databricks via GitHub OIDC, not a stored PAT

| Field | Value |
|---|---|
| **Date** | 2026-07-21 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The Databricks workspace retired Personal Access Tokens on 2026-07-21 (the same
"off tokens" move HuggingFace made). The stored GitHub Actions secret
`DATABRICKS_TOKEN` (an admin PAT) became invalid immediately: every workflow that
passed it to `pytest` or a script started failing with
`databricks.sdk.errors.platform.PermissionDenied: Invalid access token
(auth_type=pat)`. Confirmed live the same day — 4 live tests in `python-ci` and the
nightly `synced-table-heal-e2e` run failed on it.

The repo already had a secretless auth path for GitHub Actions: `terraform-apply.yml`
/ `terraform-plan.yml` / `dbt-live-ci.yml` mint a short-lived workspace token via
GitHub OIDC against the `terraform_ci` service principal. That SP's federation policy
(`terraform/modules/service_principals/main.tf`) is scoped to
`subject_claim = "repository"`, so *any* workflow in this repo can reuse it — no new
service principal, federation policy, or Terraform apply required.

## Decision

All GitHub Actions workflows mint a short-lived Databricks token via GitHub OIDC
(`WorkspaceClient(auth_type='github-oidc').config.authenticate()`, reusing the
repo-wide `terraform_ci` SP) and export it as `DATABRICKS_TOKEN`. The stored
`secrets.DATABRICKS_TOKEN` PAT is retired and no workflow references it.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Rotate to a new PAT | one-line change | workspace disabled PAT creation ("do not create a new PAT"); reintroduces a 90-day rotation liability | not possible + wrong direction |
| B. M2M OAuth (client_id + client_secret as GH secrets) | works headless | reintroduces a long-lived stored secret to rotate | OIDC is secretless and already proven in-repo |
| C. GitHub OIDC, reuse `terraform_ci` SP (chosen) | secretless, ephemeral tokens, zero new infra, matches existing terraform-apply pattern | none material for CI (fork PRs can't mint — but they get no secrets today either and are auto-closed) | — |

## Consequences

### Positive

- CI no longer depends on any stored Databricks credential; nothing to rotate or expire.
- Same federation SP and pattern as the existing Terraform/dbt-live workflows — one mental model.
- The dead `secrets.DATABRICKS_TOKEN` repo secret is deletable.

### Negative

- Each migrated workflow carries a ~15-line OIDC-mint step (minted token exported to
  `DATABRICKS_TOKEN` because several consumers — `WorkspaceClient()` unified auth,
  `run_lakebase_grants.py`, `test_staging_rowcount_vs_bronze.py` — read the literal
  env var / pass `access_token=`, so setting only `DATABRICKS_AUTH_TYPE=github-oidc`
  is insufficient).
- Fork PRs cannot mint a token (no `id-token: write`, no vars); the mint step fails
  rather than skipping. Acceptable: fork PRs get no secrets today either and are
  auto-closed by `close-fork-prs.yml`.
- **Lakebase prerequisite:** the OIDC token authenticates fine to Databricks *workspace*
  APIs (jobs, SQL, DDL), but Lakebase Postgres is identity-based — the `terraform_ci` SP
  must be a provisioned PG role, and a `databricks_superuser` member, for
  `lakebase-grants.yml` / `connect_as_superuser()` to run GRANT. The retired admin PAT
  had this implicitly (its human owner is a superuser). One-time fix, codified
  declaratively in `scripts/setup_lakebase_roles.py`
  (`DesiredRole(..., superuser=True)` → `create_role(membership_roles=[DATABRICKS_SUPERUSER])`),
  run once by an existing superuser. Missing-role symptom:
  `psycopg2 ... password authentication failed for user '<sp-app-id>'`.

### Neutral

- `dbt-ci.yml` / `dbt-live-ci.yml` were parse-only (never opened a live connection),
  so the dead PAT never broke them; they were normalized to a constant non-empty
  placeholder to drop the secret reference.
- **Out of scope** (not GitHub Actions, different auth mechanism, not currently
  failing): off-Actions runtime code that still reads `os.environ["DATABRICKS_TOKEN"]`
  — local dev scripts (switch to `auth_type="databricks-cli"` / the OAuth CLI profile)
  and the HF-Jobs publishers/trainers (`query_databricks_sql` and its clones, which run
  headless off-Databricks and would need M2M client-secret, not OIDC). Tracked as a
  follow-up.

## Related

- **Issues / PRs:** #449 (this migration); the databricks-sdk 0.121.0 bump (#447) surfaced the break — its CI run was the first to fail on the dead PAT.
- **Scripts:** `scripts/setup_lakebase_roles.py` (provisions the CI SP as a `databricks_superuser` member).
- **External references:** silly-kicks handoff 2026-07-21 (workspace moved off PATs → OAuth).
- **ADRs:** builds on the un-numbered GitHub-OIDC SP federation established for `terraform-apply.yml` / `dbt-live-ci.yml`.
