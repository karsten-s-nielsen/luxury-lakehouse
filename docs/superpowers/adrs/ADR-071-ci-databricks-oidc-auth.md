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

---

## Amendment — 2026-07-27: never materialise the bearer

**Status:** Accepted. Supersedes this ADR's "mint once per job, export as `DATABRICKS_TOKEN`" mechanism.

### What went wrong

From 2026-07-22 every scheduled workflow that ran longer than ~5 minutes failed. Lakebase
Maintenance died on its last step, Synced-Table Heal E2E mid-poll; Data Quality CI stayed green
only because it finishes in ~4m50s. The error — `403 Forbidden / Invalid Token` on
`GET /api/2.0/postgres/synced_tables/...` — looked like a Lakebase permission gap. It was not:
the CI SP already held `CAN_MANAGE` on the Lakebase project, and the *same* API succeeded three
times earlier in the *same* job.

Measured in run `30259317817`, one token, one API, only elapsed time varying:

| Since mint | Call | Result |
|---|---|---|
| +0:30 | `heal_synced_tables` → `postgres.get_synced_table` ×12 | OK |
| +1:47 | `fix_event_log_ownership.py` → same API | OK |
| **+3:59** | `grant_synced_table_permissions.py` → 41 grants | OK |
| **+5:13** | `test_synced_tables_online` → same API | `403 Invalid Token` |

### Root cause

The mint step was never necessary. Configured for `github-oidc`, the SDK re-mints on **every
request**: `_base_client.py:84` sets `session.auth = self._authenticate`, which calls
`_header_factory()` per prepared request (`:105-110`); for `github-oidc` that factory is
`refreshed_headers()` (`credentials_provider.py:494-497`), which builds a fresh
`ClientCredentials` from a live GitHub id-token fetch every call (`:473-491` →
`oidc_token_supplier.py:16-32`). A reused `WorkspaceClient` **cannot** go stale.

Snapshotting `config.authenticate()` into `$GITHUB_ENV` discarded that live credential and
replaced it with a dead string, and the job-level `DATABRICKS_AUTH_TYPE: pat` added by #489 then
forced every downstream client to prefer the dead string over the live provider. #489 correctly
fixed the oauth-vs-pat conflict; the residue was this. Its live validation covered the first two
steps of `lakebase-grants` — both inside the token's lifetime — which is why it shipped.

### Decision

**No workflow may materialise a Databricks bearer, by any transport.** `DATABRICKS_AUTH_TYPE:
github-oidc` goes at job level; every `WorkspaceClient()` holds the live credential. The
`pat` pins are removed — an explicit `github-oidc` disambiguates the #489 pair on its own, as
`dbt-live-ci.yml:42-51` and `terraform-plan.yml:24` have been demonstrating in production
throughout.

Enforced by `test_no_workflow_materialises_a_databricks_token`, which bans assignment of
`DATABRICKS_TOKEN` from anything but a hardcoded placeholder — `$GITHUB_ENV`, `$GITHUB_OUTPUT` +
`steps.*.outputs.*`, and `secrets.*` alike. Guarding one transport is not enough: the first draft
banned `$GITHUB_ENV` only and missed `terraform-apply.yml`, which routed the same bearer through
a step output. `DATABRICKS_TOKEN` is the choke point — a bearer moved by any means must land
there to be used.

Hardcoded placeholders remain legal and necessary: `dbt_project/profiles.yml` calls
`env_var('DATABRICKS_TOKEN')` with **no default** (`:10`, `:19`, `:28`, `:53`), so `dbt deps`
needs a non-empty value to render at parse time. Those never authenticate anything.

### Consequences

- Live-test skip guards moved from a bare `DATABRICKS_TOKEN` check to
  `ingestion.databricks_auth.has_databricks_auth()` (11 files). A token check would have silently
  skipped the entire live suite once CI stopped materialising one — a real signal becoming a
  vacuous green. The predicate includes host, and returns `False` on fork PRs (no id-token).
- Raw-`requests` call sites resolve per use via `auth_headers()` instead of reading the env var.
  `patch_job_retries.py` duplicates that helper deliberately: `terraform-apply.yml` runs it with a
  bare `python` and a pip-installed SDK, without the project wheel on `sys.path`.
- `CachedGitHubOidcStrategy` caches the *exchanged* bearer, because the stock strategy rebuilds
  `ClientCredentials` per call so `Refreshable`'s cache never engages — every API call would
  otherwise pay a GitHub fetch plus a token exchange. The cache sits at the exchanged-token layer,
  never at `ClientCredentials`: that class's `refresh()` re-posts its original `endpoint_params`,
  which embed the now-expired subject JWT, so caching there works until it abruptly does not.
- ~~**The exact token lifetime is still unmeasured**~~ — **measured 2026-07-27: 299 s** (was bounded
  to (3:59, 5:13]). The freshness regression in `synced-table-heal-e2e.yml` logs it and asserts
  `t1.expiry > t0.expiry`, so it cannot pass on a frozen bearer the way an elapsed-time-only test
  would.

### Tier 2 call volume — measured 2026-07-28

The saving was asserted qualitatively above and left unquantified. Measured at the
`credentials_provider.github_oidc` seam with a counting stub (one "exchange" = one GitHub
id-token fetch **plus** one token-exchange POST = **2 HTTP round-trips**):

| Scenario | Stock exchanges | Cached exchanges | Reduction |
|---|---|---|---|
| 60 calls inside one TTL (synthetic ceiling) | 60 | **1** | 60× |
| `wait_until_online` 900 s @15 s (heal-e2e) | 60 | ~4 | ~15× |
| `poll_run` 1800 s @15 s (`trigger_dbt_job`) | 120 | ~7 | ~17× |

Only real CI poll loops are listed. `_POLL_INTERVAL_S = 15` / `_MAX_POLL_ATTEMPTS = 120` are read
from `scripts/trigger_dbt_job.py:43-44`. Long *Databricks-side* jobs (e.g. the 28800 s AC drain) do
**not** belong here at any scale — they run on-cluster under ambient runtime auth, where this
strategy is inert.

Stock is 1:1 with API calls **by construction**, not by measurement: `github_oidc` (line 505) is a
thin wrapper over `_oidc_credentials_provider` (line 439), whose `refreshed_headers()` calls
`token_source_for(audience).token()` on a freshly built `TokenSource` every time
(`credentials_provider.py:473`, `:494-497`) — so the `Refreshable` cache it inherits is unreachable.

The effective cache window is **259 s** — the 299 s lifetime minus `Refreshable`'s 40 s pre-expiry
skew (`oauth.py:115-116`). The 60× figure is a synthetic ceiling (no wall-clock elapses);
**~15–17× is the honest number** for the long-poll jobs this was built for, because a real job
outlives the token and must re-exchange.

No new test was added: `test_cache_engages_within_the_freshness_window` (two calls → 1 exchange) and
`test_refresh_refetches_the_github_subject_jwt` (inside the skew window → re-exchange, never a stale
bearer) already lock both behaviours. Only the magnitude was missing, and it belongs here.
