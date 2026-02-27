# Security Audit Report — (Right! Luxury!) Lakehouse

**Audit date:** 2026-02-27
**Skill version:** `mad-skills:security-audit` v1.5.0
**Mode:** Audit (existing codebase)
**Auditor:** Claude Opus 4.6

---

## Executive Summary

- **Total findings:** 30
- **Critical:** 0 | **High:** 1 | **Medium:** 10 | **Low:** 15 | **Info:** 4
- **Resolved:** 16 (7 initial + 9 hardening round)
- **Remaining:** 14

The codebase has a strong security foundation with zero critical vulnerabilities. The single High finding (no secret scanning) is a preventive control gap. Medium findings are defense-in-depth hardening items — no exploitable attack paths were identified in the current deployment.

---

## Resolved This Session

| ID | Severity | Issue | Resolution |
|----|----------|-------|------------|
| R-1 | High | Terraform `count` depends on unknown SP `application_id` — plan fails | Fixed: added `enable_ingestion_sp_grants` bool variable |
| R-2 | High | Deploying user lacks `servicePrincipal.user` role — job `run_as` fails | Fixed: added `databricks_access_control_rule_set` resource |
| R-3 | High | No secret scanning in pre-commit or CI (H-1) | Fixed: added `detect-secrets` v1.5.0 hook + `.secrets.baseline` |
| R-4 | Medium | `unsafe_allow_html=True` with DB values (M-1) | Fixed: replaced with `st.header` + explicit `int()` casts |
| R-5 | Medium | Unhandled `psycopg2.Error` leaks traceback to browser (M-2) | Fixed: try/except with sanitized `RuntimeError` |
| R-6 | Medium | No `statement_timeout` on PG connection (M-3) | Fixed: `options="-c statement_timeout=30000"` |
| R-7 | Medium | Auth failures not logged in `_refresh_token()` (M-5) | Fixed: `except Exception` with `logger.exception` |
| R-8 | Medium | JWT `sub` claim lacks UUID validation (M-4) | Fixed: `uuid.UUID()` assertion in `_extract_jwt_subject()` |
| R-9 | Medium | Ingestion SP has `WRITE_VOLUME` on libs (M-8) | Fixed: removed `WRITE_VOLUME`, kept `READ_VOLUME` only |
| R-10 | Medium | Hardcoded infra IDs in deploy.sh/TODO.md (M-10) | Fixed: env var `DATABRICKS_JOB_ID`, redacted IDs from docs |
| R-11 | Low | `WHERE {where}` f-string SQL fragile (L-1) | Fixed: added safety constraint comments in code |
| R-12 | Low | `SELECT *` in match_summary.py (L-2) | Fixed: replaced with explicit 19-column list |
| R-13 | Low | No connection pooling (L-6) | Fixed: `ThreadedConnectionPool` with 55-min recycle |
| R-14 | Low | PG grants not in IaC (L-7) | Fixed: versioned SQL script `scripts/lakebase_grants.sql` |
| R-15 | Low | No timeout/retries on ingestion tasks (L-11) | Fixed: `timeout_seconds` + `max_retries=1` on all 3 tasks |
| R-16 | Low | dbt grants too broad (L-15) | Fixed: configurable `var('grant_select_to')` with default |

---

## Outstanding Findings — by Severity

### High

| ID | Phase | Area | CWE | File(s) | Description | Status |
|----|-------|------|-----|---------|-------------|--------|
| ~~H-1~~ | 8c | CI/CD | — | `.pre-commit-config.yaml` | ~~No secret scanning in pre-commit or CI.~~ | **Resolved** (R-3) |

**Fix:** Add to `.pre-commit-config.yaml`:
```yaml
- repo: https://github.com/Yelp/detect-secrets
  rev: v1.5.0
  hooks:
    - id: detect-secrets
      args: ['--baseline', '.secrets.baseline']
```

---

### Medium

| ID | Phase | Area | CWE | File:Line | Description | Status |
|----|-------|------|-----|-----------|-------------|--------|
| ~~M-1~~ | 4 | Streamlit | CWE-79 | `pages/match_summary.py:54` | ~~`unsafe_allow_html=True` with DB-sourced values.~~ | **Resolved** (R-4) |
| ~~M-2~~ | 6 | Streamlit | CWE-209 | `db.py:129-144` | ~~Unhandled `psycopg2.Error` leaks traceback to browser.~~ | **Resolved** (R-5) |
| ~~M-3~~ | 6 | Streamlit | CWE-770 | `db.py:98-106` | ~~No `statement_timeout` on PG connection.~~ | **Resolved** (R-6) |
| ~~M-4~~ | 7 | Auth | CWE-347 | `db.py:29-35` | ~~`_extract_jwt_subject()` lacks format validation — no guard that `sub` is a UUID before use as PG username.~~ | **Resolved** (R-8) |
| ~~M-5~~ | 11 | Monitoring | CWE-778 | `db.py:71-78` | ~~Auth failures propagate as unlogged exceptions.~~ | **Resolved** (R-7) |
| M-6 | 3a | Terraform | CWE-250 | `variables.tf:27-31` | Long-lived PAT as primary Terraform authenticator. Full workspace-admin scope, no expiry enforcement. | New |
| M-7 | 3b | Terraform | CWE-668 | `modules/workspace/` | No `databricks_ip_access_list` — workspace API reachable from any IP with a valid PAT. | New |
| ~~M-8~~ | 3d | Terraform | CWE-829 | `modules/catalog/main.tf:87-94` | ~~Ingestion SP had `WRITE_VOLUME` on `libs` — could overwrite its own wheel.~~ | **Resolved** (R-9) |
| M-9 | 5 | Web | CWE-116 | `.streamlit/config.toml` | No Content-Security-Policy or X-Frame-Options. Depends on Databricks Apps proxy injecting headers (unverified). | New |
| ~~M-10~~ | 9 | Secrets | CWE-200 | `deploy.sh`, `TODO.md` | ~~Infrastructure IDs hardcoded in deploy.sh and TODO.md.~~ Moved to env vars (`DATABRICKS_JOB_ID`) and redacted from docs. `app.yaml` retains Lakebase host (required by Databricks Apps manifest). | **Resolved** (R-10) |

---

### Low

| ID | Phase | Area | CWE | File:Line | Description | Status |
|----|-------|------|-----|-----------|-------------|--------|
| ~~L-1~~ | 0 | Streamlit | CWE-89 | `filters.py`, `shot_map.py` | ~~`WHERE {where}` f-string SQL — architecturally fragile.~~ Documented safety constraints in code comments. | **Resolved** (R-11) |
| ~~L-2~~ | 4 | Streamlit | CWE-213 | `match_summary.py:22` | ~~`SELECT *` exposes all current and future columns.~~ Replaced with explicit column list. | **Resolved** (R-12) |
| L-3 | 6 | Streamlit | CWE-20 | All `_load_*` functions | No explicit type assertion on `competition_id`/`team_id` before query. Mitigated by Streamlit widget type enforcement + `%s` params. | New |
| L-4 | 7 | Auth | CWE-362 | `db.py:25-26` | Module-level `_token_cache` dict is not thread-safe. Race on concurrent refresh is low-impact (extra token fetch). | New |
| L-5 | 7 | Auth | CWE-316 | `db.py:82-84` | OAuth token stored in plain memory, not zeroed on eviction. Python limitation; token is short-lived (60 min). | New |
| ~~L-6~~ | 4 | Streamlit | — | `db.py` | ~~Connection pooling deferred — new TCP connection per query.~~ Implemented `ThreadedConnectionPool` with 55-min recycle. | **Resolved** (R-13) |
| ~~L-7~~ | 4 | Terraform | — | `scripts/lakebase_grants.sql` | ~~PG grants applied manually, not in IaC.~~ Codified in versioned SQL script with `ALTER DEFAULT PRIVILEGES`. | **Resolved** (R-14) |
| L-8 | 3a | Terraform | CWE-732 | `modules/catalog/main.tf:78-85` | Ingestion SP has `MODIFY` on entire bronze schema — broader than needed (per-table grants preferred). | New |
| L-9 | 3a | Terraform | CWE-269 | `modules/service_principals/main.tf:19-26` | SP role grant hardcoded to single deploying user. Non-transferable without re-apply. | New |
| L-10 | 3c | Terraform | CWE-311 | `backend.tf:15` | S3 state encryption uses default AWS KMS key — no CMK, no rotation policy, no access logging. | New |
| ~~L-11~~ | 3d | Terraform | CWE-400 | `modules/workflows/main.tf` | ~~No `timeout_seconds` or `max_retries` on ingestion tasks.~~ Added timeout_seconds (3600/1800) and max_retries=1 to all tasks. | **Resolved** (R-15) |
| L-12 | 8b | CI/CD | CWE-1357 | `.github/workflows/dbt-ci.yml:24` | `uv sync` without `--frozen` — dbt CI can silently update dependencies. | New |
| L-13 | 8c | CI/CD | CWE-532 | `.github/workflows/terraform-plan.yml:52-67` | Terraform plan output posted as PR comment — may expose non-sensitive resource attributes. | New |
| L-14 | 11 | Monitoring | CWE-778 | `db.py:46-54` | REST credential fallback does not log HTTP 4xx auth failures before raising. | New |
| ~~L-15~~ | 6 | dbt | CWE-732 | `dbt_project.yml:32-42` | ~~`+grants: select: ['account users']` — overly broad.~~ Refactored to `var('grant_select_to')` with configurable principal. Default remains `account users` for dev. | **Resolved** (R-16) |

---

### Info

| ID | Phase | Area | Description |
|----|-------|------|-------------|
| I-1 | 10 | Data | No PII in data stores — all sources are public sports statistics of professional athletes. |
| I-2 | 8d | CI/CD | No SBOM generation pipeline (`cyclonedx-bom`). Recommended for production incident response. |
| I-3 | 11 | Monitoring | No centralized SIEM/log aggregation. Logs flow to Databricks built-in capture. Acceptable for dev. |
| I-4 | 11 | Monitoring | Referenced runbooks (`docs/runbooks/`) do not exist in repo. |

---

## Prioritized Action Plan

### ~~Immediate (before next release)~~ — ALL RESOLVED

1. ~~**H-1** — Add `detect-secrets` to pre-commit.~~ (R-3)
2. ~~**M-2** — Wrap `execute_query()` in try/except.~~ (R-5)
3. ~~**M-3** — Add `statement_timeout=30000`.~~ (R-6)
4. ~~**M-1** — Replace `unsafe_allow_html=True`.~~ (R-4)
5. ~~**M-5** — Log auth failures.~~ (R-7)

### ~~Next sprint~~ — ALL RESOLVED

6. ~~**M-4** — Add UUID format assertion on JWT `sub` claim.~~ (R-8)
7. ~~**M-8** — Remove `WRITE_VOLUME` from ingestion SP on `libs` volume.~~ (R-9)
8. ~~**M-10** — Move hardcoded infrastructure IDs to env vars in `deploy.sh`.~~ (R-10)
9. **M-6** — Plan migration from PAT to OAuth M2M for Terraform provider. Enforce PAT TTL < 90 days.
10. **M-7** — Add `databricks_ip_access_list` resource to restrict workspace API access.

### Backlog

11. **M-9** — Verify Databricks Apps proxy injects CSP/X-Frame-Options headers; engage platform team if not.
12. ~~**L-1** — Document `WHERE {where}` pattern constraints in code comments.~~ (R-11)
13. ~~**L-2** — Replace `SELECT *` with explicit column list in `match_summary.py`.~~ (R-12)
14. ~~**L-6** — Implement `psycopg2.pool` with 55-min recycle.~~ (R-13)
15. ~~**L-7** — Codify Lakebase PG grants in versioned SQL script.~~ (R-14)
16. ~~**L-11** — Add `timeout_seconds` and `max_retries` to ingestion tasks.~~ (R-15)
17. ~~**L-15** — Tighten dbt grants to configurable principal.~~ (R-16)

---

## What Passed (no action needed)

### Phase 0: Code Patterns — 20/24 patterns clean

- No `eval()`, `exec()`, `pickle.loads()`, `os.system()`, `subprocess(shell=True)`
- No `verify=False`, `CERT_NONE`, `DEBUG=True`
- No `dangerouslySetInnerHTML`, `document.write()`, `.innerHTML =`
- No hardcoded passwords, API keys, AWS keys, or private keys
- No `cidr_blocks = ["0.0.0.0/0"]`, `encrypted = false`, `publicly_accessible = true`

### Phase 1: Security Surface — Well-Defined

- Clear entry points: Streamlit UI, CLI ingestion, Terraform IaC
- All auth via Databricks runtime (no embedded credentials)
- HTTPS-only enforcement with SSL verification
- Explicit timeouts and retry-with-backoff on all HTTP calls

### Phase 3: Infrastructure — Strong Foundation

- Terraform state encrypted in S3 with native locking
- `databricks_token` marked `sensitive = true`
- Separate least-privilege SPs: ingestion (bronze-write) and app (gold-read)
- App restricted to `CAN_USE` on SQL warehouse via resources block

### Phase 7: Auth — Correct Model

- OAuth M2M with short-lived JWT (60 min, refreshed at 55 min)
- `sslmode=verify-full` on all PG connections
- XSRF protection enabled in Streamlit config
- CORS disabled

### Phase 8: Supply Chain — Strong

- All GitHub Actions pinned to full SHA hashes
- Minimal `permissions:` blocks on all workflows
- `pip-audit` in CI — zero known vulnerabilities
- `uv.lock` committed with SHA-256 content hashes
- `uv sync --frozen` in Python CI
- Dependabot configured for pip, GitHub Actions, and Terraform

### Phase 9: Secrets — Clean

- Zero hardcoded credentials in any source file
- `.gitignore` covers `.env`, `*.tfvars`, `*.pem`, `*.key`, `credentials.json`
- CI secrets injected via `${{ secrets.* }}`
- AWS OIDC role assumption (no long-lived AWS keys)

### Phase 10: Data — Low Risk

- All data is public open-source soccer statistics
- No PII beyond professional athlete names (publicly known)
- `_ingested_at` audit column on every bronze write

---

## Tier Coverage

| Phase | Standard | Enterprise |
|-------|----------|------------|
| Phase 0: Code Patterns | 24/24 checked, 2 findings | SAST: not configured |
| Phase 3: Infrastructure | 9 checks, 6 findings | WAF: not applicable (Databricks Apps) |
| Phase 5: Web Headers | 12 checks, 2 findings (platform-dependent) | CDN headers: Databricks-managed |
| Phase 6: API Security | 10 checks, 5 findings | API gateway WAF: not applicable |
| Phase 7: Auth & Session | 14 checks, 5 findings, 1 pass | MFA: Databricks workspace SSO |
| Phase 8: Supply Chain | 4 sub-phases, 3 findings | Artifact signing: not configured |
| Phase 9: Secrets | 12 patterns scanned, 1 finding | Vault: not configured |
| Phase 10: Data | Classification complete | DLP: not applicable (public data) |
| Phase 11: Monitoring | 6 checks, 5 findings | SIEM: not configured |

### Security Posture Rating

- **Standard tier**: 22/28 checks passed (**79% coverage**)
- **Enterprise tier**: 1/9 controls configured (Dependabot only — **11% coverage**)
- **Overall**: **Adequate** — strong code-level security, hardening gaps in monitoring and infrastructure controls
- **Ready for deployment**: **Yes** for dev environment with public data. **No** for production with sensitive data (address H-1, M-2, M-3, M-5 first).
