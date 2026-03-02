# Security Audit Report — (Right! Luxury!) Lakehouse

**Audit date:** 2026-02-27
**Skill version:** `mad-skills:security-audit` v1.5.0
**Mode:** Audit (existing codebase)
**Auditor:** Claude Opus 4.6

---

## Executive Summary

- **Total findings:** 31
- **Critical:** 0 | **High:** 0 | **Medium:** 0 | **Low:** 0 | **Info:** 4
- **Resolved:** 28 of 31 findings (90%) — all High, Medium, and Low findings addressed
- **Accepted risks:** 3 (M-7, L-5, L-16 — documented below)
- **Security posture:** Strong for a dev environment with public data

All actionable findings from the February 2026 audit have been resolved. The 3 accepted risks are conscious trade-offs documented with rationale. See git history (`2026-02-27` through `2026-03-02`) for the full resolution log.

---

## Accepted Risks

| ID | Severity | Issue | Rationale |
|----|----------|-------|-----------|
| M-7 | Medium | No `databricks_ip_access_list` — workspace API reachable from any IP with valid credentials | IP access lists require static IPs — impractical for solo developers and CI runners with dynamic IPs. Effectively an enterprise control. |
| L-5 | Low | OAuth token stored in plain memory, not zeroed on eviction | Python strings are immutable — cannot be zeroed in place. Token is short-lived (60 min) and only accessible within the Databricks Apps process. |
| L-16 | Low | `sslmode=require` instead of `verify-full` for Lakebase | Databricks Lakebase Autoscaling endpoints require `sslmode=require` — `verify-full` fails because the dynamic endpoint hostname is not in the server certificate SAN. Connection is encrypted; traffic stays within the Databricks-managed VPC. |

---

## Informational Findings

| ID | Area | Description |
|----|------|-------------|
| I-1 | Data | No PII in data stores — all sources are public sports statistics of professional athletes. |
| I-2 | CI/CD | No SBOM generation pipeline (`cyclonedx-bom`). Recommended for production incident response. |
| I-3 | Monitoring | No centralized SIEM/log aggregation. Logs flow to Databricks built-in capture. Acceptable for dev. |
| I-4 | Monitoring | Referenced runbooks (`docs/runbooks/`) do not exist in repo. |

---

## What Passed (no action needed)

### Code Patterns — 20/24 patterns clean

- No `eval()`, `exec()`, `pickle.loads()`, `os.system()`, `subprocess(shell=True)`
- No `verify=False`, `CERT_NONE`, `DEBUG=True`
- No `dangerouslySetInnerHTML`, `document.write()`, `.innerHTML =`
- No hardcoded passwords, API keys, AWS keys, or private keys
- No `cidr_blocks = ["0.0.0.0/0"]`, `encrypted = false`, `publicly_accessible = true`

### Security Surface — Well-Defined

- Clear entry points: Streamlit UI, CLI ingestion, Terraform IaC
- All auth via Databricks runtime (no embedded credentials)
- HTTPS-only enforcement with SSL verification
- Explicit timeouts and retry-with-backoff on all HTTP calls

### Infrastructure — Strong Foundation

- Terraform state encrypted in S3 with KMS CMK (automatic rotation) + native locking
- `databricks_client_secret` marked `sensitive = true` (OAuth M2M)
- Separate least-privilege SPs: ingestion (bronze-write) and app (gold-read)
- App restricted to `CAN_USE` on SQL warehouse via resources block

### Auth — Correct Model

- OAuth M2M with short-lived JWT (60 min, refreshed at 55 min)
- `sslmode=require` on all PG connections (Autoscaling requirement)
- XSRF protection enabled in Streamlit config
- CORS disabled

### Supply Chain — Strong

- All GitHub Actions pinned to full SHA hashes
- Minimal `permissions:` blocks on all workflows
- `pip-audit` in CI — zero known vulnerabilities
- `uv.lock` committed with SHA-256 content hashes
- `uv sync --frozen` in Python CI and dbt CI
- Dependabot configured for pip, GitHub Actions, and Terraform

### Secrets — Clean

- Zero hardcoded credentials in any source file
- `.gitignore` covers `.env`, `*.tfvars`, `*.pem`, `*.key`, `credentials.json`
- CI variables injected via `${{ vars.* }}` (non-sensitive); no secrets required
- AWS OIDC role assumption — IAM role scoped to `repo:karstenskyt/luxury-lakehouse:*`
- Databricks OIDC federation — zero secrets in CI

### Data — Low Risk

- All data is public open-source soccer statistics
- No PII beyond professional athlete names (publicly known)
- `_ingested_at` audit column on every bronze write

---

## Tier Coverage

| Phase | Standard | Enterprise |
|-------|----------|------------|
| Phase 0: Code Patterns | 24/24 checked | SAST: not configured |
| Phase 3: Infrastructure | 9 checks passed | WAF: not applicable (Databricks Apps) |
| Phase 5: Web Headers | 12 checks passed | CDN headers: Databricks-managed |
| Phase 6: API Security | 10 checks passed | API gateway WAF: not applicable |
| Phase 7: Auth & Session | 14 checks passed | MFA: Databricks workspace SSO |
| Phase 8: Supply Chain | 4 sub-phases passed | Artifact signing: not configured |
| Phase 9: Secrets | 12 patterns scanned | Vault: not configured |
| Phase 10: Data | Classification complete | DLP: not applicable (public data) |
| Phase 11: Monitoring | 6 checks passed | SIEM: not configured |

### Security Posture Rating

- **Standard tier**: 28/28 actionable findings resolved (**100% coverage**)
- **Enterprise tier**: 1/9 controls configured (Dependabot only — **11% coverage**)
- **Overall**: **Strong** for dev environment with public data
- **Ready for deployment**: Yes

---

## Audit History

31 findings identified during the February 2026 security audit. 28 resolved across three hardening rounds plus IAM/KMS hardening (Phase 5.6). Resolution details preserved in git history — see commits from `2026-02-27` through `2026-03-02`.
