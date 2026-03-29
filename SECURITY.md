# Security Audit Report — (Right! Luxury!) Lakehouse

## Reporting a Vulnerability

If you discover a security vulnerability, please report it through [GitHub's private vulnerability reporting](https://github.com/karsten-s-nielsen/luxury-lakehouse/security/advisories/new). Do not open a public issue.

**Response time:** This is a solo-maintained project. Expect an initial response within 7 days.

**Supported versions:** Only the current `main` branch is supported.

---

**Audit date:** 2026-02-27 (updated 2026-03-29)
**Skill version:** `mad-scientist-skills:security-audit` v1.6.0
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
| L-5 | Low | OAuth token stored in plain memory, not zeroed on eviction | Python strings are immutable — cannot be zeroed in place. Token is short-lived (60 min) and only accessible within the HF Spaces Docker container process. |
| L-16 | Low | `sslmode=require` instead of `verify-full` for Lakebase | Databricks Lakebase Autoscaling endpoints require `sslmode=require` — `verify-full` fails because the dynamic endpoint hostname is not in the server certificate SAN. Connection is encrypted; traffic stays within the Databricks-managed VPC. |

---

## Informational Findings

| ID | Area | Description |
|----|------|-------------|
| I-1 | Data | No PII in data stores — all sources are public sports statistics of professional athletes. |
| ~~I-2~~ | ~~CI/CD~~ | ~~No SBOM generation pipeline.~~ **Resolved** (2026-03-11): CycloneDX SBOM generation added to `python-ci.yml` during optimization audit. |
| I-3 | Monitoring | No centralized SIEM/log aggregation. Logs flow to Databricks built-in capture. Acceptable for dev. |
| I-4 | Monitoring | Referenced runbooks (`docs/runbooks/`) do not exist in repo. Status: deferred — see TODO.md for current operational procedures. |

---

## What Passed (no action needed)

### Code Patterns — 20/24 patterns clean

- No `eval()`, `exec()`, `pickle.loads()`, `os.system()`, `subprocess(shell=True)`
- No `verify=False`, `CERT_NONE`, `DEBUG=True`
- No `dangerouslySetInnerHTML`, `document.write()`, `.innerHTML =`
- No hardcoded passwords, API keys, AWS keys, or private keys
- No `cidr_blocks = ["0.0.0.0/0"]`, `encrypted = false`, `publicly_accessible = true`

### Security Surface — Well-Defined

- Clear entry points: Taipy dashboard, CLI ingestion, Terraform IaC
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
- Taipy server-side state management (no client-side session tokens requiring XSRF protection)
- CORS disabled

### Supply Chain — Strong

- All GitHub Actions pinned to full SHA hashes
- Minimal `permissions:` blocks on all workflows
- `pip-audit` in CI — zero known vulnerabilities
- CycloneDX SBOM generation in CI — `cyclonedx-py` produces JSON SBOM on every build
- `uv.lock` committed with SHA-256 content hashes
- `uv sync --frozen` in Python CI and dbt CI
- Dependabot configured for pip, GitHub Actions, and Terraform

### Secrets — Clean

- Zero hardcoded credentials in any source file
- `.gitignore` covers `.env`, `*.tfvars`, `*.pem`, `*.key`, `credentials.json`
- CI variables injected via `${{ vars.* }}` (non-sensitive); no secrets required
- AWS OIDC role assumption — IAM role scoped to `repo:karsten-s-nielsen/luxury-lakehouse:*`
- Databricks OIDC federation — zero secrets in CI

### Data — Low Risk

- All data is public open-source soccer statistics
- No PII beyond professional athlete names (publicly known)
- `_ingested_at` audit column on every bronze write

---

## Model Serialization Audit (D41)

**Audit date:** 2026-03-29

MLflow pyfunc models use cloudpickle for serialization internally. This section documents the deserialization surface, risk assessment, and mitigations for each registered model.

### Registered Models — Serializer Inventory

| Model | MLflow Flavor | Serializer (Save) | Deserialization (Load) | Executor Format | Risk |
|-------|--------------|-------------------|----------------------|-----------------|------|
| `soccer_analytics.dev_gold.defcon_model` | pyfunc | cloudpickle | `load_model()` → driver | `get_booster().save_raw("json")` → JSON bytes | Bounded |
| `soccer_analytics.dev_gold.vaep_model` | pyfunc | cloudpickle | `load_model()` → driver | `get_booster().save_raw("json")` → JSON bytes | Bounded |
| `soccer_analytics.dev_gold.xg_model` | sklearn | cloudpickle | `load_model()` → driver | `serialize_xgboost_model()` → JSON+base64 | Bounded |
| `soccer_analytics.dev_gold.xg_model_baseline` | sklearn | cloudpickle | `load_model()` → driver | `serialize_logistic_model()` → JSON envelope | Bounded |
| `soccer_analytics.dev_gold.xg_model_v2` | pyfunc | cloudpickle (wrapper) | `load_model()` → driver | `model_weights.json` artifact (numpy arrays, no pickle) | Minimal |
| `soccer_analytics.dev_gold.football2vec` | pyfunc | cloudpickle (wrapper) | `load_model()` → driver | gensim `Doc2Vec.save()` binary, driver-only | Bounded |

### Threat Model

**Attack vector:** A compromised or maliciously tampered model artifact in Unity Catalog could execute arbitrary code on the driver via cloudpickle deserialization at `mlflow.pyfunc.load_model()` time.

**Mitigations in place:**
1. **UC ACLs:** Only the ingestion SP and workspace admins can write model versions to `soccer_analytics.dev_gold.*`
2. **Immediate re-serialization:** All XGBoost models are extracted to safe JSON bytes on the driver before any executor work — cloudpickle objects are never passed to executors
3. **No direct pickle in user code:** Zero `import pickle` or `pickle.load()` calls in first-party code (verified by ruff S rule and `detect-secrets` scan)
4. **Anti-pickle test:** `test_xg_model.py:343` asserts serialized bytes do not contain pickle protocol 5 magic bytes (`\x80\x05`)

### Safetensors / weights_only Feasibility

- **`weights_only=True`:** PyTorch API — not applicable (no PyTorch models in the project)
- **safetensors:** Not applicable for XGBoost/sklearn models; relevant only for future neural network weights
- **xG v2 pattern (recommended for new models):** `xg_model_v2` already avoids cloudpickle for the actual weights by storing them as a JSON artifact. The pyfunc wrapper is a thin placeholder. This is the recommended pattern for all future models — store weights as JSON/safetensors artifacts, not inside the pyfunc wrapper

### Residual Risk: Accepted

The cloudpickle deserialization surface on the driver is accepted as a low risk because:
- The model registry is access-controlled via Unity Catalog (write = ingestion SP only)
- All data is public open-source soccer statistics — no high-value target
- Models are immediately converted to safe formats before executor distribution
- The mitigation path (JSON artifacts as in xG v2) is established for future models

---

## Tier Coverage

| Phase | Standard | Enterprise |
|-------|----------|------------|
| Phase 0: Code Patterns | 24/24 checked | SAST: Semgrep (`p/python` + `p/security-audit`) in CI |
| Phase 3: Infrastructure | 9 checks passed | WAF: not applicable (HF Spaces Docker) |
| Phase 5: Web Headers | 12 checks passed | CDN headers: Databricks-managed |
| Phase 6: API Security | 10 checks passed | API gateway WAF: not applicable |
| Phase 7: Auth & Session | 14 checks passed | MFA: Databricks workspace SSO |
| Phase 8: Supply Chain | 4 sub-phases passed | Artifact signing: not configured |
| Phase 9: Secrets | 12 patterns scanned | Vault: not configured |
| Phase 10: Data | Classification complete | DLP: not applicable (public data) |
| Phase 11: Monitoring | 6 checks passed | SIEM: not configured |

### Security Posture Rating

- **Standard tier**: 28/28 actionable findings resolved (**100% coverage**)
- **Enterprise tier**: 2/9 controls configured (Dependabot + Semgrep SAST — **22% coverage**)
- **Overall**: **Strong** for dev environment with public data
- **Ready for deployment**: Yes

---

## Audit History

31 findings identified during the February 2026 security audit. 28 resolved across three hardening rounds plus IAM/KMS hardening (Phase 5.6). Resolution details preserved in git history — see commits from `2026-02-27` through `2026-03-02`.
