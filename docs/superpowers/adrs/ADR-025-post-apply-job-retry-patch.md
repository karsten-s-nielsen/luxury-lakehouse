# ADR-025: Post-Apply Job Retry Patch for Terraform omitempty Bug

| Field | Value |
|---|---|
| **Date** | 2026-05-18 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt |

## Context

The Databricks Terraform provider (Go SDK) uses Go's `omitempty` JSON tag on the `max_retries` integer field in the task struct. Go's zero-value for `int` is `0`, so `max_retries = 0` is indistinguishable from "field not set" and is silently omitted from the API payload. The platform then applies its default of 1 retry.

This means every task in the `soccer-analytics-ingestion-dev` job (35 tasks) gets 1 retry regardless of what Terraform declares. For ingestion tasks that call external APIs (StatsBomb, HF Hub, pining-for-the-data), 1 retry is beneficial — transient network errors, rate limits, and provider outages are recoverable. For compute tasks (tracking context, SPADL/VAEP, pitch control, formations, etc.), retry wastes a full task timeout worth of DBU before producing the same deterministic error.

The SkillCorner tracking context failure in production run 830163656900015 (2026-05-15) demonstrated the cost: the failed iteration was retried once (30 min timeout × 2 = 60 min wasted DBU) before producing the same `AnalysisException: Column 'team' does not exist`.

## Decision

A post-apply Python script (`scripts/patch_job_retries.py`) reads the job via the Databricks REST API, classifies each task as ingestion (max_retries=1) or compute (max_retries=0), and writes the corrected settings back. The script runs automatically after every `terraform apply` in CI (`.github/workflows/terraform-apply.yml`). Terraform declares the *intended* retry count (1 for ingestion, 0 for compute) even though the provider silently drops the 0 values.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Wait for provider fix (`*int` pointer type) | Zero maintenance | Uncontrolled timeline; issue unfiled upstream; every apply until fix ships wastes DBU on failed retries | Not actionable on our timeline |
| B. `null_resource` + `local-exec` per task | Pure Terraform; no external script | 35 `null_resource` blocks; fragile `jq` piping; Windows/Linux portability; `local-exec` not idempotent | Maintenance burden disproportionate to problem |
| C. Post-apply REST API script (chosen) | Idempotent; single file; testable; CI-integrated; `--dry-run` support | Extra CI step (~15s); depends on REST API stability | — |

## Consequences

### Positive

- Compute tasks (tracking context, formations, pitch control, etc.) no longer waste a full timeout on deterministic retry.
- Ingestion tasks explicitly get 1 retry (codified intent, not accidental platform default).
- Parity test (`test_job_retry_policy.py`) catches drift between the TF file and the script's task classification at PR-CI time.

### Negative

- Extra CI step adds ~15s to `terraform-apply` workflow.
- If the Databricks REST API response shape changes (unlikely for Jobs API 2.1), the script needs updating.
- Two sources of truth for retry policy: TF declares intent, script enforces it. The parity test mitigates drift.

### Neutral

- When the provider eventually ships the `*int` pointer fix, the script becomes a no-op (all tasks already at intended values) and can be removed. No rush to remove — it's idempotent.

## Related

- **Specs:** `docs/superpowers/specs/2026-05-15-metrica-tracking-fixes-design.md`
- **External references:** Go `encoding/json` `omitempty` specification — zero-value fields are omitted; Databricks provider source uses `json:"max_retries,omitempty"` on `int` field
