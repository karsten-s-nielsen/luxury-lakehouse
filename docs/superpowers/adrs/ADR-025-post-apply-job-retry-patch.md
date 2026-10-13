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

## Amendment (2026-10-13) — the omitempty is SERVER-side; the verify cannot echo-check 0

The original decision assumed the REST script could set a verifiable `max_retries = 0` where the TF provider's `omitempty` dropped it. When `scripts/patch_job_retries._verify_post_apply` first ran live (post-#611, which fixed the earlier `.get("max_retries", 0)` read bug that had no-op'd the reset), it RAISED: every compute task read back `max_retries = None`. Investigation (read-only, 2026-10-13):

- **The `omitempty` is also SERVER-side.** The Databricks Jobs 2.1 GET response omits `max_retries = 0`, so a stored 0 and an absent field are indistinguishable on read-back. A GET-based `actual == 0` assertion can never pass for a compute task.
- **Absent does retry** (confirmed empirically): GATE-0 run `941047587677694` had `compute_tracking_marts_iteration` runs at `attempt_number = 1` with `max_retries = None` — corroborating this ADR's 2026-05-15 instance.

**Consequences of the amendment:**
- **Write/read guard split.** "We sent 0" is checkable only on the WRITE side (`_patch_tasks` + its unit tests). The READ side (GET) cannot confirm it. `_verify_post_apply` therefore asserts only GET-returnable facts: non-zero intents (ingestion = 1) round-trip, no task shows an unintended POSITIVE retry count, and the round-trip guard ran on ≥ 1 non-zero task (non-vacuity); it does NOT assert an intended-0 task reads back 0.
- **A green terraform-apply means "the verify is honest + the non-zero intents applied" — NOT "compute retries are suppressed."** Whether a reset-sent 0 actually suppresses the retry is settled by a one-time effectiveness probe (a throwaway `for_each`-wrapped always-failing task, `max_retries = 0`, read the iteration `attempt_number`); separate post-merge approval. If the probe shows the 0 is not honored, this ADR is **superseded-in-part** (the REST script cannot fix what the API will not persist; the retry-storm backstop is the ADR-087 circuit-breaker, which covers storms but not the single-retry waste this ADR targets).
- Spec/plan: `docs/superpowers/specs/2026-10-13-patch-job-retries-verify-omitempty-design.md` (+ plan).

## Related

- **Specs:** `docs/superpowers/specs/2026-05-15-metrica-tracking-fixes-design.md`
- **External references:** Go `encoding/json` `omitempty` specification — zero-value fields are omitted; Databricks provider source uses `json:"max_retries,omitempty"` on `int` field
