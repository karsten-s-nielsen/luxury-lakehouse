# ADR-014: Two HF tokens for the Ops Space — a lake-only data token and a confined job-control token

## Status

Accepted (2026-11-03). Decided after the Settings schedule read returned `403 Forbidden ... missing permissions:
job.read` on the production Space. Investigated against the HF docs and the live tokens; the owner chose the two-token
design (personal namespace, no org). Uncommitted, pending the owner's commit approval. Relates to ADR-011 (the authz
model) and spec §29.9 (the Ops Space is lake-only and never reads restricted-match data).

## Context

- The Ops Space Settings page manages the HF scheduled job: it reads its status and reschedules, suspends, resumes
  and triggers it (`HfJobsSchedule`). Those calls need an HF token with job access to the job's namespace.
- The scheduled job lives under the **personal** namespace `karstenskyt`. HF constraints, verified this cycle:
  - HF OAuth exposes no jobs scope (`docs/hub/oauth` scope list), so the signed-in operator's token cannot manage the
    job.
  - A fine-grained token's Jobs permission is **organisation-only**; the user-level and per-repo sections offer no
    Jobs toggle. A fine-grained token scoped to `spaces/karstenskyt/ops` with Write contents returns 403 on
    `inspect_scheduled_job` (proven live).
  - So the only token that can manage a personal-namespace job is a **classic `read`/`write`-role token**.
- A classic role token also reads every repo the owner can read, including the `luxury-lakehouse/restricted` bucket.
  The Ops UI must never be able to read restricted-match data (§9, §29.9). The single lake-only `HF_TOKEN` (a
  fine-grained token, Write on `luxury-lakehouse/lake` only) cannot manage the job, and a role token that can manage
  the job cannot stay lake-only. The two requirements cannot be met by one token on a personal namespace.

## Decision

1. **Two Space secrets, separation of duties.**
   - `HF_TOKEN` — the **data** credential. Unchanged: fine-grained, Write on `luxury-lakehouse/lake` only. Every
     bucket read, page render and config write uses it. The lake-only guarantee (and its two `hf_space` denial tests)
     is preserved on this token.
   - `LAKEHOUSE_HF_JOBS_TOKEN` — the **job-control** credential. A classic `write`-role token. The Settings schedule
     reads it for status, reschedule, suspend, resume and trigger.
2. **The job-control token is confined to the schedule seam.** `HfJobsSchedule.from_env` is its only reader; it is
   never handed to `HfStorage` or any bucket read. `from_env` reads `LAKEHOUSE_HF_JOBS_TOKEN`, falling back to
   `HF_TOKEN` when unset (the org case, where a fine-grained org-Jobs token is the single token).
3. **The confinement is test-enforced.** `test_jobs_token_confinement.py` fails if any module other than the schedule
   seam reads the env (an AST scan for the constant or the literal in an env lookup), and asserts the storage builder
   reads `HF_TOKEN`, never the jobs token. `check_hf_token.py jobs --job-id <id>` verifies the token can read the job.

## Consequences

- The hard lake-only guarantee for all data and page reads is unchanged (same data token, same denial tests). The new
  surface — "the job-control token could read restricted if misused" — is sealed structurally by the one-reader seam
  and the confinement test, not by token scope (HF cannot scope a personal-namespace job token narrowly).
- Setup gains one Space secret. Solo users create one classic write token; org users may omit the secret and let the
  schedule fall back to a fine-grained org-Jobs `HF_TOKEN`. Both paths work without code change.
- The deploy runbook carries the authoritative Space configuration table and the two-token scopes
  (`docs/ops/deploy.md`); the Space README points to it; `docs/ops/troubleshooting.md` documents the 403.
- A missing or wrong `LAKEHOUSE_HF_JOBS_TOKEN` degrades gracefully: the Settings page shows a "Schedule status
  unavailable" note and still renders the config section (ADR-015 is unrelated; the resilience is in `settings.refresh`).
