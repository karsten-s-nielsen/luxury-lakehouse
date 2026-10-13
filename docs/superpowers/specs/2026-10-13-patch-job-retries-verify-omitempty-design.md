# patch_job_retries verify — Databricks GET omits max_retries=0 (design)

**Status:** rev1 — DRAFT for review (spec + plan). Follow-up to ADR-090 / #611 (merged `b45cb103`): the post-merge `terraform-apply.yml` "Patch job retries" step FAILED and main CI is RED.
**Amends:** ADR-025 (post-apply job retry patch).
**Author investigation:** `D:\Development\_reviews\` + the 2026-10-13 read-only Databricks query (below).

## 1. Problem

#611's Fix B corrected `patch_job_retries.py`'s READ side (`.get("max_retries", 0)` → `.get("max_retries")`, so absent reads as `None` and a `jobs/reset` is actually SENT for compute tasks) and added `_verify_post_apply` (GET the job back, assert each task's `max_retries == intended`). Running live for the first time (post-merge terraform-apply), the verify RAISED: every compute task read back `max_retries = None` though the reset sent `0`.

**Investigation (read-only, OAUTH, 2026-10-13):**
- Live mega-job `302697362345215` after the reset: all compute for_each inners (`compute_tracking_marts_iteration`, …) `max_retries = None`; `ingest_idsse_iteration` / `ingest_gradientsports_iteration` `max_retries = 1` (non-zero round-trips + verifies).
- GATE-0 run `941047587677694`: 6 `compute_tracking_marts_iteration` runs reached `attempt_number = 1` (retried once) with `max_retries = None` → **absent DOES retry** (confirms ADR-025's premise + its 2026-05-15 instance; discard any "absent → never retry" reading).

**Root cause:** Databricks omits `max_retries = 0` on the Jobs-2.1 GET response (Go `omitempty`, SERVER-side — not only the TF provider that ADR-025 blamed). So a stored `0` and an absent field are **indistinguishable via GET**, and absent behaves as "retry". Therefore a GET-based assertion of `max_retries == 0` **can never pass** — `_verify_post_apply` is structurally broken for every compute (intended-0) task, and terraform-apply will fail identically on every re-run.

**Two consequences, separated:**
- **(A) main CI is RED** on an assertion the Databricks API cannot satisfy. Urgent; fixable independently of (B).
- **(B) It is UNVERIFIED whether the REST reset's `0` actually suppresses the retry.** ADR-025's foundational assumption ("the REST script sets 0 where TF's omitempty drops it") was never exercised before #611 (the `.get(...,0)` bug no-op'd the reset). GET cannot confirm it. The ADR-087 circuit-breaker does **NOT** substitute — it trips only on STORMS (consecutive ≥ 6 or rate > 0.5 after ≥ 20), not a single deterministic failure retried once, which is exactly ADR-025's target waste (2 documented instances: run `830163656900015` 2026-05-15, GATE-0 2026-10-11).

## 2. Goals / Non-goals

**Goals**
- G1 (A): make `_verify_post_apply` assert only facts the Databricks GET can return, so terraform-apply goes green — WITHOUT weakening the check into a guard that cannot fail.
- G2 (B): resolve whether the REST reset's `max_retries = 0` actually suppresses retries, by EVIDENCE not assumption (verify-the-mechanism), and branch the retry policy on the answer.
- G3: amend ADR-025 to record the server-side GET-omitempty + the resolved effectiveness.

**Non-goals**
- N1: changing the ADR-087 circuit-breaker (shipped; it is the STORM backstop, explicitly NOT the single-retry fix).
- N2: a new always-on empirical gate in CI (the probe is one-time).
- N3: the Stage-2 windowing fix (shipped + live; unaffected).
- N4: removing `patch_job_retries` / ADR-025 wholesale — only amended per the G2 evidence.

## 3. Design

### 3.1 Honest verify (G1 — unblocks CI)
`_verify_post_apply` asserts the GET-returnable contract:
1. **Every NON-ZERO intent round-trips:** each task whose classifier intent ≠ 0 (ingestion = 1) must GET-read back that exact value. This catches a dead or partial reset (the real failure mode) — non-vacuous (ingestion = 1 is returnable + currently passes).
2. **No task carries an UNINTENDED POSITIVE retry count:** any task that GET-reads a `max_retries ≥ 1` greater than its intent is a drift toward retries → raise. (A compute task showing an explicit `2` is a real defect.)
3. **Intended-0 tasks are NOT asserted to read back `0`** — the API cannot return it (documented inline). Reading `None` is expected + accepted.
4. **Non-vacuity self-guard (B-PJR-SPEC-01 / HFW-SPEC-03):** the round-trip assertion (1) only proves anything if at least one non-zero-intent task exists. `_verify_post_apply` counts the non-zero-intent tasks it round-trip-checked and RAISES if that count is 0 — else a job with no ingestion tasks (or a classifier regression that maps every task to 0) would pass vacuously, proving nothing. The mega-job always has ingestion tasks (intended=1), so this holds in production; the guard catches the degenerate/regressed shape.

`_patch_tasks` is UNCHANGED (still `.get("max_retries")` absent-as-None; still sets 0 for compute / 1 for ingestion in the reset payload — best-effort WRITE; see §3.3 on the write/read guard split).

### 3.2 Effectiveness probe (G2 — approval-gated, one-time)
A standalone throwaway Databricks job with an always-failing task (`raise SystemExit(1)`) **wrapped in a `for_each_task`** (matching the production waste shape — the retrying unit is a for_each INNER, `compute_tracking_marts_iteration`, not a top-level task; PJR-SPEC-01), `max_retries = 0` set on the inner via the SAME `jobs/reset` path, run ONCE; read the iteration run's `attempt_number`. (A top-level-task result would not strictly prove the for_each-inner case — `max_retries` is per-task-spec and the reset path is identical, so suppression is very likely shape-independent, but the probe matches the real shape rather than assuming it.)
- `attempt_number` stays `0` → the REST reset's `0` DOES suppress retries (stored, GET-omitted). ADR-025's mechanism is valid; §3.1's write-side-intent + non-zero-GET verify is sufficient; delete the throwaway job.
- `attempt_number` reaches `1` → the REST `0` is as futile as TF's; ADR-025's approach is superseded. Record it; evaluate alternatives (a Jobs-API retry field that round-trips, run-level vs task-level, or accept the bounded single-retry cost) as a separate decision — do NOT silently leave a non-working patch asserting success.

This is a job run → **separate explicit owner approval**, only after §3.1 ships + post-merge CI green ([no-jobs-in-ci] / [wait-post-merge-ci]). It is NOT required to green CI (§3.1 does that); it resolves whether the policy is effective.

### 3.3 ADR-025 amendment (G3) — and the write/read guard split (B-PJR-SPEC-02)
Add a Consequences note documenting that the compute `max_retries = 0` is guarded on TWO sides that do NOT meet:
- **WRITE side (what we sent):** `_patch_tasks` sets the compute inner to `0` in the reset payload — unit-tested (`test_absent_compute_max_retries_is_patched_to_explicit_zero` etc.). This is the only place "we sent 0" is checkable.
- **READ side (what the API returns):** the Jobs GET omits `max_retries = 0` (SERVER-side Go omitempty — not only the TF provider ADR-025 blamed), so `_verify_post_apply` CANNOT echo-check the 0; it verifies the non-zero intents round-trip + no unintended positive retries instead.

**Explicit acceptance required (R2):** a green terraform-apply after §3.1 means **"the verify is honest / the reset applied the non-zero intents"**, NOT **"compute retries are suppressed."** The latter is unproven until the §3.2 probe (the write side sends 0, but whether Databricks honors a reset-sent 0 is exactly what the probe settles). Effectiveness is [pending / confirmed by] §3.2; if the probe shows 0 is not honored, mark ADR-025 superseded-in-part.

## 4. Testing

- `_verify_post_apply` does NOT raise when a compute (intended-0) task reads back `None` (the omitempty state) — **the #611 `test_verify_post_apply_raises_on_absent_compute_retries` INVERTS** (absent-compute is unverifiable → must pass). Replace it.
- `_verify_post_apply` DOES raise when an ingestion (intended-1) task reads back `None`/`0` (non-zero intent didn't round-trip — the real failure mode; non-vacuous, watch it fail first).
- `_verify_post_apply` DOES raise when any task GET-reads a positive `max_retries` above its intent (unintended retries; guard-of-the-guard).
- `_verify_post_apply` PASSES on the real post-reset shape (compute = `None`, ingestion = `1`).
- `_patch_tasks` absent-handling + the `_INGESTION_TASK_KEYS` ↔ TF parity tests UNCHANGED (still green).
- All seven local gates + `validate_workflow_cards` + a real full `uv run pytest` (CI extras). `patch_job_retries.py` is a bare `_ALLOWED_BARE` script (NOT a wheel package) → **no wheel bump**; confirm `bump_wheel --check` stays clean (no version change).

## 5. Risks

- **R1 — §3.1 weakens the guard into vacuity.** Mitigation: the non-zero round-trip + unintended-positive assertions are non-vacuous (watch each fail first); the only dropped assertion is the one the API provably cannot satisfy.
- **R2 — shipping §3.1 without §3.2 leaves the set-0 effectiveness unproven.** Accepted + surfaced: §3.1 is correct regardless (a GET-0 assert is simply wrong); §3.2 resolves effectiveness under its own approval. The OOM-fix removes the dominant compute-failure source meanwhile; the residual is a bounded single retry on an unrelated deterministic compute failure.
- **R3 — the probe mutates live infra** (creates/deletes a throwaway job + one run). Mitigation: a dedicated throwaway job (never the mega-job), deleted after; approval-gated.

## 6. Rollout
One feature branch (`fix/patch-job-retries-verify-omitempty`), one commit (§3.1 + tests + ADR-025 amendment + this spec/plan), human-approval gate before commit. §3.2 probe is a separate post-merge approval. No wheel bump.
