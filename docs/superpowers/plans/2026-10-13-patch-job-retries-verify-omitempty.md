# patch_job_retries verify fix — Implementation Plan

> **For the implementer:** execute INLINE in the main session ([no-subagents-for-implementation]; read-only research subagents OK). TDD. ONE commit for the whole coherent change, fully tested, on explicit approval ([no-micro-commits] / [no-commit-no-approval]) — the per-task "verify green" steps are checkpoints, not commit points.

**Goal:** Make `patch_job_retries._verify_post_apply` assert only what the Databricks Jobs GET can return (so terraform-apply goes green) without turning the guard vacuous, amend ADR-025, and schedule the one-time effectiveness probe as a separate post-merge gate.

**Spec:** `docs/superpowers/specs/2026-10-13-patch-job-retries-verify-omitempty-design.md` (read alongside).

## Global constraints
- Python 3.10; line 120; ruff E,W,F,I,N,UP,B,S,BLE,RUF; pyright basic over the CI target; `# noqa: S101` for any assert in non-test code (repo pattern, `utils.py:597`).
- `patch_job_retries.py` is a bare `_ALLOWED_BARE` script (NOT a wheel package) → **no wheel bump**; `bump_wheel --check` must stay clean (no version change).
- No job run in this cycle (the §3.2 probe is a separate post-merge approval).

## File structure
- `scripts/patch_job_retries.py` (MODIFY) — `_verify_post_apply` logic only; `_patch_tasks` unchanged.
- `src/tests/test_job_retry_policy.py` (MODIFY) — invert/replace the absent-compute verify test; add the non-vacuous ingestion + unintended-positive guards.
- `docs/superpowers/adrs/ADR-025-post-apply-job-retry-patch.md` (MODIFY) — Consequences amendment.
- Spec + plan (this cycle, bundled with the commit).

---

## Task 1: `_verify_post_apply` — assert only GET-returnable facts

**Files:** Modify `scripts/patch_job_retries.py`; Test `src/tests/test_job_retry_policy.py`.

- [ ] **Step 1 — write the failing tests** (fake `_get_job`, no Databricks):
  - `test_verify_post_apply_passes_on_real_post_reset_shape`: job with compute task `max_retries` ABSENT + ingestion task `max_retries=1` → `_verify_post_apply` does NOT raise. (This is the live shape; currently it RAISES.)
  - `test_verify_post_apply_raises_when_nonzero_intent_missing`: an ingestion (intended-1) task reads back `None` → RAISES (reset didn't land the 1 — the real failure mode).
  - `test_verify_post_apply_raises_on_unintended_positive_retries`: a compute (intended-0) task reads back `max_retries=2` → RAISES (drift toward retries; guard-of-the-guard).
  - `test_verify_post_apply_raises_when_no_nonzero_task_checked` (B-PJR-SPEC-01 / HFW-SPEC-03): an all-compute job (no ingestion/intended-1 task) → RAISES ("round-trip guard verified 0 non-zero tasks — vacuous") so the honest verify cannot pass while proving nothing.
  - REPLACE `test_verify_post_apply_raises_on_absent_compute_retries` (from #611) — absent-compute now must NOT raise (the API cannot return 0; asserting it is a guard that can't pass). Keep `test_verify_post_apply_passes_on_explicit_zero` only if an explicit 0 is representable in the fake; otherwise drop (GET never returns 0 live).
- [ ] **Step 2 — run → the 3 new tests FAIL** (current verify raises on absent compute; doesn't yet check unintended-positive). `uv run pytest src/tests/test_job_retry_policy.py -v`.
- [ ] **Step 3 — implement.** In `_verify_post_apply`, per task (+ for_each inner): let `actual = task.get("max_retries")`, `intended = _classify_task(key)`. Mismatch rule:
  - if `intended != 0` and `actual != intended` → mismatch (non-zero intent must round-trip);
  - if `intended == 0` and `actual is not None` and `int(actual) > 0` → mismatch (unintended positive retries);
  - else OK (intended-0 reading `None`/absent is accepted — the Databricks GET omits `max_retries=0`, server-side omitempty; a stored 0 is indistinguishable from absent on GET). Comment this inline citing the spec. Keep the fail-loud `sys.exit(1)` + the diagnostic log.
  - **Non-vacuity self-guard (B-PJR-SPEC-01):** count the non-zero-intent tasks round-trip-checked; if that count is 0, RAISE ("verified 0 non-zero tasks — the round-trip guard is vacuous; classifier or job shape changed"). This keeps the honest verify unable to pass while proving nothing.
- [ ] **Step 4 — run → all PASS.** Watch the two new raise-tests fail FIRST (step 2) to prove non-vacuity.

## Task 2: ADR-025 amendment

**Files:** Modify `docs/superpowers/adrs/ADR-025-post-apply-job-retry-patch.md`.

- [ ] Add a Consequences note (per spec §3.3): (a) the `max_retries=0` omitempty is SERVER-side (the Jobs 2.1 GET omits it too, not only the TF provider), so the post-apply verify cannot echo-check 0 — it asserts the non-zero intents + absence of unintended positive retries instead; (b) the **write/read guard split (B-PJR-SPEC-02):** "we sent 0" is guarded only on the WRITE side (`_patch_tasks` unit tests), the READ side cannot confirm it; (c) **green terraform-apply = "verify honest", NOT "compute retries suppressed"** — the latter is unproven until the §3.2 probe (separate post-merge approval); if the probe shows 0 is not honored, mark ADR-025 superseded-in-part. No `--no-verify`; keep the parity test.

## Task 3: gates + commit

- [ ] Run ALL seven local gates (ruff check, ruff format --check [`src/ scripts/`], lint-imports, bump_wheel --check [unchanged version], pip_audit_ignores --check, pyright [CI target], pytest) + `validate_workflow_cards` + a real full `uv run pytest` (CI extras: `uv sync --frozen --extra analytics --extra embeddings --extra mlflow --extra jax`). Fix to green.
- [ ] **Commit gate (STOP — explicit approval).** Show the diff / file list. Do NOT `git commit`/`push`/`gh pr create`/`merge`. One commit (`scripts/patch_job_retries.py` + `test_job_retry_policy.py` + `ADR-025` + spec + plan). Message ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Post-merge (separate approvals, not this branch)
- After main CI green (terraform-apply now passes): the **§3.2 effectiveness probe** — a throwaway always-failing task **wrapped in a `for_each_task`** (match the production for_each-inner shape, PJR-SPEC-01), `max_retries=0` set on the inner via `jobs/reset`, run once, read the iteration `attempt_number`; delete the job. Resolves whether REST-0 suppresses retries. Its own explicit approval (a job run). Record the result in ADR-025.
- The ADR-090 **8-unit targeted re-run** is INDEPENDENT of this cycle (OOM-fixed iterations succeed → no retries fire) — its own approval.

## Self-review (against the spec)
- §3.1 honest verify → Task 1 (non-vacuous: ingestion-round-trip + unintended-positive fail first). §3.2 probe → post-merge gate. §3.3 ADR → Task 2. Testing §4 → Task 1 tests + Task 3 gates. No wheel bump (bare script). Single commit (Task 3).
