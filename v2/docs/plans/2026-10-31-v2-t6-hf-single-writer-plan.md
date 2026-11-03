# v2 T6 HF Single-Writer Hardening Implementation Plan

> **For agentic workers:** owner override — execute natively in one session (superpowers:executing-plans); no
> subagents, no worktrees (branch `feat/v2-ingestion`), no per-task commits. Steps use checkbox (`- [ ]`) syntax.

**Status:** Round 3 (2026-10-31): Reviewer A APPROVE (round 2), Reviewer B APPROVE WITH CHANGES (round 2, applied);
reports `D:\Development\_reviews\2026-10-31-v2-t6-hf-single-writer-plan-r2-{A,B}.md` (round 3: both APPROVE). **Owner-approved 2026-10-31**, with `karstenskyt/jobs-test`
approved explicitly and T6 run in parallel with the rest of T3. Round 1: Reviewer A REQUEST CHANGES (1 BLOCKING, 1 SHOULD FIX, 2 CONSIDER),
Reviewer B REQUEST CHANGES (1 BLOCKING, 5 SHOULD FIX, 2 CONSIDER); reports
`D:\Development\_reviews\2026-10-31-v2-t6-hf-single-writer-plan-{A,B}.md`; disposition at the end.

**Goal:** Make the HF single-writer guarantee hold by construction: an HF Hub commit compare-and-swap ledger fences the
lease, every HF conditional write is single-part, and a `CURRENT` write is read back.

**Architecture:** A `LeaseLedger` seam in `lakehouse.storage` (only `storage` may import `huggingface_hub`) with one HF
implementation, `HubLedger`, over a private control repository per lake. `Lease.acquire` and `Lease.assert_held` use
the ledger when the storage provides one (HF with gateway credentials) and keep the file-system path otherwise. The
catalog keys, readers, the T3 policy and the sweep are unchanged. Every T6 HF test runs on a separate test image, and
production changes only at the cutover, after the A/B implementation review and the owner's explicit yes.

**Tech Stack:** Python 3.12, `huggingface_hub` 2.3.0 (`HfApi.create_commit(parent_commit=...)`, `repo_info`,
`hf_hub_download`), boto3 S3 gateway, pytest, pyright strict, ruff, import-linter.

**Spec:** `docs/specs/2026-10-31-v2-t6-hf-single-writer-design.md` (A and B APPROVE; owner decisions 2026-10-31:
option A, the session creates both control repositories, the D2 and D3 residuals accepted).

## Global Constraints

- Control repositories: `luxury-lakehouse/lake-control` (production), `luxury-lakehouse/lake-test-control` (test);
  private dataset repositories; one file `lease.json`. Job variable `LAKEHOUSE_CONTROL_REPO`.
- **Production is untouched until Task 11** (T6-PLAN A-01, B-01): `karstenskyt/jobs` (the production Job image), the
  production scheduled job and `lake-control` change only in Task 11, after the A/B implementation review (Task 10)
  and the owner's explicit yes. Every earlier HF run uses the test image `karstenskyt/jobs-test` (a private Docker
  Space, same Dockerfile and stage as `karstenskyt/jobs`), the test buckets and `lake-test-control`. Until Task 11,
  HF production keeps running the scheduled job on the T3 image (audit ledger T6).
- Only the Job's `HF_TOKEN` writes a control repository; the Space token and reader tokens have no access.
- `LEASE_TTL` 10 min, `TAKEOVER_GRACE` 2 min, `HEARTBEAT` 60 s (unchanged). A ledger generation without a bucket
  object is held until the ledger commit's server time + `LEASE_TTL` + `TAKEOVER_GRACE`; never free at once.
- HTTP 429 or an unrecognised ledger outcome stops the acquisition loudly (CLI `failed`, exit 1); no unfenced fallback.
- `put_if_absent` and `replace_if_match` on HF upload one part only and raise above the measured single-part limit.
- Catalog keys stay `catalog/v/<seq>.ducklake` and the manifest form unchanged.
- Error taxonomy: compare-and-swap 409/412 and a `CURRENT` read-back mismatch raise `LeaseLostError`; an unexplained
  existing version raises `IntegrityError`; both are CLI `failed`, exit 1. `ConflictError` (CLI `skipped`, exit 3)
  stays only for "a run holds the lease".
- Every HF session (spike, race tests, the full suite) stays inside one commit budget (measured, else 60 attempts per
  hour, rejected attempts included) and outside 02:30–03:30 UTC (the 03:00 production run).
- The secret values are never read into the session; HF credentials go by name or by `--secrets-file` path.
- Heavy runs (full gate, scale test) on the Mac Mini, never the Windows AI-PC.
- No commit: T6 rides the single pre-commit-program commit, after the owner's explicit approval (Task 12).

## Review Focus

1. The HF Hub API is down or returns HTTP 5xx during an acquisition: the acquisition stops loudly and writes nothing to
   the bucket (Task 4, `test_ledger_outage_writes_nothing`).
2. `lease.json` at the head is unparsable or names a generation below the bucket's highest: never treated as free;
   the step-2 maximum and the loud stop apply (Task 4, `test_corrupt_ledger_entry_stops`).
3. Clock skew between the Hub commit time and the local clock: the 12-minute rule uses the commit time plus
   `TAKEOVER_GRACE`, which absorbs up to 2 minutes (Task 4, `test_held_until_uses_commit_time`).
4. The control repository is missing, misnamed or unreadable at startup: a `ConfigError` (exit 2) that names it, before
   any work (Task 3, `test_control_repo_must_match_the_lake`, `test_missing_control_repo_fails_at_open`).
5. A catalog near the single-part limit: the growth warning fires in a band below the limit, and a catalog above the
   limit is refused before the upload with a message that names both sizes (Task 2,
   `test_catalog_warning_band_below_the_limit`, `test_commit_refuses_a_catalog_above_the_limit`).

---

### Task 0: Baseline, ledger and the test image

- [ ] **Step 1:** Create this plan's ledger in the session scratchpad (`t6/progress.md`, first line names this plan).
- [ ] **Step 2:** On the Mac Mini, run the full gate (`scratchpad/t3/mac_gate.sh t6-baseline`) on the current tree.
  Expected: green; record the counts as the T6 baseline.
- [ ] **Step 3:** Create the private Docker Space `karstenskyt/jobs-test` (`HfApi.create_repo(repo_type="space",
  space_sdk="docker", private=True)`) with the `deploy/job/README.md` front matter. Every T6 HF run until Task 11 uses
  `hf.co/spaces/karstenskyt/jobs-test`. The owner confirms this Space explicitly at the plan approval (T6-PLAN A-06,
  B-10): decision 2 named only the control repositories, and creating a Space is an outward action.

### Task 1: The spike (HF Job on the test image; stops for the owner on any stop rule)

**Files:** none in the repo. A stage-only script `t6_spike.py` is added to the `jobs-test` stage (never to the repo);
results go to the ledger.

- [ ] **Step 1:** Create `luxury-lakehouse/lake-test-control` (private dataset repository) through `HfApi.create_repo`
  under the owner's decision 2 of 2026-10-31. Record its initial commit sha.
- [ ] **Step 2:** Write `t6_spike.py` and run it in one HF Job on `jobs-test` (test buckets, `lake-test-control`, Job
  token by name, gateway key by `--secrets-file` path), `--timeout 4h` (the attempts are spread to at most 60 per hour,
  so the spike takes about two hours), started outside 02:30–03:30 UTC. Probes, each with its stop rule:
  - **Commit compare-and-swap:** rounds of a 3-way `create_commit` race on `lease.json` with one shared
    `parent_commit`; each request records its send and reply time; a round counts when all requests were sent before
    the first reply. Pass: at least 20 overlapped rounds, each with exactly one success. **Stop:** any round with more
    than one success. Record the parent-check failure's exception class and HTTP status.
  - **Head read and commit time:** after each successful commit, `repo_info(repo_id, repo_type="dataset")` gives the
    head `sha` and `last_modified`; record that `sha` equals the commit's sha and that `last_modified` lies between the
    client clock before and after the call. The commit payload carries only summary, description and `parentCommit`
    (`huggingface_hub` 2.3.0 `_commit_api.py`), so the time is set by the server; this probe corroborates it
    (T6-PLAN B-07). **Stop:** `sha` differs from the commit's, or `last_modified` lies outside the window.
  - **Commit budget:** record every reply's rate-limit headers (if any) and whether rejected commits change the
    remaining count. **Stop:** none; no later HF session runs without a stated budget.
  - **Single-part limit:** single-part `put_object` with `IfNoneMatch="*"` (the production call, T6-PLAN B-08) of 64,
    256, 1,024 and 2,048 MiB to `data/_live/<run>/sp-<size>.bin` on the test bucket, stopping at the first failure;
    delete each after.
  - **`CURRENT` read-back race:** rounds of a 3-way `replace_if_match` race on a test key, each writer reading back at
    once, with send and reply times. Pass: at least 20 overlapped rounds. **Stop:** any round in which a writer's
    read-back shows its own bytes while the value read after the round differs.
- [ ] **Step 3:** Record every number in the ledger. If a stop rule fired, stop and bring the evidence to the owner.
  Otherwise set the plan constants: `SINGLE_PART_LIMIT` (the largest passing size), the parent-check status set, the
  commit budget.

### Task 2: Single-part conditional writes, the version fence, the catalog size limit

**Files:**
- Modify: `src/lakehouse/storage/hf.py` (`put_if_absent`, `replace_if_match`, `_s3_put`, `single_part_limit`),
  `src/lakehouse/storage/base.py` (`Storage.single_part_limit`), `src/lakehouse/storage/local.py` (returns `None`)
  (T6-PLAN A-02), `src/lakehouse/errors.py`, `src/lakehouse/catalog/working.py` (`_fence_existing`, `commit`),
  `src/lakehouse/config/model.py` and `src/lakehouse/config/defaults.yaml` (`checkpoint.warn_catalog_mb`)
- Test: `tests/unit/test_storage_hf_mocked.py`, `tests/unit/test_working_catalog_fence.py` (new),
  `tests/unit/test_checkpoint_policy.py`

**Interfaces:**
- Produces: `SINGLE_PART_LIMIT: Final[int]` in `storage/hf.py` (value from Task 1, the measurement in the comment);
  `StorageLimitError(LakehouseError)` in `errors.py` (CLI `failed`, exit 1); `Storage.single_part_limit` (a read-only
  property: `int | None`; `LocalStorage` returns `None`, no limit).
- The `checkpoint.warn_catalog_mb` default becomes at most half of the limit in MB, rounded down, and never above
  today's 2048 (T6-PLAN B-04): the warning then always fires before a catalog can reach the limit.

- [ ] **Step 1: Failing tests.** In `test_storage_hf_mocked.py`: `test_conditional_writes_are_single_part` (a `Path`
  body above the old 64 MiB threshold goes through one `put_object` with `IfNoneMatch`, never
  `create_multipart_upload`; the same for `replace_if_match`), `test_conditional_write_above_the_limit_raises`
  (parametrized over `put_if_absent` and `replace_if_match`, T6-PLAN A-04: `StorageLimitError` naming both sizes; no
  S3 call), `test_plain_put_keeps_multipart`, `test_put_if_absent_412_returns_false` (the fence input). In
  `test_working_catalog_fence.py` (T6-PLAN B-06, the version fence `_fence_existing`, reached through `commit` with a
  storage whose version key already exists): `test_existing_version_with_lost_lease_raises_lease_lost` (the lease's
  `assert_held` raises), `test_existing_version_with_moved_current_raises_lease_lost` (`CURRENT` moved since the
  commit started), `test_unexplained_existing_version_is_an_integrity_error` (lease held, `CURRENT` unchanged;
  CLI exit 1 through `cli.main` with a patched handler), `test_existing_manifest_takes_the_same_fence` (the manifest
  key exists, the version key does not), `test_commit_refuses_a_catalog_above_the_limit` (a storage whose
  `single_part_limit` is below the catalog size → `StorageLimitError` before any `put_if_absent`). In
  `test_checkpoint_policy.py`: `test_catalog_warning_band_below_the_limit` (a published size between
  `warn_catalog_mb` and the limit records `catalog_growth` and publishes; the default `warn_catalog_mb` is at most half
  the limit). The restore and cleanup paths keep building keys from `seq` (keys unchanged); their existing suites run in
  Step 4 as the "exists/delete paths" check.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement: conditional writes call `put_object` once (stream the file for a `Path`); the size check
  first; `_fence_existing`'s last branch raises `IntegrityError` ("version N already exists and neither a lost lease
  nor a moved CURRENT explains it"); `commit` checks the detached file's size against `storage.single_part_limit`
  before the version upload; the `warn_catalog_mb` default per the Interfaces rule.
- [ ] **Step 4:** Run the three files plus `tests/unit/test_working*.py tests/unit/test_cli.py tests/unit/test_restore*.py
  tests/unit/test_cleanup*.py tests/unit/test_config_model.py`. Expected: PASS. Mutate (the multipart branch back for
  conditional writes; the size check off; `IntegrityError` back to `ConflictError`) — each must fail a test.

### Task 3: The control repository setting and the ledger seam

**Files:**
- Create: `src/lakehouse/storage/ledger.py`
- Modify: `src/lakehouse/storage/roots.py` (`resolve_roots`, `open_storage`), `src/lakehouse/storage/hf.py`
  (`HfStorage(..., ledger: LeaseLedger | None = None)`, `lease_ledger()`), `src/lakehouse/storage/base.py`
  (`Storage.lease_ledger()`), `src/lakehouse/storage/local.py` (returns `None`), `src/lakehouse/errors.py`
- Test: `tests/unit/test_ledger.py` (new), `tests/unit/test_roots.py`

**Interfaces:**
- Produces, in `storage/ledger.py`:
  - `LedgerEntry(BaseModel)`: `generation: int (ge=1)`, `holder_id: str`, `run_id: str`, `acquired_at: AwareDatetime`;
    frozen, `extra="forbid"`.
  - `@dataclass(frozen=True) LedgerHead`: `sha: str`, `entry: LedgerEntry | None`, `committed_at: datetime`.
  - `class LeaseLedger(Protocol)`: `head() -> LedgerHead`; `claim(entry: LedgerEntry, *, parent: str) -> str` (the new
    sha; named `claim` by the Task 8 gate ruling, because `test_no_direct_publish` treats `.commit(<args>)` as a
    catalog publish). Raises `LedgerConflictError` on a parent-check failure (the Task 1 status set), `LedgerUnavailableError` on
    HTTP 429, and lets every other exception propagate.
  - `class HubLedger`: `__init__(self, api: HfApi, repo_id: str)`. `head()` makes at most two requests and one when the
    head is unchanged (T6-PLAN B-02): `repo_info(repo_id, repo_type="dataset")` gives `sha` and `last_modified` (the
    commit time, Task 1); `lease.json` at that sha comes from `hf_hub_download(..., revision=sha)` only when the sha
    differs from the last one read (an in-memory cache keyed by sha). It never pages the commit history. A missing file
    gives `entry=None`; an unparsable file raises `IntegrityError`.
- Produces, in `errors.py`: `LedgerConflictError(LakehouseError)`, `LedgerUnavailableError(LakehouseError)`,
  `LeaseLedgerError(LakehouseError)` (an acquisition stopped by the ledger; CLI `failed`, exit 1). (Ruling to record:
  the spec's "`LeaseError`" is this class; no `LeaseError` exists.)
- Produces, in `roots.py`: `CONTROL_REPO_ENV = "LAKEHOUSE_CONTROL_REPO"`; `ResolvedRoots.control_repo: str | None`.

- [ ] **Step 1: Failing tests.** `test_roots.py`: `test_control_repo_required_with_gateway_credentials`
  (`MissingRootError` naming the variable), `test_control_repo_must_match_the_lake` (`ConfigError` naming the value
  and the expected `<lake bucket>-control`), `test_local_lake_has_no_control_repo`. `test_ledger.py` (a fake `HfApi`
  that counts calls): `test_head_of_an_empty_repo_has_no_entry`, `test_head_reads_the_entry_at_its_sha`,
  `test_head_is_bounded` (two calls on a new sha, one on an unchanged sha; no `list_repo_commits`),
  `test_unparsable_entry_is_an_integrity_error`, `test_parent_check_failure_is_a_conflict`,
  `test_rate_limit_is_unavailable`, `test_other_errors_propagate`, `test_missing_control_repo_fails_at_open`
  (`open_storage` with a repo the API reports absent → `ConfigError` naming it).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement. `open_storage` builds `HubLedger` only for an HF lake with gateway credentials and
  `control_repo` set, and checks the repository exists (one `repo_info` call).
- [ ] **Step 4:** Run, plus `uv run lint-imports` (the ledger stays inside `lakehouse.storage`). Expected: PASS,
  contracts kept.

### Task 4: `Lease.acquire` and `Lease.assert_held` on the ledger (spec D1 steps 1–7)

**Files:**
- Modify: `src/lakehouse/storage/lease.py`
- Test: `tests/unit/test_lease_ledger.py` (new; a `FakeLedger` in `tests/support/lakekit.py` with scripted outcomes:
  conflict, lost reply, 429, outage, delays)

**Interfaces:**
- Consumes: Task 3's `LeaseLedger`, `LedgerEntry`, `LedgerHead`, errors.
- Produces: `Lease.acquire(storage, *, holder_id, run_id, clock=...)` unchanged in signature; it uses
  `storage.lease_ledger()` when not `None`.

- [ ] **Step 1: Failing tests** (each named after the spec rule it pins):
  `test_single_winner_commits_the_next_generation` (step 4), `test_generation_is_max_of_ledger_and_bucket` (step 2,
  bucket above the ledger → decides on the bucket generation and skips), `test_bootstrap_from_bucket_listing`
  (empty ledger, bucket generations 1–3 → commits 4), `test_ledger_generation_without_bucket_object_is_held`
  (12 minutes from `committed_at`, then free), `test_held_until_uses_commit_time` (Review Focus 3),
  `test_lost_race_writes_skip_record_naming_the_winner` (step 5), `test_landed_commit_with_lost_reply_is_a_win`
  (spec B-03), `test_rate_limit_stops_loudly` (`LeaseLedgerError`, no bucket write), `test_ledger_outage_writes_nothing`
  (Review Focus 1), `test_corrupt_ledger_entry_stops` (Review Focus 2), `test_bucket_fence_false_raises_and_writes_nothing`
  (step 6), `test_stalled_winner_is_caught_by_the_re_read` (step 7: a newer ledger generation appears between steps 4
  and 7 → `LeaseLostError`, no anchor or marker), `test_assert_held_reads_the_ledger` (a newer ledger generation →
  `LeaseLostError`), `test_local_lease_is_unchanged` (no ledger: the existing `test_lease.py` passes untouched).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement the seven steps in `acquire` (a private `_acquire_on_ledger`), the ledger read in
  `assert_held` before today's bucket checks, and `_confirmed_at` set from the step-7 read.
- [ ] **Step 4:** Run `test_lease_ledger.py test_lease.py test_lease_callers.py test_cli.py` and
  `tests/e2e/test_crash_recovery.py`. Expected: PASS. Mutate (step 7 off; the 12-minute rule → free at once; the
  step-2 maximum → ledger only) — each must fail a test.

### Task 5: `CURRENT` read-back (spec D3)

**Files:** Modify `src/lakehouse/catalog/publish.py` (`publish_current`); Test `tests/unit/test_publish.py`.

- [ ] **Step 1: Failing tests:** `test_replace_reads_current_back` (a storage whose `get_versioned` after the replace
  returns another pointer → `LeaseLostError` with "read back differs"), `test_read_back_mismatch_exits_1` (through
  `cli.main`: `failed`, exit 1).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement: after `replace_if_match`, `get_versioned` and compare the bytes with `pointer.to_bytes()`;
  return the pointer with the read tag.
- [ ] **Step 4:** Run `test_publish.py` plus the catalog suites (`test_working*`, `test_restore*`, `test_relocate`,
  `test_cleanup*`). Expected: PASS.

### Task 6: The HF race test, red first, then the HF suite (test image only)

**Files:** Create `tests/hf/test_lease_race_hf_live.py`; modify `tests/hf/test_space_hf_live.py`; count the commits and
the ledger reads of every lease-taking HF test in the ledger.

- [ ] **Step 1:** Write `test_six_takers_one_wins` (`hf`, `hf_job`, `test_lake`): six processes start together
  (`multiprocessing` barrier), each `Lease.acquire`; exactly one returns a lease; the others return `None` with a skip
  record; the repeat count comes from `LAKEHOUSE_RACE_REPEATS` (default 1). Add to `test_space_hf_live.py`
  `test_space_token_cannot_touch_the_control_repo` (T6-PLAN B-05): with the Space-scoped token, `repo_info` and a
  `create_commit` on the control repository both fail with HTTP 401, 403 or 404.
- [ ] **Step 2: Red first** (T6-PLAN B-03). Deploy `jobs-test` from the tree **before Tasks 2–5** (the plan executes
  Task 6 Steps 1–2 right after Task 1) and run the race test in a Job with `LAKEHOUSE_RACE_REPEATS=10`, inside the
  budget. Expected: at least one repeat fails. Record each failure mode (more than one lease returned, or the T3
  `LeaseMarkerError` "already exists") and the job id. **Stop:** if no repeat fails, the test cannot prove the fix;
  bring it to the owner.
- [ ] **Step 3:** After Tasks 2–5: count the commit attempts and the ledger reads of one full HF suite run (every
  lease-taking test, rejected attempts included; reads come from `assert_held`, which the T3 `confirm` gate limits to
  one per `HEARTBEAT` plus one per publish, T6-PLAN A-03) and check they fit the budget and HF's API request limits with
  the 20 race repeats; split the session over hours if not. Deploy `jobs-test`; run the hf_job tier, the hf_space tier
  (with the owner's Space-scoped token `HF_SPACE_TOKEN` passed as the Job's `HF_TOKEN`, by name) and
  `test_six_takers_one_wins` with `LAKEHOUSE_RACE_REPEATS=20`, outside 02:30–03:30 UTC. Expected: all pass except
  T8's page budget (owner-deferred).

### Task 7: Docs and amendments

**Files:** `docs/specs/2026-10-26-v2-ingestion-design.md` (§10.4, §10.6, §26 item 2, §29.9), `docs/adrs/ADR-007-single-writer-and-catalog-safety.md`,
`docs/ops/deploy.md` (the control repository, `LAKEHOUSE_CONTROL_REPO`, the cutover procedure, maintenance commands as
Jobs, no older-code CLI against an HF lake, the `jobs-test` image), `docs/audits/2026-10-30-v2-audit-ledger.md` (T6
implemented, with the test files and the spike numbers; DONE after Task 11).

- [ ] **Step 1:** Apply the spec's Amendments. Run `uv run pytest tests/contract -q`. Expected: PASS.

### Task 8: The full gate and the scale test (Mac Mini)

- [ ] **Step 1:** Push the tree to the Mac Mini; run the full gate (`mac_gate.sh t6-gate`). Expected: green against
  the Task 0 counts plus the new tests.
- [ ] **Step 2:** Run `tests/bench/test_catalog_scale.py -m bench` (`mac_scale.sh`). Expected: PASS, bounds as in T3.

### Task 9: Freeze the reviewed tree

- [ ] **Step 1:** Record the sha256 of every staged file of the Job-image stage built from the tree that Task 10
  reviews (`t6/stage.sha256` in the scratchpad). Between Task 10 and Task 11 the tree does not change; a review fix
  re-runs Tasks 8 and 9 (T6-PLAN A-05). All test infrastructure, including the throwaway scheduled job
  `6ae675bc0e0338d1bb7d4976` that the hf_space schedule test needs, stays until Task 11 has passed (T6-PLAN B-09).

### Task 10: The A/B implementation review

- [ ] **Step 1:** Both reviewers review the T6 implementation (reports in `D:\Development\_reviews\`). Every finding
  is fixed (with a failing test first) or answered; an A/B disagreement goes to the owner.

### Task 11: Production cutover (only after the owner's explicit yes)

- [ ] **Step 1: Gate.** Show the owner the review outcome, the cutover steps below and the rollback, and wait for an
  explicit yes for the cutover (T6-PLAN A-01). No production change before it.
- [ ] **Step 2:** Create `luxury-lakehouse/lake-control` (private dataset repository). Keep a copy of the current
  `karstenskyt/jobs` stage (the T3 image) and of the current scheduled job's spec (`inspect_scheduled_job`, names and
  environment, never secret values) for the rollback.
- [ ] **Step 3:** Cutover, outside 02:30–03:30 UTC: suspend the scheduled job; wait until `hf jobs ps` shows no Job of
  the old image; deploy `karstenskyt/jobs` from the reviewed tree; recreate the scheduled job with
  `LAKEHOUSE_CONTROL_REPO=luxury-lakehouse/lake-control` in its environment (the recreate procedure in
  `docs/ops/deploy.md`); point `LAKEHOUSE_HF_SCHEDULED_JOB_ID` at it; resume; trigger one run and read its log: the
  lease is acquired through the ledger (the first acquisition bootstraps from the highest bucket generation). The
  deployed stage's sha256 list must equal `t6/stage.sha256` (Task 9).
- [ ] **Rollback** (T6-PLAN A-05), if the triggered run fails on the new code: suspend the scheduled job, redeploy
  `karstenskyt/jobs` from the kept T3 stage, recreate the scheduled job from the kept spec (without
  `LAKEHOUSE_CONTROL_REPO`), point `LAKEHOUSE_HF_SCHEDULED_JOB_ID` at it, resume, and bring the failure to the owner.
  The bucket generation objects the T6 run wrote keep the T3 format, so the T3 image reads them; `lake-control` stays
  unused until a fixed cutover.
- [ ] **Step 4:** Delete the throwaway scheduled job `6ae675bc0e0338d1bb7d4976`, the test bucket pair,
  `lake-test-control` and `jobs-test`; mark audit ledger T6 DONE.

### Task 12: Human approval gate for the commit

- [ ] **Step 1:** Stop. Show the owner the file list of the single pre-commit-program commit and wait for an explicit
  yes. No `git commit`, `push` or PR before it.

## Self-review notes

- Spec coverage: D1 steps 1–7 (Tasks 3–4), credentials and lease takers (Tasks 3, 6, 7, 11), cutover (Task 11), D2
  (Task 2), D3 (Task 5), spike and budget (Tasks 1, 6), amendments (Task 7), residuals (accepted; no task).
- Execution order: 0, 1, 6 (Steps 1–2, red), 2, 3, 4, 5, 6 (Step 3), 7, 8, 9, 10, 11, 12.

## Round-1 review disposition

| Finding | Severity | Disposition |
|---|---|---|
| A T6-PLAN-01 | BLOCKING | The production cutover moved to Task 11, after the A/B review (Task 10), behind its own explicit owner yes; Global Constraints state that production is untouched until then. |
| A T6-PLAN-02 | SHOULD FIX | Task 2's Files list adds `storage/base.py` and `storage/local.py`. |
| A T6-PLAN-03 | CONSIDER | Task 6 Step 3 counts ledger reads too and states the `confirm` bound. |
| A T6-PLAN-04 | CONSIDER | The above-limit test is parametrized over `put_if_absent` and `replace_if_match`. |
| B T6-PLAN-01 | BLOCKING | Every T6 HF run uses the test image `karstenskyt/jobs-test` (Task 0 Step 3); `karstenskyt/jobs` and the scheduled job change only in Task 11. |
| B T6-PLAN-02 | SHOULD FIX | `HubLedger.head()` uses `repo_info` plus a cached `hf_hub_download`: at most two requests, one when unchanged; never `list_repo_commits`; test `test_head_is_bounded`. |
| B T6-PLAN-03 | SHOULD FIX | The red-first run repeats 10 times, records each failure mode, and stops if never red. |
| B T6-PLAN-04 | SHOULD FIX | The `warn_catalog_mb` default becomes at most half the limit; test of the warning band. |
| B T6-PLAN-05 | SHOULD FIX | `test_space_token_cannot_touch_the_control_repo` in the hf_space tier. |
| B T6-PLAN-06 | SHOULD FIX | Task 2 names tests for both `LeaseLostError` branches, the manifest case and the `IntegrityError` case; "exists/delete paths" defined (restore and cleanup suites, keys unchanged). |
| B T6-PLAN-07 | CONSIDER | The commit time is server-set by the payload shape (`_commit_api.py`); the probe corroborates it and also checks `repo_info`'s `sha`/`last_modified`, which `head()` uses. |
| B T6-PLAN-08 | CONSIDER | The single-part probe sends `IfNoneMatch="*"`; the spike Job has `--timeout 4h`; "Push the tree to the Mac Mini". |

## Round-2 review disposition

| Finding | Severity | Disposition |
|---|---|---|
| A T6-PLAN-05 | CONSIDER | Task 9 freezes the reviewed tree (stage sha256 list, checked at deploy); Task 11 names the rollback and keeps the T3 stage and job spec for it. The program commit stays after the owner's UI test (Task 12), so the cutover precedes it by design. |
| A T6-PLAN-06 / B T6-PLAN-10 | CONSIDER | `jobs-test` goes to the owner for an explicit yes at the plan approval; no ruling by analogy. |
| B T6-PLAN-09 | SHOULD FIX | The throwaway scheduled job stays until Task 11 Step 4. |
