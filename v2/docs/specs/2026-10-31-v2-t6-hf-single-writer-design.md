# v2 T6 — HF single-writer hardening design

**Status:** Round 3/4 (2026-10-31): Reviewer A APPROVE (round 3), Reviewer B APPROVE (round 4); reports `D:\Development\_reviews\2026-10-31-v2-t6-hf-single-writer-spec-r3-A.md` and `...-spec-r4-B.md`. **Owner decisions 2026-10-31:** open decision 1 = option A (replacing the recorded "settle, read back" mechanism); decision 2 = the session creates both control repositories; decision 3 = both residuals (D2, D3) accepted. **Spec and plan owner-approved 2026-10-31.** Round 2:
Reviewer A APPROVE (one CONSIDER, A-07), Reviewer B APPROVE WITH CHANGES (2 SHOULD FIX, 4 CONSIDER); reports
`D:\Development\_reviews\2026-10-31-v2-t6-hf-single-writer-spec-r2-{A,B}.md`. Round 1: Reviewer A REQUEST CHANGES
(1 BLOCKING, 3 SHOULD FIX, 2 CONSIDER), Reviewer B REQUEST CHANGES (1 BLOCKING, 6 SHOULD FIX, 2 CONSIDER); reports
`D:\Development\_reviews\2026-10-31-v2-t6-hf-single-writer-spec-{A,B}.md`. Every finding is answered in the
disposition tables at the end. Owner decision 2026-10-31: harden the HF single-writer guarantee in its own full cycle (spec, A/B, owner,
plan, A/B, implementation, A/B), inside the gold-standard pre-commit program (audit ledger
`docs/audits/2026-10-30-v2-audit-ledger.md`, finding T6). It amends the ingestion design
`docs/specs/2026-10-26-v2-ingestion-design.md` §10.4, §10.6, §26 item 2 and §29.9, and ADR-007, as listed in
"Amendments".

## Intent and why

The v2 lake allows exactly one writer at a time (§10.6). On HF the guarantee rests on three conditional writes through
the S3 gateway: a lease generation is created only if absent (`If-None-Match: *`), a catalog version is created only if
absent, and `CURRENT` is replaced only by compare-and-swap (`If-Match: <etag>`). The T3 Task 12 runs (2026-10-31) showed
that the gateway makes none of the three atomic under concurrency, and that it ignores the condition on a multipart
upload. Two writers that act within milliseconds of each other can both win a lease generation, both replace
`CURRENT`, or overwrite each other's catalog version. The owner wants the single-writer guarantee to hold by
construction on HF, as it does on a local lake.

**This spec departs from the mechanism the owner recorded (T6-SPEC A-03 / B-02).** The audit ledger records the
decision as: "**DECIDED — separate hardening cycle T6** (owner 2026-10-31): full spec, A/B, plan; single-part version
uploads and a write, settle, read-back confirmation for every conditional create; document the residual." That
mechanism was the author's sketch in the question to the owner. Spec work found that a settle delay cannot order
concurrent writes, so its safety would rest on timing (option C below, an anti-goal). This spec keeps the
"single-part" half (D2) and proposes an atomic primitive, an HF Hub commit compare-and-swap, in place of the
"settle, read back" half for the lease (option A). Open decision 1 asks the owner to confirm or reject that
replacement.

## Evidence (2026-10-31, test buckets `luxury-lakehouse/lake-test` and `/restricted-test`)

| Probe | Where | Result |
|---|---|---|
| Two processes take the lease together (`test_two_processes_one_wins`) | HF Job `6ae675bd…` | both acquired generation 1, 11 ms apart |
| Two takers of one expired lease (`test_two_takers_of_one_expired_lease`) | HF Job `6ae681a5…` | both won generation 2; the T3 anchor check raised `LeaseMarkerError` for one |
| 6-way single-part `put_if_absent` race | HF Jobs (both) | 2 winners |
| 3-way multipart `put_if_absent` race (11 MiB) | HF Jobs (both) | 3 winners |
| Sequential single-part `put_if_absent` twice | AI-PC | True, then False (HTTP 412 `PreconditionFailed`) |
| Sequential multipart `put_if_absent` twice | AI-PC | True, True: the second upload overwrote the object (`IfNoneMatch` ignored on `CompleteMultipartUpload`) |
| 10 rounds of a 6-thread single-part race | AI-PC | winners per round `[1, 4, 5, 3, 3, 6, 5, 2, 5, 2]` |
| 10 rounds of a 6-thread `replace_if_match` race on one ETag | AI-PC | successes per round `[1, 3, 4, 3, 3, 2, 3, 4, 3, 4]` |
| Sequential `replace_if_match` with a stale ETag (`test_replace_if_match_stale_etag_is_412`) | HF Jobs (both) | HTTP 412 (passed) |

The AI-PC probe outputs are saved verbatim in
`C:\Users\Karsten\AppData\Local\Temp\claude\D--Development-karstenskyt--luxury-lakehouse-skunk\fb69b5d5-7757-4416-a985-8eabfe3777b4\scratchpad\t3\hf_logs\probe_outputs.txt`,
beside the probe scripts and the Job logs. The client code is correct S3 usage (`storage/hf.py` `put_if_absent`,
`replace_if_match`, `_s3_put_multipart`): it sends the condition and treats HTTP 409 or 412 as a lost race
(`_PRECONDITION_STATUS`). So the gateway's conditions hold for **sequential** requests and fail for **concurrent** ones,
and a `Path` upload above `_MULTIPART_THRESHOLD` (64 MiB) has no condition at all. Only the catalog version upload
passes a `Path` (`catalog/working.py` `commit`); every other conditional write passes bytes, which always go up as a
single part.

**Already fixed in T3 (defense in depth).** In the second Job run, two takers won one generation; T3's write-once run
anchor caught the double win, but its failure path marked the shared generation released, which would release the real
winner's lease. T3 now raises without touching the generation when an anchor or marker already exists
(`storage/lease.py` `_store_anchor_and_marker`; `test_existing_anchor_or_marker_raises_without_releasing`). That makes a
double win fail loudly for one taker; it does not prevent the double win, which is T6's job.

**Exposure today.** Production HF runs only the scheduled job, which HF refuses to start twice (`--no-concurrency`,
verified by `test_schedule_controls_the_throwaway_job` in both Space-tier Jobs). The Ops "Run now" button triggers the
same scheduled job. Two writers therefore need an operator who starts a one-off job while the scheduled run is active,
or a takeover race after a lease expiry. The window is milliseconds wide. The exposure is small, but the guarantee is
false.

## Goals

- Exactly one taker wins each HF lease generation, under any concurrency, by construction.
- A writer that lost its lease cannot overwrite a catalog version that another writer published, except in the stated
  residual (D2).
- A lost update of `CURRENT` is detected and fails loudly, except in the stated stall residual (D3), which the owner
  accepts or rejects in open decision 3 (T6-SPEC A-04).
- The local lake keeps its present mechanism and behaviour (file-system create-exclusive and atomic rename are atomic).
- The commit and request volume stays inside HF's limits for a heavy run, and no test burst can block a production
  acquisition.

## Non-goals

- No change to readers, the catalog key formats, the T3 checkpoint policy, the sweep rules or the anchor intervals.
- No change to the HF scheduled-job setup (`--no-concurrency` stays).
- No second storage provider.

## Anti-goals (a failure even if the code works)

- A design whose safety depends on timing (a sleep or a settle delay) instead of an atomic primitive, unless the owner
  accepts that residual explicitly.
- A design that can exceed HF's commit or request quota during a heavy run or a test session, so a run stops on HTTP 429.
- A silent fallback: if the atomic primitive fails or is unavailable, the acquisition stops; it never writes unfenced.

## Options

**A. HF Hub commit compare-and-swap for the lease (recommended).** HF Hub repositories are git repositories. A commit
through `HfApi.create_commit(..., parent_commit=<sha>)` fails when the branch no longer points at `parent_commit`
(`huggingface_hub` 2.3.0, the version `uv.lock` pins; the parameter's reference text: "If specified and `create_pr` is
False, the commit will fail if `revision` does not point to `parent_commit`", and it "ensures the repo has not changed
before committing the changes, and can be especially useful if the repo is updated / committed to concurrently"). That is
a compare-and-swap on the branch head, if the server applies it atomically; the spike proves or disproves that under real
concurrency. A small private control repository per lake holds the lease ledger: one commit per acquisition. The
heartbeat stays a plain bucket write, so a heavy run makes one commit, far below any plausible cap.

**B. HF Hub commit compare-and-swap for the lease and for `CURRENT`.** As A, plus `CURRENT` moves into the control
repository and every publish is a commit. This removes the D3 residual, but a heavy run makes one commit per publish:
the T3 scale test made 101 publishes in 223 s for a 100-match privacy burst (Mac Mini, 2026-10-31; saved in the same
`probe_outputs.txt`), against a repository commit cap that an HF error text reports as "128 per hour". A run could stop
on HTTP 429.

**C. Write, settle, read back** (the mechanism the audit ledger recorded). Keep the gateway conditional writes and add a
settle delay and a read-back after each one: the racer whose bytes are present after the delay wins. Safety then
depends on every racing write finishing before the earliest read-back, which no API guarantees (the gateway is not
linearizable for conditional writes). This is the first anti-goal.

**Recommendation: A**, after a spike proves the commit compare-and-swap in an HF Job (Verification). If the spike fails,
the cycle stops and brings the evidence to the owner.

## Design (option A)

### D1. The control repository and the lease ledger

**The ledger is the fence of record and the source of truth for who holds the HF lease** (T6-SPEC B-10, A-06).

- One private HF dataset repository per lake, named after the lake bucket with a `-control` suffix:
  `luxury-lakehouse/lake-control` for production and `luxury-lakehouse/lake-test-control` for the test lake. The HF
  lake's settings name it (a non-secret Job variable `LAKEHOUSE_CONTROL_REPO`, checked at startup like the S3 paths,
  audit T4). The repository holds one file, `lease.json`: the generation, the holder id, the run id and the acquisition
  time. The session creates the two repositories under the owner's standing HF directive of 2026-10-31 (open decision 2).
- **Credentials.** Only the Job's `HF_TOKEN` writes the control repository. The Space token and every reader token get
  no access to it: readers never read the ledger, and the Ops pages keep reading the bucket generation objects.
- **Every lease taker against an HF lake** (T6-SPEC B-14): the run and the re-parse, and the three maintenance commands
  `sweep_now` (`catalog/cleanup.py`), relocate and restore, all go through `Lease.acquire`. Against the HF lake they run
  as HF Jobs on the Job image, with the Job token, the same as the run (`docs/ops/deploy.md` gains the one-off command
  for each). An operator machine that runs them against the HF lake needs the T6 code and a token with write access to
  the control repository; a CLI of older code must not run against an HF lake after the cutover (it would fence on the
  bucket only), and the deploy procedure says so.
- **Acquisition.**
  1. Read the head commit and `lease.json` at that commit.
  2. The current generation `g` is the higher of the ledger's generation and the highest generation in the bucket
     listing (`runs/lease/<12 digits>.json`), or 0 when there is neither (T6-SPEC A-07). On a fresh ledger this is the
     **bootstrap**: new generations never reuse a bucket number (T6-SPEC B-01b). During a cutover overlap, where only an
     old image can make the bucket lead the ledger, the acquirer then decides on the old image's live generation and
     skips, instead of committing a generation the bucket already holds.
  3. Decide from `g`'s bucket generation object as today (expiry, `released`, takeover grace, `storage/lease.py`).
     **A ledger generation without a bucket object** (its winner crashed or stalled between the commit and the bucket
     write) is held until the ledger commit's server time plus `LEASE_TTL` plus `TAKEOVER_GRACE`, the same 12 minutes as
     an expired holder; it is never free at once (T6-SPEC B-01a). A held generation gives today's skip record.
  4. Commit `lease.json` with generation `g + 1` and `parent_commit` set to the head read in step 1.
  5. **Outcome** (T6-SPEC B-03). A clean success wins. Any other outcome (the parent check failed, a timeout, an HTTP
     5xx, a dropped connection) is decided by reading the head again: a head whose `lease.json` names this holder id and
     run id is a win (the commit landed and its reply was lost); a head that names another holder is a lost race,
     which writes the skip record naming the winner, as today; anything else stops the acquisition loudly. HTTP 429
     stops the acquisition loudly (`LeaseError`, exit 1); the next run retries; there is no unfenced fallback.
     **Implementation amendment (2026-11-01, HF Job `6ae6ca05…`):** a parent-check failure (HTTP 409, the status Task 1
     measured) is a lost race by itself: the commit did not land, because the Hub refuses it atomically (20 of 20
     rounds). Right after the winner's commit the head read can still show the old head, which the two-process test
     met, so for a 409 the head read only names the winner in the skip record (no one when it still lags) and never
     turns the lost race into a loud stop. Every other non-clean outcome is decided by the head as above.
  6. After a win, the winner creates the bucket generation object with the existing single-part `put_if_absent`
     (bytes), then the T3 run anchor and acquisition marker. The bucket `put_if_absent` stays as a second fence, not the
     fence of record (T6-SPEC A-06): `False` means a writer outside the ledger created that generation (only an old
     image can, see Cutover), so the winner raises `LeaseMarkerError` and writes nothing more; the ledger generation is
     then held by the 12-minute rule of step 3.
  7. **Before it returns the lease, the winner reads the ledger head again** (T6-SPEC B-11). A head that names a newer
     generation or another holder raises `LeaseLostError`, and the acquisition writes nothing more. This closes the case
     of a winner that stalled past the 12-minute rule between its commit (step 4) and its bucket writes (step 6), while
     a new taker committed `g + 1`; today's `acquire` closes the same gap by listing again after its put
     (`storage/lease.py` `acquire`). The lease's first confirmation time is set by this read.
- **`Lease.assert_held`** reads the ledger head: a newer generation, or a `lease.json` that names another holder, raises
  `LeaseLostError`. It keeps today's bucket check of the own generation object.
- **Heartbeat and release** stay plain bucket writes of the generation object (no commit). A released or expired bucket
  object lets the next acquirer commit `g + 1`.
- **The local lake** keeps the file-system lease unchanged.
- **Cutover** (T6-SPEC B-01c). An old image fences with the bucket `put_if_absent` only; it cannot see the ledger. The
  T6 deploy procedure therefore suspends the scheduled job, waits until no Job of the old image runs (`hf jobs ps`),
  deploys the image, and resumes the job (`docs/ops/deploy.md`). No maintenance command of older code runs during or
  after the cutover (T6-SPEC B-14). A sequential mixed-image overlap that the procedure misses is still caught by the
  step-2 maximum and the step-6 bucket fence; a race between an old and a new image is the same residual that exists
  today, and only during the cutover.

### D2. Single-part conditional writes, and the version fence

- On HF, `put_if_absent` and `replace_if_match` always upload as one part (T6-SPEC B-08). A body larger than the
  gateway's single-part limit raises, with a message that names the size and the limit; they never fall back to a
  multipart upload. The single-part limit comes from the spike and is a module constant with its measurement in the
  comment. Plain `put` keeps the multipart path for large unconditional uploads.
- **The catalog version keys stay `catalog/v/<seq>.ducklake`** (and the manifest keys stay as they are). Round 1's
  per-generation key is withdrawn (T6-SPEC A-01, B-04): it would change every build-from-`seq` site (restore, cleanup,
  verify, the working catalog, `services/catalog.py` `_VERSION_GONE`, `moves.py`, `checkpoint.py`) and break `seq` as an
  identity. With the version upload single-part, the sequential `If-None-Match` fence works: a writer that lost its lease
  and later uploads a version with a sequence the new holder already published gets HTTP 412 and stops
  (`WorkingCatalog._fence_existing`).
- A catalog file larger than the single-part limit cannot be published: the commit raises before the upload, and the
  T3 `catalog_growth` warning (`checkpoint.warn_catalog_mb`, default 2048 MB) fires earlier. The plan sets the warning
  threshold below the measured limit if the limit is smaller than 2048 MB. The T3 scale test measured 10.8 MB at 1,000
  matches after compaction.
- **Residual, stated for the owner** (T6-SPEC B-13): two writers whose uploads of the same sequence overlap in time can
  both succeed. A conditional upload is concurrent for its whole duration, so the window is the upload time: seconds
  for a catalog of tens of MB, up to minutes near the single-part limit. That needs a writer that lost its lease (D3's
  stall) whose upload overlaps a new holder's upload of the same sequence. Detection: the new holder's upload check
  (`commit` reads the stored version back and compares its SHA-256 before `CURRENT`) catches an overwrite that lands
  before that check; the next run's base load compares the version's size and SHA-256 with `CURRENT` and raises
  `IntegrityError` (`catalog/working.py`, the open path) for one that lands after it. Readers check only the size, so a
  reader can read an overwritten version until that next run.
- **The version-fence error** (T6-SPEC B-15): `_fence_existing` raises `LeaseLostError` when the lease is lost or
  `CURRENT` moved (unchanged), and today raises `ConflictError` when neither explains the existing version. With the
  ledger, a stale writer stops at `assert_held` first, so that last case is an unexplained object: it becomes
  `IntegrityError` (CLI `failed`, exit 1), never the benign `skipped`.

### D3. `CURRENT` write, read-back and error taxonomy

- Only the lease holder writes `CURRENT`. The write stays `replace_if_match` (single-part bytes), which catches a
  sequential stale write with HTTP 412. After the write, the writer reads `CURRENT` back and compares the whole pointer
  with what it wrote.
- **Error taxonomy** (T6-SPEC B-05, A-05): a 409 or 412 on the compare-and-swap raises `LeaseLostError` as today
  (`publish_current`); a read-back that differs raises `LeaseLostError` with its own message ("CURRENT read back differs
  from the pointer this run wrote: another writer published"). Both are reported by the CLI as `failed`, exit 1; neither
  is a `ConflictError`, which the CLI reports as a benign `skipped`, exit 3.
- **The stall residual, stated exactly** (T6-SPEC B-09, A-04). A holder whose whole process stalls for about 11 minutes
  (`LEASE_TTL` plus `TAKEOVER_GRACE`, 12 minutes, less the time since its last 60-second renew) in the short span between
  its last `assert_held` and its `CURRENT` write can overwrite a `CURRENT` that a new holder published meanwhile. The
  overwrite is detected and fails loudly in every case but one: the new holder's next `replace_if_match` uses its own
  ETag and gets HTTP 412, and the stale holder's read-back sees any later write. The overwrite passes silently only when
  the overwritten publish was the new holder's last publish of its run. Option B removes this residual at the cost of
  the commit budget.

## Verification

- **Spike (the first plan task, in an HF Job on the test buckets and `lake-test-control`).**
  - **Commit compare-and-swap under real concurrency** (T6-SPEC B-06): rounds of an N-way `create_commit` race with the
    same `parent_commit`. Each request records its send time and its reply time; a round counts only when every request
    was sent before the first reply arrived. The spike needs at least 20 overlapped rounds, each with exactly one success.
    A round with more than one success stops the cycle and goes to the owner. The parent-check failure's error class and
    HTTP status are recorded; the code recognises only that.
  - **Commit budget** (T6-SPEC B-07): the spike reads the rate-limit headers of every commit reply (if HF sends them) to
    learn the limit's scope and whether rejected commits count. It spreads its rounds over time so that its attempts stay
    under 60 per hour (half the reported cap) until that is known, and it runs away from the 03:00 UTC production run.
    If HF sends no headers, the spike records that and keeps the 60-per-hour budget; it never passes without a stated
    budget.
  - **Single-part limit** (D2): single-part `put_if_absent` uploads of 64, 256, 1,024 and 2,048 MiB to the test bucket,
    stopping at the first failure; the largest success sets the limit.
  - **Read-back under a `CURRENT` race** (T6-SPEC B-06, B-12): N writers race `replace_if_match` and each reads
    `CURRENT` back at once, with send and reply times recorded as above. The spike needs at least 20 overlapped rounds.
    A round in which any writer's read-back shows its own pointer while the final `CURRENT` (read after the round)
    differs stops the cycle and goes to the owner, because D3's detection rests on the read-back.
  - **Commit time source** (B, "could not verify"): the spike records whether the ledger commit's time (the commit's
    `created_at` from the Hub) is set by the server, by comparing it with the client clock before and after each
    commit. The 12-minute rule of D1 step 3 uses it; a client-set time stops the cycle for a redesign of that rule.
- **Red before green** (T6-SPEC B-06): a new 6-taker lease race test runs first against the present code in an HF Job
  and must fail (the 2026-10-31 Jobs already show `test_two_processes_one_wins` and
  `test_two_takers_of_one_expired_lease` failing); after the change it passes 20 times in one Job, inside the spike's
  commit budget.
- **The commit budget of the full HF suite** (T6-SPEC B-16): the plan counts the commits that the whole HF suite makes
  (every test that takes a lease), and every HF session (the spike, the race tests, the suite) runs inside the same
  measured or 60-per-hour budget and away from the 03:00 UTC production run. If the spike shows that the limit is
  shared with production (per account or organisation), the plan states that the goal "no test burst can block a
  production acquisition" then holds by that budget and schedule, and brings it to the owner.
- **Unit tests:** the held-until rule for a ledger generation without a bucket object; the bootstrap and the step-2
  maximum (a bucket generation above the ledger's); the step-6 `False` path (raises, writes nothing more); the step-7
  re-read (a winner that stalled past the 12-minute rule gets `LeaseLostError` and writes nothing more); a commit that landed but whose reply was lost (a win);
  a parent-check failure that names another holder (a lost race with a skip record); HTTP 429 (stops loudly, exit 1);
  `assert_held` against a newer ledger generation; the single-part refusal above the limit for `put_if_absent` and
  `replace_if_match`; the read-back mismatch (`LeaseLostError`, CLI exit 1); the version fence (a stale version upload
  of an existing sequence gets 412 and stops, on the exists/delete paths too; an unexplained existing version raises
  `IntegrityError`, CLI exit 1); the local lease unchanged.
- The full HF suite passes in Jobs (except T8's page budget, owner-deferred); the T3 scale test passes on the Mac Mini.

## Amendments

- Ingestion design §10.6 and §29.9: the HF fence of record becomes the control-repository ledger (D1); the gateway
  conditional writes are a second fence, single-part only (D2).
- §10.4: `CURRENT` read-back and its error (D3); catalog version uploads are single-part, bounded by the measured limit.
- §26 item 2: record the 2026-10-31 evidence and the T6 result.
- ADR-007 (single writer and catalog safety): the gateway conditional writes are not the fence on HF; the ledger is;
  versions keep the `catalog/v/<seq>.ducklake` form (T6-SPEC B-04).
- `docs/ops/deploy.md`: the control repository, `LAKEHOUSE_CONTROL_REPO`, and the cutover procedure.

## Open decisions for the owner (decided 2026-10-31: 1 = A, 2 = the session, 3 = accept both)

1. Replace the recorded "write, settle, read back" mechanism with option A for the lease (recommendation: yes), or keep
   C, or choose B.
2. The session creates the two control repositories under the standing HF directive (recommendation), or the owner
   does.
3. The residuals as stated (T6-SPEC B-17). **D3** (a stall of about 11 minutes between the last `assert_held` and the
   `CURRENT` write; silent only when the overwritten publish was the new holder's last): accept (recommendation), or
   choose option B, which removes this residual only, at the cost of the commit budget. **D2** (a stalled writer's
   version upload overlapping a new holder's upload of the same sequence, a window of the upload duration, seconds to
   minutes; caught by the upload check or the next run's base-load check, while readers check only the size): it stays
   under every option; accept (recommendation), or ask for a further design of the version fence.

## Round-1 review disposition

| Finding | Severity | Disposition |
|---|---|---|
| A T6-SPEC-01 | BLOCKING | Per-generation version keys withdrawn; keys stay `<seq>.ducklake`; the version fence comes from single-part `If-None-Match` (D2). No build-from-`seq` site changes. |
| A T6-SPEC-02 | SHOULD FIX | Moot with the key change withdrawn: `_VERSION_GONE` keeps matching. |
| A T6-SPEC-03 | SHOULD FIX | The departure from the recorded mechanism is quoted and explained in "Intent and why"; open decision 1 asks for it. |
| A T6-SPEC-04 | SHOULD FIX | Goal 3 now excepts the D3 residual, tied to open decision 3. |
| A T6-SPEC-05 | CONSIDER | Error taxonomy stated in D3 (`LeaseLostError` for both, distinct messages). |
| A T6-SPEC-06 | CONSIDER | D1 names the ledger as the fence of record and keeps the bucket `put_if_absent` as a second fence, with its role. |
| B T6-SPEC-01 | BLOCKING | D1 states the held-until rule for a ledger generation without a bucket object (12 minutes from the commit, never free), the bootstrap from the bucket listing, and the cutover procedure plus the step-6 fence; each has a test. |
| B T6-SPEC-02 | SHOULD FIX | As A T6-SPEC-03. |
| B T6-SPEC-03 | SHOULD FIX | D1 step 5 decides every non-clean outcome by reading the head (holder id and run id); the skip record stays; unrecognised outcomes stop loudly; unit test "commit landed, reply lost". |
| B T6-SPEC-04 | SHOULD FIX | Moot for the key format (withdrawn); ADR-007 and §10.4 added to Amendments. |
| B T6-SPEC-05 | SHOULD FIX | `LeaseLostError` (CLI `failed`, exit 1), with a CLI exit-code test. |
| B T6-SPEC-06 | SHOULD FIX | Overlapped-round rule with a minimum of 20; red-first 6-taker test; read-back probe under a `CURRENT` race. |
| B T6-SPEC-07 | SHOULD FIX | The spike reads rate-limit headers, keeps a 60-per-hour budget until scope is known, avoids 03:00 UTC, never passes without a stated budget; HTTP 429 stops the acquisition loudly. The 101-publishes figure is now cited to the saved scale report. |
| B T6-SPEC-08 | SHOULD FIX | D2: conditional writes are single-part only and raise above the measured limit; unit test. |
| B T6-SPEC-09 | CONSIDER | D3 states the residual exactly (about 11 minutes; silent only when the overwritten publish was the new holder's last). |
| B T6-SPEC-10 | CONSIDER | Credentials named (only the Job token writes the control repository); the ledger is authoritative for `assert_held`; citation now names `huggingface_hub` 2.3.0. |
| B claim check | — | "Treats HTTP 412" corrected to 409 or 412; the AI-PC probe outputs and the scale figure are saved in `probe_outputs.txt`. |

## Round-2 review disposition

| Finding | Severity | Disposition |
|---|---|---|
| A T6-SPEC-07 | CONSIDER | D1 step 2 takes the higher of the ledger's and the bucket's highest generation on every acquisition, so a cutover overlap skips instead of stopping. |
| B T6-SPEC-11 | SHOULD FIX | D1 step 7: the winner reads the ledger head again before it returns the lease; `LeaseLostError` on a newer generation or another holder; unit test. |
| B T6-SPEC-12 | SHOULD FIX | The read-back race probe has a stop rule (own pointer seen while the final `CURRENT` differs) and a minimum of 20 overlapped rounds. |
| B T6-SPEC-13 | CONSIDER | D2 states the window as the upload duration and names the detections (the upload check, the next run's base-load SHA-256 check; readers check only the size). |
| B T6-SPEC-14 | CONSIDER | D1 names every lease taker (run, re-parse, `sweep_now`, relocate, restore): HF Jobs on the Job image with the Job token; no older-code CLI against an HF lake after the cutover. |
| B T6-SPEC-15 | CONSIDER | D2: an unexplained existing version raises `IntegrityError` (exit 1), not `ConflictError`; pinned in the version-fence test. |
| B T6-SPEC-16 | CONSIDER | Verification: the plan counts the full HF suite's commits; every HF session runs inside one budget and away from 03:00 UTC; a shared limit goes to the owner. |
| B could-not-verify | — | The spike records whether the ledger commit time is server-set; a client-set time stops the cycle. The 2.3.0 docstring was read from the installed package (Sources). |
| B T6-SPEC-17 (round 3) | SHOULD FIX | Open decision 3 now states each residual on its own: D3 (option B removes it) and D2 (the upload-duration window, which stays under every option). |

Sources: `huggingface_hub` 2.3.0 `HfApi.create_commit` docstring, read from the installed package on 2026-10-31 (the
`parent_commit` text quoted in option A; the same text as
https://huggingface.co/docs/huggingface_hub/v0.11.0/en/package_reference/hf_api); HF Hub rate limits
(https://huggingface.co/docs/hub/rate-limits).
