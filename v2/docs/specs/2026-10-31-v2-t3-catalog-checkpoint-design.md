# v2 T3 — bounded catalog checkpoints and version retention at scale design

**Status:** Round 9 (2026-10-31): the spike results and decisions 16–21 amend D4, D6, D7, root-cause point 5 and
the gated decisions. A/B delta review: A APPROVE, B APPROVE WITH CHANGES (applied); the owner confirmed decisions 20
and 21 explicitly on 2026-10-31 and added decisions 22 and 23. Implementation evidence from Task 12 corrects one spike result in D4 (an HF session
whose version is deleted mid-session raises HTTP 404); it goes to the A/B implementation review. Round 8 APPROVED by the owner 2026-10-31, including owner decisions
13–15, the three points of "For the owner's confirmation (round 8)", and the T3-SPEC-21 check. Reviewers A and B both APPROVE round 8 (reports
`D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-spec-r8-{A,B}.md`); Reviewer B's CONSIDER T3-SPEC-21 is
applied in D5. Round 7 was APPROVED by the owner
2026-10-31, including owner decisions 1–12 and the D5 residual, after both reviewers approved (reports
`D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-spec-r7-{A,B}.md`). The plan's round-1 review found a gap
in D5 (plan finding B-01). Round 8 applies owner decisions 13–15: it rewrites D5, adds a spike item and gated
decision O-5, and amends the `test_read_during_run.py` lines. Three points need the owner's confirmation (see "For
the owner's confirmation (round 8)"). Earlier rounds: round 1 A APPROVE WITH CHANGES, B BLOCK; rounds 2 to 4 A
APPROVE, B APPROVE WITH CHANGES; rounds 5 to 7 both APPROVE. Carried into the plan review: the failed-marker test
asserts "no unit write after the marker store fails" (Reviewer A, round 7). Part
of the gold-standard pre-commit program (audit ledger `docs/audits/2026-10-30-v2-audit-ledger.md`, load-test
finding T3). It amends the ingestion design `docs/specs/2026-10-26-v2-ingestion-design.md` and one S1 deferral, as
listed in "Amendments".

## Intent and why

A heavy run writes a full copy of the catalog for every match. On the Mac Mini load test (2026-10-31) the lake
grew to 96 GB, of which 71 GB was catalog version history and only 17 GB was bronze data. A completed run's
cleanup later pruned 1254 versions and the lake fell to 28 GB, so the bytes were reclaimable. But the write volume
and the transient peak grow faster than the lake: both the number of commits and the size of each copy grow with
the lake. At enterprise scale (a provider onboarding, a backfill, or `reparse --all` after a parser change) this
becomes the dominant cost in storage, upload bandwidth on HF, commit latency, and cleanup time.

The owner's bar: gold standard, prepared for enterprise scale, handled properly now so that it is not a concern in
the foreseeable future.

## Owner decisions (2026-10-31)

The author recorded these from the owner's answers in the working session; the owner confirms them at the owner
review.

1. **Gold standard at enterprise scale.** Fix the cause, not the symptom, on both the local lake (Mac Mini) and the
   HF lake (catalog in the bucket through the S3 gateway).
2. **Bounded checkpoints replace per-match commits.** A crash loses at most one checkpoint interval of work, and
   the next run redoes it automatically. This replaces the §17.2 bound of "at most one match".
3. **Full design cycle.** Spike, this spec, a plan, two independent reviews of each, then the implementation.
4. **Validation split.** The Mac Mini carries the correctness and scale validation. One focused `hf_job` test
   covers the S3-gateway paths. A full HF stress re-run is not required.
5. **Batched visibility moves are in this cycle** (D3), in the shape of decisions 8 and 10.
6. **The snapshot policy is decided after the spike** (O-1). Until then §10.7 is unchanged.
7. **The three round-1 disputes are resolved in Reviewer B's favour,** on the code evidence: a pruned version object
   fails at `CatalogReader._attach` (`catalog/reader.py:94`) with an unmapped `FileNotFoundError`, not
   `StaleCatalogError` (D4); the run-end publish was outside the integrity guard (D1); `CHECKPOINT` on the DuckLake
   attach is forbidden (`catalog/working.py:8-10`, D7).
8. **D3 shape (round 1; superseded for public-to-private moves by decision 10).** On the encrypted-local path a
   visibility move is a metadata flip, so batching covers both
   directions. On the register (HF) path, private-to-public moves batch; public-to-private moves stay an immediate
   per-match barrier, exactly as today (§29.7 option (a) unchanged). A batched match's privacy window grows by at
   most **5 seconds** over today's for the same change.
9. **The keyed plain open comes forward from S1 into T3,** so the spike can test a rewrite of an encrypted catalog
   file (D7). Only the primitive comes forward; keyed relocation, the encryption migration and key rotation stay
   deferred (S1 Non-goals).
10. **The privacy direction is never batched** (round 2, supersedes decision 8 for public-to-private moves). On both
    paths a public-to-private move publishes per match, immediately, as today; only private-to-public moves batch. A
    crash inside a batch therefore never leaves a now-private match public, and the privacy direction gains no
    window, so the 5-second bound of decision 8 no longer applies.
11. **The round-3 dispute on D5 is resolved in Reviewer B's favour.** The in-flight record removes a record only after
    a sweep has examined every object it covers, lists exact keys on the native path through a post-write record, and
    bounds a pre-write-only record to the writer's lease interval. The remaining residual is stated in D5. (Round 8
    proposes to drop the per-unit records and keep the removal rule; see "For the owner's confirmation".)
12. **The round-4 dispute on the D5 anchor is resolved in Reviewer B's favour.** The interval of a pre-write-only
    record ends at the write-once acquisition marker of the first later lease generation that stored one, not at the
    generation object, which every heartbeat rewrites and pruning deletes; no later marker fails closed.
13. **The plan-round-1 gap in D5 is closed by a run anchor and a write-time lease check** (plan finding B-01). On
    the encrypted lake every DuckLake write stores a data file, also the writes outside any unit, which no per-unit
    record covers. Each lease generation stores a run anchor that covers what is stored in its interval, and a writer
    confirms its lease at its write steps after a heartbeat gap (D5).
14. **The steady state is measured, then decided** (O-5). The spike measures the files and snapshot bytes that
    unchanged matches add on the encrypted lake; the plan stops for the owner with the numbers.
15. **`tests/e2e/test_read_during_run.py` stays unchanged.** It commits through `WorkingCatalog` directly, so the
    checkpoint policy does not reach it. A new run-level test pins per-publish visibility, and the spec lines are
    amended (D1, Verification).
16. **O-1: §10.7 stays** (round 9; the owner: gold standard, "1-4 are obvious"). Snapshots add about 0.9 KB per
    match transaction.
17. **O-3: the catalog rewrite is adopted** for catalog files of at least 8 MiB, with the keyed plain open (D7).
18. **O-4: the ops tables' per-row inserts become one insert per table per write** (D6). Bronze already writes one file
    per table per match; root-cause point 5 is corrected.
19. **§10.2 on native writes:** zstd level 3 and Parquet version 2 through the catalog's DuckLake options (D6).
20. **O-5: the unchanged-match ops writes are batched** into one DuckLake transaction per checkpoint. The plan made
    this an owner stop. After the owner's "1-4 are obvious" reply, the author told the owner on 2026-10-31 that it
    was going ahead with 5 and 6 as recommended; the owner's next reply addressed only question 7. Both reviewers
    recommend it on the numbers. The owner confirms it at the final review (round-9 finding T3-SPEC-23).
    **Owner-confirmed explicitly on 2026-10-31** (together with decision 21).
21. **G's default is 2 minutes.** The longest measured reader session is 0.77 s (a cold review-page render on the 28 GB
    Mac lake); an HF page takes a few seconds. During a mass public-to-private change about G ÷ (publish time)
    versions are kept, so 2 minutes keeps about a fifth of what 10 minutes would (about 4 GB instead of 21 GB at a
    10 MB catalog and a 0.28 s publish). Recorded as for decision 20: the author told the owner it was going ahead,
    both reviewers recommend it, and the owner confirms it at the final review. **Owner-confirmed explicitly on
    2026-10-31**, after the scale test measured at most 60 retained versions in a 100-move privacy burst (bound 67).
22. **The stop grace (2026-10-31, D1):** the rendered launchd and systemd tasks set an explicit 60-second grace; an HF
    Jobs cancel or timeout (about 2 s to SIGKILL, measured) has crash semantics.
23. **HF gateway conditional writes (2026-10-31, Task 12 evidence):** the HF S3 gateway does not make create-if-absent
    atomic under concurrency and ignores the condition on a multipart upload. This is outside T3; the owner chose a
    separate hardening cycle (audit ledger T6).

### For the owner's confirmation (round 8)

The author applied decision 13 as follows. Each point changes an earlier owner decision or approval. The owner
confirmed all three on 2026-10-31.

- **The per-unit records of decision 11 are dropped.** The run anchor covers every file they covered, and the files
  outside any unit. Decision 11's removal rule and decision 12's marker stay, applied to the anchor (D5).
- **The round-7 D5 residual is closed, not restated.** The anchor intervals leave no gap, so a lost-lease writer's
  in-flight files fall in the new holder's interval (D5, "Coverage").
- **The guard covers less.** Inside a surviving anchor's interval (normally the two generations before the sweeping
  run) the guard does not count doomed objects. The key-mapping check and the doomed rule still apply, and a new
  check stops the sweep when a doomed key is in the `CURRENT` manifest (D5, "What the guard no longer covers").

## Evidence (load test, 2026-10-31)

| Measure | Mac Mini (encrypted local, native writes) | HF (unencrypted, `ducklake_add_data_files`) |
|---|---|---|
| Commits in the heavy run | 1250 (one per match) | 550 (one per match) |
| Catalog size, first and last commit | 4.5 MB → 71 MB | 6.8 MB → 20.7 MB |
| Data files registered at the end | 606,116 (578,771 in `CURRENT`) | 2,074 |
| Files written for one match | 421 (one gradientsports match, 8,437,208 rows) | about 4 |
| Lake size at the peak | 96 GB (catalog 71 GB, data 17 GB, raw 8 GB) | not measured |
| After a completed run's cleanup | 1254 versions pruned, 23 kept, lake 28 GB | — |
| Commit latency at the end | about 10 s per commit at 71 MB | not measured |
| An all-unchanged daily run | — | exactly 1 commit (the run end) |

These are run outputs from the load test, not reproducible from the tree. The daily steady state publishes little:
unchanged matches do not commit (`refresh_signals` and `listed_again` write without a commit). On the encrypted lake
each of those writes still stores data files and adds a DuckLake snapshot (`DATA_INLINING_ROW_LIMIT 0`); that growth
is not measured yet (spike, O-5). The heavy runs are the risk: an initial load, a backfill, `reparse --all`, and a
mass visibility change.

## Verified mechanism (root cause)

1. **Copy-on-commit.** `WorkingCatalog.commit()` (`catalog/working.py`) asserts the lease, detaches, reads the whole
   catalog file, stores it as a new immutable version with a manifest, verifies it, asserts the lease again, and
   replaces `CURRENT` by compare-and-swap. Each commit costs the full catalog size S.
2. **Per-match commits.** `MatchWriter.write()` commits after every match (`runs/match_writer.py`). A heavy run
   writes about N × S bytes; N and S both grow with the lake.
3. **Late pruning (a spec-conformance defect).** §10.8 keeps a run's in-run versions "only until a run-end version
   supersedes them … the run's own end, or for a crashed run the end of the next completed run".
   `plan_version_pruning` (`catalog/cleanup.py:101-119`) prunes an in-run version only when a later run-end version
   exists, and `_Run.finish()` runs cleanup before its own run-end commit. So a run's in-run versions survive until
   the next run's cleanup, and a crashed run's survive until the cleanup of the run after the next one.
4. **A DuckLake snapshot per match.** Each match's bronze swap is one DuckLake transaction, which adds a snapshot
   inside the catalog. S grows per match inside the 8-day snapshot window (§10.7), whatever the publish cadence.
5. **File multiplication on the native path (corrected in round 9 by the spike).** Round 1 inferred that DuckLake
   writes one file per arrow chunk. The spike disproved it: one `INSERT` of 8,437,208 rows in 421 chunks writes exactly
   one file, and the live Mac catalog shows one bronze file per table per insert (`skillcorner_tracking_players`: 909
   files for 909 inserts). The multiplication is in the ops tables. `executemany` runs one `INSERT` per row, and with
   `DATA_INLINING_ROW_LIMIT 0` each `INSERT` stores a file: `ops_copy_fields` alone holds 560,582 one-row files of the
   606,116 in the catalog (`runs/match_writer.py:143`; the same pattern at `runs/history.py:280` and
   `resolve/tables.py:160`). Every file is a row in the catalog's file table, so these files also drive S. The native
   writer also misses §10.2's zstd level 3 and delta encodings (DuckLake defaults: SNAPPY, PLAIN).
6. **Not measured before.** §26 item 4 asked for catalog size at scale and version-retention storage.

## Goals

- Bound the catalog publishes of a run by elapsed time, pending work and barriers, not by the number of matches.
- Prune superseded in-run versions during the run and at the run's own end, safely for open readers.
- On the encrypted-local path, store one file per table per write (bronze and ops tables) in §10.2's file format.
- Measure, and where it pays reduce, the catalog size S (snapshot growth, catalog file compaction).
- Prove the bounds with numbers: a scale test that closes §26 item 4, and one `hf_job` test over the S3 gateway.
- Make catalog health visible: bytes published, retained versions, publish reasons, guard time.

## Non-goals

- No catalog server; the publish-by-copy design (§10.4) stays.
- No change to the run-end retention (7 days) or the snapshot retention (8 days), except through O-1.
- No change to the HF register path's one-file-per-match layout (§10.2), which already conforms.
- No change to the daily steady state's single commit. Its file and snapshot growth on the encrypted lake is measured
  in the spike and decided at O-5.
- No new ops table and no bronze schema migration, unless the plan shows one is unavoidable; then it comes back to
  the owner.
- No keyed relocation, no encryption migration, no key rotation, and no keyed `prepare` in restore
  (`catalog/restore.py`): those S1 deferrals stand. Only the keyed plain open primitive comes forward (decision 9).

## Anti-goals (a failure even if the run succeeds)

- A published version names a file that was deleted.
- A match that became private stays readable as public longer than it does today (§29.7; decision 10).
- A crash or a lost lease leaves a lake that the next run cannot resume.
- A crash, a lost lease, a crash inside a D3 batch, a rolled-back transaction, or a write outside any unit, with at
  most one interval of unpublished work, makes a later run's sweep stop with the large-sweep guard
  (`sweep_max_fraction`).
- A reader sees a torn state, or gets an unmapped error because its version was pruned.
- A restore point that the spec guarantees (run-end versions for 7 days) is lost, or a restore re-publishes a
  now-private match as public.
- A publish that carries a bronze write runs without `assert_bronze_integrity`, or a broken write becomes `CURRENT`.
- Work done under a lost lease is published.
- A catalog compaction changes the content of any catalog table, or touches a data file.
- The encryption key appears in a log, an error message, or `duckdb_databases().options` output.

## Design

### D1. The checkpoint policy

A new collaborator, the checkpoint policy, decides every in-run publish. A commit site calls one of two methods:

- `maybe(reason)` for a **deferrable** site. It publishes only when the time since the last publish is at least T,
  or the units written since the last publish are at least M. Otherwise the work stays in the working catalog.
- `now(reason)` for a **barrier** site. It always publishes.

Every publish covers all pending work, because there is one working catalog; a barrier therefore also acts as a
checkpoint, and the counters reset after any publish.

- **A unit** is one call to `maybe`: one match write, one not-kept lineage record, or one listing write.
- **The clock** is the run's injected `now`, so tests control time.
- **Wiring** (author's call): the policy is injected into `MatchWriter`, `MatchFiles`, `VisibilityMover` and `_Run`
  the same way the M1 collaborators receive the error sink (`runs/_common.py`).
- **Readers see progress per publish,** not per match (decision 2). A new run-level test pins that a reader sees a
  match's write only after the publish that carries it. `tests/e2e/test_read_during_run.py` commits through
  `WorkingCatalog` directly, so the policy does not reach it, and it stays unchanged (decision 15). The code is not
  bent to publish per match.
- **dbt hand-off is unchanged:** dbt reads the working catalog at the run's end, which holds every pending write.

**The integrity guard.** `assert_bronze_integrity` runs before every publish that can carry a bronze write: each
checkpoint, each barrier, and the run-end publish, including the run-end publish of a stopped run. Today it runs
only after each write at the two deferrable sites (`runs/match_writer.py`, `_Run._listings`); the per-write calls
move to the publish. Guard failure: the guard raises `GuardStopError`, which is fatal (`runs/_common.py`). The work
since the last publish (at most one interval) is discarded and the next run redoes it; a match that fails the guard
deterministically stops that next run again. The operator signal is the `GuardStopError` in the run's errors; the
plan confirms that its message names the table and the match key, and adds them if it does not. The scale test
reports the guard time per publish.

**A stop.** A stopped run (SIGTERM, the stop flag) already ends through `_Run.finish()` and its run-end publish,
which carries all pending work. No separate stop publish is added. The plan measures the run-end publish time at
the scale-test size against the kill grace of launchd and of HF Jobs; if the publish can exceed a grace, the plan
brings it to the owner.
**Measured and decided (2026-10-31).** The scale test measured the run-end publish at 0.6–2.4 s for 1,000 matches.
launchd's own grace is "system-defined" (launchd.plist(5)), and a probe job measured about 2 s between SIGTERM and
SIGKILL on an HF Jobs cancel (HF has no setting for it). Owner decision: the rendered launchd plist (`ExitTimeOut`)
and systemd unit (`TimeoutStopSec`) set an explicit 60-second grace (`schedule/base.py` `STOP_GRACE_SECONDS`), and an
HF Jobs cancel or timeout has crash semantics: the run loses at most one checkpoint interval, which the next run
redoes after the lease expires (10 minutes plus the 2-minute takeover grace).

**Settings.** A new `checkpoint` block in `Settings`: `minutes` (T), `matches` (M), `grace_minutes` (G, D4) and
`move_batch` (B, D3), each an integer of at least 1, validated like the other blocks, with defaults in
`defaults.yaml`. A saved config without the block loads with the defaults. `lakehouse config set checkpoint.<field>
<n>` sets them. The Ops settings page (`ui/pages/settings.py`) shows the block the same way it shows `retention`
and `caps` (the same-pattern rule).

### D2. Commit-site classification

Both reviewers confirmed this table against every catalog-publish `commit(` call in `src/lakehouse`; no barrier is
classified as deferrable and no site is missing. DuckDB-transaction `conn.commit` calls are a different layer.

| Commit site | What follows the commit | Class |
|---|---|---|
| `MatchWriter.write()` (match bronze write) | nothing destructive | deferrable |
| `MatchWriter.record_not_kept()` (lineage of a match another channel kept) | nothing destructive | deferrable |
| `_Run._listings()` (source-level listing write) | nothing destructive | deferrable |
| `move_match_files()` (`catalog/moves.py`), public-to-private | the old public copies are deleted after the publish | barrier |
| `move_match_files()`, private-to-public | readers' visibility changes | barrier |
| `MatchFiles.move()` and `MatchFiles._move_native()` (`runs/match_files.py`) | readers' visibility changes | barrier |
| `_Run.fold_skips()` | the skip record is deleted after the publish | barrier |
| run end (`_Run.finish()`), `sweep_now`, restore, relocate | end of the operation | unchanged |

The rule: a publish that a destructive storage action or a privacy change depends on is a barrier; a publish of
metadata only is deferrable. The register move publishes twice per match (`moves.py`, then `match_files.py` for
`ops_matches`); D3 covers both.

### D3. Batched visibility moves (decisions 8 and 10)

**The rule.** The privacy direction is never batched (decision 10). On both paths, a public-to-private move publishes
per match, immediately, exactly as today: on the register path copy, publish, delete (§29.7 option (a), "right after
its catalog commit … no exposure window"); on the encrypted-local path flip, publish. Only private-to-public moves
batch.

**Which path batches.** A mass pining change flows through the visibility pass: `VisibilityMover._apply_flags`
(`runs/visibility.py:85-98`) calls `MatchFiles.move` once per changed match. Its private-to-public moves batch. The
module loop's per-match visibility-only move (`_Run._visibility_only`) and the move-before-replace inside
`MatchWriter.write()` stay immediate per-match barriers.

**A private-to-public batch** closes at B moves (`checkpoint.move_batch`) or at the end of the visibility pass,
whichever comes first, and then publishes once (`now`).
- Encrypted-local: each move is a metadata flip (`MatchFiles._move_native`); each flip keeps its own
  single-transaction update of `_visibility`, `ops_matches.visibility`, lineage and the visibility-change ledger, so
  `_visibility` equals `ops_matches.visibility` in every published version (the S1 plan T9 invariant).
- Register path: the batch copies its restricted files to the lake root, then publishes once with every move record
  and every match's `ops_matches` and lineage update, so the two publishes a register move makes today
  (`catalog/moves.py`, then `runs/match_files.py`) become one per batch.

**No privacy window is added.** A batched match only becomes public later than it would one by one, which exposes
nothing. A public-to-private move is unchanged from today on both paths.

**Move invariants that bind every batched publish.**
- §29.11: a move covers every lake-root bronze object of the match that the current catalog registers, that a kept
  version names, or that DuckLake still tracks; move-before-replace for a public-to-private move is unchanged; the
  orphan sweep never deletes an object that DuckLake still tracks.
- §29.12: each match keeps its own move records (old key to new key) inside the batch publish; a restore still
  works per match in two passes and checks that every named file exists before it publishes; a move record is
  pruned only when no kept version names its old key and DuckLake no longer tracks that key.
- §29.7: kept, because the privacy direction is not batched.

**A crash inside a batch.** Encrypted-local: the flips are unpublished work, so the matches stay private in
`CURRENT` until the next run's visibility pass redoes them (the safe direction). Register path: copies made before
the publish are orphans in the lake root of matches already decided public; D5 covers their sweep.

### D4. Reader-safe version pruning

**The grace.** A version that is no longer `CURRENT` is deleted only after its successor has been `CURRENT` for at
least G (`checkpoint.grace_minutes`). The grace is time-based, so a burst of barrier publishes or the run-end publish
cannot delete a version a reader opened a moment earlier. The plan derives G's default from the measured longest
reader session (page render and `CatalogReader.session()`), with a margin.

**During the run.** After each in-run publish, the policy deletes this run's in-run versions that are past the
grace. The run knows its own versions from an in-memory list of the sequences it published (author's call: no
listing of the version store per publish, which costs gateway calls on HF).

**At the run end.** After the run-end commit, still under the lease, the run calls `plan_version_pruning` and
deletes what it returns, with the grace applied. That prunes this run's in-run versions and those of a crashed
predecessor (both superseded by this run end), as §10.8 states. Versions still inside the grace go at the next
run's cleanup.

**Bound.** Pruning runs after each publish, so the bound holds right after a publish: the in-run versions retained
are then `CURRENT` plus the versions superseded within the last G. Between two publishes, a version whose grace ends
waits for the next publish to be deleted; that is at most one checkpoint interval later (T), or the next barrier or
the run end if no unit arrives. During a burst of per-match barrier publishes (for example a mass public-to-private
change, which is never batched, decision 10), about G ÷ (publish time) versions are retained, each of size S, until
their grace ends; G's default (D1 settings) is chosen with this burst in view.

**Deletes.** Only version objects and manifests are deleted. Data files stay until a cleanup decides from the
kept-version file set (§10.3 step 7), as today. A delete that a Windows reader blocks is postponed, as today
(`_delete_versions`, `catalog/cleanup.py:122-131`).

**A reader whose version is gone.** `read_catalog` (`services/catalog.py`) maps today only a DuckDB "Cannot open
file" error (a data file removed by a move). It will also handle a missing version object: a `FileNotFoundError`
from `CatalogReader._attach` (`catalog/reader.py:94`), or the error an attached version raises when its object
disappears mid-session. It re-reads `CURRENT` and retries once; a second failure raises `StaleCatalogError` (the page
shows "reload"). On macOS and Linux an attached local file stays readable after an unlink, and on Windows the delete
is postponed. The spike (round 9) measured HF on the test buckets: a session whose version object is deleted
mid-session completes (every table still reads), and a fresh attach of a deleted version raises the same
`FileNotFoundError` as the local path. So the retry handles `FileNotFoundError`, and the `hf_job` test pins both HF
outcomes.
**Implementation evidence (Task 12, 2026-10-31) corrects the spike:** on the HF test buckets, the next metadata
query of a session whose version object was deleted raised `duckdb.HTTPException` "Failed to query most recent
snapshot for DuckLake: HTTP GET error on '…/catalog/v/<seq>.ducklake' (HTTP 404 Not Found)". So the session does not
always complete (the spike's queries most likely read pages already cached). `read_catalog` therefore also maps this
error, and only this one: an `HTTPException` with HTTP 404 on a `catalog/v/<seq>.ducklake` URL retries once on the
new `CURRENT`, then raises `StaleCatalogError`. Every other HTTP error propagates unchanged. G (2 minutes) against
the longest measured reader session (0.77 s) keeps this case rare.

### D5. Crash orphans, lost leases and the large-sweep guard

After a crash, the files written since the last publish are orphans: no kept manifest names them and the next run's
catalog does not track them, so the next run's sweep deletes them. The same orphans come from four more sources:

- a lost lease (the old writer keeps writing until a write step or a publish fails `assert_held`);
- a crash inside a D3 batch;
- a rolled-back DuckLake transaction (the 2026-10-31 repro run's cleanup swept 27,345 such files);
- the writes outside any unit. On the encrypted-local path every DuckLake write stores a `data/` file, because the
  attach sets `DATA_INLINING_ROW_LIMIT 0` (`catalog/working.py:228`). That includes `refresh_signals`,
  `listed_again`, the error sink, `fold_skips`, the visibility flips, resolution and dbt. On the register path, a
  DuckLake-native ops write over the inlining limit stores one too.

On a small lake these orphans can exceed `sweep_max_fraction` and stop the run.

Required behaviour: at most one interval of unpublished work, from any of these sources, never trips the guard on the
next run or on the run after it, and a genuinely large unexpected sweep still does.

**Mechanism (owner decision 13): a run anchor per lease generation, and a write-time lease check.**

**The run anchor and the acquisition marker.** Every lease acquisition does two stores after it wins its generation
and before it returns the lease. That covers all five callers of `Lease.acquire`: the run and the re-parse
(`runs/run.py`), `sweep_now`, restore and relocate.

1. It stores the generation's **run anchor**: a write-once object that names the generation and the run id.
2. Then it stores the generation's **acquisition marker** (decision 12): a write-once object whose storage
   `modified` time is the generation's start.

If either store fails, the acquisition marks its generation released and raises, so the caller writes nothing and
exits with an error. A generation that has a marker therefore always has an anchor, and a generation without a marker
wrote nothing.
**Amended (implementation, 2026-10-31, Task 12 evidence):** an anchor or marker that already exists is not a store
failure. Only a generation's winner writes them, so another taker holds the same generation; that happened on HF,
where the gateway let two takers win one generation (audit T6). The acquisition then raises `LeaseMarkerError` and
writes nothing more. It does not mark the generation released, because that would release the other taker's lease and
let a third taker in at once. Only a store error (an exception) releases the generation.

- The marker's key lies under `runs/lease/` but must not match the generation-object pattern
  `^runs/lease/(\d{12})\.json$` (`_GENERATION_KEY`, `storage/lease.py`); a matching key would be read as a phantom
  generation and break the lease. The generation listing and `acquire` ignore markers and anchors.
- `Lease._prune` deletes a generation's marker together with the generation. It never deletes an anchor.
- The generation object cannot serve as the marker: `Lease.renew()` rewrites it every heartbeat (60 s), and
  `Lease._prune` deletes old generations.

**The interval of an anchor.** The interval of generation g's anchor starts at g's marker. It ends at the earliest
marker that any later generation stored; a later generation without a marker is skipped. Both ends are storage
`modified` times, the clock that §10.3 step 7 already uses for both roots. The intervals leave no gap: an object
stored at time t falls in the interval of the highest generation whose marker is not later than t. That holds even
when a stalled acquirer stores its marker after a later generation's marker. If no later marker exists, the interval
is open; only the sweeping run's own anchor can be in that state, and its objects are newer than its
`started_storage_time`, so the sweep dooms none of them.

**What the guard exempts.** The sweep still deletes every orphan. The large-sweep guard does not count a doomed object
whose storage `modified` time falls inside the interval of a surviving anchor. With no list of keys, that covers:

- every file a generation stored and never published, on both paths: the units, the writes outside any unit, and the
  files of a rolled-back transaction;
- a crash inside a D3 batch;
- every file of a writer that lost its lease. The files it stores before the new holder's marker fall in its own
  interval. The files it stores after that marker fall in the new holder's interval, for example a write that was
  already in flight when it lost the lease. No file of a lost-lease writer is outside every interval.

A doomed object outside every surviving interval still counts, so a genuinely large unexpected sweep still stops the
run. Examples: the files of a run whose versions a restore dropped (below), and files older than every surviving
anchor. An anchor whose generation has no marker exempts nothing (fail closed).

**When an anchor is removed.**

- **By the sweep.** At the end of a sweep, after its last delete returned, the sweep removes each anchor whose
  interval ended before the sweeping run's `started_storage_time`. Every object in that interval was stored before
  the sweep started, so this sweep or an earlier one examined it. The sweep also removes each anchor whose generation
  has no marker. A sweep that raises removes nothing (fail closed).
- The sweeping run stores its own marker after its `started_storage_time`, so a sweep never removes its predecessor's
  anchor. That anchor stays for the next sweep, which examines the files that a lost-lease predecessor stored after
  the sweeping run started.
- **By a restore.** Before it publishes, a restore removes the anchors of the runs whose versions it drops (the run
  ids in the manifests of the dropped versions). Those runs' files then count again, as today, so an anchor never
  exempts the files of a run that a restore undid. A restore that fails after the removal only makes the guard count
  more (fail closed).
- **By a relocate.** Before it publishes, relocate removes the anchors of every earlier generation. The operator
  copies the files to the new roots before the relocate (§10.5.1), so their storage times no longer show which
  generation stored them (fail closed).
- **Never by a publish.** A publish proves only that its own files are registered. It does not prove that a sweep has
  examined the files that the generation rolled back or never published.

**The write-time lease check.** A writer that lost its lease stops at its next write step, not at its next publish.
The run confirms the lease before each step that writes: at each existing stop check (each module, each match, each
listing entry), before each move of the visibility pass, and before resolution and dbt in `_Run.finish()`.

- A confirmation calls `assert_held()` only when the last passing `assert_held()` is older than one `HEARTBEAT`.
  The publish's own `assert_held()` calls count, so a run with recent publishes makes no extra call.
- On `LeaseLostError` the run stops as it does today for a lost lease, and it publishes nothing.
- The check counts a passing `assert_held()`, not the last renew. `Lease.renew()` is a plain `put` that succeeds even
  after another run took over (`storage/lease.py:237-243`), so a fresh renew proves nothing.
- The check limits a stalled writer, after it resumes, to the write that was in flight. The anchor intervals already
  cover those files, so the check limits wasted work and orphans; the guard does not depend on it.

**Coverage.** Because the intervals leave no gap, none of the sources above leaves an orphan outside a surviving
interval. The round-7 residual (a lost lease, then a crash during an `INSERT`) is closed: those files fall in the new
holder's interval.

**What the guard no longer covers (stated for the owner's review).** Inside a surviving anchor's interval, the guard
does not count doomed objects, so it would not catch a defect that dooms recent tracked files. In normal operation
those are the intervals of the two generations before the sweeping run, plus any generation that crashed since the
last completed sweep. Three checks run before the sweep deletes, whatever the anchors exempt:

- the key-mapping check, unchanged: `_sweep` stops when a key of the `CURRENT` manifest is missing from the listing
  (`catalog/cleanup.py:238-243`), which catches the mapping defect that would doom live files of any age;
- the doomed rule, unchanged: an object in a kept version, or one that DuckLake tracks, is never doomed;
- the `CURRENT`-manifest check, new (round-8 finding T3-SPEC-21): the sweep stops with `GuardStopError` and deletes
  nothing when a doomed key is named by the `CURRENT` manifest. `run_cleanup` already reads those keys
  (`catalog/cleanup.py:290-294`). The check turns a kept-set defect that would delete files the published version
  names (the first anti-goal) into a hard stop.

The guard still covers every older object, and D8 logs the exempted count of each sweep.

**Per-unit records are not needed (for the owner's confirmation; supersedes part of decision 11).** Decision 11 added
a pre-write record per unit and a post-write record that lists the native keys. The anchor intervals cover every file
those records covered, and also the files outside any unit, which they did not. This revision therefore drops the
per-unit records and the writer restructuring they need, and saves up to two storage objects per unit on HF. Decision
11's removal rule (only after a sweep has examined every covered object) and decision 12's marker stay, applied to the
anchor.

A "last published version" window cannot replace the anchor (round-2 finding T3-SPEC-12): the sweep runs in
`_Run.finish()` before the run-end commit, so such a window ends at the run's own latest checkpoint, and a lost-lease
writer's files fall outside it.

### D6. One file per table per write, and the §10.2 file format, on the native path (round 9)

**Bronze already conforms.** The spike measured exactly one file per table per match insert, whatever the arrow
chunking, and the native writer already sorts tracking tables (`prepare_tracking_table`, `bronze/writers.py:265`).
`test_native_writes_one_file_per_table` pins this as a regression test.

**Ops writes become one `INSERT` per table per write.** The three `executemany` sites (`runs/match_writer.py:143`,
`runs/history.py:280`, `resolve/tables.py:160`) insert their rows with one statement per table, from an arrow table
or a `VALUES` list, inside the same transaction as today. On the native path a write then stores at most one file per
ops table it touches. The register path benefits too: a write of more than the inlining limit (10 rows) stores one
file instead of one per row. Row content and order, the transactions and the `ops_*` schemas are unchanged.

**The §10.2 file format on native writes.** Every writing attach makes sure that the catalog's DuckLake options
`parquet_compression = zstd`, `parquet_compression_level = 3` and `parquet_version = 2` are set; it sets an option
only when it is absent or differs, so a re-attach adds no catalog change. The spike measured that these options give
zstd level 3, `DELTA_BINARY_PACKED` on the integer columns (`frame`, `x`, `y`) and dictionary encoding on the others,
as §10.2 requires. Existing files keep their format until a replace or a re-parse rewrites them.

Constraints: `ENCRYPTED` still covers every file (no cleartext, as the S1 tests check), `auto_compact` stays false
(S1), and the `_visibility` column and its atomic update are unchanged.

### D7. The catalog size S

**Snapshot growth.** The spike measured about 0.9 KB of catalog per match transaction: 20,000 matches grew the
encrypted catalog from 4.2 MB to 21.8 MB (20,005 snapshots). That is not material, so §10.7 stays (O-1).

**Catalog file compaction.** `CHECKPOINT` on the DuckLake attach is forbidden: it runs DuckLake maintenance, which
merged data files in a probe and could delete files outside the guarded cleanup of §10.3 step 7
(`catalog/working.py:8-10`). Compaction instead works on the **detached** catalog file through a plain DuckDB open,
which runs no DuckLake maintenance and touches no data file:

- unencrypted (HF): `duckdb.connect` on the file;
- encrypted-local: the keyed plain open (below).

The spike measured two operations (round 9): (a) `CHECKPOINT` on the plain open changed no catalog's size; (b) a
rewrite that copies every metadata table into a fresh file (`COPY FROM DATABASE`, encrypted with the same key on the
encrypted-local path) shrank catalogs of 9 to 22 MB by 31 to 40 % in at most 0.17 s, with equal schema, rows and
checksums, and the result re-attached with DuckLake every time. DuckDB never reclaims space inside a catalog file, so
the rewrite bounds that growth. **The rewrite is adopted (O-3); `CHECKPOINT` is not used.** Before the fresh file
replaces the old one, the rewrite verifies each table's row count and a content checksum, and that the schema is
equivalent: the same tables, columns, types, defaults, constraints, sequences and indexes, compared through
`duckdb_tables()`, `duckdb_columns()`, `duckdb_constraints()`, `duckdb_sequences()` and `duckdb_indexes()`. A
compaction runs under the lease, between the detach and the version put, for a catalog file of at least 8 MiB
(below that the bytes saved are too small to matter). A failed verification discards the fresh file and publishes
the original with a `catalog_compaction` warning. **Implementation amendments (2026-11-01, audit T13 and Reviewer B
T3-IMPL-02, T3-IMPL-04, T3-IMPL-05):** the rewrite first opens the original and reads every table in full; a
working copy that cannot be opened or read raises `IntegrityError` and nothing is published (a wrong key still raises
`ConfigError`). A rewrite-side error after that (the fresh file's attach, the copy, the checks) is handled like a failed
check: the fresh file is discarded, the original, just read in full, is published, and the warning names the error
class. A catalog sequence counts as used from its version upload on, so a later publish of the same run takes the next
one. The rewrite's DuckDB instance spills into its own folder beside the catalog file, removed
afterwards: DuckDB refuses `temp_file_encryption` while another instance has temp files in the shared default
`./.tmp`, which failed a real-data run's publish. A failed publish re-attaches the working copy, so the run can record
the error.

**The keyed plain open (decision 9).** Open an encrypted catalog file as a plain DuckDB database with the key and
`STORAGE_VERSION 'latest'`, as the S1 spikes established (S1 spec, "Verified mechanism"). The S1 key rules apply:
the key resolves through the S1 seam, is validated and masked, and never reaches a log or an error message; no code
reads `duckdb_databases().options` on a keyed connection (the S1 AST gate extends to the new site);
`temp_file_encryption` is true. The spike established the form `ATTACH '<file>' AS <alias> (ENCRYPTION_KEY '<key>',
STORAGE_VERSION 'latest')`. Only the key rejection maps to `ConfigError` (fail closed). A wrong key raises
`duckdb.InvalidInputException` "Wrong encryption key used to open the database file", but a corrupt header under the
right key raises the same class ("Computed AES tag differs from read AES tag, are you using the right key?"). So the
mapping matches the wrong-key message, not the class, and every other error propagates unchanged with its own
message. The same defect exists at the DuckLake attach: `attach_or_config_error` (`catalog/duck.py:45-57`) maps
every `InvalidInputException` under a key to "the encryption key is wrong", and its docstring wrongly says a corrupt
catalog raises `IOException`. Reviewer B reproduced it (round-9 finding T3-SPEC-22): one flipped byte in an encrypted
catalog, with the right key, raises "Computed AES tag differs", wrapped in "Failed to attach DuckLake MetaData".
T3 fixes both keyed sites with one message-based helper, and a test pins the corrupt-catalog case at the DuckLake
attach.

### D8. Observability

Each publish logs its sequence, kind, reason (`threshold_time`, `threshold_matches`, a barrier name, `run_end`),
bytes, the cumulative bytes published by the run, the retained version count, and the guard and compaction time.
The run's final record carries the run totals. A retained version count or a catalog size over a configured
threshold records an ops warning (`catalog_growth`) so the Ops pages can show it. Each sweep logs its doomed count,
the count that surviving anchors exempted, and the anchors it removed (D5).

### D9. Re-parse and other heavy paths

`reparse_matches` uses the same commit sites, so it inherits the policy, the guard and the pruning. `sweep_now`,
restore and relocate keep their single run-end commit.

## Amendments

**Ingestion design** (`docs/specs/2026-10-26-v2-ingestion-design.md`):

- **§17.2:** "Catalog commit after each match" becomes "catalog publish by the checkpoint policy (time, pending work)
  and immediately at barriers; resolution and dbt once per run". The crash-loss statement becomes "a crashed run
  loses at most one checkpoint interval of work, which the next run redoes".
- **§10.8:** in-run versions are pruned during the run and after the run-end commit, after the grace G (D4); a
  crashed run's in-run versions go at the end of the next completed run; version deletes never delete data files.
- **§10.3 step 5:** remove `CHECKPOINT` from the commit steps (the code does not run it, `catalog/working.py:8-10`);
  `DETACH` flushes the catalog file; the detached file may be compacted through a plain open (D7).
- **§10.2:** the encrypted-local native path also writes exactly one file per table per match, and its native writes
  use zstd level 3 and Parquet version 2 (delta and dictionary encodings) through the catalog's DuckLake options (D6).
- **§10.3 step 7:** the large-sweep guard does not count a doomed object inside a surviving run anchor's interval;
  the sweep removes anchors by the D5 rule.
- **§10.6:** every lease acquisition stores a run anchor and then a write-once acquisition marker before it returns
  the lease; `Lease._prune` deletes a marker with its generation; a writer confirms the lease at its write steps
  after a heartbeat gap (D5).
- **§26 item 4:** closed by the scale test (Verification).
- **§10.7 (gated on O-1):** if the owner adopts non-head snapshot expiry, §10.7 records it.

**S1 design** (`docs/specs/2026-10-30-v2-s1-encryption-at-rest-design.md`), Non-goals: the keyed plain open
primitive moves into T3 (decision 9); keyed relocation, the migration and key rotation stay deferred.

## Verification

**Unit.**
- The policy's time and count thresholds (injected clock); each D2 site calls `maybe` or `now`.
- `assert_bronze_integrity` runs before every checkpoint, barrier and run-end publish, including a stopped run's
  run-end publish; a guard failure discards only the unpublished interval.
- A lost lease publishes nothing.
- Pruning: a version inside the grace survives a burst of barrier publishes and the run-end publish; after the
  run-end commit, this run's in-run versions past the grace are gone, and so are a crashed predecessor's.
- A reader whose version object is gone retries on the new `CURRENT` once, then gets `StaleCatalogError`, never an
  unmapped error.
- Batches: on both paths a private-to-public batch publishes once for up to B moves, at B or at the end of the
  visibility pass; on the encrypted-local path `_visibility` equals `ops_matches.visibility` in that version.
- The privacy direction: on both paths every public-to-private move publishes per match, immediately (register: copy,
  publish, delete; encrypted-local: flip, publish), also inside a visibility pass that batches private-to-public
  moves.
- Move invariants under batching: on each path, a public-to-private move and a batched private-to-public move,
  followed by a restore to a version from before the batch, leave every match with its latest visibility, every
  named file present, and no now-private match published as public (§29.11, §29.12).
- The keyed plain open: a wrong key raises `ConfigError` with no key in the message, and a non-key failure (for
  example a corrupt file) keeps its own error; the key never reaches a log; the AST gate covers the new site; a
  compaction keeps every metadata table's row count, checksum and schema, the result still opens with the DuckLake
  attach, and no data file changes.

**Crash, lost lease, batch crash and resume.** For each cause, with at most M unpublished units, on the register and
the native path: the next run redoes exactly that work, sweeps the orphans, and does not trip the large-sweep guard,
on a small lake and on a large one, including when that next run publishes checkpoints before its sweep. The run
after next does not trip it either.

- **Each source of D5 has its own case on the encrypted-local path:** a crash after many unchanged matches
  (`refresh_signals`, `listed_again`), after error-sink writes, inside a native flip batch, during resolution, during
  dbt, and after a rolled-back unit transaction.
- **Lost lease:** an old writer's files stored after the new holder's marker (an in-flight write) fall in the new
  holder's interval and trip no guard at the next sweep or the one after it, also when the old writer then crashes
  (the round-7 residual).
- **The anchor and the marker,** each rule with its own test:
  - an anchor or marker store failure, for each of the five `Lease.acquire` callers, marks the generation released,
    the caller writes nothing (the test goes red if any write follows the failed store), and the CLI exits non-zero;
  - a generation with an anchor and no marker exempts nothing, and the next sweep removes its anchor; the
    predecessor's interval runs on to the next marker;
  - the interval ends at the earliest later marker: a stalled acquirer whose marker is stored after a later
    generation's marker leaves no gap;
  - the marker, not the generation object, ends the interval: the test renews the later generation's lease object
    (heartbeat) past the interval and shows the interval still ends at the marker;
  - the marker key does not match `_GENERATION_KEY`; the generation listing and `acquire` ignore markers and anchors;
    `Lease._prune` deletes a generation's marker with the generation and never deletes an anchor.
- **Anchor removal:** the sweep removes an anchor only after its last delete returned, and only when the interval
  ended before the sweeping run's `started_storage_time`; a sweep that raises removes no anchor; a sweep never removes
  its predecessor's anchor. A restore removes the anchors of the runs whose versions it drops before it publishes,
  and those runs' files count again at the next sweep. A relocate removes the anchors of every earlier generation
  before it publishes.
- **The guard still trips** on doomed objects outside every surviving interval, for example files older than every
  surviving anchor. A doomed key that the `CURRENT` manifest names stops the sweep before any delete, also when a
  surviving anchor's interval covers it.
- **The write-time lease check:** a writer whose last passing `assert_held()` is older than one `HEARTBEAT` calls it
  at its next write step and stops on `LeaseLostError` before that step writes; a renew after a takeover does not
  count as a confirmation; a run whose last passing `assert_held()` is recent makes no extra call.

**Existing end-to-end tests stay green:** `tests/e2e/test_crash_recovery.py`, `test_read_during_run.py` (unchanged,
decision 15), `test_cleanup_multirun.py`, `test_run_local.py`, and the restore and relocate tests. A new run-level
test pins per-publish visibility: a reader sees a match's write only after the publish that carries it.

**Native file count:** a gradientsports-scale arrow table with many chunks writes exactly one file per table per
match, still encrypted (a regression pin). On the native path a match write, a listing write and resolution each
store at most one file per ops table they touch, whatever their row count; the register path's ops writes over the
inlining limit store one file per table too. A writing attach leaves the catalog's DuckLake options at
`parquet_compression = zstd`, `parquet_compression_level = 3` and `parquet_version = 2`, and a re-attach that finds
them set changes nothing in the catalog.

**Spike (done, round 9; results in D4, D6, D7 and the gated decisions):** snapshot growth at 20,000 matches (O-1);
compaction benefit, unencrypted and keyed (O-3); the native one-file mechanism and the rest of §10.2 (O-4); the HF
error for an attached version deleted mid-session (D4); the longest reader session (G); the exact error that a wrong
key raises on a keyed plain open (D7); the steady-state cost on the encrypted lake, as data files and snapshot bytes
added per unchanged match (`refresh_signals`, `listed_again`) over a daily run of about 1,250 unchanged matches (O-5).

**Scale test (closes §26 item 4), on the Mac Mini,** marked out of the default CI suite:
- Size: at least 1,000 matches across all tables (the §26 number).
- Bounds asserted: publishes ≤ ⌈run time / T⌉ + ⌈U / M⌉ + barriers + 1, where U is the units; right after each
  publish and its pruning, the retained in-run versions ≤ 1 + the versions superseded within the last G; after the
  run-end pruning, none superseded longer than G ago; during a burst of barrier publishes, the retained count stays
  within the D4 burst bound; bytes published per run ≤ that publish bound × the largest published copy of the run.
- §26 item 4, part by part: catalog size and attach time at 1,000 matches across all tables; the catalog growth after
  replaces (re-ingest a subset), with and without the D7 plain-open compaction (`CHECKPOINT` on the DuckLake attach is
  forbidden, so the plain-open measurement answers §26's "effect of `CHECKPOINT`"); the storage that version
  retention uses; the total size of native files and of inlined data.
- Also reported: the guard time per publish, and the run-end publish time against the kill grace (D1).

**HF.** One `hf_job` test on the throwaway test buckets (Task 33 isolation, `LAKEHOUSE_HF_TEST_LAKE=1`): a checkpoint
publish and in-run pruning over the S3 gateway, and a reader whose attached version is deleted mid-session gets
`StaleCatalogError` or completes, never an unmapped error.

**Real data, on the Mac Mini:** a heavy run on a throwaway copy of the lake records the peak catalog bytes, the
publish count, the retained versions and the wall time, against the 2026-10-31 numbers above.

## Review focus

1. A barrier classified as deferrable: a published version names a deleted file, or a privacy change waits.
2. A reader on a pruned version: barrier bursts, the run end, the Windows lock path and the HF mid-session path.
3. Orphans from any D5 source tripping the large-sweep guard on a small lake, including the writes outside any unit
   and a lost-lease writer's in-flight write; and the reverse, an anchor that exempts files it must not (a run that a
   restore undid, an interval that a sweep has not fully examined).
4. Batched moves: the privacy direction is never batched, and the §29.11 and §29.12 invariants hold through a
   restore.
5. The keyed plain open: no key in logs, errors or `duckdb_databases().options`.

## Gated owner decisions

All four were decided on 2026-10-31 after the spike (decisions 16–19); the text below keeps each question and adds
its outcome.

- **O-1. Snapshot policy (decision 6).** If the spike shows that per-match snapshots make S grow materially within the
  8-day window at enterprise scale, the option is to expire, at each cleanup, the snapshots that are not the head
  snapshot of a kept version. **Outcome: §10.7 stays** (about 0.9 KB per match transaction; D7).
- **O-3. Compaction benefit.** **Outcome: the rewrite is adopted** for catalog files of at least 8 MiB, with the keyed
  plain open (31 to 40 % smaller in at most 0.17 s; D7). `CHECKPOINT` gave no benefit and is not used.
- **O-4. Native one-file rule.** **Outcome: bronze already writes one file per table per match**; the multiplication
  is the ops tables' per-row inserts, fixed in D6 together with the §10.2 file format of native writes.
- **O-5. Steady state on the encrypted lake (decision 14).** Each all-unchanged run adds 3.0 data files and 4.0
  snapshots per match (1,250 matches: 3,752 files and 5,002 snapshots per run). **Outcome: batch** the unchanged-match
  ops writes into one DuckLake transaction per checkpoint, inside T3, without changing S1's inlining decision.

## Left to the builder

- The location and format of the D5 run anchor (write-once; it names the generation and the run id; its key does not
  match `_GENERATION_KEY`), and the form of D6's one-statement ops insert (arrow or `VALUES`).
- The log field names for D8, and the thresholds of the `catalog_growth` warning.
- The default of B. G's default is 2 minutes (decision 21).

## Author's calls (open to the owner's review)

- Defaults: T = 5 minutes, M = 50 units.
- The integrity guard runs per publish rather than per write, with the stated failure outcome.
- A stop relies on the existing run-end publish.
- The policy is wired like the M1 collaborators; the run tracks its own published sequences in memory.
- D5: every lease acquisition stores the anchor and the marker, so all five callers behave the same; the anchor is
  stored before the marker, so a generation with a marker always has an anchor; an interval ends at the earliest
  later marker; the lease check runs at most once per `HEARTBEAT`; a restore and a relocate remove anchors before
  they publish.
- The Ops settings page shows the `checkpoint` block like `retention` and `caps`.
- Finding ID T3 in a new load-test section of the audit ledger, with T1 and T2 (both done).

## Round-1 review disposition

| Finding | Severity | Disposition |
|---|---|---|
| A T3-SPEC-01 | SHOULD FIX | D3 names the §29.7, §29.11 and §29.12 invariants; Verification adds the batched public-to-private-to-public plus restore test. |
| A T3-SPEC-02 | CONSIDER | The exposure-window rule is cited as §29.7 throughout. |
| A T3-SPEC-03 | CONSIDER | Amendments lists §10.7 as gated on O-1. |
| A T3-SPEC-04 | CONSIDER | The scale test reports the native-versus-inlined sizes. |
| B T3-SPEC-01 | BLOCKING | D4 defines the time-based grace, the run-end pruning with the grace, and the reader retry-then-`StaleCatalogError` for a missing version object; local and `hf_job` tests. Dispute resolved for B (decision 7). |
| B T3-SPEC-02 | BLOCKING | D3 reshaped by decision 8 in round 1, then by decision 10 in round 2: the privacy direction is never batched on either path, so no window is added; only private-to-public moves batch. |
| B T3-SPEC-03 | BLOCKING | D1: the guard runs before every publish that can carry a bronze write, the run end and a stopped run's run end included; unit test. Dispute resolved for B (decision 7). |
| B T3-SPEC-04 | BLOCKING | D7 forbids `CHECKPOINT` on the DuckLake attach and compacts the detached file through a plain open; the keyed plain open comes forward by decision 9 with the S1 key rules; §10.3 step 5 amended. Dispute resolved for B (decision 7). |
| B T3-SPEC-05 | SHOULD FIX | The requirement and the tests cover the lost lease and the batch crash. Round 2 (T3-SPEC-12) then replaced the two candidates with one mechanism, the durable in-flight record, on storage times only. |
| B T3-SPEC-06 | SHOULD FIX | D4 runs `plan_version_pruning` after the run-end commit, so a crashed predecessor's versions go at the next completed run's end. |
| B T3-SPEC-07 | SHOULD FIX | The scale test pins the size (at least 1,000 matches across all tables), the numeric bounds, and maps each part of §26 item 4. |
| B T3-SPEC-08 | SHOULD FIX | D6 and the §10.2 amendment both say exactly one file; O-4 gates the case where DuckLake cannot. |
| B T3-SPEC-09 | CONSIDER | D1 states the guard-failure outcome and the operator signal; the stop relies on the run-end publish; the plan measures it against the kill grace. |
| B T3-SPEC-10 | CONSIDER | The Ops settings page shows the block; D3 names the visibility pass as the batched path. |

## Round-2 review disposition

Reviewer A: APPROVE, no findings. Reviewer B: APPROVE WITH CHANGES.

| Finding | Severity | Disposition |
|---|---|---|
| B T3-SPEC-11 | SHOULD FIX | Moot by owner decision 10: the privacy direction is never batched, so no time bound is needed and a crash inside a batch leaves no now-private match public. A private-to-public batch closes at B or at the end of the visibility pass. |
| B T3-SPEC-12 | SHOULD FIX | D5 drops candidate (b) and specifies the durable in-flight record: keys listed where known (register), anchored by the record's storage time where not (native); covers the crash, the lost lease and the batch crash. The test includes a next run that publishes checkpoints before its sweep. |
| B T3-SPEC-13 | SHOULD FIX | D4 states the bound the mechanism gives (it holds right after a publish; between publishes a version past its grace waits for the next publish) and the burst case (about G ÷ publish time versions); the scale-test assertion is aligned. |
| B T3-SPEC-14 | CONSIDER | D7's compaction proof adds schema equivalence; the keyed plain open maps only the key rejection to `ConfigError`. Note: S1-IMPL-17 is resolved in the current code (`catalog/duck.py:45-57` catches only `duckdb.InvalidInputException` under a key, and its docstring cites S1-IMPL-08/17); the new site follows the same narrow mapping. |

## Round-3 review disposition

Reviewer A: APPROVE, no findings (judged the D5 record sound). Reviewer B: APPROVE WITH CHANGES. The owner resolved
the D5 disagreement in B's favour (decision 11).

| Finding | Severity | Disposition |
|---|---|---|
| B T3-SPEC-15 | SHOULD FIX | D5: a record is removed only once every object it covers is older than the sweeping run's start (or gone); the native path adds a post-write record listing exact keys; a pre-write-only native record covers only the interval before the next lease generation began. Verification adds the run after next and the restore-undone case. The one remaining residual (a lost lease, then a crash during an `INSERT`) is stated in D5, with its bound and its recovery. |
| B T3-SPEC-16 | CONSIDER | Decision 5 now reads "decisions 8 and 10". |

## Round-4 review disposition

Reviewer A: APPROVE (judged the generation-object anchor correct). Reviewer B: APPROVE WITH CHANGES. The owner
resolved the anchor disagreement in B's favour (decision 12).

| Finding | Severity | Disposition |
|---|---|---|
| B T3-SPEC-17 | SHOULD FIX | D5: the interval of a pre-write-only native record ends at a write-once acquisition marker that the next generation's run stores right after it wins the lease; the generation object is rejected because `Lease.renew()` rewrites it every heartbeat and `Lease._prune` deletes it. A missing marker fails closed (only listed keys are exempted). Verification adds a heartbeat that outlasts the covered interval and the missing-marker case. |
| B T3-SPEC-18 | CONSIDER | The residual states that about five files holds only if D6 achieves one file per table, and that the residual adds to the ordinary doomed count, so it can tip a sweep already near `sweep_max_fraction`. |

## Round-5 review disposition

Both reviewers: APPROVE, each with one CONSIDER (both numbered T3-SPEC-19 in their own reports).

| Finding | Severity | Disposition |
|---|---|---|
| A T3-SPEC-19 | CONSIDER | D5: the marker key must not match `^runs/lease/(\d{12})\.json$` (a match would be a phantom generation); `Lease._prune` deletes a generation's marker with the generation. |
| B T3-SPEC-19 | CONSIDER | D5: a run stores its marker before any unit write, and a failed marker store stops the run before any write. The interval ends at the first later generation that stored a marker; a generation without one (abandoned during acquisition, `storage/lease.py:212-218`, or whose store failed) wrote nothing, so skipping it is safe. No later marker → fail closed. |

## Round-6 review disposition

Both reviewers: APPROVE, each with one CONSIDER (both numbered T3-SPEC-20 in their own reports). Both change only the
Verification text.

| Finding | Severity | Disposition |
|---|---|---|
| A T3-SPEC-20 | CONSIDER | The Verification bullet now reads "when no later marker exists, the guard fails closed", matching decision 12 and D5, and adds the test that a marker-less intermediate generation is skipped. |
| B T3-SPEC-20 | CONSIDER | Verification names a test for each marker rule: a failed marker store stops the run before any write (red if a write precedes the marker); a marker-less generation is skipped; the marker key does not match `_GENERATION_KEY`; `Lease._prune` deletes the marker. |

## Plan round-1 findings that changed the spec (round 8)

Plan review r1: Reviewer A APPROVE WITH CHANGES, Reviewer B BLOCK (reports
`D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-plan-{A,B}.md`). Only the findings below change the
spec; the other findings change the plan (plan round 2).

| Finding | Severity | Disposition |
|---|---|---|
| B T3-PLAN-01 | BLOCKING | D5 rewritten by owner decision 13: a run anchor per lease generation (stored before the marker, at every acquisition) with gap-free intervals, and a write-time lease check. The writes outside any unit (each one B lists), rolled-back files and a lost-lease writer's in-flight write are covered, whatever their count; the round-7 residual is closed. The per-unit records, and with them `native_snapshot_keys` and the snapshot-id question, are dropped, for the owner's confirmation. Verification names B's three small-lake crash cases. |
| B T3-PLAN-01 (author, steady state) | — | The author found that the same writes make a steady-state growth on the encrypted lake (files and snapshots per unchanged match). Owner decision 14: a spike item and gated decision O-5. |
| A T3-PLAN-03, B T3-PLAN-07 | CONSIDER | Owner decision 15: `test_read_during_run.py` stays unchanged; a new run-level test pins per-publish visibility; D1, Verification and the author's calls amended. |

## Round-8 review disposition

Both reviewers: APPROVE. Reviewer A: no finding. Reviewer B: one CONSIDER. Both found the three owner-confirmation
claims correct as stated; accepting them is the owner's decision.

| Finding | Severity | Disposition |
|---|---|---|
| B T3-SPEC-21 | CONSIDER | Applied. D5 adds a hard check: the sweep stops with `GuardStopError`, deleting nothing, when a doomed key is named by the `CURRENT` manifest. It narrows the exposure of point (c). One Verification line added. |

## Round-9 change (spike results)

No review finding triggered round 9; the plan's spike (Task 1) did. The spike disproved root-cause point 5: bronze
already writes one file per table per match, and the ops tables' per-row inserts made 560,582 of the 606,116 files.
D6 is retargeted at those inserts and at §10.2's file format on native writes; D4, D7 and the gated decisions record
the measured outcomes (decisions 16–21). Spike scripts and raw numbers are in the execution ledger, not the repo.

## Round-9 review disposition

Reviewer A: APPROVE (one CONSIDER). Reviewer B: APPROVE WITH CHANGES (two SHOULD FIX). A and B both report the
existing site's defect (T3-SPEC-22); A rates it out of T3's scope, B asks for the fix in T3. Under the owner's
standing "gold standard, scope is not an issue" directive the author fixes it in T3 and tells the owner.

| Finding | Severity | Disposition |
|---|---|---|
| B T3-SPEC-22, A T3-SPEC-22 | SHOULD FIX / CONSIDER | Fixed in T3: one message-based helper serves the keyed plain open and `attach_or_config_error`; the wrong docstring is corrected; a DuckLake-attach corrupt-catalog test (D7, plan Task 10). |
| B T3-SPEC-23 | SHOULD FIX | Decisions 20 and 21 now record what happened: the author told the owner it was going ahead, the owner's reply addressed only question 7, and the owner confirms both at the final review. |
