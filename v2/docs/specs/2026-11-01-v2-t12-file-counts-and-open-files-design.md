# v2 T12 — Bounded data-file counts and open-file limits design

**Status:** Round 6 (2026-11-01): design change, merge-deletion mechanism only (new D3.1). D1, D2, the merge and
rewrite steps, the slice and the budget are unchanged and stay approved. The Mac gate found the rounds 1–3
merge-deletion premise false (see "Dispositions (round 4)"); a round-4 probe showed the merge removes its replaced
files' `ducklake_data_file` rows and schedules them, so neither expiry+cleanup nor the orphan sweep is a safe owner on
its own. **The owner chose option (b) on 2026-11-01: the cleanup (`run_cleanup` step 6) defers a scheduled file a kept
version still names and deletes it, through the unguarded step-6 loop, once no kept version does — no orphan sweep, no
`sweep_max_fraction` guard on merge artifacts, no operator `--allow-large` for the backlog.** An option-(b) probe
proved the mechanic. **Round-4 review:** A APPROVE (2 CONSIDER), B REQUEST CHANGES (1 BLOCKING, 2 SHOULD FIX, 2
CONSIDER). **Round-5 review:** A REQUEST CHANGES (1 SHOULD FIX, 1 CONSIDER), B REQUEST CHANGES (1 BLOCKING — B-16, the
orphan-sweep fraction guard — 1 SHOULD FIX, 2 CONSIDER); B-16 moved the design to option (b). **Round-6 review:** A
APPROVE (2 CONSIDER), B APPROVE WITH CHANGES (3 SHOULD FIX, 2 CONSIDER), B waiving a further round; reports
`...-spec-r{4,5,6}-{A,B}.md`. All round-4/5/6 findings are answered and applied (see "Dispositions (round 4)", "(round
5)" and "(round 6)"); no further review round is required. **Owner-approved round 6 on 2026-11-01; implementation
proceeds through the plan (its own A/B re-review and owner approval).**

**Post-round-6 amendment (2026-11-01):** the step-6 crash idempotency (D3.1, Crash-safety, Observability) was added
after the round-6 approval, from Reviewer B's round-6 crash note (which B flagged outside that round); it narrows the
T3 `IntegrityError` fence for one case (a `to_delete` scheduled file already missing that names no move record is
treated as deleted and counted, not fatal). On the owner's decision it had a targeted spec re-review, **round 7
(Reviewer B only): APPROVE WITH CHANGES** (2 SHOULD FIX, 2 CONSIDER; `...-spec-r7-B.md`), all applied (see
"Dispositions (round 7)").

**Addition (2026-11-01): `missing_deferred` detection (T12-SPEC-28).** On the owner's decision to cover Reviewer B's
round-7 CONSIDER in T12, step 6 also detects a missing `to_defer` file (a broken restore point) by set membership
against the storage listing and counts it as `missing_deferred`, without deleting or un-scheduling it (D3.1,
Observability, Tests; plan Task 4b). It had a targeted A/B re-review, **spec round 8: A APPROVE (1 CONSIDER), B APPROVE
WITH CHANGES (3 SHOULD FIX, 3 CONSIDER)** (`...-spec-r8-{A,B}.md`), all applied (see "Dispositions (round 8)"); the one
owner scope call (the restore-time warning, T12-SPEC-33) was decided warn-and-allow and is applied (D3.1, plan Task 4c).

**Round 3 (2026-11-01):** Reviewers A and B APPROVE (`...-spec-r3-{A,B}.md`; B's two CONSIDERs applied
after, see "Dispositions (round 3)"). **Owner 2026-11-01: spec approved; the rewrite threshold is 0.0.** Round 2: Reviewer A REQUEST CHANGES (1 SHOULD FIX), Reviewer B APPROVE WITH
CHANGES (2 SHOULD FIX, 1 CONSIDER); reports `...-spec-r2-{A,B}.md`, answered in "Dispositions (round 2)". Round 1: Reviewer A APPROVE (2 CONSIDER), Reviewer B REQUEST CHANGES (1 BLOCKING,
2 SHOULD FIX, 2 CONSIDER); reports `D:\Development\_reviews\2026-11-01-v2-t12-file-counts-and-open-files-spec-{A,B}.md`.
Every finding is answered in "Dispositions" at the end. The owner decided the rewrite threshold (D3) on 2026-11-01.
**Owner decision 2026-11-01:** a full SDD cycle (spec, A/B,
owner, plan, A/B, implementation, A/B) with both parts: bounded data-file counts by merging small files during
run-end maintenance, within the T3 retention and manifest rules and including the live lake's backlog; and a process
that raises its own open-file limit at start, with the launchd and systemd tasks setting it explicitly. The scheduled
Mac run is left to fail safely until T12 lands (owner). It rides the gold-standard pre-commit program (audit ledger
`docs/audits/2026-10-30-v2-audit-ledger.md`, finding T12) and amends the ingestion design
`docs/specs/2026-10-26-v2-ingestion-design.md` §10.2, §10.3 and §10.8, ADR-001 and `docs/ops/deploy.md`, as listed in
"Amendments".

## Intent and why

A v2 run must not fail because of how many files the lake holds, on any supported platform, and a read must not get
slower without bound as the lake grows. Today both fail:

- Every insert into an ops table (`ops_*`, lineage, the resolution tables) writes one DuckLake data file, and v2
  never merges data files: `CHECKPOINT`, which would run DuckLake's compaction, is forbidden on the DuckLake attach
  (ADR-001), and nothing else merges. The file count of each such table grows with every match and every run.
- One DuckDB scan of a DuckLake table holds an open descriptor for many of its files at once. Under the default macOS
  soft limit of 256 open files, which the installed launchd task gets, a run fails as soon as one scanned table holds
  more than about 200 files.

The owner wants both fixed by design: file counts that stay bounded as the lake grows, and a process that does not
depend on its parent shell or service manager for a workable open-file limit.

## Evidence (2026-11-01, Mac Mini)

| Probe | Result |
|---|---|
| T3 Task 14 conditions (4 pining modules, fresh encrypted lake) under `.venv/bin/python` (soft limit 256), twice | both failed after about 7 minutes: `IOException: Cannot open file ".../ops_lineage/ducklake-....parquet": Too many open files`, in `read_lineage` (`_count_excluded`), after `statsbomb_pining` |
| The same under `uv run` (T3 Task 14, 2026-11-01) | passed: `uv run` raises the soft limit to 1,048,576 for its child (measured: `(1048576, unlimited)` against `(256, unlimited)`) |
| One scan of a DuckLake table of N one-row files (`DATA_INLINING_ROW_LIMIT 0`, as v2's encrypted attach), peak descriptors polled every millisecond (a lower bound) | N = 100: 163 (plain), 168 (encrypted); N = 300: 478 / 369; N = 2,000: 477; N = 10,000: 834; N = 2,000 with 8 threads: 715; N = 300 with 1 thread: 185 |
| The same scan at soft limit 256 | N = 300 fails with the error above, plain and encrypted |
| `enable_external_file_cache = false` or `enable_object_cache = false` | no effect (558 and 544 for N = 2,000) |
| Files per table in the T11 throwaway lake (T3 code) after 713 matches | `ops_lineage` 2,827; every other ops table 713; native bronze tables 1 per match |
| Files per table in the live Mac lake (loaded with the pre-T3 code) | `ops_copy_fields` 560,582; `ops_entity_records` and `ops_canonical_pointers` 15,909; `ops_match_files` 8,041; `ops_lineage` 4,867 |
| `ducklake_merge_adjacent_files` on an ops-like table of 40 files and a table with `auto_compact = false` of 50 files | the ops table became 1 file; the `auto_compact = false` table kept its 50 files; the same on an encrypted attach; the replaced files stayed on disk until `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` (**round-4 correction:** this row only counted on-disk files; the round-4 probe shows the merge removes the replaced files' `ducklake_data_file` rows and schedules them at once, so `expire`+`cleanup` removes them only because they are scheduled — see D3.1) |
| Merge of N one-row encrypted files (memory limit 3 GB) | N = 20,000: 5.9 s, peak 457 descriptors, peak footprint 3.0 GB, scan 0.56-1.0 s before and 0.003 s after; N = 100,000: 203 s, 498 descriptors, 3.6 GB |
| `max_compacted_files` = 1, 100 or 5,000 | no bound: every call merged all N files into one |
| Merge of files that carry delete files (Reviewer B spike: 40 ten-row files + 20 single-row `UPDATE`s) | `files 60 with deletes 20` became `files 21 with deletes 20`: the merge skips every file that has a delete file |
| `ducklake_rewrite_data_files` (same spike) | the default threshold changed nothing; `delete_threshold => 0.0` gave 2 files, then the merge 1 file, rows equal |
| An ops-lineage-like table, 5 simulated runs (50 new one-row files per run, an `UPDATE` of 80 % of the rows per run, encrypted) | merge only: 1, 1, 2, 2, 3 files after the runs, with 0, 0, 1, 1, 2 delete files (one more per run); `rewrite_data_files(delete_threshold => 0.0)` then the merge: 1 file after every run, 47-78 ms of maintenance per run |
| Deferred deletion of the rewrite (Reviewer A round 2): 20 ten-row files, an `UPDATE` of 80 % of the rows, `rewrite_data_files(delete_threshold => 0.0)`, plain and encrypted | every replaced data and delete file stayed on disk after the rewrite (41 of 41); time travel to the pre-rewrite snapshot still read all 200 rows; only `ducklake_expire_snapshots` plus `ducklake_cleanup_old_files` removed them (the one file left is the live file the `UPDATE` wrote) |
| Round-4 row-tracking probe (Reviewer B-11/B-12/B-14; plain and encrypted; `t12_rowtrack_probe.py`): merge, then inspect `ducklake_data_file` and `ducklake_files_scheduled_for_deletion` | a 20→1 merge leaves `ducklake_data_file` at (live 1, ended 0) and schedules 20 rows; after deleting those scheduled rows, `expire(all)` + `cleanup_all => true` lists 0 and the 20 files stay on disk (orphan-sweep only). `ducklake_rewrite_data_files(delete_threshold => 0.0)` schedules 0 and keeps 20 ended `ducklake_data_file` rows. Scheduled-table columns: `data_file_id`, `path`, `path_is_relative`, `schedule_start` |
| Bytes per ops table in the live Mac lake (`du`, before any merge) | `ops_copy_fields` 2.2 GB (560,582 files), `ops_entity_records` 124 MB, `ops_canonical_pointers` 62 MB, `ops_match_files` 31 MB, `ops_lineage` 19 MB, every other ops table below 20 MB |

**Partial deletes are normal in the ops tables (Reviewer B).** v2 updates or deletes ops rows every run: the lineage
write marks earlier rows `kept = false`, `flush_unchanged` updates every listed-again match, and `runs/history.py`
deletes rows it replaces. Each such statement adds delete files, which the merge alone never clears.

The probe scripts are in the session scratchpads beside the logs: rounds 1–3 `t11/fd_scan_probe.py`,
`t12/merge_probe.py`, `t12/merge_scale.py`, `t12/rewrite_deletion_probe.py`; round 4 `t12_rowtrack_probe.py` (the
merge/rewrite row-tracking probe); round 5 `t12_optionb_probe.py` (the option-(b) feasibility probe).

## Goals

- **G1.** A v2 process (CLI, web app, scheduled task, HF Job) starts with an open-file soft limit high enough for the
  lake, whatever its parent set, on macOS and Linux; it raises the limit itself, up to the hard limit.
- **G2.** A command that opens a catalog refuses to start, with a clear error, when the limit it ends up with is too
  low to work.
- **G3.** The data-file count of every table that DuckLake manages and v2 may merge stays bounded: each run's
  maintenance brings it back to a small number, however many runs and matches came before.
- **G4.** The merge keeps every T3 guarantee: the single writer, crash safety, the published-version rules, the
  retention invariant `snapshot_days > version_days` (§10.7), the guarded deletion path and the reader-mid-run
  guarantee; and every S1 guarantee: no cleartext on an encrypted lake.
- **G5.** The live lake's backlog drains through the same mechanism, in bounded time per run.

## Non-goals

- Merging bronze tables. They keep `auto_compact = false` (§10.2, ADR-001, S1): DuckLake's rewrite makes tracking
  files 2.7 to 3.5 times larger, and register-path bronze files are one file per match per table by design (file keys,
  moves, `ops_match_files`). Their file count is the match count, which the open-file limit covers (G1).
- Changing how DuckDB opens files during a scan.
- The T11 memory investigation (separate finding).

## Anti-goals

- Lowering a limit that is already higher (for example `uv run`'s 1,048,576).
- Any merge that touches a register-path bronze file, or a merge that runs when that cannot be ruled out.
- A merge whose run time grows without bound with the backlog.
- A new deletion path for data files: a rewritten or merged file leaves only through the existing cleanup
  (`run_cleanup` step 6). Candidate A option (b) (D3.1) defers a scheduled file a kept version still names, within that
  same cleanup, and adds no new deletion path.

## Decisions

### D1. The process raises its own open-file limit

A new stdlib-only module `lakehouse/limits.py`:

- `OPEN_FILES_TARGET: Final = 65_536` and `OPEN_FILES_MINIMUM: Final = 4_096`.
- `raise_open_file_limit() -> OpenFileLimit | None` with `OpenFileLimit(soft_before, soft_after, hard)`:
  - On Windows it returns `None`: there is no POSIX descriptor limit, and DuckDB opens files as Win32 handles.
  - It reads `(soft, hard)` with `resource.getrlimit(RLIMIT_NOFILE)`. When `soft >= OPEN_FILES_TARGET` it changes
    nothing.
  - Otherwise the new soft limit is `OPEN_FILES_TARGET`, capped by `hard` when `hard` is finite, and on macOS also by
    `kern.maxfilesperproc` (read with `sysctlbyname` through `ctypes`; macOS refuses a soft limit above it with
    `EINVAL`).
  - It sets the new soft limit and keeps the hard limit.
  - **Fallback (Reviewer B T12-SPEC-05):** when `sysctlbyname` cannot be called or fails, or `setrlimit` refuses the
    value, it halves the value and tries again, down to the current soft limit, and logs which value the kernel
    accepted. It never lowers the limit and never raises an exception; the minimum check below decides whether the command runs.
  - It logs one JSON line, `open-file limit`, with the three values.
- It runs first in every entry point: `lakehouse.cli.main`, `lakehouse.web.__main__`, `lakehouse.mask.main` (the HF
  Job wrapper; its child inherits the limit) and the Windows task runner (where it is a no-op). An AST test
  (Reviewer B T12-SPEC-03) checks that each of these functions calls `raise_open_file_limit` as its first statement,
  and that the list of entry points matches `[project.scripts]` and every `__main__` module.
- **Fail fast (G2):** every CLI command that opens a catalog (the plan lists them from `cli.py`; today at least
  `init`, `run`, `reparse`, `restore`, `relocate`, `sweep` and `verify`) and the web app's start raise `ConfigError`
  (exit 2) when the soft limit after the raise is below `OPEN_FILES_MINIMUM`. The message names the limit, the minimum
  and the fix (`ulimit -n`, launchd `NumberOfFiles`, systemd `LimitNOFILE`). Commands that open no catalog do not
  check.
- **Why these values:** one scan of 10,000 files held 834 descriptors; dbt runs 4 threads and the web app serves
  several readers at once, so 4,096 is the smallest limit with a safety factor of about 5 at today's scale. 65,536 is
  the target because it is far above any measured need. On the Mac Mini (macOS 26.2, arm64) `kern.maxfilesperproc` is
  92,160, so the target is accepted there; on a host whose hard limit or per-process maximum is lower, the cap applies
  and the minimum check decides.

### D2. The scheduled tasks set the limit explicitly

- launchd (`schedule/launchd.py`): the plist gets `SoftResourceLimits` `{NumberOfFiles: 65536}` and no
  `HardResourceLimits`, so launchd's unlimited hard limit stays (Reviewer B T12-SPEC-04).
- systemd (`schedule/systemd.py`): the unit gets `LimitNOFILE=65536:524288` (soft 65,536; hard 524,288, systemd's own
  default hard limit since version 240, so the unit lowers nothing on a default host).
- Windows Task Scheduler: no change (no POSIX limit).
- **Existing installs.** D1 alone fixes the installed Mac task, because launchd's default hard limit is unlimited and
  the process raises its soft limit itself. The plist keys arrive with the next `lakehouse schedule install`, which
  `docs/ops/deploy.md` states.

### D3. Run-end merge of DuckLake-managed small files

**Position.** In `_Run.finish`, after dbt and before `run_cleanup`, inside the existing `if not self.terminated:`
block, after a `lease.confirm()`, in a step span `lakehouse.run.merge`. A stopped run does no maintenance, as today.
`sweep_now` (`lakehouse sweep --allow-large`) runs the same merge before its cleanup, because it is the same
maintenance outside a run (§10.3).

**Scope.** Every DuckLake-managed table whose `auto_compact` option is true, which is every non-bronze table (the ops
tables, lineage, the resolution, move and restore tables, and any table dbt materialises). DuckLake's merge skips a
table with `auto_compact = false` (probe), and every bronze table is created with it (`bronze/writer.py`
`_ensure_table`, `bronze/writers.py` `_ensure_native_table`).

**Guard (fail closed).** Before any merge, the run reads each bronze table's `auto_compact` option. If one bronze
table lacks `auto_compact = false`, the run merges nothing, records a `GuardStopError` and ends `failed`. That keeps a
register-path bronze file from ever being merged, whatever created the table.

**Two steps per table (Reviewer B T12-SPEC-01).** The merge skips every file that carries a delete file, and the ops
tables get partial deletes every run (Evidence). So each table's maintenance is:

1. **Rewrite:** `ducklake_rewrite_data_files('lake', <table>, delete_threshold => REWRITE_DELETE_THRESHOLD)` rewrites
   each file whose deleted-row fraction is at or above the threshold into a file without deletes.
2. **Merge:** `ducklake_merge_adjacent_files('lake', <table>, ...)` merges the small files left.

**The rewrite threshold (owner decision 2026-11-01: 0.0).** `REWRITE_DELETE_THRESHOLD = 0.0`: every
file with any delete is rewritten at each run end, so every table returns to one file (spike: 1 file after each of 5
runs, 47-78 ms). The cost is write amplification: each run rewrites every ops table that it updated or deleted from,
and every rewritten copy stays until `snapshot_days` (8) has passed. The tables that get deletes every run are
`ops_copy_fields` (`match_writer.py` deletes a changed match's rows), `ops_matches` and `ops_match_files`
(`runs/history.py`), `ops_lineage` (`kept = false`), and the resolution tables (Reviewer B T12-SPEC-06). Their merged
size, measured on the live Mac lake (each table copied read-only into one encrypted Parquet file): `ops_copy_fields`
99 KB (560,582 rows; its 2.2 GB on disk is per-file overhead), `ops_lineage` 538 KB, `ops_entity_records` 244 KB,
`ops_match_files` 216 KB, `ops_canonical_pointers` 177 KB, `ops_matches` 24 KB: about 1.3 MB together. So a run
rewrites at most about 1.3 MB, and 8 days of copies hold about 10 MB. The alternative is a higher threshold (for
example 0.5): fewer bytes rewritten per run, but files with a low deleted fraction stay, so a table settles at a few
files rather than one, and the bound is no longer exactly one file. The plan measures the per-run rewrite time on the
live lake in the upgrade-path run either way.

**Threshold for maintenance.** A table gets maintenance only when it holds more than `MERGE_MIN_FILES = 16` live data
files or at least one file with a delete file (`ducklake_list_files`), so a small, clean table costs one count query.

**Bounded calls and a run budget (G5; Reviewer B T12-SPEC-02).** One merge call over 100,000 files took 203 s and the
time grows faster than the file count, and `max_compacted_files` does not bound a merge call. So:

- Each merge call covers at most `MERGE_SLICE_FILES = 20_000` files (5.9 s measured). The slice is chosen by the files'
  creation time: the run reads the live files' snapshot times from the catalog metadata and passes `newer_than` and
  `older_than` to `ducklake_merge_adjacent_files` so that each window holds at most `MERGE_SLICE_FILES` files. A
  single snapshot cannot be split, so Task 1 also measures the largest number of files one snapshot added to one table
  in the live lake (Reviewer A T12-SPEC-01); a snapshot above the slice is a stop-rule case.
- The rewrite step is bounded the same way when its parameters allow: Task 1 measures whether `max_compacted_files`
  bounds a rewrite call. If it does not, the rewrite runs per table unbounded, and Task 1 measures the live lake's
  volume of files with deletes to show that the first run still fits the budget.
- **Stop rule:** if no parameter bounds a merge call, or the rewrite cannot be bounded and the live lake's first run
  would not fit the budget, the cycle stops and the author returns to the owner.
- A run spends at most `maintenance.merge_budget_seconds` (setting, default 300) on both steps together. It starts no
  new slice or table after the budget is spent, logs what is left, and the next run continues. `sweep_now` has no
  budget.
- The live lake's 560,582-file table drains over one or more runs (G5); the upgrade-path run (below) measures how
  many. Its backlog shows why the drain matters: one full read of it on the live lake took 1,712 s.

**Publication.** The merge changes the working copy only. The run-end publish (T3 D1, the checkpoint policy) publishes
it with the rest of the run's work; the merge adds no publish of its own. The integrity guard runs before that publish,
as for every publish.

**Retention and deletion (G4).** The rewrite and the merge each add a snapshot. They differ in how the replaced files
leave, which rounds 1–3 missed and the Mac gate found (2026-11-01):

- The **rewrite** keeps its replaced files as ended `ducklake_data_file` rows and does not schedule them (round-4
  probe). They stay referenced by the pre-rewrite snapshots and leave through the existing expiry-and-cleanup path
  (`run_cleanup` steps 2 and 6): once those snapshots pass `snapshot_days`, DuckLake lists the files, the kept-version
  guard confirms no kept version names them, and the cleanup deletes them. Because `version_days < snapshot_days`
  (§10.7), a kept version never names a file that only an expired snapshot references.
- The **merge** removes its replaced files' `ducklake_data_file` rows and schedules the files for deletion at once
  (round-4 probe). They stay in `ducklake_files_scheduled_for_deletion` and leave through the same cleanup path: D3.1
  makes `run_cleanup` step 6 delete a scheduled file only when no kept version names it, and defer the rest. The orphan
  sweep (step 7) is never involved, so its `sweep_max_fraction` guard never applies to merge artifacts.

**If Task 1 finds the rewrite scheduling or deleting a replaced file before expiry, it is a stop-rule case for the
owner.** The storage overhead is the merged copy of the small tables until they are collected.

**Encryption and HF.** On an encrypted lake the merged files are written through the `ENCRYPTED` attach (probe:
encrypted, no cleartext). On HF the merge reads and writes through the DuckLake data path (the S3 gateway); these are
plain writes, so T6's single-part rule for conditional writes does not apply.

**Crash safety.** A crash during the rewrite or the merge leaves new files that no published catalog names. The T3 orphan sweep
deletes them once they are older than the next run's age fence, and the working copy is discarded (T3). A crash after
the maintenance and before the run-end publish loses only the maintenance; the replaced files are untouched, because
the merge and the rewrite delete no data file: the merge schedules its replaced files at once, and the rewrite keeps
them as ended `ducklake_data_file` rows that a later expiry schedules (Reviewer A T12-SPEC-15). D3.1's deferral — the
scheduled-row deletes in `run_cleanup` step 6 — is part of the cleanup, which runs in the working copy before the
run-end publish; a crash before the publish discards it with the working copy, so the scheduled list is left as it was.
Step 6's storage deletes are not transactional with the catalog, so a crash between a storage delete and the publish
can leave a scheduled row for a file already removed (T3 behaviour, widened by the backlog eligibility day): step 6 is
idempotent — a scheduled file already missing that names no move record is treated as deleted, not an `IntegrityError`
(Reviewer B).

**Readers.** A reader keeps its attached version (§10.4); the files of a version it may still read are not deleted
before the snapshot and version retention allow it (unchanged).

**Observability.** One log line per maintained table, `data files merged`, with `table`, `files_before`,
`deletes_before`, `rewritten`, `files_after`, `slices` and `ms`; the run's `catalog totals` line gains `merged_files` and `merge_ms`; a run that stops merging at the
budget logs `merge budget spent` with the tables and file counts left. The `cleanup` log line (and the status or
settings page where run totals appear) gains the **deferred count**, the **oldest deferred `schedule_start`**,
**`missing_treated_as_deleted`** and **`missing_deferred`** (Reviewer B-22, T12-SPEC-25, T12-SPEC-28): deferral replaces the old fatal guard, so without a signal a never-pruned version, a kept-set error
or a DuckLake change would show only as slow disk growth. A stuck deferral is a deferred count that does not fall and
an oldest `schedule_start` that keeps ageing past `snapshot_days` + `version_days`; that is the signal to investigate.
The totals also show `missing_treated_as_deleted` with its interpretation: 0 is normal; a nonzero count is expected on
the first run right after an interrupted run (the step-6 idempotency healing a crash-partial delete), but otherwise it
means a file vanished outside step 6 or a path-mapping defect misresolved a row — investigate (T12-SPEC-25,
T12-SPEC-27). A file already counted in `missing_deferred` counts once here when its restore point prunes — the same
known fault reaching end of life, not a new one (T12-SPEC-32). The totals also show `missing_deferred`: 0 is normal; a
nonzero count means a deferred file a restore point names has vanished — the restore point is broken — investigate
(T12-SPEC-28). The `cleanup` log includes a bounded sample of the missing canonical keys and the kept catalog-version
sequence numbers that name them, so the operator can find the broken restore point (T12-SPEC-33).

### D3.1 Deferring the kept-overlapping scheduled files in the cleanup (Candidate A, option b)

**The gate finding (2026-11-01).** Rounds 1–3 assumed the merge's replaced files leave only through expiry and the
cleanup, like the rewrite's. That is false for the merge. Proven by probe (DuckDB / DuckLake 2.0.0):

- `ducklake_merge_adjacent_files` schedules its replaced files for deletion at once, at merge time, into
  `__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion`.
- `run_cleanup` step 6 lists deletions with `ducklake_cleanup_old_files('lake', cleanup_all => true)`, which lists
  every scheduled file and ignores both snapshot references and age. `cleanup_all => false` lists none (it respects a
  scheduling-time threshold that the harness's simulated clock never meets).
- A v2 restore point is a separate immutable catalog version whose manifest names the old file paths. A merge replaces
  current-head files that every recent kept version names, whatever their age.

So on essentially every run the merge schedules a file that a kept restore point (younger than `version_days`) still
names. The cleanup's kept-version guard (`cleanup.py`, "DuckLake's cleanup list names kept files") stops the run;
without the guard, the non-dry `cleanup_all` would delete a file a restore point needs and corrupt it.
`older_than => (now − snapshot_days)` cannot fix it: `ducklake_files_scheduled_for_deletion.schedule_start` is real
wall-clock, not the snapshot time, so it cannot align with snapshot retention and cannot be tested with simulated days.
"Merge only settled files" is infeasible: the merge operates on current-head files. The rewrite does not have this
problem — its replaced files keep their `ducklake_data_file` rows and are not scheduled at merge time; only a later
expiry schedules them (round-4 probes).

**Candidate A, option (b) (owner 2026-11-01).** The cleanup — not the merge and not the orphan sweep — owns the
deferral. The merge leaves its eager-scheduled rows in place. `run_cleanup` step 6, after the snapshot expiry (step 2):

- reads `__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion` (columns `data_file_id`, `path`,
  `path_is_relative`, `schedule_start`); after an expiry this set equals what `ducklake_cleanup_old_files(cleanup_all
  => true)` would list (round-4 option-(b) probe A: scheduled == the dry-run list), and the cleanup already reads this
  table (`_tracked_refs`);
- partitions it by the kept-version file set: `to_delete = listed − kept`, `to_defer = listed ∩ kept`;
- deletes each `to_delete` file through storage and removes its row from the scheduled table by `data_file_id` (the
  move-record "not found" case is unchanged; a scheduled file already missing that names no move record is treated as
  deleted and counted (`missing_treated_as_deleted`), not an `IntegrityError` — the step-6 idempotency, see Crash
  safety);
- leaves every `to_defer` row in the scheduled table — deferred — and does **not** call
  `ducklake_cleanup_old_files(cleanup_all => true)`, whose all-or-nothing delete would take the deferred files too;
- compares the `to_defer` files to the current storage listing by canonical key — using a single storage listing step 6
  makes itself (one list operation, not a per-file existence probe); the orphan sweep keeps its own step-7 listing, so
  step 6's deletions never feed the sweep's `sweep_max_fraction` guard (T12-SPEC-30, no B-16 re-trigger); a `to_defer`
  file whose canonical key is absent from the listing is a broken restore point, counted as `missing_deferred` and
  logged. The kept-version
  set already holds each file's resolved key (`kept_file_set` maps a moved file's old key to its new key), so a
  relocated file is compared at its new key and is not false-flagged (T12-SPEC-29). Step 6 does not delete the file or
  remove its scheduled row (it stays deferred and tracked), because the fault is upstream, not a deletion to complete
  (owner 2026-11-01: T12-SPEC-28 covered in T12).

A `to_defer` file stays scheduled, tracked (the cleanup's `_tracked_refs` counts the scheduled table, so the orphan
sweep never treats it as an orphan), on disk, and named by its restore point, so a restore still reads it. Once that
restore point is pruned (beyond `version_days`), the file is `to_delete` on a later run and is deleted through the same
step-6 path. Because `version_days < snapshot_days` (§10.7), the only scheduled file a kept version names is a merge
artifact, so deferring is always safe (the cleanup never deletes a kept file): the round-1–3 fatal "DuckLake's cleanup
list names kept files" guard becomes a defer, not a stop.

**Scope of the step-6 idempotency (Reviewer T12-SPEC-28).** The treat-as-deleted path is `to_delete` only. Step 6
never deletes a `to_defer` file, so it cannot make a deferred file go missing. A `to_defer` file that is missing from
another cause is a broken restore point. Step 6 detects it by set membership: a `to_defer` file whose canonical key is
absent from the current storage listing is counted as `missing_deferred` (owner 2026-11-01: T12-SPEC-28 covered in
T12). The comparison uses the resolved key the kept set holds, so a relocated file is checked at its new key and is not
false-flagged (T12-SPEC-29); it uses a single listing step 6 makes itself, not a per-file probe, and not the orphan
sweep's step-7 listing, so step 6's deletions cannot inflate the sweep's fraction guard (T12-SPEC-30, no B-16
re-trigger). Step 6 does not delete or un-schedule it, because the fault is upstream. See Observability.

**Warning on a broken restore point (T12-SPEC-33, owner 2026-11-01).** When a user restores to a catalog version, the
`restore` command checks that version's files against the storage listing with the same set-membership test. If any
are missing (a broken restore point), it warns, naming a bounded sample of the missing files, and proceeds — it does
not refuse, so it never stands between the user and their only recovery point. A test covers a broken version (warns)
and a clean one (no warning).

**Preview and `--expect` (Reviewer B-20).** `preview_cleanup` and the `lakehouse sweep --dry-run` / `--expect` list use
the same `to_delete` / `to_defer` partition as `run_cleanup`, through one shared function. `sweep_now` runs the merge
before the cleanup, so the preview sees the merge's freshly scheduled files, which step 6 mostly defers; the preview
reports `to_delete` as what will be deleted and `to_defer` separately, and `--expect` checks only `to_delete`. Without
this, the D4 upgrade sweep's `--expect` would stop on the ~560k just-scheduled files, or a dry run would report them as
deletes that step 6 only defers. A test runs `--allow-large --expect` across a merge that schedules kept files.

**Why the cleanup, not the orphan sweep (Reviewer B-16).** Merge artifacts are DuckLake-scheduled deletions, so the
cleanup's step-6 deletion path owns them. That path has no `sweep_max_fraction` guard — the guard belongs to the orphan
sweep (step 7), which deletes files DuckLake does not track on v2's own kept-set reasoning and so needs a human above a
threshold. Routing merge artifacts through the orphan sweep (the round-4 draft) put an expected, possibly large
deletion behind that guard: a first load, a full reparse (`run.py` `run_reparse`) or a backfill writes about one file
per match per ops table, which the merge collapses; those come due together about `version_days` later and would trip
`sweep_max_fraction` on a scheduled run, failing it until an operator ran `sweep --allow-large` (Reviewer B-16). Under
option (b) the same deletion runs in the step-6 loop with no fraction guard, so no scheduled run fails and no operator
action is needed; the orphan sweep keeps its guard for genuine orphans. A step-6 error in the cleanup raises and
fails the run before the publish; only the pre-merge bronze guard raises `GuardStopError` (the catch-and-publish path,
`run.py` `finish`), before any merge, so a merged state is never published without its cleanup (Reviewer B-15).

**Feasibility (round-4 option-(b) probe, plain and encrypted, `t12_optionb_probe.py`).** Deleting a subset of the
scheduled files through storage and removing their rows by `data_file_id`, without `cleanup_all`, leaves the other
scheduled rows in place, keeps the table queryable and mergeable, and a later expiry-plus-`cleanup_all` deletes the
deferred ones (scheduled 20 → delete 10 → scheduled 10, disk 11, query ok; later cleanup → scheduled 0, disk 1).
`cleanup_all`'s list equals the scheduled table after an expiry (probe A).

**The residual risk and its stop rule.** A probe cannot show v2's own partition and the deletion across the retention
boundary end to end. A RED-first lifecycle test (Tests) proves it: a merged artifact is deferred while its restore
point names it, then deleted through step 6 once the restore point is pruned; and a large run followed by the
eligibility day deletes through step 6 with no failed scheduled run (Reviewer B-16). If a deferred artifact is never
deleted (a leak), the cycle stops and the author returns to the owner.

**Engine-internal dependency.** The cleanup both reads (`_tracked_refs`) and now writes
(`DELETE ... WHERE data_file_id = ?`) `__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion`. A DuckLake
upgrade could change that table, the merge's eager scheduling or the subset-delete mechanic. A guard test (Tests) pins
the scheduled-table columns, that a merge populates it and removes the replaced files' `ducklake_data_file` rows, that
a rewrite keeps its ended rows and schedules only after an expiry, and that a subset delete without `cleanup_all`
leaves the other rows and the deferred ones collectable later; it fails on an upgrade that changes any of these
(Reviewers A T12-SPEC-12, B-18).

### D4. The live lake's backlog and the upgrade path

The live Mac lake drains through D3 in its first runs after T12. The owner decided on 2026-11-01 (T3 review B-01)
that the existing-lake upgrade path is verified after T12 lands: back up the live lake and its restricted root to a
dated folder on `/Volumes/Development`, run a real ingestion with the new code against the live lake, check the
results and the bounds (file counts after the merge, run time, footprint, no failed match), and restore the backup on
any failure. Before the drain it also records the peak descriptors of one scan of the 560,582-file table (Reviewer A
T12-SPEC-02), the per-run rewrite time, and the largest files-per-snapshot (Task 1). **The upgrade starts with
`lakehouse sweep --dry-run`, then `lakehouse sweep --allow-large --expect <the dry run's list>`** (Reviewer B T12-SPEC-09; plan review
T12-PLAN-02: the dry run lists exactly what the lifted guard would delete, and the author stops for the owner above
`sweep_max_fraction`): its maintenance has no budget, so the backlog is drained
under the lease before the first ingestion run, whose per-match reads of `ops_copy_fields` (`match_writer.py`) would
otherwise each scan the 560,582 files (one full read took 1,712 s); Task 1 also times one filtered query on the
backlog to show the effect. That run is this cycle's real-data verification of D3 and D4.

**The deferred backlog (option b).** Under D3.1 option (b) the merged-away backlog files stay scheduled and leave
through `run_cleanup` step 6, not the orphan sweep. The up-front drain merges the 560,582-file table to one live file;
the step-6 cleanup defers the merged-away files while the pre-drain versions name them (younger than `version_days`).
Those pre-drain versions prune together, so about 560k files become `to_delete` on a single eligibility run — about
560k `storage.delete` calls and one batched `DELETE ... WHERE data_file_id IN (...)` (Reviewers B-21, A T12-SPEC-16).
Step 6 has no `sweep_max_fraction` guard and no time budget (unbudgeted by design, like the rest of the cleanup), so
this cannot fail a run, but its time is unmeasured: Task 1 (or the D4 run) measures one step-6 pass at backlog scale
against the run window, and a pass that does not fit is a stop-rule case for the owner. The per-row deletes are
batched. No one-time operator `sweep --allow-large` is needed (Reviewer B-16 and A T12-SPEC-14 are resolved by (b)).
Steady-state runs behave the same at a far smaller scale. The upgrade's up-front `sweep --dry-run` / `--expect` stays
as the safety for any genuine orphans the sweep itself would remove.

## Tests

- **Unit, D1:** `raise_open_file_limit` with a stubbed `resource` and `sysctlbyname`: a soft limit below the target is
  raised to the target; capped by a finite hard limit; capped by `kern.maxfilesperproc` on macOS; a soft limit at or
  above the target is unchanged; Windows returns `None`. The fail-fast check raises `ConfigError` naming the limit when
  the result is below the minimum, and only for commands that open a catalog.
- **OS, D1 (macOS and Linux, in CI):** a subprocess started with a soft limit of 256 runs a catalog-reading command on a
  lake whose ops table holds 300 files; it succeeds (RED before D1, with the production error).
- **Unit, D2:** the rendered plist holds `SoftResourceLimits` `{NumberOfFiles: 65536}` and no `HardResourceLimits`;
  the rendered unit holds `LimitNOFILE=65536:524288`.
- **Integration, D3, register and native paths:** a lake with 100 small files in an ops table and bronze files of
  several matches: after the run end, the ops table holds 1 file, its rows and content checksums are unchanged, every
  bronze file is unchanged (keys and bytes), and the published version names the merged file; on an encrypted lake no
  file holds cleartext.
- **Guard:** a bronze table with `auto_compact = true` makes the run merge nothing, record the guard stop and end
  `failed`.
- **Budget:** with a budget of 0 seconds the run merges nothing and logs the backlog; the next run with a normal
  budget merges.
- **Slices:** a table above `MERGE_SLICE_FILES` (a test value) is merged in several calls, each within the slice.
- **Partial deletes (Reviewer B T12-SPEC-01):** an ops table with an `UPDATE` and a `DELETE` per simulated run (the
  lineage `kept = false` write, the `flush_unchanged` update, a `runs/history.py` replace) ends each run end with one
  file and no delete file, rows and checksums unchanged; with the rewrite step removed the test fails (files with
  deletes accumulate).
- **Entry points (Reviewer B T12-SPEC-03):** the AST test of D1.
- **Fallback (Reviewer B T12-SPEC-05):** with `sysctlbyname` failing and `setrlimit` refusing the first values, the
  raise settles on the largest accepted value and logs it; with every value refused it keeps the current limit and the
  minimum check decides.
- **Retention and deferred deletion (D3.1 option (b), RED first):** after a merge the replaced files are still in
  `ducklake_files_scheduled_for_deletion` and on disk; `run_cleanup` step 6 deletes the scheduled files no kept version
  names and defers (leaves scheduled) the ones a restore point names, and does not call `cleanup_all => true`; a
  restore still reads a deferred file; after that restore point is pruned (beyond `version_days`) the next run's step 6
  deletes the file and no `GuardStopError` is raised. The test asserts the scheduled-table rows directly and that the
  step-6 loop (not the orphan sweep) deleted each file. Without the deferral the test fails: the old fatal guard stops
  the run (the gate finding), or an undeferred `cleanup_all => true` deletes a file a restore point names.
- **Large run, no failed scheduled run (Reviewer B-16, RED first):** a run that writes many files to an ops table
  (reparse- or backfill-scale), then the eligibility day (clock past `version_days`): the merged-away files delete
  through the step-6 loop and the scheduled run completes; `sweep_max_fraction` never fires on them. With the deletion
  routed through the orphan sweep instead, the scheduled run fails on the guard.
- **DuckLake-upgrade guard (D3.1):** a guard test pins the columns of `ducklake_files_scheduled_for_deletion`, that a
  merge populates it and removes the replaced files' `ducklake_data_file` rows, that a rewrite keeps its ended rows and
  schedules only after an expiry, and that a subset delete by `data_file_id` without `cleanup_all` leaves the other
  scheduled rows and the deferred files collectable later; a DuckLake upgrade that changes any of these fails the test
  (Reviewers A T12-SPEC-12, B-14/B-18).
- **Preview / `--expect` partition (Reviewer B-20, RED first):** `lakehouse sweep --allow-large --expect <list>` across
  a merge that schedules files a kept version names: the preview partitions like `run_cleanup`, reports only
  `to_delete` as deletes and `to_defer` separately, and `--expect` does not stop on the deferred files. Without the
  shared partition the preview lists the deferred files and `--expect` stops.
- **Scheduled-row key parity (Reviewer B-23):** the canonical keys of the resolved
  `ducklake_files_scheduled_for_deletion` rows equal the `_ducklake_dry_run` keys.
- **Crash:** a crash inside the rewrite, and one inside the merge, then the next run: the orphans are swept and the
  maintenance completes; D3.1's step-6 deferral is part of the discarded working copy, so a crash before the publish
  leaves the scheduled list as it was. A crash between a step-6 storage delete and the publish leaves a scheduled row
  for an already-deleted file; the next run accepts it as deleted (no `IntegrityError`, Reviewer B), counts it in
  `missing_treated_as_deleted`, and the test asserts both the count and that it reaches the `cleanup` log and the status
  or settings totals (T12-SPEC-25).
- **Missing deferred (T12-SPEC-28):** a `to_defer` file removed from the storage listing out of band, then a run: step
  6 counts it in `missing_deferred` by set membership, logs the broken-restore-point signal with the bounded key
  sample, and does not delete or un-schedule it; the test asserts the count and its surfacing. A separate case covers a
  moved, kept, scheduled file (`kept_file_set` holds the new key): the new key missing is counted, the new key present
  is not (T12-SPEC-29). The deletion-day case confirms step 6 uses its own listing and its `to_delete` deletions do not
  count toward the orphan sweep's `sweep_max_fraction` guard (no B-16 re-trigger, T12-SPEC-30).
- **Order:** the merge runs after dbt and before the cleanup, and only when the run was not stopped.
- **HF tier:** the merge on the HF test lake (an ops table above the threshold), with its time recorded.
- **Bench (Mac):** merge time for 20,000 and 100,000 files, recorded against the spike.

## Amendments

- Ingestion design §10.2: bronze keeps `auto_compact = false`; every other DuckLake-managed table is merged at run end
  (D3). §10.3: a "merge" step between dbt and the cleanup (step 6). §10.8: a rewritten or merged file leaves through
  the existing cleanup (`run_cleanup` step 6); the cleanup defers a scheduled file a kept version still names and
  deletes it once no kept version does (D3.1 option (b)). No new deletion path.
- ADR-001: amendment for D3 (the run-end merge replaces DuckLake's compaction for non-bronze tables; `CHECKPOINT` stays
  forbidden), for D3.1 (the cleanup defers kept-overlapping scheduled files instead of the old fatal guard, and deletes
  scheduled files by `data_file_id` in place of `cleanup_all => true`) and for D1/D2.
- These downstream documents (the ingestion design, ADR-001 and `docs/ops/deploy.md`) are rewritten to D3.1 option (b)
  during implementation, after Reviewers A and B and the owner approve this amendment; the copies written in the prior
  turn assert the disproven premise and are corrected then.
- `docs/ops/deploy.md`: the open-file limit (D1, D2), the minimum and its error, and re-running
  `lakehouse schedule install` to get the plist and unit keys.
- Audit ledger row T12.

## Left to the builder

- The exact metadata query that reads the live files' snapshot times, once Task 1 confirms the slicing parameters.
- The setting's name and place in the config model, as long as the default is 300 seconds and the settings page shows
  it with its unit.
- Where the fail-fast check sits in the CLI dispatcher.
- The exact step-6 SQL for option (b): reading the scheduled table, partitioning by the kept set, deleting a
  `to_delete` row by `data_file_id`, and whether to keep a `cleanup_all => true` call for the runs where nothing is
  deferred (`to_defer` empty) rather than always taking the per-row path — the option-(b) probe showed the per-row
  path alone keeps DuckLake queryable and mergeable.

## Review focus

1. **A register-path bronze file merged** (would break file keys, moves and the per-match registry): the guard must
   fail closed before any merge.
2. **A merged file deleted, or its run stopped, while a kept version still names it** (would break a restore point or
   stop every run): check D3.1 option (b) — `run_cleanup` step 6 defers a scheduled file a kept version names (leaves
   it scheduled, does not call `cleanup_all => true`) and deletes it, through the unguarded step-6 loop, once no kept
   version names it; the RED-first lifecycle test proves the defer-then-delete across the retention boundary, and a
   large run plus the eligibility day fails no scheduled run (B-16).
3. **A merge on a stopped or crashed run** (would publish a half-done state or leave orphans): check the position and
   the crash path.
4. **The open-file limit lowered or raised beyond what the kernel accepts** (would fail at start on macOS): check the
   caps.
5. **The merge's run time on the live lake** (would make the first scheduled run after the upgrade exceed its window):
   check the slice and budget rules and the stop rule.

## Dispositions (round 1)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-SPEC-01 | B | BLOCKING | The merge skips files with delete files; the ops tables get partial deletes every run, so `ops_lineage` stays unbounded | **Accepted.** D3 now has two steps per table (rewrite, then merge); the evidence table has B's spike and the author's 5-run spike; the rewrite threshold and its write amplification are an open decision for the owner (recommendation 0.0); a partial-delete test is added. |
| T12-SPEC-02 | B | SHOULD FIX | The slice, budget and stop rule must cover the rewrite | **Accepted.** The budget covers both steps; Task 1 measures whether `max_compacted_files` bounds a rewrite call and, if not, the live lake's volume of files with deletes; the stop rule covers both. |
| T12-SPEC-03 | B | SHOULD FIX | Prove every entry point calls the raise first | **Accepted.** An AST test checks each entry point's first statement and that the list matches `[project.scripts]` and every `__main__` module. |
| T12-SPEC-04 | B | CONSIDER | `HardResourceLimits` 65,536 caps launchd's unlimited hard limit | **Accepted.** Only `SoftResourceLimits`; systemd gets `65536:524288`, its own default hard limit. |
| T12-SPEC-05 | B | CONSIDER | `sysctlbyname` needs `ctypes`; name the failure fallback and test it | **Accepted.** D1 names the halving fallback; a test covers it. |
| T12-SPEC-01 | A | CONSIDER | A single snapshot above the slice cannot be split | **Accepted.** Task 1 measures the largest files-per-snapshot per table in the live lake; above the slice is a stop-rule case. |
| T12-SPEC-02 | A | CONSIDER | `OPEN_FILES_MINIMUM` was derived at 10,000 files; check a pre-drain scan of the 560,582-file table | **Accepted.** The upgrade-path run records the peak descriptors of a scan of that table before the drain; if it is above 4,096, the minimum is raised to the measured value times 2 before the cycle closes. |

## Dispositions (round 2)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-SPEC-03 | A | SHOULD FIX | The rewrite's deferred deletion is not established; the retention and crash paragraphs name only the merge | **Accepted.** A probe (plain and encrypted) shows the rewrite keeps every replaced file until expiry and cleanup, and time travel still reads them (evidence table); "Retention and deletion" and "Crash safety" now name both steps; a step that deletes early is a stop-rule case in Task 1. |
| T12-SPEC-06 | B | SHOULD FIX | Not "inserts only": `ops_copy_fields`, `ops_matches` and `ops_match_files` get deletes every run, so 0.0 rewrites them whole each run | **Accepted.** The open decision now lists every table that gets deletes and their measured merged sizes (about 1.3 MB together, so about 1.3 MB rewritten per run and about 10 MB over 8 days); the wrong line is corrected. |
| T12-SPEC-07 | B | SHOULD FIX | The D2 test line still names both keys and `LimitNOFILE=65536` | **Accepted.** The test line now matches D2. |
| T12-SPEC-08 | B | CONSIDER | "never raises" is ambiguous | **Accepted.** "never raises an exception". |

## Dispositions (round 3)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-SPEC-09 | B | CONSIDER | The upgrade run reads `ops_copy_fields` per match before the run-end drain | **Accepted.** D4 starts the upgrade with `lakehouse sweep --allow-large` (unbudgeted maintenance under the lease); Task 1 times one filtered query on the backlog. |
| T12-SPEC-10 | B | CONSIDER | The Retention and Crash test lines name only the merge | **Accepted.** Both lines now name the rewrite too. |

## Dispositions (round 4)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-GATE-01 | Mac gate (2026-11-01) | BLOCKING | The run-end merge schedules its replaced files for deletion at once; `run_cleanup`'s `cleanup_all => true` lists them while a kept restore point names them, so the kept-version guard stops the run (or the non-dry cleanup would corrupt a restore point) on essentially every run. The rounds 1–3 premise ("8 d > 7 d, so DuckLake never lists a kept file") is false: `cleanup_all => true` ignores snapshot references and age. Two gate tests encode it: `tests/e2e/test_maintenance_crash.py` and `tests/unit/test_run_lease_check.py::test_confirm_runs_before_resolution_and_dbt`. | **Design change (owner 2026-11-01): Candidate A (new D3.1).** The merge un-tracks its own eager scheduled-deletion entries in `catalog/maintenance.py`, so its replaced files become untracked orphans the orphan sweep owns; the cleanup guard is unchanged. A round-4 probe proved the merge removes the replaced files' `ducklake_data_file` rows (so expiry and cleanup do not collect them; the orphan sweep does). `older_than` and "merge only settled files" are ruled out (D3.1). A RED-first lifecycle test and a DuckLake-upgrade guard test are added. The §10.2 / §10.3 / §10.8 amendments, the ADR-001 merge amendment and `deploy.md` are rewritten to D3.1 during implementation. The `test_run_lease_check` confirm count becomes 3 (maintenance is a real write step in `finish`); whether `test_maintenance_crash.py` encodes the guard trip is resolved in the plan's test section against the Mac. |
| T12-SPEC-11 | A | CONSIDER | The un-track "same path resolution as `_tracked_refs`" is imprecise; the delta rows carry their own identity, and `_tracked_refs` has two resolutions | **Accepted.** D3.1 keys the un-track by `data_file_id` (the delta rows' own identity); no path resolution. |
| T12-SPEC-12 | A | CONSIDER | The upgrade-guard test should pin both the read site (`_tracked_refs`) and the write site | **Accepted.** The guard test pins both sites, the scheduled-table columns and the merge's row removal. |
| B-11 | B | BLOCKING | Departure (2) was unproven; the gate note suggested the merge removes the source `ducklake_data_file` rows, so after the un-track only the orphan sweep collects the file, the D4 drain makes ~560k orphans that later trip `sweep_max_fraction`, and the lifecycle test could not tell the two paths apart | **Accepted; B is correct.** A round-4 probe confirmed the merge removes the rows. D3.1 now names the orphan sweep as the deferral owner; the lifecycle test asserts the metadata rows and which step deletes; D4 adds the one-time `sweep --allow-large --expect` for the aged-out backlog (owner 2026-11-01). The round-4 departure (2) is withdrawn. |
| B-12 | B | SHOULD FIX | "The rewrite's sources are not scheduled" was not probed; if the rewrite also schedules, the cleanup guard trips every run | **Accepted; verified safe.** The round-4 probe shows `ducklake_rewrite_data_files(0.0)` schedules 0 rows and keeps the ended `ducklake_data_file` rows. The stop rule is widened to "schedules or deletes before expiry". |
| B-13 | B | SHOULD FIX | The rewrite-deferral citation points at the wrong Evidence row, and the merge Evidence row still states the disproven reading | **Accepted.** The citation now points at the round-4 probe and the rewrite row; the merge row carries a round-4 correction note. |
| B-14 | B | CONSIDER | Key the before/after delta by row identity, not path resolution | **Accepted.** Keyed by `data_file_id` (same as A T12-SPEC-11). |
| B-15 | B | CONSIDER | `run.py` publishes after a maintenance `GuardStopError`; state that only the pre-merge bronze guard raises it, so a merge without its un-track is never published | **Accepted.** D3.1: the un-track runs inside maintenance before any publish; an un-track failure fails the run and is never swallowed into the publish-anyway path; only the pre-merge bronze guard raises `GuardStopError`, before any merge. |

## Dispositions (round 5)

Round 5 **superseded the round-4 orphan-sweep mechanism**: Reviewer B-16 showed that routing merge artifacts through
the orphan sweep trips `sweep_max_fraction` about `version_days` after any large run; the owner chose **option (b)** —
the cleanup (`run_cleanup` step 6) owns the deferral, with no orphan sweep and no fraction guard on merge artifacts.
The round-4 rows above are kept as the record of the superseded mechanism.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| B-16 | B | BLOCKING | Orphan-sweep ownership (round 4) makes the merge orphans of any large run — first load, full reparse (`run_reparse`), backfill — come due together about `version_days` later; with no anchor left they exceed `sweep_max_fraction`, so a scheduled run fails closed until an operator runs `sweep --allow-large`. Not one-time; breaks Intent ("runs must not fail on file counts") and "no manual steps". | **Accepted; owner chose option (b) 2026-11-01.** The cleanup (step 6) deletes a scheduled file no kept version names through its unguarded delete loop and defers (leaves scheduled) the ones a kept version names; the orphan sweep and its fraction guard are not involved for merge artifacts, so no scheduled run fails and no operator sweep is needed. Feasibility proven by the option-(b) probe (`t12_optionb_probe.py`). A RED-first large-run-plus-eligibility-day test is added. D3.1, D3, D4, the Tests, the Anti-goal, Review focus #2 and the Amendments are rewritten to (b). |
| T12-SPEC-13 | A | SHOULD FIX | The Anti-goals bullet still says the merge's replaced files "follow that same [expiry-and-cleanup] path" — the disproven rounds-1–3 premise — contradicting D3/D3.1/§10.8 | **Accepted (= B-17).** The Anti-goal now names the one existing path for both (the cleanup, step 6) and states option (b) defers within it; no new deletion path. |
| B-17 | B | SHOULD FIX | Same stale Anti-goals line as A T12-SPEC-13 | **Accepted.** Fixed with T12-SPEC-13. |
| T12-SPEC-14 | A | CONSIDER | D4 does not state what a scheduled run does if it hits the aged-out ~560k orphans before the operator's one-time sweep | **Resolved by (b): moot.** Under option (b) the backlog deletes through the unguarded step-6 loop over the days after the drain; there is no `sweep_max_fraction` trip, no fail-closed window and no one-time operator sweep for the merge artifacts (D4 "The deferred backlog"). |
| B-18 | B | CONSIDER | Add the rewrite's scheduled-delta-0 and ended-rows-kept assertions to the DuckLake-upgrade guard test | **Accepted.** The guard test now pins the rewrite behaviour (keeps ended rows, schedules only after an expiry) alongside the merge behaviour and the subset-delete mechanic. |
| B-19 | B | CONSIDER | The "T3 run anchors" wording is stale (anchors gone by eligibility), and the probe-scripts list misses `rewrite_deletion_probe.py` and `t12_rowtrack_probe.py` | **Accepted.** Option (b) removes the orphan-sweep/anchor wording for merge artifacts from D3.1; the probe-scripts line now lists the round-4 and round-5 probes. |
| — (A, minor) | A | — | The Status header still read "Round 4" on the round-5 artifact | **Accepted.** The Status header tracks the current round (Round 6 now). |

## Dispositions (round 6)

Round 6 confirmed option (b): Reviewer A APPROVE, Reviewer B APPROVE WITH CHANGES (B waived a further round). The three
SHOULD FIX carry (b) into the preview, the backlog burst and observability; all are applied. No further review round is
required. Owner-approved round 6 on 2026-11-01.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| B-20 | B | SHOULD FIX | `preview_cleanup` / `--expect` lists every scheduled file; under (b) `sweep_now` merges first, so the preview sees ~560k just-scheduled files step 6 would only defer, and `--expect` wrongly stops | **Accepted.** D3.1 "Preview and `--expect`": the preview partitions like `run_cleanup` through one shared function, reports `to_delete` as deletes and `to_defer` separately, and `--expect` checks only `to_delete`; a test runs `--allow-large --expect` across a kept-scheduling merge. |
| B-21 | B | SHOULD FIX | D4 "over the runs of the days after" is imprecise — the pre-drain versions prune together, so ~560k files delete in one run, unmeasured | **Accepted (with A T12-SPEC-16).** D4 "The deferred backlog": the burst is one eligibility run; the per-row deletes are batched; Task 1 / the D4 run measures one step-6 pass at backlog scale against the run window, and a pass that does not fit is a stop-rule case; step 6 is unbudgeted by design. |
| B-22 | B | SHOULD FIX | Deferral has no production signal; a never-pruned version or kept-set error is silent disk growth | **Accepted.** Observability: the `cleanup` log and the status/settings totals gain the deferred count and the oldest deferred `schedule_start`; a stuck deferral is named. |
| T12-SPEC-15 | A | CONSIDER | D3 Crash-safety "the merge and the rewrite … only schedule" is inaccurate for the rewrite | **Accepted.** Reworded: the merge schedules at once; the rewrite keeps ended rows that a later expiry schedules. |
| T12-SPEC-16 | A | CONSIDER | The step-6 backlog drain has no time budget; up to ~560k `storage.delete` in one run | **Accepted (= B-21).** D4 measures the one-time drain time against the run window (stop rule) and states step 6 is unbudgeted by design. |
| B-23 | B | CONSIDER | Add a runtime parity check: resolved scheduled-row keys equal the `_ducklake_dry_run` keys | **Accepted.** A Tests bullet adds the key-parity check. |
| B-24 | B | CONSIDER | ":218-219" still said "kept-version guard" (now the partition) and ":297" "un-tracking error" | **Accepted.** The live-design "un-tracking error" is now "a step-6 error"; the "kept-version guard" reference remains only in the gate-finding description of the round-1–3 problem. |
| — (B, note) | B | — | A crash between a step-6 storage delete and the publish leaves a scheduled row for a deleted file → next-run `IntegrityError` (T3 behaviour, widened by the eligibility day) | **Accepted (post-round-6 amendment).** D3 Crash-safety and the Crash test: step 6 is idempotent — a scheduled file already missing that names no move record is treated as deleted and counted (`missing_treated_as_deleted`), not an `IntegrityError`. B flagged this note outside round 6, so it was added after the round-6 owner approval and is not part of the round-6 review; spec round 7 (Reviewer B) reviews it (see "Dispositions (round 7)"). |

## Dispositions (round 7)

Round 7 (2026-11-01) is a targeted re-review of the single post-round-6 amendment, the step-6 crash idempotency, on the
owner's decision (the change is spec-level and had A-only coverage). Reviewer B only (A reviewed it in plan rounds 5
and 6); report `...-spec-r7-B.md`. B APPROVE WITH CHANGES: behaviour correct, the fence-narrowing safe (`to_delete`
only; `exists()` false only on a true missing; the sweep keys kept from the listing), the plan's surfacing sufficient.
All four applied.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-SPEC-25 | B | SHOULD FIX | Observability omitted `missing_treated_as_deleted`; the safety signal lived only in the plan, so a review against the spec could not hold the implementation to it | **Accepted.** Observability now names `missing_treated_as_deleted` in the `cleanup` log and the status/settings totals with its interpretation, and the Crash test asserts the count and its surfacing. |
| T12-SPEC-26 | B | SHOULD FIX | Status said "no further review round is required" and did not record the amendment; row :540 said "the plan's round-5 re-review covers it" (stale) | **Accepted.** The Status records the post-round-6 amendment, round 7 and the owner's decision; row :540 points here. |
| T12-SPEC-27 | B | CONSIDER | The label "nonzero investigate a storage fault" false-alarms after every crash | **Accepted.** The interpretation names both causes: expected right after an interrupted run (idempotency healing a crash-partial delete); otherwise a vanished file or a path-mapping defect — investigate. |
| T12-SPEC-28 | B | CONSIDER | State the `to_delete`-only scope as a rule; say what happens to a missing `to_defer` file (a broken restore point) — nothing checks it today, adjacent to this delta | **Accepted; owner 2026-11-01 chose to cover it in T12.** D3.1 states the `to_delete`-only rule. Step 6 now also checks each `to_defer` file's presence and counts a missing one as `missing_deferred` (a broken restore point), without deleting or un-scheduling it; Observability and a Tests bullet surface it. The addition goes to a targeted A/B re-review. |

## Dispositions (round 8)

Round 8 (2026-11-01) is a targeted A/B re-review of the owner-directed `missing_deferred` addition, across the spec and
the plan. Reviewer A APPROVE (1 CONSIDER); Reviewer B APPROVE WITH CHANGES (3 SHOULD FIX, 3 CONSIDER); reports
`...-spec-r8-{A,B}.md`. Both confirm the detection correct and the "count and log, do not delete or un-schedule"
response proportionate. No A/B contradiction: B's SHOULD FIX on the presence-probe cost (T12-SPEC-30) is A's "awareness"
note, and B's set-membership fix satisfies both; A's CONSIDER is the same double-count as B's T12-SPEC-32. All applied
(the restore-time warning, T12-SPEC-33, the owner decided warn-and-allow — plan Task 4c). Out of round, Reviewer B then
flagged that reusing the sweep's step-7 listing would re-trigger B-16 on the deletion day; corrected — step 6 uses its
own listing and the sweep's step-7 listing is unchanged (see T12-SPEC-30 and the Missing deferred test).

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-SPEC-29 | B | SHOULD FIX | The "names no move record" clause for `to_defer` is undefined — which key is checked for a moved file; a `to_defer` key is never a `to_private` old key, so an implementation could let a moved file lost at its new key pass with count 0 | **Accepted.** D3.1 now compares by the resolved canonical key the kept set holds (`kept_file_set` maps a moved file's old key to its new key), so a relocated file is checked at its new key. A Tests case covers a moved, kept, scheduled file: new key missing counted, new key present not. |
| T12-SPEC-30 | B | SHOULD FIX | A per-file presence check repeats every run; the ~560k backlog stays `to_defer` ~7 days, so each run and `sweep_now` makes ~560k `exists` calls (API calls on HF); the cleanup has no budget | **Accepted.** D3.1 detects by set membership against a single storage listing step 6 makes itself (one list operation, not a per-file probe); the orphan sweep keeps its own step-7 listing, so step 6's deletions never feed the `sweep_max_fraction` guard (B's out-of-round listing-order catch). The backlog drain rides the Task 8 Step 3 bench. |
| T12-SPEC-31 | B | SHOULD FIX | Plan still carried the single-cause `missing_treated_as_deleted` label; the plan's label test would pin the old text against the spec's two-cause label | **Accepted.** The plan's `missing_treated_as_deleted` label now matches the spec's two-cause text (expected after an interrupted run; otherwise a vanished file or a path-mapping defect). |
| T12-SPEC-32 | B (= A CONSIDER) | CONSIDER | A file in `missing_deferred` is counted again in `missing_treated_as_deleted` when its restore point prunes — neither named cause | **Accepted.** The `missing_treated_as_deleted` interpretation now notes that a file previously in `missing_deferred` counts once here at prune — the same known fault at end of life, not new. |
| T12-SPEC-33 | B | CONSIDER | Log a bounded sample of the missing keys and the kept version seqs; show the broken restore points where a user picks one — refusing or warning on restore is the owner's scope call | **Split; owner 2026-11-01 chose warn-and-allow.** The `cleanup` log now includes a bounded sample of the missing canonical keys and the kept catalog-version sequence numbers (applied). On restore, the `restore` command warns (naming a bounded sample of the missing files) when the chosen version is a broken restore point and proceeds; it does not refuse. Spec D3.1 and plan Task 4c updated. |
| T12-SPEC-34 | B | CONSIDER | The spec Status did not record this addition or round 8; only the round-7 disposition row did | **Accepted.** The Status records the `missing_deferred` addition, spec round 8 and the open owner scope call. |
