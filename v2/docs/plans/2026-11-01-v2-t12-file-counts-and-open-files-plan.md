# v2 T12 Bounded Data-File Counts and Open-File Limits Implementation Plan

> **For agentic workers:** owner override — execute natively in one session (superpowers:executing-plans); no
> subagents, no worktrees (branch `feat/v2-ingestion`), no per-task commits. Steps use checkbox (`- [ ]`) syntax.

**Status:** Round 8 (2026-11-01): amended to spec **option (b)** (spec round 6, owner-approved 2026-11-01). The
deletion mechanism moves from "the existing cleanup collects the replaced files" to `run_cleanup` step 6 deferring the
kept-overlapping scheduled files (new **Task 4b**). Tasks 2 and 3 (open-file limit, schedule) and the Task 4
`maintenance.py` merge/rewrite are unchanged; `maintenance.py` does not un-track (that round-4 idea is withdrawn).
Plan round 3: A REQUEST CHANGES, B APPROVE WITH CHANGES (waived) — all applied. Plan round 4: A REQUEST CHANGES (1
SHOULD FIX — provenance — 1 CONSIDER), applied. **Step-6 crash idempotency: decided by the author as the gold-standard
design. The AGENTS.md idempotency invariant forces it — a cleanup that fails closed on crash-recovery is not
re-runnable, and the T3 `IntegrityError` fence guards a *tracked* file vanishing, not a file already scheduled for
deletion. `missing_treated_as_deleted` keeps a true storage fault visible (no silent degradation). It is a post-round-6
amendment (not in the round-6 reviewed version), so Reviewer A's round-5 re-review covers it; it is not an owner gate.**
Plan round 5: A REQUEST CHANGES (1 SHOULD FIX — T12-PLAN-12, the `missing_treated_as_deleted` count was not surfaced in
the log or the display — 1 CONSIDER — T12-PLAN-13); both round-4 findings confirmed resolved. T12-PLAN-12 applied
(the count is now in the `cleanup` log and the status/settings totals with inline interpretation). **One open
owner-only governance decision (T12-PLAN-13): the idempotency is a spec-level change with A-only review coverage (B
waived and never saw it); the two-reviewer discipline is the owner's to apply or waive. Recommendation: a targeted B
spec-review of the idempotency delta before the plan lands.** **Owner 2026-11-01: run the targeted B spec-review.**
Plan round 6: A APPROVE (T12-PLAN-12 resolved, verified in all five regions; no new findings). The targeted B
spec-review of the idempotency delta is dispatched (spec round 7, B only). A's minor spec note — the spec Observability
paragraph omitted `missing_treated_as_deleted` — folded into B's round and is applied. Spec round 7 (Reviewer B, the
idempotency delta): APPROVE WITH CHANGES (2 SHOULD FIX — T12-SPEC-25, -26 — 2 CONSIDER — T12-SPEC-27, -28;
`...-spec-r7-B.md`), all applied to the spec. **Owner 2026-11-01 (spec T12-SPEC-28): cover it in T12.** Step 6 also detects a missing `to_defer` file (a broken
restore point) by set membership against the storage listing and counts it in `missing_deferred`, without deleting or
un-scheduling it; the spec and the plan are updated (Task 4b, Task 5, Observability, Global Constraints). Spec round 8
(the addition, A/B): **A APPROVE (1 CONSIDER), B APPROVE WITH CHANGES (3 SHOULD FIX, 3 CONSIDER)**
(`...-spec-r8-{A,B}.md`), all applied (step-6-own-listing set membership for the presence check, the resolved-key comparison, the
two-cause label, the prune double-count note, the bounded key sample, the Status). **Owner 2026-11-01 (T12-SPEC-33):
warn and allow on a broken restore point — new Task 4c.** Reviewer B also flagged, out of round, that reusing the orphan
sweep's step-7 listing would re-trigger B-16 on the deletion day; corrected — step 6 uses its own listing, the sweep's
is unchanged. The plan is ready for owner approval, then implementation of the deletion change.

**Round 2 (2026-11-01):** Reviewers A and B APPROVE (`...-plan-r2-{A,B}.md`). **Owner-approved 2026-11-01.** Round 1: Reviewer B APPROVE WITH CHANGES (2 SHOULD FIX, 2 CONSIDER), report
`D:\Development\_reviews\2026-11-01-v2-t12-file-counts-and-open-files-plan-B.md`; dispositions at the end.
Reviewer A APPROVE (1 CONSIDER), report `...-plan-A.md`; also answered at the end. Round 2: Reviewer B APPROVE
(2 CONSIDER, applied; "Dispositions (round 2)").

**Goal:** A v2 process never fails on the number of files the lake holds: it raises its own open-file limit, the
scheduled tasks set the limit, and run-end maintenance keeps every non-bronze DuckLake table at one data file.

**Architecture:** A stdlib-only `lakehouse/limits.py` raises the soft `RLIMIT_NOFILE` at every entry point and fails
fast below a minimum. A new `catalog/maintenance.py` runs, per non-bronze table, `ducklake_rewrite_data_files` (delete
threshold 0.0) and then `ducklake_merge_adjacent_files` in time slices, inside a run budget, after a fail-closed guard
on the bronze tables' `auto_compact`. `_Run.finish` calls it after dbt and before the cleanup; `sweep_now` calls it
without a budget. The merge eager-schedules its replaced files; `run_cleanup` step 6 then deletes the scheduled files
no kept version names and defers (leaves scheduled) the ones a kept version names, deleting them through its unguarded
delete loop once no kept version does (option (b), spec D3.1). No orphan sweep and no `sweep_max_fraction` guard for
merge artifacts.

**Tech Stack:** Python 3.12, DuckDB 2.0.0 + DuckLake (`ducklake_rewrite_data_files`, `ducklake_merge_adjacent_files`,
`ducklake_list_files`), `resource` + `ctypes` (macOS `sysctlbyname`), launchd plist, systemd unit, pytest, pyright
strict, ruff, import-linter.

**Spec:** `docs/specs/2026-11-01-v2-t12-file-counts-and-open-files-design.md` (option (b), round 6: A APPROVE, B
APPROVE WITH CHANGES applied; **owner-approved 2026-11-01**; rewrite threshold 0.0).

## Implementation notes (as-built, 2026-11-01)

The T12 blocker fix is implemented in `src/lakehouse/catalog/cleanup.py` and verified: local (`ruff`, `pyright`, and the
cleanup unit + encrypted + e2e + cli suites) and the Mac gate (`karsten@192.168.68.59`, 74 passed). As-built deviations
from the reviewed spec/plan, for the Task 9 implementation review:

- **Task 4c (restore warn) dropped** — the `restore` command already refuses a broken restore point (existing
  `IntegrityError`); the warn-and-allow premise was a retracted reviewer statement. The existing refusal stands.
- **`missing_deferred` is computed inside the orphan sweep** from its single step-7 listing (the deferred keys are
  passed in), not from a separate step-6 listing. One listing per run, and step 6's `to_delete` deletions never enter
  `sweep_max_fraction` (the sweep keys them out as kept). Same B-16 safety the round-9 spec text intended, one fewer
  listing; the spec's "step 6 takes its own listing" describes the intent, this is the cheaper equivalent.
- **`_ducklake_dry_run` and the B-23 parity test removed** — option (b) never calls `cleanup_all`, so there is no second
  source to keep in parity. The scheduled-table upgrade guard (`test_scheduled_table_contract.py`, Task 4b Step 3)
  covers the read's correctness across DuckLake upgrades.
- **The labeled status/settings display** of the counts is deferred to **Task 10** (the live-lake upgrade), where the
  `RunRow` schema change and its migration live; the counts already reach the `cleanup` JSON log, the production signal.
  Owner-agreed 2026-11-01.
- **Mac gating note:** run the plain test suite with `LAKEHOUSE_ENCRYPTION_KEY` unset; the live key in `env.sh` makes
  the generic harness build encrypted temp lakes, and `ducklake_add_data_files` is unsupported for encrypted catalogs.

## Global Constraints

- `OPEN_FILES_TARGET = 65_536`, `OPEN_FILES_MINIMUM = 4_096`; the raise never lowers a limit and never raises an
  exception; the minimum check raises `ConfigError` (exit 2) for catalog-opening commands only.
- launchd: `SoftResourceLimits {NumberOfFiles: 65536}` and no `HardResourceLimits`; systemd: `LimitNOFILE=65536:524288`.
- `REWRITE_DELETE_THRESHOLD = 0.0` (owner), `MERGE_MIN_FILES = 16`, `MERGE_SLICE_FILES = 20_000`,
  `maintenance.merge_budget_seconds` default 300 (both steps together); `sweep_now` has no budget.
- Bronze tables keep `auto_compact = false` and are never rewritten or merged; the guard fails closed before any
  maintenance.
- No new deletion path: a rewritten or merged file leaves only through `run_cleanup` step 6. Step 6 partitions
  DuckLake's scheduled list by the kept set, deletes `listed − kept` (by `data_file_id`, batched) and defers
  `listed ∩ kept` (leaves them scheduled), replacing the round-1–3 fatal overlap guard and the `cleanup_all => true`
  call; it is idempotent (an already-missing scheduled file is treated as deleted, not `IntegrityError`). The orphan
  sweep and its `sweep_max_fraction` guard are not involved for merge artifacts. The rewrite scheduling or deleting a
  replaced file before expiry is a stop-rule case.
- The cleanup logs the deferred count, the oldest deferred `schedule_start`, `missing_treated_as_deleted` and
  `missing_deferred`; the status/settings totals show all four with inline interpretation. A stuck deferral (the count
  not falling, the oldest ageing past `snapshot_days + version_days`), any nonzero `missing_treated_as_deleted` and any
  nonzero `missing_deferred` (a broken restore point) are the signals to investigate (T12-PLAN-12, T12-SPEC-28).
- The one-time backlog deletes about 560k files in one eligibility run through step 6 (batched, unbudgeted by design).
  **Task 8 Step 3 (the Mac bench) is the single owner of this measurement**; it times one full-scale step-6 pass
  against the scheduled run window = the ingest-task budget, 15 min / 900 s (source: AGENTS "Budgets" /
  `docs/context/data-performance.md`). A pass that does not fit is a stop-rule case for the owner.
- The maintenance adds no publish; the run-end publish (T3 D1) carries it.
- Heavy runs (full gate, bench, the live-lake upgrade) on the Mac Mini, never the Windows AI-PC; HF runs on the test
  image `karstenskyt/jobs-test`, the test buckets and `lake-test-control`, within 60 commit attempts per hour and
  outside 02:30–03:30 UTC.
- The live-lake upgrade (Task 10) backs up the live lake and its restricted root first and restores on any failure.
- No commit: T12 rides the single pre-commit-program commit, after the owner's explicit approval (Task 11).

## Review Focus

1. A register-path bronze file merged or rewritten: the guard fails closed before any call (Task 4,
   `test_guard_stops_before_any_maintenance`).
2. A replaced file deleted while a kept version names it: `run_cleanup` step 6 defers it (leaves it scheduled, no
   `cleanup_all => true`) and deletes it only once no kept version names it (Task 4b,
   `test_cleanup_defers_kept_overlapping_and_deletes_when_pruned`, `test_large_run_deletes_through_step6_no_failed_run`).
3. Maintenance on a stopped run, or a crash inside it: no maintenance when stopped; orphans swept next run (Task 5,
   `test_no_maintenance_on_a_stopped_run`; Task 4, `test_crash_inside_maintenance_then_the_next_run`).
4. A limit the kernel refuses (macOS above `kern.maxfilesperproc`): capped, with the halving fallback (Task 2,
   `test_limit_capped_by_maxfilesperproc`, `test_fallback_halves_until_accepted`).
5. The first run on the live lake exceeding its window: the budget stops new slices, the upgrade starts with the
   unbudgeted sweep (Task 4, `test_budget_spent_stops_new_slices`; Task 10).

**Execution order (Reviewer A observation):** Task 0, then the Task 1 spike (its stop rules gate everything after
it), then Tasks 2 and 3, then Task 4 (maintenance), Task 4b (the option-(b) cleanup deferral) and Task 5 (wiring,
totals, sweep CLI) — they consume the Task 1 slicing mechanism — then Tasks 6 to 11 in order.

---

### Task 0: Ledger

- [ ] **Step 1:** The ledger `t12/progress.md` in the session scratchpad names this plan on its first line (it holds the
  spec evidence already). Ruling carried from T3: one Mac gate covers T3, T6, T9-T13 and T12 together (Task 8).

### Task 1: The spike (Mac; stops for the owner on any stop rule)

**Files:** none in the repo; scripts in the scratchpad `t12/`.

- [ ] **Step 1:** On a throwaway encrypted lake (`DATA_INLINING_ROW_LIMIT 0`), 40,000 one-row inserts into one table
  spread over at least four distinct snapshot-time windows; read the live files' snapshot times from the catalog
  metadata; call `ducklake_merge_adjacent_files('lake', <table>, newer_than => t0, older_than => t1)` per window.
  Pass: each call merges only the files of its window (the others stay). **Stop:** no parameter bounds a merge call.
- [ ] **Step 2:** On the same kind of lake, files with delete files above 20,000: does `max_compacted_files` bound a
  `ducklake_rewrite_data_files` call? Record yes or no.
- [ ] **Step 3:** Probe deferred deletion for both steps on 20,000 files, reading
  `ducklake_files_scheduled_for_deletion` (not only disk): the rewrite keeps its replaced files as ended rows and
  schedules them only after an expiry; the merge eager-schedules its replaced files (expected under option (b)).
  **Stop:** the rewrite schedules or deletes a replaced file before expiry. (The round-4/5 probes
  `t12_rowtrack_probe.py` / `t12_optionb_probe.py` already established this; Step 6 records them.)
- [ ] **Step 4 (live lake, read-only reader):** per ops table, the largest number of data files one snapshot added
  (Reviewer A T12-SPEC-01; **stop** above `MERGE_SLICE_FILES`); the number and total size of files that carry a delete
  file (to size the first rewrite; **stop** if the rewrite cannot be bounded and that volume would not fit the
  budget); the time of one filtered query on `ops_copy_fields` (`WHERE match_key = <one key>`, Reviewer B
  T12-SPEC-09).
- [ ] **Step 5:** Record every number in the ledger; set the slicing mechanism in Task 4 from Step 1.
- [x] **Step 6 (option (b), done 2026-11-01):** the row-tracking probe `t12_rowtrack_probe.py` (plain + encrypted) —
  a merge removes the replaced files' `ducklake_data_file` rows and schedules them (live 1, ended 0, scheduled 20);
  `ducklake_rewrite_data_files(0.0)` keeps ended rows and schedules only after an expiry. The option-(b) probe
  `t12_optionb_probe.py` — after an expiry `cleanup_all`'s list equals the scheduled table; a subset delete by
  `data_file_id` without `cleanup_all` leaves the other scheduled rows, keeps the table queryable and mergeable, and a
  later `cleanup_all` deletes the deferred. These gate Task 4b. **The backlog step-6 time at scale is measured by the
  Task 8 Step 3 Mac bench** (the upgrade run cannot wait `version_days` for the real eligibility day).

### Task 2: The open-file limit (D1)

**Files:** create `src/lakehouse/limits.py`, `tests/unit/test_limits.py`, `tests/unit/test_entry_points_raise_limit.py`,
`tests/os/test_open_file_limit.py`; modify `src/lakehouse/cli.py` (`main`, the catalog-command check),
`src/lakehouse/web/__main__.py`, `src/lakehouse/mask.py` (`main`), `src/lakehouse/schedule/windows.py` (`main`).

**Interfaces:**
- Produces: `OPEN_FILES_TARGET`, `OPEN_FILES_MINIMUM`; `@dataclass(frozen=True) OpenFileLimit(soft_before: int,
  soft_after: int, hard: int)`; `raise_open_file_limit() -> OpenFileLimit | None`;
  `require_open_file_minimum(limit: OpenFileLimit | None) -> None` (raises `ConfigError`).

- [ ] **Step 1:** Write the failing unit tests with a stubbed `resource` and `sysctlbyname` (monkeypatched module
  attributes): `test_soft_below_target_is_raised`, `test_limit_capped_by_finite_hard`,
  `test_limit_capped_by_maxfilesperproc` (darwin), `test_soft_at_or_above_target_is_unchanged` (1,048,576 stays),
  `test_windows_returns_none`, `test_fallback_halves_until_accepted` (`setrlimit` refuses 65,536 and 32,768, accepts
  16,384), `test_every_value_refused_keeps_the_limit`, `test_minimum_check_names_limit_and_fix` (the message holds
  the limit, 4,096 and `ulimit -n`/`NumberOfFiles`/`LimitNOFILE`, no other value). Run: FAIL (module missing).
- [ ] **Step 2:** Implement `limits.py` (stdlib only; `ctypes` for `sysctlbyname("kern.maxfilesperproc")`; one JSON
  log line `open-file limit`). Run Step 1: PASS.
- [ ] **Step 3:** Write both entry-point tests, then run them RED before any wiring (Reviewer B T12-PLAN-03):
  `test_entry_points_raise_limit.py` (AST): each of `lakehouse.cli.main`, `lakehouse.web.__main__` (the `if __name__`
  block), `lakehouse.mask.main` and `lakehouse.schedule.windows.main` calls `raise_open_file_limit` as its first
  statement, and the set equals `[project.scripts]` plus every `__main__` module under `src/lakehouse`; and
  `tests/os/test_open_file_limit.py` (macOS and Linux; `LAKEHOUSE_OS_TESTS=1`): a subprocess with soft limit 256
  (`preexec_fn` lowering it) runs `lakehouse verify` on a lake whose ops table holds 300 files. Run both (the OS test on
  the Mac): FAIL (the AST test; the OS test with `Too many open files`). Then wire the calls; the catalog-opening CLI
  commands (listed from `cli.py`: at least `init`, `run`, `reparse`, `restore`, `relocate`, `sweep`, `verify`) and the
  web start call `require_open_file_minimum`. Run both again: PASS.

### Task 3: The scheduled tasks (D2)

**Files:** modify `src/lakehouse/schedule/launchd.py` (`render_plist`), `src/lakehouse/schedule/systemd.py`;
`tests/unit/test_schedule_render.py`.

- [ ] **Step 1:** Failing tests `test_plist_raises_the_soft_open_file_limit_only` (`SoftResourceLimits` equals
  `{"NumberOfFiles": 65536}`, no `HardResourceLimits`) and `test_unit_sets_limit_nofile` (`LimitNOFILE=65536:524288`).
  Run: FAIL. Implement from `limits.OPEN_FILES_TARGET`. Run: PASS.

### Task 4: Run-end maintenance (D3)

**Files:** create `src/lakehouse/catalog/maintenance.py`, `tests/unit/test_maintenance.py`,
`tests/e2e/test_maintenance_crash.py`.

**Interfaces:**
- Produces: `REWRITE_DELETE_THRESHOLD`, `MERGE_MIN_FILES`, `MERGE_SLICE_FILES`; `@dataclass(frozen=True)
  TableMaintenance(table: str, files_before: int, deletes_before: int, rewritten: bool, files_after: int, slices: int,
  ms: int)`; `@dataclass(frozen=True) MaintenanceReport(tables: tuple[TableMaintenance, ...], budget_spent: bool,
  left: tuple[str, ...], ms: int)`; `maintain_tables(cat: WorkingCatalog, *, budget_s: float | None,
  monotonic: Callable[[], float] = time.monotonic) -> MaintenanceReport`.

- [ ] **Step 1:** Write the failing tests (register and native paths where they differ):
  `test_guard_stops_before_any_maintenance` (a bronze table with `auto_compact = true`: `GuardStopError`, no file
  changed); `test_small_clean_table_is_left_alone` (16 files, no deletes: no call, one count query);
  `test_ops_table_becomes_one_file` (100 files: 1 file, rows and content checksums equal, bronze files unchanged by
  key and bytes, encrypted lake has no cleartext); `test_partial_deletes_end_each_run_at_one_file` (5 simulated runs
  with an `UPDATE` and a `DELETE` each: 1 file and no delete file after each; with the rewrite step removed the test
  fails); `test_slices_bound_each_merge_call` (`MERGE_SLICE_FILES` patched to 10, 35 files: four calls, each within its
  window); `test_budget_spent_stops_new_slices` (a fake `monotonic` past the budget after the first slice: the report
  names the tables left; the next call continues); `test_rewrite_files_leave_through_the_cleanup` (the **rewrite's**
  replaced files stay on disk as ended rows and time travel reads them; after `snapshot_days` the existing
  `run_cleanup` deletes them — no kept-version guard under option (b); the **merge**-artifact defer-then-delete
  lifecycle at `version_days` is Task 4b, not here). Run: FAIL (module missing).
- [ ] **Step 2:** Implement `maintain_tables`: the guard (bronze tables are the tables with the `BRONZE_COMMENT`
  mark, the same mark the T3 guard uses; their `auto_compact` option from the catalog metadata); per non-bronze table the live-file and delete counts (`ducklake_list_files`);
  the rewrite (threshold 0.0) then the merge in slices chosen by snapshot time (the Task 1 mechanism); the budget
  check before each slice and table; one `data files merged` log line per table and a `merge budget spent` line.
  Run Step 1: PASS.
- [ ] **Step 3:** `tests/e2e/test_maintenance_crash.py`: a `_Crash(BaseException)` from the merge hook after the
  rewrite (and one from inside the rewrite), then the next run: the orphans are swept (T3 age fence) and the
  maintenance completes; the published catalog never names a file that is not stored. Under option (b) the cleanup has
  no fatal overlap guard, so this test encodes only the crash→sweep→redo lifecycle (it passed on Windows; the round-3
  "guard trip" label is void — there is no overlap guard to trip); the defer-then-delete lifecycle lives in Task 4b.
  The Mac gate (Task 8) confirms it. Run: PASS.

### Task 4b: Option (b) deferral in `run_cleanup` (D3.1)

**Files:** modify `src/lakehouse/catalog/cleanup.py` (`run_cleanup` step 6, the scheduled-table read / `_ducklake_dry_run`,
`preview_cleanup`, `CleanupPreview`, `CleanupReport`); tests `tests/unit/test_cleanup.py`, a lifecycle e2e under
`tests/e2e/`, `tests/unit/test_scheduled_table_contract.py`.

**Interfaces:**
- A shared partition helper over the scheduled table: reads `__ducklake_metadata_lake.ducklake_files_scheduled_for_deletion`
  (`data_file_id`, `path`, `path_is_relative`), maps each row to a `CanonicalKey` with the `_tracked_refs` resolution,
  and returns `(to_delete, to_defer)` against the kept-version set. `run_cleanup` and `preview_cleanup` both use it.
- `CleanupReport` / `CleanupPreview` gain `deferred: int`, `oldest_deferred: datetime | None`, and
  `missing_treated_as_deleted: int` and `missing_deferred: int` (the idempotent count and the broken-restore-point
  count; both surfaced in the `cleanup` log and the status/settings display, not only carried as fields, so a storage
  fault and a vanished deferred file stay visible on a background run, B-10, T12-PLAN-12, spec T12-SPEC-28). A
  `to_defer` file whose canonical key is absent from the storage listing is counted in `missing_deferred` (set
  membership against a single storage listing step 6 makes itself — one list call, not a per-file probe; not the
  sweep's step-7 listing, so step 6's deletions never feed the `sweep_max_fraction` guard, spec T12-SPEC-30); the
  comparison uses the resolved key the kept set holds, so a moved file is checked at its new key (spec T12-SPEC-29).
  Step 6 does not delete or un-schedule it, because the fault is upstream.
- A step-6 / cleanup error raises and fails the run before the publish; `run_cleanup` is not in the catch-and-publish
  path, so only the pre-merge bronze guard raises `GuardStopError` (spec B-15, A T12-PLAN-09).

- [ ] **Step 1 (RED first):** Write the failing tests:
  - `test_cleanup_defers_kept_overlapping_and_deletes_when_pruned`: a merge schedules files a kept version names; step
    6 deletes `to_delete` and leaves `to_defer` in the scheduled table, does not call `cleanup_all => true`, a restore
    still reads a deferred file; after the kept version prunes (clock past `version_days`) the next run's step 6
    deletes it with no `GuardStopError`. Asserts the scheduled-table rows directly and that step 6 (not `_sweep`)
    deleted each.
  - `test_large_run_deletes_through_step6_no_failed_run` (B-16): an ops table written at reparse/backfill scale, then
    the eligibility day: the merged-away files delete through step 6 and the scheduled run completes; `sweep_max_fraction`
    never fires on them. Routed through `_sweep` instead, the run fails on the guard.
  - `test_step6_is_idempotent_on_a_missing_scheduled_file`: an already-missing scheduled file that names no move record
    is treated as deleted and counted in `missing_treated_as_deleted`, not `IntegrityError` (spec D3.1 + Crash-safety;
    the count's two-cause label — expected after an interrupted run, otherwise a vanished file or a path-mapping
    defect, spec T12-SPEC-27/-31).
  - `test_step6_flags_missing_deferred_as_broken_restore_point` (spec T12-SPEC-28, -29, -30): a `to_defer` file absent
    from the storage listing, then a run: step 6 counts it in `missing_deferred` by set membership (not a per-file
    probe), logs the broken-restore-point signal with the bounded key sample, and leaves its scheduled row in place (no
    delete, no un-schedule); a nonzero count surfaces in the log and the display. A separate case covers a moved, kept,
    scheduled file (`kept_file_set` holds the new key): the new key missing is counted, the new key present is not.
  - `test_scheduled_row_keys_equal_dry_run_keys` (B-23): the resolved scheduled-row keys equal the `_ducklake_dry_run`
    keys.
  - `test_preview_partitions_and_expect_ignores_deferred` (B-20): `preview_cleanup` reports `to_delete` as deletes and
    `to_defer` separately; `sweep --allow-large --expect <to_delete>` across a kept-scheduling merge does not stop on
    the deferred files; the old whole-list behaviour stops.
  - `test_cleanup_logs_deferred_count_and_oldest` (B-22, T12-PLAN-12, T12-SPEC-28): the `cleanup` log and
    `CleanupReport` carry the deferred count, the oldest deferred `schedule_start`, `missing_treated_as_deleted` and
    `missing_deferred`; the status/settings display shows all four with inline interpretation (deferred: normal vs a
    stuck deferral older than `snapshot_days + version_days`; `missing_treated_as_deleted`: 0 normal / nonzero
    expected after an interrupted run else investigate a vanished file or a path-mapping defect; `missing_deferred`: 0
    normal / nonzero a restore point is broken, investigate), and
    the test asserts each label (B-11, the UI rule). Surfacing `missing_treated_as_deleted` and `missing_deferred` in
    the log and the display, not only as report fields, is what makes the "storage fault stays visible" and
    "broken restore point stays visible" claims true on a background run (T12-PLAN-12, T12-SPEC-28).
  Run: FAIL.
- [ ] **Step 2:** Implement in `cleanup.py`: the shared partition helper; `run_cleanup` step 6 deletes `to_delete`
  (storage delete + remove the scheduled row by `data_file_id`, batched, idempotent on a missing file) and leaves
  `to_defer`, replacing the fatal overlap guard and dropping the `cleanup_all => true` call (the option-(b) probe
  showed the per-row path keeps DuckLake consistent; a `cleanup_all => true` may be kept only for the runs where
  `to_defer` is empty — builder's choice, "Left to the builder"); `preview_cleanup` / `CleanupPreview` use the same
  partition and report deferred separately; the `cleanup` log and `CleanupReport` carry `deferred`,
  `oldest_deferred`, `missing_treated_as_deleted` and `missing_deferred`; step 6 detects a missing `to_defer` file by
  set membership against a single storage listing step 6 makes itself (one list call; the orphan sweep keeps its own
  step-7 listing), by canonical key, the kept set's resolved key, counts it in `missing_deferred` and logs the bounded
  key sample, without deleting or un-scheduling it (spec T12-SPEC-29/-30/-33). Run Step 1 + `tests/unit/test_cleanup.py`: PASS.
- [ ] **Step 3 (upgrade guard):** `tests/unit/test_scheduled_table_contract.py` pins the scheduled-table columns
  (`data_file_id`, `path`, `path_is_relative`, `schedule_start`), that a merge populates it and removes the replaced
  files' `ducklake_data_file` rows, that a rewrite keeps ended rows and schedules only after an expiry, and that a
  subset delete by `data_file_id` without `cleanup_all` leaves the other rows; a DuckLake upgrade that changes any of
  these fails it (Reviewers A T12-SPEC-12, B-14/B-18). Run: PASS.

### Task 4c: Warn on a broken restore point — DROPPED (2026-11-01)

Dropped during implementation. Reviewer B's round-9 found the `restore` command already refuses a broken restore point
(an existing `IntegrityError` when the restored copy names files not in storage), which is the safe behaviour. The
owner's warn-and-allow decision (T12-SPEC-33) rested on B's round-8 statement that restore published a broken version,
which B then retracted. So there is no warn to add; the existing refusal stands unchanged.

### Task 5: Wiring, the setting and the totals (D3)

**Files:** modify `src/lakehouse/runs/run.py` (`_Run.finish`), `src/lakehouse/catalog/cleanup.py` (`sweep_now`),
`src/lakehouse/config/model.py`, `src/lakehouse/config/defaults.yaml`, `src/lakehouse/ui/pages/settings.py`,
`src/lakehouse/runs/checkpoint.py` (totals); tests `tests/unit/test_run_steps.py`, `tests/unit/test_cleanup.py`,
`tests/unit/test_config_model.py`, `tests/unit/test_pages_display.py`.

- [ ] **Step 1:** Failing tests: `test_maintenance_runs_after_dbt_before_cleanup` (call order through spies);
  `test_no_maintenance_on_a_stopped_run`; `test_maintenance_guard_stop_fails_the_run` (Reviewer B T12-PLAN-04: a bronze
  table with `auto_compact = true`: the run records a `maintenance_guard` error with the `GuardStopError` text, merges
  nothing, still runs the cleanup and the run-end publish of its ingested data, and ends `failed`, exit 1); `test_sweep_now_maintains_without_a_budget`;
  `test_merge_budget_setting_default_and_bounds` (default 300, at least 0); the settings page shows the field with its
  unit (seconds) and a one-line explanation; `catalog totals` carries `merged_files` and `merge_ms`, and the status /
  settings totals carry the deferred count, the oldest deferred `schedule_start`, `missing_treated_as_deleted` and
  `missing_deferred` with inline interpretation (0 normal / nonzero investigate; B-22, T12-PLAN-12, T12-SPEC-28). Also update
  `tests/unit/test_run_lease_check.py::test_confirm_runs_before_resolution_and_dbt` to expect **3** `finish` confirms
  (resolution, dbt, maintenance): the maintenance is a real write step (T3 spec D5), so this is a justified
  expectation update, not a silenced finding. Run: FAIL.
- [ ] **Step 2:** Implement: `finish` calls `maintain_tables(self.cat, budget_s=...)` after `lease.confirm()` in a
  `lakehouse.run.merge` span; a `GuardStopError` from it is recorded as a run error (the run goes on to the cleanup and
  the publish of its data, and ends `failed`); `sweep_now` calls it with `budget_s=None` before its cleanup. Run Step 1
  and the run, cleanup and config suites: PASS.
- [ ] **Step 3 (Reviewer B T12-PLAN-02):** `lakehouse sweep --dry-run`: failing test
  `test_sweep_dry_run_lists_without_deleting` (it prints the doomed count, the exempted count and every doomed key,
  deletes nothing, runs no maintenance and publishes nothing; it needs no `--allow-large`) and writes the doomed keys
  to a list file; `test_allow_large_deletes_only_expected_keys` (Reviewer B T12-PLAN-05: `sweep --allow-large --expect
  <list file>` computes its own doomed set, including the version pruning at its own `now`, and stops with a
  `GuardStopError` naming every key not in the list before it deletes anything; with every key in the list it deletes
  them). Implement both on the cleanup's own computation, using the Task 4b shared partition: the preview reports
  `to_delete` (plus the orphan sweep's doomed set) as deletes and the `to_defer` merge artifacts separately, and
  `--expect` checks only the deletes, not the deferred (B-20). Run: PASS.

### Task 6: HF tier (test image)

**Files:** create `tests/hf/test_maintenance_hf_live.py` (`hf`, `hf_job`).

- [ ] **Step 1:** On the HF test lake: an ops table above the threshold (inserts plus one `UPDATE`) is rewritten and
  merged to one file through the S3 gateway path; rows equal; the time is printed. Deploy `jobs-test` and run it with
  the hf_job tier (`LAKEHOUSE_CONTROL_REPO=luxury-lakehouse/lake-test-control`), within the commit budget. Expected:
  PASS.

### Task 7: Docs and amendments

**Files:** `docs/specs/2026-10-26-v2-ingestion-design.md` (§10.2, §10.3, §10.8), `docs/adrs/ADR-001-storage-engine.md`,
`docs/ops/deploy.md`, `docs/audits/2026-10-30-v2-audit-ledger.md` (T12 implemented).

- [ ] **Step 1:** Apply the spec's Amendments **to option (b)**. §10.2 (bronze `auto_compact = false`; every other
  table merged at run end), §10.3 (the step-6 merge step), §10.8 (a rewritten or merged file leaves through
  `run_cleanup` step 6; step 6 defers a scheduled file a kept version names and deletes it once no kept version does;
  no new deletion path), ADR-001 (the run-end merge replaces DuckLake compaction for non-bronze; `CHECKPOINT` stays
  forbidden; step 6 defers kept-overlapping scheduled files and deletes scheduled files by `data_file_id` in place of
  `cleanup_all => true`), deploy.md (the open-file limit, the minimum and its error, `lakehouse schedule install` for
  the plist/unit keys, the maintenance budget setting, the deferred-count signal). These files were written in the
  prior turn asserting the disproven premise; rewrite them to option (b). Run `uv run pytest tests/contract -q`.
  Expected: PASS.

### Task 8: Full gate and bench (Mac Mini)

- [ ] **Step 1:** Push the tree; run `mac_gate.sh t12-gate`. Expected: green.
- [ ] **Step 2:** Add `tests/bench/test_maintenance_scale.py` (`bench`): maintenance of 20,000 and 100,000 one-row
  files (encrypted), times recorded against the spike; run it on the Mac. Expected: PASS within the budget per slice.
- [ ] **Step 3 (B-21, A T12-SPEC-16, step-6 backlog burst — the single owner of this measurement):** add a bench that
  schedules the full backlog scale on the Mac (about 560k files; about 2.2 GB, so full scale fits the production host)
  and times one `run_cleanup` step-6 pass that deletes them all in one run (the batched per-row `DELETE` + the storage
  deletes), encrypted. Record the time against the scheduled run window = the ingest-task budget, 15 min / 900 s
  (source: AGENTS "Budgets" / `docs/context/data-performance.md`). **Stop** for the owner if one pass does not fit that
  window. GC, Task 1 Step 6 and Task 10 Step 4 point here; the upgrade run cannot wait `version_days` for the real
  eligibility day.

### Task 9: A/B implementation review

- [ ] **Step 1:** Send the implementation review (scope: every file of Tasks 2-7, including Task 4b — the `cleanup.py`
  option-(b) deferral — the ledger and the Task 1 evidence, and the whole T11+T12+T13 change set per the handoff) to
  Reviewers A and B; fix or answer every finding; take disagreements to the owner. Then write the sha256 list of the
  reviewed tree (`v2/` without `.venv`, caches and build output) into the ledger: Task 10 runs on exactly that tree.

### Task 10: The live-lake upgrade, on the reviewed tree (owner decision 2026-11-01; T3 review B-01)

Runs after Task 9 on the hash-pinned tree (Reviewer B T12-PLAN-01). A later fix in the Task 2-5 files or the T3 code
repeats this task.

- [ ] **Step 0:** Push the reviewed tree to the Mac and check it against the Task 9 sha256 list.
- [ ] **Step 1:** Stop and confirm no lakehouse process runs (the launchd task between runs, no web run); copy
  `/Volumes/Development/lakehouse/lake` and `lake-restricted` to `/Volumes/Development/lakehouse/backup-2026-11-01/`;
  verify the copy (file count and total bytes equal).
- [ ] **Step 2:** Measure before the drain (read-only): the peak descriptors of one scan of `ops_copy_fields`
  (Reviewer A T12-SPEC-02; if above 4,096, raise `OPEN_FILES_MINIMUM` to twice the value before the cycle closes).
- [ ] **Step 3:** Run `lakehouse sweep --dry-run` (Reviewer B T12-PLAN-02): record the doomed, exempted and total
  object counts and keep the doomed key list in the ledger folder. **Stop for the owner** if the doomed share is above
  `sweep_max_fraction` (a pre-T3 lake has no run anchors) or any doomed key is not an orphan by the T3 rules.
- [ ] **Step 3b (Reviewer A T12-PLAN-01):** **Stop for the owner's go or no-go** on the first drain: show the Task 1
  Step 4 numbers (files with delete files, their size, whether a rewrite call can be bounded, the filtered-query time)
  and the Step 3 dry-run counts. The drain of Step 4 runs unbudgeted, so it runs only on the owner's explicit go.
- [ ] **Step 4:** Run `lakehouse sweep --allow-large --expect <Step 3 list file>` with the new code under
  `.venv/bin/python` (launchd's soft limit, so D1 is exercised): it deletes only keys of the recorded list and stops
  before any delete if its own computation finds another (Reviewer B T12-PLAN-05). Under option (b) the merge collapses
  the live file count to one per table, and the merged-away backlog files are **deferred** (the current version names
  them), so this drain deletes only genuine orphans, not the ~560k merge artifacts; `--expect` lists the deletes, not
  the deferred (B-20). The ~560k step-6 burst comes due about `version_days` later on a normal scheduled run; its time
  is measured by the Task 8 Step 3 bench, not here. Record the maintenance report (files before and after per table,
  rewrite and merge time, footprint), the deferred count and oldest deferred `schedule_start`, and the doomed,
  exempted and deleted counts.
- [ ] **Step 5:** Run a real ingestion (`.venv/bin/python -m lakehouse.cli run`) against the live lake: status, failed
  count, run time, footprint, file counts after the run-end maintenance, the catalog size and the T3 bounds.
- [ ] **Step 6:** On any failure, stop, restore the backup (move the failed lake aside, copy the backup back), and bring
  the evidence to the owner. On success, keep the backup until the owner's commit approval, then delete it on the
  owner's word.

### Task 11: Commit gate (owner)

- [ ] **Step 1:** Send the Task 10 evidence to Reviewers A and B (a delta note; no code changed after Task 9). Stop for
  the owner: show the full change set of the pre-commit program (the whole untracked `v2/`
  tree and `.github/workflows/v2-ci.yml`) and wait for an explicit yes for that specific commit. No commit, push or PR
  without it.

## Dispositions (round 1)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-PLAN-01 | B | SHOULD FIX | The live-lake upgrade ran before the A/B review | **Accepted.** The review is Task 9; the upgrade is Task 10 on the hash-pinned reviewed tree; a later fix in the Task 2-5 files or the T3 code repeats it. |
| T12-PLAN-02 | B | SHOULD FIX | `sweep --allow-large` lifts the large-sweep guard on a pre-T3 lake | **Accepted.** New `lakehouse sweep --dry-run` (Task 5 Step 3) on the cleanup's own computation; Task 10 Step 3 records the counts and keys and stops for the owner above `sweep_max_fraction`; Step 4's counts must match. Spec D4 amended. |
| T12-PLAN-03 | B | CONSIDER | The OS test's RED run must come before the wiring | **Accepted.** Task 2 Step 3 runs both entry-point tests RED before wiring. |
| T12-PLAN-04 | B | CONSIDER | Add a run-level guard test; say how a guard stop ends the run | **Accepted.** `test_maintenance_guard_stop_fails_the_run`; the error is recorded, the run publishes its data and ends `failed`. |
| T12-PLAN-01 | A | CONSIDER | The first drain is unbudgeted, so the rewrite stop rule cannot trigger there | **Accepted.** Task 10 Step 3b stops for the owner's go or no-go with the Task 1 Step 4 numbers and the dry-run counts before the unbudgeted drain. |
| observation | A | — | State the execution order; name the bronze registry | **Accepted.** An execution-order line before Task 0; Task 4 names the `BRONZE_COMMENT` mark. |

## Dispositions (round 2)

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-PLAN-05 | B | CONSIDER | Step 4's equality with the dry run is checked only after the deletes; its pruning runs at its own `now` | **Accepted.** `sweep --allow-large --expect <list file>` deletes only listed keys and stops before any delete on a key outside the list (`test_allow_large_deletes_only_expected_keys`); Task 10 Step 4 uses it. |
| T12-PLAN-06 | B | CONSIDER | Two places still name Task 9 for the upgrade | **Accepted.** Both now name Task 10. |

## Dispositions (round 3) — amendment to spec option (b)

The spec's merge-deletion mechanism changed across spec rounds 4–6 (gate finding → Candidate A orphan-sweep → B-16 →
owner chose option (b): the cleanup owns the deferral). This plan round 3 carries option (b):

| Change | Where |
|---|---|
| Deletion moves to `run_cleanup` step 6 (partition the scheduled list, delete `listed − kept` by `data_file_id`, defer `listed ∩ kept`, drop `cleanup_all => true`, idempotent, batched); `maintenance.py` does not un-track | New **Task 4b**; Architecture; Global Constraints; Review Focus 2 |
| Preview / `--expect` use the shared partition, report deferred separately (B-20) | Task 4b Step 1; Task 5 Step 3 |
| Deferred-count + oldest `schedule_start` in the cleanup log and the status/settings totals (B-22) | Task 4b; Task 5 Step 1; Task 7 (deploy.md) |
| The one-time ~560k backlog deletes in one eligibility run through step 6, batched; timed by a bench at scale (B-21, A T12-SPEC-16) | Task 8 Step 3; Task 10 Step 4; Global Constraints |
| Crash idempotency: an already-missing scheduled file is treated as deleted, not `IntegrityError` (B note) | Task 4b Step 1; Global Constraints |
| DuckLake-upgrade guard test (scheduled-table columns, merge row-removal, rewrite keeps-ended-rows, subset-delete mechanic) (B-18, A T12-SPEC-12) | Task 4b Step 3 |
| `test_maintenance_crash.py` encodes only the crash→sweep→redo lifecycle (no fatal overlap guard under (b)); the round-3 "guard trip" label is void; the confirm-count test expects 3 (maintenance is a real write step) | Task 4 Step 3; Task 5 Step 1 |
| Option-(b) feasibility proven by `t12_rowtrack_probe.py` + `t12_optionb_probe.py` | Task 1 Step 6 |
| Downstream docs (§10.2/§10.3/§10.8, ADR-001, deploy.md) rewritten to option (b) | Task 7 Step 1 |

Tasks 2 and 3 (open-file limit, schedule) and the Task 4 `maintenance.py` merge/rewrite are unchanged.

### Plan round-3 review dispositions (A and B)

Plan round 3: A REQUEST CHANGES (2 SHOULD FIX, 1 CONSIDER); B APPROVE WITH CHANGES (3 SHOULD FIX, 2 CONSIDER, B
waived a further round). All applied below. Pending Reviewer A's round-4 re-review and owner approval before
implementation.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-PLAN-07 | A | SHOULD FIX | The plan's step-6 idempotency diverges from the approved spec D3.1 ("`IntegrityError` unchanged"), attributed only to "(B note)" | **Resolved (provenance corrected — A T12-PLAN-10).** The idempotency is a **new, post-round-6 change**: it was added when applying Reviewer B's round-6 crash *note* (which B flagged "outside this round"); it was NOT in the version A and B reviewed in round 6, and no reviewer formally reviewed it. It drops the T3 `IntegrityError` fail-loud guard for a missing-non-moved scheduled file — a safety-adjacent behaviour change. It is correct for option (b) crash recovery (a crash-partial step-6 produces exactly this state; `missing_treated_as_deleted` keeps a true storage fault visible). **Decided by the author as the gold-standard design: the AGENTS.md idempotency invariant forces it (a cleanup that fails closed on crash-recovery is not re-runnable). Recorded as a post-round-6 amendment, so Reviewer A's round-5 re-review covers it — not an owner gate.** Spec D3.1 and Crash-safety now agree on it. |
| T12-PLAN-08 | A | SHOULD FIX | Task 4's `test_replaced_files_leave_only_through_the_cleanup` asserts the superseded pre-(b) merge semantics (kept-version guard, `snapshot_days`) | **Accepted (= B-09b).** Renamed `test_rewrite_files_leave_through_the_cleanup`, scoped to the rewrite (`snapshot_days`, no guard); the merge lifecycle (`version_days`) is Task 4b. |
| T12-PLAN-09 | A | CONSIDER | B-15 (a step-6/cleanup error fails the run before publish; only the bronze guard raises `GuardStopError`) is implicit | **Accepted.** Restated in Task 4b Interfaces. |
| B-07 | B | SHOULD FIX | Approval record: the spec Status said "pending" while the plan cited "owner-approved round 6"; the plan cited spec "round 3" | **Accepted.** The spec Status + round-6 Dispositions record the owner's round-6 approval (2026-11-01); the plan Spec line names round 6. |
| B-08 | B | SHOULD FIX | The backlog-burst measurement had three owners and no numeric run window | **Accepted.** Task 8 Step 3 is the single owner (GC, Task 1 Step 6 and Task 10 Step 4 point to it); full ~560k on the Mac (~2.2 GB fits); the window is 900 s (ingest-task budget, AGENTS Budgets) with a stop rule. |
| B-09 | B | SHOULD FIX | Stale rounds-1–3 semantics: Task 1 Step 3 stop rule (disk-only) and Task 4's test | **Accepted.** Task 1 Step 3 reads the scheduled table and stops only on the rewrite scheduling/deleting early (merge eager-scheduling expected); Task 4's test scoped to the rewrite (= T12-PLAN-08). |
| B-10 | B | CONSIDER | Count the idempotent missing-file case; fix the D3.1↔Crash contradiction | **Accepted.** `missing_treated_as_deleted` count in `CleanupReport` + test; spec D3.1 reconciled with Crash-safety. |
| B-11 | B | CONSIDER | The status/settings deferred count needs inline interpretation (UI rule) | **Accepted.** The display shows normal vs stuck (older than `snapshot_days + version_days`); the test asserts the label. |

### Plan round-4 review dispositions (A)

Plan round 4: A REQUEST CHANGES (1 SHOULD FIX, 1 CONSIDER); all round-3 findings confirmed resolved. B waived.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-PLAN-10 | A | SHOULD FIX | False provenance: the round-3 disposition claimed the step-6 idempotency "was in the owner-approved round-6 spec Crash-safety". It was not — round 6 (the reviewed version) had no idempotency and implied the opposite; the idempotency is a new post-round-6 change, so the record must present it as the change it is (it drops the T3 `IntegrityError` guard) for the owner to decide, not rubber-stamp | **Accepted; A is right, I over-claimed.** The T12-PLAN-07 disposition and the plan Status now present the idempotency honestly as a new post-round-6 change; the owner was corrected directly. The author decides it as the gold-standard design (the idempotency invariant forces it), and Reviewer A's round-5 re-review covers it — it is not an owner gate. Behaviour and the D3.1↔Crash gap-closure are unchanged (both correct). |
| T12-PLAN-11 | A | CONSIDER | The top Status still said "Pending the round-3 A/B plan re-review" although round 3 is done | **Accepted.** The Status now reads round 4, names the open owner decision, and pends Reviewer A's round-5 re-review. |

### Plan round-5 review dispositions (A)

Plan round 5: A REQUEST CHANGES (1 SHOULD FIX, 1 CONSIDER). Both round-4 findings (T12-PLAN-10, -11) confirmed
resolved. The idempotency confirmed correct for option (b), the provenance honest (verified against round-6 `9120e305`),
the cited AGENTS.md idempotency invariant real. B waived further plan rounds.

| ID | Reviewer | Severity | Finding | Disposition |
|---|---|---|---|---|
| T12-PLAN-12 | A | SHOULD FIX | `missing_treated_as_deleted` was only a `CleanupReport` field with a unit test — in no `cleanup` log line and no status/settings display. On a launchd/systemd background run a genuine storage fault increments it but is never shown, so the plan's own "keeps a true storage fault visible (no silent degradation)" and "the storage-fault signal" are not delivered | **Accepted; A is right.** The count is now surfaced in the `cleanup` log and the status/settings totals with inline interpretation (0 normal / nonzero investigate), B-11/B-22 style, with a test: Task 4b Step 1 (`test_cleanup_logs_deferred_count_and_oldest` broadened), Step 2, Interfaces, Task 5 Step 1, and the Global Constraints observability line. |
| T12-PLAN-13 | A | CONSIDER | The idempotency is a spec-level behaviour change (narrows the T3 `IntegrityError` fence) with A-only review coverage — B waived and never saw it. The two-reviewer discipline is the owner's to apply or waive, not the author's; present it to the owner for that decision | **Accepted; surfaced to the owner.** This is an owner-only governance call (the two-reviewer discipline, CLAUDE.md). Put to the owner with a recommendation: run a targeted B spec-review of the idempotency delta before the plan lands. Pending the owner's decision, which routes the final review round. |

### Plan round-6 review dispositions (A)

Plan round 6: A APPROVE. T12-PLAN-12 confirmed resolved (the `missing_treated_as_deleted` count surfaced in the
`cleanup` log and the status/settings display with inline interpretation, verified in all five regions); no new
findings. Report `...-plan-r6-A.md`.

### Round-8 dispositions (the `missing_deferred` addition, A and B)

The owner-directed `missing_deferred` addition (spec T12-SPEC-28) was reviewed across the spec and this plan as spec
round 8: A APPROVE (1 CONSIDER), B APPROVE WITH CHANGES (3 SHOULD FIX, 3 CONSIDER); reports `...-spec-r8-{A,B}.md`. The
full disposition table is in the spec ("Dispositions (round 8)", T12-SPEC-29 to -34). Plan effect: Interfaces, Step 1
(the `missing_deferred` test and the two-cause idempotency label), Step 2 (set membership, no per-file probe), and the
observability test label, plus Task 4c (the restore-time warning). The restore-time warning (T12-SPEC-33) was decided
warn-and-allow. Reviewer B's out-of-round listing-order catch is corrected: step 6 uses its own listing, not the
sweep's step-7 listing (no B-16 re-trigger).
