# v2 T3 — bounded catalog checkpoints and version retention at scale Implementation Plan

> **For agentic workers:** execute task by task with TDD (red, then green). **Owner rules override the generic
> execution skill:** no per-task commits, no micro-commits, no subagents, no worktrees. Work on the feature branch
> `feat/v2-ingestion`. T3 rides the single pre-commit-program commit (audit ledger, "Pre-commit program"); no commit
> happens in this cycle without the owner's explicit approval of that specific commit. Steps use checkbox (`- [ ]`)
> tracking. Keep the execution ledger in the session scratchpad, never in the repo (an untracked file in `v2/`
> would ride the commit).

**Round 3.** Round 2 applied spec round 8 (owner decisions 13–15) and every finding of the round-1 plan reviews
(`D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-plan-{A,B}.md`). Round 3 applies spec round 9 (the spike
outcomes, decisions 16–21): Task 9c (ops inserts and the native Parquet format) replaces Task 9's D6 step, Tasks 9b
and 10 are no longer conditional, G's default is 2 minutes, and Task 6 maps `FileNotFoundError` only. The disposition
tables are at the end.

**Goal:** Bound the catalog publishes of a run by time, pending work and barriers; prune superseded in-run versions
safely during the run and at its end; make crash orphans never trip the large-sweep guard; restore one file per table
per match on the native path; measure and, where it pays, reduce the catalog size.

**Architecture:** A new run-engine collaborator, `CheckpointPolicy` (`runs/checkpoint.py`), owns every in-run
publish: deferrable sites call `maybe`, barrier sites call `now`, and the run end publishes through it. Each publish
runs the integrity guard, commits, and prunes this run's versions past the grace G. Every lease acquisition stores a
run anchor and then a write-once acquisition marker; the sweep exempts doomed objects inside a surviving anchor's
interval and removes an anchor once a sweep has examined its interval. A writer confirms its lease at each write step
after a heartbeat gap. Private-to-public visibility moves batch; public-to-private moves stay per match.

**Tech Stack:** Python 3.12, DuckDB 2.0.0 + ducklake, pyarrow, huggingface_hub 2.3.0 / boto3 (HF gateway); uv; ruff
+ pyright strict + pytest + import-linter + pip-audit.

**Spec:** `docs/specs/2026-10-31-v2-t3-catalog-checkpoint-design.md`, round 8. Round 7 was approved by the owner;
round 8 applies owner decisions 13–15, and both reviewers approve it. The owner approved spec round 8 (including its
three "For the owner's confirmation" points) and this plan round 2 on 2026-10-31, for native execution. The spec's
decisions 1–15 are binding. Its section names (D1–D9, O-1, O-3, O-4, O-5) are used below.

## Global Constraints

- Both lake modes: the register path (unencrypted-local and HF, `RegisterWriter`) and the encrypted-local native path
  (`LocalNativeWriter`). Every change is tested on both unless a task says the path does not apply.
- Never run `CHECKPOINT` on the DuckLake attach (`catalog/working.py:8-10`).
- §10.3 step 7 stays the only data-file deletion rule (storage clock). Version-object deletes never delete data
  files.
- The privacy direction (public-to-private) is never batched, on either path (decision 10).
- A publish that can carry a bronze write runs `assert_bronze_integrity` first, including the run end and a stopped
  run's run end (D1).
- Every `Lease.acquire` stores the run anchor, then the acquisition marker, before it returns the lease; a failed
  store marks the generation released and raises `LeaseMarkerError` (D5).
- The S1 key rules apply to any new keyed connection: the key resolves only through `resolve_encryption_key`, is
  masked, never logged, never in an error message; no `duckdb_secrets(` or `duckdb_databases(` in `src` SQL (the
  blanket gate `tests/unit/test_no_key_introspection_on_keyed.py`); `temp_file_encryption` true; only the key
  rejection maps to `ConfigError`.
- Heavy runs (the spike, the scale test, the real-data run, the full gate) run on the Mac Mini, never the Windows
  AI-PC (owner rule, 2026-10-31). Push the tree with the tar method in `docs/ops/deploy.md`. Quick targeted test files
  may run on Windows.
- Fixtures stay synthetic; no real match data in the repo.
- Line length 120; ruff E,W,F,I,N,UP,B,S,BLE,RUF; pyright strict (whole project); import-linter green (6 contracts);
  pip-audit clean. No wheel bump and no `TODO.md` (v2 cycle-close is code + docs).
- The gate: `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, `uv run lint-imports`,
  `uv run pytest -m "not live and not hf and not bench"`, `uv run python scripts/pip_audit.py`.

## Review Focus

1. **A barrier site that defers, or a publish outside the policy.** A move or a skip fold whose publish waits for a
   checkpoint would delete objects a published version still names, or delay a privacy change; a direct
   `cat.commit(...)` would skip the guard and the pruning. Task 5 pins every site with a recording policy double, and
   Task 9 adds the `src`-wide AST gate (seen to fail first).
2. **A reader on a pruned version.** A burst of barrier publishes, the run-end prune, or a postponed Windows delete
   miscounting retained versions. Tasks 4, 5 and 6 test them, and Task 12 tests the HF mid-session case.
3. **An orphan outside every anchor interval.** A write before the anchor and the marker; a gap between intervals (a
   marker without an anchor, markers out of generation order); an anchor removed before a sweep examined its interval.
   Tasks 2, 7 and 8 test each one.
4. **An anchor that exempts too much.** The files of a run that a restore undid, a relocated lake, or an anchor whose
   marker is gone. Task 7 tests each one.
5. **A batched mixed pass.** A visibility pass with both directions must still publish each public-to-private move on
   its own. Task 9 tests a mixed pass on both paths, through a restore.

---

### Task 0: Baseline pin — the gate is green on the current tree

**Files:** none changed. The current tree holds the uncommitted T1 and T2 fixes and the gateway-path test fixes.

- [ ] **Step 1:** Push the tree to the Mac Mini and run the full gate there. Expected: green. Record the passed,
  skipped and deselected counts in the ledger as the T3 baseline.
- [ ] **Step 2:** On the Mac Mini, record the 2026-10-31 reference numbers from the spec's Evidence table in the
  ledger, for comparison in Task 14.

### Task 1: The spike (throwaway; Mac Mini, plus one HF job) — then stop for O-1, O-3, O-4, O-5 and G

**Done 2026-10-31.** The measurements and decisions 16–21 are in spec round 9 (D4, D6, D7, the gated decisions).
Step 6 ran as a local process against the HF test buckets; the Job-based HF test is Task 12.

**Files:** scratch only, under the session scratchpad and `/Volumes/Development/` throwaway folders. Nothing in the
repo.

**Interfaces:** Produces measured facts that Tasks 3, 6, 9b, 10 and 11 consume: G's default, the keyed plain open's
exact `ATTACH` form and wrong-key error class, the compaction size threshold, the native one-file mechanism, the HF
mid-session error, and the numbers for O-1, O-3, O-4 and O-5.

- [ ] **Step 1 (snapshot growth, O-1):** On the Mac Mini, ingest 20,000 synthetic matches into a throwaway encrypted
  lake and a throwaway unencrypted lake with per-match DuckLake transactions. Record the catalog bytes per match
  transaction, and the catalog size against the transaction count. Expected: a table of size versus transactions.
- [ ] **Step 2 (compaction, O-3, D7):** On a detached copy of each catalog, measure (a) `CHECKPOINT` through a plain
  `duckdb.connect` open and (b) a rewrite into a fresh file (encrypted with the same key for the encrypted catalog),
  verified by per-table row count, a content checksum, and schema equivalence through `duckdb_tables()`,
  `duckdb_columns()`, `duckdb_constraints()`, `duckdb_sequences()` and `duckdb_indexes()`. Record the size reduction
  and the time at several catalog sizes, and the smallest size at which compaction saves more publish time than it
  costs (the compaction threshold, consumed by Task 10). Establish the exact keyed plain open (`duckdb.connect` + key +
  `STORAGE_VERSION 'latest'`) and the exact exception class and message for a wrong key and for a corrupt file.
- [ ] **Step 3 (native one file, O-4, D6):** Write one 8,437,208-row tracking-shaped arrow table with many chunks
  through the native path with each candidate: `arrow.combine_chunks()` before the `INSERT`; DuckLake per-thread
  output and target-file settings; both. Record the file count per table, and confirm `ENCRYPTED` still covers every
  file (no cleartext marker). Also record whether native files honour the rest of §10.2: sort order, encodings,
  compression level.
- [ ] **Step 4 (G and the burst, D4):** Measure the longest reader session (a page render and a
  `CatalogReader.session()` on the Mac Mini lake, 28 GB) and the publish time at the Step 1 sizes. Propose
  `grace_minutes` ≥ 2 × the longest session, at least 10 minutes. For that G, compute the burst storage of a mass
  public-to-private change: (G ÷ publish time) × S, at the scale-test size and at 20,000 matches.
- [ ] **Step 5 (steady state, O-5):** On a throwaway encrypted lake, ingest 1,250 synthetic matches, then run again
  with every match unchanged. Record the `data/` files and the catalog bytes (snapshots) that the unchanged run added,
  per unchanged match, and the steady growth of daily runs inside the 8-day snapshot window.
- [ ] **Step 6 (HF mid-session, D4):** This needs the throwaway test bucket pair (`luxury-lakehouse/lake-test` and
  `/restricted-test`, Task 33 precedent). If the pair is absent, stop and ask the owner to create it. With the owner's
  approval to launch, run one HF job on the test buckets: attach a version, delete its object, query again. Record the
  exception class and message.
- [ ] **Step 7 (stop):** Write the numbers into the ledger and bring them to the owner for: O-1 (snapshot policy); O-3
  (compaction benefit; keyed open kept or dropped); O-4 (native one file achievable); O-5 (batch unchanged-match ops
  writes or not); any §10.2 gap from Step 3 (sort order, encodings, compression level); and G with its burst storage
  from Step 4 (the owner confirms the default, or decides when no G satisfies both the reader session and the burst
  storage). Record each decision in the spec's "Gated owner decisions" section. Tasks 2–8 do not depend on these
  decisions. Tasks 9b, 10 and 11 do not start until the owner has decided. Task 9's D6 part waits for O-4. Task 3's
  `grace_minutes` default waits for the G decision.

### Task 2: The run anchor, the acquisition marker and the lease confirmation (D5, decisions 12 and 13)

**Files:**
- Modify: `src/lakehouse/storage/lease.py` (`Lease.acquire`, `Lease._prune`, `Lease.assert_held`, new `Lease.confirm`,
  new helpers), `src/lakehouse/errors.py` (`LeaseMarkerError`)
- Test: `tests/unit/test_lease.py`, `tests/unit/test_lease_callers.py` (new), `tests/unit/test_cli.py`

**Interfaces:**
- Produces:
  - `anchor_key(generation: int) -> str` → `f"runs/lease/{generation:012d}.anchor"`;
    `marker_key(generation: int) -> str` → `f"runs/lease/{generation:012d}.acquired"`. Both lie under the gateway
    prefix `runs/lease/` (HF `put_if_absent` works only on gateway keys, `storage/hf.py:232-235`), and neither matches
    `_GENERATION_KEY`.
  - `RunAnchor(BaseModel)`: `generation: int`, `run_id: str` (frozen, `extra="forbid"`): the anchor's content.
  - `AnchorInterval` (frozen dataclass): `generation: int`, `run_id: str`, `start: datetime | None` (the own marker's
    storage `modified`, `None` when no marker exists), `end: datetime | None` (the earliest `modified` among the
    markers of higher generations, `None` when there is none); `covers(modified: datetime) -> bool` (`start` is not
    `None`, `start <= modified`, and `end` is `None` or `modified < end`); `examined_before(started: datetime) ->
    bool` (`start` is `None`, or `end` is not `None` and `end < started`).
  - `anchor_intervals(storage: Storage) -> list[AnchorInterval]` (one listing of `runs/lease/`, one `get` per anchor).
  - `remove_anchors(storage: Storage, generations: Iterable[int]) -> None`.
  - `Lease.confirm() -> None`: calls `assert_held()` only when the last passing `assert_held()` is older than
    `HEARTBEAT` on the lease's clock. `assert_held()` records the time of each pass; `acquire` counts as the first
    confirmation.
  - `LeaseMarkerError(LakehouseError)` in `errors.py`.

- [ ] **Step 1: Failing tests** in `tests/unit/test_lease.py`. First change the file's `_generations` helper to keep
  only keys that match `_GENERATION_KEY`, so the existing tests stay true with anchors and markers present.
  - `test_acquire_stores_anchor_then_marker_before_returning`: a recording storage double shows the generation put,
    then the anchor put, then the marker put, all before `acquire` returns; the anchor reads `RunAnchor(generation=g,
    run_id=...)`; the marker's `modified` is not before `lease.started_storage_time`.
  - `test_renew_and_release_do_not_touch_anchor_or_marker`.
  - `test_anchor_and_marker_keys_are_not_generations`: `_GENERATION_KEY.match` is `None` for both keys;
    `_generations()` lists only generation objects; `Lease.acquire` with anchors and markers present takes generation
    N+1 (B-04: `acquire` ignores them).
  - `test_failed_anchor_store_releases_and_raises` and `test_failed_marker_store_releases_and_raises`: with a storage
    double whose `put_if_absent` fails for that key, `acquire` raises `LeaseMarkerError` and the generation reads
    `released=True`; after a failed anchor store no marker exists; after a failed marker store the anchor exists; no
    other object was written.
  - `test_prune_deletes_marker_and_keeps_anchor`: an old generation outside `KEEP_NEWEST` and past `PRUNE_AFTER`
    loses its generation object and its marker; its anchor stays.
  - `test_interval_ends_at_earliest_later_marker`: generations 3 (anchor, marker at t1), 4 (anchor, no marker), 5
    (anchor, marker at t5): interval 3 is [t1, t5); interval 4 has `start=None` and covers nothing; interval 5 has
    `end=None`.
  - `test_out_of_order_markers_leave_no_gap`: generation 6's marker at t8, generation 7's marker at t7 < t8. For each
    of t7, t8 and t9 > t8, some interval covers it.
  - `test_interval_ignores_a_later_heartbeat`: the later generation's object is renewed past the interval; the end
    stays at its marker.
  - `test_confirm_calls_assert_held_only_after_a_heartbeat_gap`: injected clock; no call at +30 s after acquire; one
    call at +61 s; a passing `assert_held()` from a commit at +90 s means no call at +120 s.
  - `test_confirm_ignores_a_renew_after_takeover`: generation g+1 exists; g's `renew()` succeeds (a plain `put`);
    `confirm()` after the gap raises `LeaseLostError`.
- [ ] **Step 2: Failing tests** in `tests/unit/test_lease_callers.py`, parametrized over the five callers
  `run_ingestion` and `reparse_matches` (`runs/run.py:868`, `:954`), `sweep_now` (`catalog/cleanup.py:372`),
  `restore` (`catalog/restore.py:218`) and `relocate` (`catalog/relocate.py:121`):
  `test_failed_marker_store_stops_the_caller_before_any_write` — with the marker store failing, each caller raises
  `LeaseMarkerError` and writes no `catalog/`, `data/`, `raw/` or `runs/` object other than the generation object and
  the anchor. In `tests/unit/test_cli.py`: `test_cli_run_exits_1_on_failed_marker_store` — `lakehouse run` exits 1
  with status `failed` and the `LeaseMarkerError` message (the generic `LakehouseError` branch, `cli.py:580`).
- [ ] **Step 3:** Run `uv run pytest tests/unit/test_lease.py tests/unit/test_lease_callers.py tests/unit/test_cli.py
  -q`. Expected: the new tests FAIL (no `anchor_key`).
- [ ] **Step 4:** Implement. In `Lease.acquire`, after the stale-listing check (`lease.py:212-218`) and before
  constructing the `Lease`, `put_if_absent` the anchor, then the marker. On either failure, write the generation
  `released=True`, then raise `LeaseMarkerError`. Extend `_prune` to delete `marker_key(g)` with `lease_key(g)`, and
  never an anchor. Add the helpers and `confirm`.
- [ ] **Step 5:** Run the same command, plus `tests/e2e/test_crash_recovery.py`. Expected: PASS.

### Task 3: The `checkpoint` settings block (D1, D8)

**Files:**
- Modify: `src/lakehouse/config/model.py` (new `CheckpointConfig`; `Settings.checkpoint`),
  `src/lakehouse/config/defaults.yaml`, `src/lakehouse/ui/pages/settings.py`
- Test: `tests/unit/test_config_model.py`, `tests/unit/test_config_store.py`, the settings page test that covers
  `retention`

**Interfaces:**
- Produces: `CheckpointConfig(minutes: int = 5, matches: int = 50, grace_minutes: int = 2 (decision 21),
  move_batch: int = 50, warn_versions: int = 200, warn_catalog_mb: int = 2048)`, every field `Field(ge=1)`;
  `Settings.checkpoint: CheckpointConfig = CheckpointConfig()`.

- [ ] **Step 1: Failing tests:** `test_checkpoint_defaults` (the values above); `test_checkpoint_rejects_zero` (each
  field at 0 raises `ValidationError`); `test_saved_config_without_checkpoint_loads_defaults` (a stored settings JSON
  without the block loads); `test_config_set_checkpoint_minutes` (`lakehouse config set checkpoint.minutes 10` saves
  and reloads 10); `test_settings_page_shows_checkpoint` (the page prose names the four D1 values, next to retention
  and caps).
- [ ] **Step 2:** Run the four test files. Expected: FAIL.
- [ ] **Step 3:** Implement the model, the `defaults.yaml` block, and one prose line in `settings.py`, after the
  retention and caps lines: "Publish the catalog every {minutes} minutes or {matches} units; keep a superseded run
  version {grace_minutes} minutes; batch up to {move_batch} private-to-public moves."
- [ ] **Step 4:** Run the four test files. Expected: PASS.

### Task 4: The checkpoint policy and grace-aware pruning (D1, D4, D8)

**Files:**
- Create: `src/lakehouse/runs/checkpoint.py`
- Modify: `src/lakehouse/catalog/cleanup.py` (`plan_version_pruning` gains `grace`; `_delete_versions` becomes the
  public `delete_versions` and returns the confirmed deletes; `run_cleanup` passes the grace)
- Test: `tests/unit/test_checkpoint_policy.py`, `tests/unit/test_cleanup.py`

**Interfaces:**
- Consumes: `CheckpointConfig` (Task 3); `WorkingCatalog.commit(kind) -> VersionInfo`; `assert_bronze_integrity(cat)`;
  `ErrorSink` (`runs/_common.py`).
- Produces:
  - `PublishRecord(seq: int, kind: VersionKind, reason: str, size: int, at: datetime)` (frozen dataclass).
  - `CheckpointPolicy(cat: WorkingCatalog, config: CheckpointConfig, *, run_id: str, now: Callable[[], datetime],
    errors: ErrorSink, storage: Storage)` with `maybe(reason: str) -> PublishRecord | None`,
    `now(reason: str) -> PublishRecord`, `run_end() -> PublishRecord`, the property
    `published -> Sequence[PublishRecord]`, and `totals() -> PublishTotals` (frozen dataclass: `publishes: int`,
    `bytes_published: int`, `max_retained: int`, `guard_ms: int`, `compaction_ms: int`).
  - `plan_version_pruning(storage, retention, *, dropped, now, current_seq, grace: timedelta) -> list[int]`: a version
    with a manifest is prunable only when the next version's manifest `published_at` is at least `grace` before
    `now`. A version without a manifest (a crashed commit, never `CURRENT`) is pruned without the grace, as today.
  - `delete_versions(storage: Storage, seqs: Sequence[int]) -> list[int]`: returns the seqs whose objects are gone; a
    delete postponed on Windows (`cleanup.py:127-131`) is not in the result.
- Ruling to ledger: the grace compares the writer's clock with manifest `published_at` (a reader-protection delay,
  not a data-file deletion rule; §10.3 step 7 stays on the storage clock).

- [ ] **Step 1: Failing tests** in `tests/unit/test_checkpoint_policy.py` (a `kit` lake, an injected clock):
  - `test_maybe_publishes_after_minutes` (T = 5: no publish at 4 min 59 s; a publish at 5 min).
  - `test_maybe_publishes_after_matches` (M = 3: the third `maybe` publishes).
  - `test_now_always_publishes_and_resets_counters`.
  - `test_guard_runs_before_every_publish`: with `assert_bronze_integrity` patched to record calls, every
    `maybe`-publish, every `now` and `run_end` call it first; a guard failure raises `GuardStopError` and publishes
    nothing.
  - `test_lost_lease_publishes_nothing`: with the lease's `assert_held` raising `LeaseLostError`, `now` raises it, and
    `CURRENT` is unchanged.
  - `test_in_run_pruning_respects_grace`: G = 10 min; in-run versions v1, v2, v3 published at t = 0, 1 and 2 min, then
    v4 at t = 13 min. After the v4 publish the deleted set is exactly {v1, v2} (successors published 12 and 11 minutes
    earlier) and the kept set is exactly {v3, v4} (v3's successor is 0 minutes old; v4 is `CURRENT`). Versions from
    before the run are untouched.
  - `test_barrier_burst_keeps_versions_inside_grace`: 20 `now` calls within 1 minute delete nothing.
  - `test_postponed_delete_still_counts_as_retained`: with `sys.platform` patched to `"win32"` and the delete of one
    version raising `PermissionError`, the policy's retained count still includes that version, and the next publish
    retries it.
  - `test_publish_logs_d8_fields`: the `catalog commit` log carries `seq`, `kind`, `reason`, `bytes`,
    `bytes_published_run`, `retained_versions`, `guard_ms`.
  - `test_catalog_growth_warning_on_versions`: with `warn_versions = 2`, the third retained version records an
    error-sink call of kind `catalog_growth`, severity `warning`.
  - `test_catalog_growth_warning_on_size`: with `warn_catalog_mb = 1` and a published copy above 1 MB, the publish
    records the same warning, naming the size.
- [ ] **Step 2: Failing tests** in `tests/unit/test_cleanup.py`: `test_plan_version_pruning_grace_holds_recent`
  (successor 5 min old, G 10 min → not pruned); `test_plan_version_pruning_grace_releases_old` (successor 11 min old
  → pruned); `test_plan_version_pruning_prunes_manifestless_without_grace`; `test_delete_versions_returns_confirmed`;
  the existing pruning tests pass `grace=timedelta(0)` and stay green.
- [ ] **Step 3:** Run both files. Expected: the new tests FAIL.
- [ ] **Step 4:** Implement `checkpoint.py` and the cleanup changes. The policy counts retained versions without
  listing per publish: the count listed once at construction, minus this run's confirmed deletes, plus its publishes.
  A postponed version stays in the run's prune list for the next publish.
- [ ] **Step 5:** Run both files, plus `tests/e2e/test_cleanup_multirun.py`. Expected: PASS.

### Task 5: Wire the policy into the run engine (D1, D2, D4 at the run end, D8, D9)

**Files:**
- Modify: `src/lakehouse/runs/run.py` (`_Run.__init__`, `fold_skips`, `_listings`, `finish`),
  `src/lakehouse/runs/match_writer.py` (`write`, `record_not_kept`), `src/lakehouse/runs/match_files.py`
  (`_move_native` only; the register move is Task 9), `src/lakehouse/bronze/writers.py` and
  `src/lakehouse/bronze/writer.py` (guard messages)
- Test: `tests/unit/test_run_checkpoints.py`, existing run tests

**Interfaces:**
- Consumes: `CheckpointPolicy` (Task 4).
- Produces: `_Run.checkpoints: CheckpointPolicy`; `MatchWriter`, `MatchFiles` and `VisibilityMover` take a
  `checkpoints: CheckpointPolicy` constructor argument.
- **The `finish()` split (A T3-PLAN-01):** `finish()` keeps its order: resolution, dbt, then `run_cleanup(...)`
  before the run-end commit. `run_cleanup` still expires snapshots, prunes the versions that retention already
  releases (now with the grace), and sweeps, so the sweep's kept set excludes those versions as today. Then
  `write_run`, then `checkpoints.run_end()`: the guard, `cat.commit("run_end")`, and then, still under the lease,
  `plan_version_pruning` with the grace and `delete_versions`. The post-commit prune removes this run's in-run
  versions and a crashed predecessor's, which the new run-end version now supersedes. A stopped run skips
  `run_cleanup` as today, and its `run_end()` still prunes after the commit.

- [ ] **Step 1: Failing tests** in `tests/unit/test_run_checkpoints.py` (`runkit` adapters and a recording policy
  double):
  - `test_match_write_calls_maybe` / `test_record_not_kept_calls_maybe` / `test_listing_calls_maybe`.
  - `test_move_native_calls_now` / `test_fold_skips_calls_now`.
  - `test_run_end_is_guarded_and_publishes_pending`: a run with M = 1000 and 3 matches makes exactly one publish (the
    run end), after the guard.
  - `test_stopped_run_end_is_guarded`: SIGTERM after two matches; the run-end publish carries both matches and runs
    the guard.
  - `test_finish_sweeps_before_and_prunes_after_the_run_end_commit`: a call-order recorder shows `run_cleanup` (with
    its sweep) before `commit("run_end")`, and the post-commit prune after it.
  - `test_run_end_prunes_own_and_crashed_versions`: a crashed predecessor left in-run versions; after this run's
    run-end commit, the prune (grace 0 in the test) deletes this run's in-run versions and the predecessor's.
  - `test_run_end_keeps_versions_inside_grace` (B-04): G = 10 min; a version superseded 1 minute before the run end
    survives the post-commit prune; the next run's cleanup deletes it once it is past G.
  - `test_run_end_log_carries_publish_totals` (B-04, D8): the run's final record carries the `PublishTotals` fields.
  - `test_reader_sees_matches_per_publish` (decision 15): with M = 2, a reader of `CURRENT` sees 0 matches after the
    first match write and 2 after the second.
  - `test_reparse_all_publishes_per_checkpoint`: `reparse_matches(target="all")` on 10 matches with M = 4 publishes 3
    in-run versions plus the run end.
  - `test_guard_error_names_table_and_match`: a `_visibility` mismatch raises `GuardStopError` whose message names the
    table and at least one `match_key`; the register-only guard names the table and the file.
- [ ] **Step 2:** Run `uv run pytest tests/unit/test_run_checkpoints.py -q`. Expected: FAIL.
- [ ] **Step 3:** Implement. `_Run.__init__` builds the policy before the collaborators and passes it to them.
  Replace each run-engine `cat.commit("in_run")` except the register move's (`match_files.py:86`, `moves.py:310`,
  Task 9) with `checkpoints.maybe(<site>)` or `checkpoints.now(<site>)` per D2, and remove the per-write
  `assert_bronze_integrity` calls (`match_writer.py` and `_listings`). Make the `finish()` split above. Extend the
  guard messages with up to five offending match keys.
- [ ] **Step 4:** Run the new file and `tests/unit/test_run_visibility.py tests/unit/test_run_listings.py
  tests/unit/test_run_history.py tests/unit/test_reparse.py tests/unit/test_run_quarantine.py tests/e2e/test_run_local.py
  tests/e2e/test_crash_recovery.py tests/e2e/test_read_during_run.py`. Expected: PASS.
  - `tests/e2e/test_crash_recovery.py::test_crash_loses_at_most_one_match` asserts that matches 1 and 2 are published
    before the kill. Its saved settings set `checkpoint.matches = 1`, so it keeps testing the per-match bound that
    M = 1 gives, and its docstring names the setting. The interval-loss cases are new tests in Task 8 (ruling to
    ledger).
  - Another existing test that fails only because it assumes a publish per match gets `checkpoint.matches = 1` when
    its subject is not the cadence; otherwise its assertions change to per-publish. Each case is a ledger ruling.
  - `tests/e2e/test_read_during_run.py` stays unchanged (decision 15).

### Task 6: The reader retry for a missing version (D4)

**Files:**
- Modify: `src/lakehouse/services/catalog.py` (`read_catalog`)
- Test: `tests/unit/test_services_catalog.py` (create if absent)

**Interfaces:**
- Consumes: the Task 1 Step 6 result: on HF a session whose version object is deleted completes, and a fresh attach
  of a deleted version raises `FileNotFoundError`, as locally. No HF-only class needs mapping.

- [ ] **Step 1: Failing tests:**
  - `test_missing_version_retries_on_new_current`: `CURRENT` names version 5; delete version 5's object after
    `read_current` and before the attach (a storage double that deletes on the first `stat`), with version 6
    published; `read_catalog` returns version 6's rows.
  - `test_missing_version_twice_raises_stale`: both attempts miss; `read_catalog` raises `StaleCatalogError`, never
    `FileNotFoundError`.
  - `test_cannot_open_file_still_maps_to_stale` (today's mapping is unchanged).
- [ ] **Step 2:** Run the file. Expected: FAIL.
- [ ] **Step 3:** Implement: on `FileNotFoundError` from the session, close the reader, re-read `CURRENT`, and retry
  once; a second failure raises `StaleCatalogError`. No other exception changes.
- [ ] **Step 4:** Run the file and the page tests that call `read_catalog`. Expected: PASS.

### Task 7: The sweep exemption and anchor removal (D5)

**Files:**
- Modify: `src/lakehouse/catalog/cleanup.py` (`_sweep` exempts doomed objects inside a surviving anchor interval and
  removes examined anchors; `CleanupReport` and the `cleanup` log gain the counts), `src/lakehouse/catalog/restore.py`
  (`_restore_held` removes the anchors of dropped runs before its publish), `src/lakehouse/catalog/relocate.py`
  (`_relocate_held` removes earlier anchors before its publish)
- Test: `tests/unit/test_cleanup.py`, `tests/unit/test_restore.py`, `tests/unit/test_relocate.py`

**Interfaces:**
- Consumes: `anchor_intervals`, `AnchorInterval.covers/examined_before`, `remove_anchors` (Task 2).
- Produces: `CleanupReport` gains `exempted: int` and `anchors_removed: list[int]`; the `cleanup` log gains
  `exempted` and `anchors_removed` (D8).

- [ ] **Step 1: Failing tests** in `tests/unit/test_cleanup.py` (objects stored at chosen storage times):
  - `test_sweep_guard_ignores_orphans_inside_an_anchor_interval`: 9 kept objects and 3 orphans inside a surviving
    interval, max fraction 0.10 → the sweep deletes the 3, does not stop, and reports `exempted == 3`.
  - `test_sweep_guard_still_trips_outside_every_interval`: the same 3 orphans stored before the oldest anchor's start
    → `GuardStopError`, nothing deleted.
  - `test_lost_lease_in_flight_files_fall_in_the_next_interval`: generation g (anchor, marker t1), g+1 (anchor, marker
    t3), old-writer files at t4; the sweep of g+2 deletes them without stopping, through g+1's anchor.
  - `test_anchor_without_marker_exempts_nothing_and_is_removed`: also when the generation and its marker were pruned
    (B T3-PLAN-03).
  - `test_sweep_removes_only_examined_anchors`: after the sweep of generation s, the anchors of s−1 and s survive and
    the anchor of s−2 is gone.
  - `test_failed_sweep_removes_no_anchor`: a delete that raises mid-sweep leaves every anchor in place.
  - `test_sweep_stops_when_a_doomed_key_is_in_current` (spec T3-SPEC-21): with the kept set patched to omit one key of
    the `CURRENT` manifest, and that object inside a surviving anchor's interval, the sweep raises `GuardStopError`
    and deletes nothing.
- [ ] **Step 2: Failing tests:** in `tests/unit/test_restore.py`,
  `test_restore_removes_anchors_of_dropped_runs_before_publish` (after the restore, the dropped run's anchor is gone;
  the next sweep on a small lake counts that run's files and stops with `GuardStopError`; with the restore's publish
  forced to fail, the anchor is gone too); in `tests/unit/test_relocate.py`, `test_relocate_removes_earlier_anchors`
  (only the relocate's own anchor survives).
- [ ] **Step 3:** Run the three files. Expected: FAIL.
- [ ] **Step 4:** Implement. In `_sweep`, after `doomed` is computed: stop with `GuardStopError` if any doomed key is
  in `current_keys`; load the intervals once; count only the doomed objects no interval covers; run the guard on that count; delete every doomed object as today; then call
  `remove_anchors` for the intervals that are `examined_before(lease.started_storage_time)`. A raise skips the
  removal. Restore: the run ids in the manifests of `RestoreRecord.dropped_seqs`. Relocate: every anchor whose
  generation is below its own.
- [ ] **Step 5:** Run the three files plus `tests/e2e/test_cleanup_multirun.py`. Expected: PASS.

### Task 8: The write-time lease check and crash coverage (D5)

**Files:**
- Modify: `src/lakehouse/runs/run.py` (`_check_stop` confirms the lease; `finish()` confirms before resolution and
  before dbt), `src/lakehouse/runs/visibility.py` (`VisibilityMover` takes `confirm_lease: Callable[[], None]` and
  calls it before each move)
- Test: `tests/unit/test_run_lease_check.py` (new), `tests/e2e/test_crash_recovery.py`

**Interfaces:**
- Consumes: `Lease.confirm` (Task 2); the sweep exemption (Task 7).

- [ ] **Step 1: Failing tests** in `tests/unit/test_run_lease_check.py`:
  - `test_confirm_runs_at_each_write_step`: a lease double counts `confirm()` at each module, match and listing entry,
    each visibility move, and before resolution and dbt.
  - `test_stalled_writer_stops_at_its_next_match`: injected clock; after two matches the clock jumps 15 minutes and
    generation g+1 appears; the next match raises `LeaseLostError` before it writes any `data/` object, and nothing is
    published.
  - `test_recent_publishes_need_no_extra_assert_held`: with M = 1, `assert_held()` runs only inside the commits.
  - `test_failed_marker_store_leaves_no_unit_write`: with the marker store failing, a run starts no unit write and no
    `data/` object exists (the assertion is "no unit write after the marker store fails", Reviewer A round 7).
- [ ] **Step 2: Failing tests** in `tests/e2e/test_crash_recovery.py`. A crash is a `_Crash(BaseException)` raised from
  an injected hook at the named step, with `Lease.release` patched to do nothing, so neither the run end nor the
  release runs; the next run takes over after expiry plus grace on an injected clock. The existing child-process kill
  test stays as the real-kill check. For each case, on a small lake (10 objects) and a large one: the next run redoes
  exactly the lost work, sweeps the orphans and does not stop; the run after next does not stop either; one variant
  has the next run publish checkpoints before its sweep. The cases:
  - both paths: a crash after at most M unpublished units; a lost lease (the old writer writes one more unit after the
    takeover); a lost-lease writer whose in-flight write lands after the new holder's marker, then crashes (the
    round-7 residual); a crash inside a private-to-public batch;
  - encrypted-local (B T3-PLAN-01): a crash after 200 unchanged matches (`refresh_signals`, `listed_again`); after
    error-sink writes; inside a native flip batch; during resolution; during dbt; after a rolled-back unit
    transaction.
- [ ] **Step 3:** Run both files. Expected: FAIL.
- [ ] **Step 4:** Implement the confirmation points.
- [ ] **Step 5:** Run both files and `tests/unit/test_run_visibility.py tests/unit/test_run_listings.py
  tests/unit/test_encrypted_attach_sites.py`. Expected: PASS.

### Task 9: Batched private-to-public moves and the register move publish (D2, D3, decisions 8 and 10)

**Files:**
- Modify: `src/lakehouse/catalog/moves.py` (`move_match_files` split into `stage_match_move` and
  `finish_match_move`; the `moves.py:310` commit goes), `src/lakehouse/runs/match_files.py` (`move`,
  `move_public_batch`), `src/lakehouse/runs/visibility.py` (`_apply_flags`)
- Test: `tests/unit/test_visibility_batch.py`, `tests/unit/test_no_direct_publish.py` (new),
  `tests/unit/test_restore.py`

**Interfaces:**
- Consumes: `CheckpointPolicy.now` (Task 4).
- Produces: `stage_match_move(cat, storage, match_key, to_root, *, archive) -> StagedMove` (copies, swap and move
  records, no publish); `finish_match_move(storage, staged: StagedMove) -> None` (the post-publish deletes of a
  public-to-private move); `MatchFiles.move_public_batch(rows: Sequence[tuple[int, Mapping[str, Any]]]) -> None`
  (stages up to B private-to-public moves, one `checkpoints.now("move_batch")`, then finishes them). A register
  public-to-private move is stage, `checkpoints.now("move")`, finish: one publish per match where today makes two.

- [ ] **Step 1: Failing test** `tests/unit/test_no_direct_publish.py::test_no_catalog_publish_outside_the_allowlist`
  (A T3-PLAN-02, B T3-PLAN-02): an AST scan of every module under `src/lakehouse` finds no catalog-publish call,
  meaning an attribute call `.commit(...)` with at least one argument (a DuckDB `conn.commit()` has none), outside the
  allowlist. The allowlist names functions, not files (B T3-PLAN-12), so `run_cleanup` in the same module stays
  covered: `catalog/working.py` (the definition), the methods of `CheckpointPolicy` in `runs/checkpoint.py`, and the
  three maintenance publishes that are not ingestion runs (A T3-PLAN-04): `sweep_now` in `catalog/cleanup.py`,
  `_restore_held` in `catalog/restore.py`, and `_relocate_held` in `catalog/relocate.py`. Run it: FAIL on
  `catalog/moves.py:310` and `runs/match_files.py:86` (the register move, B T3-PLAN-11), the gate seen to fail first.
- [ ] **Step 2: Failing tests** in `tests/unit/test_visibility_batch.py`, on both paths:
  - `test_private_to_public_batch_publishes_once` (5 changes, B = 50 → one publish; B = 2 → three publishes).
  - `test_batch_closes_at_end_of_pass`.
  - `test_public_to_private_publishes_per_match_in_a_mixed_pass` (3 private, 4 public changes → three per-match
    publishes, then one batch publish).
  - `test_native_batch_keeps_visibility_invariant` (`_visibility` equals `ops_matches.visibility` in the batch's
    version).
  - `test_register_batch_is_one_publish_not_two` (the batch's move records, `ops_matches` and lineage ride one
    publish).
  - `test_register_private_move_is_one_publish_then_deletes` (stage, one `now`, then the old public copies are
    deleted after the publish, §29.7).
  - `test_crash_inside_native_batch_leaves_matches_private`.
- [ ] **Step 3: Failing test** in `tests/unit/test_restore.py`, `test_batched_moves_then_restore` on each path: a
  public-to-private move, then a batched private-to-public move, then a restore to a version from before the batch.
  Every match ends with its latest visibility, every named file exists, and no now-private match is published as
  public (§29.11, §29.12).
- [ ] **Step 4:** Run the three files. Expected: FAIL.
- [ ] **Step 5:** Implement the split, the register move wiring and the batch path. The visibility pass collects
  private-to-public moves and flushes them at B or at the end of the pass. Public-to-private moves call
  `MatchFiles.move` (stage, `now`, finish) at once.
- [ ] **Step 6:** Run the three files and `tests/unit/test_run_visibility.py tests/unit/test_moves.py
  tests/unit/test_moves_raw.py`. Expected: PASS.

### Task 9c: One insert per ops table per write, and the §10.2 file format on native writes (D6, decisions 18 and 19)

**Files:**
- Modify: `src/lakehouse/runs/match_writer.py` (`ops_copy_fields`, `:143`), `src/lakehouse/runs/history.py` (the
  generic row writer, `:280`), `src/lakehouse/resolve/tables.py` (`:160`), `src/lakehouse/catalog/working.py`
  (`attach` makes sure of the Parquet options)
- Test: `tests/unit/test_ops_bulk_writes.py` (new), `tests/unit/test_native_one_file.py` (new)

**Interfaces:**
- Produces: `insert_rows(conn: DuckDBPyConnection, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]])
  -> None` in `catalog/duck.py` (one statement per call; the three sites use it; it sits in `catalog` so `resolve`
  does not import `runs`, B T3-PLAN-14); `PARQUET_OPTIONS: Final =
  {"parquet_compression": "zstd", "parquet_compression_level": "3", "parquet_version": "2"}` in `catalog/duck.py`, and
  `ensure_parquet_options(conn: DuckDBPyConnection) -> None` there (reads `ducklake_options('lake')`, calls
  `lake.set_option` only for an option that is absent or differs).

- [ ] **Step 1: Failing tests** in `tests/unit/test_ops_bulk_writes.py`, on the encrypted-local path:
  - `test_match_write_stores_one_file_per_ops_table`: a match write with 300 copy fields adds at most one data file to
    `ops_copy_fields`, and at most one to each other ops table it touches (counted from `ducklake_data_file` by the
    write's snapshot).
  - `test_resolution_stores_one_file_per_table`: a resolution that writes 50 canonical pointers and entity records adds
    at most one file per table.
  - `test_insert_rows_keeps_content_and_order`: the rows read back equal the rows written, in order, for an empty, a
    one-row and a 1,000-row call.
  - `test_register_ops_write_over_the_inline_limit_is_one_file`: on the register path a 50-row ops write adds one file.
- [ ] **Step 2: Failing tests** in `tests/unit/test_native_one_file.py`:
  - `test_writing_attach_sets_the_parquet_options`: after a writing attach, `ducklake_options('lake')` shows zstd, level
    3 and version 2, on a new and on an existing catalog.
  - `test_reattach_with_options_set_changes_nothing`: a second attach adds no snapshot.
  - `test_native_writes_one_file_per_table` (a regression pin; the spike showed it already holds): an arrow table of
    8,437,208 rows in 421 chunks yields exactly one data file per table per match, with no cleartext marker.
- [ ] **Step 3:** Run both files. Expected: FAIL (the pin test passes; ruling: it is a pin, not a red step).
- [ ] **Step 4:** Implement `insert_rows`, the three call sites and `ensure_parquet_options` (called from
  `WorkingCatalog.attach` on both paths).
- [ ] **Step 5:** Run both files and `tests/unit/test_run_history.py tests/unit/test_resolve_links.py
  tests/unit/test_resolve_matches.py tests/unit/test_resolve_players.py tests/unit/test_resolve_teams.py
  tests/unit/test_local_native_writer.py tests/unit/test_encrypted_attach_sites.py`. Expected: PASS.

### Task 9b: Batch the unchanged-match ops writes (O-5, decision 20)

**Files:**
- Modify: `src/lakehouse/runs/match_writer.py` (`listed_again`, `refresh_signals` buffer their rows),
  `src/lakehouse/runs/checkpoint.py` (pre-publish flush hooks)
- Test: `tests/unit/test_unchanged_batch.py` (new)

**Interfaces:**
- Produces: `CheckpointPolicy.on_publish(flush: Callable[[], None]) -> None` (a hook that every publish calls before
  the guard); `MatchWriter.flush_unchanged() -> None` (writes the buffered rows in one DuckLake transaction).

- [ ] **Step 1: Failing tests:** `test_unchanged_writes_ride_one_transaction_per_publish` (50 unchanged matches add
  one DuckLake snapshot for their ops writes, and at most one data file per touched table, on the encrypted path);
  `test_flush_runs_before_the_guard_and_the_commit`; `test_unchanged_run_flushes_at_run_end` (an all-unchanged run
  writes its rows in the run-end publish); `test_crash_loses_the_buffer_and_the_next_run_redoes_it`.
- [ ] **Step 2:** Run. Expected: FAIL. Implement. Run the file and `tests/unit/test_run_listings.py
  tests/unit/test_run_steps.py`. Expected: PASS.

### Task 10: Catalog compaction and the keyed plain open (D7, decisions 9 and 17)

**Files:**
- Create: `src/lakehouse/catalog/compact.py`
- Modify: `src/lakehouse/catalog/duck.py` (`keyed_plain_attach`), `src/lakehouse/catalog/working.py` (`commit` calls
  the compaction between the detach and the version put)
- Test: `tests/unit/test_compact.py`, `tests/unit/test_encrypted_attach_sites.py`

**Interfaces:**
- Produces: `keyed_plain_attach(conn: DuckDBPyConnection, path: Path, *, alias: str, key: str) -> None`, the spike's
  form `ATTACH '<path>' AS <alias> (ENCRYPTION_KEY '<key>', STORAGE_VERSION 'latest')`; it maps to `ConfigError`
  only a `duckdb.InvalidInputException` whose message holds "Wrong encryption key" (a corrupt file raises the same class
  with "Computed AES tag differs", which propagates). The mapping is one helper, `is_wrong_key(exc: duckdb.Error) ->
  bool` in `catalog/duck.py`, that `attach_or_config_error` uses too, so the DuckLake attach stops labelling a corrupt
  catalog as a wrong key; its docstring's `IOException` claim is corrected (B T3-SPEC-22/T3-PLAN-13).
  `COMPACT_MIN_BYTES: Final = 8 * 1024 * 1024` in `compact.py`,
  with a comment citing the spike (31–40 % smaller, at most 0.17 s, at 9–22 MB). `compact_catalog_file(path: Path, *,
  key: str | None) -> CompactionResult` (frozen dataclass: `compacted: bool`, `failed_check: str | None`), a
  `COPY FROM DATABASE` rewrite into a fresh file.
- **The verification-failure outcome (B T3-PLAN-05):** when the rewrite fails a check (row count, checksum or schema),
  the fresh file is discarded and the publish goes ahead with the original file, which is the correct catalog (D7:
  the rewrite replaces the old file only after it verifies). The publish records an error-sink warning of kind
  `catalog_compaction` that names the failing check, so the failure is never silent (ruling to ledger).

- [ ] **Step 1:** Failing tests in `tests/unit/test_compact.py`:
  - `test_compaction_preserves_tables_rows_checksums_and_schema` (unencrypted and keyed).
  - `test_compacted_catalog_reopens_with_the_ducklake_attach`.
  - `test_compaction_touches_no_data_file` (the `data/` listing is byte-identical before and after).
  - `test_wrong_key_raises_config_error_without_the_key`.
  - `test_corrupt_file_keeps_its_own_error`.
  - `test_corrupt_encrypted_catalog_attach_keeps_its_own_error` (in `tests/unit/test_encrypted_attach_sites.py`): one
    flipped byte in an encrypted catalog, opened through the DuckLake attach with the right key, raises the DuckDB
    error, not `ConfigError`; a wrong key still raises `ConfigError` with no key in the message.
  - `test_key_never_logged` (a captured log at DEBUG holds no key substring).
  - `test_keyed_plain_open_sets_temp_file_encryption` (B T3-PLAN-04).
  - `test_commit_compacts_at_or_above_threshold_only` (below `COMPACT_MIN_BYTES` no compaction).
  - `test_verification_failure_publishes_original_with_warning`: a forced checksum mismatch publishes the original
    bytes and records the `catalog_compaction` warning.
- [ ] **Step 2:** Run. Expected: FAIL. Implement, and add `compaction_ms` to the publish log record (D8). Run
  `uv run pytest tests/unit/test_compact.py tests/unit/test_encrypted_attach_sites.py
  tests/unit/test_no_key_introspection_on_keyed.py -q`. Expected: PASS.
- O-1 kept §10.7 (decision 16), so no snapshot-policy step exists.

### Task 11: The scale test (closes §26 item 4) — Mac Mini, `bench`

**Files:**
- Create: `tests/bench/test_catalog_scale.py`

**Interfaces:**
- Consumes: the policy, pruning and anchors (Tasks 2–9).

- [ ] **Step 1:** Write `test_catalog_scale` (`@pytest.mark.bench`), which ingests at least 1,000 synthetic matches
  across all bronze tables. Re-key the repo's synthetic per-provider fixtures with distinct native ids; no real data.
  Assert:
  - publishes ≤ ⌈run time / T⌉ + ⌈U / M⌉ + barriers + 1 (U = units);
  - right after each publish and its pruning, the retained in-run versions ≤ 1 + the versions superseded within the
    last G;
  - after the run-end pruning, none superseded more than G ago;
  - during a burst of barrier publishes, retained versions stay within G ÷ (publish time) + 1;
  - bytes published per run ≤ the publish bound × the largest published copy.
- [ ] **Step 2:** The test also reports, part by part for §26 item 4: catalog size and attach time at 1,000 matches;
  the catalog growth after replaces (re-ingest 100 matches), with and without compaction if Task 10 adopted it; the
  version-retention storage; the total size of native files and of inlined data. It also reports the guard time per
  publish, the exempted count of each sweep, and the run-end publish time against the kill grace of launchd and of HF
  Jobs.
- [ ] **Step 3:** Run on the Mac Mini: `uv run pytest tests/bench/test_catalog_scale.py -m bench -q`. Expected: PASS,
  with the report in the ledger. If the run-end publish can exceed a kill grace, stop and bring it to the owner (D1).

### Task 12: The `hf_job` test over the S3 gateway

**Files:**
- Create: `tests/hf/test_checkpoint_hf_live.py`

- [ ] **Step 1:** Write `test_checkpoint_publish_and_prune_over_gateway` (`@pytest.mark.hf`, `@pytest.mark.hf_job`,
  the `test_lake` fixture): a run with M = 2 over 5 synthetic matches publishes per checkpoint over the gateway and
  prunes its versions past a G of 0 minutes; the run anchor and the acquisition marker are write-once objects under
  the gateway prefix `runs/lease/`. Write `test_reader_on_deleted_version_is_stale_or_completes`: a reader whose
  attached version is deleted mid-session gets `StaleCatalogError` or completes, never another error.
- [ ] **Step 2:** Redeploy the Job image and run the HF suite in it on the test buckets (Task 33 procedure,
  `LAKEHOUSE_HF_TEST_LAKE=1`, the existing gateway key through `--secrets-file` by path; the values are never
  read into the session). The owner directed on 2026-10-31: "I will not be doing things manually, that you have
  existing context on doing". The test pair `luxury-lakehouse/lake-test` and `/restricted-test` exists
  (created 2026-10-31 through the API; the existing gateway key reaches it). Expected: PASS. Then delete the test
  pair.

### Task 13: Docs and amendments

**Files:** `docs/specs/2026-10-26-v2-ingestion-design.md`, `docs/specs/2026-10-30-v2-s1-encryption-at-rest-design.md`,
`docs/ops/deploy.md`, `docs/audits/2026-10-30-v2-audit-ledger.md`

- [ ] **Step 1:** Apply the spec's "Amendments" list to the ingestion design: §17.2, §10.8, §10.3 step 5, §10.3 step
  7, §10.6, §10.2 (one file per table per write, and the native Parquet format), and §26 item 4; §10.7 stays (O-1).
  Add the S1 Non-goals note (the keyed plain open primitive moved into T3). Note the `checkpoint` settings in
  `deploy.md` next to the retention text. Mark T3 DONE in the audit ledger, with the test files.
- [ ] **Step 2:** Run `uv run pytest tests/contract -q`. Expected: PASS (the doc-contract tests).

### Task 14: Real-data validation on the Mac Mini

**Files:** none in the repo.

- [ ] **Step 1:** Push the tree to the Mac Mini. On a throwaway copy of the Mac Mini lake (outside `lake/`), run a
  heavy run with the new code: re-ingest gradientsports (`{kind: all}`, with `force_parse`), or `reparse --all`.
  Record the peak catalog bytes, the publish count, the retained versions, the exempted count of the sweep and the
  wall time in the ledger, against the 2026-10-31 numbers. Expected: publishes bounded by time, retained versions
  bounded by G, peak catalog bytes far below the 71 GB reference.
- [ ] **Step 2:** Delete the throwaway copy. The live Mac Mini lake stays untouched.

### Task 15: The full gate

- [ ] **Step 1:** On the Mac Mini, run the full gate. Expected: green; record the counts against the Task 0 baseline.
  Every new test file is in the default tier, except the `bench` scale test and the `hf` test.
- [ ] **Step 2:** No wheel bump and no `TODO.md`. The spec, this plan, the amendments and the ledger update ride the
  single pre-commit-program commit.

---

## Self-Review

- **Spec coverage:**
  - D1: Tasks 3, 4, 5. D2: Tasks 5, 9. D3: Task 9. D4: Tasks 1, 4, 5, 6, 12. D5: Tasks 2, 7, 8. D6: Tasks 1, 9c. D7:
    Tasks 1, 10. D8: Tasks 4, 5, 7, 11. D9: Task 5. O-5: Tasks 1, 9b.
  - Amendments: Task 13. Verification: the unit bullets map to Tasks 2–10, crash and resume to Task 8, the native
    file count to Task 9, the spike to Task 1, the scale test to Task 11, HF to Task 12, real data to Task 14.
  - O-1, O-3, O-4, O-5 and G were decided at Task 1 Step 7 (spec round 9, decisions 16–21).
- **Rulings for the ledger:**
  - `test_crash_loses_at_most_one_match` sets `checkpoint.matches = 1`; interval loss is tested in Task 8 (Task 5).
  - The grace uses the writer's clock against manifest `published_at` (Task 4).
  - `run_cleanup` keeps its pre-commit pruning, so the sweep's kept set still excludes the versions that retention
    releases; `run_end()` prunes again after the commit (Task 5).
  - `warn_versions` and `warn_catalog_mb` live in the `checkpoint` block, so the D8 thresholds are configured
    (Task 3).
  - The anchor and the marker sit under the gateway prefix `runs/lease/`, so HF `put_if_absent` works and their
    storage times share the gateway clock with the `data/` listing (Task 2).
  - A compaction whose verification fails publishes the original file with a `catalog_compaction` warning (Task 10).
  - The compaction threshold is a constant from the spike, not a setting (Task 10).
- **Type consistency:** `CheckpointPolicy.maybe/now/run_end/totals` (Task 4) are consumed in Tasks 5, 9 and 9b;
  `anchor_intervals`, `AnchorInterval`, `remove_anchors` and `Lease.confirm` (Task 2) in Tasks 7 and 8;
  `stage_match_move/finish_match_move` (Task 9) in `MatchFiles`; `plan_version_pruning(..., grace)` and
  `delete_versions -> list[int]` (Task 4) in Task 5 and `run_cleanup`.
- **Proportion:** signatures, test names and spec values; no code bodies.

## Execution handoff

Native execution in this session (no subagents), task by task, with the full gate and the heavy runs on the Mac Mini.
Then a two-reviewer A/B implementation review, then the owner's UI test and explicit approval, then the single
pre-commit-program commit. No per-task commits, no worktrees.

## Round-1 plan review disposition

Reviewer A: APPROVE WITH CHANGES (2 SHOULD FIX, 2 CONSIDER). Reviewer B: BLOCK (1 BLOCKING, 5 SHOULD FIX, 4
CONSIDER). Findings that changed the spec are also in the spec's round-8 table.

| Finding | Severity | Disposition |
|---|---|---|
| A T3-PLAN-01 | SHOULD FIX | Task 5 states the `finish()` split: `run_cleanup` (snapshot expiry, retention pruning, sweep) stays before the run-end commit; `run_end()` prunes after it. `test_finish_sweeps_before_and_prunes_after_the_run_end_commit`. |
| A T3-PLAN-02, B T3-PLAN-02 | SHOULD FIX | The register move publish is wired in Task 9 with the `moves.py` split, not in Task 5. Task 9 Step 1 adds a `src`-wide AST gate with an allowlist; it fails first on `moves.py:310` and `match_files.py:86`. |
| A T3-PLAN-03, B T3-PLAN-07 | CONSIDER | Owner decision 15; spec round 8 amends the lines. `test_read_during_run.py` stays unchanged; `test_reader_sees_matches_per_publish` pins per-publish visibility. |
| A T3-PLAN-04 | CONSIDER | The AST gate's allowlist names `sweep_now`, restore and relocate as maintenance publishes outside the policy. |
| B T3-PLAN-01 | BLOCKING | Owner decision 13, spec round 8 D5: the run anchor per generation and the write-time lease check replace the per-unit records. Tasks 2, 7 and 8 rewritten; the writer `store()` restructuring, `InflightRecorder` and `native_snapshot_keys` are gone. Task 8 names B's three small-lake crash cases and the other sources. |
| B T3-PLAN-03 | SHOULD FIX | No per-unit records remain. An anchor whose marker is missing or pruned exempts nothing and the next sweep removes it (`test_anchor_without_marker_exempts_nothing_and_is_removed`). |
| B T3-PLAN-04 | SHOULD FIX | Added: `test_run_end_keeps_versions_inside_grace`; `acquire` with anchors and markers present; `test_run_end_log_carries_publish_totals`; `test_catalog_growth_warning_on_size`; `test_keyed_plain_open_sets_temp_file_encryption`. |
| B T3-PLAN-05 | SHOULD FIX | Task 10: `COMPACT_MIN_BYTES` from Task 1 Step 2; a failed verification publishes the original file with a `catalog_compaction` warning, tested. |
| B T3-PLAN-06 | SHOULD FIX | Task 1 Step 4 measures the publish time and computes the burst storage for the proposed G; Step 7 brings G and its burst storage to the owner. |
| B T3-PLAN-08 | CONSIDER | Task 2 lists the five callers; `tests/unit/test_lease_callers.py` tests each one, and `test_cli_run_exits_1_on_failed_marker_store` pins the exit code. |
| B T3-PLAN-09 | CONSIDER | `delete_versions` returns confirmed deletes; `test_postponed_delete_still_counts_as_retained`. |
| B T3-PLAN-10 | CONSIDER | `test_in_run_pruning_respects_grace` states the exact deleted set {v1, v2} and kept set {v3, v4}. |
| B mapping note (D6) | — | Task 1 Step 7 brings any §10.2 gap to the owner explicitly. |

## Round-2 plan review disposition

Both reviewers: APPROVE (reports `D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-plan-r2-{A,B}.md`).
Reviewer A: no finding. Reviewer B: two CONSIDER. The spec round-8 CONSIDER T3-SPEC-21 also lands here.

| Finding | Severity | Disposition |
|---|---|---|
| B T3-PLAN-11 | CONSIDER | The register move commit is `runs/match_files.py:86` (`:104` is `_move_native`, which Task 5 converts). Corrected in Task 5 Step 3, Task 9 Step 1 and the round-1 table. |
| B T3-PLAN-12 | CONSIDER | The AST gate's allowlist names functions: `CheckpointPolicy` methods, `sweep_now`, `_restore_held`, `_relocate_held`, so a publish added to `run_cleanup` fails the gate. |
| Spec B T3-SPEC-21 | CONSIDER | Task 7: `_sweep` stops when a doomed key is in the `CURRENT` manifest; `test_sweep_stops_when_a_doomed_key_is_in_current`. |

## Round-3 plan review disposition

Reviewer A: APPROVE (reports `D:\Development\_reviews\2026-10-31-v2-t3-catalog-checkpoint-plan-r3-{A,B}.md`).
Reviewer B: APPROVE WITH CHANGES (one SHOULD FIX, one CONSIDER).

| Finding | Severity | Disposition |
|---|---|---|
| B T3-PLAN-13 (with spec T3-SPEC-22) | SHOULD FIX | Task 10: `is_wrong_key` in `catalog/duck.py` serves both keyed sites; `attach_or_config_error` stops mapping a corrupt catalog to a wrong key; `test_corrupt_encrypted_catalog_attach_keeps_its_own_error`. |
| B T3-PLAN-14 | CONSIDER | `insert_rows` lives in `catalog/duck.py`, so `resolve` does not import `runs` (no new package cycle). |
| B "could not verify" (Task 12) | — | Task 12 quotes the owner's direction; the secrets file goes by path only. |
