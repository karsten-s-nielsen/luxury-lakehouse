# M1 run-engine extraction — Implementation Plan

> **For workers:** execution is **native** (implemented in this session), then a two-reviewer A/B
> implementation review via the Skunk sessions, then the owner's final approval. Subagents are not used. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Status:** A/B plan review COMPLETE — `(Ragna): Review A-Skunk` and `(D32): Review B-Skunk` both APPROVE
(A round 3, B round 4). One optional round-4 consider (B-17) folded. Owner APPROVED 2026-10-30 — executing
natively; no commit until the owner's go after the A/B implementation review.

**Goal:** Split `runs/run.py` `_Run` into a thin orchestrator plus three injected collaborators
(`MatchFiles`, `VisibilityMover`, `MatchWriter`) and a leaf `_common`, with no behavior change.

**Architecture:** A new leaf module `runs/_common.py` holds the shared types, constants and helpers (and the
run-logger name). `MatchFiles` owns the match-file helpers both clusters share. `VisibilityMover` and
`MatchWriter` each own one cluster and depend on `MatchFiles`. `_Run` keeps lifecycle, the module/match loop
and the per-match orchestration, and drives the collaborators. A new import-linter layers contract forbids the
back-edges.

**Tech Stack:** Python **3.12** (`requires-python = ">=3.12,<3.13"`, pyright `pythonVersion = "3.12"`),
DuckLake/DuckDB, pyarrow, pyright strict, ruff, import-linter 2.15, pytest.

**Branch:** `feat/v2-ingestion` (the whole untracked `v2/` tree rides the single pre-commit-program commit on
this branch; no worktree).

**Spec:** `docs/specs/2026-10-30-v2-m1-run-engine-extraction-design.md` (owner-approved 2026-10-30; both
reviewers approve). Read it with this plan.

## Global Constraints

- **One commit for the whole pre-commit program. No per-task commits, no micro-commits.** Every task ends with
  its tests green and the work left in the working tree. The session never commits on its own: after the A/B
  implementation review and the owner's approval, it shows the diff and file list and waits for the owner's
  explicit yes, then makes the single commit that also carries the UI rework and the other decided audit items.
- **No behavior change.** The record shapes (`RunRow`, `ModuleRun`, `MatchStep`, `ErrorRow`, `MatchRow`,
  `MatchFile`, `LineageRow`, `MatchSource`, `Progress`), the result objects (`RunResult`, `RunCounts`,
  `RunError`), the exit codes, the error `kind` strings, the JSON log `logger` field (`lakehouse.runs.run`),
  the OpenTelemetry step-span names/attributes/nesting, and the `ops_*` schemas are all unchanged.
- **`run_ingestion` and `reparse_matches` are not edited** (run.py:1308, 1223). `_Run` keeps `fold_skips`,
  `settle_moves` as real methods and `visibility_pass` as a thin delegator.
- **Baselines (recorded in Task 0, compared in Final verification).** Because `v2/` is untracked, `git diff`
  cannot detect an accidental edit, so the no-change checks use content hashes and counts: the sha256 of each
  oracle test file, the sha256 of the Task 0 pin file (hashed after it is written and green), the sha256 of the
  `run_ingestion` and `reparse_matches` **source segments** (captured per function with
  `ast.get_source_segment`, not by line range, so an unrelated edit does not move them), and the exact
  `passed/skipped/deselected` counts of the oracle suite. A no-behavior-change refactor must not move any of
  them; the final step re-reads and fails on drift. Watched to fail once (edit one oracle file, see the compare
  fail, revert).
- **Placement decisions (resolving the spec's plan-stage deferrals):** `RunResult`, `RunCounts`, `RunError`
  stay in `run.py` (`__all__`, run.py:94). `_Stop` and `_signal_unchanged` stay in `run.py` (only
  `_Run`-retained code uses them: `_Stop` at run.py:304/452/499/1279/1362, `_signal_unchanged` at run.py:703).
  `_log` stays in `run.py` unchanged (see the logger note below). The spec prose (spec:84–86) lists
  `_Stop`/`_signal_unchanged` in the cluster-5 move set as a first pass; this plan is their resolution.
- **Shared names are module-public (no leading underscore)** so pyright strict does not raise
  `reportPrivateUsage` on a cross-module import in `src/`, and no suppression is added there. Only the module
  name stays private (`_common`). The no-suppression rule is a `src/` rule: test files that touch
  `_Run`/`failed_run` internals use the house `# pyright: ignore[reportPrivateUsage] - reason` convention, as
  the existing tests do. `run.py` may alias names on import (for example
  `from lakehouse.runs._common import Tally as _Tally`, `FATAL as _FATAL`) to keep its body byte-identical;
  each alias passes ruff N811 because it keeps its kind's casing — a class or function to an underscored
  lower-case name (`Tally -> _Tally`), a constant to an underscored ALL-CAPS name (`FATAL -> _FATAL`). N811
  fires only for a constant aliased to a non-constant-cased name, which is why `LOGGER as _log` is not used and
  the logger goes through `RUN_LOGGER_NAME` instead.
- **The logger is handled without a move.** `run.py` keeps its module-level `_log = get_logger(__name__)`
  unchanged (so the six `log=_log` lines inside the entry points stay byte-identical and the baseline holds).
  `_common` exposes a public `RUN_LOGGER_NAME = "lakehouse.runs.run"`. Each collaborator defines its own local
  `_log = get_logger(RUN_LOGGER_NAME)`. `get_logger` returns the same logger object by name, so every module
  logs under `lakehouse.runs.run`. Aliasing `LOGGER as _log` is not used (ruff N811), and referring to the
  logger as `LOGGER` in `run.py` is not used (it would edit the entry-point `log=` lines).
- Every `# noqa: S608` and `# noqa: BLE001` keeps its exact reason text and behavior. Every
  `self.cat.commit("in_run")` stays where it is (run.py:317, 411, 865, 988; moves.py:310).
- Gates (all green, run from `v2/`): `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`
  (0 errors), `uv run lint-imports` (now **6** kept), `uv run pytest -m "not live and not hf and not bench"`
  with `LAKEHOUSE_OS_TESTS=1`, `uv run python scripts/pip_audit.py`.
- Each new test (the Task 0 pin and every collaborator unit test) is watched to fail first on a **mutant** (a
  wrong value or a dropped call), not merely on an `ImportError`. The import-linter contract is watched to fail
  first on a deliberate bad import.

## Review Focus

Inputs the spec implies but that the happy-path tests may not exercise — each gets a test in the owning task:
- A visibility full-listing that raises: the run writes the warning row **and** ends `status=failed` (Task 3).
- A kept-copy flag flip public↔private: the move happens; in `_write` the public→private move precedes the
  write and private→public follows (Task 2, Task 4).
- A moved method's log/span: the `logger` field stays `lakehouse.runs.run` and the move/write/commit span
  attributes are unchanged (Task 0, green on today's `run.py`; it then guards every later task).
- A collaborator importing `run.py` (a cycle): the import-linter layers contract rejects it (Task 5).
- A not-kept copy and the reparse/crash-recovery/privacy paths: the expanded oracle suite covers them
  (Tasks 2–4 per-task runs).

## File Structure

- Create `src/lakehouse/runs/_common.py` — leaf, module-public names: `Tally`, `ModuleContext`, `FullListing`
  (Protocol), `ErrorSink` (new Protocol), `FATAL`, `THREE_SIXTY`, `TRACKING_ARTIFACTS`, `AUDIT_NAMES`,
  `RUN_LOGGER_NAME = "lakehouse.runs.run"`, `content_version`, `content_version_of`, `nonnull_fields`,
  `text_or_none`.
- Create `src/lakehouse/runs/match_files.py` — `MatchFiles`.
- Create `src/lakehouse/runs/visibility.py` — `VisibilityMover`.
- Create `src/lakehouse/runs/match_writer.py` — `MatchWriter`.
- Modify `src/lakehouse/runs/run.py` — import from the four new modules (aliasing allowed); `_Run.__init__`
  builds the collaborators; delete the extracted methods; keep the lifecycle, the loop, the per-match
  orchestration and the `visibility_pass` delegator. `_Stop`, `_signal_unchanged`, `_log`, `RunResult`,
  `RunCounts`, `RunError` stay here.
- Modify `pyproject.toml` — add the sixth import-linter contract.
- Create test `tests/unit/test_run_observability_pins.py` (Task 0). New collaborator tests:
  `tests/unit/test_match_files.py`, `tests/unit/test_visibility_mover.py`, `tests/unit/test_match_writer.py`.
- Behavior oracle (not edited): `tests/e2e/test_run_local.py`, `test_hf_run_scenario_local.py`,
  `test_cleanup_multirun.py`, `test_read_during_run.py`, `test_crash_recovery.py`, `test_dbt_handoff.py`,
  `test_no_private_rows.py`; `tests/unit/test_run_history.py`, `test_run_visibility.py`, `test_run_listings.py`,
  `test_run_steps.py`, `test_reparse.py`, `test_cli.py`; `tests/integration/test_run_observability_e2e.py`.

---

### Task 0: Baselines and the observability pin (green on today's `run.py`)

**Files:**
- Create: `tests/unit/test_run_observability_pins.py`
- Baseline artifact: a sha256 + count record (task notes / scratch file, not committed).

- [ ] **Step 1: Record the file + count baselines** — sha256 each oracle file listed in File Structure; capture
  the `run_ingestion` and `reparse_matches` source **segments** via `ast.get_source_segment` and sha256 each;
  record the oracle suite's `passed/skipped/deselected` from `uv run pytest <oracle files> -q` with
  `LAKEHOUSE_OS_TESTS=1`.
- [ ] **Step 2: Write the pin** `test_logger_and_spans_on_move_and_write` in
  `tests/unit/test_run_observability_pins.py`: drive the minimal `_Run` path that performs one `_move` and one
  `_write`, capture the JSON logs and spans, assert the `logger` field equals `lakehouse.runs.run` and the
  `lakehouse.run.move` / `.write` / `.commit` spans keep their names and attributes.
- [ ] **Step 3: Run it on today's `run.py`** — `uv run pytest tests/unit/test_run_observability_pins.py -q`.
  Expected: PASS (nothing extracted yet — the pre-move baseline).
- [ ] **Step 4: Mutant** — change run.py's `_log = get_logger(__name__)` to `get_logger("x")`; confirm the pin
  goes red; revert.
- [ ] **Step 5: Hash the pin file** (now written and green) and add its sha256 to the baseline set, so the
  Final-step compare guards the pin itself.

### Task 1: `_common` leaf module

**Files:**
- Create: `src/lakehouse/runs/_common.py`
- Modify: `src/lakehouse/runs/run.py` (move the names out; import them back, aliasing to the existing local
  spellings; leave `_log` as is)
- Test: the oracle suite is the check (pure move).

**Interfaces:**
- Produces (all module-public): `Tally`, `ModuleContext` (fields `module`, `adapter`, `parser`,
  `module_run_id`, `tally`, `steps`), `FullListing` (Protocol), `ErrorSink` (Protocol, below), `FATAL`,
  `THREE_SIXTY`, `TRACKING_ARTIFACTS`, `AUDIT_NAMES`, `RUN_LOGGER_NAME` (`str`),
  `content_version(deliveries: Mapping[str, Delivery]) -> str`,
  `content_version_of(parsed: ParsedMatch) -> str`, `nonnull_fields(parsed: ParsedMatch) -> dict[str, bool]`,
  `text_or_none(value: object) -> str | None`.
- `ErrorSink` signature (matches the bound `_Run._error`):
  ```python
  class ErrorSink(Protocol):
      def __call__(
          self,
          *,
          kind: str,
          message: str,
          severity: Literal["error", "warning"],
          module_run_id: str | None = None,
          module_id: str | None = None,
          native: str | None = None,
      ) -> None: ...
  ```

- [ ] **Step 1: Create `_common.py`** with the names above, `RUN_LOGGER_NAME = "lakehouse.runs.run"`. Leave
  `_Stop`, `_signal_unchanged`, `_log`, `RunResult`, `RunCounts`, `RunError` in `run.py`.
- [ ] **Step 2: Re-import in `run.py`** — `from lakehouse.runs._common import (Tally as _Tally, ...)`; `run.py`
  keeps its own `_log = get_logger(__name__)` unchanged.
- [ ] **Step 3: Gate** — `uv run pyright` (0 errors — module-public names, no `reportPrivateUsage`, no
  suppressions); `uv run ruff check .` (no N811); `uv run pytest tests/unit/test_run_history.py
  tests/unit/test_run_visibility.py tests/unit/test_run_listings.py tests/unit/test_run_steps.py
  tests/unit/test_reparse.py tests/unit/test_cli.py tests/e2e/test_run_local.py
  tests/unit/test_run_observability_pins.py -q`. Expected: PASS / 0 errors.

### Task 2: `MatchFiles`

**Files:**
- Create: `src/lakehouse/runs/match_files.py`
- Modify: `src/lakehouse/runs/run.py` (`_Run.__init__` builds `self._files`; `_apply_flags`, `_visibility_only`,
  `_write`, `_count_excluded`, `_match` call it; delete `_ops_matches`/`_files_of`/`_move` from `_Run`)
- Test: `tests/unit/test_match_files.py`

**Interfaces:**
- Consumes: `_common.RUN_LOGGER_NAME` (defines a local `_log = get_logger(RUN_LOGGER_NAME)`).
- Produces: `MatchFiles(cat: WorkingCatalog, storage: Storage, archive: RawArchive, run_id: str,
  now: Callable[[], datetime])` with `ops_matches() -> dict[int, dict[str, Any]]`,
  `files_of(key: int) -> list[MatchFile]`,
  `move(key: int, visibility: Literal["public", "private"], row: Mapping[str, Any]) -> None`. `move` performs
  the `commit("in_run")` and the lineage update exactly as run.py:389–411.

- [ ] **Step 1: Failing test** `test_move_follows_new_file_ids` — move a match's files, assert the registry
  rows carry the new write ids/root and the lineage `visibility` is updated.
- [ ] **Step 2: Verify it fails** — `uv run pytest tests/unit/test_match_files.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement `MatchFiles`** by moving the three methods verbatim; wire `_Run`.
- [ ] **Step 4: Gate** — `uv run pytest tests/unit/test_match_files.py tests/unit/test_run_visibility.py
  tests/e2e/test_run_local.py tests/e2e/test_no_private_rows.py tests/e2e/test_crash_recovery.py -q`. Expected:
  PASS.
- [ ] **Step 5: Mutant** — `new_key.root` → `file.root`; confirm `test_move_follows_new_file_ids` red; revert.

### Task 3: `VisibilityMover`

**Files:**
- Create: `src/lakehouse/runs/visibility.py`
- Modify: `src/lakehouse/runs/run.py` (build `self._visibility`; `visibility_pass` becomes the delegator;
  delete `_apply_flags`)
- Test: `tests/unit/test_visibility_mover.py`

**Interfaces:**
- Consumes: `MatchFiles`, `_common.FATAL`, `_common.RUN_LOGGER_NAME`, `_common.FullListing`, `ErrorSink`.
- Produces: `VisibilityMover(cat: WorkingCatalog, settings: Settings, adapters: Mapping[ModuleId,
  ChannelAdapter], files: MatchFiles, errors: ErrorSink)` with
  `run() -> tuple[dict[ModuleId, list[SourceEntry]], bool]`. The `_Run.visibility_pass` delegator:
  `listings, failed = self._visibility.run(); self.failed_run = self.failed_run or failed; return listings`.
  The `or` is a defensive invariant (nothing sets `failed_run` True before the delegator today); Step 5(c)
  pins it so it cannot silently rot.

- [ ] **Step 1: Failing tests** in `tests/unit/test_visibility_mover.py`: `test_kept_copy_flag_flip_moves`;
  `test_failed_full_listing_fails_the_run` (adapter raises → `errors(...)` warning row **and** `run()` returns
  `failed=True`); `test_visibility_pass_preserves_prior_failed_run` (on the `_Run` delegator: set
  `failed_run=True`, stub `run()` to return `({}, False)`, assert `failed_run` stays `True`).
- [ ] **Step 2: Verify they fail** — `uv run pytest tests/unit/test_visibility_mover.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement `VisibilityMover`** (typed signatures) by moving `visibility_pass` + `_apply_flags`,
  replacing the direct `self.failed_run = True` (run.py:368) with a returned `failed` flag. Add the delegator.
- [ ] **Step 4: Gate** — `uv run pytest tests/unit/test_visibility_mover.py tests/unit/test_run_visibility.py
  tests/e2e/test_run_local.py tests/unit/test_run_observability_pins.py -q`. Expected: PASS.
- [ ] **Step 5: Mutants** — (a) `run()` returns `failed=False` on a raising listing →
  `test_failed_full_listing_fails_the_run` red; (b) drop the move in the flag-flip branch →
  `test_kept_copy_flag_flip_moves` red; (c) delegator `self.failed_run = failed` (drop the `or`) →
  `test_visibility_pass_preserves_prior_failed_run` red. (The propagate-failed direction is additionally
  guarded by the oracle `tests/unit/test_run_visibility.py::test_visibility_listing_failure_visible`, line 75.)
  Revert each.

### Task 4: `MatchWriter`

**Files:**
- Create: `src/lakehouse/runs/match_writer.py`
- Modify: `src/lakehouse/runs/run.py` (build `self._writer`; `_match` calls it; delete the write cluster)
- Test: `tests/unit/test_match_writer.py`

**Interfaces:**
- Consumes: `MatchFiles`, `_common` (`AUDIT_NAMES`, `ModuleContext`, `THREE_SIXTY`, `TRACKING_ARTIFACTS`,
  `content_version`, `content_version_of`, `nonnull_fields`, `text_or_none`, `RUN_LOGGER_NAME`), `ErrorSink`.
- Produces: `MatchWriter(cat: WorkingCatalog, storage: Storage, work_dir: Path, run_id: str,
  now: Callable[[], datetime], files: MatchFiles, errors: ErrorSink)` with fully-typed public methods:
  - `choose(key: int, channel: Channel, listed: SourceEntry, parsed: ParsedMatch, lineage: Sequence[LineageRow]) -> Choice`
  - `record_not_kept(key: int, context: ModuleContext, listed: SourceEntry, deliveries: Mapping[str, Delivery], parsed: ParsedMatch, choice: Choice) -> None`
  - `write(key: int, context: ModuleContext, listed: SourceEntry, deliveries: Mapping[str, Delivery], parsed: ParsedMatch, choice: Choice, previous: Mapping[str, Any] | None) -> None`
  - `refresh_signals(key: int, channel: Channel, deliveries: Mapping[str, Delivery]) -> None`
  - `listed_again(key: int, context: ModuleContext, listed: SourceEntry) -> None`
  Reads only `ModuleContext`'s identity fields (`module`, `adapter`, `parser`, `module_run_id`).

- [ ] **Step 1: Failing tests** in `tests/unit/test_match_writer.py`:
  `test_choose_keeps_public_when_pining_adds_nothing`; `test_choose_keeps_pining_when_it_adds_fields`;
  `test_write_produces_expected_match_and_lineage`; `test_write_moves_public_to_private_before_write`;
  `test_write_moves_private_to_public_after_write`; `test_record_not_kept_writes_lineage_without_bronze`;
  `test_writer_reads_only_context_identity` (pass a `ModuleContext` whose `tally` and `steps` are properties
  that raise `AssertionError` on **any** access — a bare read trips it too; `write` and `choose` still
  succeed).
- [ ] **Step 2: Verify they fail** — `uv run pytest tests/unit/test_match_writer.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement `MatchWriter`** (typed as above) by moving the write cluster verbatim, swapping
  `self.` state for injected deps and `self._move`/`self._files_of` for `self._files.move`/`files_of`. Wire
  `_Run._match`.
- [ ] **Step 4: Gate** — `uv run pytest tests/unit/test_match_writer.py tests/unit/test_run_history.py
  tests/unit/test_run_listings.py tests/unit/test_reparse.py tests/e2e/test_run_local.py
  tests/e2e/test_cleanup_multirun.py tests/e2e/test_no_private_rows.py tests/e2e/test_crash_recovery.py
  tests/e2e/test_dbt_handoff.py -q`. Expected: PASS.
- [ ] **Step 5: Mutants (one per test)** — drop the public→private pre-move (red:
  `..._public_to_private_before_write`); do the private→public move before the write (red:
  `..._private_to_public_after_write`); return the wrong channel in `choose` (red: both choose tests); change
  one `MatchRow` field in `write` (red: `..._produces_expected_match_and_lineage`); write a bronze file in
  `record_not_kept` (red: `..._writes_lineage_without_bronze`); read `context.tally` in `write` (red:
  `test_writer_reads_only_context_identity`). Revert each.
- [ ] **Step 6: Collaborator logger mutants (one module at a time)** — (a) set `_log = get_logger(__name__)`
  in `match_files.py` only; run `uv run pytest tests/unit/test_run_observability_pins.py -q`; confirm
  `test_logger_and_spans_on_move_and_write` goes red (the move records would log under
  `lakehouse.runs.match_files`); revert. (b) set `_log = get_logger(__name__)` in `match_writer.py` only; run
  the pin; confirm it goes red (the write records would log under `lakehouse.runs.match_writer`); revert. Two
  separate runs prove the pin checks the move records and the write records independently. (The Task 0 `run.py`
  `get_logger("x")` mutant stays as the pre-move proof.)

### Task 5: Acyclicity contract

**Files:**
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: the four new modules (all must exist; import-linter cannot resolve a layer whose module is absent,
  so the contract follows Tasks 1–4 — the fail-first is the deliberate bad import below).

- [ ] **Step 1: Add the layers contract** to `pyproject.toml`:
  ```toml
  [[tool.importlinter.contracts]]
  name = "The run engine is layered: run > visibility/match_writer > match_files > _common"
  type = "layers"
  containers = ["lakehouse.runs"]
  exhaustive = false
  layers = [
      "run",
      "visibility | match_writer",
      "match_files",
      "_common",
  ]
  ```
- [ ] **Step 2: Watch it fail first** — add `from lakehouse.runs import run` to `match_files.py`; run
  `uv run lint-imports`; confirm the new contract is **broken** (total 6 contracts); remove the bad import;
  confirm **6 kept**.

### Final verification (no commit)

- [ ] **Step 1: Compare the Task 0 baselines** — re-sha256 the oracle files, the pin file, and the
  `run_ingestion`/`reparse_matches` source segments; re-count the oracle suite. Expected: identical to Task 0.
  (Watched to fail: edit one oracle file, see the compare fail, revert.)
- [ ] **Step 2: Full gate from `v2/`** — `uv run ruff check .`; `uv run ruff format --check .`;
  `uv run pyright` (0 errors); `uv run lint-imports` (6 kept); `uv run pytest -m "not live and not hf and not
  bench"` with `LAKEHOUSE_OS_TESTS=1` (baseline count + the added collaborator/pin tests);
  `uv run python scripts/pip_audit.py`.
- [ ] **Step 3: Hand off for the A/B implementation review** (Skunk sessions), then the owner's final approval.
  Do **not** commit: the work stays in the working tree for the single pre-commit-program commit.

## Round-1 review responses

(Resolved in rev 2; summarized.) B-01 BLOCKING → sha256 + count baselines (Task 0 / Final Step 1), not
`git diff`. B-02 → module-public shared names, no suppressions; the logger is handled by `RUN_LOGGER_NAME`
without a move (see round 2). B-03 → the pin is Task 0, new file, move+write, green on today's `run.py`. B-04 →
five oracle files added. B-05 → a mutant per new test. B-06/07/08 → delegator mutant (see round 2);
`exhaustive = false` + fail-first bad import; typed signatures + identity-field test; Python 3.12. A-01..04 →
`_Stop`/`_signal_unchanged` and `RunResult`/`RunCounts`/`RunError` placement stated; branch named; test-count
baseline.

## Round-2 review responses

- **A-05 / B-11 (same finding — the delegator-OR mutant was equivalent / unguarded):** fixed. The `or` is kept
  as a defensive invariant and pinned by a new delegator unit test
  `test_visibility_pass_preserves_prior_failed_run` (pre-set `failed_run=True`, `run()` returns `failed=False`,
  assert it stays `True`), which makes `self.failed_run = failed` a killable mutant (Task 3 Step 5c). The
  propagate-failed direction stays guarded by the oracle `test_visibility_listing_failure_visible`.
- **B-09 (the pin file was hashed before it existed):** fixed. Task 0 Step 5 hashes the pin after it is green
  and adds it to the baseline set; Final Step 1 compares it.
- **B-10 + logger follow-up (`LOGGER as _log` trips N811; `LOGGER` in run.py would edit the entry points):**
  fixed. `run.py` keeps `_log = get_logger(__name__)` unchanged; `_common` exposes public `RUN_LOGGER_NAME`;
  each collaborator defines its own `_log = get_logger(RUN_LOGGER_NAME)`. No alias, no N811, no
  `reportPrivateUsage`, no entry-point edit.
- **B-12 (entry-point hashing):** fixed. The baseline hashes the `run_ingestion`/`reparse_matches` source
  **segments** via `ast.get_source_segment`, not a line range.
- **B-13 (Task 4 typing; identity sentinel):** fixed. `record_not_kept`/`write`/`refresh_signals`/
  `listed_again` are fully typed; `test_writer_reads_only_context_identity` uses a `ModuleContext` whose
  `tally`/`steps` raise on any access.
- **A-06 (test count):** corrected. The new tests are: Task 0 pin (1), Task 2 (1), Task 3 (3), Task 4 (7) —
  each with a mutant; the earlier "eight" was wrong.

## Round-3 review responses

- **A — APPROVED round 3, no open findings.** The delegator mutant is confirmed non-equivalent; the
  `RUN_LOGGER_NAME` change does not regress the module-public-names consistency.
- **B-14 (pin must go red on a collaborator mutant, not only the `run.py` one):** fixed. Task 4 Step 6 sets
  `_log = get_logger(__name__)` in `match_files` and `match_writer` and confirms the pin goes red; the Task 0
  `run.py` mutant stays as the pre-move proof.
- **B-15 (test-side `reportPrivateUsage`):** clarified. The no-suppression rule is a `src/` rule; test files
  touching `_Run`/`failed_run` use the house `# pyright: ignore[reportPrivateUsage] - reason` convention.
- **B-16 (N811 reason):** corrected. An alias passes N811 when it keeps its kind's casing
  (`FATAL -> _FATAL` stays ALL-CAPS); N811 fires only for a constant aliased to a non-constant-cased name.
- **B-17 (round 4, consider — split the collaborator logger mutants):** fixed. Task 4 Step 6 runs two separate
  one-module-at-a-time mutants, so the pin proves it checks the move records and the write records
  independently. Both reviewers then APPROVE (A round 3, B round 4).

## Self-review notes

- Spec coverage complete (T0 baselines+observable, T1 `_common`, T2 `MatchFiles`, T3 `VisibilityMover` incl.
  the failed_run seam + the delegator invariant, T4 `MatchWriter` incl. move ordering / not-kept /
  identity-field, T5 contract; entry points unedited). Each Review-Focus input has an owning test. No per-task
  commit (owner rule); one commit at the end, gated on the owner.
