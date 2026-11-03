# v2 M1 — run engine extraction (Option A) design

**Status:** REVISED after A/B spec review round 2. Reviewer `(Ragna): Review A-Skunk` APPROVED round 2;
reviewer `(D32): Review B-Skunk` round 2 was REQUEST CHANGES (1 should-fix + 3 consider), all folded here, and
B APPROVED round 3 (two optional nits folded). Both reviewers APPROVE; owner APPROVED 2026-10-30 — proceeding
to the plan stage.
Both confirmed Option A and the `MatchFiles` seam direction as sound; the cluster
partition and the DAG were refined through the two review rounds. Findings are answered in the review-response
sections at the end. Approach (Option A) approved by the owner 2026-10-30. Part of the gold-standard pre-commit
program (audit ledger `docs/audits/2026-10-30-v2-audit-ledger.md`, finding M1).

## Intent and why

`src/lakehouse/runs/run.py` `_Run` (lines 236–1079, about 840 lines) is a god module. One class folds six
concern-clusters that share `cat`, `storage`, `archive`, `totals` and the `_step`/`_error` sinks:

1. lifecycle and run-start housekeeping — `__init__`, `_step`, `_error`, `_check_stop`, `fold_skips`,
   `settle_moves`, `finish`, `problem`, `stopped`, `counts`;
2. visibility (§9, §29.14) — `visibility_pass`, `_apply_flags`;
3. per-module loop — `modules`, `_record_module`, `_listed`, `_module`, `_match_steps`, `_listing_parser`,
   `_listings`, `_count_excluded`;
4. per-match kept-copy write and lineage — `_match_guarded`, `_match`, `_visibility_only`, `_listed_again`,
   `_refresh_signals`, `_source`, `_choose`, `_has_copy_fields`, `_copy_fields`, `_lineage_rows`,
   `_record_not_kept`, `_write`, `_has_360`;
5. shared match-file helpers — `_ops_matches`, `_files_of`, `_move`;
6. reparse — `reparse` (with module-level `_reparse_targets` and `reparse_matches`).

The class has many reasons to change, and the write and move logic is not unit-testable without a full run.
This is audit finding M1 (Medium). This cycle gives each unit one reason to change and makes the heavy
collaborators testable in isolation, with no behavior change.

## Goals

- Extract **`VisibilityMover`** (cluster 2), **`MatchWriter`** (cluster 4) and a small shared **`MatchFiles`**
  (cluster 5) as injected collaborators.
- Leave `_Run` as a thin orchestrator: lifecycle, run-start housekeeping and the module and per-match loop,
  driving the collaborators.
- Preserve behavior exactly. The public `run_ingestion` and `reparse_matches` entry points are unchanged, down
  to the methods they call on `_Run`. The existing end-to-end run tests pass unchanged, with no edits.
- Add focused unit tests for each new collaborator.

## Non-goals

- No behavior change and no change to any observable output (see "Behavior-preservation guardrail").
- Do not extract the module loop (cluster 3) or reparse (cluster 6). The module loop is the orchestration
  spine and stays in `_Run`. Reparse delegates to the module-level `_reparse_targets`/`reparse_matches`.
- Not Option B (a phase-pipeline with a `RunState` object — it relocates the shared state into a new
  god-object) and not Option C (extract `MatchWriter` only — it leaves visibility and the shared helpers
  tangled).

## Anti-goals

- A collaborator that imports `run.py`, or that depends on `_Run` through bound `_Run` methods (a cycle or
  hidden coupling). This anti-goal is **enforced**, not just asserted (see "Acyclicity enforcement"). The one
  sanctioned exception is the error sink: an `ErrorSink` Protocol satisfied by the bound `_Run._error`, passed
  by explicit injection — a declared dependency, not hidden coupling.
- A single shared mutable state object threaded through every collaborator (the Option B failure mode).

## Constraints

- The five existing import-linter contracts stay green, and a new sixth contract enforces the internal
  layering of `lakehouse.runs` (see "Acyclicity enforcement"). The new classes add no cross-layer edge.
- pyright strict (run via `uv run pyright`), ruff rules E,W,F,I,N,UP,B,S,BLE,RUF, 120-column.
- Every `# noqa: S608` (constant table name in an f-string) and every `# noqa: BLE001` catch keeps its exact
  reason text and behavior. The BLE catches that record a warning and continue (§12.3) behave identically.
- WorkingCatalog is a single DuckLake connection. Every `self.cat.commit("in_run")` stays exactly where it is:
  in `fold_skips` (run.py:317), in `_move` (run.py:411, plus the move inside `move_match_files` at
  `catalog/moves.py:310`), in `_record_not_kept` (run.py:865), and in `_write` **after** `swap_match_files`
  (run.py:988). The move ordering (public→private before the write, private→public after; run.py:879, 990) is
  load-bearing and preserved.

## The design

### Shared state today

`_Run.__init__` holds `storage`, `trigger`, `adapters`, `parsers`, `now`, `run_id`, `lease`, `settings`, `cat`
(a `WorkingCatalog` with `.conn`, `.current`, `.commit`), `work_dir`, `stop`, `archive` (a `RawArchive`),
`errors`, `failed_run`, `terminated`, `started_at`, `progress`, `matches_done`, `totals` (a `_Tally`) and
`force_parse`. `_step`, `_error` and `_check_stop` are the bookkeeping sinks. `failed_run` is set directly by
`visibility_pass` (run.py:368) and by `_error` when `severity == "error"` (run.py:298–300).

### Module placement (kills the import cycle)

The extracted methods use module-level names that live in `run.py` today: the types `_ModuleContext`, `_Tally`,
`_Stop`, `FullListing`; the constants `_FATAL`, `_THREE_SIXTY`, `_TRACKING_ARTIFACTS`, `_AUDIT_NAMES`; the
logger `_log`; and the helpers `_content_version`, `_content_version_of`, `_signal_unchanged`,
`_nonnull_fields`, `_text`. If a collaborator imported these from `run.py` while `run.py` imported the
collaborator, that is an import cycle.

Resolution: move those shared leaf names into a new leaf module **`runs/_common.py`**. Both `run.py` and the
three collaborators import from `_common`; no collaborator imports `run.py`. The plan decides whether the
collaborators are three modules (`match_files.py`, `visibility.py`, `match_writer.py`) or fewer, with the new
import-linter contract as the constraint.

### The seam (`MatchFiles`)

Three helpers are called from more than one cluster: `_ops_matches` (read `ops_matches` into a dict) has three
callers — `_apply_flags` (VisibilityMover), `_match` and `_count_excluded` (both stay in `_Run`); `_move` is called by `_apply_flags` (VisibilityMover),
`_visibility_only` and `_write`; `_files_of` is called by `_move` and `_write`. To avoid a two-way
dependency, these three helpers form a leaf collaborator **`MatchFiles`**. `VisibilityMover`, `MatchWriter` and
`_Run` all depend on `MatchFiles`; `MatchFiles` depends on nothing in `lakehouse.runs` except `_common`. The
dependency graph is a DAG:

```
_Run ─┬─> VisibilityMover ─┐
      ├─> MatchWriter ──────┼─> MatchFiles ─> {cat, storage, archive, run_id, now}
      └─> MatchFiles ───────┘
all of the above ─> _common (leaf: types, constants, helpers, the run logger)
```

### `MatchFiles` (leaf)

Owns `_ops_matches`, `_files_of`, `_move`. Injected dependencies, each used: `cat` (reads/updates, commits in
`_move`), `storage` (`move_match_files`), `archive` (`move_match_files` passes it), `run_id` (the moved
`MatchRow.run_id`, run.py:404), `now` (the moved-row `updated_at`, run.py:404). It logs via the `_common`
`_log` (imported, not injected). It performs the `commit("in_run")` inside `_move` exactly as today.

### `VisibilityMover`

Owns `visibility_pass` and `_apply_flags`. Injected and used: `cat`, `settings`, `adapters`,
`MatchFiles` (for `_ops_matches` and `_move`), and an error sink. It logs via the `_common` `_log` (imported,
not injected).
`visibility_pass` returns the full listings dict **and** whether the run failed, because a failed full listing
writes a `severity="warning"` row and then marks the run failed directly (run.py:361–368) — the error sink does
*not* set `failed_run` for a warning. `_Run`'s thin `visibility_pass` delegator ORs that returned flag into
`self.failed_run`. It is injected neither `run_id` nor `now` nor a step sink (none of its two methods use them).

### `MatchWriter`

Owns `_choose`, `_has_copy_fields`, `_copy_fields`, `_lineage_rows`, `_record_not_kept`, `_write`, `_source`,
`_has_360`, `_refresh_signals`, `_listed_again`. Its public surface, called by `_Run._match`, is `choose`,
`record_not_kept`, `write`, `refresh_signals` and `listed_again`. Injected and used: `cat`, `storage`,
`work_dir` (the per-match out-dir in `_write`), `run_id`, `now`,
`MatchFiles` (for `_move` and `_files_of` in `_write`), and an error sink (the `list_fields_mismatch` warnings
and the `has_360` mismatch warning). It logs via the `_common` `_log` (imported, not injected), and reads only
the identity fields of `_ModuleContext` (`module`, `adapter`, `parser`, `module_run_id`), not the loop's
`tally`/`steps`. It is **not** injected `archive`: `_write` does not touch the archive; `archive.store` is
called in `_Run._match` (run.py:719–720), which stays in `_Run`.

### `_Run` after the extraction

Keeps `__init__` (which builds `MatchFiles`, then `VisibilityMover`, `MatchWriter`), the lifecycle and
run-start housekeeping (`_step`, `_error`, `_check_stop`, `fold_skips`, `settle_moves`, `finish`, `problem`,
`stopped`, `counts`), the module loop (`modules`, `_record_module`, `_listed`, `_module`, `_match_steps`,
`_listing_parser`, `_listings`, `_count_excluded`), the per-match orchestration (`_match_guarded`, `_match`,
`_visibility_only`) and `reparse`. `_Run` holds a `MatchFiles` reference and uses `_ops_matches` in
`_count_excluded` and `_match`, and `MatchFiles.move` in `_visibility_only`. `_match` orchestrates fetch →
archive → parse, then calls `MatchWriter` for choose/record-not-kept/write/refresh/listed-again. `_Run` drops
to roughly 450–500 lines.

`fold_skips` (lease and run-history housekeeping) and `settle_moves` (a start-of-run cleanup) stay in `_Run`;
they are not visibility logic, so they do not go to `VisibilityMover`.

### Entry points and delegators

`run_ingestion` calls `run.fold_skips()`, `run.settle_moves()`, `run.visibility_pass()`, `run.modules(...)`
(run.py:1357–1361). `reparse_matches` calls `run.fold_skips()`, `run.settle_moves()`, `run.reparse(...)`
(run.py:1271–1278). To keep both entry points byte-for-byte unchanged, `_Run` keeps `fold_skips` and
`settle_moves` as real methods, and keeps a **thin `visibility_pass` delegator**:
`listings, failed = self._visibility.run(); self.failed_run = self.failed_run or failed; return listings`.
`run_ingestion` and `reparse_matches` are not edited.

### Entry point proper and reparse

`run_ingestion` (run.py:1308) and `reparse_matches` (run.py:1223) keep their signatures and bodies.
`_reparse_targets` and `reparse_matches` stay module-level and still drive `MatchWriter` through `_match_steps`.

## Behavior-preservation guardrail

The existing end-to-end run tests are the behavior oracle: they pass unchanged and are not edited. The plan
names the exact oracle files. No observable changes; the pinned surface is:

- the record shapes `RunRow`, `ModuleRun`, `MatchStep`, `ErrorRow`, `MatchRow`, `MatchFile`, `LineageRow`,
  `MatchSource` (the `ops_match_sources` rows via `_source`/`_listed_again`), and the `Progress` records
  written by `write_progress`;
- the result objects `RunResult`, `RunCounts`, `RunError` and the exit codes (`exit_code`);
- the error `kind` strings and severities;
- the JSON log **`logger` field** (shown and searched in the Logs page) — every moved method keeps logging
  under the name `lakehouse.runs.run`. `_common` defines `_log = get_logger("lakehouse.runs.run")` with the
  explicit name (not `__name__`, which off `_common` would read `lakehouse.runs._common`); `run.py` and the
  collaborators import that `_log`;
- the OpenTelemetry step-span **names, attributes and nesting** (for example `lakehouse.run.move`,
  `lakehouse.run.write`, `lakehouse.run.commit`, and attributes `lakehouse.match_key`, `lakehouse.rows`,
  `lakehouse.bytes`, `lakehouse.catalog_seq`);
- the `ops_*` table schemas and the `ops_copy_fields` DDL.

## Acyclicity enforcement

The acyclic anti-goal is enforced by a new import-linter **layers** contract for `lakehouse.runs` (watched to
fail first, per the guards rule): `run` (highest) above `visibility` / `match_writer` above `match_files`
above `_common` (lowest). A collaborator importing `run.py`, or `match_files` importing a collaborator, breaks
the contract. `lint-imports` then reports 6 contracts kept. The plan writes the exact contract; the impl adds
it and the author watches it fail on a deliberate bad import before wiring the real edges.

## Test and verification plan

- New unit tests, each watched to fail first by a **mutant** (a wrong value or a dropped call), not merely by
  an `ImportError`: `VisibilityMover` (a kept-copy flag flip triggers a move; a failed full listing records a
  warning **and** the run status becomes `failed` — asserting the status, not only the warning row);
  `MatchWriter` (choose kept vs not-kept; `_write` produces identical `MatchRow`/`MatchFile`/`LineageRow`; the
  public→private move precedes the write and private→public follows); `MatchFiles` (the file registry follows
  the new write ids after a move).
- A logging-and-spans test (for example `tests/unit/test_run_observability_unchanged.py`; the plan confirms
  the name) that exercises a moved path — a `_move` and a `_write` — and asserts the JSON `logger` field
  equals `lakehouse.runs.run` and that the `lakehouse.run.move` / `.write` / `.commit` span names and
  attributes are unchanged, watched red on a `get_logger(__name__)` mutant in a collaborator.
- The new import-linter contract watched to fail first on a deliberate bad import.
- Full default-tier pytest green with `LAKEHOUSE_OS_TESTS=1`.
- `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright` (0 errors), `uv run lint-imports`
  (6 kept), `uv run python scripts/pip_audit.py`.
- The UI latency tests are untouched (this cycle does not touch `ui/` or `web/`).

## Approval and commit gate

This is the gold-standard pre-commit program. The owner gives the **final approval** of this spec, of the plan,
and of the implementation; a peer review is not that approval. The work rides the single pre-commit-program
commit together with the UI rework and the other decided audit items (S1, S2, O1). No micro-commits, and the
session never self-commits: it stops at the point of committing, shows the diff and the file list, and waits
for the owner's explicit yes.

## Left to the plan stage

- The exact constructor signatures and build order in `_Run.__init__`.
- The error-sink interface is an `ErrorSink` Protocol satisfied by the bound `_Run._error` (settled — the
  anti-goal names it); the plan writes the Protocol's exact signature.
- The exact contents and name of `_common` (and whether the output dataclasses `RunResult`/`RunCounts`/
  `RunError`, used only by the entry points, stay in `run.py`).
- One new module versus three, under the new import-linter contract.
- The exact oracle-test file names and the mutant for each new unit test.

## Round-1 review responses

- **A1 / B02 (failed_run; A blocking, B should-fix — severity difference, both fix):** fixed. The design now
  states that `visibility_pass` sets `failed_run` directly (run.py:368), that `VisibilityMover.run` returns the
  failed flag, that the `_Run` delegator ORs it into `self.failed_run`, and that the unit test asserts
  `status=failed`, not only the warning row.
- **A2 / B01 (entry points "unchanged"):** fixed. `fold_skips` and `settle_moves` stay real `_Run` methods;
  `visibility_pass` becomes a thin `_Run` delegator; `run_ingestion` and `reparse_matches` are not edited.
- **B03 / B04 (import cycle from `_ModuleContext` and the module-level helpers):** fixed by the `runs/_common.py`
  leaf module; no collaborator imports `run.py`.
- **A3 / B05 (injected deps not derived from usage; `_Run→MatchFiles` edge; `MatchFiles` needs `now`/`run_id`):**
  fixed. Dep lists are now derived per method; `VisibilityMover` drops `run_id`/`now`/step-sink; `MatchWriter`
  drops `archive`; `MatchFiles` lists `run_id`/`now`; the DAG shows the `_Run→MatchFiles` edge.
- **A4 / B06 (logger name):** fixed. `_common._log = get_logger("lakehouse.runs.run")`; the logger field is a
  pinned observable with a test.
- **B06 (other unpinned observables):** fixed. `Progress`, `RunResult`/`RunCounts`/`RunError`,
  `ops_match_sources` (`MatchSource`), and span attributes/nesting are added to the pinned surface.
- **B07 (acyclicity not enforced):** fixed by the new import-linter layers contract (6 kept), watched to fail
  first.
- **B08 (`fold_skips` mis-clustered):** fixed. `fold_skips` stays in `_Run` (lease/run-history housekeeping),
  not in `VisibilityMover`.
- **A5 / B09 (approval/commit gate not in the spec):** fixed by the "Approval and commit gate" section.
- **A6 (`_ops_matches` third caller):** fixed. The seam text names all three callers (`_apply_flags`,
  `_match`, `_count_excluded`).
- **A7 / B10 (commit wording):** fixed. `_write` commits **after** `swap_match_files` (run.py:988); `_move`
  commits at run.py:411 and inside `move_match_files` (moves.py:310); the constraint now also names
  `fold_skips` (run.py:317) and `_record_not_kept` (run.py:865).
- **B11 (oracle tests not named; red-green needs a mutant):** partly fixed now (the test plan requires a
  mutant, not an `ImportError`); the exact oracle-test file names are named in the plan.

## Round-2 review responses

- **A — APPROVED round 2.** All eight round-1 findings verified resolved in the code; `_common` provably
  complete (the missed set is empty); the import-linter layers contract sound.
- **A-09 (CONSIDER — `_log` described as both injected and imported):** fixed. `_log` is a module global
  imported from `_common`; it is removed from every injected-dependency list and is not a constructor
  parameter.
- **B-06 (should-fix — logger/span test):** fixed. The test plan now names a logging-and-spans test with a
  `get_logger(__name__)` mutant as the red.
- **B-12 (cluster count and overstated agreement):** fixed. The intro says six clusters; the status line no
  longer claims both reviewers confirmed the partition and DAG in round 1.
- **B-13 (`VisibilityMover` `storage`; `_files_of` callers):** fixed. `storage` is dropped from
  `VisibilityMover`; the seam text now says `_files_of` is called only by `_move` and `_write`.
- **B-14 (error sink versus the bound-method anti-goal):** fixed. The error sink is stated as the one
  sanctioned injected callback (an `ErrorSink` Protocol over `_Run._error`), and `MatchWriter` reads only the
  identity fields of `_ModuleContext`.
