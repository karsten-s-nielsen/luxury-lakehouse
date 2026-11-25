# v2 Tracking-Compute Performance Implementation Plan

> **For agentic workers:** execute this plan with superpowers:executing-plans, inline, in one session. No subagents
> (owner standing rule). No git worktree; work on branch `feat/v2-calc-tracking-metrics`. Steps use checkbox
> (`- [ ]`) syntax for tracking. **No step commits.** This work rides in the calc-layer cycle's single commit, which
> needs the owner's explicit approval after Task 12 (one commit per cycle).

**Status:** A/B-approved round 2 and owner-approved 2026-11-06; edited after approval with the round-2 CONSIDERs and the "Amendment" (no-duration sequencing of Tasks 9 and 11), A/B-approved (no-duration plan r3) and owner-approved 2026-11-06. Re-amended 2026-11-06 for the owner's clean-slate rebuild (Task 9 Steps 2-4, Task 11 Steps 1-2, the Amendment); A/B-approved with the no-duration amendment (round 8) and owner-approved 2026-11-06. **Amended 2026-11-23** (owner ruling, calc-layer spec D10; see the last section "Amendment 2026-11-23").

**Goal:** Make the v2 tracking computes (pitch_control, shape_graphs, line_breaking) complete the 980-match lake on
the HF `cpu-basic` job, resumably across nights, with no loss of output fidelity. (Amendment 2026-11-23: in this cycle
only line_breaking and the SPADL steps run by default; pitch_control and shape_graphs are excluded until the
silly-kicks batch upgrade, calc-layer spec D10.)

**Architecture:** Add numba (N). Build each match frame once for pitch_control + shape_graphs (S). Replace the
frame-content skip key with an exact in-DuckDB input hash plus a derived code digest (K). Give the compute a deadline,
checkpointed progress, and stop handling (R). Batch pitch-control TTI only if the profile shows it pays (C). Record the
decision in v2 ADR-017.

**Tech Stack:** Python 3.12, DuckDB 2.0.0 / DuckLake, pandas, pyarrow, silly-kicks 4.139.1 (read-only), numba (via the
silly-kicks `numba` extra), pydantic config, pytest + pytest-benchmark 5.3.1 (5.3.0 until the pytest 9.2 fix, 2026-11-23), HF Jobs.

**Spec:** `v2/docs/specs/2026-11-05-v2-tracking-compute-performance-design.md` (A/B-approved round 5, owner-approved
2026-11-06). Read it with this plan. Section refs (§) below are the spec's.

## Global Constraints

- Python `>=3.12,<3.13`. pyright **strict** (`v2/pyproject.toml [tool.pyright]`, include `src`, `tests`, `scripts`).
  pandas/silly-kicks boundary modules use the file-level directive
  `# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false`.
- Local gate, run from `v2/`, all five must pass before a task is done:
  `uv run ruff check .` · `uv run ruff format --check .` · `uv run lint-imports` · `uv run pyright` ·
  `uv run pytest -m "not live and not hf and not bench"` (the task's own tests first, the full tier at Tasks 10 and 12).
- silly-kicks stays `4.139.1` and is never edited. Private silly-kicks imports are pinned to 4.139.1 in a docstring.
- **No fidelity loss** (§0.5): every frame is scored; no downsampling anywhere.
- **One heavy match at a time** (ingestion spec §10.9, C7): no process pool, no thread pool for the computes.
- Parity comparator (§4): per-match arrow rows minus `_ingested_at`, `_run_id`, `_source_hash`, `match_key`, sorted by
  the table's key columns, compared value for value.
- Every outward action (scp to the Mac with a run, an HF deploy, an HF job, an HF config write) needs the owner's
  explicit yes at that step. Never touch the production scheduled job's definition (`6ae602100e0338d1bb7d2ed8`).
- **Production gets reviewed code only.** The A/B implementation review (Task 10) passes before any HF deploy
  (Task 11). A code fix after the HF deploy changes the code digest and re-computes every match, so it needs its own
  re-review and the owner's yes.
- **The compute deadline applies to every compute step**, the SPADL steps included (they gain durable progress and the
  soft deadline). Their skip-guard is unchanged (spec §11). (Superseded 2026-11-07: the owner gave the SPADL steps the exact input key, `SPADL_CODE_MODULES` and `SPADL_SKIP_SQL`; calc-layer spec amendment.)
- **Output logic lives only in digested modules.** A function that turns read rows into output values (for example
  the home/away team choice) lives in `frames.py` or a scorer module, never in a step driver (Task 5).
- Reviews write to `D:\Development\_reviews\`, never into the repo. Prose follows ASD-STE100.

## Review Focus

1. **A saved config with no `compute` block** (the live HF lake config is version 2, pre-dates this work): it must
   load with the safe `ComputeConfig()` defaults, not fail and not run unbounded. Test in Task 4.
2. **NULL, NaN, and -0.0 in an input read**: the K hash must be stable across two runs on identical data and must
   change when a NULL becomes a value. Test in Task 3.
3. **A recompute that is abandoned at the hard deadline**: the match's previous rows stay untouched and its stale key
   stays, so the next run recomputes it. Test in Task 6.
4. **A stop that arrives during a dbt build, not the compute**: the remaining phases and cleanup are skipped and the
   run-end commit still runs. Test in Task 6.
5. **numba JIT warm-up on the first match of a run**: the first call compiles; the per-match timing log must show it,
   and `worst_match_seconds` calibration (Task 11) must include a cold first match. Test: Task 2 asserts the backend
   log line; Task 11 records cold-first-match seconds.

---

### Task 1: Profiling harness and the pre-change Mac baseline (§6 steps 1, 5)

**Files:**
- Create: `v2/scripts/profile_tracking_compute.py`
- Test: `v2/tests/unit/test_profile_tracking_compute.py`

**Interfaces:**
- Produces: `summarise_profile(stats: pstats.Stats, phases: Mapping[str, tuple[str, ...]]) -> dict[str, float]` (seconds
  per phase, by summing cumulative time of the named functions) and a CLI
  `python scripts/profile_tracking_compute.py --match-key <int> --provider <p> --step <s> --out <json>` that opens the
  PUBLISHED catalog read-only (`resolve_roots` + `open_storage` + `CatalogReader(...).session()`), times one match for
  the chosen step under `cProfile`, and writes JSON `{provider, match_key, step, frames, backend, phases: {...},
  total_s}`. `--step` is one of `pitch_control`, `shape_graphs`, `line_breaking` (default: `pitch_control` and
  `shape_graphs`, one JSON each). Task 1 uses the two per-frame steps; Task 9 Step 6 uses `--step line_breaking`.
- Phase map (fixed): `read_convert` = `build_match_frame`; `source_hash` = `_frame_source_hash`;
  `velocity` = `smooth_frames` + `derive_velocities`; `tti` = `compute_tti`; `influence` = `_compute_influence`;
  `combine` = `_spearman_combine` (net of its `influence` calls); `extract` = `_extract_frame_players`;
  `ball` = `_resolve_ball_position`; `shape` = `compute_shape_graph` +
  `infer_positions`; `line_breaking` = `detect_line_breaking`; `skip_key` = today's `_frame_source_hash` +
  `_already_computed` (after Task 3: `input_key`). The profile is read-only, so `write` (write + swap) comes from the
  per-match `"compute match"` log of real runs (Task 6 adds it; Tasks 9 and 11 read it).
- `line_breaking` needs a populated `fct_actions`. The compute-less republish (Step 5) leaves it empty, so the
  line_breaking profile runs in Task 9 Step 6, after the Mac backfill publishes `fct_actions`.

- [ ] **Step 1: Write the failing test** `test_summarise_profile_sums_named_functions`: profile a tiny function pair
  (`def a(): b()`, `def b(): time.sleep(0.01)`), call `summarise_profile(stats, {"x": ("b",)})`, assert
  `result["x"] >= 0.01` and `set(result) == {"x"}`.
- [ ] **Step 2: Run it, expect FAIL** (`ModuleNotFoundError`): `uv run pytest tests/unit/test_profile_tracking_compute.py -v`
- [ ] **Step 3: Implement** `summarise_profile` + the CLI in `scripts/profile_tracking_compute.py` (stdlib `cProfile`,
  `pstats`; the script imports the scorers and `build_match_frame` directly).
- [ ] **Step 4: Run it, expect PASS.** Run the local gate on the two files.
- [ ] **Step 5: Re-publish the epoch-fixed views on the Mac** (owner yes first). Use the owner-sanctioned
  compute-less reparse of 2026-11-05 (ledger): scp the repo, back up `src/lakehouse/cli.py` to `cli.py.diagbak`, set
  the reparse `compute=()`, run `uv run lakehouse reparse --all` (background, poll every 30 s), then
  `mv src/lakehouse/cli.py.diagbak src/lakehouse/cli.py` and confirm `sha256sum` equals the repo copy.
  Expected: the run ends `ok`; `SELECT typeof(time_seconds) FROM lake.main.fct_tracking_ball LIMIT 1` gives `DOUBLE`.
- [ ] **Step 6: Profile on the Mac** (numpy backend): the largest match per provider (skillcorner, gradientsports,
  idsse by frame count from `fct_tracking_frames`). Save the three JSONs to
  `D:\Development\_v2tmp\perf\baseline-numpy-<provider>.json`. Record the phase split in the ledger.

### Task 2: Lever N — numba backend (§7.1)

**Files:**
- Modify: `v2/pyproject.toml:27` → `"silly-kicks[parse-dfl,numba]==4.139.1"`; `v2/uv.lock` (regenerated)
- Create: `v2/src/lakehouse/compute/backend.py`
- Test: `v2/tests/unit/test_compute_backend.py`; `v2/tests/bench/test_bench_pitch_control_backend.py`

**Interfaces:**
- Produces: `numba_active() -> bool` (the one place that reads the private
  `silly_kicks.tracking.pitch_control._spearman._HAS_NUMBA`; pin recorded in the module docstring) and
  `BACKEND_TOLERANCE: Final[float] = 1e-9`.

- [ ] **Step 1: Write the failing test** `test_numba_backend_is_active`: `assert numba_active() is True`.
- [ ] **Step 2: Run it, expect FAIL** (module missing, then `False` once the module exists without the dependency).
- [ ] **Step 3: Implement** `backend.py`; change `pyproject.toml:27`; run `uv lock` then `uv sync`.
- [ ] **Step 4: Run it, expect PASS.** Also confirm `uv.lock` now has `name = "numba"` and `name = "llvmlite"`.
- [ ] **Step 5: Write** `test_numba_matches_numpy_within_tolerance` (unit, on the `test_frame_builder` fixture frame):
  compute `pitch_control_values` once as is, once with `monkeypatch.setattr(_spearman, "_HAS_NUMBA", False)`; assert
  `max(abs(a - b)) <= BACKEND_TOLERANCE` over `control_fraction`, and the key columns are identical.
- [ ] **Step 6: Write the bench variant** `test_bench_backend_tolerance_real_match` (`bench` + `heavy` markers; reads
  the published Mac catalog, largest gradientsports match); records `max_abs_diff` in `benchmark.extra_info` and
  asserts it `<= BACKEND_TOLERANCE`. Run it on the Mac in Task 9.
- [ ] **Step 7: Re-profile on the Mac with numba** (Task 1 CLI), save `baseline-numba-<provider>.json`. Record in the
  ledger: per-provider per-frame ms under numba, and the TTI share of pitch_control time. This decides Task 7.

### Task 3: Lever K core — the exact input key (§7.4)

**Files:**
- Create: `v2/src/lakehouse/compute/input_key.py`
- Modify: `v2/src/lakehouse/compute/frames.py:57-70` (declare the four reads; `build_match_frame` uses them)
- Test: `v2/tests/unit/test_input_key.py`

**Interfaces:**
- Produces in `frames.py` (so `frames.py` gains **no** runtime `lakehouse` import — the code-digest closure stays
  small):
  - `@dataclass(frozen=True) class InputRead: name: str; sql: str; with_provider: bool` and
    `def params(self, match_key: int, provider: str) -> list[object]`.
  - `FRAME_INPUT_READS: Final[tuple[InputRead, ...]]` — exactly the four reads of `frames.py:58-70`, same SQL text.
- Produces in `input_key.py` (imports `frames`; it is **not** in the code list — a change to the key logic changes
  every stored key, so it re-computes conservatively by itself):
  - `ACTIONS_INPUT_READ: Final[InputRead]` — `SELECT * FROM lake.main.fct_actions WHERE canonical_match_key = ?`.
  - `HOME_TEAM_READ: Final[InputRead]` — `SELECT home_team_key FROM lake.main.fct_match_params WHERE
    canonical_match_key = ?`. It decides the home/away split, so it is an **input** read of pitch_control.
  - `CONTROL_READS: Final[frozenset[str]]` — the **exact** SQL texts of the control reads (match discovery per step,
    the `duckdb_tables()` presence check, `_match_visibility`, the skip lookup). Matching is by exact text, not prefix.
  - `read_content_hash(conn: duckdb.DuckDBPyConnection, read: InputRead, match_key: int, provider: str) -> str`.
  - `code_digest(modules: Sequence[str]) -> str` and `library_versions(dists: Sequence[str]) -> str`.
  - `TRACKING_CODE_MODULES: Final[tuple[str, ...]]` = (`lakehouse.compute.frames`, `lakehouse.compute.pitch_control`,
    `lakehouse.compute.shape_graphs`, `lakehouse.compute.line_breaking`, `lakehouse.compute.control`,
    `lakehouse.compute.tracking_steps`). The step driver module is digested too, so any driver edit re-computes
    (conservative). `tracking_steps` is added to the tuple in Task 5, when the module exists.
  - `INFRA_MODULES: Final[frozenset[str]]` — the modules the driver may import that read, write, or log but never
    shape output values: `lakehouse.compute.base`, `lakehouse.compute.input_key`, `lakehouse.compute.backend`, and the
    `lakehouse.bronze.*`, `lakehouse.catalog.*`, `lakehouse.logging`, `lakehouse.hashing`, `lakehouse.runs._common`,
    `lakehouse.config.model` packages. Each entry has a one-line reason comment. Infra changes do not re-compute.
  - `TRACKING_LIBRARIES: Final[tuple[str, ...]]` = (`silly-kicks`, `numba`, `llvmlite`, `numpy`, `scipy`, `pandas`,
    `pyarrow`, `duckdb`).
  - `input_key(conn, *, match_key: int, provider: str, reads: Sequence[InputRead], code_modules: Sequence[str]) -> str`
    = SHA-256 over the read hashes (in declaration order), `code_digest(code_modules)`,
    `library_versions(TRACKING_LIBRARIES)`, and `numba_active()`.
- **Hash algorithm (fixed):** for a read `R`, run
  `SELECT count(*), sum(md5_number_upper(to_json(s))::HUGEINT), sum(md5_number_lower(to_json(s))::HUGEINT)
  FROM (SELECT r AS s FROM (<R>) r)`; the read hash is SHA-256 of `"{count}:{sum_upper}:{sum_lower}"`. Each row's
  identity is its full 128-bit MD5 over a canonical JSON text, so the two sums are independent.
  (DuckDB's `hash(x, k)` is **not** usable as a second hash: it combines `hash(x)` with `hash(k)`, so
  `hash(0, 7919) == hash(7919)` — both reviewers and the author probed this on 2.0.0.) Author probe on DuckDB 2.0.0:
  `to_json` quotes and escapes strings, keeps NaN distinct, prints floats round-trip (`0.30000000000000004` vs
  `0.3`), and writes `-0.0` as `0.0` (numerically equal values); 3.2M rows hash in 0.34 s in memory. MD5 is used here
  as a content fingerprint, not for security.
- **Code digest (fixed):** per module, `ast.parse(inspect.getsource(module))`, remove every module/class/function
  docstring node, `ast.dump(tree, include_attributes=False)`; SHA-256 over the module names and dumps in list order.

- [ ] **Step 1: Write the failing tests** in `test_input_key.py` (in-memory DuckDB tables standing in for the views):
  - `test_unchanged_reads_give_the_same_key` (two calls, equal).
  - `test_a_changed_value_in_any_column_changes_the_key` (parametrised over every column of a 4-column fixture,
    including a `team_key` column).
  - `test_a_duplicated_row_changes_the_key`.
  - `test_null_to_value_changes_the_key_and_null_is_stable` (Review Focus 2; also NaN and `-0.0` stable across calls).
  - `test_row_digest_halves_are_independent`: for 1,000 generated rows, `sum_upper` and `sum_lower` are not functions
    of each other (a single-row change moves both), and a string containing `", 'd': 1"` does not collide with the
    split row it imitates.
  - `test_float_change_in_the_last_bit_changes_the_key` (`0.1 + 0.2` vs `0.3`).
  - `test_lake_table_present_branch_flip_changes_the_key` (spec §7.4): build the real dbt views on a seeded lake
    (`run_dbt`, as `test_dbt_materialisation.py` does) without a provider's tracking table, take the key, add the
    table, rebuild, and assert the key changed.
  - `test_docstring_only_change_keeps_the_code_digest` / `test_code_change_changes_the_code_digest` (digest of a temp
    module written twice, via `importlib`).
  - `test_library_version_change_changes_the_key` (monkeypatch `importlib.metadata.version`).
  - `test_build_match_frame_reads_only_declared_sql`: run `build_match_frame` against a recording connection wrapper
    (fake results as in `test_frame_builder.py`); every executed SQL is exactly one `FRAME_INPUT_READS` SQL. (The
    whole-step read guard runs in Task 5, once the steps exist.)
  - `test_code_modules_cover_the_import_closure`: for each `TRACKING_CODE_MODULES` entry, walk its runtime
    `import`/`from` nodes (skip `if TYPE_CHECKING:` blocks), transitively; every `lakehouse.*` target is in
    `TRACKING_CODE_MODULES` or matches `INFRA_MODULES`.
  - `test_libraries_cover_third_party_imports`: every third-party top-level import of those modules maps through
    `importlib.metadata.packages_distributions()` to a name in `TRACKING_LIBRARIES` (both sides normalised per
    PEP 503, so `silly_kicks` matches `silly-kicks`).
- [ ] **Step 2: Run, expect FAIL** (module missing).
- [ ] **Step 3: Implement** `input_key.py`; switch `build_match_frame` to `FRAME_INPUT_READS` (no SQL change).
- [ ] **Step 4: Run** `test_input_key.py` + `test_frame_builder.py` + `test_tracking_frames_view.py`, expect PASS.
  The import-closure test will fail until Task 5 moves the step classes out of the scorer modules: mark it
  `xfail(strict=True, reason="Task 5 splits steps from scorers")` now; Task 5 removes the marker.

### Task 4: Lever R core — config, control, and the scorer check point (§7.5 steps 2, 3)

**Files:**
- Create: `v2/src/lakehouse/compute/control.py`
- Modify: `v2/src/lakehouse/config/model.py` (new `ComputeConfig`; `Settings.compute`)
- Modify: `v2/src/lakehouse/compute/pitch_control.py:59-97`, `v2/src/lakehouse/compute/shape_graphs.py:61-105`
- Test: `v2/tests/unit/test_compute_control.py`, `v2/tests/unit/test_config_model.py`,
  `v2/tests/unit/test_pitch_control_compute.py`, `v2/tests/unit/test_shape_graphs_compute.py`

**Interfaces:**
- Produces in `config/model.py`:
  `class ComputeConfig(_Frozen)` with `budget_seconds: int = Field(default=18000, ge=60)`,
  `startup_allowance_seconds: int = Field(default=600, ge=0)`, `finish_reserve_seconds: int = Field(default=2700, ge=0)`,
  `layer2_reserve_seconds: int = Field(default=1800, ge=0)`, `worst_match_seconds: int = Field(default=1200, ge=0)`,
  `check_every_frames: int = Field(default=500, ge=1)`, `chunk_frames: int = Field(default=2000, ge=1)` (removed in Task 7: it existed only for lever C, which was not built; perf ledger ruling), and a validator
  `budget_seconds > finish_reserve + layer2_reserve + worst_match`. `Settings.compute: ComputeConfig =
  Field(default_factory=ComputeConfig)`. `budget_seconds` starts at 18,000 s (not 21,000), so the first,
  uncalibrated HF run keeps ~50 min of slack inside the 21,600 s timeout. Task 11 replaces the values with
  HF-measured ones.
- **Seed the reserves from evidence (Step 0 below):** the existing production scheduled runs on HF already log their
  phase timings (`step_span` lines for resolve, dbt, merge, cleanup, commit). Read the latest successful run's log
  read-only (`hf jobs logs`), and set `finish_reserve_seconds` to at least 2× its dbt + merge + cleanup + commit time.
  If the values above are smaller, raise them; record the source run id in the ledger.
- **Rollback order** (`Settings` is `extra="forbid"`): once a lake config holds a `compute.*` key, code older than this
  plan cannot load that config. So no `compute.*` key is written to the HF lake config before calibration (Task 11
  Step 3), and a rollback first restores the config without those keys, then redeploys the older image. `deploy.md`
  states this order (Task 8).
- Produces in `compute/control.py` (in `TRACKING_CODE_MODULES`, so it has **no** runtime `lakehouse` import:
  `ComputeConfig` is imported under `if TYPE_CHECKING:` only):
  - `class ComputeAbandoned(Exception)` — raised by a scorer when `should_stop()` is `True`.
  - `@dataclass(frozen=True) class ComputeControl` with `soft_deadline: datetime | None`,
    `hard_deadline: datetime | None`, `now: Callable[[], datetime]`, `stop: Callable[[], bool]`,
    `on_written: Callable[[], None]`, `check_every: int`; methods `may_start_match() -> bool` (not stopped and before
    the soft deadline) and `should_stop() -> bool` (stopped or past the hard deadline).
  - `ComputeControl.for_layer(config: ComputeConfig, *, started_at: datetime, layer: int, now, stop, on_written) ->
    ComputeControl`: hard = `started_at + budget − finish_reserve − (layer2_reserve if layer == 1 else 0)`;
    soft = hard − `worst_match`.
  - `UNBOUNDED: ComputeControl` (no deadlines, never stops, no-op `on_written`) for unit tests and library callers.
- Scorer change: `pitch_control_values(frame, *, home_team_id, away_team_id, params=None, should_stop: Callable[[],
  bool] | None = None, check_every: int = 500)` and `shape_graph_values(frame, *, should_stop=None, check_every=500)`.
  Inside the existing per-group loop (`pitch_control.py:80`, `shape_graphs.py:79`), after velocity derivation, call
  `should_stop()` every `check_every` groups; raise `ComputeAbandoned` when it returns `True`.

- [ ] **Step 0: Seed the reserves** from the latest successful HF scheduled run's log (read-only; see above).
- [ ] **Step 1: Write the failing tests:**
  - `test_saved_config_without_compute_block_gets_safe_defaults` (Review Focus 1): `Settings.model_validate` on a
    saved dict with no `compute` key gives `compute.budget_seconds == 18000`.
  - `test_compute_budget_fits_the_deployed_timeout`: parse `--timeout (\d+)` from the "Scheduled job:" line of
    `v2/docs/ops/deploy.md` and from `v2/architecture.dsl:70`; assert both equal and
    `ComputeConfig().budget_seconds + ComputeConfig().startup_allowance_seconds <= timeout`.
  - `test_compute_config_rejects_a_budget_without_room` (validator).
  - `test_for_layer_deadlines` (layer 1 and 2 arithmetic against a fixed `started_at`).
  - `test_may_start_match_and_should_stop` (fake clock before/after soft and hard, and `stop=lambda: True`).
  - `test_pitch_control_abandons_when_should_stop_turns_true` and the same for shape_graphs: a `should_stop` that
    returns `True` on its second call raises `ComputeAbandoned`.
  - `test_pitch_control_output_unchanged_with_should_stop_none` and the same for shape_graphs (equal to a call
    without the keyword, §4 comparator).
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** `ComputeConfig`, `control.py`, and the two scorer keywords.
- [ ] **Step 4: Run** the four test files, expect PASS. Run the local gate on the touched files.

### Task 5: Lever S — one frame build per match, with the exact key (§7.3, §7.4)

**Files:**
- Create: `v2/src/lakehouse/compute/tracking_steps.py` (the step classes move here; the scorer modules keep only pure
  code)
- Modify: `v2/src/lakehouse/compute/pitch_control.py` (remove `_PitchControlStep`, `PITCH_CONTROL`,
  `_frame_source_hash`, `_already_computed`; turn `_home_and_away` into the pure `home_and_away` below and keep it
  here), `shape_graphs.py` (remove `_ShapeGraphsStep`, `SHAPE_GRAPHS`, `_frame_source_hash`, `_already_computed`),
  `line_breaking.py` (remove `_LineBreakingStep`, `LINE_BREAKING`; `_actions_for_scorer` stays here)
- Modify: `v2/src/lakehouse/compute/input_key.py` (append `lakehouse.compute.tracking_steps` to
  `TRACKING_CODE_MODULES`), `v2/src/lakehouse/compute/base.py` (`ComputeStepGroup`; `run_compute` runs a group),
  `v2/src/lakehouse/compute/registry.py`
- Test: `v2/tests/unit/test_tracking_steps.py`, `v2/tests/e2e/test_compute_local.py`,
  `v2/tests/unit/test_compute_seam.py`, `v2/tests/unit/test_input_key.py`

**Interfaces:**
- Consumes: `FRAME_INPUT_READS` (Task 3, `frames.py`); `ACTIONS_INPUT_READ`, `HOME_TEAM_READ`, `CONTROL_READS`,
  `input_key`, `TRACKING_CODE_MODULES`, `INFRA_MODULES` (Task 3); `ComputeControl`, `ComputeAbandoned`, `UNBOUNDED`
  (Task 4).
- Produces in `pitch_control.py`: `home_and_away(frame: pd.DataFrame, home_team_key: int) -> tuple[int, int]` — pure;
  the same rule as today's `_home_and_away` (`pitch_control.py:107-117`: away = the frame's other non-ball team, or
  home when there is none). The driver only reads `home_team_key` (`HOME_TEAM_READ`, an input read) and calls it.
- Produces in `base.py`: `class ComputeStepGroup(Protocol)` with read-only `names: tuple[str, ...]`, `layer: int`,
  `input_select: str`, and `__call__(cat, *, run_id, caps, now, error, control: ComputeControl) ->
  tuple[ComputeOutcome, ...]`. `run_compute(..., steps: Sequence[ComputeStep | ComputeStepGroup], control_for:
  Callable[[int], ComputeControl] = lambda _layer: UNBOUNDED)`; a group's outcomes are appended in `names` order, each
  with its own `"compute step"` log line (`base.py:266-274` fields unchanged). A whole-group exception records one
  `compute_failed` per name (`f"{name}: {type}: {msg}"`). `FATAL` propagates out of a group unchanged.
- Produces in `tracking_steps.py`:
  - `TRACKING_FRAME_METRICS: ComputeStepGroup` — `names=("pitch_control", "shape_graphs")`, `layer=1`, output tables
    `pitch_control_values` / `shape_graph_labels` (unchanged). Per match: compute each read's content hash once;
    pitch_control's key = `input_key(..., reads=FRAME_INPUT_READS + (HOME_TEAM_READ,), code_modules=<frames,
    pitch_control, control, tracking_steps>)`; shape_graphs' key = `input_key(..., reads=FRAME_INPUT_READS,
    code_modules=<frames, shape_graphs, control, tracking_steps>)`. Look up each table's skip state; if both match →
    both `DONE`, **no frame build**; else build the frame once, run each stale scorer in its own `try` (a
    `compute_failed` names that scorer only), write and swap each table on its own.
  - `LINE_BREAKING: ComputeStep` — `name="line_breaking"`, `layer=2`; its `fct_actions` read **is**
    `ACTIONS_INPUT_READ` (no second SQL text); key = `input_key(..., reads=FRAME_INPUT_READS + (ACTIONS_INPUT_READ,),
    code_modules=<frames, line_breaking, control, tracking_steps>)`; skip before any build.
- Registry: `DEFAULT_COMPUTE_STEPS` = the five SPADL steps, `TRACKING_FRAME_METRICS`, `LINE_BREAKING` (type
  `tuple[ComputeStep | ComputeStepGroup, ...]`). (Amendment 2026-11-23: `TRACKING_FRAME_METRICS` is now in
  `EXCLUDED_COMPUTE_STEPS` and out of the default run; Task 8b.)

- [ ] **Step 1: Write the failing tests:**
  - `test_group_reports_one_outcome_per_scorer_name` (unit, fake scorers via injection): outcomes named
    `pitch_control`, `shape_graphs`.
  - `test_unchanged_match_builds_no_frame` (count `build_match_frame` calls with a patched builder: 0 on the second
    run).
  - `test_only_the_stale_scorer_runs` (pitch key matches, shape key does not → one build, only shape scores).
  - `test_one_scorer_failing_leaves_the_other_written`: shape raises → pitch rows written, exactly one
    `compute_failed` with message prefix `"shape_graphs: match_key="`, shape's previous rows unchanged.
  - `test_fatal_from_one_scorer_propagates_out_of_the_group` (B CONSIDER 11).
  - `test_home_away_logic_change_recomputes` (A PERF-PLAN-01): a changed `home_and_away` source (temp edit through
    `code_digest` on a patched module source) changes pitch_control's key; a changed `home_team_key` value changes it
    too.
  - `test_driver_edit_changes_every_tracking_key`: `tracking_steps` is in each key's code list.
  - `test_line_breaking_recomputes_on_a_frame_change` (B r3 HFTC-SPEC-21).
  - `test_steps_issue_only_declared_reads` (e2e, seeded `kit`; B HFTC-PLAN-03): run `TRACKING_FRAME_METRICS` and
    `LINE_BREAKING` through a recording connection; every executed SQL text is exactly one declared input read or one
    `CONTROL_READS` entry.
  - `test_group_output_equals_the_pure_scorers` (e2e, seeded `kit`): each table's rows equal `pitch_control_values` /
    `shape_graph_values` called directly on `build_match_frame` for every seeded match (§4 comparator). The
    production-scale run of this oracle is Task 9 Step 7.
  - Remove the Task 3 `xfail` marker from `test_code_modules_cover_the_import_closure`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** `tracking_steps.py`, `home_and_away`, the `base.py` group support, the registry change;
  delete the moved code from the scorer modules. `cli.py` imports only `DEFAULT_COMPUTE_STEPS` (`cli.py:42`), so it
  needs no change.
- [ ] **Step 4: Run** `test_tracking_steps.py`, `test_input_key.py`, `test_compute_seam.py`, `test_compute_local.py`,
  `test_workflow_cards.py`, `test_cli.py`, expect PASS. Card ids `pitch_control` / `shape_graphs` stay unchanged.

### Task 6: Lever R wiring — deadlines, durable progress, deferred matches, stop (§7.5 steps 1, 2, 4, 5)

**Files:**
- Modify: `v2/src/lakehouse/compute/base.py:68-77` (`ComputeOutcome.deferred_matches: int = 0`), `:154-201`
  (`run_per_match_bronze(..., control: ComputeControl = UNBOUNDED)`), `:236-264` (`run_compute`)
- Modify: `v2/src/lakehouse/compute/spadl_actions.py` (the five SPADL steps accept and pass `control`)
- Modify: `v2/src/lakehouse/compute/tracking_steps.py` (pass `control.should_stop` + `check_every` to the scorers)
- Modify: `v2/src/lakehouse/runs/run.py:669-760` (`finish`)
- Test: `v2/tests/unit/test_compute_write_loop.py`, `v2/tests/unit/test_compute_seam.py`,
  `v2/tests/unit/test_run_compute_stop.py` (new)

**Interfaces:**
- Consumes: `ComputeControl.for_layer`, `ComputeConfig` (Task 4); `ComputeStepGroup` (Task 5).
- `ComputeStep.__call__` gains keyword `control: ComputeControl`.
- `run_per_match_bronze` loop, per match: if `not control.may_start_match()` → stop the loop and count every match not
  yet reached as **not reached**; else run `produce`; `ComputeAbandoned` → counted as not reached (no write, no
  `compute_failed`); after a write → `control.on_written()`; log `"compute match"` with
  `{step, match_key, seconds, rows, outcome}`. `ComputeOutcome.deferred_matches` = the not-reached count. Matches not
  reached were not skip-checked, so some may already be current; the record says so: if `deferred_matches > 0`,
  record one `error(kind="compute_deferred", severity="warning", native=None,
  message=f"{step}: {n} matches not reached this run (skip state unchecked); the next run continues")`. (A CONSIDER
  09: a skip check past the soft deadline would spend reserve time, so the count is "not reached", stated plainly.)
- `finish()`: build `control_for(layer)` from `self.settings.compute`, `self.started_at`, `self.now`, `self.stop`,
  `on_written=lambda: self.checkpoints.maybe("compute")`; log `"compute backend"` with `{numba: numba_active()}` once
  before layer 1. After each phase (each dbt build, each compute layer, maintenance) call
  `_halt_if_stopped() -> bool`: if `self.stop()` and not yet terminated → `self.stopped()`; when `True`, skip every
  later phase **including `run_cleanup`** and go to `write_run` + `run_end` (today's terminated path, `run.py:671`,
  `:751-760`).

- [ ] **Step 1: Write the failing tests:**
  - `test_a_write_calls_on_written` (`checkpoints.maybe("compute")` reached once per written match).
  - `test_past_soft_deadline_starts_no_match_and_counts_not_reached` (fake clock; `deferred_matches == remaining`; one
    `compute_deferred` warning with the "skip state unchecked" text; the run is not failed by it).
  - `test_abandoned_match_writes_nothing_and_keeps_previous_rows` (Review Focus 3): a match with existing rows and a
    stale key, scorer raises `ComputeAbandoned` → rows and `_source_hash` unchanged; the next run recomputes it.
  - `test_spadl_step_honours_the_soft_deadline` (a SPADL step stops starting matches past the soft deadline).
  - `test_stop_during_compute_skips_later_phases_and_cleanup` (`test_run_compute_stop.py`): stop flag set inside
    layer 1 → no layer-2 call, no further dbt build, `run_cleanup` not called, `run_end` called, the completed
    matches are in the run-end publish, status `failed` with reason `terminated`.
  - `test_stop_during_dbt_build_skips_the_rest` (Review Focus 4): stop set during the middle dbt build → layer 2,
    final build, maintenance, and cleanup skipped; `run_end` called.
  - `test_compute_backend_logged_once` (Review Focus 5).
  - Each existing fake `ComputeStep` in `test_compute_seam.py` / `test_compute_write_loop.py` accepts `control`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** the loop, outcome, record, `finish()` changes, and the SPADL step pass-through.
- [ ] **Step 4: Run** the three test files plus `test_run_maintenance.py`, `test_run_lease_check.py`,
  `test_run_dbt_key.py`, `test_run_steps.py`, `test_run_checkpoints.py`, `test_crash_recovery.py`, expect PASS.

### Task 7: Lever C — batched TTI, only if the profile supports it (§7.2)

**Decision gate (first step):** build C only if Task 2 Step 7 shows `tti` ≥ **20 %** of pitch_control's per-match time
under numba on the gradientsports or skillcorner baseline. Otherwise record the measured share in the ledger and in
ADR-017 (Task 8) as the reason, and skip Steps 2-6.

**Files:**
- Modify: `v2/src/lakehouse/compute/pitch_control.py` (the scoring loop)
- Test: `v2/tests/unit/test_pitch_control_compute.py`; `v2/tests/bench/test_bench_tracking_compute.py` (Task 9)

**Interfaces:**
- `pitch_control_values(..., chunk_frames: int | None = None)`: `None` keeps the per-frame `compute_pitch_control`
  loop (the oracle). An int sorts the velocity frame by `(period_id, frame_id)` once, finds contiguous chunk bounds
  with `numpy.searchsorted` over the frame keys, and calls `compute_pitch_control_batch(chunk_rows, requests)` with
  **only that chunk's rows**; `should_stop()` is called between chunks.

- [ ] **Step 1: Decide** per the gate; write the decision and numbers in the ledger.
- [ ] **Step 2: Write the failing tests** `test_batched_equals_per_frame_loop` (numba; §4 comparator; chunk sizes 1,
  7, and larger than the match) and `test_batched_abandons_between_chunks`.
- [ ] **Step 3: Run, expect FAIL.**
- [ ] **Step 4: Implement**; `tracking_steps.py` passes `settings.compute.chunk_frames`.
- [ ] **Step 5: Size `chunk_frames` against memory** (B CONSIDER 08, spec §7.2): on the Mac, the largest idsse match,
  chunk sizes 500 / 1,000 / 2,000 / 4,000; record peak RSS (child-process sampling, as `benchkit.measure` does) and
  time; pick the largest size whose peak RSS plus the DuckDB `memory_limit` stays ≤ 12 GB (75 % of `cpu-basic`'s
  16 GB). Set the `chunk_frames` default to it.
- [ ] **Step 6: Run, expect PASS**; re-profile one gradientsports match on the Mac and record the gain.

### Task 8: ADR-017 and the operator docs (§9)

**Files:**
- Create: `v2/docs/adrs/ADR-017-tracking-compute-scale.md` (Nygard format)
- Modify: `v2/docs/ops/deploy.md` (a "Compute budget" paragraph next to the checkpoint table: the `compute.*` keys,
  their defaults, that `budget_seconds + startup_allowance_seconds` must stay within the job `--timeout`, how to set
  them with `lakehouse config set compute.<field> <n>`, and the rollback order from Task 4), `v2/AGENTS.md` (one
  class-1 bullet with a pointer to ADR-017: "tracking computes skip on the input key; output logic lives only in
  digested modules; never hand-keep a code version"), the calc-layer spec D9 cross-reference line.

- [ ] **Step 1: Write ADR-017.** It records: the §4 classification rule; §0.5 no downsampling; the governing runtime
  and window; numba (backend change, tolerance `1e-9`); the exact skip key (MD5-of-JSON row digest, code digest over
  `TRACKING_CODE_MODULES`, `INFRA_MODULES` and why), the deadline/resume design; the coverage rule (v1
  `AGENTS.md:132`, adopted); lever C's decision with the Task 2/7 numbers, including what C does not fuse (the
  measured `extract`, `ball`, `combine` shares); the line_breaking chunk decision (Task 9); the parity gates.
  It also records **one known exception to the spec §11 skip-key anti-goal**: `to_json` writes `-0.0` as `0.0`, so a
  change from `0.0` to `-0.0` alone does not change the key. The values are numerically equal, and no scorer branches
  on the sign of zero.
- [ ] **Step 2: Update** `deploy.md` and `AGENTS.md`. v2 has no AGENTS.md budget test; keep the new bullet to one
  line ending in `→ ADR-017`, the same form as its neighbours.

### Task 9: Mac e2e — correctness, deadline, resume, steady state, real-scale oracles, budgets (§4, §6, §9, §10)

**Files (bench, written before the Mac runs):**
- Create: `v2/tests/bench/test_bench_tracking_compute.py` (`bench` + `heavy` markers; reads the published Mac catalog;
  extends `budget.json` with a `tracking_compute` section through the existing `--bench-record` flag and the same
  25 % headroom as `test_bench_tracking.py`)

**Interfaces (bench tests):**
- ~~`test_bench_group_equals_pure_scorers_real_match`~~ (S oracle): **removed by the Amendment 2026-11-23.** The
  `TRACKING_FRAME_METRICS` group is out of the default run and the silly-kicks upgrade replaces its scorers, so that
  cycle owns its real-scale oracle.
- ~~`test_bench_batched_equals_loop_real_match`~~ (C oracle): not built (Task 7 gate: C not taken).
- `test_bench_tracking_compute_budget` (A PERF-PLAN-04; the §10 regression benchmark): for line_breaking, per
  provider, ms per frame and peak RSS; and the K skip-check seconds for an unchanged match; fails above `budget.json`.
  (Amendment 2026-11-23: pitch_control and shape_graphs are no longer measured here.)

All Mac steps are outward (Mac run): owner yes before Step 1.

- [ ] **Step 1: Sync** the full uncommitted `v2/` tree to `/Volumes/Development/lakehouse/v2`
  (`karsten@192.168.68.59`); `source ../env.sh`; `uv sync --locked`; confirm
  `python -c "from lakehouse.compute.backend import numba_active; print(numba_active())"` prints `True`.
- [ ] **Steps 2-4 (deadline rehearsal, resume, backfill) are executed by the bronze no-duration plan's Task 7 Step 4**
  (`v2/docs/plans/2026-11-06-v2-bronze-no-duration-plan.md`): the Mac lake is rebuilt from a clean slate (deleted,
  `init`, a fresh capped ingest by `lakehouse run`, which runs the calc layer). Phase A (budget 5800 s) finishes the
  ingest; Phase B (budget 7200 s) is the deadline rehearsal (a match written, `deferred_matches > 0`, in-run
  publishes); Phase C (18,000 s runs) is the resume (no match computed twice) and the cold backfill (per-match seconds,
  cold first match included). Do not repeat them here; continue with Step 5 once that task's Step 5 verification is
  green.
- [ ] **Step 4b (Amendment 2026-11-23): finish the backlog with the reduced default.** Sync the D10 code (owner yes;
  only when no silly-kicks run uses the Mac). Run `lakehouse run` (`compute.budget_seconds 18000`) until
  `deferred_matches == 0` for every step in `DEFAULT_COMPUTE_STEPS` (the five SPADL steps and line_breaking). Expected:
  no `"compute step"` line for pitch_control or shape_graphs; no match computed twice.
- [ ] **Step 5: Verify outputs.** `spadl_actions`, `fct_actions`, `line_breaking_results` and `fct_tracking_metrics`
  are non-empty; 0 `compute_failed` at scale. `pitch_control_values` and `shape_graph_labels` are not required
  (D10). Record the idsse SPADL action count per match as the known D10 gap. The Workflows page shows the
  `excluded` and `incomplete` cards with their notes.
- [ ] **Step 6: Profile line_breaking and decide its chunk fallback** (spec §7.5 step 3; A PERF-PLAN-05 / B
  HFTC-PLAN-04). `fct_actions` is now published: profile the largest line_breaking match per provider with the
  Task 1 CLI (`line_breaking` phase). **Gate:** if any match's `detect_line_breaking` time is above **30 s** (half the
  local 60 s grace), build the chunk fallback as a Task 9b (actions chunked on the lakehouse side, `should_stop()`
  between chunks, plus `test_bench_line_breaking_chunked_equals_whole_real_match`, §4 comparator, before relying on
  it). Otherwise record the measured maximum in the ledger and ADR-017. Add line_breaking's worst match to
  `worst_match_seconds` either way. (Amendment 2026-11-23: the idsse numbers cover the incomplete idsse action set,
  18-27 actions per match; record that with them. The upgrade cycle re-runs this step after the DFL derive, D10
  item 6.)
- [ ] **Step 7: Run the bench** on the Mac: `uv run pytest tests/bench -m bench -k "tracking_compute"
  --bench-record` once to write the budgets, then without `--bench-record`. Expected: the line_breaking and skip-check
  budgets are recorded and pass. (Amendment 2026-11-23: the S oracle and the pitch_control backend bench belong to
  the excluded group; they are not run here.)
- [ ] **Step 8: Steady state.** One more `uv run lakehouse run` with no new matches. Expected: zero
  `build_match_frame` calls (every `"compute match"` line has `outcome=done`), tracking-step time recorded.

### Task 10: Review gate — before any production deploy (A PERF-PLAN-06 / B HFTC-PLAN-06)

- [ ] **Step 1:** Full local gate (all five commands in Global Constraints) over the whole `v2/` tree. Expected: all
  green; report the counts. (Amendment 2026-11-23: the static checks run on the dev PC; the pytest tier, which
  collects the e2e tests, runs on the Mac with `LAKEHOUSE_ENCRYPTION_KEY` unset; owner rule: no heavy jobs on the
  dev PC.)
- [ ] **Step 2:** `mad-scientist-skills:final-review` over the cycle diff; fix findings.
- [ ] **Step 3:** Send the implementation to Reviewers A and B (`/review-impl`, reports to `D:\Development\_reviews\`);
  fix or answer every finding; re-review until both approve. A fix that touches a `TRACKING_CODE_MODULES` module
  re-runs Task 9 Steps 7-8 on the Mac before Task 11.

### Task 11: HF deploy, calibration, and HF e2e (§6 steps 1-4, §10)

Each deploy, job, and config write is outward: owner yes at each step. Read `project_v2_hf_production_topology` and
`v2/docs/ops/deploy.md` first; never probe ids.

**Why the `cpu-basic` measurements (§6) come here, not in Task 1:** the HF lake has no calc-layer views until a run
with this code publishes them, and the only Job image is the one the nightly scheduled job uses. Deploying the
pre-change code would send the slow, deadline-less compute into the nightly run, where the 6 h timeout would SIGKILL
it and lose its work (§2). So HF gets the code only after N, S, K, and R land and pass review, and the first HF run is
itself the measurement run. The Task 4 defaults (seeded from the existing HF run logs, 18,000 s budget) keep that run
inside its window. Task 1 gives the Mac attribution that ranks the levers; this task gives the `cpu-basic` numbers
that size nights and the reserve.

- [ ] **Step 1: Deploy and rebuild = the bronze no-duration plan's Task 8 Steps 1-8** (owner suspends the schedule and
  pauses the ops Space → record the config → deploy the Job image and the ops Space from the reviewed branch stage →
  owner yes: empty both buckets, keep `lake-control` → one-off `lakehouse init --profile hf` job + the recorded
  `config set` jobs → one one-off `lakehouse run` job on `cpu-basic`, or more only until the ingest is complete;
  that task's Step 6 names the calibration run (it leaves a compute backlog; job ids and logs are kept) → verify → owner resumes the Space and the schedule). The deploy lives inside that suspend/resume window, so no scheduled run fires on a half-installed
  lake.
- [ ] **Step 2: Trigger** one run of the scheduled job after the schedule is resumed (ops Settings trigger or the
  HfApi trigger on the existing scheduled id; the definition is not changed). Expected: the run ends `ok` inside
  21,600 s, with in-run publishes and **0 `compute_failed`** (A CONSIDER 11); `compute_deferred` only while the
  backfill is not finished.
- [ ] **Step 3: Calibrate on `cpu-basic`.** From the logs of the calibration run that the no-duration Task 8 Step 6 names (in
  its branch 2, the Step 2 run takes that role) and of the Step 2 run (a compute-only night with a backlog): the `"compute backend"` line (record
  `numba: true` for the HF Job image, §7.1), startup offset (job start → `started_at`), the non-compute phases, the
  in-run publish cost, per-match seconds, the cold first match, and the uninterruptible pre-check segment (the
  profile's `read_convert` + `velocity` for the largest idsse match; it goes into `finish_reserve_seconds`). Then a
  one-off `hf jobs run --flavor cpu-basic` of `scripts/profile_tracking_compute.py` (read-only) for the largest match
  per provider. **Checkpoint-threshold decision** (B CONSIDER 10): if in-run publishes take more than 5 % of `W`, set
  compute-specific `checkpoint.minutes` / `checkpoint.matches` that bring it under 5 %; else keep the defaults and
  record the share. Set measured values with `lakehouse config set compute.<field> <n>` on the HF lake config, each
  with a ≥ 25 % margin; update the `ComputeConfig` defaults and `test_compute_budget_fits_the_deployed_timeout` inputs
  to match (a code change → the re-review of Task 10 Step 3). Record nights ≈ remaining cold CPU-hours ÷ `W` in the
  ledger and report it to the owner (§8.1). (Amendment 2026-11-23: pitch_control and shape_graphs do not run on HF
  in this cycle. The per-match seconds of the SPADL steps come from the run logs' `"compute match"` lines; the
  one-off profile job covers line_breaking only, `profile_tracking_compute.py --step line_breaking`. The idsse
  numbers cover the incomplete action set; the upgrade cycle re-calibrates them, D10 item 6.)
- [ ] **Step 4 — DEFERRED to the next cycle (owner 2026-11-06: "HF backfill can be deferred"; the plan is parallel HF
  `cpu-upgrade` jobs, pennies per hour).** Observe the nightly runs until `deferred_matches == 0` for every step in `DEFAULT_COMPUTE_STEPS` (Amendment 2026-11-23). Each run ends
  `ok` inside the window with 0 `compute_failed`.
- [ ] **Step 5 — DEFERRED with Step 4 (it needs the finished backfill). Steady state on HF.** The first run after the backfill builds zero frames; record its tracking time.
- [ ] **Step 6: Verify** the Workflows page and the new marts via the ops Space (on the partly backfilled lake; the
  marts show the matches computed so far).

### Task 12: Close-out and the owner commit gate

- [ ] **Step 1:** Re-run the full local gate if anything changed since Task 10 (the pytest tier on the Mac, as in Task
  10 Step 1). Expected: all green; report the counts.
- [ ] **Step 2:** Clean up the Mac diagnostics: `repro_pc.py`, `prof_pc.py`, `count_frames.py`, `count_frames2.py`,
  `count_providers.py`, `verify_epoch.py`, `schema.py`, `reparse5.sh`, `reparse3/4/5/6.log`, `reparse5.done` (owner
  yes; list them first).
- [ ] **Step 3:** Update the ledger and the memory entry. v2 has no `TODO.md`; the root `TODO.md` holds v1 entries.
  Ask the owner whether the calc-layer cycle gets a root `TODO.md` entry (cycle-close rule) before writing one.
- [ ] **Step 4: Stop.** Show the owner the full file list and diff stat of the calc-layer cycle and ask for explicit
  approval of the one cycle commit. Do not commit, push, or open a PR without that yes.

## Dispositions (plan review round 1)

Reviewer A = `…-perf-plan-A.md` (PERF-PLAN-nn). Reviewer B = `…-perf-plan-B.md` (HFTC-PLAN-nn). They agree on every
shared point; there is no disagreement.

| ID | Severity | Disposition |
|---|---|---|
| A-01 / B-01 home/away outside the digest | BLOCKING / SHOULD FIX | Fixed. `home_and_away` is pure in `pitch_control.py`; `HOME_TEAM_READ` is an input read; `tracking_steps` is in `TRACKING_CODE_MODULES`; the closure guard checks against `TRACKING_CODE_MODULES ∪ INFRA_MODULES`; new tests in Task 5; Global Constraint "output logic lives only in digested modules". |
| A-02 / B-02 64-bit row hash | SHOULD FIX | Fixed. Verified `hash(0,7919) == hash(7919)`. Task 3 uses the MD5 halves of `to_json(row)` (128-bit, probed: escaping, NaN, float round-trip, 0.34 s / 3.2M rows) plus an independence test. |
| A-03 / B-05 oracles not at real scale | SHOULD FIX | Fixed. Task 9 bench oracles on the largest real match per provider (S, C, and the line_breaking chunk fallback if built). |
| A-04 benchmark + budget.json missing | SHOULD FIX | Fixed. `test_bench_tracking_compute_budget` writes and checks `budget.json` (Task 9 Step 7). |
| A-05 / B-04 line_breaking profile + chunk gate | SHOULD FIX | Fixed. Task 1 phase map; Task 9 Step 6 profiles line_breaking once `fct_actions` is published and gates the chunk fallback at 30 s. |
| A-06 / B-06 review after production | SHOULD FIX | Fixed. New Task 10 review gate before the Task 11 HF deploy; a post-deploy fix re-reviews. |
| A-07 / B-07 spec status | SHOULD FIX | Fixed. Spec status records the owner approval (2026-11-06, sha `0ae8f717…`). A-07's missing branch-flip test is added to Task 3. |
| B-03 read guard on the whole step | SHOULD FIX | Fixed. `test_steps_issue_only_declared_reads` runs both steps; line_breaking's read is `ACTIONS_INPUT_READ`. |
| A-08 control-read classification, prefix match | CONSIDER | Fixed. `HOME_TEAM_READ` is an input read; `CONTROL_READS` matches by exact text. |
| A-09 deferred counts DONE matches | CONSIDER | Answered. The count is "not reached (skip state unchecked)" and the record says so; a skip check past the soft deadline would spend reserve time. |
| A-10 SPADL deadline-bounded | CONSIDER | Fixed. Global Constraint states it; test in Task 6. |
| A-11 HF 0 `compute_failed` | CONSIDER | Fixed. Task 11 Steps 2 and 4. |
| A-12 / B-12 first HF run uncalibrated | CONSIDER | Fixed. Task 4 Step 0 seeds reserves from existing HF run logs; default budget 18,000 s. |
| B-08 chunk memory sizing | CONSIDER | Fixed. Task 7 Step 5. |
| B-09 rollback order | CONSIDER | Fixed. Task 4 and Task 8 (`deploy.md`). |
| B-10 checkpoint-threshold decision | CONSIDER | Fixed. Task 11 Step 3. |
| B-11 FATAL out of a group | CONSIDER | Fixed. Task 5 test. |

## Dispositions (plan review round 2)

Reviewer A (`…-plan-r2-A.md`) and Reviewer B (`…-plan-r2-B.md`) both APPROVE. Their CONSIDERs are applied:

| ID | Disposition |
|---|---|
| B r2 HFTC-PLAN-13 `combine` phase | Fixed. Task 1 phase map adds `combine`; ADR-017 records what C does not fuse. |
| A r2 PERF-PLAN-13 CLI step scope | Fixed. Task 1 CLI gains `--step`; Task 9 Step 6 uses `--step line_breaking`. |
| A r2 PERF-PLAN-14 `-0.0` | Fixed. ADR-017 records it as a known exception to the spec §11 anti-goal. |

## Amendment (2026-11-06, after the owner's bronze no-duration ruling)

The owner ruled that v2 bronze stores no duration type (spec `2026-11-06-v2-bronze-no-duration-design.md`, approved)
and that it lands in this cycle before the Mac e2e. That plan (`2026-11-06-v2-bronze-no-duration-plan.md`) rebuilds
both lakes from a CLEAN SLATE (owner 2026-11-06: delete, `init`, a fresh `lakehouse run`, which runs the calc layer;
no in-place migration). So:

- **Task 9 Steps 2-4** are executed by the no-duration plan's Task 7 Step 4 (Phase A ingest at 5800 s, Phase B the
  7200 s deadline rehearsal, Phase C the 18,000 s resume and backfill), on the capped Mac lake (about 100 matches per
  provider, owner).
- **Task 11 Steps 1-2** become the no-duration plan's Task 8 (suspend → record config → deploy → empty → `init
  --profile hf` → fresh ingest → verify → resume) followed by the trigger.
- Order of work: perf Tasks 1-8 → no-duration Tasks 1-3, 6 (4-5 removed) → no-duration Task 7 (= perf Task 9 Steps
  2-4) → perf Task 9 Steps 5-8 → perf Task 10 (review gate, both plans' code) → no-duration Task 8 (= perf Task 11
  Step 1) → perf Task 11 Steps 2-6 → perf Task 12.
- Also applied after the round-2 approval: the round-2 CONSIDERs (A's `--step` and `-0.0` note, B's `combine` phase).

## Amendment 2026-11-23 (owner: pitch_control and shape_graphs leave the default run; calc-layer spec D10)

The silly-kicks tracking-batch cycle is late. The owner chose to finish this cycle without the two unoptimised
metrics (option A), to reduce this plan "as makes sense", and to ship idsse SPADL marked as known incomplete. The
binding text is calc-layer spec D10. Changes to this plan:

- **New Task 8b (TDD, local, before the Task 9 Mac steps):**
  - Files: `v2/src/lakehouse/compute/registry.py`, `v2/src/lakehouse/cards/model.py`,
    `v2/src/lakehouse/cards/slice.py`, `v2/src/lakehouse/ui/pages/workflows.py`,
    `v2/src/lakehouse/ui/pages/_workflows_dag.py`, and the tests named below.
  - Interfaces: `REGISTERED_COMPUTE_STEPS`, `EXCLUDED_COMPUTE_STEPS` and `DEFAULT_COMPUTE_STEPS` (registered minus
    excluded) in `registry.py`; `STEP_OUTPUTS` over the registered steps. `CardStatus` adds `excluded` and
    `incomplete` (D10 "Terms": `excluded` is not a deferred match); `WorkflowCard.note: str = ""`, required
    (non-blank) for those two statuses.
  - RED tests first:
    - `test_default_run_leaves_out_pitch_control_and_shape_graphs`;
    - `test_compute_card_is_excluded_exactly_when_its_step_is_not_in_the_default_run`;
    - `test_excluded_or_incomplete_card_needs_a_note`;
    - `test_idsse_spadl_and_the_mart_are_marked_incomplete` (also `fct_actions` and `line_breaking`, each naming the
      IDSSE gap; the mart note names the idsse `line_breaking_passes` gap);
    - `test_a_card_below_an_excluded_or_incomplete_card_is_not_active` (the D10 item 3 rule, every DAG edge);
    - `test_excluded_and_mart_notes_say_earlier_rows_stay` (D10 item 2);
    - the Workflows page shows a `Note` column, and the DAG marks a non-active node (dashed outline, status line,
      hover title with the note, legend entry).
  - The e2e tests that check the group's output (`test_compute_local.py`, `test_skillcorner_owner_format.py`) pass
    `REGISTERED_COMPUTE_STEPS`, so the kept code stays tested. They run on the Mac (no heavy jobs on the AI-PC).
- **Task 9:** new Step 4b (finish the SPADL and line_breaking backlog with the reduced default). Step 5 no longer
  requires pitch_control or shape_graphs output. The S oracle and the pitch_control backend bench are removed. The
  budget test covers line_breaking and the skip check.
- **Task 11:** Step 3 calibrates the SPADL steps and line_breaking only. Steps 4-5 stay deferred (owner
  2026-11-06); their condition now covers the default steps only.
- **No-duration plan Task 7 Step 4 Phase C:** "the three tracking steps" now reads "every step in
  `DEFAULT_COMPUTE_STEPS`". This rewrite came after Phase C had partly run: the E-loop on the Mac (2026-11-08/09) ran
  Phases B and C on the old default. Perf Task 9 Step 4b completes Phase C, and its result closes that checkbox.
- **Out of this cycle (the silly-kicks upgrade cycle, D10 item 6):** pitch_control on `compute_pitch_control_batch`,
  shape_graphs on `compute_shape_graphs` (keep only players with `shape_graph_source == "ok"`), idsse SPADL on the DFL
  derive, the pin bump, the group back in the default run, the real-scale gates that moved with the group (the S
  oracle and the pitch_control backend bench, on the new scorers), a re-run of Task 9 Step 6 and of the Task 11 Step 3
  idsse calibration after the DFL derive, the six marked cards back to `active`, and the HF backfill of the two
  metrics.

This amendment and spec D10 went to an A/B delta review (round 1: A and B REQUEST CHANGES, no BLOCKING; every
finding fixed, see D10 and the ledger). The implementation is reviewed at Task 10 as before.

## Amendment 2026-11-23 (b): the key reads push the match filter down (spec §7.4a; owner "Go ahead")

The Task 9 Step 8 steady state spent 56 min in line_breaking skip checks. Spec §7.4a holds the measured causes and
the design. Changes to this plan:

- **New Task 8c (TDD, after Task 9 Step 8):**
  - Files:
    - dbt: `v2/dbt/models/gold/tracking_cross_provider.sql`, `fct_tracking_frames.sql`, `fct_tracking_ball.sql`
      (its match and possession-team joins become scalar lookups too, spec §7.4a item 2), `_gold__models.yml`;
    - code: `v2/src/lakehouse/compute/reads.py`, `base.py`, `input_key.py`, `frames.py`, `tracking_steps.py`;
      `v2/src/lakehouse/resolve/links.py`; `v2/scripts/profile_tracking_compute.py`;
    - tests: `v2/tests/unit/test_input_key.py`, `test_frame_builder.py`, `test_tracking_steps.py` (its two
      `frame_builder=` fakes take `record_keys`), `test_tracking_frames_view.py`, `test_tracking_ball_view.py`,
      `test_resolve_links.py`, the new `test_tracking_read_pushdown.py`, `v2/tests/e2e/test_compute_local.py`, and
      `v2/tests/bench/test_bench_pitch_control_backend.py` (its `build_match_frame` call passes `record_keys`).
  - Interfaces:
    - `InputRead.with_record_keys: bool = False`;
    - `InputRead.params(match_key, provider, record_keys: Sequence[int] = ()) -> list[object]` returns the
      canonical key, then the provider (when `with_provider`), then `list(record_keys)` (when `with_record_keys`).
      It raises `ValueError` when `with_record_keys` is set and `record_keys` is empty;
    - `base.MATCH_RECORD_KEYS_SQL` =
      `SELECT record_key FROM lake.main.ops_canonical_pointers WHERE entity = 'match' AND canonical_key = ?`.
      `MATCH_VISIBILITY_SQL` embeds it, so the sub-select has one declaration and its text stays as it is today.
      `input_key.CONTROL_READS` lists it;
    - `input_key.match_record_keys(conn, match_key) -> tuple[int, ...]` returns the sorted set of `match_key` and
      every `record_key` that `MATCH_RECORD_KEYS_SQL` returns;
    - `read_sums`, `read_content_hash` and `input_key` take `record_keys: Sequence[int] = ()` (keyword) and pass it
      to `params`;
    - in `FRAME_INPUT_READS`, the frames read is
      `SELECT * EXCLUDE (match_key) FROM lake.main.fct_tracking_frames WHERE canonical_match_key = ? AND provider = ?
      AND list_contains(?::BIGINT[], match_key)`. The ball read is the same on `fct_tracking_ball` without the
      provider. Both set `with_record_keys=True`;
    - `build_match_frame(cat, match_key, *, provider, record_keys: Sequence[int])`: `record_keys` is a required
      keyword. The builder issues no lookup, so its read guard still sees exactly the four `FRAME_INPUT_READS`;
    - the one call site per match is the step driver's `produce` (`LineBreaking` and `TrackingFrameMetrics`) and
      the profile script. It calls `match_record_keys` once and passes the same tuple to the key reads and to
      `frame_builder(..., record_keys=...)`;
    - `links._pointers` raises `ValueError` naming the entity and the record keys when two rows share a
      `record_key`, before `replace_rows` (owner ruling 2026-11-23: fail loud + writer guard).
  - RED tests first:
    - `test_pushed_frame_reads_return_the_canonical_rows` (real dbt views): a match with no pointer, and a
      canonical match fed by two record keys. The pushed frames and ball reads return the same rows as
      `SELECT * EXCLUDE (match_key) ... WHERE canonical_match_key = ?` (plus the provider for frames);
    - `test_pushed_gradientsports_frames_read_filters_the_bronze_scan`: the `EXPLAIN` of the pushed frames read
      shows the `match_key` filter on the `gradientsports_tracking_players` table scan;
    - `test_match_record_keys_include_the_match_and_its_pointed_records`;
    - `test_a_read_with_record_keys_refuses_an_empty_list`;
    - `test_key_and_build_get_the_same_record_keys` (`test_tracking_steps.py`; the fake builder records its
      `record_keys`, a recording connection records the key-read parameters);
    - `test_a_duplicate_pointer_fails_the_reads_that_meet_it`, parametrized over `match`, `player` and `team`: the
      affected match's pushed frames read and pushed ball read (team: the possession team; player: frames only),
      and the canonical-only `SELECT *` read of each view, raise `duckdb.InvalidInputException` with
      `More than one row returned by a subquery`; an unrelated match's pushed reads still return their rows;
    - `test_resolution_refuses_a_duplicate_pointer` (`test_resolve_links.py`);
    - the frames and ball view contract tests list `match_key`;
    - the read guards (`test_build_match_frame_reads_only_declared_sql`, four reads; the e2e recording test, which
      accepts `MATCH_RECORD_KEYS_SQL` as a control read and nothing else undeclared).
  - RED observation (A PD-PLAN-04 / B-02):
    - run the tests that do not need dbt on the AI-PC before the code. They fail on a missing name or on the old
      signature. Record each failure;
    - sync the new and changed TEST files only to the Mac, then run the dbt-backed tests there against the
      pre-change views and code. Expected: the pushed-rows test fails (`fct_tracking_frames` has no column
      `match_key`); the duplicate-pointer test fails (the joins duplicate rows, no error); the contract tests fail
      (no `match_key`); the `EXPLAIN` test fails (no `match_key` filter). Record each failure reason in the ledger;
    - in the same Mac session, time the whole-view baseline: `TRACKING_DISCOVERY_SQL` and
      `SELECT count(*) FROM lake.main.fct_tracking_metrics`.
  - Verify GREEN: ruff, pyright, lint-imports and the touched light unit tests on the AI-PC. Then sync the code
    and run the dbt-backed tests on the Mac.
- **Task 9, new Step 9 (Mac, after Task 8c):**
  - measure-before-optimize. The baseline is the recorded skip_check_s in `budget.json`: gradientsports 45.4 s,
    idsse 3.68 s, skillcorner 3.25 s; Step 8 total 56 min 13 s over 169 matches (skillcorner 98, gradientsports 64,
    idsse 7).
  - Before the run, export every match's `line_breaking_results` rows from the published lake to
    `/Volumes/Development/lakehouse/perf/pushdown/lb-before.parquet` (outside the repo and the lake). Record the
    published version it came from.
  - Sync, then one `lakehouse run`. Expected: every line_breaking match re-computes once (the key changed), 0
    `compute_failed`.
  - Parity (spec §4, ADR-017 item 1): compare the new per-match `line_breaking_results` with the export, using the
    comparator in Global Constraints. Expected: 0 differing matches, and the same 169 matches and 213973 rows.
  - One more `lakehouse run` with no new matches. Expected: zero frame builds; line_breaking step time about 8 min
    (spec §7.4a). A time of 10 min or more is reported to the owner with the per-read timings. It does not become
    the new budget without the owner.
  - Time the whole-view readers again (`TRACKING_DISCOVERY_SQL`, `count(*)` over `fct_tracking_metrics`) and
    report them beside the baseline.
  - Re-record the bench (`-k "tracking_compute" --bench-record`), then run it without `--bench-record`. Report the
    before and after numbers.

## Dispositions (pushdown delta review round 1)

Reviewer A = `D:\Development\_reviews\2026-11-23-v2-perf-skip-key-pushdown-amendment-plan-A.md`, Reviewer B =
`…-plan-B.md`. They agree on the shared findings. On parity they differ only in severity (A BLOCKING, B SHOULD FIX),
and one fix resolves both. A-3 asked for an owner call. The owner ruled "fail loud + writer guard" (2026-11-23).

| ID | Severity | Disposition |
|---|---|---|
| A PD-PLAN-01 / B PUSH-AMD-B-01 parity by counts only | BLOCKING / SHOULD FIX | Fixed. Step 9 exports every match's `line_breaking_results` before the re-compute and compares with the Global Constraints comparator; expected 0 differing matches. Spec §7.4a "Parity". |
| A PD-PLAN-02 lookup call site | SHOULD FIX | Fixed. `build_match_frame(..., record_keys)` is a required keyword; the driver `produce` (and the profile script) is the one call site; the builder guard stays at four reads; `test_key_and_build_get_the_same_record_keys`; the `test_tracking_steps.py` fakes are in the file list. Spec item 6. |
| A PD-PLAN-03 / B PUSH-AMD-B-03, -04 duplicate-pointer reach | SHOULD FIX / CONSIDER | Fixed. Owner ruling: fail loud + writer guard. Spec item 2 states which reads fail and which do not; the test is parametrized over match, player and team, asserts the multiple-rows error, and pins an unrelated pushed read as unaffected; `test_resolution_refuses_a_duplicate_pointer`. |
| A PD-PLAN-04 / B PUSH-AMD-B-02 RED not observed | SHOULD FIX | Fixed. "RED observation": the light tests run RED on the AI-PC; the dbt-backed tests run RED on the Mac with a tests-only sync before the code, each expected failure recorded. |
| A PD-PLAN-05 / B PUSH-AMD-B-04 whole-view readers | CONSIDER | Applied. `TRACKING_DISCOVERY_SQL` and `count(*)` over `fct_tracking_metrics` are timed before (Task 8c) and after (Step 9). |
| A PD-PLAN-06 estimate | CONSIDER | Applied. Spec §7.4a states the per-provider counts (98 / 64 / 7), counts the other reads, gives about 8 min, and says a time of 10 min or more is reported to the owner. |
| A PD-PLAN-07 single-key evidence | CONSIDER | Applied. Spec §7.4a says the multi-key case has fixture coverage only. |
| A PD-PLAN-08 / B PUSH-AMD-B-05 "same session snapshot" | CONSIDER | Applied. Spec item 5 states the premise (no pointer write during the compute, single-writer lease); item 6 makes one lookup feed key and build. |
| A PD-PLAN-09 repeated sub-select | CONSIDER | Applied. `base.MATCH_RECORD_KEYS_SQL`; `MATCH_VISIBILITY_SQL` embeds it (same text). |
| A PD-PLAN-10 pushdown pinned only by the bench | CONSIDER | Applied. `test_pushed_gradientsports_frames_read_filters_the_bronze_scan` (an `EXPLAIN` check in the normal tier). |
| B PUSH-AMD-B-06 ADR silent on the duplicate behaviour | CONSIDER | Applied. ADR-017 item 4 records the change and the owner ruling. |

## Dispositions (pushdown delta review round 2)

A and B: APPROVE (`…-amendment-plan-r2-A.md`, `…-plan-r2-B.md`); four new CONSIDERs, all applied.

| ID | Severity | Disposition |
|---|---|---|
| A 11 / B PUSH-AMD-B-08 bench caller of `build_match_frame` | CONSIDER | Applied. `tests/bench/test_bench_pitch_control_backend.py` is in the Task 8c file list and passes `record_keys`. |
| A 12 ball view keeps its joins | CONSIDER | Applied as the wider option: `fct_tracking_ball`'s match and possession-team joins become scalar lookups too, so the owner's fail-loud ruling holds for both views. The duplicate test pins the ball read. Step 9 measures the ball read after the change. |
| B PUSH-AMD-B-07 discovery and pruned lookups | CONSIDER | Applied. Spec item 2 ties each failing reader to the lookups it projects; a player or team duplicate may not fail `TRACKING_DISCOVERY_SQL`. The test asserts only the `SELECT *` and pushed reads. |

## Dispositions (D10 delta review round 1)

Reviewer A = `…-d10-default-run-amendment-plan-A.md`, Reviewer B = `…-plan-B.md`. They agree on the shared finding;
there is no disagreement.

| ID | Severity | Disposition |
|---|---|---|
| A-1 / B-01 idsse line_breaking shown active | SHOULD FIX | Fixed. D10 item 3 rule: a card whose input card is `excluded` or `incomplete` is not `active`; `fct_actions` and `line_breaking` are `incomplete` with IDSSE notes; the mart note names the idsse `line_breaking_passes` gap and no longer says "is filled". `test_a_card_below_an_excluded_or_incomplete_card_is_not_active` checks every edge (RED: 3 failed after the rename, then GREEN). |
| A-2 / B-03 "stay empty" vs the kept rows | SHOULD FIX / CONSIDER | Fixed (note text, A's recommendation). D10 item 2 and the notes: filled only for matches computed before 2026-11-23; those rows stay and are not re-checked until the upgrade. `test_excluded_and_mart_notes_say_earlier_rows_stay`. |
| A-3 idsse sizing on incomplete actions | SHOULD FIX | Fixed. Task 9 Step 6 and Task 11 Step 3 record that the idsse numbers cover the incomplete action set; D10 item 6 re-runs both after the DFL derive. |
| A-04 gates that moved with the group | CONSIDER | Applied. ADR-017 item 9 and D10 item 6 name the S oracle and the backend bench. |
| A-05 no "D10 governs" / stale lines | CONSIDER | Applied. D10 status line and a spec status clause; plan Status, Goal and registry line; ADR-017 Status. |
| A-06 where the full tier runs | CONSIDER | Applied. Task 10 Step 1 and Task 12 Step 1: the pytest tier runs on the Mac. |
| A-07 no SPADL `--step` | CONSIDER | Applied. Task 11 Step 3: SPADL seconds from the run logs. |
| A-08 Phase C rewritten after it ran | CONSIDER | Applied. Step 4b completes Phase C and closes its checkbox. |
| B-02 "the three cards" | CONSIDER | Applied. D10 item 6 names the six marked cards; the edge-rule test ties the mart to the excluded group. |
| B-04 "deferred" means two things | CONSIDER | Applied. The status is renamed `excluded` (`EXCLUDED_COMPUTE_STEPS`); D10 "Terms" separates it from a deferred match. |
| B outside: `architecture.dsl` | note | Left to Task 10 Step 2 (`final-review` regenerates the C4 model). |
