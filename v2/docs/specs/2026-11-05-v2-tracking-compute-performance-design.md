# v2 Tracking-Compute Performance Redesign — Design

**Status:** APPROVED. A/B-approved at round 5 (2026-11-06); owner-approved 2026-11-06 ("Spec approved, write the
plan") at content sha256 `0ae8f717b050a701…` (this status line is the only later edit). Rounds 1-3 got REQUEST CHANGES from
both reviewers; round 4 got APPROVE from A and REQUEST CHANGES (one finding) from B; round 5 got APPROVE from both
(`D:\Development\_reviews\2026-11-06-v2-tracking-compute-perf-spec[-r2|-r3|-r4|-r5]-{A,B}.md`). The two round-5
CONSIDERs (a line reference, the §9 guard count) are applied. This doc records the owner decisions of 2026-11-06
(§0) and answers every finding (§12-§15). No code until this doc is approved and a plan is
written, reviewed, and approved. **Amended 2026-11-23 (§7.4a, owner "Go ahead"):** the lever K reads push the match
filter down. It is A/B delta-reviewed before code.

**Spec owner:** Karsten. **Author session:** calc-layer (branch `feat/v2-calc-tracking-metrics`).

**Supersedes nothing.** Amends the performance posture of the approved calc-layer slice
(`2026-11-04-v2-calc-layer-tracking-metrics-design.md`, D3/D8/D9). The correctness of the tracking computes is not in
question here. Only their wall-clock at the real 980-match scale is.

---

## 0. Owner decisions (2026-11-06)

These came from the owner after the round-1 and round-2 reviews. They set the bar for this design.

1. **The governing runtime is the HF scheduled job.** The lake holds only public data, so HF is acceptable. The Mac
   e2e stays a correctness gate. It is not the production budget proof.
2. **The cold backfill completes across nightly runs (resume).** Each run must end cleanly inside the job window.
   The next run continues where the last run stopped. After the backfill, each nightly run computes only new or
   changed matches.
3. **Add numba** (the silly-kicks `numba` extra). Pin the parity oracle to the numba backend.
4. **No in-process parallelism this cycle.** The approved rule "A run processes one heavy match at a time" (ingestion
   spec §10.9, C7; parent spec T12) stands unchanged. The owner plans HF scale-out later, probably as parallel
   `cpu-upgrade` tasks. That is owner-scheduled future work, not part of this cycle. Round 1's lever A is withdrawn.
5. **No downsampling.** "Gold standard with no loss in quality." Every tracking frame is scored. Round 1's lever B
   (frame-resolution scope change) is withdrawn. If the measured design needs more nights than expected, the
   backfill takes more nights. More speed then comes from the owner's later HF scale-out, never from fewer frames.

## 1. Intent — why this exists

The real-scale Mac e2e (the owner-mandated "e2e before commit") proved that two v2 layer-1 tracking computes,
`pitch_control` and `shape_graphs`, are correct but too slow on the live 980-match lake. They cannot complete inside
the production job window (§5). So the calc-layer e2e cannot complete, and the cycle cannot commit.

v1 did not have this problem, for two reasons. v1 had numba as a direct dependency (root `pyproject.toml:563`), so its
per-frame TTI and influence kernels (`src/analytics/pitch_control_numba.py`, per-frame `tti_numba` and
`influence_numba`, no batch function) ran compiled. v1 also fanned the frames out over Spark. v2 has neither. numba is
not in `v2/pyproject.toml` or `v2/uv.lock`, so silly-kicks sets `_HAS_NUMBA = False`
(`silly_kicks/tracking/pitch_control/_spearman.py:24-28`) and runs the numpy fallback. The author verified this on
the Mac venv on 2026-11-06 (`HAS_NUMBA False`). v2 also runs on one process.

This redesign makes the tracking computes viable at 980-match scale on the production runtime, at the project's
gold-standard bar, with **no loss of output fidelity** (§0.5).

## 2. The problem, with evidence

`pitch_control_values` (`src/lakehouse/compute/pitch_control.py:59-97`) and `shape_graph_values`
(`shape_graphs.py:61-105`) each score one surface per tracking frame. They loop per `(period_id, frame_id)` (and per
team for shape_graphs) in a pandas `groupby`. The driver `run_per_match_bronze` (`compute/base.py:154-201`) iterates
the 980 matches one after another in one process.

Surface count, measured from `fct_tracking_frames` on the live Mac lake (2026-11-05):

| provider | matches | surfaces (distinct frames) | per match | frame rate |
|---|---|---|---|---|
| skillcorner | 909 | 54,644,706 | ~60,100 | ~10 Hz (inferred) |
| gradientsports | 64 | 11,849,815 | ~185,000 | ~30 Hz (inferred) |
| idsse | 7 | 1,002,644 | ~143,000 | 25 Hz (`parsers/idsse.py` `FRAME_MS=40`) |
| **total** | **980** | **67,497,165** | — | — |

The inferred rates are frames per match ÷ ~95 minutes. The plan confirms each rate from the frame count ÷ the
period durations in `fct_tracking_ball.time_seconds`. (`fct_match_params.frame_rate` is not a valid source for this:
it is the literal `25.0` for idsse, `fct_match_params.sql:52`, and null when a provider table is absent.)

**The baseline figure is end-to-end, not a kernel figure, and it is a Mac figure.** The reparse6 e2e ran the
`pitch_control` step only on the Mac (shape_graphs was not reached before the kill). About 58 matches, or ~4M frames,
took 3 h 15 min. That gives **~2.9 ms per frame, end-to-end, on the Mac**. The figure includes, per match:

- `build_match_frame`: four DuckDB view reads plus `convert_to_frames` (`frames.py:58-99`).
- `_frame_source_hash`: a sort plus a `to_csv` serialisation of the whole frame (`pitch_control.py:100-104`).
- The skip-guard query, `smooth_frames` + `derive_velocities`, the pandas `groupby`, and the per-frame kernel.
- The bronze write + swap (`base.py:117-151`).

How the 2.9 ms splits between these terms is **not known yet**, and the `cpu-basic` cost is not known yet. The
profile in §6 measures both. The lever order in §7 is conditional on that profile. (The v1 `AGENTS.md` "≤5 ms/frame"
figure is a budget for v1's batched kernel. v2 has no per-frame budget, so this spec does not compare against it.)

Cold-run CPU time at today's Mac cost, on one core:

- pitch_control: 67,497,165 × 2.9 ms ≈ **54 h**.
- shape_graphs: not measured. If it costs the same per frame, both computes need ≈ 108 h ≈ **4.5 days**.
- `line_breaking` scores per action, not per frame, but it also calls `build_match_frame` (`line_breaking.py:26,119`).
  Its per-match cost is not measured.

**Repeated per-match work.** `pitch_control`, `shape_graphs`, and `line_breaking` each build and hash the same match
frame on their own (`pitch_control.py:158-161`, `shape_graphs.py:152-155`, `line_breaking.py:119`). That is three
full reads and builds per match.

**The steady-state run is as expensive as the cold run, except for the scoring.** Match discovery selects every match
(`pitch_control.py:153-155`). The skip-guard runs only after `build_match_frame` and `_frame_source_hash`
(`:158-163`; same in `shape_graphs.py:152-157`). So a nightly run with zero new matches still reads, converts, and
hashes all 980 matches, once per tracking step.

**A timed-out run loses its compute work.** Compute writes swap into the working catalog (`base.py:151`) but never go
through the checkpoint policy (`runs/checkpoint.py`). Nothing is published until `run_end` (`runs/run.py:781`). HF
Jobs sends SIGKILL about 2 seconds after SIGTERM, with no setting to change it (`docs/ops/deploy.md:343`, measured
2026-10-31). So a timed-out HF run is a crash, and that run's compute output is lost. Resume (§0.2) cannot work
until this is fixed.

**SIGTERM does not reach the compute phase.** The SIGTERM handler sets an Event (`run.py:1044-1057`) that
`run.stop()` reads (`:1006`). `_check_stop` polls it during ingest (`:273-275`), and an ingest `_Stop` calls
`stopped()`, which sets `terminated` (`:811-814`). `finish()` checks `terminated` once, at its start (`:671`). Nothing
polls `stop()` after that, so a SIGTERM during the dbt builds or the compute is ignored until the run ends.

v2 is DuckDB single-node (DuckLake), not Spark. The v1 `AGENTS.md` executor and `applyInPandas` rules do not apply.
No other v1 Databricks rule applies either (the 8 h drain budget and the 2700 s watchdog of v1 ADR-037/082 included).

## 3. Goals

1. **Production fit.** On the governing runtime (§5), every scheduled run ends cleanly inside its
   `--timeout 21600` window. The compute shares that window with job startup, ingest, resolution, the three dbt
   builds, maintenance, cleanup, and the run-end commit.
2. **Resumable cold backfill.** The 67.5M-surface cold backfill completes across consecutive nightly runs. The
   **deadline** (§7.5) is the clean stop on HF. A run that is killed instead (SIGKILL, crash) loses at most the work
   since its last publish, and the next run redoes it. The plan reports the measured number of nights (§8.1).
3. **Cheap steady state.** A nightly run with no new or changed tracking matches spends at most a small, measured
   time in the tracking computes. It does not build any match frame.
4. **No loss of output fidelity** (§0.5). Every change is an **optimization** (the output rows are the same) or the
   one **backend change** (numba, within a recorded tolerance against numpy). Every frame is scored.
5. **Measure before optimize.** A cProfile of the per-match hot path and `pytest-benchmark` tests at production
   scale back every claim. The figures above are the starting baseline.
6. **Gold standard.** A clean, documented design with a new v2 ADR (ADR-017), not a one-off fix.

## 4. Change classification rule

No v2 ADR records this rule yet, so this spec states it here and ADR-017 records it (§9). (Round 1 cited "ADR-004"
for it. That was wrong: v2 ADR-004 is the canonical pointer, and v1 ADR-004 is the pre-aggregated marts.)

- **Optimization:** the output rows are the same. Gate: a parity oracle that compares the old and new output.
- **Backend change:** the same algorithm with a different floating-point operation order (numpy vs numba). Gate: a
  recorded maximum absolute difference against the other backend, inside a stated tolerance.
- **Scope change:** a different set of computed rows (for example, fewer frames). **None is allowed in this design**
  (§0.5). Any future one needs the owner's explicit sign-off and a stated-tolerance parity test.

**Parity comparator** (for every oracle in this spec): the per-match output arrow rows, without the audit columns
(`_ingested_at`, `_run_id`, `_source_hash`, and the injected `match_key`), sorted by the table's key columns, compared
value for value. The upstream silly-kicks parity test (`test_spearman_batch.py`) uses small synthetic fixtures, so it
is not a gate here. This spec's oracles run on real production-scale frames.

## 5. Constraints

- **Governing runtime:** the HF scheduled job `lakehouse-ingest`: `cpu-basic` (2 vCPU, 16 GB, from
  `hf jobs hardware`), cron `0 3 * * *` UTC, `--no-concurrency`, `--timeout 21600` (`docs/ops/deploy.md:270`). The
  timeout counts from job start, so it also covers the startup before the run's `started_at` (`run.py:230`).
- **SIGKILL ~2 s after SIGTERM on HF** (`deploy.md:343`). No in-run SIGTERM handling can finish a publish in 2 s. The
  local schedulers (launchd, systemd) give 60 s (`deploy.md:341-342`).
- **One heavy match at a time** (ingestion spec §10.9, C7; parent spec T12). It stays in force (§0.4). The compute
  holds one match frame at a time. `ResourceCaps` (`config/model.py:147-150`) bounds DuckDB and arrow.
- **Memory:** the worst-case match frame is idsse, at ~3.2M player rows per match (`tests/bench/budget.json`).
  SkillCorner is ~15 rows per frame. Any chunk intermediate (§7.2) adds to the one held frame and must fit the 16 GB
  job with the DuckDB `memory_limit` cap beside it.
- **Single writer and bronze swap** (v2 ADR-007). The compute writes and swaps on the run's connection. Every publish
  goes through the checkpoint policy and its bronze integrity guard (`runs/checkpoint.py`).
- **silly-kicks is read-only from this repo.** The design changes only lakehouse call sites.
- **The job image builds from `uv.lock`** (`docs/ops/deploy.md:222-229`). A dependency change reaches HF only through
  the lock and a Job-image redeploy.

## 6. Baseline and measurement (the plan's first task)

| metric | value | source |
|---|---|---|
| total surfaces | 67,497,165 | `count(distinct (match, period, frame))` on `fct_tracking_frames` |
| pitch_control, end-to-end per frame, Mac | ~2.9 ms | reparse6: ~4M frames in 3 h 15 min, pitch_control step only |
| pitch_control, cold, 1 Mac core | ~54 h | 67.5M × 2.9 ms |
| both per-frame computes, cold, 1 Mac core | ~108 h (4.5 days) if shape_graphs costs the same | not measured |
| production window | 21,600 s per run, from job start | `deploy.md:270` |

The plan's first task produces the attribution that this spec cannot yet give. **Every figure that sets the number
of nights or the deadline reserve is measured on an HF `cpu-basic` job**, not on the Mac. The Mac figures are for
development only.

1. A cProfile of one real match per provider for `pitch_control`, `shape_graphs`, and `line_breaking`, under **both**
   backends (numpy now, numba once added), on the Mac and on `cpu-basic`. It splits each step into: frame read +
   convert, source hash, skip-guard, velocity derivation, per-frame kernel (TTI, influence, extract, ball resolve,
   combine), and write + swap.
2. The worst-case single-match compute time on `cpu-basic` (the largest gradientsports match, and the largest idsse
   match by rows).
3. The steady-state cost on `cpu-basic`: one run with zero new matches, timed per tracking step, including the K
   content hash (§7.4).
4. The non-compute share of the window on `cpu-basic`: job startup before `started_at`, ingest, resolution, the three
   dbt builds, maintenance, cleanup, and the run-end commit, on a quiet nightly run. Plus the cost of one in-run
   checkpoint publish (`assert_bronze_integrity` + the commit). These set the compute window `W` per night.
5. The results go into the existing benchmark harness: `tests/bench/` (`test_bench_tracking.py`, `budget.json`),
   the `bench` marker (`pyproject.toml:103`), and `pytest-benchmark==5.3.0`. No new harness.

The lever ranking in §7 is a hypothesis until step 1 lands. The plan re-ranks the levers on the measured shares
before it builds them.

## 7. The approach

Five levers, all fidelity-preserving. **N, S, K, and R are required.** N is the one backend change; S and K are
optimizations; R is correctness work that resume needs. **C** is an optimization, required for pitch_control if the
profile shows TTI is a material share under numba.

### 7.1 Lever N — numba backend (backend change; owner decision §0.3)

Add numba through the silly-kicks extra: `silly-kicks[parse-dfl,numba]==4.139.1` in `v2/pyproject.toml` (the extra
requires `numba>=0.59.0`). Regenerate `uv.lock`. silly-kicks then imports `tti_numba` and `influence_numba`, and
`compute_tti` / `_compute_influence` take the compiled path.

- **Classification:** a backend change, not an optimization. The numpy path (`_spearman.py:63-83`) and the numba path
  (`_numba_kernels.py:72-83`) use a different operation order, so bit equality is not assumed.
- **Oracle backend:** numba is the pinned backend for every C and S oracle in this spec.
- **Tolerance record:** a test computes pitch_control over real frames under both backends and records the maximum
  absolute difference of `control_fraction`. The plan states the tolerance (candidate: ≤ 1e-9) and the test asserts
  it. No pitch_control output has shipped yet (the calc-layer cycle is uncommitted), so no published value changes.
- **Environment guard:** a test fails when `_HAS_NUMBA` is `False` in the run environment. The run also logs the
  backend once per compute phase. The plan records `_HAS_NUMBA` for the Mac venv and the HF Job image.
- **Expected effect:** removes the numpy fallback's `(players × 1,600 targets × 2)` float64 intermediates per frame.
  The profile measures the gain. numba does not apply to shape_graphs (its Delaunay runs in scipy).

### 7.2 Lever C — batched TTI kernel (optimization, pitch_control only)

silly-kicks 4.139.1 ships `compute_pitch_control_batch` (`_dispatch.py:178`). For the default `method="spearman"` it
calls `compute_spearman_batch` (`_spearman_batch.py`), documented as byte-identical to the per-frame loop. It joins
every requested frame's players into **one** `compute_tti` call (`_spearman_batch.py:66-70`) and slices the result
back per frame.

**What C fuses and what it does not.** C fuses only `compute_tti`. These steps still run once per frame inside the
batch: `groups.get` (`_dispatch.py:245`), `_resolve_ball_position` (a boolean mask, `:255`, `:281`),
`_extract_frame_players` (a `.copy()`, `dropna`, and four `to_numpy`, `_spearman.py:193-213`), and `ids_match` +
`_spearman_combine` (two `influence_numba` calls plus reductions, `_spearman_batch.py:76-99`). So C's gain is bounded
by the TTI share that the profile measures under numba. The plan builds C only if that share is material. If the
plan does not build C, it records the measured share as the reason.

**Chunking without a rescan.** `compute_pitch_control_batch(frames, requests)` runs `group_rows` over the **whole**
`frames` argument on each call (`_dispatch.py:207-209`). So each chunk call passes **only that chunk's frame rows**,
never the whole match frame. The match frame is sorted by `(period_id, frame_id)` once. Each chunk is a contiguous
slice found with `numpy.searchsorted`, not a boolean mask per chunk. Velocity derivation (`smooth_frames`,
`derive_velocities`) runs once on the whole match before chunking, because it needs neighbouring frames.

**Chunk size:** a config value with a safe default. The plan sizes it against memory under numba: a TTI chunk holds
about chunk × ~22 players × 1,600 targets × 8 B (≈ 0.28 MB per frame) plus the influence arrays, on top of the one
held match frame (§5). A test proves that a chunked run equals an unchunked run (the parity comparator, §4).

**Gate:** byte-identical to the per-frame `compute_pitch_control` loop under numba (§4 comparator).

**shape_graphs:** silly-kicks 4.139.1 has no batched shape-graph kernel. `_shape_graph.py` has only the per-call
`compute_shape_graph` (`:290`) and `infer_positions` (`:863`). Both reviewers confirmed this. C does not apply to
shape_graphs.

### 7.3 Lever S — one frame build per match (optimization)

Build, convert, and smooth each match frame **once** for both layer-1 tracking computes. Then score `pitch_control`
and `shape_graphs` from that one frame.

- **Mechanism (recommended):** one layer-1 driver for the two tracking scorers. Per match it decides each scorer's
  skip state (§7.4), builds the frame once if at least one scorer must run, and runs each scorer that must run. The
  two pure scorers keep their outputs. Their only signature change is the optional `should_stop` check of §7.5. The
  plan settles the exact `ComputeStep` and `run_per_match_bronze` change. The other six registered steps keep their
  behaviour.
- **Each scorer stays independent** (today's semantics, `base.py:182-200`):
  - Each scorer runs in its own per-match `try`. A `shape_graphs` exception records `compute_failed` for
    `shape_graphs` on that match only. The match's `pitch_control` rows are still written, and the reverse.
  - Each output table keeps its own skip state (its own `_source_hash`). A match where only one table is stale runs
    only that scorer.
  - Each scorer's bronze is written and swapped on its own, so a failed scorer leaves its table's previous rows as
    they were.
  - `FATAL` still propagates.
- **Observable shape is unchanged** (Hyrum's Law): the run still records one `ComputeOutcome` per step name
  (`pitch_control`, `shape_graphs`), and the log line per step keeps its fields (`base.py:266-274`). The
  `compute_failed` message keeps its `"{step}: match_key=..."` form.
- **line_breaking** is layer 2. It runs after the `+fct_actions` build, so it cannot share the layer-1 frame without
  holding all 980 frames. It keeps its own frame build. The profile measures its cost. K and R apply to it.
- **Gate:** byte-identical to the separate steps' output (§4 comparator), over real frames, plus a case where one
  scorer raises on a match: the other scorer's output for that match is identical to the separate-step output, and
  the failed scorer records exactly one `compute_failed`.
- **Memory:** still one match frame at a time (§5).

### 7.3a Lever P — one period at a time (considered; dropped by the owner 2026-11-08)

A per-period frame build was designed against a 14.0 GB gradientsports peak. The cause of that peak was a data
defect: GS silver kept the raw and the provider-smoothed player rows and duplicate frame records. With the GS
views clean, the whole-match build peaks at a 6.5 GB footprint on the Mac (219 MB compressed), under the 8 GB
target. The owner dropped Lever P. The per-match duplicate check stays in `build_match_frame`.

### 7.4 Lever K — an exact skip key that does not build the frame (optimization of the skip decision)

Decide "already computed" **before** the frame is built, and make the key **more exact** than today's.

**Today's keys are not exact** (verified 2026-11-06). `_frame_source_hash` hashes only `player_id`, `period_id`,
`frame_id`, `x`, and `y` of the built frame (`pitch_control.py:100-102`; `shape_graphs.py:110`). So a change to
`team_id` or `is_goalkeeper` (a team re-pointing, a `fct_roster_gk` change, a GK-substitution fix such as
B-CALC-IMPL-01) does not re-compute. line_breaking's key hashes only `fct_actions[["action_id", "start_x",
"start_y"]]` (`line_breaking.py:125`), so it misses every frame change and most action columns. Today's keys do catch
a lakehouse builder-code change that moves `x` or `y`, because they hash the builder's output. These are defects of
the uncommitted calc-layer code. Lever K fixes them.

Round 2 proposed an enumerated lineage fingerprint (model checksums, source hashes, pointers). Reviewer A showed it
misses macro changes: the input views branch on `lake_table_present` (`fct_tracking_ball.sql`, `fct_match_params.sql`,
`fct_roster_gk.sql`; 12 calls). So that design is withdrawn.

**Design: an input content hash computed inside DuckDB, plus a derived code digest.** The key has three parts:

1. **Input content.** For each match, run the **same reads** the step makes (the four `build_match_frame` reads,
   `frames.py:58-70`; for line_breaking also its `fct_actions` read), but aggregate each one to a hash in SQL instead
   of transferring the rows into pandas. Every column the reads return is hashed, not a subset.
2. **Code digest, derived, never hand-kept.** A SHA-256 over the normalised source of every lakehouse module on the
   step's shaping and scoring path (at least `compute/frames.py` and the step's scorer module). Normalised = the
   `ast` dump with docstrings removed, so a comment, docstring, or format-only edit does not re-compute 980 matches,
   but any code change does. There is no manual "code-version constant" to forget.
3. **Library versions.** The installed versions (`importlib.metadata`) of every library on the path: `silly-kicks`,
   `numba`, `llvmlite`, `numpy`, `scipy`, `pandas`, `pyarrow`, `duckdb`, plus the backend flag (`_HAS_NUMBA`).

The combined key is stored as each output's `_source_hash`, so the existing skip comparison stays in place.

- **Exact for inputs.** Part 1 covers every row and column the step reads, after every view, macro, and pointer
  resolution. No lineage list exists to get wrong.
- **Exact for code.** Part 2 changes whenever the code that turns those rows into output changes.
- **One declared read list.** The builder (and line_breaking's read) and the key both take their **input** queries
  from one module-level declaration. The step's **control** reads (match discovery, `duckdb_tables()` presence checks,
  `_match_visibility`, the skip lookup) are a second, separate declaration: they decide what to run, not what the
  output is, so they stay out of the key. The completeness guard is a test that runs the step against a recording
  connection and fails if the step issues any read that is in neither declaration.
- **One declared code list.** Part 2's module list is a module-level declaration. A guard test fails if any
  `lakehouse.*` module imported by the builder or a scorer (transitively) is missing from it.
- **One declared library list.** A guard test maps every third-party top-level import in the declared modules to its
  distribution (`importlib.metadata.packages_distributions`) and fails if that distribution is missing from part 3's
  list.
- **Hash requirements** (the plan picks the exact SQL): independent of row order; sensitive to duplicate rows (so
  not a plain `bit_xor`); at least 128 bits; computed by DuckDB without materialising the frame in Python.
- **Behaviour tests (fail first):** an unchanged match skips; a single changed value in any column of any declared
  read re-computes (including `team_key` and the roster GK); a duplicated row re-computes; a `lake_table_present`
  branch flip (a provider table appears) re-computes; a code change in a declared module re-computes; a docstring-only
  change does not; a library version change re-computes. For line_breaking: a frame change re-computes.
- **Cost:** the steady state still scans each match's input rows once in DuckDB, but it does no `convert_to_frames`,
  no pandas transfer, and no `to_csv`. The plan measures it on `cpu-basic` (§6 step 3) against Goal 3. With lever S,
  the layer-1 key is computed once per match for both scorers.
- **Match discovery** stays a cheap `SELECT DISTINCT` over the views.

### 7.4a Amendment 2026-11-23 — the key reads push the match filter down (owner: "Go ahead")

**Problem (measured on the Mac, 2026-11-23).** The steady-state run (plan Task 9 Step 8) spent 56 min 13 s in
line_breaking skip checks and built no frame. Read-only probes on the published Mac lake found two causes. All
timings below are md5-sum key reads of one match. Every variant returned the same row count and the same two sums
as the shipped read. Every probed match has one record key (each pointer in the lake is a self-pointer), so the
multi-key case is covered by the fixture tests only.

1. **The filter does not reach the gradientsports dedup window.** `canonical_match_key` is derived after the
   provider union and the pointer join (`tracking_cross_provider.sql`). A filter on it cannot pass the `qualify`
   window in `stg_gradientsports__tracking_players` (and in the gradientsports CTE of `fct_tracking_ball`). So every
   gradientsports key read windows the whole gradientsports table. Largest match (5.59 M rows): 40.5 s. The
   staging view filtered on its own `match_key` answers `count(*)` in 0.15 s.
2. **With a per-match filter, DuckDB flips the pointer joins.** Once the read is filtered on `match_key`, the planner
   turns the `tracking_cross_provider` LEFT joins into RIGHT joins. The tracking rows become the build side, and
   the md5 projection then runs on one thread. idsse (3.65 M rows): 1.22 s shipped, 4.6 s filtered. The filtered
   bronze scan alone is parallel (0.88 s at 4 threads, 3.23 s at 1). With the flip disabled, or with the pointer
   lookups written as scalar subqueries, the filtered reads take 1.32 s (idsse) and 2.15 s (gradientsports).

**Design.**

1. **Expose the provider match key.** `fct_tracking_frames` and `fct_tracking_ball` add `match_key` (the provider
   record key, BIGINT), contracted. It is additive.
2. **Scalar pointer lookups in both views.** The three `ops_canonical_pointers` LEFT joins of
   `tracking_cross_provider` and the two of `fct_tracking_ball` (match, possession team) become correlated scalar
   subqueries:
   `coalesce((select p.canonical_key from ops_canonical_pointers p where p.entity = '<e>' and p.record_key = core.<k>),
   core.<k>)`. Resolution writes one pointer per `(entity, record_key)` (`resolve/links.py`), so the values are the
   same as the joins. The ball view changes for the owner ruling below, not for speed: its pushed read already takes
   0.19-0.33 s with the joins, and the plan measures it after the change.
   **A duplicate pointer fails loudly (owner ruling 2026-11-23: "Fail loud + writer guard").** The join silently
   duplicated rows in every reader. A scalar lookup that finds two rows raises DuckDB's
   `More than one row returned by a subquery` error (`scalar_subquery_error_on_multiple_rows`, default `true`). It
   raises in every read that evaluates that lookup for the duplicated key. A read that projects the looked-up column
   evaluates the lookup. A read that does not project it may skip the lookup, because DuckDB can prune an unused
   subquery. So:
   - a duplicate match pointer fails every whole-view or canonical-only reader of either view, because each one
     filters or projects `canonical_match_key`: `fct_tracking_metrics`, `TRACKING_DISCOVERY_SQL`, and the pages over
     the tracking views;
   - a duplicate player or team pointer fails every reader that projects `canonical_player_key`,
     `canonical_team_key` or `possession_team_key`. This includes `fct_tracking_metrics` and the frame reads. A
     reader that projects none of them, such as `TRACKING_DISCOVERY_SQL`, may still succeed;
   - the pushed reads of the affected match fail;
   - the pushed read of an unrelated match does not fail, because its record-key filter removes the duplicated key
     before the lookup.

   To put the error at its source, the resolution writer (`links._pointers`) refuses
   to write two pointers with the same `(entity, record_key)` and names them. Resolution cannot make one by
   construction (one pointer per cluster record; record keys are hashed from provider and native id), so the guard
   protects an invariant that already holds.
3. **The frames and ball reads filter on the record keys too.** Each read keeps `canonical_match_key = ?` and adds
   `list_contains(?::BIGINT[], match_key)`. The read selects `* EXCLUDE (match_key)`, so the builder input and the
   hashed columns stay as they are. This is the one list form that pushes down. `match_key IN (SELECT ...)`,
   `match_key IN (SELECT unnest(?))` and `= ANY(?)` do not push down (about 47 s on gradientsports).
   `list_contains` takes 2.48 s (gradientsports) and 1.50 s (idsse).
4. **The record keys** of a match are `{match_key}` plus every `record_key` whose match pointer has
   `canonical_key = match_key`. One declared CONTROL read finds them:
   `SELECT record_key FROM lake.main.ops_canonical_pointers WHERE entity = 'match' AND canonical_key = ?`. It is a
   control read, not an input read, because it only prunes. The canonical filter still decides the row set.
5. **Proof that the rows do not change.** Take a row with `canonical_match_key = C`. If its `match_key` has a match
   pointer, that pointer's `canonical_key` is `C`, so the `match_key` is in the record keys. If it has no pointer,
   its `match_key` is `C`, which is in the record keys too. The record-key filter therefore removes no row that the
   canonical filter keeps. The lookup and the reads are separate autocommit statements, not one transaction. The
   premise is that nothing writes pointers during the compute: resolution runs before it under the run's
   single-writer lease. The content hash then covers the rows each read returns.
6. **The same reads and the same record keys serve the key and the build.** `build_match_frame` and the key take
   both reads from `FRAME_INPUT_READS` (§7.4 "one declared read list"), so the frame build gets the same speed-up.
   The step driver looks up the record keys once per match and passes the same tuple to the key reads and to
   `build_match_frame`, so one lookup feeds both.

**Cost.** `frames.py` and `tracking_steps.py` are in `TRACKING_CODE_MODULES`, so every stored line_breaking key
changes. Each line_breaking match re-computes once (about 71 min on the Mac). HF holds no line_breaking results yet.
The excluded pitch_control and shape_graphs keys change too; they re-compute when the silly-kicks upgrade brings the
group back (calc-layer spec D10 item 6), which re-computes them anyway.

**Expected result (an estimate; the plan measures it).** The Mac lake has 169 line_breaking matches: skillcorner
98, gradientsports 64, idsse 7. A gradientsports skip check keeps its other reads (`fct_match_params`,
`fct_roster_gk`, `fct_actions`, the skip lookup; about 2.6 s by subtraction from the 45.4 s baseline), so it falls
to about 5.4 s. skillcorner falls from 3.25 s to about 1.3 s (the ball read carries the gain). idsse falls from 3.68 s
to about 1.9 s (its frames read gets slightly slower, 1.22 to 1.50 s; the ball read gains 2 s). Total: about 8 min,
against 56 min. A measured total of 10 min or more is reported to the owner with the per-read timings; it does not
silently become the new budget.

**Whole-view readers.** Item 2 changes the plan of every reader of `tracking_cross_provider`, not only the per-match
reads. The plan times one whole-view reader (`TRACKING_DISCOVERY_SQL` and a `count(*)` over `fct_tracking_metrics`)
before and after.

**Parity (the §4 gate for an optimization).** The build input path changes, so line_breaking's output passes the
parity comparator. The plan exports every match's `line_breaking_results` rows before the re-compute and compares
them value for value after it. Expected: 0 differing matches.

**Tests (fail first):**
- the pushed reads return the same rows as the canonical-only reads, for a match with no pointer and for a
  canonical match fed by two record keys;
- the record-key lookup returns the match key and every record key that points to it;
- a read that needs record keys refuses an empty list;
- the driver passes the same record-key tuple to the key reads and to `build_match_frame`;
- a duplicate match, player or team pointer fails the affected match's pushed frames and ball reads and the
  canonical-only `SELECT *` reads with
  the multiple-rows error, and an unrelated match's pushed read still succeeds;
- the resolution writer refuses a duplicate `(entity, record_key)` pointer;
- the pushed gradientsports frames read applies the `match_key` filter in the bronze table scan (an `EXPLAIN`
  check), so a rewrite to a form that does not push down fails in the normal test tier, not only in the Mac bench;
- the view contracts carry `match_key`;
- the read guard accepts the new control read and nothing else.

The skip-check budgets in `tests/bench/budget.json` are re-recorded on the Mac.

### 7.5 Lever R — deadline, durable progress, and stop handling (correctness work that resume needs)

Resume across nights (§0.2) needs these changes. Without them a timed-out run loses its compute work (§2).

1. **Durable progress.** Each per-match compute write calls the checkpoint policy's deferrable site
   (`checkpoints.maybe("compute")`). The existing T (`checkpoint.minutes`, default 5) and M (`checkpoint.matches`,
   default 50) thresholds then publish compute progress during the run, through the bronze integrity guard. A crash
   or SIGKILL loses at most the work since the last publish. The plan measures one publish's cost (§6 step 4) and
   may set compute-specific thresholds if the publish cost is a material share of `W`.
2. **The deadline: the only clean stop on HF.** HF sends SIGKILL ~2 s after SIGTERM (§5), so on HF only a deadline
   that the run sets for itself can end a run cleanly. New config values:
   - `budget`: the run's time budget, counted from job start. A config test fails unless
     `budget + startup_allowance ≤` the deployed `--timeout` (`deploy.md:270`, Hyrum's Law), where
     `startup_allowance` is the measured job-start-to-`started_at` time (§6 step 4).
   - `finish_reserve`: the measured time for the work that must still run after the compute stops: the middle and
     final dbt builds, the layer-2 compute's minimum share, maintenance (`merge_budget_seconds`), cleanup, one
     checkpoint publish, and the run-end commit, plus a safety margin.
   - **Soft deadline** = `budget − finish_reserve − worst_match`, where `worst_match` is the measured worst-case
     single-match compute time (§6 step 2). The compute starts no new match after it.
   - **Hard deadline** = `budget − finish_reserve`. It is checked inside a match through the in-match check point
     below. If it is reached, the in-flight match is abandoned with no partial write and is counted as deferred.
   Each layer gets a share of the window, so line_breaking also makes progress on each backfill night. The plan sets
   the split from the measured costs.
3. **The in-match check point, independent of lever C.** The scorers cannot be chunked from outside:
   `pitch_control_values` derives velocities over its whole input (`pitch_control.py:73-74`), and shape_graphs has no
   chunks. So the two per-frame scorers (`pitch_control_values`, `shape_graph_values`) each gain one optional
   keyword, `should_stop: Callable[[], bool] | None = None`. Each calls it every N frames inside its own per-group loop
   (`pitch_control.py:80`, `shape_graphs.py:79`), after velocity derivation. When it returns `True`, the scorer raises a
   dedicated abandon signal. The
   driver catches that signal (it is not a `compute_failed`), writes nothing for the match, and counts it as deferred.
   The driver's `should_stop` returns `True` when the hard deadline is reached or `run.stop()` is set. With lever C,
   the batch loop calls the same check between chunks. N is a config value; the plan sizes it so that one check
   interval is a few seconds on `cpu-basic`. With `should_stop=None` the scorer output is unchanged, and the S and C
   oracles run with it unset.

   **line_breaking has no lakehouse loop.** `line_breaking_values` makes one call, `detect_line_breaking(actions,
   frame)` (`line_breaking.py:60`), and that function loops over the actions inside read-only silly-kicks. So
   line_breaking is checked **between matches only**. Its measured worst-case match time (§6 step 2) is part of
   `worst_match` under the soft deadline. If the plan's measurement shows one line_breaking match runs longer than the
   check interval allows (for the local 60 s grace), the plan chunks the actions on the lakehouse side instead, and
   adds a parity oracle "chunked = whole-match" on real matches before it relies on that. (silly-kicks would have to
   confirm that `detect_line_breaking` scores each action independently.)

   **The segment before the first check cannot be interrupted.** The frame read, `convert_to_frames`, and velocity
   derivation for a whole match (worst case an idsse match, ~3.2M rows) run before the first `should_stop` call. The
   plan measures that segment on `cpu-basic` (§6 step 2), and `finish_reserve`'s safety margin covers it.
4. **Local stop handling (launchd / systemd, 60 s grace).** The compute polls `run.stop()` between matches and through
   the in-match check point. On a stop it abandons the in-flight match (no partial write) and calls `run.stopped()`.
   `finish()` then skips its remaining dbt builds, compute layers, maintenance, **and cleanup**, and goes straight to
   the run-end commit, which publishes the completed matches (`deploy.md:339-340`: "A stopped run (SIGTERM) publishes
   its pending work at the run end"). Skipping cleanup keeps today's behaviour: `run_cleanup` is inside
   `if not self.terminated:` (`run.py:671`, `:751-759`), and a storage sweep could outlast the 60 s grace before the
   commit. The plan adds the `terminated` checks between the `finish()` phases. On HF the same SIGTERM is followed by
   SIGKILL within ~2 s, so there it is a crash bounded by step 1.
5. **Tests (fail first):** a compute write publishes through `checkpoints.maybe("compute")`; a run past the soft
   deadline starts no new match; for pitch_control (with and without lever C) and shape_graphs, a `should_stop` that
   turns `True` mid-match makes the match write nothing and count as deferred; for line_breaking, a stop or a soft
   deadline between matches starts no further match (and, if the plan chunks its actions, the chunk check and the
   chunked-vs-whole oracle); a `stop()` during the compute calls
   `stopped()`, skips the later phases including cleanup, and the run-end publish holds the completed matches; the
   `budget` / `--timeout` parity test.

**Visible coverage, no silent gap.** A match not reached by the deadline is **deferred**, not failed. `ComputeOutcome`
gains a `deferred_matches` count. The run logs it and records it as a warning-severity ops record (not a run
failure). A deferred match has no mart row yet. No app page reads `fct_tracking_metrics` this cycle (verified by grep
of `ui/` and `web/`). Any later page that shows it must show its coverage, under the owner's standing rule against
silent data gaps (v1 `AGENTS.md:132` "never silently substitute data", adopted for v2; ADR-017 records it).

## 8. Open decisions (for the A/B re-review and the owner)

1. **The expected backfill length** (owner, for information). The plan reports the measured number of nights:
   nights ≈ cold compute CPU-hours on `cpu-basic` ÷ `W`, where `W` is the measured compute window per night (§6
   step 4). There is no fidelity fallback (§0.5). If the number is too large for the owner, the remedy is the owner's
   HF scale-out (§0.4).
2. **Lever S mechanism** (builder, in the plan): the exact driver shape, within the §7.3 semantics.
3. **Lever C** (builder, in the plan): build it or not, from the measured TTI share under numba.
4. **The K hash SQL** (builder, in the plan): within the §7.4 requirements.

## 9. What's left to the builder (plan scope)

- The measurement task (§6), on the Mac and on HF `cpu-basic`, with the results in `tests/bench/budget.json` and the
  bench tests.
- Lever N: the dependency and lock change, the environment guard test, the numba-vs-numpy tolerance test, and the
  `_HAS_NUMBA` record for the Mac venv and the HF Job image.
- Lever S: the one-build-per-match driver with independent scorers, the byte-identical oracle, and the
  one-scorer-fails case.
- Lever K: the four declarations (input reads, control reads, code modules, libraries), the in-DuckDB content hash,
  the derived code digest, the library versions, the three completeness guards (reads, code modules, libraries), and
  the behaviour tests (for all three tracking scorers).
- Lever R: the checkpoint call, the `budget` / `startup_allowance` / `finish_reserve` config and the `--timeout`
  parity test, the soft and hard deadlines, the `should_stop` check point in the two per-frame scorers (line_breaking
  between matches, or chunked with its oracle if the measurement needs it), the stop handling in
  the compute and between the `finish()` phases, the `deferred_matches` count and record, and the fail-first tests
  of §7.5 step 5.
- Lever C, if the profile supports it: chunked batch calls with frame-subset slicing, the chunk-size config, the
  chunked-vs-unchunked and batch-vs-loop oracles.
- **ADR-017** (v2): the tracking-compute scale decision. It records the change classification rule (§4), the
  no-downsampling ruling (§0.5), the governing runtime and window, the backend choice, the exact skip key, the resume
  design, the coverage rule, and the parity gates.

## 10. Definition of done

- On the HF scheduled-job runtime: a run during the cold backfill stops at its deadline, publishes its compute
  progress, and ends `ok` inside the window. The next run continues from that point. Both are proven on real HF runs.
- The cold backfill completes on HF, scoring every frame. The plan reports its measured number of nights (§8.1).
- A steady-state run with no new tracking matches builds zero frames and fits its window (measured on HF).
- pitch_control, shape_graphs, and line_breaking produce 0 failed matches at scale.
- Gates green: the N tolerance record and environment guard; the byte-identical oracles for S and C (if built),
  including the one-scorer-fails case; the K completeness guard and behaviour tests; the R tests.
- `pytest-benchmark` at production scale guards against regression. ADR-017 is written.
- The calc-layer e2e (all five SPADL providers, the tracking computes, and the marts) passes on the Mac **and** on HF.
- **Then the owner gives an explicit approval for the cycle commit.** This DoD is not commit authority.

## 11. Non-goals and anti-goals

- **Not** changing the `fct_tracking_metrics` schema, grain, or semantics.
- **Not** editing silly-kicks. N uses its published extra. C uses its public `compute_pitch_control_batch`.
- **Not** in-process parallelism or HF fan-out this cycle (§0.4).
- **Not** downsampling or any other change to the set of scored frames (§0.5).
- **Not** changing the SPADL computes. They keep their current skip-guard.
- **Anti-goal:** any result computed over fewer or coarser frames.
- **Anti-goal:** a skip key that misses any input row, input column, or code change that can alter the output.
- **Anti-goal:** any change that breaks the single writer, the bronze swap, the checkpoint integrity guard, or the
  per-match skip-guard.

## 12. Dispositions (round 1)

Reviewer A = `…-spec-A.md` (PERF-SPEC-nn). Reviewer B = `…-spec-B.md` (HFTC-SPEC-nn). There is no disagreement in
substance between A and B. Where both raised the same point, both IDs are listed.

| ID | Severity | Disposition |
|---|---|---|
| A-01 numba absent | BLOCKING | Fixed. Verified: no numba in `pyproject.toml`, `uv.lock`, the local venv, or the Mac venv (`HAS_NUMBA False`). Owner decision §0.3. §1 corrected. New lever N (§7.1) with its own classification, tolerance record, environment guard, and pinned oracle backend. |
| B-01 / A-03 wrong budget | BLOCKING / SHOULD FIX | Fixed. Owner decision §0.1-0.2. §3 and §5 name the HF job (`cpu-basic`, 2 vCPU, 16 GB, `--timeout 21600`, whole run). v1 8 h and 2700 s removed. DoD proves the budget on HF (§10). |
| B-02 one heavy match at a time | BLOCKING | Resolved by the owner decision §0.4: no in-process parallelism; the rule stands. Lever A withdrawn. |
| B-03 / A-02 B arithmetic | SHOULD FIX | Superseded: lever B withdrawn by the owner decision §0.5. (Round 2 had corrected it to ~2.3× at 5 fps, ~5.7× at 2 fps.) |
| B-04 / A-05 2.9 ms is end-to-end | SHOULD FIX | Fixed. §2 labels it end-to-end, Mac, pitch_control-only; "not the kernel" removed; ranking conditional on the §6 profile. Shared frame build designed as lever S. 4.5 days corrected. |
| B-05 / A-06 C fuses only TTI; rescan | SHOULD FIX | Fixed. §7.2 lists what stays per frame; C built only on a material measured TTI share; each chunk passes only its frame rows, found by `searchsorted`. |
| B-06 steady state unsized | SHOULD FIX | Fixed. §2 states the cost. Lever K (§7.4) and Goal 3; measured in §6 step 3; DoD in §10. |
| B-07 / A-04 A failure mapping, seam, serial share | SHOULD FIX | Not applicable: lever A withdrawn (§0.4). Lever S's seam and failure semantics are in §7.3. |
| B-08 / A-03 citations | SHOULD FIX | Fixed. §4 states the rule inline; ADR-017 records it. ADR-004, the 2700 s watchdog, and the v1 ≤5 ms budget are removed as v2 references. |
| B-09 4.5 days | CONSIDER | Fixed (§2, §6). |
| B-10 byte-identical comparator | CONSIDER | Fixed. §4 parity comparator. |
| B-11 / A-09 shape-graph batch | CONSIDER | Fixed. Recorded as settled in §7.2. |
| B-12 rows per frame | CONSIDER | Fixed. §5: idsse is the worst case; SkillCorner ~15 rows per frame. |
| B-13 / A-07 v1 history | CONSIDER | Fixed. §1: v1 had per-frame numba kernels plus Spark fan-out, no batch function. |
| B-14 / A-13 chunk memory | CONSIDER | Fixed. §7.2 sizes the chunk under numba, on top of the one held frame. Worker count no longer applies. |
| A-08 frame rates | CONSIDER | Fixed. §2 gives sources; the plan confirms the rates from frame counts ÷ period durations. |
| A-10 upstream parity test is fixture-scale | CONSIDER | Fixed. §4: it is not a gate; this spec's oracles run on production-scale frames. |
| A-11 existing bench harness | CONSIDER | Fixed. §6 step 5 names `tests/bench/`, the `bench` marker, and `budget.json`. |
| A-12 commit gate in DoD | CONSIDER | Fixed. §10 last bullet. |

## 13. Dispositions (round 2)

Reviewer A = `…-spec-r2-A.md`. Reviewer B = `…-spec-r2-B.md`. Both resolved every round-1 finding. Their round-2
findings on stop handling agree in substance: B asked for `stop()` polling with skipped phases, A showed that on HF
SIGTERM is followed by SIGKILL in ~2 s. §7.5 does both: the deadline is the HF clean stop, and `stop()` polling serves
the local 60 s window.

| ID | Severity | Disposition |
|---|---|---|
| A r2 PERF-SPEC-14 K misses macro lineage | BLOCKING | Fixed. The enumerated fingerprint is withdrawn. §7.4 now uses an in-DuckDB content hash over the builder's own four reads: exact by construction, one declared read list, a recording-connection completeness guard, and a branch-flip test. |
| A r2 PERF-SPEC-15 / B r2 HFTC-SPEC-15 SIGTERM | SHOULD FIX | Fixed. Verified `run.py:1006`, `:273-275`, `:671`, `:811-814` and `deploy.md:343`. §2 states both facts; §7.5 step 2 makes the deadline the only HF clean stop; step 3 polls `stop()`, calls `stopped()`, and skips the later `finish()` phases locally; Goal 2 restated as "loses at most the work since its last publish". |
| A r2 PERF-SPEC-16 / B r2 HFTC-SPEC-16 reserve | SHOULD FIX | Fixed. §7.5 step 2: soft deadline subtracts the measured worst-case match; hard deadline abandons the in-flight match between chunks; `startup_allowance` measured and asserted against `--timeout`; one checkpoint publish in the reserve. All measured on `cpu-basic` (§6 steps 2, 4). |
| A r2 PERF-SPEC-17 S failure semantics | SHOULD FIX | Fixed. §7.3: independent per-scorer `try`, skip state, and swap; one `ComputeOutcome` and log line per step name kept; oracle includes a one-scorer-fails case. |
| B r2 HFTC-SPEC-17 / A r2 consider publish cost | CONSIDER | Fixed. §6 step 4 measures one publish; §7.5 step 1 allows compute-specific thresholds. |
| B r2 HFTC-SPEC-18 K guard mechanism | CONSIDER | Fixed. §7.4: one module-level read declaration shared by the builder and the key; a recording-connection test. |
| A r2 consider: measuring machine | CONSIDER | Fixed. §6: every figure that sets nights or the reserve is measured on `cpu-basic`. |
| A r2 consider: `frame_rate` literal | CONSIDER | Fixed. §2 no longer uses `fct_match_params.frame_rate` as the rate source. |
| A r2 consider: v1 rule cited as v2 | CONSIDER | Fixed. §7.5 cites v1 `AGENTS.md:132` as adopted for v2; ADR-017 records it. |
| Owner ruling 2026-11-06 | — | §0.5: no downsampling. Lever B removed from §3, §4, §7, §8, §9, §10; §11 adds it as a non-goal. |

## 14. Dispositions (round 3)

Reviewer A = `…-spec-r3-A.md`. Reviewer B = `…-spec-r3-B.md`. Both resolved every round-2 finding. A's PERF-SPEC-22 and
B's HFTC-SPEC-19 are the same point, and so are A's PERF-SPEC-23 and B's HFTC-SPEC-20. There is no disagreement.

| ID | Severity | Disposition |
|---|---|---|
| A r3 PERF-SPEC-22 / B r3 HFTC-SPEC-19 K misses code; today's key not exact | SHOULD FIX | Fixed. Verified `pitch_control.py:100-102`, `shape_graphs.py:110`, `line_breaking.py:125`. §7.4 corrects the premise and adds a derived code digest (normalised `ast` of a declared module list, with an import-closure guard) and the library versions. No hand-kept constant. Tests: code change re-computes; docstring-only change does not. |
| A r3 PERF-SPEC-23 / B r3 HFTC-SPEC-20 check point needs chunks | SHOULD FIX | Fixed. §7.5 step 3: an optional `should_stop` keyword in all three scorers, called every N frames after velocity derivation, independent of C; an abandon signal the driver maps to deferred. §7.3 amended. Tests cover each scorer with and without C. |
| A r3 PERF-SPEC-24 cleanup on stop | SHOULD FIX | Fixed. Verified `run.py:671`, `:751-759`. §7.5 step 4 keeps today's behaviour (a stopped run skips cleanup) and states why; the stop test asserts it. |
| B r3 HFTC-SPEC-21 line_breaking key | CONSIDER | Fixed. §7.4: line_breaking's key hashes its `fct_actions` read and the four frame reads in full, plus its code digest. |

## 15. Dispositions (round 4)

Reviewer A = `…-spec-r4-A.md` (APPROVE, 2 CONSIDER). Reviewer B = `…-spec-r4-B.md` (REQUEST CHANGES, 1 SHOULD FIX + 1
CONSIDER). There is no disagreement: B's finding is in a part A did not test against the line_breaking body.

| ID | Severity | Disposition |
|---|---|---|
| B r4 HFTC-SPEC-22 line_breaking has no loop | SHOULD FIX | Fixed. Verified `line_breaking.py:60` (one `detect_line_breaking` call) and `silly_kicks/tracking/_line_breaking.py` (the action loop is inside silly-kicks). §7.5 step 3 takes option (b): line_breaking is checked between matches only, its worst match is in `worst_match`; option (a) chunking, with its parity oracle, is the plan's fallback if the measured match time is too long. §7.5 step 5 updated. |
| B r4 HFTC-SPEC-23 library list unguarded | CONSIDER | Fixed. §7.4: a guard maps every third-party import in the declared modules to its distribution. |
| A r4 PERF-SPEC-25 guard scopes | CONSIDER | Fixed. §7.4: the code guard covers `lakehouse.*`; input reads and control reads are separate declarations, and only input reads enter the key. |
| A r4 PERF-SPEC-26 uninterruptible first segment | CONSIDER | Fixed. §7.5 step 3: the pre-check segment is measured on `cpu-basic` and covered by `finish_reserve`'s safety margin. |
