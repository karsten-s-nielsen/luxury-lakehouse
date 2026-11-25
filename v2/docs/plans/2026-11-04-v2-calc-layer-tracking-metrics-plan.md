# v2 Calculation Layer — tracking-metrics first slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task,
> **inline in one session** (this project bans subagent-driven / worktree execution by default). Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** Add v2's first calculation-layer slice — three heuristic tracking computes feeding one gold mart — as a single clean forward pass, and establish the reusable compute-phase pattern.

**Architecture:** A new `lakehouse.compute` phase runs inside a run as a **layer-driven build/compute alternation** (spec D8/D9 amendment): `resolve → dbt(select=None: full — dims + layer-0 input views) → compute(layer 1: spadl_actions, pitch_control, shape_graphs → bronze) → dbt(+fct_actions) → compute(layer 2: line_breaking, reads fct_actions → bronze) → dbt(+fct_tracking_metrics) → maintain`. Each compute builds silly-kicks' canonical frame (coords → 0-105/0-68, goalkeeper sourced from the roster, velocity via the preprocess seam), calls a silly-kicks scorer, and writes one bronze table through the existing bronze writer; dbt views (`ref()`) are the forward lineage — no compute reads another compute's bronze directly. A code-level AST gate forbids any HF dependency; HF publishing is a defined-but-unbuilt multi-dataset sink.

**Tech Stack:** Python 3.12, DuckLake/DuckDB, dbt-duckdb (views, contracts), pyarrow, silly-kicks (SPADL/pitch-control/line-breaking/shape-graph scorers), Taipy (`lakehouse.ui`), pytest.

**Spec:** `v2/docs/specs/2026-11-04-v2-calc-layer-tracking-metrics-design.md` (owner-approved 2026-11-04; read it with this plan).

**Review status (2026-11-04):** A/B plan review complete — Reviewer A APPROVE (round 2), Reviewer B APPROVE (round 3;
the lone SHOULD FIX, CALC-PLAN-09, verified resolved). Reports `…-plan-{A,B}.md`, `…-plan-r2-A.md`, `…-plan-r3-B.md`.
No open findings. **Awaiting owner approval; no code until then.** On approval: implement inline via
superpowers:executing-plans on branch `feat/v2-calc-tracking-metrics`, one owner-gated cycle commit (Task 11).

## Amendment (2026-11-04): team-enriched frames (spec D7)

Implementing Task 4 found that the silly-kicks scorers need team membership + ball + attacking-team per frame, which
v2's positions-only tracking core does not surface (spec D7). Tasks 1–3 landed green and stand. This amendment adds
**Task 3b** (surface `team_key` into the tracking frames + a `fct_tracking_ball` view + a per-frame possession
signal) before the computes, and updates the compute tasks (4–6) to read the enriched inputs. The data exists in
bronze (`*_tracking_players.team_key`, `*_tracking_frames` ball, provider possession fields); it is surfaced via dbt
views, not re-ingested. This plan amendment rides the spec D7 amendment through an A/B re-review + owner approval
before Task 3b or the computes are built.

## Amendment (2026-11-04): silly-kicks compute contract + SPADL actions layer (spec D8 + D9)

A spike + a silly-kicks session (4.139.1) established the scorers' real contract, correcting ruling R4 and revealing
that line_breaking needs a SPADL actions layer v2 does not have. The owner chose to **build the SPADL layer** and to
keep it in **one mega-cycle** (2026-11-04). This amendment:

- **D8 — compute integration contract.** A shared **canonical frame builder** (new **Task 3c**) turns the layer-0
  views into silly-kicks' `TRACKING_FRAMES_COLUMNS`: coords → the SPADL 0-105/0-68 pitch frame (per-match pitch
  size), `team_key` → `team_id`, ball row, and **`is_goalkeeper` sourced** from the per-provider roster via
  `silly_kicks.keeper_identity` (native) with `derive_goalkeepers` (derived) as fallback — never hand-rolled
  (corrects R4). `fct_tracking_frames` gains `provider` (R6). pitch_control (Task 4) and shape_graphs (Task 6) consume
  the builder; the view itself stays positions + team only.
- **D9 — SPADL actions layer + line_breaking.** New **Task 4b** adds a per-provider events→SPADL compute
  (**gradientsports + idsse + skillcorner**, the three dual-modality providers; converter seam extends to the
  event-only providers later) writing
  `spadl_actions` bronze → `stg__spadl_actions` → `fct_actions` (contract). **Task 5 (line_breaking) is rewritten** to
  an actions × frames compute that reads `fct_actions` + the frame builder; its old event/360 view creation is
  dropped (the StatsBomb-360 view too — StatsBomb has no tracking). The run seam (Task 1) **generalises to two
  compute layers / three view-builds**: `dbt(L0) → compute(L1: spadl_actions, pitch_control, shape_graphs) →
  dbt(L1 views incl. fct_actions) → compute(L2: line_breaking) → dbt(mart) → maintain`.
- **Known risk:** two of the three adapters are higher-risk — idsse (flattened DFL bronze → silly-kicks'
  kloppy-oriented sportec converter) and skillcorner (inference-based `spadl.skillcorner` over
  `skillcorner_dynamic_events`). gradientsports (direct converter) is built and proven first. Dropping either
  higher-risk provider from `spadl_actions` + line_breaking would be a cut and returns to the owner.

This rides the spec D8/D9 amendment through an A/B amendment review + owner approval before Task 3c, 4b, the computes,
or the line_breaking rewrite are built. Tasks 1–3b (green) stand; Task 1's seam is revised by this amendment.

## Branch & commit discipline

- **One feature branch** off `main`: `feat/v2-calc-tracking-metrics`. All tasks land on it. No worktree, no parallel branch.
- **One commit per cycle.** The per-task **Step 4 "verify"** gates stand, but tasks do **not** commit individually — several are not independently valid (Task 1's seam predates the computes; Tasks 4–6's bronze predates any reader; Task 7's mart refs Tasks 4–6). The whole slice is committed **once**, in Task 11, after all tasks are green.
- **The commit is owner-gated.** When the suite is green, stop at "ready to commit", show the diff/file list, and wait for the owner's explicit yes. No commit, push, PR, or merge without it.

## Global Constraints

- v2 is DuckLake/DuckDB — **no Spark, no `applyInPandas`**; each compute is a per-match Python/pandas pass.
- dbt gold models are **views** with an enforced contract; only `dim_*`, `*_links`, `ops_*`, `lineage_*` may be native tables (spec §9, §11.1, §11.6). Computed rows live in a **bronze table** written by `lakehouse.bronze.writer`, never a dbt-materialised table.
- **Velocity, goalkeeper, and pitch-frame coords are compute-pass concerns, via silly-kicks seams — never in SQL and never hand-rolled** (spec D8). Velocity via the preprocess seam (`smooth_frames` then `derive_velocities`, `PreprocessConfig.default()`); coordinates transformed to the SPADL **0-105 × 0-68** pitch frame (per-match pitch size) before the goalkeeper derive and pitch-control; **`is_goalkeeper` sourced** from the per-provider roster via `silly_kicks.keeper_identity` (native) with `silly_kicks.tracking._gk_identification.derive_goalkeepers` (derived) as the only fallback. The layer-0 views carry positions + team + ball only. A hand-rolled goalkeeper heuristic is an anti-goal.
- Bronze writes go through `write_bronze_file(...) → store_written_file(...) → register_match_files/replace_match_files(...)`; audit columns `_ingested_at`, `_run_id`, `_source_hash` are mandatory; one file per match per table; a bronze file is never empty.
- silly-kicks is never edited here; the v1 `src/analytics/*.py` wrappers are reference, not import targets.
- **Nothing may depend on a published HF dataset or model.** No `import-*` steps. HF publish is an optional sink nothing reads.
- Python 3.12; ruff, ruff format, pyright (whole v2 tree), import-linter, 120-column lines. The six v2 CI checks must pass.
- **The run seam is a layer-driven build/compute alternation (spec D8/D9).** Three dbt builds, two compute stages: `dbt(select=None)` [**full** first build — dims + layer-0 input views + the still-empty staging/mart views; `select=None` per landed ruling R1, because dims are not ancestors of the compute inputs so a selective first build would skip them] → `compute(layer 1)` → `dbt(select=COMPUTE_L1_SELECT="+fct_actions")` → `compute(layer 2)` → `dbt(select=COMPUTE_MART_SELECT="+fct_tracking_metrics")`. The `+`-prefixed selectors rebuild their upstream staging views, not only the leaf (a bare leaf select leaves the `lake_table_present` staging views on their empty branch — the permanently-empty-mart trap, spec D2/CALC-SPEC-11). A compute step declares `layer: int` (1 or 2); layer-1 steps read only layer-0 views, the one layer-2 step (line_breaking) reads `fct_actions`. The **middle** build is `+fct_actions` only — line_breaking (layer 2) reads only `fct_actions` (+ its upstream `stg__spadl_actions`); the pitch_control + shape_graphs staging views are not needed until the mart, so they build in the **final** `+fct_tracking_metrics` with `stg__line_breaking_results` and the mart (CALC-A2-01/02: no `tracking_metric_stage1` tag, no model built by two of the three builds). `fct_tracking_frames`/`fct_tracking_ball` keep the `compute_input` tag as layer-0 metadata for the cards/humans (R1: with the full first build the tag is not a build selector).

## Review Focus

- **A compute reads a dbt gold view that the first build did not create** (wrong `select`) → the compute must fail loud with the missing-relation name, not read an empty set. (Task 1 test.)
- **A compute raises on one match of many** → that **match** is recorded to `ops_errors` (`kind="compute_failed"`, the failing `match_key` carried in the record `message`, `native=None`), the other matches keep their rows, the run ends `failed`; "no frames for a match" (legitimate empty) is distinguished from "compute failed". (Task 1 + each compute task — per-match, spec D6.)
- **The second dbt build fails** → no partial mart (it is a view); the prior committed catalog version stands; the run ends `failed`. (Task 11 e2e.)
- **A future edit makes a compute/mart/publish module import the HF client or read an HF URL** → the AST gate fails. Watched red first. (Task 2.)
- **A layer-1 bronze table is absent on a fresh lake** → its staging view is empty-but-typed and the mart builds (empty), no hard error; once the compute writes, the next build picks up rows. (Task 7 + each compute task.)
- **A match has no roster goalkeeper** (no position field) → the frame builder falls back to `derive_goalkeepers` and stamps `is_goalkeeper_source="derived"`; a match with a roster GK is `"native"`; **neither path hand-rolls GK**, and a frame that reaches a scorer with no `is_goalkeeper` column is a loud failure, not a silent zero. (Task 3c.)
- **A compute scores on coordinates still in centred provider metres** (transform skipped) → `derive_goalkeepers`/`compute_pitch_control` must reject out-of-range coords loudly; the frame builder asserts 0-105/0-68 before the scorer. (Task 3c + Task 4.)
- **A SPADL action and a tracking frame are mis-aligned in time** (period-relative `time_seconds` vs `frame_id`/`frame_rate`) → line_breaking records the unmatched action rather than scoring it against the wrong frame; alignment is never guessed. (Task 5.)
- **A higher-risk adapter proves disproportionate** (idsse kloppy-oriented DFL, or skillcorner inference-based) → dropping that one provider from `spadl_actions` + line_breaking is a scope cut and returns to the owner; it is not taken silently, and gradientsports (direct) is built and proven first. (Task 4b.)
- **The layer-2 build runs before the layer-1 compute wrote `spadl_actions`** (wrong stage order) → `fct_actions` is empty-but-typed and line_breaking reads an empty action set; the seam must build `fct_actions` only after the layer-1 compute. (Task 1 + Task 4b.)

---

### Task 1: Compute-phase seam in a run

> **Revised by the D8/D9 amendment (2026-11-04).** Task 1 landed green as a **two-build** seam
> (`dbt(COMPUTE_INPUT_SELECT) → compute → dbt(COMPUTE_MART_SELECT)`). D9 generalises it to a **two-compute-layer,
> three-build** seam because line_breaking (layer 2) reads `fct_actions`, a view over another compute's bronze. The
> deltas below (a `layer` field on `ComputeStep`, a third selector, layer-ordered `run_compute`, a three-build
> `finish`) are applied in this cycle; the landed tests are updated, watched fail, then pass. Nothing is committed yet.

**Files:**
- Create: `v2/src/lakehouse/compute/__init__.py`, `v2/src/lakehouse/compute/base.py`
- Modify: `v2/src/lakehouse/runs/run.py` (`_Run.finish()`; `run_ingestion`/`reparse_matches` signatures)
- Test: `v2/tests/unit/test_compute_seam.py`

**Interfaces:**
- Produces:
  - `class ComputeStep(Protocol)` with `name: str`, `output_table: str`, `layer: int` (1 reads layer-0 views; 2 reads a layer-1 output view — spec D9), `input_select: str` (the dbt select for this step's input view(s), tagged `compute_input`), and `__call__(cat: WorkingCatalog, *, run_id: str, caps: ResourceCaps, now: Callable[[], datetime], error: ErrorSink) -> ComputeOutcome`. The step catches **per match**, calls `error(kind="compute_failed", message=f"{name}: match_key={match_key} failed: {…}", native=None)` on a failing match, continues the remaining matches, and returns. (CALC-PLAN-09: the failing `match_key` is a canonical `bigint`, so it is carried in the str `message`, not in `native` — `ErrorSink.native: str | None` writes `ops_errors.native_match_id`, the provider's native-id string, not the canonical surrogate; passing a bigint there is a pyright failure and the wrong column.)
  - `@dataclass(frozen=True) class ComputeOutcome: step: str; matches: int; rows: int; empty_matches: int; failed_matches: int`.
  - `@dataclass(frozen=True) class ComputeResult: outcomes: list[ComputeOutcome]`.
  - `run_compute(cat, *, run_id, caps, now, steps: Sequence[ComputeStep], layer: int, error: ErrorSink) -> ComputeResult` — runs **the steps whose `step.layer == layer`**; a step that itself raises (not a per-match failure) is recorded via `error(kind="compute_failed", …)` and the remaining steps of that layer still run (mirror `_Run._module`, run.py:432, §12.3). Any `failed_matches > 0` or step-level raise sets the run failed. `finish()` calls it once per layer, with a dbt build between the layers (see below).
  - Build 1 is the existing **full** build (`select=None`, landed ruling R1 — dims are not ancestors of the compute inputs). Two new selector constants: `COMPUTE_L1_SELECT: str = "+fct_actions"` (the only layer-1 output view a layer-2 step reads — line_breaking reads `fct_actions`; `+` pulls in its upstream `stg__spadl_actions`); `COMPUTE_MART_SELECT: str = "+fct_tracking_metrics"` (builds the pitch_control + shape_graphs + line_breaking staging views + the mart — none built by the middle selector, so no model is built by two of the three builds, CALC-A2-01/02). Both selectors use the `+` upstream prefix so `lake_table_present` staging views rebuild off their populated bronze, never their empty branch (spec D2). `COMPUTE_MART_SELECT` is already landed; `COMPUTE_L1_SELECT` is added.
- Consumes: `WorkingCatalog`, `ResourceCaps`, `ErrorSink` (`lakehouse.runs._common:40`), `DbtStep`/`run_dbt` (run.py).

- [ ] **Step 1 — failing test** `v2/tests/unit/test_compute_seam.py`:

```python
def test_finish_runs_layered_build_compute_alternation(order_recording_run):
    # spec D9: two compute layers, three view-builds
    assert order_recording_run.calls == [
        "resolve",
        ("dbt", None),
        ("compute", 1),  # build 1 is the full build (R1); layer-1 compute
        ("dbt", COMPUTE_L1_SELECT),
        ("compute", 2),  # +fct_actions; layer-2 compute (line_breaking)
        ("dbt", COMPUTE_MART_SELECT),
        "maintain",
    ]


def test_layer2_build_runs_after_layer1_compute(order_recording_run):
    # fct_actions (COMPUTE_L1_SELECT) is built only after the layer-1 compute wrote spadl_actions bronze
    calls = order_recording_run.calls
    assert calls.index(("compute", 1)) < calls.index(("dbt", COMPUTE_L1_SELECT)) < calls.index(("compute", 2))


def test_step_level_failure_sets_run_failed_but_runs_remaining_steps(fake_steps):
    # step A raises; step B succeeds; status == "failed"; B's outcome present; one ops_errors compute_failed for A
    ...


def test_per_match_failure_keeps_sibling_matches(two_match_step):  # spec D6
    # match 1 raises inside the step, match 2 succeeds; match 2's rows are present;
    # ops_errors has one compute_failed row whose MESSAGE carries match 1's match_key and whose native_match_id is
    # NULL (native=None); outcome.failed_matches == 1; status "failed"
    ...


def test_no_frames_is_not_a_failure(empty_input_step):
    # a match with no frames writes nothing, counts empty_matches, run stays "ok"
    ...


def test_missing_input_view_fails_loud(step_reading_unbuilt_view):
    # first build did not create the view -> the step raises a missing-relation error (recorded), not empty read
    ...


def test_empty_compute_skips_the_phase_and_keeps_single_build(no_steps_run):
    # compute=() -> one dbt(select=None) build, no compute span, run unchanged from today
    ...
```

- [ ] **Step 2 — run, verify it fails** (`uv run pytest v2/tests/unit/test_compute_seam.py -v` → FAIL, undefined).
- [ ] **Step 3 — implement** `ComputeStep`/`ComputeOutcome`/`ComputeResult`/`run_compute` in `compute/base.py`; wire `_Run.finish()` to the layered alternation `dbt(select=None) → run_compute(layer=1) → dbt(select=COMPUTE_L1_SELECT) → run_compute(layer=2) → dbt(select=COMPUTE_MART_SELECT) → maintenance`, each under `self.lease.confirm()` and a `step_span("lakehouse.run.compute")`. A layer with no registered step is a no-op that still runs its following build (so a single layer-1 step with no layer-2 step still rebuilds the mart). Add `compute: Sequence[ComputeStep] = ()` to `run_ingestion`/`reparse_matches` (empty default → **both** layers empty → `finish` collapses to today's single `dbt(select=None)` build, no compute span, so non-calc runs are byte-for-byte unchanged). `run_compute` uses `self._error` as the `ErrorSink`.
- [ ] **Step 4 — run, verify it passes.**

> D6: `run_compute` and each step record `compute_failed` to `ops_errors` and set `failed_run`; nothing is swallowed. The second dbt build failing is the existing dbt-failure path in `finish`; the e2e in Task 11 asserts the mart is absent/empty and the run `failed`.

### Task 2: No-HF code-level gate (red-first)

**Files:** Test `v2/tests/unit/test_no_hf_dependency.py`.

**Interfaces:** an AST scan over `lakehouse.compute.*`, `lakehouse.cards.*`, `lakehouse.publish.*` and the mart modules, modelled on `v2/tests/unit/test_jobs_token_confinement.py`.

- [ ] **Step 1 — write the test**: `test_no_compute_or_publish_module_imports_hf()` — AST-parse every module under the scanned packages; assert none imports `huggingface_hub` / `lakehouse.storage.hf` or contains a string matching `^hf://` or `huggingface.co`. `test_gate_catches_a_violation()` — feed the scanner a fixture module source that imports `huggingface_hub`; assert it is reported.
- [ ] **Step 2 — prove red first**: temporarily add `import huggingface_hub` to a scratch module under the scanned path; run `uv run pytest v2/tests/unit/test_no_hf_dependency.py -v`; **verify it FAILS**; remove the scratch import.
- [ ] **Step 3 — implement** the scanner so the real tree passes.
- [ ] **Step 4 — run, verify it passes** on the clean tree.

### Task 3: Layer-0 tracking-frames input view

> **Superseded in part by the D9 amendment.** As landed, Task 3 shipped **only `fct_tracking_frames`** (ruling R5
> deferred line_breaking's inputs). D9 then replaced line_breaking's event/360 views with the SPADL `fct_actions`
> layer (Task 4b) — so **no event or 360 input view is built in this cycle at all**, here or in Task 5. The
> `test_event_360_input_views_present_and_tagged` test below is dropped; `fct_tracking_frames` stands as landed and
> gains `team_key`/`canonical_team_key` (Task 3b) and `provider` (Task 3c).

**Files:**
- Create: `v2/dbt/models/gold/fct_tracking_frames.sql` (+ contract in `v2/dbt/models/gold/<schema>.yml`), tagged `compute_input`.
- Test: `v2/tests/unit/test_tracking_frames_view.py`, extend `v2/tests/unit/test_dbt_dag.py`.

**Interfaces:**
- Produces (as landed): `fct_tracking_frames` columns `canonical_match_key bigint, canonical_player_key bigint, period tinyint, frame integer, x_mm integer, y_mm integer, is_visible boolean, _visibility varchar` — **positions only, no velocity, no is_goalkeeper**; over `tracking_cross_provider`; tagged `compute_input`. (`team_key`/`canonical_team_key` added by Task 3b; `provider` by Task 3c.)
- Consumes: `tracking_cross_provider`.

- [ ] **Step 1 — failing test**: `test_fct_tracking_frames_is_a_view_positions_only()` — parse the dbt manifest (reuse `parse_manifest`, `lakehouse.runs.dbt:148`); assert the node is `materialized: view`, has an enforced contract, has **no** column whose name contains `velocity`/`vx`/`vy`, and is tagged `compute_input`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the SQL + `schema.yml` contract.
- [ ] **Step 4 — verify pass**; `uv run pytest v2/tests/unit/test_dbt_materialisation.py -v` stays green (no new native table).

### Task 3b: team + ball + possession enrichment of the tracking frames (spec D7)

**Files:**
- Modify: `v2/dbt/models/staging/{idsse,skillcorner,gradientsports}/stg_*__tracking_players.sql` (select the
  existing `team_key` extra column, both branches; metrica is not a v2 provider), `v2/dbt/models/gold/tracking_cross_provider.sql` +
  `fct_tracking_frames.sql` (+ their contracts in `_gold__models.yml`) to add `team_key` + `canonical_team_key`.
- Create: `v2/dbt/models/gold/fct_tracking_ball.sql` (+ contract, tag `compute_input`) over `*_tracking_frames`:
  per `(canonical_match_key, period, frame)` → `ball_x_mm, ball_y_mm, ball_detected, possession_team_key` (canonical,
  null where the provider gives no possession signal).
- Test: `v2/tests/unit/test_tracking_frames_view.py` (extend: `team_key`/`canonical_team_key` present),
  `v2/tests/unit/test_tracking_ball_view.py` (new).

**Interfaces:**
- Produces: `fct_tracking_frames` now `(canonical_match_key, canonical_player_key, team_key, canonical_team_key,
  period, frame, x_mm, y_mm, is_visible, _visibility)` — still no velocity/goalkeeper; `fct_tracking_ball` as above.
- Consumes: the per-provider `team_key` extra bronze column + `*_tracking_frames` ball + provider possession fields.

- [ ] **Step 1 — failing tests**: `test_fct_tracking_frames_carries_team()` (the two team columns exist, typed);
  `test_fct_tracking_ball_is_a_contracted_view_with_possession()` (view, contract, the ball + nullable
  `possession_team_key`); update `test_dbt_materialisation::test_gold_contracts_enforced` gold set
  (+`fct_tracking_ball`).
- [ ] **Step 2 — run, verify fail.**
- [ ] **Step 3 — implement** the staging + gold SQL + contracts; resolve `canonical_team_key` /
  `possession_team_key` via `ops_canonical_pointers`; normalise each provider's possession field (idsse
  `ball_possession` 1/2 → home/guest team key; skillcorner `possession.player_id` / `possession.group` on the
  tracking frame → team via the match roster (NOT `team_in_possession`/`attacking_side`, which are phase/event-grain);
  gradientsports possession event; null where absent). A provider's possession normalisation is builder detail; it
  must never invent a value.
- [ ] **Step 4 — verify pass** (+ `test_dbt_materialisation.py`, no new native table).

### Task 3c: canonical silly-kicks frame builder + goalkeeper resolution (spec D8)

> **Implementation reconciliation (2026-11-05; A/B impl review, owner "Reconciled").** As shipped, Task 3c differs from
> the `keeper_identity` / shared-`to_spadl_ltr` contract described below — see the spec's "D8 implementation
> reconciliation". In short: `build_match_frame` dispatches by `provider` to
> `silly_kicks.tracking.<provider>.convert_to_frames` (coordinate transform + per-period direction + derive-GK fallback
> bundled; no hand-rolled `to_spadl_ltr`, no private import); and `is_goalkeeper` comes from the rostered-keeper SET
> (`fct_roster_gk` — every GK-position player per team, including the idsse `Substitution@PlayerIn='TW'` sub keepers)
> flagged by `resolve_roster_goalkeepers`, substitution-safe, NOT `keeper_identity` (which needs the `actions` table,
> built after these frames in the DAG). Adds two layer-0 views `fct_match_params` + `fct_roster_gk` (ruling R9). The
> `derive_goalkeepers` fallback (a team with no roster GK) and the "never hand-rolled" anti-goal stand.

**Files:**
- Create: `v2/src/lakehouse/compute/frames.py`.
- Modify: `v2/dbt/models/gold/fct_tracking_frames.sql` + `tracking_cross_provider.sql` (+ their contracts in
  `_gold__models.yml`) and the 3 `stg_*__tracking_players.sql` to carry a `provider` passthrough column (D8 / R6),
  like `team_key`.
- Test: `v2/tests/unit/test_frame_builder.py`; extend `v2/tests/unit/test_tracking_frames_view.py` (`provider` present).

**Interfaces:**
- Produces:
  - `build_match_frame(cat: WorkingCatalog, match_key: int, *, provider: str) -> pandas.DataFrame` — the shared frame
    builder. Reads `fct_tracking_frames` + `fct_tracking_ball` for the one match and returns silly-kicks'
    `TRACKING_FRAMES_COLUMNS` (dtypes load-bearing: `game_id int64, period_id int64, frame_id int64, player_id Int64`
    [NA on the ball row], `team_id category, is_ball bool, is_goalkeeper bool, x/y float32`, plus
    `is_goalkeeper_source` in `{"native","derived"}`). It (a) transforms `x_mm`/`y_mm` → the SPADL **0-105/0-68**
    pitch frame using the per-match pitch size (`provider` selects the source: idsse `environment_pitch_x_mm/y_mm`,
    skillcorner `pitch_length/width`, gradientsports `stadium_pitches[].length/width`) via the silly-kicks
    `spadl.orientation` / `to_spadl_ltr` seam; (b) maps `team_key → team_id` via `silly_kicks.id_compat` (never raw
    `==` on ids); (c) adds the ball row (`is_ball=True`, `player_id` NA); (d) resolves `is_goalkeeper` — **native**
    from the provider roster (`silly_kicks.keeper_identity.resolve_keeper_identities` +
    `apply_keeper_identities_to_frames`; roster source per provider: `idsse_persons.playing_position`,
    `skillcorner_players.player_role_*`, `gradientsports_rosters.position_group_type`), **derived** fallback via
    `silly_kicks.tracking._gk_identification.derive_goalkeepers` (ADR-007; a PRIVATE import — record the pin in a
    module docstring) where no roster position exists. Asserts coords are inside 0-105/0-68 before returning.
  - `resolve_attacking_direction(frame, *, home_team_key, home_start_left) -> int` — +1/-1 via silly-kicks
    `spadl.orientation.play_left_to_right`, for the shape_graphs + line_breaking scorers (pitch_control needs none).
- Consumes: `fct_tracking_frames` + `fct_tracking_ball` (Task 3b), the per-provider roster + pitch-size bronze via
  `source()`, and `silly_kicks.{keeper_identity, tracking._gk_identification, spadl.orientation, id_compat,
  tracking.preprocess}`. The exact silly-kicks call signatures are confirmed against 4.139.1 at build time and pinned.

- [ ] **Step 1 — failing tests**:
  - `test_build_match_frame_is_tracking_frames_columns()` — the returned frame has exactly `TRACKING_FRAMES_COLUMNS`
    with the load-bearing dtypes, one ball row (`is_ball`, `player_id` NA), and all `x`/`y` within [0,105]/[0,68].
  - `test_goalkeeper_native_from_roster()` — a fixture match with a roster GK position → that player's `is_goalkeeper`
    is True and `is_goalkeeper_source == "native"`; no hand-rolled pick.
  - `test_goalkeeper_derived_fallback()` — a fixture with no roster position → `derive_goalkeepers` sets exactly one
    GK per team, `is_goalkeeper_source == "derived"`.
  - `test_untransformed_coords_raise()` — centred-metre coords (not transformed) make the builder raise, not silently
    pass out-of-grid coordinates to a scorer.
  - `test_built_frame_drives_the_scorers()` — the de-risking round-trip: `smooth_frames → derive_velocities →
    compute_pitch_control(frame, attacking_team_id)` and `add_shape_graph(...)` both run on the built frame without
    error and return a surface / shape graph (proves the contract end-to-end before the compute tasks depend on it).
  - `test_fct_tracking_frames_has_provider()` — the view carries `provider`, typed, contract-enforced.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** `frames.py` + the `provider` column; confirm the silly-kicks signatures against 4.139.1
  and pin them.
- [ ] **Step 4 — verify pass** (+ `test_tracking_frames_view.py`, `test_dbt_materialisation.py`).

### Task 4: pitch_control compute + bronze + staging view

**Files:** Create `v2/src/lakehouse/compute/pitch_control.py`, `v2/dbt/models/staging/stg__pitch_control_values.sql` (built in the final `+fct_tracking_metrics` build with the mart — it has no layer-2 consumer, CALC-A2-01/02); Test `v2/tests/unit/test_pitch_control_compute.py`.

**Interfaces:** a `ComputeStep` `PITCH_CONTROL` — `name="pitch_control"`, `output_table="pitch_control_values"`, `layer=1`, `input_select` covering `fct_tracking_frames` + `fct_tracking_ball`. Per match it calls `build_match_frame(...)` (Task 3c) to get the canonical goalkeeper-resolved, pitch-framed frame, runs `smooth_frames → derive_velocities` (`PreprocessConfig.default()`), then `compute_pitch_control(frame, attacking_team_id, method="spearman")` — `attacking_team_id` from `fct_tracking_ball.possession_team_key` where present, else the documented both-teams fallback (spec D7; a compute never invents possession). Writes bronze `pitch_control_values` (one file per match) via the bronze-writer chain; schema `match_key bigint, period tinyint, frame integer, <scorer columns>` + audit columns; `source_hash` = a deterministic hash of the match's frame input (`lakehouse.hashing`). Consumes Task 3c's `build_match_frame` + the silly-kicks pitch-control scorer; the goalkeeper flag it needs comes from the builder, not a frame-view column (spec D8, corrects R4).

- [ ] **Step 1 — failing test**: `test_pitch_control_writes_one_bronze_file_per_match()` (two-match fixture; both present, one file each, audit columns set, `_run_id==run_id`); `test_skip_guard_skips_present_match()`; `test_empty_match_writes_nothing_counts_empty()`; `test_one_bad_match_records_compute_failed_keeps_others()` (spec D6 per-match).
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the per-match read → velocity preprocess → scorer → arrow → write/register, memory-bounded (T12).
- [ ] **Step 4 — verify pass.**

### Task 4b: SPADL actions layer — events → SPADL (gradientsports + idsse + skillcorner) (spec D9)

**Files:**
- Create: `v2/src/lakehouse/compute/spadl_actions.py` (per-provider adapter + converter driver),
  `v2/dbt/models/staging/stg__spadl_actions.sql`, `v2/dbt/models/gold/fct_actions.sql` (+ contract in
  `_gold__models.yml`).
- Test: `v2/tests/unit/test_spadl_actions_compute.py`, `v2/tests/unit/test_fct_actions_view.py`.

**Interfaces:**
- Produces: a `ComputeStep` per event provider — `SPADL_ACTIONS_GRADIENTSPORTS`, `SPADL_ACTIONS_IDSSE`,
  `SPADL_ACTIONS_SKILLCORNER` (owner widened the scope to all three dual-modality providers, 2026-11-04) —
  `name="spadl_actions:<provider>"`, `output_table="spadl_actions"`, `layer=1`, `input_select` covering that
  provider's event bronze view (gradientsports `gradientsports_events`, idsse `idsse_events`, skillcorner
  `skillcorner_dynamic_events`). Each turns the provider's **flattened** event bronze into SPADL actions through a
  per-provider **adapter** (flattened v2 bronze → the silly-kicks converter's documented input) +
  `silly_kicks.spadl.<provider>.convert_to_actions(...)` + `silly_kicks.spadl.validate_spadl(...)`, and writes bronze
  `spadl_actions` (one file per match): `SPADL_COLUMNS` (`period_id`, period-relative `time_seconds`, `start_x/y`,
  `end_x/y` in 0-105/0-68, `type_id`/`result_id`/`bodypart_id`, `original_event_id`) + `canonical_match_key`,
  `canonical_player_key`, `canonical_team_key` + audit columns. Keyed per `(canonical_match_key, period_id,
  action_id)`. gradientsports + idsse use **direct** converters; **skillcorner uses the inference-based
  `spadl.skillcorner` / `_skillcorner_inference.py`** over its `skillcorner_dynamic_events` model (a different
  conversion path — a named risk, spec D9). The adapter is a **provider-pluggable seam** (`statsbomb`/`wyscout`
  extend later for future event computes; not built this cycle).
- A gold view `fct_actions` (contract enforced), named directly by `COMPUTE_L1_SELECT="+fct_actions"` (which also
  builds its upstream `stg__spadl_actions`), over `stg__spadl_actions` (`lake_table_present`-guarded). This is the one
  layer-1 output view line_breaking reads; no `tracking_metric_stage1` tag (CALC-A2-01/02).
- Consumes: `gradientsports_events` + `idsse_events` + `skillcorner_dynamic_events` bronze; `silly_kicks.spadl.*`; `lakehouse.bronze.writer`.

> **Build order: gradientsports first** (direct converter + `home_team_start_left`, the cleanest), prove it green,
> then idsse and skillcorner. Two adapters are higher-risk (spec D9): idsse (flattened DFL bronze → kloppy-oriented
> `spadl.sportec`) and skillcorner (inference-based `spadl.skillcorner` over `skillcorner_dynamic_events`). If either
> proves disproportionate this cycle, dropping that one provider from `spadl_actions` + line_breaking is a **cut** and
> returns to the owner — not taken silently; the pluggable seam leaves the other two intact.

- [ ] **Step 1 — failing tests**: `test_gradientsports_events_convert_to_spadl()` (a fixture match → non-empty
  `spadl_actions`, every row passing `validate_spadl`, coords in 0-105/0-68, `time_seconds` period-relative);
  `test_spadl_actions_writes_one_bronze_file_per_match()` (audit columns, `_run_id`); `test_adapter_emits_no_action_a_
  provider_did_not_encode()` (anti-goal: a synthetic event with no SPADL mapping yields no row, not a sentinel);
  `test_idsse_events_convert_to_spadl()`; `test_skillcorner_dynamic_events_convert_to_spadl()` (the inference path);
  `test_fct_actions_is_a_contracted_view()` (view, contract enforced, empty-but-typed on a fresh lake; built by
  `+fct_actions`).
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the gradientsports adapter + converter driver first (verify green), then idsse and
  skillcorner; the staging + gold views. Confirm each converter's input contract against silly-kicks 4.139.1 and pin
  it.
- [ ] **Step 4 — verify pass** (+ `test_dbt_materialisation.py`, no new native table).

### Task 5: line_breaking compute (actions × frames) + bronze + staging view (spec D9)

**Files:** Create `v2/src/lakehouse/compute/line_breaking.py`, `v2/dbt/models/staging/stg__line_breaking_results.sql`; Test `v2/tests/unit/test_line_breaking_compute.py`. (No event/360 views here — D9 drops them; line_breaking reads `fct_actions` from Task 4b.)

**Interfaces:** a `ComputeStep` `LINE_BREAKING` — `name="line_breaking"`, `output_table="line_breaking_results"`, **`layer=2`**, `input_select` covering **`fct_actions` + `fct_tracking_frames` + `fct_tracking_ball`**. Per match it builds the canonical frame (`build_match_frame`, Task 3c), reads that match's `fct_actions`, resolves the attacking direction (`resolve_attacking_direction`, using `home_team_start_left` where the provider supplies it — gradientsports does), aligns actions to frames on `(period_id, time)` (SPADL `time_seconds` is period-relative; the frame axis is `frame_id`/`frame_rate`), calls the silly-kicks line-breaking scorer (`detect_line_breaking` / `add_line_break`), and writes bronze `line_breaking_results` (per match) + audit columns. An action that cannot be aligned to a frame is recorded, never scored against the wrong frame (spec D9 anti-goal). Matches with actions but no tracking (statsbomb/wyscout) produce no line_breaking rows this cycle — only the dual-modality providers (gradientsports, idsse, skillcorner) feed it.

- [ ] **Step 1 — failing test**: `test_line_breaking_scores_actions_against_frames()` (a gradientsports fixture with actions + frames → per-pass line-break labels); `test_unalignable_action_is_recorded_not_mis_scored()`; `test_empty_match_writes_nothing()`; `test_one_bad_match_keeps_others()` (spec D6).
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the layer-2 per-match pass: frame build → read `fct_actions` → direction → align → score → write.
- [ ] **Step 4 — verify pass.**

### Task 6: shape_graphs compute + bronze + staging view

**Files:** Create `v2/src/lakehouse/compute/shape_graphs.py`, `v2/dbt/models/staging/stg__shape_graph_labels.sql` (built in the final `+fct_tracking_metrics` build with the mart — it has no layer-2 consumer, CALC-A2-01/02); Test `v2/tests/unit/test_shape_graphs_compute.py`.

**Interfaces:** `ComputeStep` `SHAPE_GRAPHS`, `output_table="shape_graph_labels"`, `layer=1`, `input_select` covering `fct_tracking_frames` + `fct_tracking_ball`. Per match it calls `build_match_frame(...)` (Task 3c) for the goalkeeper-resolved, pitch-framed frame, resolves the attacking direction (`resolve_attacking_direction`, Task 3c — `add_shape_graph` needs it to label vertical levels), and calls the silly-kicks shape-graph scorer (`add_shape_graph` / `compute_shape_graph`), which builds the Delaunay graph over the **outfield** players (it drops the GK via `is_goalkeeper`, so the builder's flag is required). Bronze `shape_graph_labels` (per match): per-player position labels + time-in-position, audit columns. Consumes Task 3c's `build_match_frame` + the silly-kicks shape-graph scorer.

- [ ] **Step 1 — failing test**: `test_shape_graphs_writes_per_match()`; `test_position_labels_are_5x5()`; `test_empty_match_writes_nothing()`; `test_one_bad_match_keeps_others()`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement.**
- [ ] **Step 4 — verify pass.**

### Task 7: Gold mart `fct_tracking_metrics` + registry wiring

**Files:**
- Create: `v2/dbt/models/gold/fct_tracking_metrics.sql` (+ contract in `<schema>.yml`, tag `tracking_metric`), `v2/src/lakehouse/compute/registry.py` (`DEFAULT_COMPUTE_STEPS = (SPADL_ACTIONS_GRADIENTSPORTS, SPADL_ACTIONS_IDSSE, SPADL_ACTIONS_SKILLCORNER, PITCH_CONTROL, SHAPE_GRAPHS, LINE_BREAKING)` — tuple order is irrelevant; the seam groups by `step.layer`, so the five layer-1 steps run first, then line_breaking at layer 2, spec D9).
- Modify: `v2/src/lakehouse/runs/run.py` — ~~flip the `run_ingestion` default to `compute=DEFAULT_COMPUTE_STEPS`~~. **Shipped ruling (2026-11-05):** `run_ingestion`/`reparse_matches` KEEP the `compute=()` default (library-safe); the CLI (`cli.py`) opts in by passing `compute=DEFAULT_COMPUTE_STEPS`. Flipping the library default broke the non-calc run tests (a compute span where none was expected); a non-CLI caller must pass `DEFAULT_COMPUTE_STEPS` explicitly (B's latent-foot-gun note, accepted). A positive mart-presence assertion in the dbt-dag test.
- Test: `v2/tests/unit/test_tracking_metrics_mart.py`, extend `v2/tests/unit/test_dbt_materialisation.py`.

**Interfaces:**
- Produces: `fct_tracking_metrics` — a **view**, contract enforced, grain **one row per `(canonical_match_key, canonical_player_key)`**, built by `ref()` of the three **tracking-metric** staging views (pitch_control → controlled-area summary; line_breaking → through-line counts; shape_graphs → modal position + time-in-position), each aggregated to that grain. Column list is the builder's (spec D-open-1); grain and `ref()`-only shape are fixed. `fct_actions` (Task 4b) is a **sibling** layer-1 gold view at action grain — it feeds line_breaking and future event computes (VAEP/xT), and is **not** part of this per-player mart.
- Consumes: the three tracking-metric `stg__*` views (Tasks 4, 5, 6).

- [ ] **Step 1 — failing test**: `test_mart_is_a_contracted_view_grain_match_player()` — manifest node is `materialized: view`, contract enforced, grain keys exactly `canonical_match_key, canonical_player_key`; `test_mart_refs_only_tracking_metric_staging()` — its `refs` are exactly `stg__pitch_control_values`, `stg__line_breaking_results`, `stg__shape_graph_labels` (NOT `fct_actions`/`stg__spadl_actions`); `test_registry_steps_are_layered()` — the spadl/pitch/shape steps are `layer==1`, line_breaking is `layer==2`; `test_native_table_allowlist_still_empty()` — `tables_outside_allowlist(manifest)` (dbt.py:181) is empty, i.e. the new mart + `fct_actions` added no native table (a positive assertion that the view rule held, not a change to the allowlist).
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the SQL aggregations + contract; set `DEFAULT_COMPUTE_STEPS` and flip the run default.
- [ ] **Step 4 — verify pass** (+ `test_dbt_materialisation.py`).

### Task 8: Typed workflow-card model + no-HF card gate

**Files:** Create `v2/src/lakehouse/cards/__init__.py`, `v2/src/lakehouse/cards/model.py`, `v2/src/lakehouse/cards/slice.py`; Test `v2/tests/unit/test_workflow_cards.py`.

**Interfaces:** `class WorkflowCard(BaseModel, frozen=True)` — `id, name, type: Literal[...], status, depends_on: tuple[str,...], inputs: CardIO, outputs: CardIO`; `CardIO` has `tables`, `views`, and **no** HF field; `SLICE_CARDS: tuple[WorkflowCard, ...]` for the layer-0 sources (tracking frames/ball + the three event sources), the **layer-1** computes (spadl_actions ×3 — gradientsports, idsse, skillcorner — pitch_control, shape_graphs) + `fct_actions`, the **layer-2** compute (line_breaking), the mart, and the deferred publish sink (`status="planned"`). `depends_on` encodes line_breaking → `fct_actions` → spadl_actions, so the page DAG shows the two compute layers.

- [ ] **Step 1 — failing test**: `test_cards_form_a_dag_matching_the_compute_registry()` (every `ComputeStep` — all five — has a card; `depends_on` matches the `ref()`/input graph, incl. line_breaking depending on `fct_actions`); `test_no_card_declares_an_hf_input()` (defence in depth); `test_card_outputs_match_bronze_and_mart_names()`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the typed model + slice cards.
- [ ] **Step 4 — verify pass.**

### Task 9: AI/ML Workflows page in v2

**Files:** Create `v2/src/lakehouse/ui/pages/workflows.py` (+ any state module); Modify `v2/src/lakehouse/ui/shell.py` (`PAGE_MODULES` entry) and the page's own `PageConfig` (`nav_section` + route/title/icon, the v2 nav mechanism — **not** `PAGE_TERMS`, which does not exist). Test `v2/tests/unit/test_workflows_page.py`.

**Interfaces:** a page that renders `SLICE_CARDS` as a Cytoscape/dagre DAG (reuse the v1 approach: nodes coloured by `type`, edges from `depends_on`, a self-contained inline script — no external CDN under the v2 CSP; embed or use the v2 template's accepted mechanism) with a per-card detail panel, built through v2's declarative `PageConfig` per `hf_taipy_app/AGENTS.md` (no hand-crafted layout). Consumes `lakehouse.cards.SLICE_CARDS` (Task 8).

- [ ] **Step 1 — failing test**: `test_workflows_page_registered()` (in `PAGE_MODULES` with a goal-oriented nav label via `PageConfig.nav_section`); `test_dag_html_has_a_node_per_card_and_edges_from_depends_on()`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the page + registration.
- [ ] **Step 4 — verify pass.**

### Task 10: HF multi-dataset publish sink seam (defined, build deferred)

**Files:** Create `v2/src/lakehouse/publish/__init__.py`, `v2/src/lakehouse/publish/sink.py`; Test `v2/tests/unit/test_publish_sink_seam.py`.

**Interfaces:** `class PublishTarget(BaseModel, frozen=True)` — `repo: str`, `visibility: Literal["public","private"]`, `marts: tuple[str, ...]`; `class PublishPlan(BaseModel, frozen=True)` — `targets: tuple[PublishTarget, ...]` (supports N targets, e.g. public + private from one lake); `def publish(plan: PublishPlan, *, cat: WorkingCatalog) -> None` **raises `NotImplementedError("HF publish build deferred — spec D5")`** this cycle. No HF client import (Task 2 gate covers `publish/`).

- [ ] **Step 1 — failing test**: `test_publish_plan_supports_multiple_targets()` (a public + a private target validate); `test_publish_is_not_wired_into_the_run()` (AST/string check that `run.py`/`finish()` never calls `publish`); `test_publish_build_is_deferred()` (`publish(...)` raises `NotImplementedError`); `test_publish_module_imports_no_hf_client()`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** the typed seam + the deferred `publish`.
- [ ] **Step 4 — verify pass.**

### Task 11: End-to-end run, amendments, and the single cycle commit

**Files:**
- Create: `v2/tests/e2e/test_compute_e2e.py`
- Modify: `v2/docs/specs/2026-10-26-v2-ingestion-design.md` (§10.3 run-step list: add the compute phase; §9/§11 native-table references); any v2 version/TODO doc the repo maintains.

**Interfaces:** consumes the whole slice.

- [ ] **Step 1 — failing e2e** `test_full_run_builds_tracking_metrics_from_fixture()` — on a seeded local lake with **gradientsports + idsse + skillcorner** tracking **and** events (the three dual-modality providers, so the SPADL + line_breaking path runs for each), `run_ingestion(..., compute=DEFAULT_COMPUTE_STEPS)` produces a non-empty `fct_actions` (rows from all three providers), non-empty `line_breaking_results`, and a non-empty `fct_tracking_metrics` at grain `(match,player)`; `test_run_does_three_builds_and_two_compute_layers()` (the layered seam order, spec D9); `test_second_dbt_build_failure_leaves_prior_version_and_fails_run()`; `test_fresh_lake_empty_mart_and_fct_actions_build_ok()`.
- [ ] **Step 2 — run, verify it fails.**
- [ ] **Step 3 — implement** any wiring the e2e exposes (fixtures, seed helpers); amend the ingestion design §10.3 + native-table references; update the v2 TODO/version doc if present.
- [ ] **Step 4 — verify the whole cycle**: the e2e passes; then the six v2 CI checks locally — `uv run ruff check`, `uv run ruff format --check`, `uv run pyright`, `uv run lint-imports`, `LAKEHOUSE_OS_TESTS=1 uv run pytest -m "not live and not hf and not bench"`, `uv run python scripts/pip_audit.py` — all green.
- [ ] **Step 5 — the single cycle commit (OWNER-GATED).** Stop at "ready to commit"; show the owner the diff / file list for the whole slice; on the owner's explicit yes, make **one** commit on `feat/v2-calc-tracking-metrics`. No commit/push/PR/merge without that yes.

---

## Self-review notes

- **Spec coverage:** D1 → Tasks 4–7; D2 seam + extensibility → Task 1; D3 slice → Tasks 3–7; D4 page → Tasks 8–9; D5 no-HF gate → Task 2, publish sink → Task 10; D6 per-match fail-loud → Task 1 + each compute task + Task 11; **D7 team+ball → Task 3b; D8 compute contract (frame builder, coords, sourced GK, `provider`) → Task 3c (+ Tasks 4, 6); D9 SPADL actions layer → Task 4b, line_breaking on actions×frames → Task 5, layered 3-build/2-compute-layer seam → Task 1 (+ registry Task 7, e2e Task 11).** Every section maps to a task; no orphan tasks.
- **Commit discipline:** one feature branch, one owner-gated cycle commit (Task 11 Step 5); per-task Step-4 verify gates only (round-1 A/B BLOCKING).
- **Open items:** D-open-1 (mart column list) is the only builder freedom (Task 7); grain fixed. D-open-2 resolved (publish deferred, Task 10). D-open-3 resolved (typed cards, Task 8).
- **Not in this plan:** no training/champion build (Task 1 seam accepts a future sibling phase, unbuilt); no xT-grid metrics; no action-context drain; no `import-*`.
- **Proportion:** signatures + test names/assertions + exact paths/values; no implementation bodies (compute algorithms live in silly-kicks; dbt logic is SQL the builder writes from the fixed grain).

## Dispositions (round 1)

- **CALC-PLAN-01 (A+B, BLOCKING) — fixed.** Per-task commits removed; one owner-gated cycle commit in Task 11; Step-4 verify gates kept (Branch & commit discipline).
- **CALC-PLAN-02 (B, BLOCKING) — fixed.** The cycle commit stops at "ready to commit" for an explicit owner yes; the subagent autonomous-commit workflow is dropped.
- **CALC-PLAN-02 (A) / D6 per-match — fixed.** `ComputeStep.__call__` takes an `ErrorSink`; `ComputeOutcome` gains `failed_matches`; steps catch per match, record `compute_failed` with the match key, keep siblings; a per-match test is added to Task 1 and each compute task.
- **CALC-PLAN-03 (A+B) — fixed.** The header mandates inline `executing-plans` (no subagents); one feature branch `feat/v2-calc-tracking-metrics` is named.
- **CALC-PLAN-04 (B) — fixed.** Task 1 defaults `compute=()`; Task 7 flips to `DEFAULT_COMPUTE_STEPS`.
- **CONSIDERs — fixed.** `PAGE_TERMS` → `PAGE_MODULES` + `PageConfig.nav_section` (Task 9); `COMPUTE_INPUT_SELECT = "+tag:compute_input"` derived from the steps, redundant `+fct_tracking_frames` dropped; Task 7's §9 item reworded as a positive view-presence assertion, not an allowlist change.
- **No A/B conflict.** Both verdicts REQUEST CHANGES on the same structural points; task content unchanged, per both reviewers.

## Dispositions (round 2)

Round-2 re-review: Reviewer A APPROVE (1 trivial CONSIDER = CALC-PLAN-07); Reviewer B REQUEST CHANGES (1 SHOULD FIX =
CALC-PLAN-09). Both confirmed the round-1 BLOCKINGs resolved and no regression. A and B point at the same line; B's
type analysis supersedes A's "optional", so it is fixed.

- **CALC-PLAN-09 (B SHOULD FIX / A CALC-PLAN-07 CONSIDER) — fixed.** The per-match failure record carried the
  canonical `match_key` in `error(native=…)`, but `ErrorSink.native: str | None` writes `ops_errors.native_match_id`
  (the provider native-id string) — a bigint there is a pyright failure and the wrong column. The failing `match_key`
  is now carried in the str `message` with `native=None`; Task 1's per-match test asserts the message carries the key
  and `native_match_id` is NULL. No schema change (no new canonical column; the owner-approved-field option was not
  needed).
- **Round-2 verified-clean (no change):** Task 2 gate scope already covers `cards.*`/`publish.*`; the Review-Focus
  second-build-failure cite already reads Task 11; no dangling Step-5 refs; no selector drift.

## Dispositions (amendment #2 — D8/D9 A/B)

Reviewer A REQUEST CHANGES (3 SHOULD FIX, 1 CONSIDER); Reviewer B REQUEST CHANGES (1 SHOULD FIX, 3 CONSIDER); 0
BLOCKING. All doc reconciliation — no task redesign. Full dispositions + the A/B skillcorner disagreement are in the
spec's "Dispositions (amendment #2)". Plan-side changes:

- **CALC-A2-01/02 (double-build) — fixed.** `COMPUTE_L1_SELECT` is now `"+fct_actions"` (not a tag); the
  `tracking_metric_stage1` tag is removed from `stg__pitch_control_values` + `stg__shape_graph_labels`, which build in
  the final `+fct_tracking_metrics` pass. Global Constraints, Task 1 selectors + `test_layer2_build_runs_after_layer1_compute`,
  Task 4/4b/6 files updated. No model is built by two selectors.
- **CALC-A2-02 (provider premise) — fixed in spec D9;** the premise is corrected (skillcorner IS dual-modality). On
  that corrected basis the **owner widened Task 4b to all three dual-modality providers** (gradientsports + idsse +
  skillcorner, 2026-11-04). Build order: gradientsports first (direct, cleanest), then idsse + skillcorner (both
  higher-risk adapters — kloppy-oriented DFL, inference-based respectively); dropping either is a cut → owner.
- **CALC-A2-03 (`provider`) — reflected:** Task 3c adds `provider` to `fct_tracking_frames`; Task 3 note records it.
- **CALC-A2-04 (time axis) — reflected:** Task 5 aligns on period-relative seconds using the provider per-frame
  elapsed time; the fixture test bounds the tolerance.
- **Private `derive_goalkeepers` (CONSIDER) — Task 3c keeps the pin (module docstring) + the round-trip guard test;**
  the silly-kicks-side request for a public seam is an owner follow-up, outside this session.
