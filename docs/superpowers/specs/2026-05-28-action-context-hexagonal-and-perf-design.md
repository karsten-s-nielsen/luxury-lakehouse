# AC-1 Action Context — Hexagonal Architecture + Performance Foundation

| Field | Value |
|---|---|
| **Date** | 2026-05-28 |
| **Status** | In review (rev 2) |
| **Author** | Karsten Nielsen |
| **Scope** | `action_context` only (sets the going-forward hexagonal pattern; does not refactor other pipelines) |

## 1. Problem

`compute_action_context` cannot complete a single IDSSE half-game within its 1800 s
iteration timeout, and because the Delta write is atomic at the end of the
`applyInPandas` DAG, a timed-out iteration produces **zero rows**. Eight IDSSE
halves were observed each burning ~30 min with no output (2026-05-28). AC-1 had
never executed successfully before (the preflight always failed first), so this
enrichment-runtime problem was only surfaced now.

AC-1 is the **consolidation** pipeline: it replaces OBSO, PAUSA, space-creation,
shape-graph, elastic-sync, and tracking-context with one wide action-grain table
(`bronze.spadl_action_context`). The chain is therefore *intended* to be a
superset — it computes ~22 enrichment steps including the 5 heaviest spatial-surface
operators (`add_obso`, `add_pausa`, `add_space_creation`, `add_shape_graph`,
`add_elastic_sync`) that `tracking_context` does not run. The problem is not *what*
it computes but *how*: the single pass is too slow, and we have **no way to verify
the values are even correct** or to measure where the time goes without deploying to
Databricks and running blind.

Two forcing functions:
1. We cannot optimize what we cannot measure. We need to run the real enrichment
   locally on one game (or part of one) and profile it per step.
2. We cannot ship a rewrite of perf-sensitive code without proving the outputs are
   unchanged. We need a correctness harness first.

## 2. Goals / Non-goals

**Goals**
- Run the real `action_context` enrichment **locally**, with no Spark/Databricks, on
  one game or a frame-slice of one game, for any provider.
- Verify correctness via **differential comparison** against the (being-retired)
  OBSO/PAUSA/space/shape/tracking-context outputs for the same game.
- Produce a **per-step profiling breakdown** of the enrichment chain for one work unit.
- Establish a **hexagonal boundary**: pure domain core + swappable I/O adapters, as
  the going-forward pattern so ingestion can later run outside Databricks via new
  adapters only.

**Non-goals (this effort)**
- The actual perf optimizations (this is the *follow-on* the harness enables).
- Any change to AC-1 outputs (Phase A is strictly behavior-preserving).
- Refactoring other pipelines into the hexagon (we set the precedent only).
- Removing the legacy pipelines (they remain the differential ground truth for now).

## 3. Investigation findings (evidence basis)

- Dispatch model is **identical** to `compute_tracking_context` (which completes):
  same per-match driver loop, same `groupBy("match_id","period","frame_batch_id")
  .applyInPandas`, same `frame_batch_id = floor(frame/250)`, same `actions_records`
  broadcast. Dispatch is not the bottleneck.
- The divergence is the chain: diffing the `silly_kicks` import blocks,
  `action_context._enrich_tracking_match` adds `add_obso`, `add_pausa`,
  `add_space_creation`, `add_shape_graph`, `add_elastic_sync` (+ `add_game_state`,
  `add_pre_shot_gk_position/angle`) over `tracking_context`'s ~14 steps. These 5 are
  the heaviest spatial-surface computations in the codebase (OBSO surface,
  shape-graph construction/inference, off-ball-xt-frame are CLAUDE.md-flagged hot
  paths).
- **An IDSSE half produces ~283–302 `applyInPandas` groups, NOT ~8** (verified live on
  fixture game J03WMX: period 1 = 1,555,576 rows / 70,708 distinct frames / **283**
  frame-batches; period 2 = 1,655,698 rows / 75,259 frames / **302** batches —
  `SELECT period, COUNT(DISTINCT floor(frame/250)) FROM bronze.idsse_tracking WHERE
  match_id='J03WMX' GROUP BY period`). An earlier draft of this spec claimed "~2000
  frames ÷ 250 ≈ 8 groups" — that was wrong by ~35× and is corrected here. The
  consequence: with ~283 *independent* groups (~5,500 rows each), **executor/core count
  and serverless scheduling ARE candidate levers** — and far cheaper to test than any
  rewrite. We must NOT pre-commit to "per-group compute volume" as the cause.
- The 1800 s timeout therefore has **three** candidate causes, and Phase D must
  attribute wall-time across all three before any optimization is chosen:
  (a) genuinely heavy per-group compute (the surface-sharing hypothesis below);
  (b) executor-slot starvation — ~283 groups contending for too few concurrent slots;
  (c) per-group fixed overhead — e.g. the actions-DataFrame reconstruction that runs
  inside *every* group (see Risk L1), ×283.
- Surface-sharing hypothesis (one of the three; NOT yet confirmed): OBSO, PAUSA,
  space-creation, and pitch-control all need the same underlying pitch-control/influence
  surfaces; if each `silly_kicks` step recomputes them per frame instead of sharing,
  AC-1 pays for the most expensive computation 4–5× per group. The profiler (Section 8)
  settles whether this — or (b)/(c) — dominates.
- **The domain core is already pure**: `_enrich_tracking_match(actions_df,
  tracking_df, xt, home_team_id) -> pd.DataFrame` contains no Spark. All Databricks
  coupling is in `_process_tracking_match` (reads/writes) and the UDF closure
  (provider bronze→frames conversion). The hexagon is mostly *relocation*, not rewrite.

## 4. Architecture — hexagon

**Dependency direction.** `src/analytics/` is the project's isolated pure layer
(import-linter forbids importing `ingestion`/pyspark). `src/ingestion/` is the
Spark/Databricks layer. Therefore:

```
src/analytics/action_context/          # DOMAIN — pure (pandas, numpy, silly_kicks). No pyspark.
    __init__.py
    work_unit.py     # WorkUnit, MatchMeta dataclasses (the chunking abstraction)
    ports.py         # Protocols: TrackingSource, ActionsSource, XtSource,
                     #            MatchMetadataSource, ResultSink
    convert.py       # provider bronze→frames conversion. COPIED (not moved) from the
                     #   pure helpers in ingestion.tracking_context (_bronze_idsse_to_sportec_input,
                     #   _bronze_metrica_to_frames, _bronze_skillcorner_to_frames) plus the
                     #   GS converter already in action_context. See M4 decision below:
                     #   legacy tracking_context keeps its own copies UNTOUCHED until after
                     #   Phase C validation, so the differential oracle is not coupled to new
                     #   code during the window we validate against it. De-dup is a follow-on.
    enrich.py        # _enrich_tracking_match (tracking tier), _enrich_sb360_match,
                     #   _enrich_event_only_match — moved verbatim, behavior-preserving
    schema.py        # _RESULT_COLUMNS, _ACTION_CONTEXT_DDL, _build_output, struct builders
    pipeline.py      # enrich_batch(...) = THE shared per-250-frame-batch contract:
                     #   action-window filter (±_ACTION_TIME_BUFFER_SECONDS) → convert →
                     #   enrich → build_output for ONE frame_batch. Both prod and local
                     #   call this exact function.
                     #   run_work_unit(work_unit, sources, sink, *, profile=False):
                     #   pulls via ports → loops floor(frame/250) batches calling
                     #   enrich_batch per batch → concat → sink. The loop replicates
                     #   production's Spark groupBy(frame_batch_id) dispatch.
    profiling.py     # per-step timing wrapper used by enrich_batch when profile=True
    local/
        parquet_sources.py   # Parquet{Tracking,Actions,Xt,MatchMetadata}Source, ParquetResultSink

src/ingestion/action_context.py        # ADAPTERS + COMPOSITION ROOT — pyspark/delta/dbutils
    # Spark/Delta adapters implementing the ports (driver-side reads)
    # _make_action_context_udf: thin shell that calls analytics.action_context.enrich_batch
    #   ONCE per Spark frame_batch_id group (= one loop iteration of run_work_unit)
    # main()/main_preflight(): wire Spark adapters → domain; guard; Delta write
```

**Key invariant (H3 — load-bearing).** The shared unit of compute is **`enrich_batch` over
ONE 250-frame batch**, NOT the whole work unit. Production runs `enrich_batch` once per
Spark `groupBy(match_id,period,frame_batch_id)` group (~283 groups/IDSSE half); the local
`run_work_unit` runs the **identical** `enrich_batch` in a `for batch in floor(frame/250)`
loop and concatenates. This matters because `fct_tracking_context` (the differential oracle)
was computed with the SAME per-250-batch dispatch, and several features are
**window-dependent** — their value depends on which frames are in `tracking_df`
(`add_elastic_sync` aligns over the whole set; OBSO peak/optimal and sync_score scan
candidate frames). Enriching a 7500-frame slice in one call would produce different numbers
than 283×250-frame batches, invalidating both the Phase C differential and the Phase D
profiler. So **the 250-frame batching + ±`_ACTION_TIME_BUFFER_SECONDS` action-window
filtering is part of the domain contract (`enrich_batch`), fixed in Phase A — not a Spark
dispatch detail.** A Phase A test asserts: `run_work_unit` over a 2-batch (500-frame)
fixture == two separate `enrich_batch(250)` calls concatenated.

The ports/adapters abstract *I/O orchestration on the driver*; the per-batch compute
(`enrich_batch`) is the pure domain, identical in both worlds.

**Constraint to verify during implementation:** `src/analytics/` must remain free of
pyspark imports and the import-linter contract must stay green. `silly_kicks` is
pure-python and already an allowed analytics dependency.

## 5. WorkUnit + ports

```python
@dataclass(frozen=True)
class WorkUnit:
    provider: str                 # idsse | metrica | skillcorner | gradientsports | statsbomb | wyscout
    match_id: str                 # native match id
    period: int | None = None     # IDSSE half-game chunking; None = whole match
    frame_range: tuple[int, int] | None = None  # optional slice for fast profiling fixtures
```

`WorkUnit` is the chunking abstraction and mirrors production exactly (IDSSE → one
period; other tracking → match; event-only → match; profiling → frame slice).

Ports (domain-defined Protocols; `WorkUnit`-in, pandas-out — never Spark-typed):
- `FrameSource.frames(wu) -> FrameBundle` — returns the **tier-appropriate** frames:
  tracking frames (idsse/metrica/skillcorner/gradientsports), per-event StatsBomb-360
  freeze-frames (`bronze.statsbomb_360`, shaped via `snapshot_to_tracking_frames`), or an
  empty bundle (pure event-only: statsbomb-no-360, wyscout). `FrameBundle` carries the
  frames + a `tier` tag.
- `ActionsSource.actions(wu) -> pd.DataFrame`
- `XtSource.grid() -> tuple[list[list[float]], int, int]`
- `MatchMetadataSource.metadata(wu) -> MatchMeta`  (home_team_id, home_start_left, GS roster maps)
- `ResultSink.write(wu, result_df) -> int`  (returns row count)

**M6 — the domain has three enrich tiers, so the ports model all three (not just
tracking).** `enrich.py` holds `_enrich_tracking_match`, `_enrich_sb360_match`,
`_enrich_event_only_match`; `FrameSource` + `FrameBundle.tier` generalize the earlier
tracking-only port. **Tier dispatch lives in `pipeline.run_work_unit`**: it classifies the
provider (`_TRACKING_PROVIDERS` / SB360-present / event-only — the existing logic in
`action_context.py`, moved into the domain), asks `FrameSource` for the matching bundle,
and routes to the correct enrich tier.

A future non-Databricks runtime implements these Protocols against whatever store; the
domain is untouched.

## 6. Adapters

- **Prod (Spark/Delta)** in `src/ingestion/action_context.py`: read bronze tables with
  the existing column projections (`_IDSSE/_METRICA/_SKILLCORNER_TRACKING_SELECT_COLS`),
  resolve metadata on the driver (IDSSE events, GS roster, SkillCorner matches), read
  xT from `bronze.expected_threat_grids`, write via `write_delta_table` with the
  existing period-scoped `replaceWhere`. The `applyInPandas` UDF stays the executor
  vehicle but delegates conversion+enrichment to the domain.
- **Local (Parquet)** in `src/analytics/action_context/local/`: read the committed
  fixture parquet for the `WorkUnit`; `ParquetResultSink` writes the result parquet for
  inspection/diff.

## 7. Extract tool + fixtures

`scripts/extract_action_context_fixture.py`:
```
--provider idsse --match-id J03WMX --period 1 [--frame-start N --frame-end M]
```
Pulls via Databricks SQL, for the given `WorkUnit`:
- bronze tracking (projected cols), bronze SPADL actions, xT grid, provider metadata
  (IDSSE events / GS roster+events / SkillCorner matches);
- **and the legacy ground-truth outputs** for the same game, per the §9.1 matrix:
  `fct_tracking_context`, `fct_off_ball_xt`, `fct_pausa_values`, `fct_space_creation`,
  `fct_formation_labels`/`fct_tracking_shape_timeline`, `bronze.elastic_sync_results`, and
  `int_running_score`/`fct_action_values` (game_state) — for the differential harness.

Writes to `src/tests/fixtures/action_context/<provider>/<match>[_p<period>]/*.parquet`.
Run live only to create/refresh a fixture. Document a refresh procedure.

**Default committed fixture = a `--frame-range` slice, not a full half (L3).** A full
IDSSE half is ~1.55–1.66M tracking rows; committing two such parquets is a heavy git
binary and this repo has prior tracking-data size/OOM history. So: the *committed*
fixture is a small frame-slice (enough actions to exercise every enrichment tier +
the differential), and the full half is regenerated on demand via the extract tool for
profiling. Before committing any fixture, confirm compressed size; use git-LFS if a
full half ever must be committed.

## 8. Profiling

Phase D must attribute the 1800 s wall-time across **all three** candidate causes from
§3 (per-group compute, executor-slot starvation, per-group fixed overhead) — not just
per-step compute. Two complementary profiles:

1. **Per-step compute profile (local).** `src/analytics/action_context/profiling.py`
   wraps each enrichment step with a monotonic timer when
   `run_work_unit(..., profile=True)`. Emits a per-step table (step → wall seconds,
   optional surface-computation counts) for one work unit. Settles the surface-sharing
   hypothesis (a). **Must attribute the per-group actions-DataFrame reconstruction
   (Risk L1) as its own line** so 283× `pd.DataFrame(actions_records)` + period/time
   filtering is not misread as `silly_kicks` compute.
2. **Parallelism/scheduling profile (Databricks).** Groups (~283/half) vs concurrent
   executor slots, and wall-clock vs `_FRAME_BATCH_SIZE`. This rules in/out cause (b) —
   executor starvation — a far cheaper lever (cluster sizing / batch size) than any
   silly_kicks rewrite. **M7 — feasible method:** a full IDSSE half times out at 1800 s
   with zero output, so you cannot read scheduling from it and a fresh full-half probe
   times out again. Instead run the **`--frame-range` slice** (sized to ~20–30 batches so
   it actually completes) and read the **Spark UI executor timeline / event log** for
   slot occupancy and per-group wall-time. Extrapolate to the full 283 groups. This is the
   measurement gating the §8 "cheap lever first" decision, so it must use a job that
   finishes.

**Decision gate:** do not conclude the fix requires a silly_kicks surface-sharing
change until (b) executor starvation and (c) per-group overhead have been measured and
ruled out. The cheap levers get tested first.

## 9. Differential correctness harness

`src/tests/action_context/test_differential.py` (and a runnable CLI variant):
1. Load a fixture `WorkUnit`.
2. `run_work_unit` with Parquet sources → result DataFrame.
3. Join to the legacy ground-truth outputs at chunk grain (action_id / match+period).
4. Assert per-column agreement. **Phase C is tolerance-based, NOT byte-for-byte (M3).**
   Cross-pipeline comparison runs the legacy Spark path vs the new local path; OBSO /
   pitch-control / spatial surfaces drift with BLAS/numpy version, thread count, and
   summation order, so equality would throw false diffs that look like regressions.
   - Per-column oracle mapping is the **coverage matrix below (§9.1)** — not the handful
     named in an earlier draft.
   - Float columns: per-column `abs/rel` epsilon with documented rationale. Bool/int: exact.
   - Coordinate/range invariants as a backstop (LTR, OBSO ∈ [0,1]).
   - **Determinism pinning:** the harness sets `OMP_NUM_THREADS=MKL_NUM_THREADS=1` and
     pins numpy, so reruns are reproducible and tolerances can be tight.
   - (Phase A self-comparison — same code, new path vs old path — IS byte-for-byte
     legitimate and uses exact equality; only Phase C cross-pipeline uses tolerances.)
5. Report divergences as a table (column, n_mismatch, max_delta) — not just pass/fail.

**Golden snapshot is captured in Phase C, in-scope, while legacy truth still exists (M2).**
The differential ground truth is on a retirement clock. The ordering is: (1) legacy
differential validates the new path → (2) **freeze the validated new-path output for the
fixture as a committed golden file** → (3) only then is it safe for legacy to be removed.
If legacy is deleted before the golden file is frozen, we lose the only independent
oracle. "Freeze validated golden snapshot" is an explicit Phase C deliverable, not a
long-term afterthought. Post-legacy (Approach 3), routine correctness = invariants +
drift-vs-golden.

Chunk-awareness: the harness runs and compares at the same grain production uses (a half
for IDSSE).

### 9.1 Column-coverage matrix (M5 — the correctness story)

AC-1 has **never produced rows** (`bronze.spadl_action_context` = 0 rows;
`fct_action_context` mart never built), so there is **no production baseline**. Phase A
can only prove the refactor didn't change the numbers; **all first-time correctness
evidence lives in this differential.** Every AC-1 output column maps to one of:
{differential oracle = a specific legacy table | invariant/range-only | unvalidated}.

Oracle tables — **verified live via `information_schema.columns` (2026-05-28)**, which
corrected three assumptions from the prior draft:

- `fct_tracking_context` (84 cols) — column names **match AC-1's exactly** for the 66
  tracking features (frame linkage, GK resolution+spatial, action context, actor
  pre-window, pressure, pitch-control, defensive line, off-ball, Ward line-break, team
  shape, DAS, GK influence, cover shadows, sync score). **No name-map needed — direct
  differential.**
- `fct_pausa_values` (18 cols) is the oracle for **both OBSO and PAUSA**: AC-1
  `obso_actual/peak/optimal` → `actual_obso/peak_obso/optimal_obso`; AC-1
  `pausa_temporal/spatial/composite` → `temporal_judgment/spatial_selection/pausa_score`.
  Grain = per `pass_id`; join to action via pass→action. **`fct_off_ball_xt` is NOT an
  oracle** (it's player-match aggregate `total/avg_off_ball_xt` — wrong grain + metric).
- `bronze.elastic_sync_results` (6 cols) is the elastic oracle: AC-1
  `elastic_frame_id/confidence/error_seconds` → `frame_id/alignment_confidence/alignment_error_seconds`,
  per `event_id`.
  > **Post-execution delta (2026-05-29):** this oracle turned out NOT to be valid. The legacy
  > `analytics.elastic_sync` that writes it has an IDSSE frame-origin bug (`frame≈25·ts`, 0-based —
  > verified `frame_id=25.000·ts−0.9`, intercept≈0 vs correct +10000), so it has no results for the
  > first ~400s and ~400s-misaligned ones after. silly-kicks 3.25.0 fixed exactly this in AC-1, so
  > `elastic_*` is **INVARIANT_ONLY** (range-checked), not oracle-compared. The id spaces match
  > (422/1285 overlap) — it was never a join problem. See
  > `memory/project_legacy_elastic_sync_frame_origin_bug.md`; to be captured in ADR-028.
- **`shape_graph_*` (6) has NO legacy oracle** — `fct_formation_labels` is formation
  strings, `fct_tracking_shape_timeline` is per-player position timeline; neither carries
  density/n_edges/mean_stability. → **invariant-only.**
- **`space_created_*` (2) is grain-mismatched** — `fct_space_creation` is player-frame
  (`space_created_m2/destroyed/net`), AC-1 is action-team/opponent. No clean 1:1
  differential. → **invariant-only (or aggregate-compare if Phase C finds a sound roll-up).**

| AC-1 column group (count) | Oracle | Grain / join |
|---|---|---|
| Identity (12) | join keys — exact | action_id |
| `game_state` (1) | `int_running_score` / `fct_action_values.game_state` | action_id |
| Frame linkage (4) | `fct_tracking_context` (exact names) | action_id |
| GK resolution (4) | `fct_tracking_context` (exact names) | action_id |
| GK spatial (6) | `fct_tracking_context` (exact names) | action_id |
| Action context (4) | `fct_tracking_context` (exact names) | action_id |
| Actor pre-window (2) | `fct_tracking_context` (exact names) | action_id |
| Pressure (3) | `fct_tracking_context` (exact names) | action_id |
| Pitch control (3) | `fct_tracking_context` (exact names) | action_id |
| Defensive line (6) | `fct_tracking_context` (exact names) | action_id |
| Off-ball context (6) | `fct_tracking_context` (exact names) | action_id |
| Ward line-breaking (3) | `fct_tracking_context` (exact names) | action_id |
| Team shape (14) | `fct_tracking_context` (exact names) | action_id |
| DAS (3) | `fct_tracking_context` (exact names) | action_id |
| GK influence (4) | `fct_tracking_context` (exact names) | action_id |
| Cover shadows (5) | `fct_tracking_context` (exact names) | action_id |
| Sync score (3) | `fct_tracking_context` (exact names) | action_id |
| OBSO (3) | `fct_pausa_values.actual_obso/peak_obso/optimal_obso` | pass_id → action |
| PAUSA (3) | `fct_pausa_values.temporal_judgment/spatial_selection/pausa_score` | pass_id → action |
| ELASTIC sync (3) | `bronze.elastic_sync_results.frame_id/alignment_confidence/alignment_error_seconds` | event_id → action |
| **Shape graph (6)** | **NONE — invariant-only** (no legacy table carries these metrics) | range/invariant |
| **Space creation (2)** | **grain-mismatch — invariant-only** (fct_space_creation is player-frame) | range/invariant |
| Audit `_ingested_at` (1) | n/a | — |

**Differential coverage is PROVIDER-DEPENDENT — the matrix above is the IDSSE story.**
Verified live row counts per oracle (2026-05-28):

| Oracle | Providers with rows | Consequence |
|---|---|---|
| `fct_tracking_context` (66 cols) | skillcorner (10 matches), idsse (7), metrica (3, thin + known low quality), **gradientsports = 0** | GS has NO tracking oracle at all |
| `fct_pausa_values` (OBSO+PAUSA, 6 cols) | **IDSSE only** (7 matches) | OBSO/PAUSA differential exists only for IDSSE |
| `bronze.elastic_sync_results` (3 cols) | **IDSSE only** | elastic differential exists only for IDSSE |

Per-provider feature-column differential coverage (of ~92):
- **IDSSE: ~84/92** — the full matrix above. Only shape_graph (6) + space (2) invariant-only.
  **The 5 heaviest operators (OBSO/PAUSA/elastic + the surfaces) are validatable ONLY here.**
- **SkillCorner: ~66/92** — tracking only; OBSO/PAUSA/elastic/shape/space (~16 cols, incl.
  the 5 heaviest) are invariant-only.
- **Metrica: ~66/92** but only 3 oracle matches, thin + low data quality.
- **GradientSports: ~1/92** — only `game_state` (via `fct_action_values` if present);
  fct_tracking_context has **zero** GS rows. Effectively **no differential**.

**Therefore IDSSE (fixture J03WMX) is the ANCHOR differential fixture** — the sole provider
where the 5 heavy spatial operators (the entire reason AC-1 is a superset and the entire
perf risk) can be checked against ground truth. **GradientSports is demoted to
golden+invariant + profiling validation only** (no differential); picking GS to "exercise
the differential" would be the worst choice given zero oracle rows.

**Match-id join is NOT uniform — each oracle uses a different key (confirms M8):**
- AC-1 output `match_id` = bare native (`J03WMX`).
- `fct_tracking_context` has **no native match_id — only surrogate `match_key`**; scope via
  `dim_matches` (`J03WMX → match_key`) before joining.
- `fct_pausa_values` uses **prefixed** native (`idsse_J03WMX`); normalize the `idsse_` prefix.
- `bronze.elastic_sync_results` stores the SAME match under **both** `J03WMX` and
  `idsse_J03WMX` with **different row counts** (e.g. 544 vs 483) — disambiguate to the
  authoritative set (max `_ingested_at` per (match,event_id), verified against the actions'
  event_ids) before joining, or you silently pick wrong / double-count.
- `action_id` is **per-match** (only 744 distinct across 7 IDSSE matches ≈ 6× collision) —
  oracles MUST be **match-scoped first, then joined on action_id**. `oracle_map` needs three
  distinct match-join strategies (surrogate via dim_matches / prefix-normalize / dedupe), not one.

The 8 globally-invariant-only columns (shape_graph 6 + space 2) plus all off-IDSSE
invariant-only columns get Phase C golden frozen at first-capture, guarded only by
range/invariant (L5). This is exactly why local single-game testing matters: for those
columns there is no other oracle.

## 10. Sequencing

- **Phase A0 — Test-coverage inventory + pure unit nets FIRST (TDD; M1).** Before
  moving anything, inventory current test coverage of every function being relocated
  (enrich tiers, the 3 converters being copied, `_build_output`, schema builders). Where
  coverage is Spark-integration-only (no fast local exercise), **write pure unit tests
  first** so the move happens *under* a green local net. This delivers the hexagon's
  headline benefit (testable without Databricks) rather than asserting it.
- **Phase A — Hexagon extraction (behavior-preserving).** Relocate/copy domain to
  `src/analytics/action_context/`, define WorkUnit/ports, make the UDF a thin shell, wire
  Spark adapters in the composition root. Add unit tests for the NEW `ports` / `pipeline`
  / local Parquet adapters **in this phase, not later**. Acceptance: A0 + new unit tests
  green locally with no Spark; import-linter green; **behavior PRESERVED**, verified by
  snapshotting the **pre-refactor** `_enrich_tracking_match`/converter output on the
  committed fixture, then asserting the **post-refactor** path reproduces it byte-for-byte
  (legit here — same code, old path vs new path; runs locally, no Spark/Delta). **H2:**
  this does NOT compare against `fct_action_context` — that mart has never been built
  (`bronze.spadl_action_context` = 0 rows). Phase A proves behavior-preservation **only**;
  first-time correctness is Phase C (§9.1).
- **Phase B — Extract tool + fixtures** for at least IDSSE (half), one of
  Metrica/SkillCorner, GradientSports, and one event-only provider. Committed fixtures are
  frame-slices (L3); pull legacy ground-truth outputs alongside.
- **Phase C — Differential harness + GOLDEN CAPTURE.** Wire the tolerance-based
  differential (M3) to legacy ground truth; record baseline agreement; **then freeze the
  validated new-path output as a committed golden file while legacy still exists (M2).**
  This phase is the last point at which the independent oracle is available.
- **Phase D — Profiler** on the IDSSE-half fixture: per-step compute profile (attributing
  the actions-reconstruction overhead, L1) AND the parallelism/scheduling profile (H1).
  Publish the breakdown; rule out the cheap levers before pinning the cause.
- (Follow-on, separate spec) **Optimization** guided by Phase D, guarded by Phase C +
  golden snapshot.

Phases A0–D are this spec. Optimization is explicitly the next, separate effort.

## 11. Going-forward convention

The hexagon (pure domain in `src/analytics/<feature>/`, Spark/Databricks adapters +
composition root in `src/ingestion/`, ports defined in domain terms taking a
work-unit and returning pandas) becomes the recommended pattern for compute pipelines
so that running ingestion/compute outside Databricks later is a pure adapter swap.
Capture as an ADR (ADR-028) when Phase A lands. **ADR-028's Decision must state the
hexagon is recommended for NEW or actively-touched pipelines — explicitly NOT a mandate
to retrofit existing pipelines (L2)** — so "going-forward convention" can't be read as a
silent requirement to hexagon-ify everything. This spec does not retrofit existing
pipelines.

## 12. Risks / open questions

- **Converter copy-vs-move resolved = COPY (M4).** Phase A *copies* the 3 pure
  converters into `analytics/action_context/convert.py` and leaves `ingestion.tracking_context`'s
  own copies UNTOUCHED. Rationale: tracking_context is the differential oracle; moving
  the converters out would couple the oracle to the new module during the exact window we
  validate against it, and contradicts "don't touch other pipelines." Transient
  duplication is accepted; de-duplication is a follow-on after the legacy pipelines retire.
  **Drift guard (L4):** a ~15-line test runs both copies (analytics + tracking_context) on
  one fixture and asserts identical output, enforced for as long as both exist — two copies
  of behavior-critical coordinate logic must not silently diverge and corrupt the oracle
  relationship the copy was meant to protect.
- **Golden enshrines first-capture on any un-oracled column (L5).** If Phase B demotes any
  AC-1 column to invariant-only (no resolvable legacy counterpart), its Phase C golden is
  frozen at "whatever the code emitted," checked only by range/invariant — a golden test
  silently enshrines a first-capture bug on exactly those columns. Per §9.1 the plan
  expects zero such columns, but any that arise must be listed explicitly as accepted
  residual risk, not hidden in the golden.
- **silly_kicks step boundaries for profiling/sharing.** Whether `silly_kicks`
  exposes shared pitch-control surfaces (so we can compute once and feed OBSO/PAUSA/
  space-creation) is unknown until Phase D profiling. If it does not, the optimization
  follow-on may require a silly_kicks change (the lib is user-maintained at
  `D:\Development\karstenskyt__silly-kicks`). NOTE: only pursue this after Phase D rules
  out the cheaper levers (executor starvation, per-group overhead) — see §3/§8.
- **Fixture size for IDSSE (L3).** A full half is ~1.55–1.66M tracking rows (verified).
  Committed fixture defaults to a `--frame-range` slice; full half regenerated on demand;
  confirm compressed size and use git-LFS before committing any full half.
- **Legacy output availability at chunk grain.** The differential requires the legacy
  pipelines' outputs to exist for the chosen fixture games; the extract tool must pull
  them, and we confirm coverage when selecting fixtures. The golden file must be frozen
  (Phase C) before any legacy pipeline is deleted (M2).
- **Executor vehicle unchanged in Phase A.** We keep `applyInPandas` + frame-batch
  sizing as-is in Phase A; any change to the unit-of-work (finer chunking, shared
  surfaces per frame, or cluster/concurrency sizing — now a live candidate given the
  verified ~283 groups/half) is an optimization-phase decision driven by Phase D.
