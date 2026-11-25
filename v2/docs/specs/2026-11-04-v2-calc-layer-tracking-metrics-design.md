# v2 Calculation Layer — first slice: tracking metrics (clean forward DAG) design

**Status:** Round 1 (2026-11-04). Scope confirmed with the owner in brainstorming; design drafted; **round-1 A/B
spec review done** — Reviewer A REQUEST CHANGES (7 SHOULD FIX, 1 CONSIDER, 0 BLOCKING), Reviewer B REQUEST CHANGES
(1 BLOCKING, 4 SHOULD FIX, CONSIDERs); reports `D:\Development\_reviews\2026-11-04-v2-calc-layer-tracking-metrics-spec-{A,B}.md`.
Every finding is answered in "Dispositions (round 1)" and applied below. **Owner decisions (2026-11-04):** keep
line_breaking and grow layer 0 to provide its event/360 input views; define the HF-publish sink seam but defer its
build to a later cycle. One A finding (CALC-SPEC-05, collapse to a single dbt build) is **not adopted**, with
evidence — see its disposition. **Round-2 A/B re-review done — Reviewer A APPROVE, Reviewer B APPROVE** (reports
`…-spec-r2-{A,B}.md`); both verified all seven round-1 findings resolved, both independently confirmed the
CALC-SPEC-05 decline from `lake_table_present.sql` (build-time; two builds required; do not reopen), and both raised
one CONSIDER (the second-build selector must rebuild the staging views) now applied as CALC-SPEC-11; A's CALC-SPEC-12
(Databricks "UC-volume" vocab) is applied too. No further review round is required.
**Owner approved 2026-11-04; D-open-1 confirmed — only the mart column list is left to the builder (the grain,
single-gold-view, and `ref()`-only shape are fixed here).** The plan
(`docs/plans/2026-11-04-v2-calc-layer-tracking-metrics-plan.md`) now begins, with its own A/B re-review and owner
approval before any code.

**AMENDMENT 2026-11-04 (implementation found a data-model gap; owner chose "grow this cycle").** Implementing the
computes revealed the silly-kicks scorers need team membership + ball + attacking-team per frame, which v2's
positions-only tracking core does not surface. New section **D7** specifies the team+ball enrichment (the data
exists in bronze; it is surfaced, not re-ingested). Tasks 1–3 already landed green (compute seam, no-HF gate,
fct_tracking_frames); D7 and the enrichment/computes are **pending an A/B spec re-review (round 3) and owner
approval** before more code.

**AMENDMENT 2026-11-04 #2 (a spike + a silly-kicks session fixed the scorer contract; owner chose "build a SPADL
actions layer" + "one mega-cycle").** New sections **D8** and **D9** supersede parts of the design above. **D8**
records the real silly-kicks compute contract: a shared canonical frame builder, coordinates transformed to the SPADL
0-105/0-68 pitch frame, and **`is_goalkeeper` sourced from the roster (`keeper_identity`) or `derive_goalkeepers`,
never hand-rolled** — this **corrects ruling R4** (the scorer does not resolve the goalkeeper; the caller supplies
it). **D9** records that `line_breaking` is an ACTIONS × FRAMES scorer needing a **SPADL actions layer** v2 lacks: it
adds an events→SPADL layer (`fct_actions`, providers **gradientsports + idsse**), rewrites `line_breaking` onto
`fct_actions` + frames, **drops the event/360 views** (R5), and **generalises the run seam to two compute layers /
three dbt builds**. Where this doc's earlier Goals/D1/D2/D3 text mentions the event/360 views or a two-build seam, D8
and D9 govern. D8/D9 and the plan changes are **pending an A/B amendment review and owner approval** before any
compute, SPADL, or line_breaking code is built; Tasks 1–3b (green) stand.

**AMENDMENT 2026-11-23 (owner ruling; section D10).** pitch_control and shape_graphs leave the default run until
the silly-kicks batch upgrade, and idsse SPADL ships marked as known incomplete. Where the text above or D3/D8/D9
lists pitch_control and shape_graphs as steps of the default run, D10 governs.

## Intent and why

v2 has an ingestion layer (bronze), entity resolution (dimensions), and a thin cross-provider tracking view. It has
**no calculation layer** — none of v1's 59 analytics workflow cards exist in v2 yet. This cycle starts that
migration with a small, representative slice and, more importantly, **establishes the v2 pattern** for a computed
metric so every later metric follows one clean shape.

Two things in the v1 calculation DAG do not move cleanly forward, and this design exists to not reproduce them:

1. **HF round-trips as load-bearing steps.** `wf-import-psxg` and `wf-import-obso` compute a value on HF Jobs,
   publish it to the HF Hub, and a *later* run downloads that artifact back into bronze. The card states its
   `depends_on` is "LINEAGE ONLY, NOT RUNTIME ORDERING" and that it "pulls from a PREVIOUS HF Jobs run" (ADR-074).
   A downstream step therefore depends on a previously-published HF artifact, not on anything produced earlier in
   the same run. The owner's rule for v2: **nothing may depend on a published HF dataset or model.**
2. **The three-stage dbt barrier.** v1 builds `input marts → compute tasks → intermediate marts → output marts`,
   where the compute tasks hand-wire a Databricks `depends_on` so they read "today's gold rather than yesterday's"
   (ADR-019). The forward pass is faked with run-ordering across separately-scheduled tasks, not expressed as one
   lineage graph.

v2 already has the tool to fix both: dbt `ref()` is an acyclic forward lineage the engine enforces, and a run is a
single ordered process, so a compute step placed in the middle of the graph runs forward within one run — no
cross-run hand-off, no "today's gold", no HF import.

## Goals

- **Establish the v2 computed-metric pattern** (D1, D2): a Python compute step writes a bronze table through the
  bronze writer; a dbt staging view exposes it; a dbt gold view (contract enforced) is the consumable mart. This
  mirrors v1 ADR-013 (Python writer → bronze → staging view → gold mart) and fits v2's native-table rule (spec §9,
  with §11.1/§11.6 on views + contracts; §23.6 holds the enforcing test): only `dim_*`, `*_links`, `ops_*`,
  `lineage_*` are native tables, every row-level gold model is a view.
- **Introduce the compute-phase seam** in `lakehouse.runs` (D2). Today a run is `resolve → dbt → maintain`; this
  cycle adds a compute phase, designed so a later training or champion-selection phase slots in as a sibling
  (D2 "Extensibility") without reshaping what this cycle builds.
- **Ship the slice** (D3; layer-1 inputs revised by D8/D9): layer 0 input views (`fct_tracking_frames` +
  `fct_tracking_ball`; the event/360 views are **superseded** — D9 replaces them with the SPADL `fct_actions` layer);
  a shared canonical frame builder (D8); layer 1 heuristic computes — `pitch_control`, `shape_graphs`, the
  `spadl_actions` converters (D9) — plus the layer-2 `line_breaking` on actions × frames; one combined gold mart
  `fct_tracking_metrics`.
- **Bring the AI/ML Workflows page into v2** (D4), representing these steps and their forward edges.
- **Keep HF publishing optional** (D5): define the optional-sink seam now; nothing in the graph reads its output.

## Non-goals (this cycle)

- No training or champion-model steps: VAEP, `xg_v2`, the xT-grid fit, ScoutGPT, football2vec, the evolve engine.
  The seam (D2) must accept them later as a sibling phase; this cycle ships none.
- No xT-grid-dependent metrics: `off_ball_xt`, OBSO, PAUSA, space-creation, territory.
- No full action-context drain (`wf-action-context`, 28800 s worker-drain).
- No `import-*` steps. They are **eliminated** (the value is computed forward in-repo), not ported.
- **No HF-publish build** (owner, 2026-11-04). D5 defines the sink seam; the publish code ships in a later cycle.

## Anti-goals (a failure even if it runs green)

- Any v2 step that reads a previously-published HF dataset or model as an input.
- Any "today's gold vs yesterday's" or lineage-not-runtime coupling between steps.
- A computed metric materialised as a native row-level dbt table (violates spec §9 — a row-level data leak into the
  lake root on a non-encrypted lake).
- The compute phase built so narrowly that a future training/champion phase cannot attach without reshaping it
  (D2 "Extensibility").
- A compute failure that leaves a **silently empty** mart indistinguishable from "no data yet" (D6).

## Constraints

- **v2 is DuckLake/DuckDB, not Spark.** There is no `applyInPandas`. Each compute is a per-match Python/pandas pass
  over the match's frames, reusing the silly-kicks scoring functions (the lakehouse never edits silly-kicks). The
  v1 `src/analytics/*.py` wrappers are reference, not import targets.
- **dbt gold models are views** with an enforced contract (`v2/dbt/dbt_project.yml`; spec §9, §11.1, §11.6); only
  the four native-table patterns may be tables. A computed metric's rows therefore live in a **bronze table** the
  compute writes, never a dbt-materialised table.
- **Velocity is derived in the compute pass, never in SQL.** The silly-kicks preprocess seam
  (`PreprocessConfig.default()`, ADR-067) derives velocities inside the per-match pandas pass. A dbt SQL view
  cannot call it, so the layer-0 view carries positions only (D1, D3).
- **One DuckDB attacher at a time.** `run_dbt` detaches the working catalog, lets dbt attach it, and re-attaches
  after. A compute step that needs the catalog runs outside a dbt invocation (D2).
- **Memory + file-count budgets** (T12): per-match iteration, bounded open files; the compute honours the same
  limits as the rest of a run.
- v2 standards: Python 3.12; ruff, pyright, import-linter, 120-column lines; `lakehouse.runs` import boundaries.
- silly-kicks owns SPADL/pitch-control/line-breaking/shape-graph logic (ADR-016/022/029/053/067 lineage from v1).

## D1 — the computed-metric pattern

A computed metric in v2 is four artifacts, strictly forward:

1. **Input view(s)** — one or more dbt gold/staging views the compute reads (e.g. `fct_tracking_frames`; for
   line_breaking, `fct_actions` + tracking frames — *D9 supersedes the earlier "event and 360 views" here*). A view
   stores no data; building it is free. The view carries positions and identifiers only — no derived velocity
   (velocity is a compute concern).
2. **Compute step** — a Python function that reads its declared input view(s) per match, derives velocity via the
   silly-kicks preprocess seam where needed, calls the silly-kicks scorer, and writes one **bronze table** through
   the v2 bronze writer (`lakehouse.bronze.writer`), with the audit columns and visibility the writer enforces.
   Idempotent per `(match_key, …)` with a skip guard, like v1.
3. **Staging view** — a dbt staging view over that bronze table, guarded by `lake_table_present` so it is
   empty-but-typed before the first compute writes (no hard failure on a fresh lake). See D2 for why this guard
   forces the build order.
4. **Gold view** — a dbt gold view (contract enforced) that `ref()`s the staging view(s). This is the mart.

The forward edge from compute output to mart is a dbt `ref()`; there is no runtime hand-off outside the run and no
read of a prior run's output.

## D2 — the compute-phase seam in a run

Today `_Run.finish()` is `resolve → dbt → maintain`. This cycle makes it:

```
resolve → dbt (build input views) → compute (layer 1 → bronze) → dbt (build staging + mart views) → maintain
```

- **A compute step declares:** its input view(s) (one or more), its output bronze table, and its skip-guard key.
  (Round-1 fix CALC-SPEC-02 / seam: input is a *set* of views, not one — line_breaking reads tracking **and** the
  event/360 views. The three layer-1 steps are **independent of each other** — they share no output and may run in
  any order or in parallel — not "independent given layer 0", which was false for line_breaking.)

- **Why two dbt builds, not one (round-1 fix CALC-SPEC-05).** *(Superseded by D9: the seam is now **three** builds /
  **two** compute layers, because line_breaking reads a computed view — see D9. The build-time `lake_table_present`
  reasoning below is unchanged and is why each stage still needs its own build.)* All models are views, so a single
  pre-compute build looks tempting. It does not work: `lake_table_present` is evaluated at **build time**
  (`{% if not execute %}…`
  then `adapter.get_relation`). A layer-1 staging view built *before* its bronze exists compiles to its empty
  branch and stays empty — the mart would be permanently empty (exactly B's silently-empty-mart failure). The
  staging + mart views must be built **after** the compute writes bronze, so `lake_table_present` is true and the
  view selects real rows. Hence two selective builds, both view-only and cheap: `run_dbt(select="+fct_tracking_frames
  …event/360 views")` first (the compute's inputs), then `run_dbt(select="+fct_tracking_metrics")` after the compute.
  The second selector uses the `+` upstream prefix so it rebuilds the layer-1 **staging views** as well as the mart
  (round-1 fix CALC-SPEC-11, round-2 A/B): a bare `tag:tracking_metric` would rebuild only the mart and leave the
  staging views on their empty branch — the exact permanently-empty-mart this ordering exists to prevent. This is
  **one run, strictly forward (A→B→C)** — not v1's cross-run two-pass and not a "today's gold" read. The cost is a
  second detach/attach + `dbtRunner` spin-up, accepted for correctness.

- **Extensibility to training/champion (round-1 fix CALC-SPEC-03).** The compute phase this cycle is a list of
  **per-match → bronze-table** steps. A future training/champion phase does **not** share that output shape, and
  the design does not claim it does. It attaches as a **sibling phase**, sketched (not built) as: a training step
  runs locally or on HF Jobs, writes a **model artifact** to a local or object-store path (optionally mirrored to HF
  as a non-load-bearing copy), and records a **champion pointer** in an `ops_*` table; downstream inference reads the
  champion pointer, never an HF URL. Nothing this cycle builds blocks adding that sibling: the per-match seam owns
  no shared mutable contract a model phase would have to undo. The training-phase contract is designed in its own
  cycle; this cycle only keeps the door open.

## D3 — the slice

**Layer 0 — input views (dbt).**
- `fct_tracking_frames` (gold view) over `tracking_cross_provider` (already in v2): per-frame player position +
  `team_key`/`canonical_team_key` (added by the D7 amendment) + `provider` (added by D8 / ruling R6), keyed by
  canonical match/player pointers. **Positions + team + provider only — no velocity and no `is_goalkeeper`**;
  velocity and goalkeeper are compute-pass concerns (via the silly-kicks preprocess seam + the sourced-GK path, D8),
  derived in the compute, never in SQL (Constraints + D1). The `fct_tracking_ball` view (D7) is the other layer-0
  input.
- ~~Event + 360 input views~~ — **superseded by D9 (CALC-A2-01 / reviewer B).** The event/360 views are **not**
  built: D9 replaces line_breaking's inputs with the SPADL `fct_actions` layer (events → SPADL actions) + the
  tracking frames. The StatsBomb-360 view is dropped (StatsBomb has no v2 tracking). See D9.

**Layer 1 — heuristic computes (independent of each other; each writes one bronze table). Revised by D9 — the layer-1
set is `spadl_actions` + `pitch_control` + `shape_graphs`, and `line_breaking` moves to layer 2 (reads `fct_actions`):**

| Step | Method | Input view(s) | Writes (bronze) |
|---|---|---|---|
| `spadl_actions` (D9; amended) | per-provider events → SPADL via silly-kicks converter | event bronze (statsbomb, wyscout, idsse, skillcorner, gradientsports) | `spadl_actions` |
| `pitch_control` | Spearman physics surfaces (heuristic) | `fct_tracking_frames` + `fct_tracking_ball` | `pitch_control_values` |
| `shape_graphs` | Delaunay shape-graph formation/positions (heuristic) | `fct_tracking_frames` + `fct_tracking_ball` | `shape_graph_labels` |
| `line_breaking` (**layer 2**, D9) | actions × frames defensive-line crossing (heuristic) | `fct_actions` + tracking frames | `line_breaking_results` |

All are `type: heuristic`, train nothing, and read no HF artifact. **The input column above is revised by D9**: the
event/360 views are gone; `line_breaking` reads `fct_actions` and runs in **layer 2** (see D9's layered seam).

**D9 amendment (2026-11-05 — owner: "SPADL for every provider we currently ingest, do not skip any").** `spadl_actions`
runs for **all five** providers v2 ingests — `statsbomb`, `wyscout`, `idsse`, `skillcorner`, `gradientsports` — not
only the three with tracking. statsbomb and wyscout are **events-only** in v2 (no continuous tracking), so they
contribute actions to `fct_actions` but **no frames**; the tracking computes (`pitch_control`, `shape_graphs`,
`line_breaking`) correctly stay on the three tracking providers ("tracking-related is obviously not all"). Each
events-only provider gets its own `_SpadlActions<Provider>Step` (a no-op when the provider's event bronze is absent,
via `_distinct_match_keys`) and a Workflows-page card; `fct_actions` unions all five. The statsbomb adapter rebuilds the
converter's nested `extra` dict from v2's flattened `_TYPE_OBJECTS` columns and passes `timestamp` (a `pa.duration('ms')`
→ Timedelta on the encrypted-lake read) straight to the converter's `pd.to_timedelta`; the wyscout adapter passes
`positions`/`tags` through (v2 keeps them nested) normalised to plain-dict lists, and maps `match_period`→`period_id` /
`event_sec`→`milliseconds`. Both canonicalise via `_native_to_canonical` (numeric native ids with surrogate
`player_key`/`team_key` columns). A guard — `test_every_ingested_provider_has_a_spadl_step` — pins the invariant. This
**supersedes** the earlier D9 note that scoped statsbomb/wyscout out ("event-only, no v2 tracking, so no frames for
line_breaking to score against"): their SPADL actions are valuable in `fct_actions` independent of line_breaking. wyscout
had no data in the working lake, so its data is ingested (online `wyscout_public` fetch — figshare 2018 World Cup) as
part of this cycle's e2e, per the owner's "Build + ingest + e2e" ruling.

**Layer 2 — `fct_tracking_metrics` (one dbt gold view, contract enforced).** Grain = **per
`(canonical_match_key, canonical_player_key)`** (round-1 fix CALC-SPEC-07). Each layer-1 output is aggregated to
that player-match grain and joined by `ref()` of its staging view: pitch_control → a controlled-area summary per
player; line_breaking → through-line pass counts per player; shape_graphs → modal position + time-in-position per
player. A `§9` manifest/coverage test is extended to cover the new mart. Exact column list stays with the builder
(D-open-1); the grain, the single-gold-view shape, and the `ref()`-only rule are fixed here.

## D4 — the AI/ML Workflows page in v2

v1 renders the DAG from `workflow-cards/*.yaml` (each card: `id`, `name`, `type`, `status`, `depends_on`, `inputs`,
`outputs`) as a Cytoscape/dagre graph with a detail drill-down (`hf_taipy_app/src/pages/workflows.py`,
`state/workflows_dag.py`). v2 has its own page template (`lakehouse.ui`).

- **Decided (my call):** a typed, v2-native card model (not raw YAML, not a verbatim port of the v1 Taipy page —
  v2's template rules differ, `hf_taipy_app/AGENTS.md`): one record per step (the layer-0 views as sources, the
  three layer-1 computes, the layer-2 mart), rendered through v2's page template. The Cytoscape/dagre rendering
  approach carries over; the page is built the v2 way. The card model carries `inputs`/`outputs` so the no-HF gate
  (D5) can read them as defence in depth.

## D5 — HF publishing is optional (seam defined, build deferred)

- **Code-level gate is the enforcement (round-1 fix CALC-SPEC-04 / B BLOCKING).** The owner's rule — no v2 step
  depends on a published HF dataset or model — is enforced by a **code-level import/AST gate** (the cycle deliverable
  modelled on v1's `resolve_hf_token` AST gate): no `lakehouse.*` compute/mart module may import the HF client or
  read an HF URL as an input. It is watched **red first** (a deliberate violating stub must fail it). The card
  `inputs` test (no `source: huggingface`) stays as **defence in depth**, not the primary gate.
- `import-*` is not ported.
- **The publish sink seam (owner: defer the build; multi-dataset).** The seam is specified now: an optional step
  that reads one or more marts and uploads them; it is a pure sink — the graph has no edge out of it, nothing reads
  its output. **The seam must support N publish targets from the same lake** (owner, 2026-11-04) — e.g. a *public*
  dataset and a *private* dataset published together, each target naming its own source mart(s), its own visibility
  filter, and its own HF repo. No publish code ships this cycle; the seam carries the multi-target shape so the
  later build has a fixed contract (exact per-target config deferred with the build).

## D6 — compute failure and rollback (round-1 fix CALC-SPEC-06 / B)

- **Fail loud, never silently empty.** "No frames for a match" (a legitimate empty input) is distinguished from "the
  compute failed" (an error). An empty input writes nothing and is recorded as such; a compute error is recorded to
  `ops_errors` and the run ends `failed`, exactly like a failed dbt model or match today.
- **Per-match idempotent writes.** Each match's bronze write is partition-scoped and re-runnable; a failed match is
  recorded and does not corrupt a prior match's rows.
- **Second dbt build fails → run failed, prior catalog version stands.** Because the mart is a view, a failed second
  build leaves no partial mart data; the T3 checkpoint means the last committed catalog version is what readers see.
  The empty-safe staging view is for a *fresh* lake (first run, no bronze yet), not a mask for a failed compute —
  the fail-loud rule above is what catches a failed compute.

## What is left to the builder

- **D-open-1:** the exact `fct_tracking_metrics` column list and the per-step aggregation columns. The **grain is
  fixed** (per `(canonical_match_key, canonical_player_key)`), the single-gold-view + `ref()`-only shape is fixed;
  the owner is asked to confirm leaving only the column list to the builder.
- The per-match compute harness in v2 (match iteration over DuckDB, memory bounds, the skip-guard read).
- Exact silly-kicks APIs and pinned versions for the three scorers, and the velocity `PreprocessConfig`.
- Whether `fct_tracking_frames` is a new gold view or an extension of `tracking_cross_provider`; the exact event/360
  view set line_breaking needs.
- **Resolved, not open:** D-open-2 (HF publish) — seam defined, build deferred (owner). D-open-3 (card model) —
  typed v2-native (D4).

## D7 — team-enriched tracking frames (amendment, 2026-11-04; owner "grow this cycle")

**Why (found at implementation).** The silly-kicks scorers need each frame as `player_id, team_id, x, y` plus an
`attacking_team_id`; pitch_control also needs the ball. v2's tracking **core** (`TRACKING_PLAYERS_CORE` →
`stg_*__tracking_players` → `tracking_cross_provider` → `fct_tracking_frames`) is **positions-only: no `team_key`**,
and the ball is a separate per-frame bronze table (`*_tracking_frames`: `ball_x_mm/ball_y_mm`). So the layer-0 view
as specified cannot feed the computes. The data **exists** — it is not an ingestion gap — so this amendment surfaces
it rather than re-ingesting.

**Verified facts.** The three v2 tracking providers are **idsse, skillcorner, gradientsports** (metrica is NOT a v2
provider — no parser, no staging, excluded by the ingestion spec; it is out of this slice). Each one's
`*_tracking_players` bronze carries `team_key` as an extra column beyond the shared core (verified: skillcorner,
idsse, gradientsports). A per-frame possession signal exists per provider: idsse `ball_possession` (1=home, 2=guest);
skillcorner `possession.player_id` / `possession.group` on the tracking frame (NOT `team_in_possession`/
`attacking_side`, which are phase/event-grain) — normalised to a team via the match roster (`possession.group`
home/away, or `possession.player_id` → `team_key`), never guessed; gradientsports frames are written per possession
event. statsbomb and wyscout have no continuous tracking, so they feed no team-position frame; statsbomb's 360
freeze-frames do feed line_breaking as an event/360 input (not as tracking), and wyscout feeds nothing here.

**The model (decided; provider detail to the builder):**
- Extend each `stg_*__tracking_players` to select the existing `team_key` extra column (and its empty-branch twin).
- `tracking_cross_provider` and `fct_tracking_frames` gain `team_key` + `canonical_team_key` (resolved via the same
  `ops_canonical_pointers` join the match/player keys use). `fct_tracking_frames` stays positions + team only — still
  no velocity, still no `is_goalkeeper`, both compute-pass concerns (velocity via the silly-kicks preprocess seam,
  goalkeeper via silly-kicks), as the landed Task 3 view already is.
- New layer-0 ball view `fct_tracking_ball` (contract, tagged `compute_input`) over `*_tracking_frames`:
  per `(canonical_match_key, period, frame)` → `ball_x_mm, ball_y_mm, ball_detected`, plus the provider possession
  signal normalised to a `possession_team_key` (canonical) column where the provider supplies it, null where it does
  not.
- `attacking_team_id` per frame: the compute uses `possession_team_key` where present; where a provider has no
  possession signal, the **documented fallback** is to compute pitch-control for both teams (no possession guess) —
  the exact fallback per compute is builder detail, but a compute MUST NOT invent possession.

**Non-goal unchanged:** no training. **New anti-goal:** inventing a team membership or possession value a provider
did not supply (a wrong `team_key`/`attacking_team_id` silently corrupts every downstream metric) — the enrichment
carries only what the provider delivered, null otherwise.

This amendment updates D1/D3 (the layer-0 inputs now include team + ball), and the plan adds a team+ball enrichment
task before the computes. It has NOT been reviewed: it goes to an A/B spec re-review (round 3) and owner approval
before the enrichment or the computes are built. Tasks 1–3 already landed green are unaffected except that
`fct_tracking_frames` gains the two team columns (Task 3's test updates with the enrichment task).

## D8 — silly-kicks compute integration contract (amendment, 2026-11-04; corrects R4)

**Why (found at implementation, confirmed by a silly-kicks session).** A spike proved the scorers' real input
contract, which the spec left to the builder (D-open) and ruling R4 guessed wrong. R4 said "goalkeeper via
silly-kicks" as if the scorer resolves it. It does not: `compute_pitch_control` raises `KeyError('is_goalkeeper')` —
the **caller** supplies the goalkeeper flag. The silly-kicks maintainers confirmed the full 4.139.1 contract. This
amendment records it so the computes use silly-kicks' intended seams, not hand-rolled heuristics.

**The contract (silly-kicks 4.139.1, authoritative).**

> **Superseded in part (2026-11-05) — read the "D8 implementation reconciliation" at the end of this section as
> authoritative for what shipped.** Two items below are REPLACED by it: the goalkeeper-sourcing bullet (native via
> `keeper_identity`) and the frame "model" (shared provider-agnostic builder + `to_spadl_ltr`). The shipped contract is
> per-provider `silly_kicks.tracking.<provider>.convert_to_frames` + the rostered-keeper SET
> (`fct_roster_gk` / `resolve_roster_goalkeepers`), with `derive_goalkeepers` as the fallback. Everything else in this
> contract (the dtypes, the 0-105/0-68 coords, the per-scorer needs, the no-hand-rolled-GK anti-goal) stands.
- Canonical frame = `TRACKING_FRAMES_COLUMNS` (dtypes load-bearing): `game_id int64, period_id int64, frame_id int64,
  player_id Int64 (NA on the ball row), team_id category, is_ball bool, is_goalkeeper bool, x/y float32`, plus
  `is_goalkeeper_source` in `{"native","derived"}`. Chain: build frame → coords to the SPADL pitch frame → resolve
  goalkeeper → `smooth_frames` → `derive_velocities` → scorer.
- Coordinates MUST be the SPADL pitch frame, **0-105 × 0-68 metres**. `compute_pitch_control` scores a hard-coded
  0-105/0-68 grid; `derive_goalkeepers` raises if coords are outside it. v2 `x_mm`/`y_mm` are centred provider metres
  → transform first, using the per-match pitch size (idsse `environment_pitch_x_mm/y_mm`, skillcorner
  `pitch_length/width`, gradientsports `stadium_pitches[].length/width`). The transform is a silly-kicks seam
  (`spadl.orientation` / `to_spadl_ltr` family), never hand-rolled.
- **Goalkeeper is sourced, never inferred by the scorer.** Two sanctioned sources, in order: (a) **native** — the
  per-provider roster position (`statsbomb_lineups.positions`, `idsse_persons.playing_position`,
  `skillcorner_players.player_role_*`, `gradientsports_rosters.position_group_type`, `wyscout_players.role_*`) fed
  through `silly_kicks.keeper_identity` (`resolve_keeper_identities` + `apply_keeper_identities_to_frames`,
  substitution-safe), stamped `is_goalkeeper_source="native"`; (b) **derived** fallback —
  `silly_kicks.tracking._gk_identification.derive_goalkeepers` (ADR-007 geometric; a PRIVATE import whose pin is
  recorded), stamped `"derived"`, used only where no roster position exists. `_gk_median`/`gk_*` consume the flag and
  never produce it.
- Per scorer: `pitch_control` needs `is_goalkeeper` + vx/vy + pitch-frame coords, **no** attacking direction.
  `shape_graphs` (`add_shape_graph`) needs `is_goalkeeper` + an attacking direction (`play_left_to_right` /
  `orient_frames_to_ltr`). `line_breaking` is an ACTIONS × FRAMES scorer — see D9.

**The model (decided; exact APIs to the builder, pinned + round-trip-tested).**
- A shared, provider-agnostic **frame builder** turns `fct_tracking_frames` + `fct_tracking_ball` for one match into
  the canonical `TRACKING_FRAMES_COLUMNS` frame: coords → 0-105/0-68 (per-match pitch size), `team_key` → `team_id`
  (via `silly_kicks.id_compat`), ball row with `player_id` NA, `is_goalkeeper` via the roster port with the derive
  fallback. pitch_control and shape_graphs both consume it.
- `fct_tracking_frames` gains exactly **one** column from this amendment — `provider` (ruling R6, below). It is
  otherwise **unchanged**: still positions + team, NO `is_goalkeeper`, NO velocity (D3/R4 as to the view). The
  goalkeeper flag and velocity are added in the compute pass on the in-memory frame, exactly where the spec always
  put them — R4's error was only the claim that the scorer resolves GK, not the view's columns.
- `provider` is added to `fct_tracking_frames` (ruling R6): the frame builder needs it to pick the per-match
  pitch-size source and the roster source. One extra passthrough column, carried like `team_key` (CALC-A2-03: this is
  the one column change; the "unchanged" statement above is scoped to velocity/goalkeeper).

**New anti-goal:** a hand-rolled goalkeeper heuristic (e.g. "furthest-back player = GK"). GK comes from the roster
port or `derive_goalkeepers`, nothing else. Corrects ruling R4; subsumes R6 (provider column) and R6b (no timestamp —
the time axis is `frame_id` + `frame_rate`).

**D8 implementation reconciliation (2026-11-05; from the A/B impl review — pending the amendment review + owner approval).**
The frame builder diverged from the named D8 contract above on two points the reviewers raised (A-IMPL-01,
B-CALC-IMPL-01). The owner chose "Reconciled" (2026-11-05); the shipped contract is:
- **Frame geometry via the per-provider `silly_kicks.tracking.<provider>.convert_to_frames`, not a hand-rolled
  `to_spadl_ltr`.** `build_match_frame` dispatches by `provider` to that converter, which bundles the coordinate
  transform, the per-period direction, AND the positional derive-GK fallback. This supersedes the "shared
  provider-agnostic builder + `to_spadl_ltr`" model (rulings R7/R8): it is cleaner — silly-kicks owns the math — and
  imports no private silly-kicks symbol (which also moots CALC-A2-04).
- **`is_goalkeeper` from the rostered-keeper SET, not `keeper_identity`.** `keeper_identity`
  (`resolve_keeper_identities` / `apply_keeper_identities_to_frames`) is substitution-safe but REQUIRES the `actions`
  table (it links actions↔frames and reads goal-kick events). The frames for `pitch_control`/`shape_graphs` are built in
  layer 1, BEFORE `fct_actions` exists, so `keeper_identity` cannot run there. Instead `fct_roster_gk` emits EVERY
  rostered or substituted goalkeeper per team — gradientsports `position_group_type ∈ {GK,GOALKEEPER}`, idsse
  `persons.playing_position='TW'` UNION the `Substitution@PlayerIn` where `@PlayingPosition='TW'`, skillcorner
  `player_role_acronym='GK'` — and `resolve_roster_goalkeepers` flags `is_goalkeeper` for any player in a team's GK set.
  A starter AND a substitute keeper are each flagged on their OWN frames, so a GK substitution is correct
  (substitution-safe — the D8 requirement B-CALC-IMPL-01 found violated by the earlier one-keeper-per-team view),
  including a late sub the roster/substitution source lists. A team with no roster GK still falls to
  `convert_to_frames`' derive (`derive_goalkeepers`, the D8 "derived" source). Ruling R9; adds two layer-0 views
  (`fct_match_params`, `fct_roster_gk`) not in the original plan.
- **R9 amendment (2026-11-07, owner "option 1"): the skillcorner frame rate comes from the tracking, not the match
  file.** The owner-tier (RM-format) skillcorner match file has no `match_periods`, so the first view, which took the
  rate only from `match_periods`, gave null for every owner-tier match. The Mac clean-slate run then failed every
  skillcorner `pitch_control` and `line_breaking` match. The parser now derives `frame_rate` from the match-clock
  cadence with the v1 rule (`src/ingestion/skillcorner_tracking.py` `derive_frame_rate`: 1/median step within each
  period, snapped to 10, 25 or 30 within 5 %, fail loud, never a default) and writes it to bronze
  `skillcorner_tracking_frames` (skillcorner `PARSER_VERSION` "3"). `fct_match_params` reads that column and has a
  dbt `not_null` test on `frame_rate`. `build_match_frame` raises on a null rate.
- **SPADL skip guard (2026-11-07, owner: "every time like this needs a skip guard").** Each `spadl_actions:<provider>`
  step skips a match whose exact input key matches the `_source_hash` of its `spadl_actions` rows. The key is the
  in-DuckDB hash of the step's declared per-match reads (`SpadlRead`), the canonical pointers of the match's own match,
  player and team keys, the code digest of `SPADL_CODE_MODULES` and the library versions (the tracking-step key). The
  converter gets only those reads, so the key covers every input. A pointer of another match does not re-compute it.
- **GS time base (2026-11-07, owner: port v1 ADR-040).** GS action `time_seconds` is `startGameClock` minus the
  nominal period start (period 0, the pre-match marker, and 1: 0 s; 2: 2700; 3: 5400; 4: 6300; 5: 7200 s), so it is
  period-relative like the GS frames and every other
  provider. The 2026-11-06 Mac lake held GS period-2 actions at 2700 s+ and line_breaking linked 23 % of them.
- **GS tracking is clean before the computes (2026-11-08, owner: fix in staging).** GS bronze keeps every frame
  record (one per possession event, up to 16 copies of a frame) and both the raw and the provider-smoothed player
  rows. Silver keeps only the raw rows (v1 scored raw positions; silly-kicks smooths) and the first record of each
  (period, frame) by `record_index` (v1 ADR-030 keep-first), for the players and the ball. The frame builder fails a
  match with any duplicate (period, frame, player) or (period, frame) ball row. A full-table dbt test was rejected: it
  does not scale and crashed the dbt build.
- **line_breaking time-base guard (2026-11-07, owner: the v1 ADR-040 rule).** Per period, the first action and the
  first frame must both be before 1800 s, and at least 50 % of the actions inside the period's frame coverage must
  link to a frame. A failure raises `TimeBaseError`: the match is `compute_failed` and no rows are written. **The
  coverage (ruling 2026-11-07, after the fixture-slice conflict was shown to the owner)** is the frames' contiguous
  spans: consecutive frames at most 1 s apart (`COVERAGE_GAP_SECONDS`), not [first frame, last frame]. The idsse
  fixture slice is sparse, and the window rule failed a correct match on one action in a gap. Real tracking at
  10-25 fps is continuous, so a covered action that does not link is still caught. Cost: an action inside a real
  tracking gap over 1 s is not counted.
- **Exact int64 canonical keys (2026-11-08, owner).** The SPADL steps map native ids to canonical keys as Python ints
  into an int64 array, never through a float. Reason: `Series.map(dict)` returned float64 when one actor was null and
  rounded the 64-bit keys, so 90053 of 90831 gradientsports actions named no dimension row and line_breaking wrote
  100 % null results with 0 failed matches. A non-null native id with no canonical key fails the match
  (`compute_failed`); a null actor keeps a null key.
- **line_breaking action-key guard (2026-11-08, owner: "line_breaking fails loud").** `check_action_keys` raises
  `FrameKeyError` (the match is `compute_failed`) when a period has linked frames but no action (period, team) key
  matches a frame row. Reason: the rounded keys above gave all-null results that no guard caught. Known limit, per the
  ruling's text: a period passes when one team's key matches, so a defect in one team's key alone stays a null
  result (impl review r2, A PERF-IMPL-14 / B-09).
- **statsbomb match records from the metadata (2026-11-08, owner: "Add metadata, make sure pining can deliver").** The
  2026-11-08 lake held no statsbomb match record in `ops_entity_records`, so the 95 statsbomb matches in `fct_actions`
  had no `dim_matches` row. The match and competition records also read `statsbomb_match_metadata`
  (pining delivers it for all 327 matches), after the listing: the listing query runs first, so its row wins when
  both hold the match.
- **Known limitation: the gradientsports action clock (2026-11-24, owner: "Mark it, fix in upgrade").** GS
  `time_seconds` comes from `startGameClock` (2026-11-07 ruling above), which times the start of the on-ball sequence.
  On the Mac lake (140909 GS events) it is a median 0.78 s before `eventTime` minus the period kickoff (p25 1.74 s,
  p05 4.2 s). So line_breaking judges a GS action on an early frame. No guard detects this. silly-kicks 4.144.0
  (ADR-133) derives the GS action time from `eventTime` in its converter, so the silly-kicks upgrade cycle fixes it
  (D10 item 6); the 4.144.0 converter refuses the current adapter input loud, so the change cannot be missed. On the
  converted GS actions the offset is a median 0.81 s (154065 action-to-possession-event pairs, not one-to-one;
  silly-kicks' exact pairing: 1.29 s). Owner 2026-11-24 (impl review r3, A PERF-IMPL-17): the
  `spadl_actions:gradientsports` card is `incomplete`, and it, `fct_actions`, `line_breaking` and the mart card say
  "about 1 s early".
- **skillcorner converter parameters only with tracking (2026-11-24, owner: "Scope to tracking matches").**
  `fct_match_params` has a skillcorner row only for a match with tracking frames. An events-only skillcorner match
  has nothing to convert, and its null rate would fail the dbt `not_null` on `frame_rate`, and so the whole run.
  Without a row, line_breaking discovery (which joins `fct_match_params`) does not visit it either. On 2026-11-24 no
  listed skillcorner match lacks tracking (0 of 909).
- **Anti-goal unchanged:** no hand-rolled "furthest-back = GK"; GK is the roster set or `derive_goalkeepers`.

## D9 — SPADL actions layer + line_breaking on actions × frames (amendment, 2026-11-04; owner "build a SPADL actions layer" + "one mega-cycle")

> **Cross-reference (2026-11-06):** the run-time design of these computes at production scale — the exact input skip
> key, one frame build for pitch_control + shape_graphs, numba, and the deadline / defer / resume control — is
> `docs/specs/2026-11-05-v2-tracking-compute-performance-design.md`, recorded in ADR-017.

**Why (found at implementation).** The spec (D3 / ruling R5) assumed `line_breaking` reads event + 360 **views**. The
silly-kicks scorer (`detect_line_breaking` / `add_line_break`) is an **ACTIONS × FRAMES** scorer: it needs a **SPADL
actions table** (one row per on-ball action: `period_id` + period-relative `time_seconds`, coords 0-105/0-68,
`type_id`/`result_id`) aligned to the tracking frames, plus `is_goalkeeper` + a resolved attacking direction. v2 has
**no SPADL actions layer**, so line_breaking cannot be built from event views — it needs an events→SPADL-actions layer
first. The owner chose to build that layer and to keep it in one mega-cycle (2026-11-04).

**Verified facts (silly-kicks 4.139.1 + v2 bronze).**
- silly-kicks ships per-provider SPADL converters: `silly_kicks.spadl.{statsbomb, wyscout, gradientsports, sportec,
  opta, skillcorner, kloppy, metrica}`. Output = `SPADL_COLUMNS` (already 0-105/0-68, period-relative `time_seconds`)
  + the output gate `validate_spadl`. Orientation is silly-kicks-owned (`spadl.orientation`:
  `detect_input_convention`, `to_spadl_ltr`, `play_left_to_right`, `require_et_direction`).
- v2 event bronze exists, **flattened** by the v2 parser: `statsbomb_events` (keeps `location` list + `type_name`;
  `extra.*` exploded into columns), `wyscout_events` (`positions`, `tags`, `event_sec`, `match_period`),
  `idsse_events` (DFL-wide per-event-type columns, `start_frame`, `x_position_mm`), `gradientsports_events`
  (`gameEvents.*` + `possessionEvents.*`, `game_events_period`, `home_team_start_left`).
- **Modality matrix (corrected — CALC-A2-02 / reviewer A).** line_breaking needs tracking frames AND SPADL actions
  for the **same match**. Three providers are dual-modality in v2: **gradientsports** (tracking + `gradientsports_events`;
  `home_team_start_left` present), **idsse** (tracking + `idsse_events`), and **skillcorner** (tracking +
  `skillcorner_dynamic_events`, with `silly_kicks.spadl.skillcorner`). statsbomb and wyscout are **event-only** (no v2
  tracking) — their SPADL actions would have no frames to score against, so they are out this cycle. So the choice is
  **not** "which providers have both" — it is which of the three dual-modality providers to convert first.

**The model (decided; the provider scope is my call, surfaced for A/B + owner to veto).**
- New compute-phase sibling (same D1/D2 seam, writes bronze via the v2 writer): a **SPADL actions compute** per event
  provider turns that provider's flattened event bronze into SPADL actions, through a per-provider **adapter**
  (flattened v2 bronze → the silly-kicks converter's documented input) + the silly-kicks converter + `validate_spadl`.
  One bronze table `spadl_actions`, keyed per `(canonical_match_key, period_id, action_id)`, carrying
  `canonical_player_key` / `canonical_team_key`.
- Staging view `stg__spadl_actions` (`lake_table_present`-guarded) → gold view `fct_actions` (contract enforced),
  per D1.
- **Providers this cycle = gradientsports + idsse + skillcorner** — all three dual-modality providers (owner widened
  the scope on 2026-11-04 from the gradientsports + idsse recommendation). gradientsports and idsse have **direct
  event-stream** converters (`spadl.gradientsports`, `spadl.sportec`); **skillcorner's converter is inference-based**
  (`spadl.skillcorner` / `_skillcorner_inference.py`) over its own `skillcorner_dynamic_events` model — a different,
  higher-risk adapter (see Known risk). statsbomb / wyscout are out (event-only, no v2 tracking, so no frames for
  line_breaking to score against). The adapter is a provider-pluggable seam; the event-only providers extend later for
  future event computes with no code this cycle.
- **line_breaking becomes an actions × frames compute.** Per match it builds the canonical frame (D8) + reads
  `fct_actions`, resolves the attacking direction (`play_left_to_right`, using `home_team_start_left` where the
  provider supplies it — gradientsports does), calls `detect_line_breaking`, and writes `line_breaking_results`. Its
  input views (D2 / R5) change from "event + 360 views" to **`fct_actions` + `fct_tracking_frames` +
  `fct_tracking_ball`**. The StatsBomb-360 view (R5) is **dropped** from line_breaking's inputs — 360 is
  StatsBomb-only and StatsBomb has no tracking, so it never fed this scorer; this removes the 360-view work R5 added.
- **Run seam becomes layered (revises D2 and Task 1).** line_breaking reads `fct_actions`, a dbt view over the
  `spadl_actions` **bronze** that another compute (the SPADL-actions step) writes. A compute reading another compute's
  output through a dbt view is a **compute dependency**, which D2's fixed two-build seam (`dbt → compute → dbt`) does
  not express. The clean-forward fix — not an intra-run compute-reads-bronze hand-off, which is exactly the unclean
  pattern v2 exists to remove — is a **two-stage compute phase**: a step declares its `layer` (1 reads layer-0 views;
  2 reads a layer-1 output view). The seam runs
  `resolve → dbt(full build, `select=None` — R1: all layer-0 input views) → compute(layer 1: spadl_actions, pitch_control, shape_graphs) → dbt(+fct_actions
  only — the one layer-1 view a layer-2 step reads) → compute(layer 2: line_breaking) → dbt(+fct_tracking_metrics:
  the pitch_control + shape_graphs + line_breaking staging views + the mart) → maintain`. Three view-only dbt builds,
  two compute stages. `spadl_actions`, `pitch_control`, `shape_graphs` are **layer 1** (read only layer-0 views);
  `line_breaking` is the sole **layer 2** step (reads `fct_actions`). The middle build is `+fct_actions` **only** —
  the pitch_control/shape_graphs staging views have no layer-2 consumer, so they build in the final `+fct_tracking_metrics`
  pass; no model is built in two of the three builds (CALC-A2-01/02). This generalises the landed Task-1 seam from a
  fixed two-build to a layer-driven build/compute alternation, bounded to the two layers this slice needs (a general
  N-layer DAG is YAGNI). Each build stays `lake_table_present`-safe (D2), so a fresh lake still builds empty-but-typed.
- Time alignment (clarified — CALC-A2-04 / reviewer B). Two distinct time axes must not be conflated: (a) inside the
  frame builder, silly-kicks' `smooth_frames`/`derive_velocities` use `frame_id` + `frame_rate` to derive velocity —
  that is the **preprocess** axis (D8) and is internal to the frame. (b) To align an action to a frame, line_breaking
  converts both to **period-relative seconds**: the SPADL action already carries period-relative `time_seconds`; each
  frame's period-relative seconds come from the provider's per-frame elapsed-time column where present
  (gradientsports `timestamp` = `periodElapsedTime`) and from `frame_id / frame_rate` where it is not (idsse). The
  compute matches on `(period_id, seconds)` to the nearest frame within a tolerance; the tolerance is builder detail,
  bounded by a fixture test. The builder uses the provider's per-frame elapsed time as the authoritative clock when
  the provider supplies it.

**Known risk (named, not hidden).** Two of the three provider adapters are higher-risk than gradientsports:
- **idsse→SPADL** uses silly-kicks' DFL/sportec converter (kloppy-oriented) while v2's `idsse_events` is a flattened
  DFL XML dump — the adapter (flattened bronze → the converter's input) is the heaviest.
- **skillcorner→SPADL** uses silly-kicks' **inference-based** converter (`spadl.skillcorner` / `_skillcorner_inference.py`)
  over `skillcorner_dynamic_events` (SkillCorner's own derived event model, not a raw event stream) — a different
  conversion path from the two direct converters.

**Build order:** **gradientsports first** (direct converter + `home_team_start_left`, the cleanest), each proven green
before the next; then idsse and skillcorner. If either higher-risk adapter proves disproportionate to this cycle,
dropping that one provider from `spadl_actions` + line_breaking is a **cut** and returns to the owner — it is not
taken silently. The pluggable adapter seam means a dropped provider leaves the other two intact.

**New anti-goal:** emitting a SPADL action a provider did not encode, or scoring line_breaking on an unaligned
action/frame pair (a mis-aligned action silently mis-scores). The adapter carries only converted provider events;
alignment failures are recorded, never guessed.

**Not reviewed.** D8 + D9 and the plan changes go to an A/B amendment review and owner approval before any compute,
SPADL, or line_breaking code is built. Tasks 1–3b (green) stand; `fct_tracking_frames` gains `provider` (D8 / R6)
when the computes land. This amendment revises D1/D2/D3 (the computed-metric pattern now includes the canonical frame
builder and the SPADL actions sub-layer; line_breaking's inputs change) and the layer-1 table in D3.

## D10 — pitch_control and shape_graphs leave the default run; idsse SPADL ships known incomplete (owner ruling, 2026-11-23)

**Status:** owner ruling 2026-11-23; A/B delta review round 1 answered (reports
`D:\Development\_reviews\2026-11-23-v2-calc-d10-default-run-amendment-plan-{A,B}.md`). **D10 governs** where earlier
text in this spec, the performance spec or the performance plan lists pitch_control and shape_graphs as steps of the
default run.

**Why.** The silly-kicks tracking-batch cycle is late. It brings `compute_pitch_control_batch`,
`compute_shape_graphs` and the DFL event-period derive. The current v2 scorers are single-core. The Mac measurements
are about 2.9 ms per frame for pitch_control and 1900-2500 s of pitch_control plus shape_graphs per gradientsports
match. The largest gradientsports match takes 4858 s for shape_graphs alone. The silly-kicks upgrade replaces the code
of both scorers, so work to speed up the current code has no value. The owner chose to finish this cycle without
the two metrics and to ship idsse SPADL marked as known incomplete.

**Terms.** A step is *excluded* when it is registered but not in the default run. This is different from a *deferred
match* (`ComputeOutcome.deferred_matches`, ADR-017 item 6), which is a match a run did not reach before its deadline.

**Ruling.**
1. **The default run.** `DEFAULT_COMPUTE_STEPS` no longer holds `TRACKING_FRAME_METRICS` (the pitch_control and
   shape_graphs group). `REGISTERED_COMPUTE_STEPS` lists every step. `EXCLUDED_COMPUTE_STEPS` lists the steps that
   are registered but excluded. `STEP_OUTPUTS` and the workflow cards use the registered steps. The code, its tests,
   the bronze tables, the staging views and the mart columns stay.
2. **The mart.** `fct_tracking_metrics` keeps its columns. It left-joins the two metrics, so
   `mean_controlled_fraction`, `modal_position` and `time_in_position` are null for a match without pitch_control or
   shape_graphs rows. The staging views are empty-but-typed until the bronze table exists (D2). Rows that runs wrote
   before 2026-11-23 stay in bronze and stay visible. No skip key re-checks them, because an excluded step does not
   run and the mart does not compare `_source_hash`. They are re-checked when the upgrade cycle runs the step again.
   The Mac lake holds such rows from its 2026-11-08/09 runs. A clean-slate lake has none.
3. **Not silent (anti-goal D6).** `CardStatus` gains `excluded` (built, not in the default run) and `incomplete`
   (it runs, and its output is known to be partial). A card with either status must carry a `note` that says why and
   what is missing. The AI/ML Workflows page shows the status and the note in the detail table, and marks the node in
   the DAG.
   - **Rule:** a card whose input card is `excluded` or `incomplete` is not `active`. A partial or missing input
     makes the output below it partial. A test checks every edge of the DAG, so the rule holds along every path.
   - The pitch_control and shape_graphs cards are `excluded`.
   - `spadl_actions:idsse` is `incomplete`, and so are the cards below it and below the excluded cards: the
     `fct_actions` view, `line_breaking` and the `fct_tracking_metrics` mart. Each note names the gap it carries.
   - `spadl_actions:gradientsports` is `incomplete` too (owner 2026-11-24): its actions are timed about 1 s early
     until the silly-kicks upgrade (Amendments, "Known limitation: the gradientsports action clock").
   - A test ties a compute card's `excluded` status to the absence of its step from `DEFAULT_COMPUTE_STEPS`, in both
     directions.
4. **idsse SPADL ships known incomplete.** v2 places idsse events by `calculated_frame`, and only shots carry it. The
   Mac lake keeps 18-27 actions per match from about 1500 events. The fix is the silly-kicks DFL event-period derive
   (handoff `D:\Development\_handoffs\silly-kicks-dfl-event-periods\`). idsse line_breaking reads `fct_actions`, so it
   has the same gap: its `line_breaking_passes` values for idsse matches are far too low.
5. **No new run option.** No CLI or config option puts the excluded steps back into a lake run. The upgrade cycle
   puts them back in `DEFAULT_COMPUTE_STEPS`. `scripts/profile_tracking_compute.py --step` still scores one match for
   measurement, and the tests still run the group directly.
6. **Re-entry (the silly-kicks upgrade cycle, not this one).** That cycle:
   - moves pitch_control to `compute_pitch_control_batch`;
   - moves shape_graphs to `compute_shape_graphs` and keeps only players with `shape_graph_source == "ok"`;
   - moves idsse SPADL to the DFL derive;
   - moves the gradientsports action time to the converter's `eventTime` derive (silly-kicks 4.144.0, ADR-133;
     owner 2026-11-24, impl review r2 A PERF-IMPL-12), then recomputes the GS SPADL actions and line_breaking;
   - bumps the silly-kicks pin;
   - puts the group back in `DEFAULT_COMPUTE_STEPS`;
   - runs the real-scale gates that moved with the group: the S oracle and the pitch_control backend bench (perf
     plan Task 9), on the new scorers;
   - re-runs perf plan Task 9 Step 6 (the line_breaking chunk gate) and the Task 11 Step 3 idsse calibration
     (`worst_match_seconds`) after the DFL derive, because this cycle measures idsse line_breaking on the incomplete
     action set;
   - sets these cards back to `active`: `pitch_control`, `shape_graphs`, `spadl_actions:idsse`,
     `spadl_actions:gradientsports`, `fct_actions`, `line_breaking` and `fct_tracking_metrics`. The edge-rule test
     then allows it only when no input card is still marked.

**Effect on the performance plan.** `docs/plans/2026-11-06-v2-tracking-compute-performance-plan.md` "Amendment
2026-11-23" reduces Tasks 9-11 to the steps that stay in the default run.

## Amendments

When the plan lands, the ingestion design `docs/specs/2026-10-26-v2-ingestion-design.md` run-step list (§10.3) is
amended to add the compute phase between dbt and maintenance, and the native-table references (§9, §11.1, §11.6) are
cited for the bronze-backed metric pattern. Listed here then.

## Dispositions (round 1)

- **CALC-SPEC-01 (A, velocity in a SQL view) — fixed.** Layer-0 view carries positions only; velocity derives in
  the compute pass via the silly-kicks preprocess seam (Constraints, D1, D3).
- **CALC-SPEC-02 (A) / Seam-vs-line_breaking (B) — fixed.** The seam declares input view(s), plural; the claim is
  corrected to "the three are independent of each other"; layer 0 grows to provide line_breaking's event/360 views
  (owner) (D2, D3).
- **CALC-SPEC-03 (A) / Training extensibility (B) — fixed.** The overclaim is removed; a future training/champion
  phase is a sibling with a model-artifact + champion-pointer output, sketched not built (D2 "Extensibility").
- **CALC-SPEC-04 (A) / D5 no-HF enforcement (B BLOCKING) — fixed.** A code-level import/AST gate is the cycle
  deliverable, watched red first; the card gate is defence in depth (D5).
- **CALC-SPEC-05 (A, collapse to one dbt build) — NOT adopted, with evidence.** `lake_table_present` is build-time,
  so a single pre-compute build compiles the staging views to their empty branch and the mart is permanently empty.
  Two selective builds are required; the rationale is now in D2. (No A/B conflict: B flagged the silently-empty-mart
  risk, which the two-build order prevents.)
- **CALC-SPEC-06 (A) / Failure-rollback (B) — fixed.** New D6 specifies fail-loud, the "no frames" vs "failed"
  distinction, per-match idempotency, and the second-build-fails end state.
- **CALC-SPEC-07 (A) / D-open-1 feasibility (B) — fixed.** The mart grain is decided (per player-match); each
  heterogeneous-grain layer-1 output is aggregated to it (D3).
- **§23.6 miscite (A CONSIDER, B SHOULD FIX) — fixed.** Corrected to §9 (+§11.1/§11.6) in all three places; §23.6 is
  named only as the enforcing test.
- **CONSIDERs — fixed.** `shape_graphs` bronze table named (`shape_graph_labels`); the §9 manifest test is named to
  cover the new mart (D3); "~60" → 59.
- **Owner scope confirmations:** HF publish — define-and-defer (owner, 2026-11-04). line_breaking — keep + grow
  layer 0 (owner, 2026-11-04). HF publish seam must be multi-dataset (N targets from one lake, e.g. public + private;
  owner, 2026-11-04), build still deferred (D5). D-open-1 — grain fixed in spec; owner asked to confirm leaving only
  the column list to the builder (B scope note).

## Dispositions (round 2)

Both reviewers APPROVE; all seven round-1 findings verified resolved; no regression.

- **CALC-SPEC-11 (A + B CONSIDER) — fixed.** The second dbt build selector is `+fct_tracking_metrics` (upstream
  prefix), so it rebuilds the layer-1 staging views, not only the mart. A bare `tag:tracking_metric` would have left
  the staging views on their empty branch (D2).
- **CALC-SPEC-12 (A CONSIDER) — fixed.** "UC-volume" (Databricks vocabulary; v2 has no Databricks) is removed from
  the deferred training sketch; it now reads "a local or object-store path" (D2).
- **CALC-SPEC-05 — decline confirmed by both reviewers from `lake_table_present.sql`** (build-time; a single
  pre-compute build yields a permanently-empty mart; two builds required). Durable; not to be reopened.

## Dispositions (amendment #2 — D8/D9 A/B)

Reviewer A REQUEST CHANGES (3 SHOULD FIX, 1 CONSIDER); Reviewer B REQUEST CHANGES (0 BLOCKING, 1 SHOULD FIX, 3
CONSIDER). Both independently **verified the silly-kicks 4.139.1 contract and the modality matrix accurate** against
the silly-kicks clone + the v2 sources; both judged the layered seam JUSTIFIED and the idsse risk honestly surfaced.
All findings are doc reconciliation / cheap cleanups — no design change. Reports
`…-amendment2-{A,B}.md`.

- **A/B disagreement — skillcorner modality (reviewer A right).** A found skillcorner is **dual-modality** (tracking +
  `skillcorner_dynamic_events` + `silly_kicks.spadl.skillcorner`); B repeated the draft's "only gradientsports + idsse
  have both". Verified in the v2 sources + the silly-kicks clone: **A is correct.** The modality matrix (D9) and the
  providers bullet are corrected — the deferral of skillcorner rests on its **inference-based converter**, not a
  missing modality. Surfaced to the owner, who **widened the scope to include skillcorner** (2026-11-04): all three
  dual-modality providers are in this cycle. skillcorner's inference-based converter is a named Known risk; the
  pluggable seam + per-provider build order keep it isolated.
- **CALC-A2-01 (A SHOULD FIX, tag double-build) / CALC-A2-02 (B CONSIDER) — fixed.** The middle build is
  `+fct_actions` **only**; the `tracking_metric_stage1` tag is dropped; `stg__pitch_control_values` +
  `stg__shape_graph_labels` build in the final `+fct_tracking_metrics` pass (they have no layer-2 consumer). No model
  is built in two of the three builds. D9 run-seam bullet + the plan selectors/tasks updated.
- **CALC-A2-02 (A SHOULD FIX, false dual-modality premise) — fixed.** See the A/B disagreement above; modality matrix
  + providers bullet corrected.
- **CALC-A2-03 (A SHOULD FIX, D3/D8 `provider`) — fixed.** D3 layer-0 bullet now lists `provider`; D8's "unchanged"
  statement is scoped to velocity/goalkeeper and states the one added column (`provider`, R6).
- **CALC-A2-01 (B SHOULD FIX, inline supersede markers) — fixed.** Inline "superseded by D9" markers added at the D1
  input-view item, the D2 two-build rationale, the D3 layer-0 event/360 bullet + the layer-1 table + heading; the top
  banner already governed.
- **CALC-A2-04 (B CONSIDER, time axis) — fixed.** D9 now separates the preprocess axis (`frame_id`/`frame_rate`, for
  velocity) from the alignment axis (period-relative seconds); the builder uses the provider's per-frame elapsed-time
  column (gradientsports `timestamp`) as the authoritative clock, else `frame_id / frame_rate` (idsse).
- **CALC-A2-04/A-04 (both CONSIDER, register `derive_goalkeepers` with silly-kicks + request a public GK seam) —
  noted, not done here.** Editing silly-kicks (its `PRIVATE_CONSUMERS.md` / adding a public seam) is outside this
  session's boundary; recorded as a silly-kicks-repo follow-up for the owner. The lakehouse-side mitigation (pin
  recorded in the frame-builder docstring + the round-trip guard test, Task 3c) stands.
- **Could not verify (both, honest):** the exact silly-kicks 4.139.1 call signatures (external to the repo under
  review) and the idsse `idsse_events`/`idsse_persons` fields that underpin the idsse adapter + native GK — these are
  confirmed by the builder at build time (plan Task 3c/4b "confirm + pin"), and the idsse adapter is the named risk
  with gradientsports built first.
- **No round-3 required (recommendation):** 0 BLOCKING; every fix is doc-consistency with no design change. Owner
  approval is the next gate.
