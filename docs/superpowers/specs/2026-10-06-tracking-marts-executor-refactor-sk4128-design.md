# Tracking-Marts Executor Refactor + silly-kicks 4.128 Adoption — Design

**Date:** 2026-10-06 (rev2 — reconciles independent reviews A & B + owner scope decisions)
**Status:** rev3 — **re-review pending** (§4.2 transport re-architected mid-execution). rev2 was APPROVED by both sessions (A-r2 + B-r2); rev3 changes ONLY the §4.2 transport (and its ripples in §9/§11/§12) after implementation exposed that six different-cardinality marts cannot ride one AC-style single-schema `mapInPandas`. Everything else (bump, sentinels, gkdv, shot_freeze, dtypes, DAS audit, governance, re-mat, commit discipline) is unchanged from the approved rev2.
**Repo pin:** `karstenskyt__luxury-lakehouse` @ `d7d1a1e92c57df2cd82dc8b3cdef1b87a9935218` (branch `main`, clean tree)
**Author:** Karsten (with Claude)
**Upstream:** silly-kicks 4.128.0 (BREAKING; ADR-106/107/108), released 2026-10-05
**Reviews reconciled:** `D:\Development\_reviews\2026-10-06-tracking-marts-executor-refactor-sk4128-spec.md` (two sessions; both REQUEST CHANGES, NO BLOCKING, architecture endorsed).

### rev3 changelog (what this revision changes vs the approved rev2)
- **§4.2 transport re-architected** (owner decision 2026-10-06, re-review pending): the six tracking-marts outputs are different-cardinality → cannot share one AC-style single-schema `mapInPandas`, and per-mart rebuilds would pay the BUILD cost 6×. New design = **two-stage build-once → UC-Volume-Parquet spill → six per-mart scoring passes**. The UC-Volume spill is now the PRIMARY transport (was rev2's conditional "Approach 2 / Task 9b" fallback). Also: the scorers' driver Spark reads (`_read_xg_preds`, `resolve_unit_meta`) move to the UDF closure.
- Ripples: §9 (build-spill test + per-mart oracle retained), §11 (ADR), §12 (new risk: UC-Volume read inside a serverless UDF). shot_freeze (single output) stays on the rev2 single-schema `mapInPandas` — it has no multi-output problem.

### rev2 changelog (what this revision changes vs rev1)
- §1.4 / §2 / §4.6 / §8 — **xt_gk_v2 is retrained at 4.128** (owner decision). rev1's blanket "no model retrain" was wrong: `xt_gk_v2` consumes a frame-derived AC feature that re-materialize refreshes. All *other* trainers remain SPADL-safe (full enumeration in §1.4).
- §4.2 (R-4.2-a) — profile-gate metric made precise (pandas-group bytes vs executor RSS; the UDF-group cap basis) and non-vacuity re-aimed at the **new executor instrument**, not the old driver (reviews A-01 / B-02).
- §4.1 (R-4.1-a) — `_topandas_exemptions.yml` is **function-keyed**; only `_read_unit` is removed, shot_freeze is code-deletion-only, `_pool_gkdv` is **preserved** and re-validated (reviews A-04 / B-03).
- §4.4 — the gkdv **pooling stage** (`main_gkdv_pool`/`_pool_gkdv`) is enumerated.
- §4.5 — shot_freeze provider scope: **enable IDSSE, exclude Metrica** (owner decision).
- §6 (R-6-b) — re-anchored: `convert.py:26` is the GradientSports converter, no IDSSE build there; §6 is **mostly verify-only** (shared builder). player_id-stays-Int64 reason corrected to the post-build-mutation rule (review A-03).
- §7 (R-7-d) — `[das]`-drop broadened beyond imports to the `exec_visibility.py:402` probe + version-list string + docstrings (review B-04).
- §9 — transport-neutrality carried by the single-group **oracle** fixture; `batch_size=` value-identity cited; mandatory pinned-4.123 baseline artifact.
- §4.6 / §8 — governance-edit split (review B r2 CONSIDER TM-SPEC-12): the code commit carries only "4.128 retrain pending" markers; produced-model metrics + status flip + governance-test re-run land with the post-merge retrain, so `main` never documents an unproduced model.

---

## 1. Context & Problem

### 1.1 The OOM
The `compute_tracking_marts` drain OOMs (exit 137) on gradientsports units. Confirmed architectural root cause: the path is **driver-bound**.

- Entry point `src/ingestion/tracking_marts_drain.py`: `main_tracking_marts_preflight` bin-packs units with `assign_workers(units, _N_TRACKING_MARTS_WORKERS)` (`_N_TRACKING_MARTS_WORKERS = 8`, L59); 8 workers each loop their units **sequentially** via `drain_worker`.
- `src/ingestion/tracking_marts_driver.py::_read_unit` (L66-132): **L123 `trk_pdf = trk.toPandas()`** materializes an entire `(match_id, period)` half of raw tracking frames onto the driver as pandas; **L126-128** pulls the unit's SPADL actions `.toPandas()`; `_resolve_meta` adds further driver `.toPandas()` calls (L159/L189/L197).
- Orientation/dtype conversion happens in `read_and_build_unit_inputs` (`tracking_marts_driver.py:216`), the shared builder (§6), NOT in `_read_unit`.
- `TrackingMartsProcessor.process` then runs all scorers in that driver process.

Profiled (Databricks one-off, gradientsports 10502 p1 = 1.91M rows / 84K frames, all scorers, fresh process): **3.4 GB peak** (oriented frames alone 1.27 GB float64). A 6-unit loop plateaus ~3.5 GB (no unbounded accumulation). Largest halves: gradientsports 2.4M rows, idsse 1.73M, skillcorner 665K, metrica small. The exact 16 GB real-worker trigger is serverless-opaque, but every plausible trigger (single-unit size, loop accumulation) is eliminated by moving frame materialization and scoring **off the driver onto executors**. (These profiled MB figures are the pre-refactor baseline; a reviewer cannot re-derive them without a run — they are recorded here as the measured basis, and R-4.2-a re-measures them on the new path.)

### 1.2 The reference pattern already in the repo
`compute_action_context` processes the same raw tracking frames **executor-distributed** without OOM (`src/ingestion/action_context.py`):

- Frames stay a Spark DataFrame; only small SPADL actions hit the driver.
- Dispatch is **NOT** `groupBy().applyInPandas` — it is (ADR-045) `trk_sdf.repartition(_UDF_SHUFFLE_PARTITIONS, *keys).sortWithinPartitions(*keys).mapInPandas(mapper, schema)` with `_UDF_SHUFFLE_PARTITIONS = 64` (L87); the dispatch chain is ~L2063-2079. Plain `applyInPandas` was abandoned because AQE byte-based shuffle coalescing collapsed a whole Metrica half into 1 task (measured concurrency 1.00).
- `_make_streaming_group_mapper` (L343-391) adapts a per-group pandas UDF into a `mapInPandas` iterator, with a `carry`/flush discipline over Arrow chunks so each UDF call receives exactly the rows a per-group call would (`spark.sql.execution.arrow.maxRecordsPerBatch` can split a group across chunks).
- Group key: `(match_id, period, frame_batch_id)` where `frame_batch_id = floor(frame / resolve_frame_batch_size(provider))`. `resolve_frame_batch_size` (`src/analytics/action_context/batching.py:63`) returns a small provider-tuned frame window (universal default 250; per-provider `FRAME_BATCH_SIZE_BY_PROVIDER`; env override `AC_FRAME_BATCH_SIZE`). Per-group memory discipline is driven **entirely** by that frame-window size — there is no byte-size check in the dispatch code.
- The resolved `frame_batch_size` int travels in the UDF closure; the closure otherwise captures only small serializable scalars/structures (serverless lazy-capture).

### 1.3 silly-kicks 4.128.0 (BREAKING)
Released 2026-10-05 (ADR-106/107/108). Lakehouse-facing breaks:

- **F1b frame schema (ADR-106):** tracking-frame coordinates store `float32`, compute `float64` (every kernel upcasts at its boundary); `team_id` is `category`; `player_id` stays `Int64` (sk `tracking/schema.py:17-27`). Storage-rounding delta ~1e-5 m (above the trained-model feature-contract atol 1e-6), so every frame-geometry bundled model is re-fit **inside sk** (pulled by the pin bump — no separate lakehouse download).
- **Native DAS (ADR-107/108):** the `[das]` / `accessible-space` runtime extra is **gone** (native engine; `silly_kicks/tracking/_das_engine.py` + `_das_numba.py`). `**kwargs` and `use_progress_bar` are **removed**; new fail-loud `ValueError`s; periodic quadrature **moves every DAS value** (owner-corpus parity shift: gradientsports median 0.00495 / p90 0.153 / max 5.37; idsse 0.00565 / 0.16 / 4.82; skillcorner 0.00633 / 0.19 / 16). Native engine is sentinel-free — no `player_id = "ball"` write (`_das.py:10`). D-KEY: 0 of 980 matches reuse a `frame_id` across periods; frame direction agreed on all 902,184 compared frames (orientation unaffected). *(sk-side parity numbers are quoted from the sk CHANGELOG; not independently re-run here.)*
- **Downstream relay (sk CHANGELOG):** "re-materialize the lakehouse `das_*` and gkdv `delta_das` together with the F1b frame-geometry re-materialize; the `<5` version pin holds."

### 1.4 Lakehouse-trained-model exposure to F1b (the retrain floor)
Every sk-sentinel'd trainer was enumerated for a frame-derived input (a feature computed from raw tracking frames, which F1b moves):

| Trainer | Primary corpus | Frame-derived input? | Retrain under re-mat? |
|---|---|---|---|
| `train_xt_gk_v2_hf.py` | `spadl_action_context` ⋈ `spadl_actions` ⋈ `fct_shot_xg` | **YES** — `ac.pressure_on_actor__andrienko_oval` (`:111,:114`), computed from frames (`enrich.py:319-323`, `methods=("andrienko_oval",…)`) | **YES** (owner decision, §4.6) |
| `train_xg_v3_hf.py` | SPADL Deep-Sets (`spadl_shot_geometry`) | no | no |
| `train_psxg_hf.py` | StatsBomb on-target shots | no | no |
| `train_vaep_model_hf.py` | SPADL actions | no | no |
| `train_scoutgpt_hf.py` | SPADL/events feature build | no (no `spadl_action_context`/pressure/das/pitch_control read) | no |
| `train_football2vec*.py` ×3 | `fct_action_values` (SPADL) | no | no |

**Floor:** exactly **one** model (`xt_gk_v2`) consumes a feature F1b moves. Its sole frame-derived input is `pressure_on_actor__andrienko_oval` — a geometric (non-DAS) pressure, so the shift is float32-storage-only (~1e-5). Owner decision (§4.6): retrain `xt_gk_v2` at 4.128 so train and serve share one vintage. All other trainers are SPADL-canonical (105×68, untouched by raw-frame float32) and are **not** retrained. The sk-bundled frame-geometry arms (ghost-GK, ghost-outfield, gk_completion, xshot, xcross, receiver) are re-fit inside sk 4.128.

---

## 2. Goals / Non-Goals

### Goals
1. Bump silly-kicks 4.123.0 → 4.128.0 across every pin and sentinel (BREAKING; drop the `[das]` extra).
2. Refactor `compute_tracking_marts` **and** `compute_shot_freeze_frames` from driver-bound to executor-distributed, mirroring the ADR-045 `repartition + mapInPandas` transport.
3. Re-enable gkdv (`GKDV_ENABLED = True`) — the perf project it was gated on is now shipped (sk 4.128 batched DAS + F1b).
4. Ensure the refactored oriented frames carry sk 4.128 frame dtypes (float32 coords, `team_id` category, `player_id` Int64) — mostly inherited via the shared sk builder (§6).
5. Enable IDSSE in `compute_shot_freeze_frames` (its driver-toPandas blocker is removed by the refactor); exclude Metrica (owner: the 3 legacy matches are not quality data).
6. Retrain `xt_gk_v2` at 4.128 (§4.6) and deliver the operator runbook to re-materialize `das_*` / gkdv `delta_das` / frame-geometry marts in the correct order (§8).

### Non-Goals (explicit — do not expand without owner approval)
- **No model retrain other than `xt_gk_v2`** (§1.4 enumerates the floor; the other seven trainers are SPADL-safe).
- No re-architecture of `compute_action_context` itself (it is the reference; touched only for the shared transport seam, §4.3).
- No Metrica in shot_freeze; no ExT-v2 promotion, pausa reconciliation, IDSSE pass/cross fix, or metrica y-invert fix (tracked separately).
- The re-materialize + xt_gk_v2 retrain **executions** are NOT part of the code commit — they are post-merge operator runtime steps under separate approval (§8).

---

## 3. Global Constraints (verbatim, apply to every task)

- **Python 3.10** (`>=3.10,<3.11`); line length 120; Ruff E,W,F,I,N,UP,B,S,BLE,RUF; pyright basic over the CI target (NOT `pyright src/`).
- **Databricks serverless:** 16 GB driver; **1 GB UDF-group cap**; the per-group pandas budget target is **≤800 MB** peak (§4.2 defines the metric); no broadcast/cache/persist of large data; no internet in UDFs; no local-FS writes; frozen dataclasses for UDF config; lazy closure capture.
- **Canonical SPADL 105×68** — never bend to StatsBomb yards.
- **No secrets in code; HTTPS only; verify=True; explicit timeouts; no `eval`/`exec`/`pickle.loads`/`shell=True`** (the `src/evolve` exec exception does not apply here).
- **BLE001 (no silent swallow):** broad catches need narrowing or `# noqa` with reason; per ADR-002 the drain may swallow a per-unit failure but `main_*_drain_worker` MUST call `raise_on_failed_units`.
- **Structured JSON-line logging** with source name, row counts, timing.
- **Idempotent writes:** per-unit `replaceWhere` (`data_source='{provider}' AND match_id='{match_id}' AND period_id={period}`), never full-table drops; pass `row_count` to `write_delta_table`.
- **ADR-068 lifecycle events** preserved (`mode="append"`, `run_id={{job.run_id}}`, per-worker tables + UNION, fail-open on events / fail-loud on `slice_completed`); drain keeps the ADR-087 hybrid circuit-breaker + in-preflight dry-run canary + the 2700s watchdog.

---

## 4. Requirements — Refactor

### 4.1 Transport (both `compute_tracking_marts` and `compute_shot_freeze_frames`)
Replace the driver `toPandas()` + sequential per-worker scoring with the ADR-045 transport:

```
trk_sdf
  .repartition(_UDF_SHUFFLE_PARTITIONS, *group_keys)
  .sortWithinPartitions(*group_keys)
  .mapInPandas(mapper, schema=<explicit StructType>)
```

- Reuse `_make_streaming_group_mapper` (carry/flush) — do not hand-roll group assembly.
- Use an explicit partition count (`_UDF_SHUFFLE_PARTITIONS = 64`); never a plain `groupBy().applyInPandas` (AQE-coalescing regression).
- Keep per-unit `replaceWhere` writes, ADR-068 events, watchdog, circuit-breaker, canary.
- Only small SPADL actions / meta may cross to the driver (as AC does); raw frames must never be materialized on the driver.

**R-4.1-a — toPandas boundedness (`_topandas_exemptions.yml` is keyed by `(file, ENCLOSING FUNCTION qualname)`, NOT line):**
- `tracking_marts_driver.py::_read_unit` (entry yml L92-93) — its `trk.toPandas()` (L123) and actions `.toPandas()` (L128) are deleted by the refactor → **remove that exemption entry**.
- `shot_freeze_frames.py` `_process_match` (the per-period `trk_period_pdf...toPandas()`, L792) — has **no** exemption entry today → the refactor is **code-deletion-only**; nothing to remove from the yml.
- `tracking_marts_drain.py::_pool_gkdv` (entry yml L88-90) — its driver `toPandas()` (L423) **must be preserved**: it reduces the bounded `gkdv_observations` table (a few-thousand narrow keeper rows), and it goes **live** when gkdv is re-enabled (§4.4). Re-validate its bound against *real* gkdv-on data and keep the exemption. See §4.4.
- After edits, `test_topandas_boundedness` passes with exactly the above delta (one removal, one preserved, zero new unbounded calls).

### 4.2 Transport: two-stage build-once → spill → per-mart scoring (the central decision; rev3)

**Why not one AC-style pass.** `TrackingMartsProcessor.process` emits SIX outputs of different grain and cardinality from one unit's built inputs:

| Output (mart) | sk call | grain / cardinality | pitch control? |
|---|---|---|---|
| off_ball_runs | `compute_off_ball_runs` → `detect_off_ball_runs`/`value_off_ball_runs` | per-run | yes (unit-global link) |
| defensive_credit agg | `compute_action_defensive_credit` → `add_defensive_credit` | per-action | no (sk ADR-103 F6) |
| defensive_credit long | `compute_defensive_credit_long` → `compute_defensive_credits` | per-action×defender | no |
| gkdv_obs | `score_gkdv_unit` | per-keeper-frame | yes (per-frame DAS ×2) |
| gk_decision | `score_gk_decision_unit` | per-decision | — |
| rest_defense | `compute_rest_defense_samples` → `compute_rest_defense` | per-sample | yes (unit-global) |

AC emits ONE wide table, so a single-schema `mapInPandas` fits it. These six are **different-cardinality** — they cannot be row-aligned into one `mapInPandas` output schema; and rebuilding the oriented frame once per mart (six passes) would pay the dominant BUILD cost 6×. The unit-global scorers also cannot be chopped to sub-unit `frame_batch_id` groups (the action→frame link is global-over-unit). Hence a two-stage design.

**Stage 1 — build-once + spill (one distributed pass).** Read each unit's RAW tracking frames as a Spark DF; dispatch `repartition(UDF_SHUFFLE_PARTITIONS, match_id, period).sortWithinPartitions(match_id, period, <frame>).mapInPandas(build_udf, built_schema)`. The UDF runs `build_unit_inputs` on the group's raw frames and EMITS the BUILT oriented frames — a single homogeneous schema. The driver writes that output to a **UC Volume Parquet** intermediate, partitioned by `(provider, match_id, period)`, via a normal Spark write (NOT an in-UDF FS write). Raw frames are narrow (~tens of MB/half); the Stage-1 UDF peak is the built frame alone (~0.64 GB float32), with NO scorer/pitch-control working set on top — much lower than the old 3.4 GB combined peak.

**Stage 2 — per-mart scoring (six cheap passes, no rebuild).** For each mart: dispatch `mapInPandas` keyed by `(match_id, period)` where the GROUP carries only the unit key (tiny); the UDF READS that unit's built-frame Parquet from the UC Volume (the AGENTS-sanctioned `applyInPandas → UC Volume Parquet` escalation) and scores that ONE mart, streaming frames via sk `batch_size`. Returns that mart's single `StructType`; the driver writes it to its bronze table with the per-unit `replaceWhere`. The frame BUILD is never repeated; each mart stays a clean single-output pass (keeps the per-mart oracle, §9).

**Cleanup.** The UC Volume intermediate is deleted after Stage 2 (idempotent; no orphan paths; on failure the next run overwrites its unit partitions).

**Driver-Spark-reads move to the closure.** `process` currently does `_read_xg_preds` (defensive_credit) and `resolve_unit_meta` (gkdv) as driver Spark reads INSIDE scoring — illegal on an executor. Pre-load both on the driver (per-unit dicts) and pass via the UDF closure, alongside actions / xt / the bundled completion model (all small, lazy-captured).

**Fit levers:** sk 4.128 **float32** coords (~1.27→0.64 GB built frame); built-frame **column-prune** to the scorer UNION before spill; `team_id` **category**; sk **`batch_size=`** bounded default on off_ball_runs + rest_defense (byte-identical, sk 4.127 CHANGELOG — §9 relies on it); one `PitchControlCache()` per mart-pass.

**R-4.2-a — profile gate (precise metric + non-vacuity on the NEW path):** measure per provider at its largest half, gkdv on, BOTH:
1. **Stage-1 build-UDF peak** — built-frame pandas bytes (`memory_usage(deep=True).sum()`) + executor RSS. Acceptance **≤800 MB** at float32 + prune.
2. **Stage-2 scoring-UDF peak — of the path ACTUALLY TAKEN (TM-DELTA-01):** if direct-read (§12.1a) is confirmed, the streamed per-mart working set (Parquet chunk + pitch-control surfaces); if the fallback is used, the whole-0.64 GB-group + per-mart working set. Profile **per mart incl. gkdv** under whichever path ships. Acceptance **≤800 MB**.
**Non-vacuity (watch the NEW instrument bust):** run Stage 1 once in a deliberately un-optimized config — **float64 + no prune + largest half** — and confirm the in-UDF instrument REPORTS a bust (>800 MB). Reproducing the old *driver* high-water is NOT sufficient — it does not prove the executor sampler can observe an executor-side bust. Only then is a green optimized reading meaningful.

**R-4.2-b — fit outcomes + remedies recorded (both stages):**
- **Stage-1 build busts ≤800 MB** (unlikely — ~0.64 GB float32) → sub-batch the build in frame-windows. **Caveat (TM-PLAN-13 / TM-DELTA-04):** `build_unit_inputs`' Savitzky-Golay velocity derivation is sequence-continuous — a naive window split perturbs velocity at window boundaries, so a build sub-batch MUST carry a halo (overlap) and prove neutrality vs the whole-unit build. This is the risky branch; flag it.
- **Stage-2 fallback busts ≤800 MB for a mart** (gkdv most likely, under the no-stream fallback): for the per-frame scorer (gkdv) frame-window sub-batch within the unit (gkdv is per-frame → window-batchable without a halo); the unit-global scorers (off_ball/rest_defense) keep sk `batch_size` bounding their working set over the held frames. If a mart still busts → STOP and surface (not a silent scope cut).
- Record the per-provider Stage-1/Stage-2 peaks, the §12.1a path taken, and any sub-batching as explicit constants with the profiling evidence.

**(rev3 note):** this REPLACES rev2's "whole-unit mapInPandas primary + conditional UC-Volume-Parquet fallback (Approach 2 / Task 9b)". The spill is now the PRIMARY transport — it is what makes six heterogeneous marts expressible from one build AND bounds memory. Driven by the multi-output grain mismatch (owner decision 2026-10-06, re-reviewed).

### 4.3 Shared transport seam
If the mapper construction, `_make_streaming_group_mapper`, `_UDF_SHUFFLE_PARTITIONS`, and schema plumbing are reused from `action_context.py`, extract the shared pieces into a module both drains import (follow `src/analytics/action_context/` structure) rather than copy-paste. Do not restructure AC beyond the seam. **Confirm at plan time** whether `_make_streaming_group_mapper` / `_UDF_SHUFFLE_PARTITIONS` are already import-safe for a second consumer or need promoting to a shared module.

### 4.4 gkdv re-enable (including the pooling stage)
- `tracking_marts_processor.py` `GKDV_ENABLED` (L119/gate ~L70-71) `False → True`; the drain's gkdv activation (`tracking_marts_drain.py:453`) derives from the same single lever.
- The gkdv scoring arm (`score_gkdv_unit` → `build_ghost_frames` + `build_keeper_observations`, writing `gkdv_observations`) runs within the whole-unit group; it is the heaviest DAS arm and MUST be in the R-4.2-a profile.
- **The gkdv pooling stage** `main_gkdv_pool` → `_pool_gkdv` (`tracking_marts_drain.py:400-463`, driver `toPandas` L423 → `gkdv_writer.pool_keepers:273`) is a separate post-drain reduce that is a **no-op today** (gkdv off ⇒ empty `gkdv_observations`) and goes **live** on re-enable. Enumerate it in the task plan; keep its bounded driver read + exemption (R-4.1-a) after re-validating the bound on real gkdv-on volumes.
- Amend ADR-082 (gkdv un-gated). **Re-measure** the gkdv per-unit budget post-refactor against the documented worker-drain/watchdog budgets (ADR-037/082); the `>45 min/unit` note (`tracking_marts_processor.py` L110-118) is the pre-refactor figure to beat.

### 4.5 shot_freeze provider scope
`_DEFAULT_PROVIDERS` (`shot_freeze_frames.py:106`) = GradientSports + SkillCorner + **IDSSE** (add idsse; **exclude Metrica** — owner: the 3 legacy matches are not quality data). IDSSE's 1.73M-row half MUST be in the R-4.2-a fit profile for shot_freeze. Record the Metrica exclusion + rationale in the module docstring.

### 4.6 xt_gk_v2 retrain at 4.128 (owner decision)
`xt_gk_v2`'s sole frame-derived input (`pressure_on_actor__andrienko_oval`) refreshes to 4.128 vintage under re-materialize (§1.4). To avoid train/serve vintage skew, `xt_gk_v2` is retrained on the 4.128 corpus. Requirements:
- Train **after** `spadl_action_context` is re-materialized at 4.128 (so the `pressure` corpus is 4.128-vintage); the acyclicity (v2-free corpus, never `fct_action_context`) is unchanged — `train_xt_gk_v2_hf.py:30-31`.
- Use the existing ADR-012 delivery path already in the trainer: `require_mlflow_env` (`:248`), `upload_weights_to_uc_volume`, `set_and_verify_mlflow_champion` (`:241`). Champion promotion is part of the retrain.
- **AI governance (mandatory, AGENTS AI-Governance rule):** `xt_gk_v2` is a §5 **Direct/evaluative** system (AI_GOVERNANCE.md §5 row 19). The retrain updates §5 + the model card `docs/huggingface/model-cards/xt-gk-v2.md` + the `governance:` YAML (`wf-xt-gk-v2.yaml`) + **re-runs the governance test**.
- **Governance-edit split (so `main` never documents an unproduced model — reviewer-flagged):** the **code commit** carries only the trainer sentinel bump + the *structural* governance edits marked **"4.128 retrain pending"** (the card/§5 note that a 4.128 refit is required by the F1b `pressure` refresh, with NO produced-model metrics and NO status flip to "produced"). The *produced-model* content — new metrics, champion alias, status flip to produced, and the governance-test re-run against the produced card — lands **with the post-merge retrain step** (§8), under its own approval.
- The retrain **run** is a post-merge operator step under separate approval (§8).

---

## 5. Requirements — silly-kicks 4.128 bump

### 5.1 Pins (drop `[das]`)
| File:line | From | To |
|---|---|---|
| `pyproject.toml:71` | `silly-kicks[das,ghost-gk,parse-dfl]>=4.123.0,<5` | `silly-kicks[ghost-gk,parse-dfl]>=4.128.0,<5` |
| `pyproject.toml:150` (comment) | `[das,ghost-gk,parse-dfl]` | `[ghost-gk,parse-dfl]` |
| `terraform/modules/workflows/main.tf:1927` | `silly-kicks[das,ghost-gk,parse-dfl]==4.123.0` | `silly-kicks[ghost-gk,parse-dfl]==4.128.0` |
| `scripts/submit_ac1_oneshot.py:55` | `silly-kicks[das,ghost-gk,parse-dfl]==4.123.0` | `silly-kicks[ghost-gk,parse-dfl]==4.128.0` |
| `uv.lock` L1696/L1722/L1817 extras + L3430/L3432 sdist/wheel | `das,ghost-gk[,parse-dfl]` 4.123.0 | regenerate via `uv lock` |

Procedure (ADR-046): edit `pyproject.toml` → `uv lock` → `scripts/sync_tf_env_pins.py` (generates the TF `==` pins — do not hand-edit them) → verify its `--check`. In `uv.lock`, `accessible-space` currently appears only as the `[das]` transitive; dropping `[das]` removes it from the lakehouse lock — intended (native engine).

*Note on the sk-side `das-reference` extra:* sk 4.128 `pyproject.toml` `[project.optional-dependencies]` defines `das-reference = ["accessible-space==2.0.15", "pandas<3"]` — a **dev-only, sk-internal** extra for regenerating the DAS parity oracle. It is **not** a lakehouse dependency and the lakehouse never installs it; it is mentioned only to preempt confusion (review B read its absence from the lakehouse lock as the extra not existing — it exists, sk-side).

### 5.2 `_REQUIRED_SK_MIN` sentinels — 21 declarations, (4,123,0) → (4,128,0)
14 in `src/ingestion/`: `bravery_writer.py:47`, `defensive_credit_writer.py:45`, `duels_writer.py:63`, `exec_visibility.py:450`, `expected_threat.py:69`, `gk_decision_writer.py:72`, `gkdv_writer.py:53`, `match_outcome_writer.py:54`, `off_ball_runs_writer.py:30`, `restdefense_writer.py:41`, `shot_stopping_writer.py:69`, `team_metrics_writer.py:57`, `territory_writer.py:82`, `xt_gk_v2_writer.py:58`.
7 in `scripts/`: `train_football2vec.py:80`, `train_football2vec_360.py:76`, `train_football2vec_v2.py:78`, `train_scoutgpt_hf.py:84`, `train_vaep_model_hf.py:79`, `train_xg_v3_hf.py:146`, `train_xt_gk_v2_hf.py:81`.

Test assertions to update (both pin the literal):
- `src/tests/test_sk3_mig_b_orchestrator_invariants.py`: comment L258, **stale docstring L367 (currently `(4, 121, 0)` — pre-existing drift; fix to `(4, 128, 0)`)**, `expected` L385, message strings L390/L392.
- `src/tests/test_train_xt_gk_v2.py:95` (`== (4, 123, 0)`).

### 5.3 Wheel bump
`scripts/bump_wheel.py`: bump `pyproject.toml` wheel version (0.5.116 → next), run the propagation (deploy scripts / TF / PEP-723 trainer headers), verify `bump_wheel.py --check`.

---

## 6. Requirements — frame dtypes (mostly verify-only)

There is **no** lakehouse-side frame dtype cast today; float32/category is silly-kicks-builder-internal, inherited transitively through the sk tracking builders (`convert_to_frames` / the `sk_frame_adapters.py` + `pipeline.py` builder paths) that `read_and_build_unit_inputs` (`tracking_marts_driver.py:216`, shared with AC) routes through. Consequences:

- **R-6-a:** confirm (plan time) that `read_and_build_unit_inputs` builds every provider's frames via the sk builders, so skillcorner/metrica/gradientsports/idsse frames inherit sk 4.128 float32/category automatically. If so, §6 is **verify-only** (no cast to add).
- **R-6-b (re-anchored):** rev1 cited `convert.py:53-55` as an "IDSSE hand-build" — that is **wrong**: `src/analytics/action_context/convert.py` contains only `_bronze_gradientsports_to_converter_input` (`:26`), a GradientSports column-mapper, and no IDSSE converter. Locate the **actual** per-provider float64 hand-build sites, if any remain, at plan time; add explicit casts (coords `float32`, `team_id` `category`, **`player_id` stays `Int64`**) only where a path does not inherit from the sk builder. Any masked coord write-back must `.astype(col.dtype)` (pandas-3 `LossySetitemError` rule).
- **R-6-c (the player_id reason, corrected):** `player_id` stays `Int64` (not `category`) because it is **mutated post-build** — sk `tracking/schema.py:17-27` records `_run_values` Int64-reassign + keeper/actor identity bridges (the ADR-103 dynamic-column rule); `category` is not transparent to a new-category setitem. The 4.128 native DAS no longer writes a `"ball"` sentinel (`_das.py:10`), so that is **not** the reason — do not cite it. `team_id` is `category` (static, low-card). `value_counts`/`groupby` on a category key use `observed=True` / `.astype("object")` at the call site; `id_compat._decat` unwraps the category at compute boundaries.

---

## 7. Requirements — native DAS breaking audit

- **R-7-a:** enumerate every lakehouse DAS call site (start: `gkdv_writer.py`, and any `restdefense`/`territory` DAS path). Remove `use_progress_bar=` and any reliance on `**kwargs` passthrough (both removed in 4.128).
- **R-7-b:** the native engine adds fail-loud `ValueError`s. Treat these as **new raise paths**: test them against **LIVE bronze** frames (not just fixtures) across gradientsports/idsse/skillcorner/metrica — do real frames trip any new `ValueError`? If so, surface it (do not silently catch).
- **R-7-c:** the periodic-quadrature DAS value shift is **expected, not a bug** (§1.3). Goldens that pin DAS values (the mini-golden CI gate; any das_* golden) are **regenerated** as part of this cycle, with the regeneration recorded. Regenerate BOTH mini-goldens after the sk bump (IDSSE 3-action; SB360 variant).
- **R-7-d (import audit broadened beyond imports):** dropping `[das]` / the `accessible_space` module requires touching, at minimum:
  - `src/ingestion/exec_visibility.py:402` — the import-probe tuple `("numba", "accessible_space", "silly_kicks", …)` would report a **permanent false import-error** post-drop; remove `accessible_space` from it (and the L365 docstring).
  - `src/ingestion/action_context.py:2152` — the `"get_dangerous_accessible_space"` version/DAS-entry string (cosmetic log noise post-drop).
  - `gkdv_writer.py` docstrings referencing the old DAS extra.
  - Confirm no module still `import`s from the removed `accessible_space` path.

---

## 8. Post-merge operator runbook (SEPARATE approval; nothing runs in CI)

**Execution is NOT part of the code commit.** No wheel-consuming job runs until post-merge `main` CI is green ([no-jobs-in-ci]); every job run below is a separate explicit approval.

**Precondition (added 2026-10-07):** the Stage-1 spill UC Volume `tracking_marts_build_spill` (bronze schema) + the ingestion-SP `READ_VOLUME`/`WRITE_VOLUME` grant on it MUST exist before the drain — provisioned by `terraform/modules/catalog/main.tf` (`databricks_volume.tracking_marts_build_spill`) and applied via the Terraform Apply workflow. This was missed in the original PR (#599) and caught by the ADR-087 preflight canary (`[UC_VOLUME_NOT_FOUND]` on gradientsports:10502:1); landed in the `fix/tracking-marts-spill-volume` hotfix. Verify with `SHOW VOLUMES IN {catalog}.bronze` before step 1.

Scope — the tracking-derived marts whose values move under sk 4.128 (DAS value shift + float32 + re-fit bundles):
`fct_off_ball_runs`, `fct_rest_defense`, `fct_defensive_credit_attributions`, `fct_gk_tracking_actions`, `fct_gk_tracking_stats`, `fct_action_context` (das_* + gkdv `delta_das`), `fct_action_defensive`, `fct_defcon_actions`, `fct_physical_stats`, `fct_tracking_frames`, `fct_gk_shot_stopping_pooled`.

**Order matters (acyclicity for the xt_gk_v2 retrain):**
1. Wipe the bronze das_*/gkdv/tracking-marts tables in scope.
2. `run_now` `[tracking_marts_preflight, tracking_marts_drain]` (gkdv-enabled, incl. `main_gkdv_pool`) and `[AC preflight, compute]` — this re-materializes `spadl_action_context` (incl. `pressure_on_actor__andrienko_oval`) at 4.128.
3. **Retrain `xt_gk_v2`** on the now-4.128 corpus (§4.6); promote the champion; land the *produced-model* governance content here — flip the card/§5 from "4.128 retrain pending" to produced (metrics, champion alias, status) + re-run the governance test against the produced card. (The in-commit edits were the "retrain pending" markers only — §4.6.)
4. Re-run `xt_gk_v2_writer` and any dependent marts so serving uses the 4.128-vintage model on 4.128-vintage `pressure`.
5. Rebuild dbt staging.
6. `rederive_synced_marts.py` for the TRIGGERED-synced marts in scope (NEVER `dbt --full-refresh` a TRIGGERED mart); `refresh_synced_tables` for SNAPSHOT marts. **Confirm each mart's synced mode at plan time** and list it.
7. `fct_action_defensive` requires `--vars xg_v3_enabled:true`.
8. Confirm post-recompute parity against the regenerated goldens.

**Expected under 4.128 (not a regression):** sk 4.128 F1b stores frame coords/timestamps as float32 (ADR-106), which nudges values ~1e-5. A small number of **borderline** action→frame links (sitting right at `link_actions_to_frames`'s acceptance threshold) flip to unlinked — their `elastic_frame_id`/`elastic_confidence`/`elastic_error_seconds` and all frame-derived features (das_*, ghost_gk, xshot/xcross) become **honest NaN** for those actions. The golden regen measured exactly **1 of 97** actions flipping on the IDSSE J03WMX anchor (action 105, prior link error 0.369s — the weakest linked action). This is an expected boundary effect of a thresholded matcher under a storage-precision change, not a drain failure; do not treat occasional new NaN das_*/frame features on marginal actions as a re-materialize regression.

---

## 9. Testing & shift-left

- **All seven local gates** (ruff check, ruff format --check, lint-imports, `bump_wheel.py --check`, `pip_audit_ignores --check`, pyright over the CI target, full `uv run pytest`) + the hidden eighth **`validate_workflow_cards`** (the gkdv + xt-gk-v2 cards change).
- **Full `uv run pytest`** (never `--collect-only`/subset) before declaring done and at final review.
- **Transport value-neutrality is carried by a single-group ORACLE fixture**, not the mega-golden aggregate: assert the refactored whole-unit UDF output equals the pre-refactor per-unit scorer output **on a pinned sk 4.123** (transport-only, no bump) for a representative unit per provider. The aggregate §1.3 DAS-shift numbers are NOT a usable per-row oracle. This pinned-4.123 transport-only run is a **mandatory recorded artifact** — without it, "the bump moved only DAS" has no baseline.
- Then, **with** the 4.128 bump, the remaining output delta is attributable to the intended DAS/float32 shift; the goldens are regenerated (R-7-c).
- **Red-green** every new/changed guard: the R-4.2-a profile gate (watch the new executor instrument bust first), the new DAS `ValueError` paths, the deleted-toPandas boundedness, any dtype cast.
- **Production-scale benchmark** of the refactored path per provider; invoke `mad-scientist-skills:measure-before-optimize` before touching any benchmarked/hot-path function.
- **`test_topandas_boundedness`:** apply the R-4.1-a delta (remove `_read_unit`; preserve `_pool_gkdv`) and rerun.
- **Boundary fixtures (named):**
  - **(rev3) build-spill round-trip:** Stage-1 built frames written to the UC-Volume Parquet == the in-UDF built frames (byte-identical read-back); Stage-2 scoring from the spilled Parquet == scoring from the in-memory built frames (per-mart). This is the seam the two-stage split introduces.
  - a Stage-1 build group at/above the largest-half row count, per in-scope provider incl. idsse (fit gate);
  - a group spanning an Arrow-chunk boundary (carry/flush vs single-group oracle);
  - an empty group (short-circuit, both stages);
  - a gkdv unit with a keeper-removed counterfactual frame (DAS on explicit frame slices) + the `_pool_gkdv` reduce on real gkdv-on rows;
  - a post-cast frame (float32 coords + category team_id + Int64 player_id) round-trips through the scorers without `LossySetitemError` / new-category setitem.

---

## 10. Branch / commit / approval discipline

- One feature branch off `origin/main`; `git fetch` + `git pull --ff-only` before branching; verify alignment vs `origin/main`. **No worktrees.**
- The bump + refactor + gkdv re-enable + dtype + shot_freeze-idsse + governance-doc edits land as **one coherent, fully-tested commit** (all gates green). **No micro-commits.**
- The spec + plan are committed **bundled with** the implementation PR.
- **Explicit human-approval gate (hard):** the session stops at the point of committing and shows the diff / file list; `git commit`, `git push`, `gh pr create`, `gh pr merge` are each a **separate** explicit approval. None is implied by "tests pass", by this spec, by the plan, or by a prior approval. Every job `run_now` (profile, re-materialize, xt_gk_v2 retrain) is likewise a separate approval, and none runs before post-merge `main` CI is green.
- TODO.md updated **in** the shipping commit; verify `git status TODO.md` + `bump_wheel --check` at cycle close.

---

## 11. ADRs

- **New ADR** — tracking-marts + shot_freeze executor refactor: driver → `repartition + mapInPandas` (ADR-045 lineage). Record the **two-stage build-once → UC-Volume-Parquet spill → six per-mart scoring passes** design and WHY (six different-cardinality marts can't share one single-schema `mapInPandas`; per-mart rebuild = 6× BUILD cost; unit-global action→frame link blocks sub-unit grouping); the Stage-1 build-fit profile gate + build-sub-batch fallback; the `batch_size=` byte-identity reliance; the driver-reads→closure move. shot_freeze (single output) stays single-schema `mapInPandas`.
- **Amend ADR-082** — gkdv un-gated; re-measured budget; the pooling stage.
- **xt_gk_v2 retrain** — recorded via the governance updates (§4.6) + a note that the retrain is forced by the F1b `pressure` vintage refresh.
- **Consumption note** — sk 4.128 (ADR-106/107/108) downstream re-materialize; `[das]` extra dropped (native engine).

---

## 12. Risks & open questions (resolve at plan time, none deferred silently)

1. **Fit (highest risk):** does the Stage-1 build-UDF fit ≤800 MB (built frame ~0.64 GB float32, no scorer working set) for the largest idsse/gradientsports halves? Resolved by R-4.2-a; build sub-batching (R-4.2-b) is the fallback. Infeasible for a provider = a blocker to surface.
1a. **(rev3) UC-Volume read inside a serverless UDF — the keystone (TM-DELTA-01):** Stage-2's PREFERRED read-path reads the unit's built-frame Parquet from a UC Volume inside the scoring UDF and **streams it via sk `batch_size`** (bounded per-mart peak = stream chunk + working set). This is the AGENTS-sanctioned `applyInPandas → UC Volume Parquet` escalation, but ADR-012 documents only DRIVER-side volume I/O — so CONFIRM serverless permits the in-UDF (executor) volume read via a cheap spike BEFORE building the Stage-2 UDF (§9 / plan T3b).
  - **If NOT permitted — fallback is not costless:** Stage-2 reads built-frames as a Spark DF and re-groups by `(match,period)`, which materializes the **WHOLE ~0.64 GB built-frame group inside the UDF** (no streaming). Per-mart peak then = 0.64 GB + that mart's working set. For the pitch-control-free marts (defcred agg/long, gk_decision) that fits; for **gkdv** (per-frame DAS ×2) and off_ball/rest_defense (pitch control) it must be measured against the **800 MB gate** (~160 MB headroom over the 0.64 GB frame), NOT the old 3.4 GB. R-4.2-a profiles the path actually taken; R-4.2-b carries the fallback remedy.
2. **Frame-build routing (R-6-a/b):** confirm `read_and_build_unit_inputs` builds all providers via the sk builders (→ §6 verify-only) and locate any residual float64 hand-build. rev1's IDSSE anchor was wrong; the real site is unlocated.
3. **Native DAS new raises vs LIVE bronze (R-7-b):** unknown until run against live frames.
4. **Transport reuse (R-4.3):** are `_make_streaming_group_mapper` / `_UDF_SHUFFLE_PARTITIONS` import-safe for a second consumer, or must they be promoted to a shared module?
5. **Per-mart synced mode (§8):** TRIGGERED vs SNAPSHOT for each of the 11 marts — pin the list.
6. **`_pool_gkdv` bound under gkdv-on (§4.4):** the exemption's "few-thousand rows" claim is from the gkdv-off era; re-validate on real gkdv-on volumes.
7. **xt_gk_v2 retrain cost/governance (§4.6):** confirm the DGX/HF-Jobs training path + the governance test are current; sequence it after the AC re-materialize.

---

## 13. Verification checklist (for reviewers)

- [ ] `_read_unit` L123/L128 `toPandas` and `shot_freeze_frames` L792 `toPandas` removed; `_topandas_exemptions.yml` delta = remove `_read_unit`, preserve `_pool_gkdv`, no shot_freeze entry (R-4.1-a, function-keyed).
- [ ] Transport is `repartition(64,…).sortWithinPartitions(…).mapInPandas(…)`, not `applyInPandas` (ADR-045).
- [ ] Whole-unit grain justified; profile gate R-4.2-a defines pandas-group-bytes vs RSS and watches the **new** executor instrument bust under a float64/no-prune config first.
- [ ] `[das]` dropped from all 4 pin sites + uv.lock; `exec_visibility.py:402` probe + `action_context.py:2152` string + docstrings fixed (R-7-d); `sync_tf_env_pins.py --check` honored.
- [ ] 21 `_REQUIRED_SK_MIN` → (4,128,0); 2 test files updated incl the stale `(4,121,0)` docstring.
- [ ] Retrain floor correct: only `xt_gk_v2` retrains (§1.4 enumerates all 8); its governance (§5 + model card + YAML + test) updated; retrain ordered after AC re-mat (§8).
- [ ] shot_freeze `_DEFAULT_PROVIDERS` = GS+SkillCorner+IDSSE, **no Metrica**; idsse in the fit profile.
- [ ] gkdv re-enabled via the single lever; pooling stage (`main_gkdv_pool`) enumerated + its exemption preserved+revalidated; ADR-082 amended.
- [ ] player_id-stays-Int64 reason = post-build mutation (sk schema.py:17-27), NOT the removed "ball" sentinel.
- [ ] DAS value shift handled as expected (goldens regenerated); new `ValueError`s tested vs LIVE bronze; transport-neutrality proven by the pinned-4.123 single-group oracle artifact.
- [ ] Re-materialize + retrain are post-merge operator steps under separate approval; no job runs in CI.
- [ ] Per-action human-approval gate before every commit/push/PR/merge/job-run; one coherent commit; no worktrees.
- [ ] Nothing deferred to a follow-up/TODO without owner approval.
