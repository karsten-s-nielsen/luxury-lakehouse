# silly-kicks session brief — AC-1 required updates (copy/paste)

> **STATUS: DONE (2026-05-29).** All four items shipped in silly-kicks and adopted by the lakehouse:
> **(1) ELASTIC frame-origin fix** — silly-kicks 3.25.0 (`_fit_frame_time_relationship`); AC-1
> `elastic_*` now populates correctly for IDSSE. **(2) DAS/shape_graph linked-frame restriction** —
> 3.25.0 (native; lakehouse `_restrict_to_linked_frames` workaround removed). **(3) obso +
> cover_shadows perf** — 3.25.0/3.25.1 (cover_shadows leave-one-out vectorization, PR-S65).
> **(4) shared `PitchControlCache`** — 3.25.0 (TF-7; threaded through obso/cover_shadows/gk_influence/
> space_creation/pitch_control_at_action). Plus ghost_gk linked-frame restriction (3.26.0, ~100×).
> Lakehouse pinned `silly-kicks[das,ghost-gk]>=3.27.0`.
>
> **CORRECTION to the §1 cross-check (lines below):** the "separate downstream whole-half
> `elastic_sync_results` pipeline" that "DOES produce IDSSE alignments" is the LEGACY
> `analytics.elastic_sync`, and it is itself **frame-origin-buggy** (`frame≈25·ts`, 0-based — verified
> oracle `frame_id=25.000·ts−0.9`, intercept≈0 vs correct +10000). The two paths do **NOT** converge:
> the legacy one is wrong; silly-kicks 3.25.0 is the fix. AC-1 elastic is therefore range-checked
> (INVARIANT_ONLY), not validated against the legacy oracle. See
> `memory/project_legacy_elastic_sync_frame_origin_bug.md`.

> Paste everything below the line into a fresh Claude session opened in the silly-kicks repo
> (`D:\Development\karstenskyt__silly-kicks`). Self-contained. Four items, all in scope:
> **(1) ELASTIC frame-origin bug [required, correctness]**, **(2) DAS/shape_graph linked-frame
> restriction [perf]**, **(3) obso + cover_shadows per-function perf [perf]**, **(4) a shared
> per-frame pitch-control surface [perf, highest leverage]**. Every perf item is quality-preserving
> (bit-identical output) — the standard is gold-standard-where-realistic, never trade accuracy for
> speed. Investigate each as needed; the evidence + fix direction + validation are given per item.

---

You maintain **silly-kicks** (every merge to main bumps version + git tag → PyPI publish; current
3.23.0). It is consumed downstream by the **luxury-lakehouse** AC-1 pipeline
(`bronze.spadl_action_context`), which runs the tracking enrichment chain per 250-frame batch via
`applyInPandas`. All findings below were verified on real IDSSE data (match J03WMX, period 1).

---

## 1. [REQUIRED — correctness] ELASTIC produces all-NaN for native-frame-numbered providers

**Symptom:** `silly_kicks.tracking._elastic_sync.align_events_to_frames` returns an empty result
(→ `add_elastic_sync` fills `elastic_frame_id / elastic_confidence / elastic_error_seconds` with
all-NaN) for any provider whose tracking `frame_id` is not zero-based-and-time-aligned — e.g.
**IDSSE / Sportec**, where period-1 frames are numbered from 10000 (period 2 from 100000) while
`time_seconds` is period-elapsed (0-based).

**Root cause (verified):** the function maps an action time to its candidate frame window via

```python
nominal_frame = round(action_time * params.frame_rate)   # assumes frame_id == time * rate (0-based)
frame_min = nominal_frame - window_frames
frame_max = nominal_frame + window_frames
candidate_frames = period_frames[searchsorted(frame_min) : searchsorted(frame_max)]
```

For an IDSSE action at `time_seconds = 2.83`, `nominal_frame = round(2.83 * 25) = 71`, but the real
frames at that moment have `frame_id ≈ 10000–10070`. `searchsorted([10000..], 46)` → empty slice →
`continue` for **every** action → empty alignment. The `window_seconds` (default 1.0s) window is
fine; the defect is purely the frame-id origin assumption.

**Evidence:** converted IDSSE frames have `frame_id` 10000–10499 while `time_seconds` is 0.00–19.96
(different origins). `align_events_to_frames(actions, frames)` returns shape `(0, 4)` — it does NOT
raise; `@nan_safe_enrichment` is not involved.

**Fix direction:** derive the nominal frame from the frames' OWN `(frame_id, time_seconds)`
relationship, not from `time * frame_rate`. fps is constant, so per `(game_id, period_id)` fit
`frame_id ≈ slope * time_seconds + intercept` from the group's frames (e.g. from min/max
`(time, frame)` pairs) and use `nominal_frame = round(slope * action_time + intercept)`. Fall back
to `time * frame_rate` only when frames lack `time_seconds`. Correct for both 0-based
(Metrica/StatsBomb) and native-numbered (IDSSE/Sportec) providers.

**Tests:** add a regression case feeding frames with a non-zero origin (`frame_id = 10000 + i`,
`time_seconds = i/25`) and assert `align_events_to_frames` returns non-empty alignments with
`elastic_frame_id` in the 10000+ range. Add to the `_elastic_sync` test module.

**Cross-check:** a separate downstream whole-half `elastic_sync_results` pipeline DOES produce
IDSSE alignments (544 rows for J03WMX) — it presumably feeds 0-based frame ids, which is why it was
unaffected. Confirm both paths converge after the fix.

---

## 2. [RECOMMENDED — quality-preserving perf] Per-frame-snapshot aggregators recompute all frames

**Problem:** `add_das` and `add_shape_graph` are per-frame **snapshot** metrics, but they compute
the (expensive) per-frame value over EVERY input frame, then use `links` to map only the
action-linked frames (~3 of 250 per batch) to actions. ~98% of the per-frame work is discarded.

- `features._precompute_das_lookup` docstring: *"Run get_individual_das ONCE on all frames"* — runs
  the accessible-space pass simulation for every frame; `_map_das_to_actions` then maps via `links`.
- `add_shape_graph` loops `team_frames.groupby(["game_id","period_id","frame_id"])` calling
  `compute_shape_graph` per frame for all frames, then maps linked frames to actions.

Both functions **already accept `links`** but only use it for the final mapping, not to prune the
precompute. On a 250-frame batch this made DAS ~213 s and shape_graph ~43 s of a 366 s run.

**Verified impact (lakehouse-side workaround):** the lakehouse currently pre-filters frames to the
linked frame_ids before calling these (helper `_restrict_to_linked_frames` in
`analytics/action_context/enrich.py`). Result: **366 s → 70 s (~5.2×) for 30 batches, with the
frozen golden output byte-identical** — i.e. zero quality change, because the metric is a per-frame
snapshot (a linked frame's value is independent of which other frames are in the input).

**Fix direction (move it into silly-kicks):** when `links is not None`, restrict the per-frame
precompute to the linked `frame_id`s inside `add_das` / `_precompute_das_lookup` and `add_shape_graph`
before the heavy per-frame loop. Gate strictly on `links is not None` so the no-links call path
(full per-frame lookup) is unchanged for other consumers. For DAS, infer ball-carrier /
`derive_team_in_possession` on the FULL frames first (contiguous hysteresis), THEN restrict to linked
frames for the simulation. Output for actions is identical; this lets downstream remove the lakehouse
workaround.

**Tests:** for each function, assert that calling with `links` provided yields identical action-level
output whether `frames` is the full batch or pre-restricted to the linked frame_ids (per-frame
snapshot ⇒ equality). Use a fixture with ≥2 distinct linked frames among many frames.

---

## 3. [IN SCOPE — gold-standard perf, quality-preserving] obso + cover_shadows

These are NOT the linked-frame pattern (do not apply that here — see why below), but each has its
own quality-preserving optimizations worth doing to gold standard. Profiled post-DAS/shape-graph:
`add_obso` ~19 s, `add_cover_shadows` ~28 s for 30 batches.

### 3a. `add_obso` / `_precompute_obso_lookup` (~19 s)

Why linked-frame does NOT apply: it iterates per-pass-action and builds a ±window (pre 3 s /
post 1 s, ~100 frames) around each pass, so it needs contiguous window frames, not just the
linked frame.

Quality-preserving fixes (evidence from the current source):
- **Hoist loop-invariant work out of the per-pass loop (pure speedup, trivially safe).** Lines
  ~64-67 recompute `period_frames = frames[frames["period_id"] == period_id]` and
  `drop_duplicates("frame_id")[["frame_id","time_seconds"]].sort_values(...)` **inside** the
  per-pass loop -> O(passes x frames). Precompute one sorted `(frame_id, time_seconds)` table per
  `period_id` once, before the loop, and slice the window from it. Identical output.
- **Share pitch-control across overlapping pass windows.** `compute_pass_obso` computes
  pitch-control per window frame; consecutive passes within ~4 s have overlapping windows, so the
  same frame's pitch-control is recomputed. A per-`(period_id, frame_id, method)` pitch-control
  cache (see section 4) makes these reuse - quality-identical. This is the bulk of OBSO's cost
  (`compute_pitch_control` ~6.5k calls / 30 batches).
- **Note (not perf):** `except Exception: continue` swallows per-pass OBSO failures silently. If
  gold-standard error hygiene is wanted, narrow it or surface the count (the lakehouse follows
  ADR-002 "no silent swallow"; silly-kicks may want the same).

### 3b. `add_cover_shadows` / `lane_control` (~28 s)

Why linked-frame does NOT apply: `_compute_cover_shadow_dict` is already per-linked-action
(~3.5 calls/batch). The cost is `lane_control` (**5299 calls / 30 batches**), driven by the
"lightweight approximation" nested loop: for **each lane-blocker d** x **each receiver**, it
re-runs `lane_control` without d to get `delta_P_received` - O(blockers x receivers) per action.
`lane_control` itself is TTI-geometric (samples the passing corridor), NOT pitch-control.

Quality-preserving fixes:
- **Prune blockers that cannot affect a given receiver's lane (safe, exact).** Re-running
  `lane_control` without a blocker `d` only changes the result if `d` lies within (or near) that
  receiver's lane corridor. Restrict the inner re-run to blockers whose position is within the
  corridor half-width of the passer->receiver segment; for all others `delta = 0` exactly. A
  geometric pre-filter, not an approximation - identical output, large call reduction in
  compact/wide formations.
- **Vectorize the per-receiver / per-blocker lane probabilities.** `lane_control` already builds
  `center/left/right` sample arrays with numpy; the per-(blocker, receiver) re-runs can be batched
  into a single vectorized `_compute_lane_probabilities` over an (n_blockers x n_receivers x
  n_sample_points) tensor. Bit-identical if the reduction order is preserved; validate vs the
  scalar path.
- `compute_blocking_score` uses pitch-control (grid-based Voronoi counterfactual) and benefits from
  the shared cache in section 4.

## 4. [IN SCOPE — highest-leverage gold-standard win] Shared per-frame pitch-control surface

`compute_pitch_control` is invoked independently by **four** enrichment families on overlapping
frames: `add_obso` (~6.5k calls/30 batches), `compute_blocking_score` (cover_shadows),
`add_gk_influence` (threat-weighted pitch-control share), and `pitch_control_at_action` (3 methods).
Each recomputes the same per-frame pitch-control field.

**Optimization:** compute the per-frame pitch-control surface **once** per
`(period_id, frame_id, method)` and share it across all consumers in a single enrichment pass
(a frame-keyed cache / surface provider threaded through the tracking aggregators, or memoization
keyed on frame identity + params). **Quality-identical** (same surface, same inputs); the single
biggest remaining lever - it cuts redundant pitch-control across obso + cover_shadows +
gk_influence + pitch_control_at_action at once. This is the "surface-sharing" idea and does NOT
require re-implementing accessible-space (that remains an option only if the cache cannot reach the
accessible-space-backed methods).

**Validation:** a regression test asserting cached vs uncached pitch-control are bit-identical per
frame, plus the existing feature tests (obso / cover_shadows / gk_influence values unchanged).

---

## Downstream coordination

After the silly-kicks fixes land + a version bump + tag, the lakehouse will re-pin
`silly-kicks[das]>=<new>` and (a) ELASTIC columns will populate for IDSSE, (b) the
`_restrict_to_linked_frames` workaround can be deleted (harmless to keep — it's idempotent). No
lakehouse API change is required from your side; just the version bump.
