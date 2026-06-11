# ADR-047: AC-1 `_FRAME_BATCH_SIZE` 250 → 2500 (a metric-definition change, not just a perf knob)

| Field | Value |
|---|---|
| **Date** | 2026-06-10 |
| **Status** | Accepted; **amended 2026-06-11 (amendment 2: per-provider sizes + run override)** |
| **Deciders** | Karsten Nielsen, Claude |

## Amendment 2 (2026-06-11): per-provider sizes + run-scoped override

The fixed 2500 failed its first production contact: in the scoped max-units=4 test
(run 883267532931612, 24 units), **13 of 16 tracking units died with
`UDF_PYSPARK_ERROR.OOM`** against the 1 GB serverless UDF cap — gradientsports 4/4,
idsse 4/4, metrica 3/4, skillcorner 2/4 (the run still reported SUCCESS — the known
drain not-fail-loud gap). The local A/B above measured **throughput**, not the memory
envelope — and the envelope grew between the A/B and prod: PR #368 landed the 4.22
column families (xT-GK incl. five philosophy presets, gk_completion) whose per-batch
allocations the A/B never saw.

**Decision:** the batch size becomes per-provider + run-overridable, resolved through ONE
module — `analytics.action_context.batching.resolve_frame_batch_size` — imported by the
Spark driver, the executor UDF closure (the resolved int travels in the closure), the
local hexagon, and the fixture extractor (H3 lockstep is now BY SHARED IMPORT; the
lockstep sentinel asserts function identity instead of constant equality). Per the OOM
census, EVERY provider defaults back to 250 (the universally prod-proven value); the
per-provider map ships EMPTY as the documented seam — an entry requires a passing scoped
prod run at that size on the current column set, cited in a comment. Override precedence:
`frame_batch_size` job parameter → drain worker `--frame-batch-size` → driver env
`AC_FRAME_BATCH_SIZE` → per-provider map → 250.

**Consequences:** goldens regenerate again (the J03WMX golden returns to 250 semantics;
the mini is byte-identical across sizes), and the 8 window-dependent oracle columns
regain differential coverage for 250-providers (provider-conditional split in
oracle_map). The −17% wall win is forfeited everywhere until re-earned with evidence —
the override is the instrument: scoped runs walking the envelope up per provider
(e.g. `{"provider":"metrica","max_units":"4","frame_batch_size":"1000"}`), then a
one-line map edit. The "future batch-size change after data exists requires a full
recompute" caveat below now applies PER PROVIDER.

## Context

The action-context pipeline enriches tracking halves per `(match_id, period, frame_batch_id)`
group, with `frame_batch_id = floor(frame / _FRAME_BATCH_SIZE)`. The batch size was 250 — chosen
when the constraining belief was "250 frames ≈ 200 MB peak" against the 1 GB serverless UDF group
cap. The ADR-045 audit found that comment ~100× stale: a 250-frame IDSSE batch is ~5,750 rows
(~1–3 MB). ADR-045 removed the dominant per-batch overheads (model reload, env fingerprint,
unconditional gc) but deliberately deferred raising the batch size as Tier C, because it changes
golden values.

A local A/B (2026-06-10, `idsse/J03WMX_p1` fixture, pinned env silly-kicks 4.20.1 / numba 0.64.0,
two independent sweeps with a repeat-at-250 noise floor) measured the deferred win and — more
importantly — characterised the value change. Per-batch fixed overhead is ~0.6 s locally; per-half
wall drops ~8% at 500, ~12% at 1000, ~17% at 2500. Prod halves are sparser than the fixture
(~1.6 actions per 250-frame batch vs 3.5), so the prod win is at least the local one.

The decisive finding: the value drift is **not** primarily the savgol velocity-window edge effect
ADR-045 anticipated (that class is ≤0.2% — `actor_speed`, pitch control, gk closing times). It is
**window-semantics**: `*_pre_window` features stop being truncated at 250-frame batch edges
(`n_off_ball_runners_pre_window` changes by up to 8; NaN↔value flips on actions whose pre-window
crossed an old batch edge), and `elastic_sync` searches a wider window. Per H3, the batch size is
part of the domain contract — so raising it is a metric-definition change affecting ~26% of
actions, in the direction of *fuller windows, closer to each metric's intent*.

Timing forcing function: `bronze.spadl_action_context` is at clean slate (2026-06-08 wipe) with
the full all-provider recompute still pending — changing the definition now creates zero
re-derive debt.

## Decision

Raise `_FRAME_BATCH_SIZE` from 250 to 2500 in the AC-1 pipeline (`analytics/action_context/
pipeline.py`, `ingestion/action_context.py`, `scripts/extract_action_context_fixture.py` — H3
lockstep, sentinel-tested), regenerate both goldens, and reclassify the eight empirically
window-dependent oracle columns as `known_divergence` (the legacy 250-batched
`fct_tracking_context` oracle is no longer a valid target for them). `ingestion/
tracking_context.py` stays at 250 — it is a separate, deprecating pipeline.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Stay at 250 | zero value churn | pays ~0.6 s/batch × ~300 batches/half forever; pre-window metrics stay edge-truncated | the truncation is a quality defect, and the clean-slate moment makes the change uniquely cheap |
| B. 1000 | −12% local, smaller drift surface (15.5% of actions) | leaves a third of the measured win; SAME class of metric-definition change — paying the golden-regen/ADR cost for less | if the definition changes at all, take the full win |
| C. 5000+ | marginally more amortization | ~15 groups/half < the observed 12–14 peak executor slots → idles executors at the tail; window gains taper | parallelism floor; 2500 keeps ≥2 waves |
| D. 2500 (chosen) | −17% local per-half wall; ~30 groups/half ≥ 2 waves over peak slots; ~57.5K rows ≈ 10–30 MB ≪ 1 GB cap; pre-windows 10× less edge truncation | golden regen; 7 oracle columns lose differential coverage; coarser straggler granularity | — |

`_UDF_SHUFFLE_PARTITIONS` stays 64 (one variable changed, not two): at ~30 groups/half, ~24 of 64
partitions are non-empty (hash collisions double up a few) — still ≥2 waves over peak concurrency,
and empty partitions are near-free.

## Consequences

### Positive

- Per-half wall drops ≥17% (local measurement; prod is sparser so likely more). On the pending
  ~5.5 h full AC recompute that is roughly 45–55 minutes, recurring on every future recompute.
- `*_pre_window`, off-ball-run, and elastic-sync features see full windows on ~10× more actions —
  values closer to the metric intent.
- M13 single-owner invariant verified to hold at 2500 (identical action_id sets, zero dupes, in
  both A/B sweeps); the pipeline is fully deterministic (repeat-at-250 drift was exactly zero).

### Negative

- Both goldens regenerated — bisecting a value change across this commit requires re-running
  `scripts/build_ac1_*_golden.py` at the old constant.
- Eight window-dependent columns (`n_off_ball_runners[_toward_goal]_pre_window`,
  `max_off_ball_run_displacement_pre_window`, `mean_off_ball_run_speed_pre_window`,
  `actor_{arc_length,displacement}_pre_window`, `pressure_on_actor__bekkers_pi`,
  `n_candidate_frames` — the elastic candidate window, batch-edge-clipped at 250) are now
  `known_divergence` vs the legacy oracle — range/invariant checks only.
- Anything previously computed at 250 is definition-incompatible with new output. Acceptable
  solely because the AC table is empty; a future batch-size change after data exists would
  require a full recompute to stay self-consistent.

### Neutral

- The mini-golden fixture window (~2 batches at 250) is a single batch at 2500; the mini gate
  still recomputes the identical real chain.
- Per-batch elapsed in prod grows ~10× (fewer, bigger units under the same 2700 s per-half
  watchdog — total per-half wall *drops*).

## Related

- **ADRs:** ADR-045 (Tier C deferral → this ADR closes it), ADR-036 (golden gates), ADR-037
  (worker-drain budgets), ADR-028 (hexagonal AC-1 / H3 contract)
- **Issues / PRs:** #360 (per-batch overhead removal), this PR
- **External references:** A/B harness + raw results: `tmp/profile_ac1_batch_ab.py`,
  `tmp/profile_BATCHAB_sweep{,2}.txt`, `tmp/profile_BATCHAB_drift_{500,1000,2500}.csv`
  (session-local; numbers summarised above and in the PR description)

## Notes

A/B summary (idsse/J03WMX_p1, 97 actions, mean of two sweeps; ±5% noise band):

| size | wall vs 250 | actions w/ value drift | drifting cols |
|---|---|---|---|
| 500 | −8% | 10.3% | 31 |
| 1000 | −12% | 15.5% | 38 |
| 2500 | −17% | 25.8% | 41 |

Savgol-class numeric drift (the ADR-045 worry) measured ≤0.2% of column max — negligible. The
drift that matters is window-semantics, concentrated in pre-window/elastic features.
