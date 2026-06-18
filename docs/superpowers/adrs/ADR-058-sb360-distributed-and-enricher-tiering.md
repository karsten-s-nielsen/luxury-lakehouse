# ADR-058: SB360 action-context — snapshot vectorization, enricher tiering, and distributed rewrite

| Field | Value |
|---|---|
| **Date** | 2026-06-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

StatsBomb-360 (sb360) is the frames-required action-context tier for `statsbomb` (ADR-057): freeze-frame
snapshots are converted to synthetic tracking frames and enriched. A `max_units=8` AC run measured
**~15 min/match** for sb360 — far slower than expected. The cost was localized with a faithful
**serverless one-off timing probe** (full data, match 3788746, deployed wheel 0.5.43):

```
read spadl_actions toPandas : 21.4s
read statsbomb_360 toPandas :  1.9s     ← table scans are NOT the bottleneck
build_snapshots (iterrows)  : 147.7s    ← DOMINANT (52%); 56,207 raw rows -> 32,633 snapshots
enrich_sb360_match          : 109.1s    ← 2nd (includes ghost-GK in the deployed wheel)
```

Two earlier hypotheses were **refuted by measurement**: (1) per-match table scans dominate — false
(`statsbomb_360` reads in 1.9s; bronze is unclustered but reads fast), so **no bronze clustering is
warranted**; (2) ghost-GK is ~83% of the cost — false, that came from a contaminated local fixture
(the Databricks Statement Execution API serializes booleans to the strings `'true'`/`'false'`, and
`bool('false') is True`, which mis-flagged every player as goalkeeper; the same API also truncates
large results to the first chunk, so the fixture had 34% of the freeze-frames). The real hot spot is
the per-row `iterrows`+`json.loads` snapshot loop.

Separately: ghost-GK is a velocity-aware tracking model (5 of its 26 features are velocity-derived);
freeze-frames have no velocity, so those features are NaN and the tree-ensemble leaf-matching the KDE
depends on degenerates → ~7–14% action coverage with ~85% of those positions clamped off-pitch. And
the cycle's headline feature `pitch_control_at_target__voronoi` was never emitted on sb360 even though
voronoi is position-only and feasible. Finally, the sb360 path resolved `home_team_id` as
`str(unique()[0])` — an arbitrary (often away) team — so orientation-aware enrichers were
systematically wrong for ~half of matches; and it ran driver-side, per match, inside the 8-worker
drain (per-match `replaceWhere` commit contention).

## Decision

For the sb360 action-context path: (1) **vectorize** the snapshot build into a pure, shared
`analytics.action_context.sb360_snapshots.build_sb360_snapshots` (147s → ~seconds); (2) **exclude
ghost-GK** and **emit `pitch_control_at_target__voronoi`** in `_enrich_sb360_match`; (3) resolve
`home_team_id` from the real `home_team_id_native` via a shared `resolve_home_team_id`; (4) **process
all pending statsbomb matches in one distributed `cogroup.applyInPandas` job** (statsbomb exits the
per-match drain), scanning each bronze table once and writing with `replaceWhere="data_source =
'statsbomb' AND match_id IN (<native ids>)"`. **No bronze clustering** (scans measured fast).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Liquid-cluster bronze by `match_id` | helps all per-match reads | the reads are already 1.9s/21s | the scan is not the bottleneck — measured |
| B. Backend port / GPU for ghost-GK on sb360 | reuse the enricher | velocity features still NaN → degenerate, clamped | OOD model; can't run properly without a position-only re-fit |
| C. Keep driver-side per-match, just vectorize snapshots | smallest change | leaves the 8-worker per-match commit contention + no scan amortization at backlog scale | partial; chosen vectorization + the distributed rewrite together |
| D. (chosen) vectorize + tier + distributed cogroup + native home | fixes hot spot, quality, orientation, and scale | larger surface; cogroup is a new API here | — |

## Consequences

### Positive

- sb360 snapshot build drops from ~147s/match to ~seconds (vectorized); enrich drops further with ghost-GK gone.
- `pitch_control_at_target__voronoi` is now populated on sb360 (the headline at_target feature).
- Orientation-aware enrichers (team_shape / defensive_line / line_break(ward) / shape_graph / gk_influence) are now correctly oriented (real home team), fixing ~half of sb360 matches.
- One distributed job scans each bronze table once and removes the per-match `replaceWhere` commit contention; scales to the full statsbomb backlog.

### Negative — value drift on every previously-processed sb360 match (THREE independent sources)

Re-materializing sb360 AC shifts values for three reasons, all of which the golden regen + downstream consumers must account for:
- `ghost_gk_x/y/density_spread/method` become **NULL** (Hyrum check: `fct_action_context` is a leaf mart; ghost-GK was sb360-degenerate anyway).
- `pitch_control_at_target__voronoi` goes from NULL to populated.
- Orientation-aware columns change because home flips from arbitrary-team to real-home for ~half of matches.

### Neutral

- The dup-`original_event_id` `keep="last"` tie-break is an **inherited arbitrary artifact** preserved for parity (Chesterton's fence) — not a designed choice; a future pass might attach the freeze-frame to the event's primary action. Made deterministic via a `sort_values("action_id")` at the path entry.
- cogroup is the first use of `cogroup.applyInPandas` in this codebase (only `groupBy().applyInPandas` existed). Its Arrow conversion has no intervening `createDataFrame` coercion, so the float64↔BIGINT seam (all-NaN tracking columns) is verified live, not offline (pyspark is mocked in CI).

## References

ADR-056 (at_target / Kimball slim), ADR-057 (frames-required), ADR-037 (worker-drain fan-out),
ADR-019 (cross-table id canonicalization), ADR-045 (applyInPandas closure rules / dispatch).
Implementation plan: `docs/superpowers/plans/2026-06-17-sb360-snapshot-vectorization-and-distributed-rewrite.md`.
