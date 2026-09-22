# ADR-085: ExT-v2 — single canonical xT surface (retire in-repo v1, consume silly-kicks `ExpectedThreat`)

| Field | Value |
|---|---|
| **Date** | 2026-09-21 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (owner), luxury-lakehouse session |
| **Cycle** | sk4118 P1, Phase G (folded per owner 2026-09-21) |

## Context

The lakehouse carried **two** Expected-Threat implementations:

1. The in-repo **v1** `analytics/expected_threat.py` — a numpy Markov-chain xT grid (`XTGrid`,
   `compute_expected_threat_grid`, `ZoneCounters`), 12×8 default, producing the bronze
   `expected_threat_grids` table (long-format `zone_x/zone_y/xt_value` rows) consumed by the AC drain,
   off-ball-xt, and the xt-grid seed. `analytics/ext_v2/` was an Optuna-driven **research harness** that
   *reproduces* v1 (`SinghProducer` must match `compute_expected_threat_grid` to tolerance).
2. **silly-kicks `ExpectedThreat`** (`silly_kicks.xthreat`), the toolkit's own xT — 16×12 default, with
   a `.transition_matrix` the TF-54b `territory` counterfactual and `destination_profiles` require.

The sk4118 `territory` counterfactual (Phase C) needed a fitted sk `ExpectedThreat` **with** its
transition matrix, which the value-only v1 grid could not supply — so `territory_writer` had been
driver-fitting its own sk model (a second, divergent xT surface + a full-fact `.toPandas()` fit). That
made **three** xT surfaces in the lakehouse (v1 bronze grid; territory's driver-fit; sk elsewhere).

sk 4.121.0 (ADR-100) added `ExpectedThreat.to_dict()/from_dict()`, a JSON cross-process transport for a
fitted model — the missing piece to make one fitted sk `ExpectedThreat` the single surface everywhere.

## Decision

**Retire the in-repo v1 xT entirely; the ONE canonical xT surface is a fitted silly-kicks
`ExpectedThreat` (16×12), persisted once and reconstructed by every consumer.**

- **Producer** (`ingestion/expected_threat.py` daily + `scripts/compute_xt_grid_hf.py` HF mirror): fit
  `ExpectedThreat(l=16, w=12)` per competition + a global (union-fit — sk has no additive `ZoneCounters`
  equivalent, so the global grid is fit on the concatenated per-comp slices), and persist each model's
  `to_dict()` JSON. Bronze `expected_threat_grids` schema changes long-format zone rows →
  `competition_id / xt_model_json / format_version / _ingested_at` (one row per competition + `global`).
- **Consumers reconstruct via `from_dict`**: the AC drain (`ingestion/action_context.py` loader; the
  drain already reconstructed an sk `ExpectedThreat`, now it gets the transition matrix too),
  `territory_writer` (drops its driver-fit + the `_topandas_exemptions` entry — loads the canonical
  bronze model), `off_ball_xt` (`analytics/off_ball_xt.py` + `ingestion/off_ball_xt.py` — `XTGrid.lookup`
  → the sk `values_at_points` seam).
- **ADR-063 guards** (directionality / structural / materiality-drift), which lived as `XTGrid` methods,
  are reimplemented against the sk model / its `.xT` in a new `analytics/xt_grid_guards.py`. The
  directionality gate samples through the sk `physical_grid` seam (ADR-041 orientation-neutralised), so
  it is orientation-safe by construction (raw `.xT` is `(w, l)` y-inverted; x-axis un-flipped —
  empirically pinned).
- **Retire** `analytics/expected_threat.py` (v1) + the entire `analytics/ext_v2/` research subsystem +
  its `scripts/run_ext_v2_phase{0,1}.py` runners + smoke gates, and **drop the Optuna dependency** (its
  sole consumer was ext_v2). Excise the `ext_v2_p0/p1` cycle-items from the dormant SK3-MIG-B retrain
  orchestrator/telemetry/parity scaffolding.

## Amendment (2026-09-21, sk4118 G-fix) — derived zone-table companion for SQL consumers

The original decision missed two **dbt SQL** consumers of the retired long-form grid:
`fct_action_values.gk_xt_delta` (ADR-056) and `fct_goalkeeper_stats`, which do per-zone xT lookups in
SQL (`SELECT zone_x, zone_y, xt_value ... WHERE competition_id='global'`). A JSON `xt_model_json` blob
cannot serve a per-zone SQL join, so dropping the long-form table would have broken both marts' dbt
build once the G1 migration applied.

**Resolution (approach 1, owner-approved):** the producer writes a SECOND bronze table
`expected_threat_grid_zones` — a deterministic **16×12 physical-oriented long-form projection of the
SAME fitted model** (via the sk `physical_grid` seam, ADR-041), written ALONGSIDE the JSON under the
same materiality gate so the two never diverge. It is **not a second fit** — the single-canonical-surface
invariant holds (JSON is canonical for Python consumers; the zone table is a read-only projection for
SQL consumers). Both marts re-point to it and their binning moves 12×8 → 16×12 (clamps 15/11), so
`gk_xt_delta` / `gk_xt_delta_total` / `gk_xt_per_pass` re-baseline with the rest of the AC xT recompute
in P2. `_spadl__sources.yml` gains the new source; the G1 migration additionally `CREATE`s the zone
table. Projection helper: `ingestion.expected_threat._project_physical_zones`; test:
`test_expected_threat_producer.py::test_project_physical_zones_shape_and_orientation`.

## Alternatives considered

| Option | Why rejected |
|---|---|
| (A) territory-cf only: persist a sk `to_dict` for territory; leave the AC drain / off-ball / GK on the v1 grid | Leaves the multi-surface divergence the owner rejected; territory-cf still differs from every other xT consumer. |
| (B) re-point `ext_v2` to sk instead of retiring it | `ext_v2` is a *v1-reproduction* harness — once v1 is replaced by sk, its reproduction target is gone; its purpose evaporates. |
| **(C) chosen** — retire v1 + ext_v2, single sk `ExpectedThreat` surface via the `to_dict` bronze contract | — |

## Consequences

### Positive
- One canonical xT surface: the AC drain, territory (both methods), off-ball-xt and the xt seed all
  read the SAME fitted sk model. The territory driver-fit + its full-fact `.toPandas()` are gone.
- The bronze `to_dict` payload carries the transition matrix, so any future counterfactual consumer is
  served without a re-fit.
- Deletes ~600 lines of v1 + a whole research subsystem + the Optuna dependency (fewer CVEs to floor).

### Negative
- **Forced recompute (P2):** the 16×12 sk surface differs numerically from the 12×8 v1 grid, so every
  xt-derived AC column re-baselines. The producer runs FIRST, then the AC re-materialize (rides P2's
  already-planned AC recompute). AC goldens (`ac1_{full,mini}`) regenerated; the `xt_grid.parquet`
  golden fixture is a deterministic sk fit on the committed fixture actions (the P2 extractor refreshes
  it from the live sk bronze).
- The bronze `xt_model_json` is a cross-consumer contract — a `format_version` bump is a coordinated
  change (mirrors ADR-100's own contract).
- Global-grid producer reverts the OPT-1 `ZoneCounters` streaming for the global grid (sk has no
  additive fit) — the global fit holds the union of the per-comp filtered slices at the driver
  (bounded, well within 16 GB; per-comp `.toPandas()` calls stay `competition_id`-filtered, so no new
  boundedness-gate violation).

### Neutral
- `goalkeeper.compute_gk_distribution_xt` (dead, v1-independent — takes a raw ndarray) is untouched.

## Related

- **ADR-100** (silly-kicks): the `ExpectedThreat.to_dict()/from_dict()` serialize seam this consumes.
- **ADR-063**: the xT-grid directionality / materiality guards, reimplemented in `xt_grid_guards.py`.
- **ADR-041** (silly-kicks): the `.xT` y-inverted storage orientation the `physical_grid` seam neutralises.
- **Spec** `docs/superpowers/specs/2026-09-17-silly-kicks-4118-adoption-design.md` §5.5; **plan** Phase G.
