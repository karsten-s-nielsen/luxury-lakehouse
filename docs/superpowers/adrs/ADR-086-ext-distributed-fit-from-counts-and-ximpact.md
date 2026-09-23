# ADR-086: Distributed `fit_from_counts` ExT producer + xImpact adoption (sk4123 P2)

| Field | Value |
|---|---|
| **Date** | 2026-09-22 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (owner), luxury-lakehouse session |
| **Amends** | [ADR-085](ADR-085-ext-v2-single-canonical-xt-surface.md) (ExT-v2 single-canonical xT surface) |
| **Depends on** | silly-kicks 4.123.0 — `ExpectedThreat.fit_from_counts` (SK-XT-COUNTS, sk ADR-102) + TF-63 xImpact (sk ADR-101) |

## Context

Two changes ship together in one PR (two commits) because both must land before the P2 recompute so a
single recompute session materializes both.

**1 — the ExT producer was driver-bound and timed out.** ADR-085 made the canonical xT surface a fitted
silly-kicks `ExpectedThreat`, but sk exposed only `.fit(actions)`. So `ingestion.expected_threat.run_pipeline`
pulled each competition's actions to the Spark driver via `.toPandas()` (~30 reads) and `pd.concat`-ed ~1 GB
for the global fit. In P2 (2026-09-22) it hit the 600 s task timeout with 0/28 grids — and it was a scale
cliff. Before ADR-085 the in-repo v1 producer used single-pass distributed ZoneCounter accumulation; that had
to be reverted because sk had no counts-based fit.

**2 — xImpact rides the same recompute.** sk 4.123.0 also carries TF-63 xImpact (`VAEP.rate_ximpact` +
`win_probability`). `xImpact = VAEP_adjusted × ΔP(win | goal)` is a per-action value — the same grain as the
existing `vaep_value` / `vaep_adjusted_value` on `fct_action_values`. Adopting it now (bundled with the P2
`compute_spadl_vaep` re-run) avoids a second full-corpus VAEP pass later.

## Decision

**Commit 1 — distributed count aggregation.** Rewrite `run_pipeline` to fit every grid from
`ExpectedThreat.fit_from_counts`: one Spark `groupBy` reduces the corpus to per-`(competition, zone)` counts
(`_aggregate_counts_spark`), the `global` grid is the element-wise sum of the per-competition counts (additive),
and each grid is fit from those. No `.toPandas()` of action rows, no `pd.concat` of the corpus. The binning is
replicated in Spark SQL (`_sql_flat_index`) from the same constants as a pure-numpy mirror (`_zone_flat_index`,
parity-tested vs sk `_get_flat_indexes`). Only `{pass, dribble, cross, shot}` are counted — the sk grid ignores
the rest of `_RELEVANT_TYPES`. This restores the OPT-1 single-pass accumulation ADR-085 had to revert. The
manual HF producer (`scripts/compute_xt_grid_hf.py`) stays on `.fit` (off the timeout critical path; byte-
identical grids). Grids are byte-identical to the `.fit` path (proven on a fixture + the frozen-oracle chain).

**Commit 2 — xImpact + win_prob_leverage on `fct_action_values`.** `ingestion.spadl_vaep` computes, per action,
`win_prob_leverage` (`goal_leverage`, ΔP(win|goal)) and `ximpact` (`VAEP_adjusted × win_prob_leverage`) via the
bundled `WinProbabilityModel`, byte-consistent with `VAEP.rate_ximpact`. The home `team_id` for `goal_leverage`
is resolved **provider-agnostically** from the `team_id_native == home_team_id_native` mapping in the data —
NOT by re-hashing the native id (`team_id` is `hash_native_id_to_bigint` for IDSSE/Metrica/SkillCorner/GS but
the stringified numeric for StatsBomb/Wyscout, ADR-016; a blanket hash would mis-resolve the open-data
providers). Two additive columns + a per-player `total_ximpact` rollup on `fct_vaep_breakdown_agg`. Under the
existing `wf-vaep` evaluative card (no new card).

## Alternatives considered

| Option | Why rejected |
|---|---|
| Raise the `compute_expected_threat` timeout | band-aid; the driver pull is still a scale cliff (owner rejected) |
| Interim lakehouse-only `applyInPandas` per-comp fit | throwaway; the global leg still pulls ~1 GB to the driver; saves no P2 time |
| Blanket-hash `home_team_id` for `goal_leverage` | mis-resolves StatsBomb/Wyscout (numeric `team_id`, not hashed) → all-NaN leverage on the open-data corpus |
| Defer xImpact to a later cycle | would need a second full `compute_spadl_vaep` corpus pass; owner chose to fold it into the one P2 recompute |

## Consequences

### Positive
- `compute_expected_threat` fits from one distributed pass — no driver `.toPandas()`/`pd.concat`; cost no longer grows with the corpus.
- xImpact + leverage land on `fct_action_values` in the same recompute; per-player `total_ximpact` for free (dbt-only).
- Byte-identity: xT grids == the `.fit` path; `ximpact` == `VAEP.rate_ximpact`.

### Negative
- Folding xImpact in adds one `compute_spadl_vaep` full-corpus re-run to the P2 operator session.
- The Spark binning SQL (`_sql_flat_index`) mirrors the pure `_zone_flat_index` by construction; the unit gate pins the pure function vs sk, the SQL is validated by the live drain.

### Neutral
- No new mart, workflow card, C4 entity, or synced table. sk floor bumped 4.121.0 → 4.123.0 (ADR-046 exact pins). Governance folds under `wf-vaep` (AI_GOVERNANCE §5, `vaep-model.md`, ARCHITECTURE Appendix D: Dixon & Robinson 1998, Robberechts et al. 2019).

## Amendment (2026-09-22, wheel 0.5.113) — grid-write dtype contract + a write→DDL parity guard

The first P2 `compute_expected_threat` run fit the grids correctly but failed the WRITE with
`[DELTA_FAILED_TO_MERGE_FIELDS] format_version`: `_write_grid_if_material` built the JSON-grid payload
with `format_version=[1]`, which pandas infers to int64 → Spark LONG, and `replaceWhere` into the
`format_version INT` column rejects the LONG↔INT merge (the zones write already cast int32; the grid
write did not). A Phase-G (ADR-085) latent gap — the write path had no unit test ("exercised by the live
drain, not here") and was never reached before (env-fail → timeout at 0/28).

**Contract:** an inferred-schema Delta write (`spark.createDataFrame(pandas)`) must carry EXPLICIT dtypes
matching the target column types — a bare Python int is int64/LONG and only matches BIGINT, never INT.
The grid/zones payloads are now built by `_grids_payload`/`_zones_payload` with explicit int32.

**Defense-in-depth:** `src/tests/test_writer_ddl_dtype_parity.py` — a pyspark-free write-dtype↔DDL parity
guard over EVERY P2 first-materialization writer (the 6 P1 `_struct_type` writers vs their migration DDL;
the ExT grid/zones payloads vs `_RESULTS_SCHEMA`/`_ZONES_SCHEMA`; duels' inferred write asserted free of
INT columns). The audit found the other writers dtype-correct; only the ExT grid write was affected.

## Specs

`docs/superpowers/specs/2026-09-22-ext-producer-fit-from-counts-design.md` +
`docs/superpowers/specs/2026-09-22-ximpact-leverage-action-values-design.md` (both fresh-session reviewed).
