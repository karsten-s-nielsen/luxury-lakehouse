# ADR-011: Unified Kimball Match Dimension with Conformed Pass Fact

| Field | Value |
|---|---|
| **Date** | 2026-04-20 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The warehouse ingests match-level data from four providers with heterogeneous native match-identifier formats:

- StatsBomb: BIGINT (e.g., `3895302`)
- Wyscout: BIGINT (e.g., `5154201`)
- IDSSE: STRING (e.g., `J03WMX`, carried as `idsse_J03WMX` in bronze)
- Metrica: STRING (e.g., `Sample_Game_1`)

Until PR 1, fact tables stored the native ID directly in a column called `match_id` typed as `BIGINT` — relying on the happy accident that StatsBomb and Wyscout integer IDs did not collide in the observed ranges. This is a "smart key" anti-pattern: source semantics embedded in the primary key. It has three concrete symptoms:

1. **Type mismatch when landing tracking-provider passes.** Metrica and IDSSE `match_id` values are strings but `fct_passes.match_id` is `BIGINT`. Attempting to union them into `fct_passes` fails the dbt contract.
2. **Cross-provider collisions are theoretically possible.** StatsBomb and Wyscout both use small positive integers; only the observed distribution has kept them apart.
3. **Schema-level coupling between source system and warehouse.** If StatsBomb renumbers their open-data matches, our fact tables must rebuild.

The forcing function for this ADR is the LB-IDSSE + LB-METRICA cycle, which requires landing tracking-provider passes in `fct_passes`. The options are documented in §Alternatives considered.

## Decision

Adopt a Kimball-style conformed match dimension (`dim_matches`) keyed by a **deterministic surrogate `BIGINT`** generated via the `generate_match_key(provider, native_match_id)` dbt macro (Spark `xxhash64` over `concat_ws('|', provider, cast(native_match_id as string))`). Every fact table that references a match will carry `match_key BIGINT` as a foreign key to `dim_matches.match_key`. Natural keys (`provider`, `native_match_id`) are preserved on the dim as attributes for lineage, debugging, and human-readable joins.

The migration is staged across PR 2 through PR 8 to keep each PR reviewable and each deploy reversible. PR 1 (this PR) ships the dim and macro only; no fact tables are modified.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Synthetic bigint for tracking-only; keep native IDs on StatsBomb/Wyscout | Minimum blast radius for the LB-IDSSE cycle; tracking providers get a surrogate, existing facts unchanged | Kimball-violating (smart keys remain); postpones the right thing; every new provider relitigates the decision | Smart-key anti-pattern perpetuated; structural debt |
| B. Stringify `match_id` across all facts; use native strings everywhere | No surrogate layer; raw ID visible in every mart | Huge blast radius (all Lakebase synced tables recreate, PG indexes rebuild, many Taipy query type-compat audits); smart-key anti-pattern still present; string joins slightly less performant than BIGINT | Blast radius + retains smart keys |
| C. Kimball surrogate on unified `dim_matches` (chosen) | Warehouse independence from source systems; collision-free by construction (hash includes provider); deterministic under rebuild; single-column BIGINT join; Type-2 SCD-ready; new providers plug in uniformly | Larger migration (fact-layer rename from `match_id` to `match_key`); extra dim-join for debugging raw native IDs; requires an ADR + a staged rollout plan | — |

## Consequences

### Positive

- **Collision-free across providers.** The hash includes `provider` in the input, so Wyscout `match_id=123` and StatsBomb `match_id=123` produce different `match_key` values even though their natives collide as integers.
- **Warehouse-source independence.** If StatsBomb changes their match-ID scheme, our downstream keys are unchanged.
- **Determinism across rebuilds.** `xxhash64` is pure; `dbt build --full-refresh` produces identical `match_key` values.
- **Uniform provider onboarding.** Respo.Vision, SkillCorner events (when they arrive), homegrown tracking all plug in via a new staging model + dim union — no architecture discussion per provider.
- **Conformed-fact alignment.** Downstream unified facts (`fct_passes`, `fct_match_summary`, `fct_line_breaking_results`) use a single BIGINT FK. Cross-provider analytics become one-table queries.

### Negative

- **Migration cost.** ~28 mart tables + ~23 Taipy UI files + ~80 Python modules reference `match_id` today. Migrating each to `match_key` is spread across PR 2-8 to stay reviewable.
- **Extra dim-join for raw native IDs.** Debugging from `fct_passes` back to StatsBomb's native match page requires joining `dim_matches` to recover `native_match_id`. The one-hop cost is low; the indirection is the price of the surrogate.
- **Lakebase synced-table recreation.** Each migrated fact table must recreate its synced table to accommodate the column rename, triggering grant re-application per ADR-005. Managed by scheduling migrations in PR-sized batches.

### Neutral

- **Surrogate is signed BIGINT.** Spark's `xxhash64` returns a signed 64-bit integer, including negatives. PostgreSQL `BIGINT` accepts the full int64 range, so no adjustment is needed. Signed vs unsigned does not affect collision probability.
- **Delimiter choice `'|'`.** Prevents concatenation ambiguity. Not present in any current provider name or native ID format. Documented in the macro source.

## CLAUDE.md Amendment

No CLAUDE.md amendment. This ADR establishes a new pattern that complements existing rules rather than carving out an exception.

## Related

- **Branches:** `feat/tracking-passes-idsse-metrica`
- **Plans:** `docs/superpowers/plans/2026-04-20-pr1-kimball-foundation.md`; subsequent plans for PR 2-8 will be written per PR.
- **ADRs:** ADR-005 (Lakebase synced-table grants — each migration PR will re-apply grants).
- **External references:**
  - Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. (Wiley 2013), Ch. 1 "Dimensional Modeling Primer" pp. 13–16 on surrogate keys; Ch. 4 on conformed dimensions.
  - Spark `xxhash64` documentation: https://spark.apache.org/docs/latest/api/sql/index.html#xxhash64

## Notes

### Staged rollout policy

| PR | Scope | Status |
|---|---|---|
| PR 1 | Foundation: `generate_match_key` macro + `dim_matches` + ADR-011 | Active |
| PR 2 | Passes conformed + LB-IDSSE + LB-METRICA functional surfacing | Planned |
| PR 3 | Shots + xG migration | Planned |
| PR 4 | Action values + VAEP migration | Planned |
| PR 5 | Player stats + embeddings migration | Planned |
| PR 6 | Defensive + goalkeeper + pitch control migration | Planned |
| PR 7 | Tracking + formations + pausa + tail facts migration | Planned |
| PR 8 | Scripts + final cleanup + doc sweep | Planned |

After PR 8 merges, the warehouse contains zero smart-keyed `match_id` columns. Legacy bronze tables retain their native match_ids (provenance layer).

### Collision math

`xxhash64` is a 64-bit hash. Birthday collision probability for `N` hashed items is approximately `N² / 2·2⁶⁴`.

- `N = 10,000` per provider → ~2.7 × 10⁻¹²
- `N = 40,000` total across 4 providers → ~4.3 × 10⁻¹¹
- `N = 100,000,000` → ~2.7 × 10⁻⁴ (revisit with `xxhash128` or `uuid_v5` if the dim ever grows this large)

Comfortably below any operational threshold at the foreseeable scale.
