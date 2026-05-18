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
| PR 1 | Foundation: `generate_match_key` macro + `dim_matches` + ADR-011 | Shipped (2026-04-21, #165) |
| PR 2 | Passes conformed + LB-IDSSE + LB-METRICA functional surfacing + bronze→staging completeness sweep | Shipped (2026-04-22) |
| PR 3 | Shots + xG migration | Planned |
| PR 4 | Action values + VAEP migration | Planned |
| PR 5 | Player stats + embeddings migration | Shipped (PR 5a #190 2026-04-24, PR 5b #202 2026-04-25) |
| PR 6 | Defensive + goalkeeper + pitch control migration | Shipped (#207 2026-04-27, plus followups #208/#209/#210 + DEFCON-cast-fix branch widening BIGINT end-to-end through staging+marts and adding `test_defcon_schema_parity.py` writer/DDL guard) |
| PR 7 | Tracking + formations + pausa + tail facts migration + Q3 conformed-fact closures (fct_passes/fct_shots/fct_action_values/fct_line_breaking_results gain team_key+player_key) + Option A SkillCorner dim onboarding + fct_match_summary tracking-provider home/away extension. **`fct_pausa_values` also promoted Python→dbt mart under ADR-013 as part of this PR**. **`pitch_control_batch.py` writer schema widened with data_source + match_key, collapsing the PR 6 prefix-CASE bridge in stg_pitch_control__values to a passthrough.** | Shipped (#214 2026-04-27 + post-deploy hotfixes #215/#216/#217/#218/#219/#220 closing latent staging-canonicalization gaps, see "PR 7 lessons-learned" below) |
| PR 8 | Scripts + final cleanup + doc sweep | Planned |

> **ADR-013 applications:** PR 3 is the first application of [ADR-013](ADR-013-ml-inference-outputs-dbt-mart.md) (xG v2 promotion to `fct_xg_predictions_v2`); PR 7 is the second (`fct_pausa_values` promotion to a dbt mart under the same pattern).

After PR 8 merges, the warehouse contains zero smart-keyed `match_id` columns. Legacy bronze tables retain their native match_ids (provenance layer).

### Collision math

`xxhash64` is a 64-bit hash. Birthday collision probability for `N` hashed items is approximately `N² / 2·2⁶⁴`.

- `N = 10,000` per provider → ~2.7 × 10⁻¹²
- `N = 40,000` total across 4 providers → ~4.3 × 10⁻¹¹
- `N = 100,000,000` → ~2.7 × 10⁻⁴ (revisit with `xxhash128` or `uuid_v5` if the dim ever grows this large)

Comfortably below any operational threshold at the foreseeable scale.

### Staging canonicalization principle

PR 7's hotfix #3 surfaced a recurring pattern that warrants codification: **native identifiers from a provider must be canonicalized to dim-compatible form at the staging boundary, never inside individual mart SQLs.**

Concretely, every `stg_<provider>__<entity>.sql` is responsible for transforming the raw bronze identifier into the exact form that `dim_matches.native_match_id` / `dim_teams.native_team_id` / `dim_players.native_player_id` use for that provider:

- **IDSSE** — strip the `idsse_` prefix on bronze `match_id` (bronze carries `idsse_J03WMX`, dim carries `J03WMX`).
- **Metrica** — synthesize `metrica_<match_id>_<team_side>_<player_map_key>` from the raw bronze map key + match + team side, since dim_players uses the synthesized form (PR 5a). Strip the `Player` prefix where it appears in raw bronze data.
- **Wyscout** — drop `playerId: 0` "unknown player" sentinel rows at the staging boundary (verified 0.65% of action rows, 5% of pass-recipient rows; the sentinel never resolves in dim_players).
- **StatsBomb** — passthrough; bronze native IDs already match dim form.

The reason this principle is load-bearing: any mart that reads `stg_<provider>__<entity>` and JOINs to `dim_*` on `(provider, native_*_id)` must be able to assume the staging output is already canonical. If canonicalization is duplicated inside individual marts, the next mart that's added either re-derives the same logic (DRY violation) or silently produces NULL FKs at scale (the bug class PR 7 hotfix #3 surfaced across 12 catastrophic + 8 partial mart resolutions).

Audit hook: any new `stg_<provider>__<entity>.sql` PR must document its canonicalization transform in the file header. Any mart that still has a `regexp_replace`/`concat`/sentinel-filter on a native ID is treated as evidence that canonicalization has leaked out of staging — fix in staging instead.

### PR-LL2 (2026-04-29) — `*_native` STRING column extension to bronze layer

PR-LL2 (silly-kicks SPADL post-conversion enrichment + 4-source coverage) extends ADR-011's `(provider, native_id)` join pattern from `dim_*` (silver) into `bronze.spadl_actions` and `bronze.vaep_action_values` directly. The two bronze tables now carry five `*_native` STRING columns: `team_id_native`, `home_team_id_native`, `competition_native_id`, `season_native_id`, `match_id_native`. Always populated for all 4 sources; for StatsBomb/Wyscout the values are stringified ints, for IDSSE these are real DFL identifiers (`DFL-CLU-XXXXXX` / `DFL-COM-XXXXXX` / `DFL-SEA-XXXXXX`), for Metrica they are synthetic per-match identifiers (`Sample_Game_N-Home/Away` / `metrica-sample` / `metrica-open-2017`).

Why bronze, not staging-only: IDSSE, Metrica, and SkillCorner carry STRING native identifiers that don't fit the legacy BIGINT-typed `bronze.spadl_actions.competition_id` / `season_id` / `player_id` columns; those legacy columns remain NULL. The `match_id` / `game_id` and `team_id` BIGINTs are populated via deterministic `hash_native_id_to_bigint` (SHA-256[:15]) — `match_id` so `applyInPandas(groupBy("match_id"))` continues to dispatch per-match groups, `team_id` so VAEP's `fs.team()` and `sameteam` equality comparisons produce correct features. Original strings preserved in `match_id_native` / `team_id_native`.

PR-LL2 also closes a 24-hour mart-level alias introduced by PR-LL1: `possession_team_id` (the alias of `statsbomb_possession_team_id`) was retained in PR-LL1 inside the standard 90-day dual-column window. PR-LL2 dropped the alias — acceptable in this specific case because zero downstream consumers had time to accrue dependence on the alias (`hf_taipy_app/`, `src/`, `dbt_project/` greps clean at rename time). The 90-day window remains in force for the original PR 8 legacy columns; sunset 2026-07-22 unchanged. See ADR-016 for the canonical/native naming rule that drove the rename.

### PR 7 lessons-learned

The original PR 7 (#214) shipped 2026-04-27 with green CI and green slim-CI. Six post-merge hotfix PRs (#215–#220) closed latent gaps that live-CI's `state:modified+` selection could not surface. Documenting the gap classes here so future Kimball PRs can prevent rather than chase them:

1. **Wyscout `playerId: 0` sentinel** (#215, #220) — bronze rows where Wyscout's open-data has no recorded player. Always 0; never resolves in dim_players. Filter at staging, not in marts. Affected `fct_passes` (28k rows) and `fct_action_values` (16k rows) and `fct_off_ball_xt` (small).
2. **IDSSE `idsse_` prefix on bronze `match_id`** (#216, #217, #220) — bronze carries the prefix, dim does not. The prefix-strip must happen in every `stg_<entity>__*` that reads IDSSE bronze, not in `int_*` or fact marts. Affected tracking, off_ball_xt, formation_labels, pausa staging.
3. **Metrica synthesized `player_id`** (#216, #217) — dim_players uses `metrica_<match>_<side>_<map_key>` (PR 5a). Bronze tracking carries the bare map key. Stripping `Player` prefix is a separate but related normalization. Affected tracking, off_ball_xt, formation_labels, position_maps, player_positions, pausa rankings.
4. **Cross-staging dim resolution requires bridge views** (#217) — when a mart needs to resolve `team_key` from a tracking-provider source where the staging row carries only `(side='home'|'away', match_id)`, the answer requires another provider's staging table (e.g., `stg_idsse__home_away_teams` or its successor). Express via a Kimball factless-fact bridge under `intermediate/` (`int_tracking__match_side_team_bridge`, `int_tracking__player_match_team_bridge`). Never embed cross-staging JOINs inside individual mart SQLs.
5. **`contract: enforced: true` validates only on `--full-refresh`** (#218) — incremental builds with `on_schema_change='append_new_columns'` silently absorb new columns via `ALTER TABLE` without invoking the contract assertion. dbt-live-CI runs `state:modified+` (incremental), so contract drift surfaces only at the next `--full-refresh` deploy. Mitigation: any PR that adds a SELECT-emitted column to a `contract: enforced: true` incremental mart must update the YAML contract in the same commit. Captured in `memory/reference_contract_enforced_full_refresh_only.md`.

Process change: PR 8 (Scripts + final cleanup) will scope-include a "provider-add scaling test" that exercises every staging canonicalization for every provider against every Kimball-FK mart, so that the next provider added (Respo.Vision, SkillCorner-events, homegrown tracking) does not relitigate the gap class. Tracked as TODO G4.
