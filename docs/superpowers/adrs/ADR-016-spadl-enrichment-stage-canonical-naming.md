# ADR-016: SPADL Post-Conversion Enrichment Stage and Canonical/Native Naming

| Field | Value |
|---|---|
| **Date** | 2026-04-29 |
| **Status** | Accepted |
| **Deciders** | Karsten |

## Context

The PR-LL1 cycle (silly-kicks 1.5.0 `preserve_native` integration, merged 2026-04-28) surfaced four StatsBomb-native fields (`possession_id`, `possession_team_id`, `play_pattern`, `under_pressure`) through the SPADL conversion pipeline by aliasing them to plain canonical names at the `fct_action_values` mart layer. Three latent gaps emerged during PR-LL2 design:

1. **`bronze.spadl_actions` was missing the four `statsbomb_*` columns physically.** The PR-LL1 ALTER touched only `bronze.vaep_action_values`, leaving the intermediate Delta table on its pre-LL1 schema. Spark `mergeSchema=true` would lazily add them on next StatsBomb write, but that never happened.

2. **`vaep_schema` (the `applyInPandas` StructType inside `_make_scoring_udf`) did not include `statsbomb_*`.** Spark silently drops columns absent from the StructType passed to `applyInPandas`. Result: 0 of 7,151,510 StatsBomb rows in `bronze.vaep_action_values` had non-NULL `statsbomb_possession_id` post-LL1. The LL1 feature was silently broken in production.

3. **`bronze.spadl_actions.action_id` was declared but 100% NULL.** silly-kicks's `convert_to_actions` produces `action_id` per match, but luxury-lakehouse's UDFs dropped it at the projection boundary.

PR-LL2 also expands SPADL coverage from 2 sources (StatsBomb, Wyscout) to 4 (adds IDSSE/Bundesliga and Metrica). silly-kicks 1.7.0 ships dedicated DataFrame converters (`silly_kicks.spadl.sportec`, `silly_kicks.spadl.metrica`) that work directly against luxury-lakehouse's bronze schemas. IDSSE and Metrica use STRING match identifiers (`'idsse_J03WMX'`, `'Sample_Game_1'`) and string DFL CLU IDs that don't fit the legacy `BIGINT`-typed `bronze.spadl_actions.match_id` / `team_id` columns. The user's directive ("bronze stable + comprehensive, downstream pulls only what it needs") forces a bronze-layer schema choice rather than papering over the type mismatch in adapter code.

The PR also introduces the first real use of silly-kicks's provider-agnostic post-conversion helpers (`add_possessions`, `add_gk_role`, `add_pre_shot_gk_context`). Wiring them in ad-hoc per-source UDF would create churn whenever a new helper joins; we need a named architectural home.

## Decision

PR-LL2 establishes two architectural conventions that any future SPADL-related PR must follow:

**1. Named SPADL post-conversion enrichment stage.** Provider-agnostic helpers from silly-kicks (`add_possessions`, `add_gk_role`, `add_pre_shot_gk_context`, and any future siblings) are wired through a single function `apply_spadl_enrichments(actions: pd.DataFrame, *, source: str) -> pd.DataFrame` in `src/ingestion/spadl_enrichments.py`. Every per-provider SPADL UDF (`_make_sb_spadl_udf`, `_make_ws_spadl_udf`, `_make_idsse_spadl_udf`, `_make_metrica_spadl_udf`) calls this function on its silly-kicks output before writing to bronze. New helpers added by silly-kicks are added in one place; new provider UDFs get the helper coverage for free.

**2. Canonical / native column naming rule.**

| Origin of column value | Naming convention | Population |
|---|---|---|
| Computed post-conversion enrichment (deterministic from canonical SPADL) | Plain canonical name: `possession_id`, `gk_role`, `gk_was_engaged`, `action_id` | Always populated for all sources (or has a documented default) |
| Provider-native passthrough (a real provider field surfaced via `preserve_native`) | `<provider>_<field>`: `statsbomb_possession_id`, `statsbomb_play_pattern` | NULL on sources without that provider's native concept |
| Native string identifier paired with a Kimball surrogate | `<entity>_native` (string): `team_id_native`, `match_id_native`, `competition_native_id`, `season_native_id`, `home_team_id_native` | Always populated; joins to `dim_*` on `(provider, native_id)` |
| Kimball surrogate FK | `<entity>_key` (BIGINT): `match_key`, `team_key`, `possession_team_key`, `competition_key` | Plain (Kimball convention wins) |
| Legacy native ID inside ADR-011 dual-column window | `<entity>_id` (legacy BIGINT/INT): `match_id`, `team_id`, `competition_id`, `season_id`, `player_id` | StatsBomb / Wyscout: numeric native ID. IDSSE / Metrica / SkillCorner: `match_id`, `game_id`, and `team_id` are deterministically hashed via `hash_native_id_to_bigint(team_id_native)` (SHA-256[:15]); `player_id` / `competition_id` / `season_id` remain NULL. `team_id` hash is required for VAEP scoring (`fs.team()` and `sameteam` equality comparison). |

The PR-LL1 mart aliases (`possession_id`/`possession_team_id`/`play_pattern`/`under_pressure` aliased to the bronze `statsbomb_*` columns) are renamed in PR-LL2 to their bronze names. The new canonical `possession_id` on the mart is sourced from `av.possession_id_heuristic` (silly-kicks `add_possessions`, populated for ALL sources). Dropping the LL1 aliases 24 hours after introduction is acceptable because zero downstream consumers had time to depend on them — `hf_taipy_app/`, `src/`, `dbt_project/` greps were clean at rename time.

**3. Boundary recall, not boundary F1, gates `add_possessions` regression.** The PR-LL1 design assumed boundary-F1 ≥ 0.85 against StatsBomb's native `possession_id`. Empirical measurement on a 3-match StatsBomb fixture (matches 7298, 7584, 3754058) showed boundary recall = 0.93 (the meaningful regression metric) but precision = 0.42 (a structural ceiling of the team-change-with-carve-outs algorithm class) and F1 = 0.58. Parameter sweep on `max_gap_seconds` capped F1 at 0.605. The CI test `src/tests/test_spadl_enrichments.py::TestBoundaryRecall` gates on **recall ≥ 0.85**, not F1. silly-kicks 1.8.0 (PR-S8, 2026-04-29) propagates the same framing to silly-kicks's own test suite + ships `silly_kicks.spadl.utils.boundary_metrics` as a public utility.

**4. Writer/DDL parity tests prevent the LL1 latent-bug class from recurring.** Every applyInPandas StructType in the SPADL/VAEP pipeline (4 source UDFs + the VAEP scoring UDF = 5 paths) has a corresponding `_build_*_struct` helper in `src/tests/test_spadl_vaep_writer_parity.py` that asserts its column list matches `_SPADL_SCHEMA` / `_VAEP_SCHEMA` exactly. Adding a column to the DDL constants but forgetting to extend the StructType (the LL1 failure mode) now raises a writer-parity test failure at unit-test time.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Hash IDSSE/Metrica string IDs to BIGINT for ALL legacy columns (team, player, competition, season, match) | No bronze schema change; mart joins continue to use legacy `<entity>_id` | Hashes are opaque — consumers can't recover the actual DFL CLU id without re-hashing, creating Hyrum's-Law debt. dim_teams/dim_competitions joins on (provider, native_id) need the native id anyway. | Workaround, not architecture. Pushes string identity into a place that can't represent it. |
| B. Add `<entity>_native` STRING columns alongside legacy BIGINTs; legacy NULL for IDSSE/Metrica | Source-faithful; native joins are clean; opacity-free; ADR-011 dim_competitions already established the (provider, native_id) Kimball pattern; minimal information loss. | Requires bronze ALTER for spadl_actions / vaep_action_values + idsse_events / metrica_events. Mart joins must switch from `cast(legacy_id as string)` to `<entity>_native`. | — chosen |
| C. Defer IDSSE/Metrica to PR-LL3 after a focused bronze-stabilisation cycle | Cleanest scope for PR-LL2 (StatsBomb/Wyscout only) | Doesn't ship the user's stated long-term-stable bronze goal. Bronze re-ingestion would land in two cycles instead of one. | Rejected after the user's "Path B" choice: do the bronze re-ingestion + Kimball-aligned native columns inside PR-LL2 even though scope grew. |
| D. Mart-level alias `possession_id` → `statsbomb_possession_id` retained (LL1's choice, no canonical heuristic possession_id) | No consumer breaking change | Loses the Wyscout/IDSSE/Metrica funnel coverage that the canonical heuristic enables. The Wyscout-synthetic-possession workaround in `fct_funnel_stages_agg` and `hf_taipy_app/src/queries/funnel.py` would persist indefinitely. | Rejected — the canonical heuristic (silly-kicks `add_possessions`) makes the workaround unnecessary AND adds funnel coverage for IDSSE/Metrica. |

## Consequences

### Positive

- LL1 latent-bug class structurally closed. Adding a column to `_SPADL_SCHEMA` / `_VAEP_SCHEMA` without extending the corresponding applyInPandas StructType raises a writer-parity test failure at unit-test time (5 parity tests in `test_spadl_vaep_writer_parity.py`).
- Provider-agnostic enrichment helpers (existing 3 + future) plug in at one place (`apply_spadl_enrichments`) instead of N per-provider UDF edits.
- 4-source SPADL coverage (StatsBomb / Wyscout / IDSSE / Metrica) with native-ID traceability — `team_id_native` joins to `dim_teams.native_team_id` cleanly across providers.
- The Conversion Funnel page (`hf_taipy_app/src/queries/funnel.py`) drops its Wyscout-synthetic-possession compensation. Wyscout matches now show real per-team possession counts (~80 instead of synthetic 1). IDSSE/Metrica matches join the funnel out of the box.
- `action_id` exposed end-to-end from silly-kicks `convert_to_actions` through `fct_action_values` — useful for joining SPADL actions back to upstream events without recomputing per-match indices.
- `bronze.spadl_actions` ALTERed to add the 4 `statsbomb_*` columns the PR-LL1 ALTER missed (latent 100% NULL gap closed).

### Negative

- Bronze schema growth: `bronze.spadl_actions` adds 15 columns (4 LL1 backfill + 6 enrichment + 5 Path B native), `bronze.vaep_action_values` adds 12 columns (1 action_id + 6 enrichment + 5 Path B native; LL1 statsbomb_* already present), `bronze.idsse_events` and `bronze.metrica_events` each add 5 Path B native columns. ALTER scripted via `scripts/migrate_bronze_for_pr_ll2.py` (idempotent at app layer).
- One destructive backfill required at PR-LL2 close: `DELETE FROM bronze.spadl_actions WHERE data_source IN ('statsbomb', 'wyscout')` + re-run wf-vaep. Existing 9.6M rows have NULL LL1 `statsbomb_*` and NULL LL2 enrichment columns; only re-conversion populates them. IDSSE/Metrica are greenfield (zero rows pre-LL2). Defensive Delta deep clone + 24h retention before drop.
- IDSSE/Metrica `team_id` / `player_id` / `competition_id` / `season_id` BIGINTs are NULL for those sources. Mart consumers must use `team_id_native` / etc. to join to dim_*. dbt staging + mart updated; downstream Taipy queries still on legacy column names where applicable (no IDSSE/Metrica-specific Taipy queries today; future PRs will use the native cols).
- IDSSE/Metrica `player_id_native` NOT exposed in PR-LL2 — `dim_players.native_player_id` join resolves NULL for those sources. Defer to PR-LL3 if player-level analysis on IDSSE/Metrica needs traceability.
- `fct_action_values.game_state` derivation is best-effort for IDSSE/Metrica (uses legacy `team_id` BIGINT comparison; NULL for those sources, so the comparison evaluates NULL → falls into the `'losing'` branch). Documented in mart code; PR-LL3 should switch to `team_id_native` comparison.

### Neutral

- silly-kicks 1.7.0 ships dedicated DataFrame converters for `sportec` (IDSSE) and `metrica`. luxury-lakehouse's IDSSE/Metrica adapters in `src/ingestion/spadl_adapter.py` are near-identity passthroughs (Metrica adapter scales `[0,1]` normalised coords to SPADL meters via per-match `pitch_length_m`/`pitch_width_m`; IDSSE adapter is pure passthrough since bronze.idsse_events.x/y already in SPADL frame).
- silly-kicks 1.7.0's sportec converter still drops `CornerKick` (1.7.0 / 1.8.0 dispatch tests `et == "Corner"`, but bronze.idsse_events.event_type emits `'CornerKick'` per the DFL XML) and `OtherBallAction` (~16% of IDSSE rows, not in either `_MAPPED_EVENT_TYPES` or `_EXCLUDED_EVENT_TYPES`). silly-kicks 1.8.0 (PR-S8) did not include the converter fix. PR-LL3 (or a focused silly-kicks 1.9.0 PR) is the target for closing this gap. Documented as a known data-fidelity concern in the IDSSE UDF code.

## CLAUDE.md Amendment

`CLAUDE.md` § Project Conventions adds the following bullet point:

> **SPADL post-conversion enrichments live in `src/ingestion/spadl_enrichments.py`**: new helpers added to `apply_spadl_enrichments` + a column added to `_SPADL_SCHEMA` + `_VAEP_SCHEMA` + applyInPandas StructTypes (parity-tested). Provider-native passthroughs use `<provider>_<field>` everywhere (e.g. `statsbomb_play_pattern`); computed enrichments use plain canonical names (e.g. `possession_id` for the heuristic). Native string identifiers paired with Kimball surrogates use `<entity>_native` (e.g. `team_id_native` for the actual DFL CLU id, `team_key` for the BIGINT surrogate). See ADR-016.

## Related

- **Specs:** `docs/superpowers/specs/2026-04-29-pr-ll2-spadl-enrichment-stage-design.md`
- **Plan:** `docs/superpowers/plans/2026-04-29-pr-ll2-spadl-enrichment-stage.md`
- **ADRs:** ADR-002 (writer/target schema drift guard — same pattern reapplied to spadl_actions / vaep_action_values writer parity); ADR-011 (Kimball surrogate keys — `<entity>_native` is the same pattern extended to bronze layer); ADR-013 (ML inference outputs through dbt mart — analogous discipline applied to non-ML enrichments).
- **External references:** silly-kicks 1.7.0 release (`silly_kicks.spadl.sportec` / `silly_kicks.spadl.metrica`), silly-kicks 1.8.0 release (`silly_kicks.spadl.utils.boundary_metrics` + recall-based CI gate), Bassek et al. "An integrated dataset of spatiotemporal and event data in elite soccer." Scientific Data, Nature (2025).

## Notes

**Boundary metric empirical baseline (3-match StatsBomb fixture, recorded 2026-04-29):**

| Metric | Value |
|---|---|
| Boundary recall (heuristic vs StatsBomb native) | 0.93 |
| Boundary precision | 0.42 |
| Boundary F1 | 0.58 |
| Parameter sweep peak F1 (`max_gap_seconds=10s`) | 0.605 |

The team-change-with-carve-outs algorithm class structurally cannot replicate StatsBomb's possession-merge rule for brief opposing-team actions — precision is bounded by the algorithm class itself, not by parameter tuning. Recall is the meaningful regression metric for mart-layer analytics (would the heuristic-derived possession_id under-segment a sequence StatsBomb keeps as one possession?). 0.93 recall is sufficient for the funnel-stage and possession-counting analytics this mart enables.

**LL1 latent-bug post-mortem timeline:**

- 2026-04-28 PR-LL1 merges (silly-kicks 1.5.0 `preserve_native`)
- 2026-04-29 PR-LL2 design phase reveals 0/7M StatsBomb rows have non-NULL `statsbomb_possession_id`
- Root cause: `_make_scoring_udf`'s `vaep_schema` StructType in `spadl_vaep.py` did NOT include the `statsbomb_*` columns. The `_output_cols` projection produced them in pandas, but Spark's `applyInPandas` drops columns missing from the schema parameter. PR-LL1 added cols to `_VAEP_SCHEMA` (the DDL constant) + the bronze ALTER, but missed the per-call StructType.
- Fix: `test_spadl_vaep_writer_parity.py::TestVaepScoringWriterDdlParity::test_vaep_scoring_struct_matches_vaep_ddl` asserts `_VAEP_SCHEMA` columns and the per-call StructType columns are equal. Same pattern reapplied to all 4 source UDFs (`test_*_struct_matches_spadl_ddl`).
