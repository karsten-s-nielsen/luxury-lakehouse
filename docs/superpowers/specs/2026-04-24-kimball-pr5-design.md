# Kimball PR 5 — Conformed team + player dimensions, entity resolution, Metrica pseudo-competition — design spec

| | |
|---|---|
| **Date** | 2026-04-24 |
| **Branches** | `kimball-pr5a-foundation`, `kimball-pr5b-player-marts` (two sub-PRs; 5b cut from main after 5a merges) |
| **Author** | Karsten Skyt Nielsen (with Claude Opus 4.7) |
| **Status** | Draft — awaiting user review |
| **Supersedes** | — |
| **Related** | ADR-011 (Kimball surrogate keys — extended here from matches/competitions to teams/players); ADR-005 (Lakebase synced-table grants); ADR-002 (silent-exception policy); [2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md](2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md) (PR 4 reference — reuses the dual-column pattern and live-CI playbook); [2026-04-22-kimball-pr3-shots-xg-design.md](2026-04-22-kimball-pr3-shots-xg-design.md) (PR 3 — the try_cast pushdown pattern); `docs/plans/2026-03-06-player-entity-resolution.md` (dormant plan under whose framework entity resolution is activated here) |

## 1. Goal

Complete PR 5 of the ADR-011 staged Kimball migration. Two sub-PRs:

- **PR 5a — Foundation + minimum-viable fact migrations.** Kimball-conform `dim_teams` and `dim_players` with `(provider, native_team_id)` / `(provider, native_player_id)` BIGINT surrogates via new `generate_team_key` + `generate_player_key` macros. Activate cross-provider entity resolution (SB↔WS↔IDSSE) with a new `scripts/generate_entity_xref.py` generator; extend `int_player_xref` to carry provider labels; add `int_team_xref`. Add a Metrica pseudo-competition row to `dim_competitions` so Metrica's 2,884 passes surface in Pass Map. Surface real DFL `team_id` from `bronze.idsse_tracking` in IDSSE staging. Parse `stg_wyscout__matches.teams_data_parsed` via a new `stg_wyscout__home_away_teams` bridge so `fct_match_summary` populates Wyscout home/away team_ids (currently NULL). Synthesise Metrica team + player identities per-match with `is_synthesized` + `synthesis_reason` attributes. Migrate `fct_player_stats` (add `player_key` + `team_key`) and `fct_funnel_stages_agg` (add `team_key` + `opponent_team_key` + `match_key`), closing the two `severity: warn` suppressions that PR 4b left open.
- **PR 5b — Player-side embedding + percentile mart migrations.** Add `player_key` to `fct_player_embeddings` + `_season` + `_career` + `_season_360` + `_career_360` + `fct_player_percentiles`. Consumer-side: Taipy query modules read `player_key` preferentially with `canonical_player_id` fallback during the dual-column window.

Every legacy column is retained through the coordinated 2026-07-22 sunset policy established in PR 2/3/4; PR 8 drops them together.

The remainder of the scope-C migration (team_key/player_key additions on fct_passes, fct_match_summary, fct_shots, fct_action_values, fct_defcon_*, fct_goalkeeper_*, fct_tracking_*, fct_formation_*, fct_off_ball_xt, fct_pausa_*, fct_space_creation, fct_heatmap_agg, fct_vaep_breakdown_agg, fct_discipline_events, fct_pass_timing, fct_physical_stats, fct_player_positions, fct_position_maps) lands in PR 6-8 alongside those PRs' original ADR-011 scope — that is, each subsequent PR adds team/player surrogates to the facts it already touches for algorithmic reasons.

## 2. Scope

### In scope — PR 5a (session 1 deliverable)

**New dbt macros**

- `dbt_project/macros/generate_team_key.sql` — `xxhash64(concat_ws('|', provider, cast(native_team_id as string)))`, returns BIGINT, NULL input → NULL output. Mirrors `generate_competition_key`.
- `dbt_project/macros/generate_player_key.sql` — identical shape, `(provider, native_player_id)`.

**Bronze schema changes**

- `bronze.metrica_tracking` — add column `is_anonymized BOOLEAN NOT NULL`. `src/ingestion/metrica.py` sets `true` for all 3 sample-CSV matches; future subscription-path ingestion sets `false`. Column default + migration: a one-time `ALTER TABLE bronze.metrica_tracking ADD COLUMN is_anonymized BOOLEAN`, then `UPDATE ... SET is_anonymized = true` for existing rows; committed as a small migration script `scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql` with a corresponding test in `test_bronze_live_schema.py`.
- `bronze.tracking_player_metadata` — add column `is_anonymized BOOLEAN` (nullable — Metrica is not currently a source here, but the column arrives for forward-compat; IDSSE + SkillCorner rows get `false`).
- `bronze.player_xref_raw` — add columns `source_a STRING NOT NULL`, `source_b STRING NOT NULL`. One-time backfill commit: `UPDATE bronze.player_xref_raw SET source_a = 'statsbomb', source_b = 'wyscout'` for the existing 2,780 rows (confirmed live). The update is additive + idempotent.
- `bronze.team_xref_raw` — new bronze table. Schema: `source_a STRING, team_id_a STRING, source_b STRING, team_id_b STRING, confidence DOUBLE, match_layer INT, resolution_type STRING, _ingested_at TIMESTAMP`. Mirrors `player_xref_raw` shape. Populated by the new xref generator script.

**Bronze source YAML updates**

- `dbt_project/models/staging/idsse/_idsse__sources.yml` — add the 14 columns currently present in `bronze.idsse_tracking` but absent from the YAML (confirmed via live DESCRIBE): `team_id STRING`, `t STRING`, `s DOUBLE`, `a DOUBLE`, `d DOUBLE`, `m BOOLEAN`, `ball_z DOUBLE`, `ball_s DOUBLE`, `ball_a DOUBLE`, `ball_d DOUBLE`, `ball_m BOOLEAN`, `ball_t STRING`, `ball_possession STRING`, `ball_status STRING`. (Only `team_id` is load-bearing for this PR; the others close a separate bronze-completeness audit gap inherited from PR 1.8 that surfaced during Section 1 fact-check.)
- `dbt_project/models/staging/metrica/_metrica__sources.yml` — add `is_anonymized BOOLEAN` to `bronze.metrica_tracking`.
- `dbt_project/models/staging/tracking/_tracking__sources.yml` — add `is_anonymized BOOLEAN` to `bronze.tracking_player_metadata`.
- `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml` — add `source_a STRING`, `source_b STRING` to `bronze.player_xref_raw`; add the new `bronze.team_xref_raw` table definition.

**New and modified staging models**

- `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` — passthrough `team_id` unchanged.
- `dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql` **(new)** — `SELECT DISTINCT match_id, team AS side, team_id FROM {{ ref('stg_idsse__tracking') }} WHERE team IN ('home', 'away') AND team_id IS NOT NULL`. One row per `(match_id, side)` carrying the real DFL `DFL-CLU-XXXXXX` TeamId.
- `dbt_project/models/staging/idsse/stg_idsse__passes.sql` — LEFT JOIN `stg_idsse__home_away_teams` on `(match_id, team)` so pass rows carry a real `team_id` in place of today's NULL.
- `dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql` **(new)** — explodes `stg_wyscout__matches.teams_data_parsed` MAP using a `LATERAL VIEW explode(...)` against the map key (team_id) and value (struct). Primary path: `side = case v.side when 'home' then 'home' else 'away' end`. Fallback path: when the parsed map is NULL (parse failure) or empty, emit two synthesised rows `(match_id, 'home', concat('wyscout_unresolved_', match_id, '_home'))` and `(match_id, 'away', concat('wyscout_unresolved_', match_id, '_away'))` with a `is_synthesized = true` flag column so downstream dim_teams can mark them. A `synthesis_reason = 'wyscout_unresolved_teamsdata'` literal attaches.
- `dbt_project/models/staging/metrica/stg_metrica__matches.sql` — add hardcoded `competition_id = 'metrica-sample'` column.
- `dbt_project/models/staging/metrica/stg_metrica__team_players.sql` **(new)** — for each Metrica match, union `(match_id, side='home', native_team_id = concat('metrica_', match_id, '_home'), player_key_in_map, native_player_id = concat('metrica_', match_id, '_home_', player_key_in_map))` from `from_json(home_players, 'MAP<STRING, STRUCT<...>>')` + the symmetric away pattern. Carries the `is_anonymized` flag from bronze; `is_synthesized = true`, `synthesis_reason = 'metrica_anonymized'` when `is_anonymized = true`; real-identity branch for future subscription data.
- `dbt_project/models/staging/wyscout/stg_wyscout__teams.sql` **(new)** — cleans + types Wyscout team roster from the newly-ingested `bronze.wyscout_teams`. Columns: `team_id INT` (from `wyId`), `official_name STRING`, `team_name STRING` (from `name`), `city STRING`, `area_name STRING`, `area_alpha3 STRING`, `team_type STRING`. ~30 LOC mirroring the `stg_wyscout__players.sql` shape.

**Wyscout teams.json ingestion (closes pre-existing data-coverage gap)**

The original 2026-03-12 Wyscout ingestion pulled events, matches, and players from Figshare but not teams.json. `dim_teams.team_name` has been NULL for all Wyscout rows since day one. Closed in PR 5a by extending the existing Figshare-download pattern in `src/ingestion/wyscout.py`.

- `src/ingestion/wyscout.py` — add `_TEAMS_URL = "https://ndownloader.figshare.com/files/<id>"` constant (the `<id>` resolved at implementation time via a Figshare collection-page lookup — `https://figshare.com/collections/Soccer_match_event_dataset/4415000`) + new `ingest_teams(spark, catalog, schema)` function mirroring the existing `ingest_players()` shape (download → `pd.read_json` → column serialize → `finalize_bronze_df` → `write_delta_table` to `bronze.wyscout_teams`). Entry point's `main()` dispatches to `ingest_teams` alongside the existing three tasks.
- `src/tests/fixtures/wyscout_bronze_schema_snapshot.json` — add `wyscout_teams` entry (expected columns: `wyId INT`, `officialName STRING`, `name STRING`, `city STRING`, `area STRING` (JSON string, per the existing Wyscout nested-struct pattern), `type STRING`, `_ingested_at TIMESTAMP`). Expected count: ~280 teams across the 7 covered competitions.
- `dbt_project/models/staging/wyscout/_wyscout__sources.yml` — add `wyscout_teams` table with full column documentation.
- `dbt_project/models/staging/wyscout/_wyscout__models.yml` — add `stg_wyscout__teams` entry.
- `src/tests/test_wyscout_bronze_coverage.py` — add `wyscout_teams` entry (asserts every bronze column surfaces in staging per PR 1.5 pattern).
- `src/tests/test_bronze_live_schema.py` — add live-DESCRIBE assertion for `soccer_analytics.bronze.wyscout_teams` per G1 drop-safety (PR #173).
- `workflow-cards/wf-wyscout.yaml` — add `wyscout_teams` to the ingestion-task output list (TF block ordering + output enumeration per workflow-card conventions).
- **Initial ingestion trigger** — one-shot invocation of the `ingest_wyscout` Databricks Job after the code lands, to populate `bronze.wyscout_teams` for the first time. Idempotent. Post-run verification: `SELECT count(*) FROM bronze.wyscout_teams` ≈ 280 (exact count depends on Figshare payload).
- `dbt_project/models/marts/dim_teams.sql` — Wyscout CTE gains LEFT JOIN to `stg_wyscout__teams` on `team_id` so `team_name` populates for Wyscout real rows (previously NULL).

**Entity resolution activation**

- `dbt_project/dbt_project.yml` — flip `vars.entity_resolution_enabled` default from `false` to `true`.
- `dbt_project/models/intermediate/int_player_xref.sql` — remove the gated-off branch; honour `source_a`, `source_b` columns for cross-provider pair resolution beyond just SB↔WS. Grain: `(source_a, player_id_a, source_b, player_id_b)`. Materialisation changes from `ephemeral` to `view` so `test_int_player_xref_invariants.py` can query it as a table without materialising extra state.
- `dbt_project/models/intermediate/int_team_xref.sql` **(new)** — identical shape to `int_player_xref` but against `bronze.team_xref_raw` + a new `team_xref_overrides` seed (initially empty; created for future manual-override support). Materialised as `view`.
- `dbt_project/seeds/player_xref_overrides.csv` — existing seed, schema-extended to carry `source_a`, `source_b` columns; existing rows backfilled with `('statsbomb', 'wyscout')`.
- `dbt_project/seeds/team_xref_overrides.csv` **(new)** — header-only on first commit; columns `(source_a, team_id_a, source_b, team_id_b, action)` where `action ∈ {'force_match', 'force_reject'}`.

**Entity xref generator**

- `scripts/generate_entity_xref.py` **(new)** — PEP 723 script using `rapidfuzz` for fuzzy name matching. Reads:
  - StatsBomb player roster from `dev_gold.stg_statsbomb__lineups` (player_name, player_id, team_name)
  - Wyscout player roster from `dev_gold.stg_wyscout__players` (player_name, wyId, currentTeamId, nationality, birth_date)
  - IDSSE player roster from `dev_gold.stg_tracking__player_metadata` WHERE provider='idsse' (player_display_name, player_id, team_display_name)
  - StatsBomb teams from `dev_gold.stg_statsbomb__events` DISTINCT (team_name, team_id)
  - Wyscout teams from `dev_gold.stg_wyscout__teams` (official_name, team_name, team_id, area_name) — sourced from the teams.json ingestion folded into PR 5a.
  - IDSSE teams from `dev_gold.stg_tracking__player_metadata` DISTINCT (team_id, team_display_name).
  
  Outputs:
  - New rows for `bronze.player_xref_raw` covering SB↔IDSSE and WS↔IDSSE pairs at confidence ≥ 70.
  - New rows for `bronze.team_xref_raw` covering SB↔IDSSE + WS↔IDSSE + SB↔WS pairs at confidence ≥ 70.
  
  Idempotency: the script re-computes pairs each run; writes via `MERGE INTO ... ON (source_a, team_id_a, source_b, team_id_b)` (or equivalent for player_xref_raw). Re-running in place updates confidences without duplicating pairs.
  
  Matching logic:
  - Player names: `rapidfuzz.process.extractOne(name_a, names_b_list, scorer=rapidfuzz.fuzz.token_sort_ratio)` with secondary disambiguation via nationality (if both sides carry it) + birth_date window (±1 year).
  - Team names: `token_sort_ratio` on display names, no secondary disambiguation.
  - Only pairs with confidence ≥ 70 are written.

**Updated dim models**

- `dbt_project/models/marts/dim_competitions.sql` — new Metrica CTE:
  ```sql
  metrica_competitions as (
      select distinct
          'metrica'              as provider,
          'metrica-sample'       as native_competition_id,
          cast(null as int)      as competition_id_legacy,
          'Metrica Sample Dataset' as competition_name
      from {{ ref('stg_metrica__matches') }}
      where native_match_id is not null
  )
  ```
  Unioned into `all_competitions`. `seed_metadata` join gets a fallback for Metrica so `country` + `gender` surface as NULL.
- `dbt_project/models/marts/dim_teams.sql` — complete rewrite to ADR-011 pattern. Structure:
  ```sql
  with statsbomb_teams as (
      select 'statsbomb' as provider, cast(team_id as string) as native_team_id,
             team_id as team_id_legacy, team_name, false as is_synthesized,
             cast(null as string) as synthesis_reason
      from {{ ref('stg_statsbomb__events') }} where team_id is not null
      group by 1, 2, 3, 4
  ),
  wyscout_teams as (
      -- Real-identity path from events + names (where available) + home/away bridge
      select 'wyscout' as provider, cast(team_id as string) as native_team_id,
             team_id as team_id_legacy, cast(null as string) as team_name,
             false as is_synthesized, cast(null as string) as synthesis_reason
      from {{ ref('stg_wyscout__events') }} where team_id is not null
      group by 1, 2, 3
      union all
      -- Synthesised fallback rows from the home/away bridge
      select 'wyscout' as provider, hat.native_team_id,
             cast(null as int) as team_id_legacy, cast(null as string) as team_name,
             hat.is_synthesized, hat.synthesis_reason
      from {{ ref('stg_wyscout__home_away_teams') }} hat
      where hat.is_synthesized = true
  ),
  idsse_teams as (
      select 'idsse' as provider, team_id as native_team_id,
             cast(null as int) as team_id_legacy,
             max(team_display_name) as team_name,
             false as is_synthesized, cast(null as string) as synthesis_reason
      from {{ ref('stg_idsse__home_away_teams') }} hat
      left join {{ ref('stg_tracking__player_metadata') }} pm
          on pm.provider = 'idsse' and pm.match_id = hat.match_id and pm.team_side = hat.side
      where team_id is not null
      group by 1, 2, 3
  ),
  metrica_teams as (
      -- Anonymised-sample path (current data)
      select 'metrica' as provider, native_team_id,
             cast(null as int) as team_id_legacy,
             concat('Metrica ', match_id, ' ', initcap(side)) as team_name,
             true as is_synthesized,
             'metrica_anonymized' as synthesis_reason
      from {{ ref('stg_metrica__team_players') }}
      where is_anonymized = true
      group by 1, 2, 3, 4
      union all
      -- Real-identity path for future subscription data (zero rows today;
      -- ships live when Metrica subscription ingestion is added per §4)
      select 'metrica' as provider, native_team_id,
             cast(null as int) as team_id_legacy,
             max(team_display_name) as team_name,
             false as is_synthesized,
             cast(null as string) as synthesis_reason
      from {{ ref('stg_metrica__team_players') }}
      where is_anonymized = false
      group by 1, 2, 3
  )
  ```
  Final CTE computes `team_key = generate_team_key(provider, native_team_id)`. LEFT JOIN `int_team_xref` resolves `canonical_team_key` (self-pointer when no match).
  
  Columns (in final select order): `team_key BIGINT`, `provider STRING`, `native_team_id STRING`, `team_id INT` (legacy, SB/WS real rows only, NULL otherwise), `team_name STRING`, `canonical_team_key BIGINT`, `is_synthesized BOOLEAN`, `is_anonymized BOOLEAN`, `synthesis_reason STRING`, `team_data_source STRING` (renamed from `data_source`).
- `dbt_project/models/marts/dim_players.sql` — complete rewrite. Activates Wyscout + IDSSE + Metrica paths. Structure mirrors `dim_teams`. Columns: `player_key BIGINT`, `canonical_player_id STRING` (legacy hash via existing `dbt_utils.generate_surrogate_key` call, preserved for Hyrum's Law compat per the 57-file downstream cascade), `canonical_player_key BIGINT` (xref-resolved or self), `provider STRING`, `native_player_id STRING`, `player_id INT` (legacy, SB/WS real rows only), `player_name STRING`, `player_display_name STRING`, `primary_position STRING`, `position_group STRING`, `statsbomb_player_id INT` (xref cross-ref, nullable), `wyscout_player_id INT` (xref cross-ref, nullable), `idsse_player_id STRING` (xref cross-ref, nullable), `match_confidence DOUBLE`, `match_layer INT`, `birth_date STRING`, `nationality STRING`, `is_synthesized BOOLEAN`, `is_anonymized BOOLEAN`, `synthesis_reason STRING`, `data_sources STRING`.
  
  Header comments document the Metrica-siloed-by-design choice + the forward-compat `is_anonymized` flag semantics.

**dim contract + test updates**

- `dbt_project/models/marts/_marts__models.yml` — update `dim_teams` + `dim_players` + `dim_competitions` column lists; keep `contract: enforced: true`; add `data_tests` entries: `unique(team_key)`, `not_null(team_key)`, `dbt_utils.unique_combination_of_columns([provider, native_team_id])` on dim_teams; same shape on dim_players (`player_key`) and dim_competitions (`competition_key`, already present from PR 2).

**Intermediate + xref tests**

- `src/tests/test_int_player_xref_invariants.py` **(new)** — live-warehouse tests: confidence range 70-100; no self-loops (`source_a = source_b AND player_id_a = player_id_b`); no provider mismatch (row asserts both providers appear in `{'statsbomb', 'wyscout', 'idsse', 'metrica'}`); injectivity per `(source_a, source_b)` pair (each player on one side maps to at most one player on the other side at confidence ≥ 70 after override resolution).
- `src/tests/test_int_team_xref_invariants.py` **(new)** — same shape for teams.
- dbt schema tests in `_intermediate__models.yml`: `unique(int_player_xref, [source_a, player_id_a, source_b, player_id_b])`; `unique(int_team_xref, [source_a, team_id_a, source_b, team_id_b])`.

**Fact migrations (PR 5a subset)**

- `dbt_project/models/marts/fct_match_summary.sql` — Wyscout branch gains a LEFT JOIN to `stg_wyscout__home_away_teams` so `home_team_id` + `away_team_id` populate for the ~36% of rows currently NULL. Fallback path (synthesised Wyscout unresolved team_ids from the bridge's fallback branch) populates with the synth IDs — downstream fct_funnel_stages_agg.opponent_team_id derivation still works. Contract update: legacy `int` types retained; no new columns this PR.
- `dbt_project/models/marts/fct_player_stats.sql` — add `player_key BIGINT NOT NULL` via INNER JOIN to `dim_players` on `(dp.provider = agg.data_source AND dp.native_player_id = cast(agg.player_id as string))`. Add `team_key BIGINT` (nullable — some aggregate rows don't resolve cleanly to a single team; left as nullable for this PR, warn-severity relationship test). The 1 NULL `player_id` StatsBomb outlier (confirmed live, match 3825894) drops via INNER JOIN. Contract change in `_marts__models.yml`: new columns added; `not_null_fct_player_stats_player_id` flipped from `severity: warn` to default (error).
- `dbt_project/models/marts/fct_funnel_stages_agg.sql` — add `team_key BIGINT NOT NULL`, `opponent_team_key BIGINT NOT NULL`, `match_key BIGINT NOT NULL`. Switch the `fct_match_summary` JOIN from the current `using (match_key)` (already there post PR 2) — no change there. `team_key` resolution via `JOIN dim_teams dt_own ON (dt_own.provider = 'statsbomb' OR dt_own.provider = 'wyscout') AND dt_own.native_team_id = cast(g.team_id as string)` — two providers since funnel is SB+WS only. `opponent_team_key` resolution via same pattern on `opponent_team_id`. Contract update: new columns; `not_null_fct_funnel_stages_agg_opponent_team_id` flipped from `severity: warn` to default (error); `relationships` restored to `fct_match_summary(match_key)`.

**Synced-table refresh (PR 5a deploy)**

- No `maintain_synced_tables.py` recreate needed; all schema changes are additive. `refresh_synced_tables.py` auto-evolution handles new columns per `reference_lakebase_synced_table_auto_evolution`. Tables refreshed: `dim_teams_synced`, `dim_players_synced`, `dim_competitions_synced`, `fct_player_stats_synced`, `fct_funnel_stages_agg_synced`, `fct_match_summary_synced`. Grants via `maintain_synced_tables.py` Step 0.5.

**Ingestion-layer contract doc (forward-compat)**

- `src/ingestion/metrica.py` docstring + header comment — document the `is_anonymized` field contract: "Sample-CSV ingestion path sets `is_anonymized=true`; subscription-API ingestion path sets `is_anonymized=false`. Downstream `dim_teams` + `dim_players` branch on this flag — anonymised rows are synthesised per-match; real rows participate in entity resolution. Do not remove this flag."
- `dbt_project/models/marts/dim_teams.sql` + `dim_players.sql` headers — document the synthesis rules + the Metrica-siloed-by-design choice.

### In scope — PR 5b (session 2 deliverable)

**Embedding + percentile mart migrations**

- `dbt_project/models/marts/fct_player_embeddings.sql` — add `player_key BIGINT` column; resolved via LEFT JOIN to `dim_players` on `(dp.provider = stg.data_source AND dp.native_player_id = cast(stg.player_id as string))`. Legacy `canonical_player_id` preserved (Hyrum's Law). `embedding_id` surrogate unchanged to avoid the existing `DELTA_MULTIPLE_SOURCE_ROW_MATCHING` guard referenced in the model header comment.
- `dbt_project/models/marts/fct_player_embeddings_season.sql` — same pattern.
- `dbt_project/models/marts/fct_player_embeddings_career.sql` — same pattern.
- `dbt_project/models/marts/fct_player_embeddings_season_360.sql` — same pattern.
- `dbt_project/models/marts/fct_player_embeddings_career_360.sql` — same pattern.
- `dbt_project/models/marts/fct_player_percentiles.sql` — add `player_key BIGINT` column.
- `dbt_project/models/marts/_marts__models.yml` — contract updates for all six marts; relationships `fct_player_* → dim_players(player_key)` added at `severity: warn` for the 90-day dual-column window (consumers can still read on `canonical_player_id` during this period).

**Taipy consumer updates (non-breaking, dual-read)**

- `hf_taipy_app/src/queries/players.py` — any SQL filter on `canonical_player_id` gains a parallel `player_key` filter where the app already knows both forms; write a small `resolve_player_identity(...)` helper in `hf_taipy_app/src/state/shared.py` that returns both keys for a selected player (reads both from `dim_players_synced`). Preserves UI behaviour.
- `hf_taipy_app/src/queries/tracking.py` — same pattern.
- `hf_taipy_app/src/state/player_similarity.py` — dual-read in the similarity query.
- No PageConfig changes; UX unchanged.

**Scripts left alone**

- `scripts/train_football2vec_*`, `src/ingestion/export_scoutgpt_training_data.py`, `src/ingestion/export_embeddings_training_data.py`, `src/ingestion/player_embeddings_v2.py`, `src/ingestion/player_embeddings_common.py` — untouched. They continue reading `canonical_player_id`. Migrated in PR 8's coordinated cleanup.

**HF dataset card documentation (no payload changes)**

- `docs/huggingface/dataset-cards/football2vec-player-embeddings.md` + `football2vec-360-embeddings.md` + `football2vec-training-data.md` + `football2vec-360-training-data.md` + `football2vec-statsbomb-wyscout.md` — add a "Dual-column window" section noting that `canonical_player_id` (legacy) and `player_key` (current) both ship on the dataset until 2026-07-22 when `canonical_player_id` is dropped. Pushed via `scripts/publish_hf_cards.py --kind dataset --name <card>.md` per ADR-014.
- No changes to the HF dataset *payloads* — PR 8 handles the payload column additions + the 2026-07-22 drops coordinated with the sunset blocks.

**Synced-table refresh (PR 5b deploy)**

- Additive columns on six marts. `refresh_synced_tables.py` on `fct_player_embeddings_synced`, `fct_player_embeddings_season_synced`, `fct_player_embeddings_career_synced`, `fct_player_embeddings_season_360_synced`, `fct_player_embeddings_career_360_synced`, `fct_player_percentiles_synced`. Grants via Step 0.5.

### Explicitly out of scope — both sub-PRs

- **fct_passes, fct_line_breaking_results, fct_match_summary team_key additions.** These carry `match_key` from PR 2 but not yet `team_key`/`opponent_team_key`. Added in PR 6 alongside defensive-mart scope. Choice documented in §1; PR 5a touches `fct_match_summary` only to populate its existing NULL Wyscout `home_team_id`/`away_team_id` columns.
- **fct_shots, fct_xg_predictions*, fct_action_values team_key/player_key additions.** Carry `match_key`/`competition_key` from PR 3/4b. Added in PR 6/7 alongside their algorithmic scope.
- **All defensive, goalkeeper, pitch-control, tracking, formation, pausa, off-ball-xT, space-creation, heatmap, physical-stats, VAEP-breakdown, discipline-events, pass-timing, player-position, position-map, tracking-frame mart team_key/player_key additions.** PR 6/7 alongside their original ADR-011 scope.
- **Legacy column drops** (`match_id`, `competition_id`, `team_id`, `player_id`, `canonical_player_id`). PR 8 coordinated 2026-07-22 sunset.
- **HF dataset payload column additions or drops.** PR 5b updates card *documentation* only. PR 8 touches payloads coordinated with the 2026-07-22 sunset blocks already published on HF Hub.
- **Training + export scripts migration to `player_key`.** Untouched in PR 5b. PR 8 cleanup batches all script-side migrations.
- **Metrica player cross-provider entity resolution.** Data constraint (anonymised source) → siloed by design, documented in dim_players header. Not a deferral.
- **Metrica team cross-provider entity resolution.** Same. Documented.
- **Wyscout↔IDSSE team xref becomes eligible post-teams.json ingestion.** With the Wyscout teams.json work folded into PR 5a, `stg_wyscout__teams.team_name` is now populated; `generate_entity_xref.py` includes WS↔IDSSE team pairs in the match-generation pass alongside SB↔IDSSE. No deferral.
- **IDSSE xref override seed.** `player_xref_overrides.csv` + `team_xref_overrides.csv` both ship with header-only content initially. User manually populates as false-positives/negatives surface post-merge.

## 3. Data model — dim, fact, and xref schemas

### 3.1 `dim_teams` after PR 5a

Grain: one row per `(provider, native_team_id)`.

| Column | Type | Nullable | Source |
|---|---|---|---|
| `team_key` | BIGINT | N | `generate_team_key(provider, native_team_id)` |
| `provider` | STRING | N | Literal per provider CTE |
| `native_team_id` | STRING | N | Real team_id for SB/WS real rows + IDSSE DFL TeamId; synthesised for Wyscout unresolved + Metrica |
| `team_id` | INT | Y | Legacy native integer — SB + WS real rows only; NULL for IDSSE/Metrica and Wyscout synthesised |
| `team_name` | STRING | Y | Real name for SB + IDSSE + WS real rows (populated via `stg_wyscout__teams` enrichment landed in this PR); synthesised label for Metrica; NULL only for Wyscout synthesised-fallback rows (no real team exists) |
| `canonical_team_key` | BIGINT | N | xref-resolved canonical pointer; self when no xref match |
| `is_synthesized` | BOOLEAN | N | `true` for Metrica anonymised + Wyscout unresolved fallback |
| `is_anonymized` | BOOLEAN | Y | Forward-compat; `true` when Metrica sample data, `false` when future Metrica subscription, NULL otherwise |
| `synthesis_reason` | STRING | Y | `'metrica_anonymized'`, `'wyscout_unresolved_teamsdata'`, `NULL` |
| `team_data_source` | STRING | N | Provider label (redundant with `provider` but kept for backward compat with existing consumers) |

Cardinality estimate: SB + WS real (~453) + IDSSE real (~14 DFL clubs distinct across 7 matches) + Metrica synthesised (6) + Wyscout synthesised (unknown, measured at implementation time) ≈ **473 + Wyscout-fallback count**.

### 3.2 `dim_players` after PR 5a

Grain: one row per `(provider, native_player_id)`.

| Column | Type | Nullable | Source |
|---|---|---|---|
| `player_key` | BIGINT | N | `generate_player_key(provider, native_player_id)` |
| `canonical_player_id` | STRING | N | Legacy hash preserved for Hyrum's Law — 57 downstream files reference it |
| `canonical_player_key` | BIGINT | N | xref-resolved BIGINT canonical pointer (prefer StatsBomb > Wyscout > IDSSE; self when no match) |
| `provider` | STRING | N | Literal per CTE |
| `native_player_id` | STRING | N | Real ID for SB/WS/IDSSE; synthesised for Metrica anonymised |
| `player_id` | INT | Y | Legacy native integer — SB + WS only |
| `player_name` | STRING | Y | Real for SB/WS/IDSSE; NULL for Metrica anonymised |
| `player_display_name` | STRING | Y | Nickname/display variant |
| `primary_position` | STRING | Y | From stg |
| `position_group` | STRING | Y | From `position_mapping` seed |
| `statsbomb_player_id` | INT | Y | xref cross-ref |
| `wyscout_player_id` | INT | Y | xref cross-ref |
| `idsse_player_id` | STRING | Y | xref cross-ref |
| `match_confidence` | DOUBLE | Y | xref confidence (70-100) |
| `match_layer` | INT | Y | xref layer (0=manual override, 1+=automated confidence tiers) |
| `birth_date` | STRING | Y | From stg_wyscout__players where available |
| `nationality` | STRING | Y | From stg_wyscout__players where available |
| `is_synthesized` | BOOLEAN | N | `true` for Metrica anonymised |
| `is_anonymized` | BOOLEAN | Y | Forward-compat |
| `synthesis_reason` | STRING | Y | `'metrica_anonymized'` or NULL |
| `data_sources` | STRING | N | Comma-separated provider list for xref-collapsed rows (e.g., `'statsbomb,wyscout'`) |

Cardinality estimate: ~11,626 SB + ~3,603 WS (minus matched pairs) + ~210 IDSSE + ~66 Metrica synth ≈ **~15,000 rows**.

### 3.3 `int_player_xref` after PR 5a

Grain: one row per `(source_a, player_id_a, source_b, player_id_b)` pair.

| Column | Type | Source |
|---|---|---|
| `source_a` | STRING | `bronze.player_xref_raw` or seed |
| `player_id_a` | STRING | Native ID on side A (stringified from INT for SB/WS) |
| `source_b` | STRING | side B provider |
| `player_id_b` | STRING | Native ID on side B |
| `confidence` | DOUBLE | 70-100 |
| `match_layer` | INT | 0=manual override; ≥1 automated |
| `resolution_type` | STRING | `'automated'` or `'manual_override'` |

Ordering convention: `source_a < source_b` lexicographically, so each unordered pair appears exactly once. Enforced by the generator script + an invariant test.

### 3.4 `int_team_xref` after PR 5a

Same shape as `int_player_xref`, with `team_id_a`/`team_id_b` in place of player IDs.

### 3.5 `fct_player_stats` after PR 5a

New columns (additive):

| Column | Type | Nullable | Source |
|---|---|---|---|
| `player_key` | BIGINT | N | INNER JOIN dim_players → dropped 1 NULL player_id row |
| `team_key` | BIGINT | Y | LEFT JOIN dim_teams (nullable for aggregates that span teams) |

Contract flips: `not_null_fct_player_stats_player_id` → error severity.

### 3.6 `fct_funnel_stages_agg` after PR 5a

New columns (additive):

| Column | Type | Nullable | Source |
|---|---|---|---|
| `match_key` | BIGINT | N | JOIN dim_matches on native_match_id (already on base row pre-migration via the fct_match_summary JOIN) |
| `team_key` | BIGINT | N | JOIN dim_teams |
| `opponent_team_key` | BIGINT | N | JOIN dim_teams — resolves via the Wyscout home/away fix in fct_match_summary |

Contract flips: `not_null_fct_funnel_stages_agg_opponent_team_id` → error severity. `relationships` restored from `fct_match_summary(match_key)`.

## 4. Forward-compat — Metrica sample vs subscription data

Current state: 3 Metrica sample matches, all anonymised (no real team/player names, match_ids `Sample_Game_1/2/3`). `is_anonymized = true` in bronze.

Future state (if/when someone adds Metrica subscription-API ingestion):

| Dimension | Sample behaviour | Subscription behaviour |
|---|---|---|
| Bronze `is_anonymized` | `true` | `false` |
| `stg_metrica__team_players` | Per-match synthesised IDs | Real IDs from subscription payload |
| `dim_teams` / `dim_players` rows | `is_synthesized=true`, `synthesis_reason='metrica_anonymized'`, self-pointer `canonical_*_key` | `is_synthesized=false`, `synthesis_reason=NULL`, xref-eligible via `generate_entity_xref.py` next run |
| Cross-provider entity resolution | Unreachable (no real names) — documented data constraint | Available via existing xref infrastructure |
| `match_key` | Stable xxhash of `('metrica', 'Sample_Game_1')` etc. | New xxhash per real match UUID — no collision, coexists in dim_matches |
| Taipy UX | Displays tag synthetic entities distinctly (`is_synthesized` surfaces) | No tag — real identity shows through |

The sample-vs-subscription boundary is carried by one bronze column. No staging, dim, or fact schema changes are required when subscription data lands; the ingestion script just passes the correct flag value. The dim CTEs already branch on it.

## 5. Edge-case resolution (each without deferral)

| # | Edge case | Resolution in PR 5a |
|---|---|---|
| A | StatsBomb↔Wyscout team_id integer collision (both use small ints) | Provider grain in dim_teams + `xxhash64(provider|native_team_id)` separates them; fact JOINs qualify by provider. |
| B | Entity resolution quality (confidence<70 drops; false positives from manual-override gap) | Ship `test_int_player_xref_invariants.py` + `test_int_team_xref_invariants.py` + dbt schema tests. User manual spot-check sample 20 xref pairs post-generator-run. `_overrides.csv` seeds are the ongoing safety valve. |
| C | Wyscout `teams_data_parsed` NULL / malformed JSON | Fallback branch in `stg_wyscout__home_away_teams` synthesises `wyscout_unresolved_{match_id}_{side}` with `is_synthesized=true`, `synthesis_reason='wyscout_unresolved_teamsdata'`. All Wyscout rows produce valid `(home_team_id, away_team_id)`; `opponent_team_id` warn-flip passes unconditionally. |
| D | Metrica player anonymous-ID stability across matches | Per-match synthesis — `Player11` in game 1 ≠ `Player11` in game 2. Best practice; documented in dim_players header. |
| E | IDSSE cross-provider xref | Extend `int_player_xref` + `bronze.player_xref_raw` to carry provider labels; `generate_entity_xref.py` emits SB↔IDSSE + WS↔IDSSE pairs via fuzzy name matching against `stg_tracking__player_metadata.player_display_name`. Same for teams via `int_team_xref`. Metrica stays siloed (anonymised, unreachable by name matching) — data constraint, not a deferral. |
| F | try_cast on native ID joins | JOIN direction in PR 5a is fact.int_id → dim.string_native_id (cast int→string safe), so try_cast not needed on the join key. Any future Python reading `fact.team_key` back to a native form honours the per-provider cast via try_cast. |
| G | Lakebase synced-table schema evolution | Additive columns auto-evolve on refresh per `reference_lakebase_synced_table_auto_evolution`; no manual recreation. Grain growth on dim_teams (~453→~473+) propagates via same refresh. |
| H | StatsBomb↔Wyscout competition_id post-mapping (e.g., WS 795→SB 11 for La Liga) | dim_competitions already preserves them as separate rows because `provider` differs (PR 2 shape). Not a collision risk for team_key derivation. |

## 6. Testing strategy

### Per-layer test pyramid

| Layer | Files | Gate |
|---|---|---|
| Python unit | `test_generate_entity_xref.py` (fuzzy-match logic, MERGE idempotency), `test_metrica_ingestion_flag.py` (verify `is_anonymized=true` path vs subscription stub path) | `uv run pytest src/tests/ -v` |
| dbt schema | `_marts__models.yml`: unique/not_null/unique_combination on new keys; relationships `fct_* → dim_*`. `_intermediate__models.yml`: unique on xref grain. | `uvx --from 'dbt-core>=1.10.0,<1.12.0' dbt test` |
| Xref invariants | `test_int_player_xref_invariants.py`, `test_int_team_xref_invariants.py` — confidence range 70-100, no self-loops, no provider mismatch, injectivity per `(source_a, source_b)` pair | pytest on live warehouse |
| Bronze live schema | `test_bronze_live_schema.py` — new cols on `metrica_tracking`, `tracking_player_metadata`, `player_xref_raw`; new source `team_xref_raw` | pytest live |
| Staging coverage | `test_idsse_bronze_coverage.py` (team_id surfaced), `test_metrica_bronze_coverage.py` (is_anonymized surfaced), `test_wyscout_bronze_coverage.py` (teams_data_parsed consumed + new `wyscout_teams` entry), `test_staging_coverage.py` | pytest local |
| Live CI | `.github/workflows/dbt-live-ci.yml` via PR 4a path — `state:modified+` selector runs full downstream build on Databricks Job | GH Actions on PR |
| E2E Taipy | Puppeteer suite targeting Conversion Funnel (Wyscout opponent render), Pass Map (Metrica competition dropdown present + clickable), Player Similarity (cross-provider xref-collapsed entities visible) | Manual + puppeteer per `reference_puppeteer_taipy_dropdowns` |

### TDD sequence per phase

1. **Phase 1 (macros + bronze schema).** Write macro tests → macros. Write bronze-live-schema assertions → run migration script → green assertions. Commit.
2. **Phase 2 (staging).** Write staging coverage tests → staging models → green.
3. **Phase 3 (xref infra).** Write xref invariant tests against empty xref state → write `generate_entity_xref.py` → run against dev warehouse → tests green.
4. **Phase 4 (dims).** Write dim contract tests → dim rewrites → green.
5. **Phase 5 (facts).** Write `_marts__models.yml` contract changes (including warn→error flips) → rewrite fct_match_summary/fct_player_stats/fct_funnel_stages_agg → tests green.
6. **Phase 6 (deploy).** Execute `refresh_synced_tables.py` → grants → run E2E Taipy suite → green.

## 7. Ship criteria — PR 5a

All of the following must be green before PR 5a is shipped:

- `uv run ruff check src/ scripts/` — zero violations
- `uv run ruff format --check src/ scripts/` — clean
- `uv run pyright src/` — zero errors
- `uv run pytest src/tests/ -v` — all tests green
- Live-CI dbt build via `.github/workflows/dbt-live-ci.yml` on the PR branch — green (both dim migrations + 3 modified marts + all downstream transitive models selected by `state:modified+` build and pass tests)
- Both `severity: warn` suppressions (`not_null_fct_funnel_stages_agg_opponent_team_id`, `not_null_fct_player_stats_player_id`) pass at error severity
- `bronze.wyscout_teams` populated with ~280 rows via the one-shot `ingest_wyscout` trigger (new table per the teams.json work folded in)
- `dim_teams.team_name` non-NULL for all Wyscout real rows (closes the pre-existing Wyscout team-name coverage gap)
- Lakebase synced tables for `dim_teams_synced`, `dim_players_synced`, `dim_competitions_synced`, `fct_player_stats_synced`, `fct_funnel_stages_agg_synced`, `fct_match_summary_synced` — new columns visible via `psql`; grants applied
- Taipy E2E on dev Space:
  - Conversion Funnel page renders for a Wyscout match (opponent column populated, previously NULL)
  - Pass Map competition cascade includes "Metrica Sample Dataset"; selecting it renders Metrica passes
  - Player Similarity page surfaces a cross-provider xref-collapsed entity correctly (sample a known SB↔WS↔IDSSE match from the generator output)
  - Any page displaying Wyscout team names now shows real names (previously NULL/blank)
- Manual spot-check: 20 random xref pairs from the generator output are plausibly correct (no obvious false positives)
- Memory entry `project_kimball_pr5a_shipped.md` captures the delta for session 2 kickoff

## 8. Ship criteria — PR 5b

- All code-quality gates same as PR 5a
- Live-CI dbt build — green (6 modified embedding/percentile marts + transitive downstream)
- Taipy E2E:
  - Player Similarity page, Player Profile page, Similarity Explorer page — no regression; both `player_key` and `canonical_player_id` paths produce same results during dual-read window
- HF dataset card parity test (`test_hf_publish_parity.py`) — green
- Lakebase synced tables for the 6 migrated marts updated
- No HF dataset payload changes (cards only)

## 9. Risks + mitigations

| Risk | Mitigation |
|---|---|
| `generate_entity_xref.py` produces too few pairs (IDSSE Bundesliga doesn't overlap with SB/WS open-data coverage) | Run the script first; if zero pairs at ≥70 confidence, document the data-coverage constraint in dim_players header and ship IDSSE as siloed-by-data (not by design). Not a blocker for PR 5a ship. |
| Wyscout `teams_data_parsed` parse failure rate is high enough that the synthesised fallback pollutes dim_teams with many `wyscout_unresolved_*` rows | Live-measure the parse-success rate during Phase 1; if > 5% failures, investigate whether the JSON schema in bronze needs expansion before shipping the fallback branch. Either fix at source or accept the fallback as the best-practice resolution (per the no-deferral discipline). |
| Entity-resolution activation causes dim_players grain to change in ways that break incremental `fct_player_embeddings` MERGE (known `DELTA_MULTIPLE_SOURCE_ROW_MATCHING` risk per the mart's header comment) | Rebuild fct_player_embeddings `--full-refresh` after activating xref; the 7-match IDSSE scale doesn't meaningfully increase rebuild time. Staging + full-refresh path tested before PR 5b merge. |
| Live-CI latent-bug cascade (per `reference_live_ci_surfaces_latent_bugs`) — `state:modified+` on dim_teams/dim_players pulls in many downstream marts that haven't built since PR 2 | Budget triage time per PR 4b playbook. Compile errors must be fixed in-PR (direct column-rename swaps). `not_null` failures outside PR 5a scope get `severity: warn` with YAML pointer at the closing PR (typically PR 6-8). |
| 57-file `canonical_player_id` cascade — consumer code breaks if dim_players rewrite accidentally changes how existing `canonical_player_id` values are computed | `canonical_player_id` column preserved verbatim via existing `dbt_utils.generate_surrogate_key(['player_id', "'statsbomb'"])` call unchanged in the new dim_players.sql. Regression guard: a test asserting existing values are stable (sample of 50 known canonical_player_id values before/after the rewrite). |
| Backfill of `bronze.player_xref_raw` with `source_a='statsbomb', source_b='wyscout'` hits a constraint or races with ingestion | The legacy Python matcher writes via append-only INSERT; the new `generate_entity_xref.py` uses Delta `MERGE INTO` keyed on `(source_a, player_id_a, source_b, player_id_b)`. The one-time backfill is a single UPDATE statement run before the first generator invocation, outside legacy-matcher run hours. Idempotent (re-run safe). Post-backfill, the table carries merge semantics cleanly because `source_a` + `source_b` complete the MERGE key — no row duplication. |
| PR 4a live-CI Databricks Job spec needs update for the new xref + dim_teams + dim_players models | Verify the Job's `--select` computation handles new intermediates. No change needed to the Job itself; `state:modified+` naturally selects them. |
| Figshare teams.json URL stale or dataset moved | Precedent: the 3 existing Figshare URLs in `wyscout.py` have been stable since 2026-03. Mitigation at implementation time: WebFetch the collection page `https://figshare.com/collections/Soccer_match_event_dataset/4415000` to resolve the current teams.json `ndownloader` ID; if 404 on the old ID, the collection page lists the live ID. Fallback: Figshare's REST API (`https://api.figshare.com/v2/collections/4415000`) enumerates files with stable IDs. |

## 10. Rollout sequence

1. **Brainstorm + spec** — this document. User reviews. (Current step.)
2. **Write plan** — `docs/superpowers/plans/2026-04-24-kimball-pr5a-foundation.md` following `superpowers:writing-plans` skill.
3. **Branch 5a** — created 2026-04-24 (`kimball-pr5a-foundation` tracking `origin/main` at 728245f, post-PR-4c merge).
4. **Implement 5a** — TDD-driven phases per §6. Sequencing note: the Wyscout teams.json ingestion work (`ingest_teams` in `wyscout.py` + schema snapshot + source YAML + staging model + tests) ships inside Phase 1/2 (bronze + staging). The one-shot ingestion trigger runs after the code lands on dev and before dim_teams build — otherwise `stg_wyscout__teams` returns 0 rows and `dim_teams.team_name` stays NULL in testing.
5. **Review + merge 5a** — single commit per branch (squash-friendly). User reviews + approves commit, PR, merge.
6. **Deploy 5a** — trigger `ingest_wyscout` Job one-shot to populate `bronze.wyscout_teams`; `refresh_synced_tables.py` for the 6 affected synced tables; grants via Step 0.5; E2E verification per §7.
7. **Memory entry + handoff** — `project_kimball_pr5a_shipped.md` captures state for session 2.
8. **Session 2 kickoff** — re-verify memory against live state (per `feedback_verify_reference_memory_against_source`); branch 5b from current main; TDD-driven phases per §6.
9. **Review + merge 5b** — same discipline.
10. **Deploy 5b** — `refresh_synced_tables.py`, grants, E2E verification.

User approval is required at each of: every commit, every PR creation, every merge, every deploy — per `feedback_no_commits_without_approval` and `feedback_no_approval_asks_in_plan_execution` (approvals at genuine git-operation checkpoints only; inline work proceeds without per-task approval).

## 11. Open questions / items requiring live verification at implementation time

1. **Wyscout `teams_data_parsed` parse failure rate.** Need `SELECT count(*) FILTER (WHERE teams_data_parsed IS NULL) FROM dev_silver.stg_wyscout__matches` (or the bronze equivalent). Determines whether the synthesised fallback is cosmetic (~0 rows) or structurally meaningful. Flagged in §9.
2. **IDSSE↔StatsBomb player overlap.** Need the generator's output row count + spot-sample. Determines whether IDSSE is xref-populated or silo-by-data. Flagged in §9.
3. **Figshare `teams.json` download ID.** The Wyscout teams-ingestion work folded into PR 5a needs the exact `ndownloader.figshare.com/files/<id>` URL from the Figshare collection at `https://figshare.com/collections/Soccer_match_event_dataset/4415000`. One WebFetch on the collection page at implementation time resolves it; the other three Figshare URLs in `wyscout.py` (events, matches, players) are stable precedent, so a fourth ID on the same collection carries the same stability expectation. Confirmed live: `bronze.wyscout_teams` does not exist today; no pre-existing ingestion artefact to preserve.
4. **Live DESCRIBE on `bronze.idsse_tracking`** against every staging environment (dev + prod) to confirm the 14 missing columns are present everywhere before the `_idsse__sources.yml` update merges. Section 1 verified dev; prod check is additive.

All four are fact-checks, not design decisions. Resolved at Phase 1 of implementation.

---

**End of spec.**

