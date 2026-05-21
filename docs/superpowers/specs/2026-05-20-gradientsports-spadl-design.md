# Gradient Sports SPADL/VAEP Conversion + HF License Gate

## Goal

Extend the existing 5-provider SPADL pipeline (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner) with Gradient Sports as the 6th source (64 WC2022 matches, 144K event rows in `bronze.gradientsports_events`). Gate HF dataset publishers so GS data is computed internally but not published externally until a license is secured.

## Context

- Bronze events already ingested (PR #298/#300): 64 matches, 144,541 rows.
- `bronze.gradientsports_events` uses `json_normalize` dot-notation columns (e.g., `gameEvents.gameEventType`, `possessionEvents.passType`, `stadiumMetadata.homeTeamStartLeft`).
- silly-kicks provides `silly_kicks.spadl.gradientsports.convert_to_actions(events, home_team_id, home_team_start_left, home_team_start_left_extratime, ...)` expecting 47 snake_case columns (`EXPECTED_INPUT_COLUMNS`).
- The converter outputs 18 columns (`GRADIENTSPORTS_SPADL_COLUMNS`) including 4 populated tackle qualifier columns (`tackle_winner_player_id`, `tackle_winner_team_id`, `tackle_loser_player_id`, `tackle_loser_team_id`).
- GS match IDs are numeric strings (e.g., `"10502"`) -- same hashed-ID pattern as IDSSE/Metrica/SkillCorner.
- SkillCorner SPADL conversion already exists (merged to main) -- NOT in scope.
- TC-3 calibration sweep needs GS SPADL data to include GS in the calibration.
- WC 2022 has ~5-6 extra-time matches (Argentina-France final, Argentina-Netherlands QF, etc.) with `period_id` in {3, 4}.

## Architecture

Follows the IDSSE batch-dispatch pattern (closest analogue: metadata embedded in events table, tackle qualifier handling, hashed string match IDs):

1. **Adapter** (`spadl_adapter.py`): `adapt_gradientsports_events(pdf)` -- rename + NaN-fill only, returns `pd.DataFrame`. Separate `extract_gradientsports_match_metadata(pdf)` returns metadata dict.
2. **Conversion UDF** (`spadl_conversion.py`): Single `_make_gradientsports_spadl_udf()` closure. The UDF extracts metadata from bronze columns at execution time (IDSSE pattern), calls the adapter, calls the converter, applies IDSSE-style tackle qualifier mapping and shared post-processing helpers.
3. **Orchestrator** (`spadl_conversion.py`): `_convert_gradientsports_from_bronze()` -- IDSSE batch pattern: filter new matches, `groupBy("match_id").applyInPandas()` with one UDF (1 Spark job for all 64 matches), write Delta with replaceWhere.
4. **Guard extension** (`spadl_vaep.py`): Add `gs_new` discovery via `_diff_hashed_source_against_spadl()`, wire into `FilterResult.metadata`, chunk building, and `run_pipeline`.
5. **Native ID generators** (`shared/identifiers.py`): `gradientsports_native_match_id()`, `gradientsports_native_team_id()`, `gradientsports_native_player_id()`, plus classmethods on all three NamedTuples: `NativeMatchId.gradientsports()`, `NativeTeamId.gradientsports()`, `NativePlayerId.gradientsports()`.
6. **Enrichment source registration** (`spadl_enrichments.py`): Add `"gradientsports"` to `_VALID_SOURCES` frozenset.
7. **HF license gate**: Add `WHERE data_source != 'gradientsports'` to SQL in `publish_spadl_vaep_hf.py` and `publish_tracking_context_hf.py`.
8. **dbt test coverage**: Add `'gradientsports'` to `accepted_values` list for `data_source` and update description in `_spadl__models.yml`.

## Design Decisions

### D1: Adapter and metadata extraction (separate functions)

The bronze table stores `json_normalize` output with dot-notation prefixes. Existing adapters (`adapt_statsbomb_events`, `adapt_wyscout_events`, `adapt_idsse_events_for_silly_kicks`, `adapt_metrica_events_for_silly_kicks`) all return `pd.DataFrame` only -- they never return metadata. To preserve this contract:

**`adapt_gradientsports_events(pdf) -> pd.DataFrame`** -- two responsibilities:
- (a) Rename 47 event columns via module-level constant dict mapping bronze dot-notation to snake_case
- (b) NaN-fill absent optional columns (not all 47 exist in every match's JSON)

**`extract_gradientsports_match_metadata(pdf) -> dict`** -- separate function, returns:
- `home_team_id` (int): from `stadiumMetadata.homeTeamId`
- `home_team_start_left` (bool): from `stadiumMetadata.homeTeamStartLeft`
- `home_team_start_left_extratime` (bool | None): from `stadiumMetadata.homeTeamStartLeftExtraTime`

Both extracted from the first row of the match group (these are match-level constants denormalized into every event row).

**Rename map examples:**
- `gameEvents.gameEventType` -> `game_event_type`
- `possessionEvents.passOutcomeType` -> `pass_outcome_type`
- `gameId` -> `game_id` (top-level, no dot prefix)
- `possessionEventId` -> `possession_event_id`
- etc. for all 47 columns in `EXPECTED_INPUT_COLUMNS`

**Disambiguation: `match_id` vs `gameId`.** Bronze ingestion adds `match_id` (the native string match ID, e.g., `"10502"`) at `gradientsports_events.py:69`. The event JSON contains `gameId` (numeric, same value). The adapter renames `gameId` -> `game_id` for the converter. The `match_id` column is used separately for grouping/hashing.

### D2: Orchestrator dispatch pattern (IDSSE batch)

GS metadata lives in the events table (denormalized per-row), NOT in a separate table. This matches the IDSSE pattern exactly, NOT the SkillCorner per-match loop pattern.

**IDSSE batch pattern** (verified at `spadl_conversion.py:1098`):
1. Filter bronze to new matches: `events_sdf.filter(col("match_id").isin(new_match_ids))`
2. Single `groupBy("match_id").applyInPandas(udf_fn, schema=spadl_schema)` -- 1 Spark job for all matches
3. UDF extracts metadata from bronze columns at execution time (`pdf["stadiumMetadata.homeTeamId"].iloc[0]`)
4. Write Delta with `replaceWhere`

This is simpler and more efficient than SkillCorner's per-match loop (1 Spark job vs. 64). No per-match closure needed -- the UDF reads metadata from the input DataFrame directly.

### D3: Direction-of-play (including extra time)

`stadiumMetadata.homeTeamStartLeft` is available per-event in the bronze table (match-level constant denormalized into every row). The `extract_gradientsports_match_metadata()` function reads it from the first row. This is simpler than IDSSE (which infers from kickoff positions) because GS provides the flag directly.

**Extra time (CRITICAL):** WC 2022 has ~5-6 matches with extra time (period_id in {3, 4}). The silly-kicks converter raises `ValueError` when `home_team_start_left_extratime` is `None` but ET periods exist. `extract_gradientsports_match_metadata()` MUST extract `stadiumMetadata.homeTeamStartLeftExtraTime`:
- Present and non-null: cast to `bool`
- Present and null/missing: return `None` (converter will raise if ET periods exist -- correct fail-loud behavior)
- Column absent: return `None`

No closure capture needed -- the UDF calls `extract_gradientsports_match_metadata(pdf)` at execution time (IDSSE batch pattern).

### D4: HF license gate

Until a license agreement is in writing, GS data must not be published to HuggingFace. The gate is a SQL `WHERE data_source != 'gradientsports'` filter in the two HF publisher scripts. This is the minimal, reversible change -- remove the filter when the license is secured.

Both publishers affected:
- `scripts/publish_spadl_vaep_hf.py` (queries `fct_action_values`)
- `scripts/publish_tracking_context_hf.py` (queries `fct_tracking_context`)

### D5: Guard + pipeline integration

The guard (`_VaepGuard.check()`) already has the pattern for hashed-ID sources. Adding GS requires:
- One new `_diff_hashed_source_against_spadl()` call for `gradientsports_events`
- New `gs_new` key in metadata dict
- Update `total_new` sum
- Update `_PROVIDER_METADATA_KEYS`: `"gs_new": "gradientsports"`
- Update `_CHUNK_SIZES`: `"gradientsports": 50`
- Add `"gradientsports"` to `_VALID_CHUNK_PROVIDERS` frozenset
- Update `_run_chunk` converters dict
- Wire `_convert_gradientsports_from_bronze` into `run_pipeline`'s Phase A
- Add `set(filter_result.metadata["gs_new"])` to `unscored_ids` union in `run_pipeline`

### D6: Chunk size

GS has 64 matches (WC2022 tournament). Using chunk size 50 (same as IDSSE/Metrica/SkillCorner) means 2 chunks. Appropriate for the data volume.

### D7: Tackle qualifier mapping

GS is the 2nd provider (after IDSSE) with populated tackle qualifier columns. The silly-kicks converter outputs 4 Int64 tackle columns on challenge events: `tackle_winner_player_id`, `tackle_winner_team_id`, `tackle_loser_player_id`, `tackle_loser_team_id`.

These must NOT be null-filled via `null_fill_tackle_qualifiers()`. Instead, follow the IDSSE pattern (`spadl_conversion.py:1028-1039`):

```
for native_col, key_col, sk_col in (
    ("tackle_winner_player_id_native", "tackle_winner_player_key", "tackle_winner_player_id"),
    ("tackle_winner_team_id_native", "tackle_winner_team_key", "tackle_winner_team_id"),
    ("tackle_loser_player_id_native", "tackle_loser_player_key", "tackle_loser_player_id"),
    ("tackle_loser_team_id_native", "tackle_loser_team_key", "tackle_loser_team_id"),
):
    actions[native_col] = actions[sk_col].astype("string")
    actions[key_col] = actions[native_col].map(_hash_or_na).astype("Int64")
```

**Dtype note:** IDSSE tackle columns are object dtype (DFL string IDs); GS tackle columns are Int64 (numeric player/team IDs). `.astype("string")` handles both correctly.

**Docstring update:** `null_fill_tackle_qualifiers` in `spadl_udf_shared.py:93` currently says "for non-IDSSE sources." Must be updated to "for sources without native tackle qualifiers (i.e., not IDSSE or GradientSports)."

### D8: UDF post-processing helper sequence

The GS UDF follows the IDSSE batch pattern: extract metadata and adapt events inside the UDF at execution time, then post-process. Full sequence:

1. `metadata = extract_gradientsports_match_metadata(pdf)` -- read home_team_id, direction flags from bronze columns
2. `adapted = adapt_gradientsports_events(pdf)` -- rename 47 columns + NaN-fill
3. `actions, report = convert_to_actions(adapted, home_team_id=metadata["home_team_id"], home_team_start_left=metadata["home_team_start_left"], home_team_start_left_extratime=metadata["home_team_start_left_extratime"])`
4. `apply_player_id_native(actions, source="gradientsports")` -- MUST precede legacy BIGINT NULL-fill. Note: GS player_ids are Int64 from the converter; `apply_player_id_native` routes to the else branch (`spadl_udf_shared.py:38-41`) which calls `.astype("string")` -- this works correctly on Int64 despite the docstring saying "IDSSE/Metrica: already string-shaped". The plan should update that docstring.
5. Hash `match_id` and `game_id` via `hash_native_id_to_bigint(match_id_str)`
6. Hash `team_id`: `team_id_native = gradientsports_native_team_id(str(team_id))` -> `team_id = hash_native_id_to_bigint(team_id_native)`, with `UNKNOWN_TEAM_SENTINEL` for NULLs
7. NULL-fill legacy BIGINTs: `player_id`, `competition_id`, `season_id` = `pd.NA` (Int64)
8. `actions["data_source"] = "gradientsports"`
9. `apply_spadl_enrichments(actions, source="gradientsports")` -- requires adding `"gradientsports"` to `_VALID_SOURCES`
10. `actions["original_event_id"] = actions["original_event_id"].astype(str)` -- silly-kicks maps this from `events["event_id"]` (the GS possessionEventId)
11. `null_fill_statsbomb_columns(actions, n=n)` -- fills SB-specific cols with NA
12. `cast_enrichment_dtypes(actions)`
13. `apply_match_level_natives(actions, home_team_id_native=str(home_team_id), competition_native_id=pd.NA, season_native_id=pd.NA, match_id_native=gradientsports_native_match_id(match_id_str))`
14. **Tackle qualifier mapping** (IDSSE pattern, NOT `null_fill_tackle_qualifiers`) -- see D7

### D9: StructType output schema

The UDF requires a Spark StructType matching the unified `_spadl_cols` (~40 fields). This is boilerplate matching all other UDFs (e.g., IDSSE at `spadl_conversion.py:1100-1164`) but must be defined explicitly -- schema mismatch causes Spark runtime failures. The schema is identical to all existing UDFs.

### D10: replaceWhere predicate

`_make_gradientsports_replace_where(hashed_match_ids)` returns `"data_source = 'gradientsports' AND match_id IN (...)"`. Follows the exact pattern of all other providers (e.g., `_make_skillcorner_replace_where` at `spadl_conversion.py:1491-1497`).

## Files Touched

| File | Action | Purpose |
|------|--------|---------|
| `src/ingestion/spadl_adapter.py` | Modify | Add `adapt_gradientsports_events()` + `extract_gradientsports_match_metadata()` + rename map |
| `src/ingestion/spadl_conversion.py` | Modify | Add `_make_gradientsports_spadl_udf()`, `_convert_gradientsports_from_bronze()`, `_make_gradientsports_replace_where()` |
| `src/ingestion/spadl_vaep.py` | Modify | Add `gs_new` to guard, `_PROVIDER_METADATA_KEYS`, `_CHUNK_SIZES`, `_VALID_CHUNK_PROVIDERS`, converters dict, pipeline dispatch |
| `src/ingestion/spadl_enrichments.py` | Modify | Add `"gradientsports"` to `_VALID_SOURCES` frozenset |
| `src/ingestion/spadl_udf_shared.py` | Modify | Update `null_fill_tackle_qualifiers` docstring + `apply_player_id_native` docstring to include GS |
| `src/shared/identifiers.py` | Modify | Add `gradientsports_native_match_id()`, `gradientsports_native_team_id()`, `gradientsports_native_player_id()`, classmethods on `NativeMatchId`, `NativeTeamId`, `NativePlayerId` |
| `scripts/publish_spadl_vaep_hf.py` | Modify | Add `WHERE data_source != 'gradientsports'` |
| `scripts/publish_tracking_context_hf.py` | Modify | Add `WHERE data_source != 'gradientsports'` |
| `dbt_project/models/staging/spadl/_spadl__models.yml` | Modify | Add `'gradientsports'` to `accepted_values`, update `data_source` description |
| `src/tests/test_gradientsports_spadl.py` | Create | Unit tests (see Test Plan) |
| `src/tests/test_format_contract.py` | Modify | Add GS native ID format contracts |

## Test Plan

**Fixture strategy:** Synthetic fixtures with edge cases. GS is not license-cleared for committing real bronze slices. Fixtures cover: regular match, ET match (period 3/4), empty match, match with tackles, match without optional columns.

### Adapter tests (`test_gradientsports_spadl.py`)
- `test_adapt_rename_completeness`: all 47 `EXPECTED_INPUT_COLUMNS` present after adaptation
- `test_adapt_nan_fill_missing_columns`: absent optional columns (tackle/shot) filled with NaN
- `test_adapt_empty_match`: returns empty DataFrame with correct 47 columns
- `test_adapt_match_id_vs_game_id`: `match_id` (ingestion-added) preserved, `gameId` renamed to `game_id`

### Metadata extraction tests (`test_gradientsports_spadl.py`)
- `test_extract_metadata_regular`: `home_team_id`, `home_team_start_left` extracted correctly
- `test_extract_metadata_et`: `home_team_start_left_extratime` correctly extracted from `stadiumMetadata.homeTeamStartLeftExtraTime`
- `test_extract_metadata_et_absent`: returns `None` for `home_team_start_left_extratime` when column absent

### UDF tests (`test_gradientsports_spadl.py`)
- `test_udf_output_columns`: output column count matches unified `_spadl_cols` (~40 columns)
- `test_udf_output_dtypes`: output dtypes match StructType schema
- `test_udf_tackle_qualifier_mapping`: `_native`/`_key` pairs populated on challenge events (NOT null-filled)
- `test_udf_legacy_bigint_null_fill`: `player_id`, `competition_id`, `season_id` are `pd.NA`
- `test_udf_team_id_hashing`: `team_id_native` is string, `team_id` is hashed BIGINT
- `test_udf_data_source`: value is `"gradientsports"`
- `test_udf_unknown_team_sentinel`: `UNKNOWN_TEAM_SENTINEL` applied for NULL team_id rows

### ET integration tests (`test_gradientsports_spadl.py`)
- `test_et_match_does_not_crash`: fixture with period 3/4 rows + valid `stadiumMetadata.homeTeamStartLeftExtraTime` -> converter produces valid SPADL actions for ET periods
- `test_et_match_missing_flag_raises`: fixture with period 3/4 rows + NULL `stadiumMetadata.homeTeamStartLeftExtraTime` -> converter raises `ValueError` (fail-loud)

### Guard tests (`test_gradientsports_spadl.py`)
- `test_guard_gs_new_detection`: diff detection for new GS matches
- `test_guard_gs_empty_diff`: empty-diff case (all matches already converted)
- `test_guard_gs_new_key_in_metadata`: `gs_new` key present in metadata

### Format contract tests (`test_format_contract.py`)
- `gradientsports_native_match_id` regex validation
- `gradientsports_native_team_id` regex validation
- `gradientsports_native_player_id` regex validation

### Integration/E2E (`test_gradientsports_spadl.py`)
- `test_e2e_bronze_to_spadl`: full pipeline from synthetic bronze fixture -> SPADL output with column/dtype assertions (local PySpark)

## Out of Scope

- GS tracking context conversion (tracking is a separate pipeline; the HF license gate touches `publish_tracking_context_hf.py` but GS tracking context conversion is not in scope)
- HF dataset README updates for GS (deferred until license secured)
