# SPADL Team ID Resolution for Tracking Providers

## Problem Statement

The SPADL conversion for IDSSE, Metrica, and SkillCorner sets `team_id = NULL` in `bronze.spadl_actions`. This breaks:

1. **VAEP feature extraction** — `fs.team()` computes `a.team_id == a0.team_id`; NULL produces `False` for all rows, corrupting the `team_1`/`team_2`/`team_3` features.
2. **VAEP value formula** — `offensive_value()` / `defensive_value()` compute `sameteam = _prev(actions.team_id) == actions.team_id`; NULL breaks the probability chain, yielding NaN VAEP.

**Impact**: 20 matches (7 IDSSE, 3 Metrica, 10 SkillCorner) produce 23,686 null VAEP values — 90% of their actions unscored.

**Root cause**: `src/ingestion/spadl_conversion.py` lines 966, 1311, 1596 — intentional NULL-fill that downstream consumers don't tolerate.

## Design Decision

Populate `team_id` with `hash_native_id_to_bigint(team_id_native)` — the same SHA-256[:15] pure function already used for `match_id` on these providers.

### Why hash (not dim_teams lookup)

| Concern | Dim lookup | Hash |
|---------|-----------|------|
| VAEP correctness | Works | Works (equality comparison only) |
| Bootstrap ordering | Fails on fresh env — dim_teams doesn't exist | Always works (pure function) |
| Cross-layer dependency | Bronze writer reads gold mart (inverts data flow) | None |
| Spark dependency inside UDF | Requires broadcast or closure dict | Pure function, already importable in UDF closures |
| Semantic consistency | Introduces xxhash64 surrogates alongside raw provider ints | Consistent with `match_id` pattern for same providers |
| DAG changes | May need `dbt_build_output_marts` before SPADL | None |

The `team_id` column in `spadl_actions` is already semantically "a stable BIGINT that identifies the team within this provider's data" — StatsBomb has raw SB team_ids, Wyscout has raw WS team_ids. Adding `hash_native_id_to_bigint(team_id_native)` for tracking providers extends this semantic consistently. The Kimball surrogate resolution remains the gold layer's responsibility (dbt mart JOINs via `team_id_native`).

### Scope

**In scope:**
- Fix `team_id` NULL → hash for IDSSE, Metrica, SkillCorner SPADL converters
- Defense-in-depth guard in VAEP scoring UDF (raise if NULL team_id reaches scoring)
- Tests that catch NULL team_id going forward
- Backfill plan for existing broken data

**Out of scope:**
- `player_id` resolution — not used by VAEP features or formula. Same hash pattern applies when addressed separately.
- `competition_id` / `season_id` resolution — not used by VAEP. Trivial to add later with same pattern.

## Implementation

### Change 1: SPADL Conversion (3 providers)

In each tracking-provider UDF, replace the NULL-fill with a hash:

```python
# BEFORE (broken):
actions["team_id"] = _pd.array([_pd.NA] * n, dtype="Int64")

# AFTER:
actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
```

`_hash_id` is `hash_native_id_to_bigint` — already imported in all 3 UDFs for `match_id` hashing.

**Locations:**
- `_make_idsse_spadl_udf` (line 966): replace NULL-fill after `team_id_native` is populated (line 938)
- `_make_metrica_spadl_udf` (line 1311): replace NULL-fill after `team_id_native` is populated (line 1292)
- `_make_skillcorner_spadl_udf` (line 1596): replace NULL-fill after `team_id_native` is populated (line 1574-1578)

**Ordering constraint**: The `team_id_native` column must be populated BEFORE the hash. Current code already populates `team_id_native` before the NULL-fill block, so the order is naturally correct.

**Edge-case handling (ADR-002 §5)**: Diagnostic query confirmed 4 IDSSE rows have NULL `team_id_native` — all are `type_id=4` (freekick_short) events where silly-kicks produces a team label that the `_team_label_to_dfl_id` mapper cannot resolve (neither "home" nor "away"). These are legitimate edge cases in the silly-kicks output, not data corruption.

Strategy: log a structured warning (not crash) for NULL `team_id_native` rows, then fill `team_id` with a deterministic sentinel hash so the row survives VAEP scoring. The sentinel uses `hash_native_id_to_bigint("__UNKNOWN_TEAM__")` — a fixed, reproducible value that will never collide with a real team's hash (no provider uses that string as a team identifier). This preserves the row for downstream consumers while making the anomaly visible in logs.

```python
null_team_mask = actions["team_id_native"].isna()
if null_team_mask.any():
    _logger.warning(
        "NULL team_id_native in %d rows for match_id=%s (type_ids=%s). "
        "Filling with sentinel hash.",
        null_team_mask.sum(),
        match_id_str,
        actions.loc[null_team_mask, "type_id"].unique().tolist(),
    )
    actions.loc[null_team_mask, "team_id_native"] = "__UNKNOWN_TEAM__"
actions["team_id"] = actions["team_id_native"].map(_hash_id).astype("Int64")
```

The sentinel approach is preferred over raising because: (a) 4 rows out of ~24K tracking-provider actions is 0.017% — crashing the entire match for marginal edge cases is disproportionate; (b) the VAEP formula only needs team_id equality semantics — a sentinel hash that differs from both real teams produces correct "different team" comparisons for adjacent actions; (c) these rows already had NULL team_id in production for months without operational impact beyond the NaN VAEP values we're fixing.

**Hash collision acknowledgment**: SHA-256[:15] over short strings (<50 chars) has a collision probability of ~1 in 2^60 for pairwise comparisons. With <100 distinct team identifiers across all providers, collisions are astronomically unlikely and not a practical concern.

### Change 2: VAEP Scoring Guard (defense-in-depth)

In the VAEP scoring UDF (`_make_scoring_udf` closure), before calling `_vaepformula.value()`, validate that `team_id` is non-NULL:

```python
if game_actions["team_id"].isna().any():
    null_count = game_actions["team_id"].isna().sum()
    raise RuntimeError(
        f"VAEP scoring received {null_count} NULL team_id rows for game_id={game_id}. "
        f"SPADL conversion must resolve team_id before scoring."
    )
```

This guard ensures that if the conversion fix is ever reverted or a new provider is added without resolution, the failure is loud and immediate rather than producing silent NaN.

### Change 3: Backfill

After deploying the fix, re-run the affected matches. The DELETE order matters — we must collect match_ids from spadl_actions BEFORE deleting spadl rows, then delete vaep values (which reference those match_ids), then delete spadl rows:

```sql
-- Step 1: Collect match_ids while spadl_actions still has the rows
CREATE OR REPLACE TEMPORARY VIEW _tracking_match_ids AS
SELECT DISTINCT match_id FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('idsse', 'metrica', 'skillcorner');

-- Step 2: Delete VAEP values for those matches
DELETE FROM soccer_analytics.bronze.vaep_action_values
WHERE match_id IN (SELECT match_id FROM _tracking_match_ids);

-- Step 3: Delete SPADL actions
DELETE FROM soccer_analytics.bronze.spadl_actions
WHERE data_source IN ('idsse', 'metrica', 'skillcorner');
```

Then trigger selective re-run:

```bash
databricks jobs run-now --json '{"job_id": 302697362345215, "only": ["preflight_spadl_vaep", "compute_spadl_vaep"]}' --no-wait
```

The preflight guard will discover all tracking-provider matches as "new" (not in spadl_actions) and emit them as chunks. The for_each_task fan-out will reprocess them with the fixed conversion code.

## Testing

### Unit Tests (new: `src/tests/test_spadl_team_resolution.py`)

1. **test_team_id_populated_for_idsse_actions** — Build a minimal IDSSE-like DataFrame with `team_id_native` populated, run through the hash, assert `team_id` is non-NULL and consistent (same native → same hash).
2. **test_team_id_populated_for_skillcorner_actions** — Same for SkillCorner native IDs (numeric strings like "1805").
3. **test_team_id_populated_for_metrica_actions** — Same for Metrica synthetic IDs.
4. **test_team_id_null_native_fills_sentinel** — If `team_id_native` is NULL for some rows (the freekick_short edge case), the conversion logs a warning and fills those rows with the deterministic sentinel hash `hash_native_id_to_bigint("__UNKNOWN_TEAM__")`. Assert: no NULL `team_id` in output, sentinel hash differs from both real team hashes, warning is logged with match context and affected type_ids.
5. **test_two_teams_produce_distinct_hashes** — Two different `team_id_native` values produce different `team_id` hashes (the equality semantics that VAEP relies on).
6. **test_hash_is_deterministic** — Same `team_id_native` always produces the same `team_id` (idempotent re-runs produce same data).

### Integration Test (new: `src/tests/test_spadl_vaep_tracking_providers.py`)

7. **test_vaep_non_null_for_two_team_fixture** — Build a realistic fixture with ~100 SPADL actions across 2 teams (proper action_id sequencing, period_id, time_seconds, type_id distribution). Populate `team_id` via hash. Run through the VAEP scoring UDF with a **test-trained XGBoost model** fitted on the fixture's feature dimensionality (not the production Champion model — test isolation requires no MLflow/UC dependency). The test trains a trivial model on the fixture features at setup time, uses it for scoring, and asserts that VAEP values are non-NULL for all actions except the expected last-action-per-period boundary (≤4 NaN per match for 2 periods + possible extra time). The test validates the team_id→feature→formula pipeline, not model accuracy.

8. **test_vaep_raises_on_null_team_id** — Build a fixture with NULL `team_id`, pass to VAEP scoring UDF. Assert `RuntimeError` is raised with a clear message (not silent NaN production).

### Regression Guard (added to existing tests)

9. **test_spadl_conversion_never_null_team_id** — In `src/tests/test_spadl_vaep.py`, add a test that asserts: after the SPADL conversion UDF runs on test fixtures for each tracking provider, `team_id` has zero NULL values. This is the direct regression test.

### Fixture Design

Fixtures must be realistic enough to exercise the VAEP formula's same-team logic:
- 2 teams, alternating possession (passes within team, then turnover)
- Mix of action types: pass (type_id=0), dribble (1), shot (11), tackle (8), interception (9)
- Proper time_seconds progression (monotonically increasing within period)
- period_id: 1 and 2
- `team_id_native`: two distinct string values (e.g., "DFL-CLU-000008", "DFL-CLU-00000G")
- `team_id`: hash of native (the fix under test)

## Files Modified

| File | Change |
|------|--------|
| `src/ingestion/spadl_conversion.py` | Replace `team_id = NULL` with `team_id = hash(team_id_native)` in 3 UDFs; add NULL-native guard |
| `src/ingestion/spadl_vaep.py` | Add NULL team_id guard in scoring UDF (defense-in-depth) |
| `src/tests/test_spadl_team_resolution.py` | New: unit tests for team_id hash resolution |
| `src/tests/test_spadl_vaep_tracking_providers.py` | New: integration test for VAEP on tracking-provider data |
| `src/tests/test_spadl_vaep.py` | Add regression guard for NULL team_id |

## Non-Goals

- `player_id` resolution (not used by VAEP; same pattern applies later).
- `competition_id` / `season_id` resolution (not used by VAEP; trivial to add later).
- Changing dim_teams schema or entity resolution pipeline.
- Modifying the silly-kicks VAEP formula (formula is correct; input was wrong).
- Migrating StatsBomb/Wyscout `team_id` to hashes (they already have valid raw provider ints).
