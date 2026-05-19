# TC-2: `is_goalkeeper` Migration — Tracking Context as Single Source of Truth

**Date:** 2026-05-18
**Status:** Draft (rev 2 — incorporates silly-kicks source-of-truth review)
**Author:** Karsten Skyt Nielsen + Claude Opus 4.6

## 1. Problem

`fct_tracking_frames.is_goalkeeper` is derived from three independent provider-specific heuristics:

| Provider | Heuristic | Source | Quality |
|----------|-----------|--------|---------|
| IDSSE | `PlayingPosition="TW"` in DFL XML | `stg_idsse__tracking` | Authoritative (ground truth), per-frame |
| Metrica | `jersey_number == "1"` | `stg_metrica__tracking` | **Wrong on 6/6 sampled team-matches** |
| SkillCorner | `position_name = 'goalkeeper'` from match roster | `stg_skillcorner__tracking` | Reasonable but fragile (6/10 old matches return `has_gk=False`) |

The TC-1 tracking context pipeline (PRs #273-#294) already runs silly-kicks `derive_goalkeepers()` with 3-tier identification (Tier 1: Sportec native ground truth; Tier 2: positional heuristic for Metrica/SkillCorner). The `defending_gk_player_id_native` column in `stg_spadl__tracking_context` is the authoritative GK identification source.

**Additionally:** `fct_tracking_frames` contains 10 ghost SkillCorner rows from the legacy kloppy/GitHub pipeline. These have different `match_key` values than the current bronze data (re-ingested via pining-for-the-data API). A full-refresh is needed to flush them.

## 2. Approach

**Approach A (selected):** Create a dbt intermediate model that extracts GK player identities per match from the tracking_context staging layer, and use it in `fct_tracking_frames` via a single LEFT JOIN. Replaces all three provider-specific heuristics with one authoritative source.

**Rejected alternatives:**

- **B — Bronze-level `derive_goalkeepers()` at ingestion time:** Puts truth at the lowest layer and preserves per-frame temporal granularity (see §4 H1). Requires touching 3 Python ingestion modules + re-ingestion to backfill. Duplicates logic that tracking_context already runs. Worth revisiting if the H1 GK-substitution regression proves material.
- **C — Point-fix staging heuristics per provider:** Least change but perpetuates the "3 different GK identification strategies" fragmentation. Doesn't leverage the silly-kicks 3-tier identification that TC-1 already invested in.

## 3. Design

### 3.1 New model: `int_tracking_goalkeepers` (ephemeral)

**Path:** `dbt_project/models/intermediate/int_tracking_goalkeepers.sql`
**Materialization:** `ephemeral` (no physical table — injected as CTE into consumers)
**Grain:** one row per (match_key, player_key) for each GK player in a match

Logic:
1. Extract distinct `(data_source, native_match_id, defending_gk_player_id_native)` from `stg_spadl__tracking_context` where the GK ID is non-NULL.
2. Resolve to Kimball surrogate keys via `INNER JOIN dim_matches` (on provider + native_match_id) and `INNER JOIN dim_players` (on provider + native_player_id).
3. Output: `(match_key, player_key)` pairs.

Expected output: ~2 rows per match (one per team's GK). Exceptions: Metrica = 1 (home GK only, de-identified data limitation), GK substitution matches = 3.

**Note on INNER JOINs (L1):** The intermediate uses INNER JOINs for dimension resolution while the consuming mart (`fct_tracking_frames`) uses LEFT JOINs. If a valid GK player ID in tracking_context has no `dim_players` entry, the GK is silently dropped from the intermediate. A warning-severity singular test (§5.4) guards against this.

### 3.2 Changes to `fct_tracking_frames.sql`

**a) `tracking` CTE:** Remove `is_goalkeeper` from the 15-column SELECT list across all three staging model unions. The column no longer flows from staging into the mart.

**b) `final` CTE:** After the existing `dim_matches`/`dim_teams`/`dim_players` LEFT JOINs, add:

```sql
left join {{ ref('int_tracking_goalkeepers') }} gk
    on gk.match_key = dm.match_key
   and gk.player_key = dp.player_key
```

Replace the current `wl.is_goalkeeper` column reference with:

```sql
gk.player_key is not null as is_goalkeeper
```

**c) No other changes.** The velocity/speed/acceleration CTEs, the incremental filter, the existing Kimball joins — all untouched.

### 3.3 Staging model disposition

Leave all three staging models (`stg_idsse__tracking`, `stg_metrica__tracking`, `stg_skillcorner__tracking`) unchanged. They continue to surface their provider-native `is_goalkeeper` columns — that's their job as staging models. The mart simply stops consuming them.

### 3.4 Full-refresh requirement

After merging, execute:

```
dbt run --full-refresh --select fct_tracking_frames+
```

The `+` suffix cascades to the three downstream marts: `fct_physical_stats`, `fct_tracking_avg_positions`, `fct_tracking_shape_timeline`.

**Why full-refresh:**
- Flushes the 10 ghost SkillCorner rows from the old kloppy pipeline (stale match_keys that no longer exist in bronze).
- Recomputes `is_goalkeeper` for all existing rows using the new intermediate model.

**Staging materialization (H2):** All staging models are `+materialized: view` (`dbt_project.yml:38`). Views read directly from bronze source tables — no stale data possible. The full-refresh of the mart is sufficient; no staging refresh needed.

### 3.5 Ongoing pipeline ordering (M5)

`fct_tracking_frames` uses `incremental_strategy='merge'` with the filter `where match_id not in (select match_id from existing_matches)`. Once a match enters the mart, its `is_goalkeeper` values are frozen — the merge never re-fires for existing match_ids.

**Hard constraint for every new match:** the TC-1 tracking_context Python pipeline MUST run before the `dbt run` that materializes `fct_tracking_frames` for that match. If dbt runs first, `int_tracking_goalkeepers` has no GK data for the new match, all frames get `is_goalkeeper = false`, and the values are frozen until the next full-refresh.

This ordering constraint is not new — all incremental columns in the mart have always been frozen at first-write. But TC-2 makes it more consequential because GK identification now depends on a separate pipeline (tracking_context) rather than being self-contained in staging.

**Current daily job topology already satisfies this:** `compute_tracking_context` runs before `dbt_build_input_marts` (which materializes `fct_tracking_frames`). No topology change needed. The constraint is documented here so it is not accidentally violated by future job reordering.

### 3.6 Synced table refresh

After the dbt full-refresh completes, the daily `lakebase-grants.yml` workflow will refresh `fct_tracking_frames_synced` on its next run. For immediate manual refresh: `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh` followed by a synced table refresh via the Databricks UI.

## 4. Known Limitations

| ID | Limitation | Impact | Mitigation |
|----|------------|--------|------------|
| H1 | **GK substitution temporal granularity regression (IDSSE).** The current `stg_idsse__tracking` derives `is_goalkeeper` per-frame from DFL XML `PlayingPosition="TW"` — frame-accurate during GK substitutions. The proposed match-level approach flags all GKs who played in a match as `is_goalkeeper = true` for ALL their frames, losing the exact substitution boundary. Downstream pitch control uses `is_goalkeeper` to select kinematic parameters (GK: `reaction_time=0.4s`, `max_accel=7.0 m/s²` vs outfield: `0.7s`, `5-6 m/s²`). | Near-zero practical impact. A substituted GK is **off the pitch** after the sub and does not appear in tracking frames — so the match-level flag only affects frames where the player IS tracked. The only material scenario is the extremely rare "outfield player goes in goal after red card without formal substitution" case. ~2-5% of matches have any GK sub at all. | Accepted. If this regression proves material in production, revisit with Approach B (bronze-level `derive_goalkeepers()` at ingestion time), which preserves per-frame temporal granularity. |
| | Metrica: 1 GK per match (home only) | Away GK gets `is_goalkeeper = false` | De-identified data limitation — genuinely unidentifiable. More correct than jersey-#1 heuristic which was wrong on all sampled matches. |
| H3 | **Single-team action coverage.** `defending_gk_player_id_native` captures the opposing team's GK per action. Both teams' GKs are identified only if at least one action exists from each team. If all SPADL actions belong to one team (incomplete event data), the other team's GK is invisible. | Extremely unlikely in normal matches — both teams generate actions (passes, tackles, clearances). | Stronger singular test (§5.1) catches the 1-GK-when-2-expected case for IDSSE/SkillCorner. |
| | Coverage gated on tracking_context | Matches without SPADL actions get `is_goalkeeper = false` for all players | Unlikely in practice — tracking ingestion always produces SPADL alongside tracking. All 20 current matches have coverage. |
| M3 | **`is_goalkeeper_source` not surfaced.** silly-kicks 3.3.0 (PR-S26) introduced `is_goalkeeper_source` ("native" / "derived") on the tracking schema. The tracking_context pipeline carries this. This PR does not surface it on `fct_tracking_frames`. | No downstream consumer currently needs GK provenance. | Follow-up: surface `is_goalkeeper_source` in a later PR if downstream consumers need to distinguish authoritative (IDSSE native) vs. algorithmically derived (Metrica/SkillCorner) GK identification. |

## 5. Testing

### 5.1 GK count per provider (permanent singular test)

Expects 2 GKs per match for IDSSE/SkillCorner, allows ≥1 for Metrica (de-identified limitation):

```sql
-- assert_tracking_frames_gk_count_by_provider.sql
WITH match_gk_counts AS (
    SELECT
        match_key,
        data_source,
        COUNT(DISTINCT CASE WHEN is_goalkeeper THEN player_key END) AS n_gks
    FROM {{ ref('fct_tracking_frames') }}
    GROUP BY match_key, data_source
)
SELECT *
FROM match_gk_counts
WHERE
    (data_source IN ('idsse', 'skillcorner') AND n_gks < 2)
    OR n_gks = 0
```

Expected: 0 rows.

### 5.2 IDSSE before/after comparison (permanent singular test)

Verify IDSSE GK identification is unchanged. The IDSSE staging already derives GK from authoritative DFL metadata — both old and new paths should produce the same set of GK player_keys per match. Codified as a singular test that compares the intermediate's IDSSE GK set against the staging model's GK set:

```sql
-- assert_idsse_gk_parity.sql
-- GK players identified by int_tracking_goalkeepers should match
-- the set identified by stg_idsse__tracking's per-frame flag.
WITH from_intermediate AS (
    SELECT DISTINCT gk.match_key, gk.player_key
    FROM {{ ref('int_tracking_goalkeepers') }} gk
    INNER JOIN {{ ref('dim_matches') }} dm ON dm.match_key = gk.match_key
    WHERE dm.provider = 'idsse'
),
from_staging AS (
    SELECT DISTINCT dm.match_key, dp.player_key
    FROM {{ ref('stg_idsse__tracking') }} st
    INNER JOIN {{ ref('dim_matches') }} dm
        ON dm.provider = 'idsse'
       AND dm.native_match_id = cast(st.match_id as string)
    INNER JOIN {{ ref('dim_players') }} dp
        ON dp.provider = 'idsse'
       AND dp.native_player_id = cast(st.player_id as string)
    WHERE st.is_goalkeeper = true
)
-- Any mismatch in either direction
SELECT * FROM from_intermediate
EXCEPT
SELECT * FROM from_staging
UNION ALL
SELECT * FROM from_staging
EXCEPT
SELECT * FROM from_intermediate
```

Expected: 0 rows (perfect parity). A non-empty result indicates the GK substitution temporal regression (H1) is material for this match set.

### 5.3 SkillCorner GK count (permanent singular test)

Subsumed by §5.1 — the provider-aware test already asserts SkillCorner matches have ≥2 GKs.

### 5.4 Unresolved GK player IDs (warning-severity singular test)

Guards against silent drops from the INNER JOIN in `int_tracking_goalkeepers` (M4):

```sql
-- warn_unresolved_gk_player_ids.sql
-- config: severity: warn
SELECT DISTINCT tc.data_source, tc.defending_gk_player_id_native
FROM {{ ref('stg_spadl__tracking_context') }} tc
LEFT JOIN {{ ref('dim_players') }} dp
    ON dp.provider = tc.data_source
   AND dp.native_player_id = tc.defending_gk_player_id_native
WHERE tc.defending_gk_player_id_native IS NOT NULL
  AND dp.player_key IS NULL
```

Expected: 0 rows. Non-zero = data quality gap in `dim_players`.

### 5.5 Standard CI gates

No Python changes, so only dbt compilation + singular tests apply. `dbt compile` to validate the model graph, `dbt test` for the singular tests.

## 6. Files Changed

| File | Change |
|------|--------|
| `dbt_project/models/intermediate/int_tracking_goalkeepers.sql` | **New** — ephemeral GK lookup from tracking_context staging |
| `dbt_project/models/marts/fct_tracking_frames.sql` | Remove `is_goalkeeper` from staging union, add GK join in `final` |
| `dbt_project/tests/assert_tracking_frames_gk_count_by_provider.sql` | **New** — singular test: 2 GKs for IDSSE/SkillCorner, ≥1 for Metrica |
| `dbt_project/tests/assert_idsse_gk_parity.sql` | **New** — singular test: intermediate vs staging GK parity for IDSSE |
| `dbt_project/tests/warn_unresolved_gk_player_ids.sql` | **New** — warning-severity test for dimension resolution gaps |

## 7. Downstream Impact

| Consumer | Impact |
|----------|--------|
| `fct_physical_stats` | Rebuilds via `+` cascade — no schema change |
| `fct_tracking_avg_positions` | Rebuilds via `+` cascade — no schema change |
| `fct_tracking_shape_timeline` | Rebuilds via `+` cascade — no schema change |
| `fct_tracking_frames_synced` (Lakebase) | Refreshed on next daily run — `is_goalkeeper` values updated |
| Taipy tracking/team-shape pages | Read from synced table — automatically pick up new GK values |
| Python compute pipelines (pitch_control, OBSO, etc.) | Read from `fct_tracking_frames` gold table — automatically pick up new GK values on next run |
