# Kimball PR 7 Hotfix #3 — Mart Kimball-FK resolution + canonical native-id staging

**Status:** Design approved 2026-04-27. Awaiting implementation plan via `superpowers:writing-plans`.

**Author:** Karsten Skyt (with Claude Opus 4.7).

**Branch:** `kimball-pr7-hotfix-3-mart-fk-resolution` (already created from main at squash commit `dba6183` — i.e. post-PR-#216-merge state).

## 1. Context

PR #214 (Kimball PR 7) closed the ADR-011 staged migration by adding Kimball surrogate FK columns (`match_key` / `team_key` / `player_key` and per-mart variants) to every fact mart in the warehouse. Two follow-up hotfixes already shipped after live verification surfaced silent JOIN-cardinality bugs:

- PR #215: `stg_pausa__values` surrogate `pass_id` recipe + Wyscout `player_id=0` upstream filter.
- PR #216: `stg_idsse__passes` `play_team` direct path + `stg_metrica__passes` `Player` prefix strip.

Hotfix #3 closes a third class of latent bugs surfaced during the Phase 2 step 15 contract-threshold calibration: 12 (mart, key) pairs at 100% NULL plus 8 partial-coverage cases. The pattern is consistent across them: the staging layer didn't uniformly canonicalize native ids to the form `dim_*` carries, so fact-side LEFT JOINs to dim tables either fail entirely or fail per-provider.

### Live evidence (pre-fix, post-#216 dev_gold state, 2026-04-27)

Catastrophic 100% NULL:

| mart | key | rows |
|---|---|---|
| `fct_action_values` | team_key | 9,531,948 |
| `fct_action_values` | player_key | 9,531,948 |
| `fct_tracking_frames` | match_key | 38,118,607 |
| `fct_tracking_frames` | team_key | 38,118,607 |
| `fct_tracking_frames` | player_key | 38,118,607 |
| `fct_player_positions` | team_key | 8,521 |
| `fct_position_maps` | team_key | 3,180 |
| `fct_formation_labels` | team_key | 802 |
| `fct_physical_stats` | match_key | 607 |
| `fct_physical_stats` | player_key | 607 |
| `fct_off_ball_xt` | match_key | 616 |
| `fct_off_ball_xt` | player_key | 616 |

Partial coverage:

| mart | key | rate |
|---|---|---|
| `fct_passes.recipient_player_key` | — | 62.77% (Wyscout has no recipient field — true source-data gap) |
| `fct_shots.player_key` | — | 99.998% (3 SB shots without player attribution) |
| `fct_match_summary.home/away_team_key` | — | 99.61% (21 NULLs from IDSSE/Metrica missing home/away resolution) |
| `fct_player_positions` | match_key 68.4%, player_key 84.8% | partial — likely stale-incremental |
| `fct_position_maps` | match_key 67.1%, player_key 84.8% | partial — likely stale-incremental |
| `fct_formation_labels` | match_key 66.8% | partial — likely stale-incremental |

### Diagnosis

Investigation (logs in `bgqww4ko1`, `bqk0ixaxk`, `baks4tmt0`, `bsrxk0hds` background-task outputs from 2026-04-27 session) confirmed:

1. **Stale-incremental:** `fct_action_values`, `fct_tracking_frames`, `fct_physical_stats`, `fct_off_ball_xt` are `materialized='incremental'` with `on_schema_change='append_new_columns'`. PR 7 added new key columns; existing rows kept NULL. Direct JOIN test confirms 100% match if rebuilt from current sources. **Fix: `--full-refresh`, no SQL change.** The "data_source = NULL" downstream effect on `fct_physical_stats` and `fct_off_ball_xt` is auto-fixed once their parent `fct_tracking_frames` rebuilds.

2. **`stg_idsse__tracking` carries `idsse_` prefix on `match_id`:** downstream consumers (`stg_idsse__home_away_teams`, `stg_idsse__passes.ball_at_end_frame`) each apply local `regexp_replace(...)` strip. The mart-side JOIN to `dim_matches` fails because `dim_matches.native_match_id` is the unprefixed form. **Fix: strip at staging boundary; downstream local strips become idempotent no-ops, left in place defensively.**

3. **`stg_metrica__tracking` emits bare `player_id`** (e.g. `'5'`) but `dim_players` synthesises `'metrica_<match>_<side>_5'`. Three different forms across the Metrica pipeline (bronze events: `'Player5'`, bronze tracking JSON keys: `'5'`, dim: `'metrica_<match>_<side>_5'`). **Fix: canonicalize at staging — `concat('metrica_', match_id, '_', team, '_', player_key)` to match dim.**

4. **Formations marts lack `team_id` source:** `fct_player_positions` / `fct_position_maps` / `fct_formation_labels` source data comes from the formations algorithm output which strips team labels. `dim_players` has no `team_key` column (per-player career grain, not per-match). **Fix: introduce shared bridge view `int_tracking__player_match_team_bridge` deriving per-(match, player) → team_id from the three `stg_*_tracking` views.**

5. **`fct_match_summary` IDSSE/Metrica home/away comment is overly conservative:** the SQL claims "cannot be pivoted" but bridges DO exist (`stg_idsse__home_away_teams` for IDSSE; `dim_teams` `metrica_<match>_<side>` rows for Metrica). **Fix: extend home/away resolution to all 4 providers via existing bridges.**

6. **`fct_passes.recipient_player_key` 62.77%** — Wyscout open-data has no recipient field; `kloppy` strips it at parse. True source gap, not a JOIN bug. **Fix: not a fix — calibrate per-provider threshold to 100% on SB only with comment, 0% expected on WS.**

7. **`fct_shots.player_key` 3 NULL rows** — sample query shows 3 SB shots with `player_id` set but no `dim_players` match. Likely 3 historical SB players missing from `dim_players`. **Fix: investigation pass; if confirmed dim-coverage gap, extend dim_players generator to include them.**

## 2. Goals

**Primary**: every Kimball FK column on every fact mart resolves to its dim parent for every provider with the relevant data. No 100%-NULL columns on any post-fix mart. Partial-NULL cases are explicitly modeled as either provider structural gaps (with calibrated per-provider thresholds) or true bugs to fix.

**Secondary**: structural test guards prevent recurrence on PR 8+. Specifically:

- `dbt` `relationships` schema test on every PR-7 Kimball FK column → fails dbt-CI on first build of any future drift.
- Per-(mart, key, provider) parameterized Python test → single-provider drift surfaces against the named provider rather than hiding behind aggregate counts.

**Non-goals**:

- No grain change to `dim_players` (would explode rowcount; out of scope).
- No Python writer changes to formations / off-ball / physical-stats pipelines (out of scope).
- No HF dataset payload republishes (Phase 2 step 17 onward — gated on this hotfix landing).
- No bronze schema changes (the `idsse_` prefix on bronze `match_id` is preserved; only staging strips it).

## 3. Architecture

### 3.1 Single principle (the rule violated by PR 7)

> **The staging layer is where native ids are canonicalized to the form `dim_*` carries.**

After staging, every column whose name matches `*_native_id` or that semantically refers to a native identifier MUST equal the form `dim_*.native_*_id` carries for the same provider. Marts JOIN dim tables on these canonicalized columns and trust them.

PR 7 introduced the dim-side surrogate-key pattern but didn't enforce the staging-side canonicalization uniformly. Hotfix #3 enforces it.

### 3.2 Component changes

#### 3.2.1 Staging — in-place column rewrites (Approach A)

| File | Change | Rationale |
|---|---|---|
| `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` | `match_id` column value: strip `^idsse_` prefix at staging boundary | Canonicalizes to dim_matches form. Downstream `regexp_replace(...)` calls in `stg_idsse__home_away_teams` + `stg_idsse__passes.ball_at_end_frame` become idempotent no-ops; left in place defensively. |
| `dbt_project/models/staging/metrica/stg_metrica__tracking.sql` | `player_id` column value: change from bare key to `concat('metrica_', match_id, '_', team, '_', player_key)` | Canonicalizes to dim_players synth form. The bare key is preserved internally as `player_key` (the lateral-view explode column) — accessible if any future consumer needs the raw map key. |

In-place rather than parallel `native_*` columns because (a) the existing values are already 100%-broken from a JOIN-resolution standpoint — there's no functioning consumer relying on them in the affected marts; (b) Hyrum's-Law applies to *observed* behaviour, and the observed behaviour here is "broken", so the rewrite breaks nobody legitimate; (c) any straggler consumer (e.g. a Taipy query) surfaces in dbt-CI via the live-build full-refresh, allowing fail-fast triage rather than slow-burn drift.

#### 3.2.2 Intermediate — new shared bridge view (Approach X)

New file: `dbt_project/models/intermediate/int_tracking__player_match_team_bridge.sql`.

Materialized as **table** (not view): bridge cardinality on current dev_gold is **616 rows total** (IDSSE 218 + Metrica 89 + SkillCorner 309). The DISTINCT collapse over the underlying ~38M tracking rows is the expensive operation; tabling pays it once at build, then 4 downstream consumer JOINs hit a tiny lookup table. View materialization would force a fresh 38M-row distinct on every consumer JOIN — savings of ~90s per dbt build.

```
{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, player_id, team_id) — the per-match
-- player→team mapping needed by formations marts where the formations
-- algorithm output strips team labels and dim_players doesn't carry team_key.
--
-- Sourced from the three stg_*__tracking views (NOT fct_tracking_frames) to
-- avoid the circular dependency where a formations mart depending on a
-- fact_tracking_frames-derived bridge would force fact_tracking_frames to
-- be built before formations marts. Tracking staging is provider-canonical
-- post-staging-fix (idsse_ prefix stripped, metrica player_id synth form),
-- so the union here works directly.

with idsse_pmt as (
    select distinct
        'idsse'         as source_provider,
        match_id,
        player_id,
        team_id
    from {{ ref('stg_idsse__tracking') }}
    where team_id is not null
      and player_id is not null
),
metrica_pmt as (
    select distinct
        'metrica'       as source_provider,
        match_id,
        player_id,
        team_id
    from {{ ref('stg_metrica__tracking') }}
    where team_id is not null
      and player_id is not null
),
skillcorner_pmt as (
    select distinct
        'skillcorner'   as source_provider,
        match_id,
        player_id,
        team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team_id is not null
      and player_id is not null
)
select * from idsse_pmt
union all select * from metrica_pmt
union all select * from skillcorner_pmt
```

Materialized as view: small (~50K rows expected after distinct collapse); read on demand by formations marts; not worth tabling.

YAML entry in `dbt_project/models/intermediate/_intermediate__models.yml`:

- `not_null` on (source_provider, match_id, player_id, team_id).
- `unique_combination_of_columns` on (source_provider, match_id, player_id) — within a match, a player belongs to one team. (If this fails post-build, surfaces a real data-quality issue worth investigating: a player who switched teams mid-match? data ingestion duplicate? — but expected to pass on current data.)
- `relationships` on `team_id → dim_teams.native_team_id` filtered by source_provider.

Second new file: `dbt_project/models/intermediate/int_tracking__match_side_team_bridge.sql`.

Same structure as the player-match bridge but at per-(match, side='home'/'away') grain. Cardinality on current dev_gold: **40 rows total** (IDSSE 14 + Metrica 6 + SkillCorner 20). Materialized as **table** for the same reason.

```
{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, side='home'/'away', team_id) — the
-- per-match home/away team mapping needed by fct_match_summary IDSSE / Metrica /
-- SkillCorner home/away resolution and fct_formation_labels which is per-(match, side).
--
-- Generalises stg_idsse__home_away_teams across all 3 tracking providers.
-- Sourced from the three stg_*__tracking views (post-staging-canonicalization).

with idsse_mst as (
    select distinct
        'idsse'         as source_provider,
        match_id,
        team            as side,
        team_id
    from {{ ref('stg_idsse__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
metrica_mst as (
    select distinct
        'metrica'       as source_provider,
        match_id,
        team            as side,
        team_id
    from {{ ref('stg_metrica__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
skillcorner_mst as (
    select distinct
        'skillcorner'   as source_provider,
        match_id,
        team            as side,
        team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
)
select * from idsse_mst
union all select * from metrica_mst
union all select * from skillcorner_mst
```

YAML tests on this bridge: `not_null`, `unique_combination_of_columns` on (source_provider, match_id, side), `relationships` on team_id.

`stg_idsse__home_away_teams` is **deleted** in this hotfix (the new bridge subsumes it). Its single consumer is `stg_idsse__passes.ball_at_end_frame` CTE (file `stg_idsse__passes.sql` line 22) — update to read from `int_tracking__match_side_team_bridge` filtered to `source_provider='idsse'`. No backward-compat alias view; long-term-best-practice answer is single source of truth, not redundant mappings.

#### 3.2.3 Marts — formations / position / off-ball using the bridge

| File | Change |
|---|---|
| `fct_player_positions.sql` | Add LEFT JOIN to bridge on `(source_provider, match_id, player_id)` → `team_id`; LEFT JOIN dim_teams on `(provider, native_team_id)` → `team_key`. Replace existing `cast(null as bigint) as team_key` (or whatever placeholder) with the resolved value. |
| `fct_position_maps.sql` | Same. |
| `fct_formation_labels.sql` | Different shape: this mart is per-(match, side='home'/'away') not per-player. Resolve via a new shared `int_tracking__match_side_team_bridge` intermediate view (extends `stg_idsse__home_away_teams` pattern across all 3 tracking providers) — one row per (source_provider, match_id, side) → team_id. Decision: extract as new bridge rather than per-mart inline derivation, parallel to `int_tracking__player_match_team_bridge`. Both bridges share the canonical-derivation principle. |

#### 3.2.4 Mart — fct_match_summary home/away resolution extended

`fct_match_summary.sql` currently has tracking-derived CTEs around lines 65-115 (`match_team_ids` and similar) that pivot team_id from event sources for SB/WS. The mart's comment near line 91 says IDSSE/Metrica "cannot be pivoted home/away" — that's stale.

**Implementation requirement: REPLACE the existing IDSSE/Metrica branches; do NOT add the bridge JOIN alongside.** The mart must have a single resolution path per provider:

- StatsBomb / Wyscout: existing event-team-id pivot (unchanged).
- IDSSE / Metrica / SkillCorner: JOIN `int_tracking__match_side_team_bridge` on (source_provider, match_id, side='home'/'away') → team_id → JOIN `dim_teams` → team_key. **The existing "cannot be pivoted" code path is removed entirely** — no dead code, no double-resolution, no commented-out blocks.

(Note: SkillCorner is tracking-only and may not currently produce match_summary rows since match_summary is event-derived — verify scope during implementation. If SkillCorner has no match_summary rows, this branch is a no-op but kept defensively for forward-compat with any future SkillCorner+events pairing.)

#### 3.2.5 Mart YAML — relationships schema tests

`dbt_project/models/marts/_marts__models.yml` — for every PR-7 Kimball FK column on every affected mart:

```yaml
- name: <key_column>
  data_tests:
    - not_null:
        config:
          where: "<provider-specific predicate, see calibration table>"
    - relationships:
        arguments:
          to: ref('dim_<dim>')
          field: <dim_key>
```

`not_null` is filtered per-provider where some providers have legitimate gaps (e.g. `recipient_player_key` is `not_null` only for SB). `relationships` runs unconditionally (every non-null value must resolve).

#### 3.2.6 Tests — parameterized per-(mart, key, provider)

`src/tests/test_marts_kimball_contracts.py` — replace `0.0` placeholder thresholds with calibrated per-provider thresholds in a parameterized form:

```python
_CASES_PR7 = [
    # (mart, key, provider, expected_min_rate, comment)
    ("fct_passes",            "team_key",            "statsbomb", 1.0, "100% via dim_teams (SB native_team_id is real BIGINT)"),
    ("fct_passes",            "team_key",            "wyscout",   1.0, "100% post player_id=0 filter"),
    ("fct_passes",            "team_key",            "idsse",     1.0, "100% post play_team direct path"),
    ("fct_passes",            "team_key",            "metrica",   1.0, "100% via dim_teams synth"),
    ("fct_passes",            "passer_player_key",   "statsbomb", 1.0, "100% via dim_players"),
    ("fct_passes",            "passer_player_key",   "wyscout",   1.0, "100% post player_id=0 filter"),
    ("fct_passes",            "passer_player_key",   "idsse",     1.0, "100% via dim_players (DFL-OBJ-* native form)"),
    ("fct_passes",            "passer_player_key",   "metrica",   1.0, "100% post Player-prefix strip"),
    ("fct_passes",            "recipient_player_key","statsbomb", 1.0, "100% — recipient field present"),
    ("fct_passes",            "recipient_player_key","wyscout",   0.0, "Wyscout open-data has no recipient field — kloppy strips at parse. Structural source gap."),
    ("fct_passes",            "recipient_player_key","idsse",     0.5, "Best-effort coverage on IDSSE — calibrate post-rebuild and update."),
    ("fct_passes",            "recipient_player_key","metrica",   0.5, "Best-effort — calibrate post-rebuild."),
    ("fct_action_values",     "team_key",            "statsbomb", 1.0, ""),
    ("fct_action_values",     "team_key",            "wyscout",   1.0, ""),
    ("fct_action_values",     "player_key",          "statsbomb", 1.0, ""),
    ("fct_action_values",     "player_key",          "wyscout",   1.0, ""),
    ("fct_tracking_frames",   "match_key",           "idsse",     1.0, "Post idsse_ prefix strip at staging"),
    ("fct_tracking_frames",   "match_key",           "metrica",   1.0, ""),
    ("fct_tracking_frames",   "match_key",           "skillcorner", 1.0, ""),
    ("fct_tracking_frames",   "team_key",            "idsse",     1.0, ""),
    ("fct_tracking_frames",   "team_key",            "metrica",   1.0, ""),
    ("fct_tracking_frames",   "team_key",            "skillcorner", 1.0, ""),
    ("fct_tracking_frames",   "player_key",          "idsse",     1.0, ""),
    ("fct_tracking_frames",   "player_key",          "metrica",   1.0, "Post Player-prefix strip"),
    ("fct_tracking_frames",   "player_key",          "skillcorner", 1.0, ""),
    # ... continuing for all 32 PR-7 (mart, key) pairs across 4 providers ...
]
```

Calibrated post-rebuild on dev_gold; final values committed in the same hotfix-#3 PR.

**`fct_shots.player_key` 3 NULLs** — investigation in implementation phase. Two outcomes:

- If 3 dim_players gaps → extend dim_players generator to include them. Threshold stays 100%.
- If 3 source-data NULLs (player_id missing on event) → relax to `>= 0.99998` with comment, OR filter at staging à la Wyscout `player_id=0`. Decide post-investigation.

#### 3.2.7 Deploy step

Single Databricks `runs/submit` after merge:

```
dbt build --full-refresh --select \
    +int_tracking__player_match_team_bridge+ \
    +int_tracking__match_side_team_bridge+ \
    fct_action_values+ \
    fct_tracking_frames+ \
    fct_match_summary+
```

Selector breakdown:

- `+int_tracking__player_match_team_bridge+`: ancestors (stg_*_tracking) + bridge self + descendants (formations marts).
- `+int_tracking__match_side_team_bridge+`: same shape; covers fct_match_summary's bridge dependency.
- `fct_action_values+`: self + downstream (covers fct_xg_predictions etc. — verify nothing else depends on it).
- `fct_tracking_frames+`: self + downstream (covers fct_physical_stats, fct_off_ball_xt, etc.).
- `fct_match_summary+`: self + downstream.

`fct_passes` is intentionally NOT in the selector — hotfix #2 already rebuilt it correctly with the IDSSE/Metrica fixes; rebuilding again would waste compute.

Wheel `0.3.19` already deployed (PR-#216 wheel cycle); no version bump required since the new bridge models live inside the existing dbt project, no Python change.

Estimated runtime: **~90-180 min** (the descendant closure pulls in ~25-30 marts including fct_player_stats, fct_funnel_stages_agg, fct_vaep_breakdown_agg, fct_gk_actions_detail, fct_goalkeeper_stats, fct_physical_stats, fct_off_ball_xt, fct_player_embeddings_season/_360, fct_player_percentiles, fct_player_positions, fct_position_maps, fct_formation_labels, fct_tracking_avg_positions, fct_tracking_shape_timeline). `fct_tracking_frames` full-refresh on 38M rows with window functions for velocity/acceleration is the single dominant cost (~30-60 min); the rest of the marts add up to another 30-60 min. Previous narrower `+fct_passes+` rebuild was 6 min — this is intentionally wider to cover everything that needs the corrected staging.

If the actual runtime exceeds 180 min, abort and split the selector into two phases: phase 1 = `+int_tracking__player_match_team_bridge+ +int_tracking__match_side_team_bridge+ fct_tracking_frames+` (foundation + tracking descendants), phase 2 = `fct_action_values+ fct_match_summary+` (event-derived descendants). Both phases idempotent under dbt-CI re-runs.

After dbt build:

```
uv run python -m ingestion.refresh_synced_tables --wait
```

Refreshes all affected synced tables (additive auto-evolve per `reference_lakebase_synced_table_auto_evolution.md`).

## 4. Data flow

```
bronze
  ↓                       (no schema change)
staging  ← canonicalize native_id here  (in-place rewrite — stg_idsse__tracking, stg_metrica__tracking)
  ↓
intermediate
  ├── dim_*               (no change — already correct)
  └── int_tracking__player_match_team_bridge  ← NEW bridge view
  ↓
fact mart  ← LEFT JOIN dim_* on canonical native_id; LEFT JOIN bridge for formations marts
  ↓
synced table  ← refresh after dbt build
  ↓
Lakebase / HF dataset republish (Phase 2 step 17 onward)
```

Failure modes guarded:

- Recipe drift between staging and dim → `relationships` schema tests fail in dbt-CI build.
- Single-provider regression → parameterized Python test `[provider]` parameter fails against named provider.
- Stale incremental columns on rebuild miss → wide-selector `--full-refresh` post-merge covers all.
- Bridge cardinality bug (player on multiple teams within same match) → `unique_combination_of_columns` schema test fails.
- New PR 8+ regression of any of the above → same tests catch immediately on build.

## 5. Error handling

`fct_match_summary` IDSSE/Metrica home/away: if a match is missing from the corresponding bridge, the home_team_key / away_team_key lands NULL. Test asserts post-build coverage; investigate any newly-NULL match (likely missing tracking data for that match → dim_teams doesn't have the synth row).

`int_tracking__player_match_team_bridge` empty for a provider: indicates upstream tracking ingestion failure for that provider, OR `team_id IS NULL` filter dropped all rows. Failing `not_null` schema test catches.

## 6. Testing

### Static (always-run, no Databricks env)

- New file existence assertions: `int_tracking__player_match_team_bridge.sql` exists.
- SQL pattern checks: stg_idsse__tracking has `regexp_replace(..., '^idsse_', '')` on match_id; stg_metrica__tracking has `concat('metrica_', match_id, '_', team, '_', ...)` on player_id; bridge view present in fct_player_positions JOINs.

### Live (require DATABRICKS_* env vars)

- Parameterized per-(mart, key, provider) coverage in `test_marts_kimball_contracts.py`. Calibrated thresholds (Section 3.2.6).
- Bridge integrity: `int_tracking__player_match_team_bridge` rowcount > 0 per provider; unique_combination_of_columns passes.
- `fct_match_summary` per-provider home/away coverage: SB/WS/IDSSE/Metrica/SkillCorner all 100%.

### dbt schema tests (run on every dbt-CI build)

- `relationships` on every PR-7 FK column (Section 3.2.5).
- `not_null` filtered per-provider where structural gaps exist.

## 7. Deployment plan summary

1. Branch `kimball-pr7-hotfix-3-mart-fk-resolution` from main.
2. Implement Section 3.2 changes.
3. Local validation: `ruff` + `pyright` + `dbt parse` + `dbt compile` on changed models.
4. Commit + push + open PR.
5. CI validation: `live-build` workflow rebuilds via `dbt build --select state:modified+` against the PR's incremental selector — verifies the affected marts populate correctly.
6. User squash-merge approval.
7. Merge → main-branch Python CI deploys wheel (no version bump unless we add a package, which we don't — new model is dbt-project-internal).
8. Trigger Databricks `runs/submit` with the wide selector full-refresh (Section 3.2.7).
9. Verify post-rebuild: per-(mart, key, provider) coverage all green.
10. Refresh synced tables.
11. Resume Phase 2 step 16 onward.

## 8. Risks

- **`fct_tracking_frames` 38M-row full-refresh runtime**: 30-60 min based on prior `+fct_passes+` rebuild timings. Mitigation: monitor; if exceeds 90 min, abort and split selector.
- **Test calibration drift**: thresholds set today on 7-IDSSE-match / 3-Metrica-match dev sample may not hold on production-scale data. Mitigation: parameterized tests — easy to adjust per-provider when production data lands.
- **Hidden recipe-drift on a partial-coverage mart we haven't probed yet**: `fct_player_positions` match_key 68% / `fct_position_maps` 67% / `fct_formation_labels` 66.8% may have additional bugs beyond stale-incremental. Mitigation: post-rebuild verification step explicitly checks these; if still partial, root-cause and add to the same PR (no follow-up TODO).

## 9. Decisions summary

| Decision | Choice |
|---|---|
| Native-id canonicalization scope | Approach **A** (in-place column rewrite at staging) |
| Formations team_key bridge | Approach **X** (shared `int_tracking__player_match_team_bridge` view) |
| Test calibration shape | Parameterized per-(mart, key, provider) |
| `fct_match_summary` home/away | Extended to all 4 providers (remove "cannot be pivoted" caveat) |
| Deploy strategy | Single Databricks `runs/submit` with wide selector full-refresh |
| `fct_passes.recipient_player_key` Wyscout | Calibrated to 0.0 (structural gap — Wyscout open-data has no recipient field) |
| `fct_shots.player_key` 3 NULLs | Investigate during implementation; fix dim_players if dim gap, otherwise upstream filter |

## 10. References

- ADR-011 (`docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`) — the staged Kimball migration this hotfix closes for real.
- PR #214 squash `cfb570a` — original PR 7 commit.
- PR #215 squash `a7dc653` — pausa surrogate JOIN + Wyscout `player_id=0` filter.
- PR #216 squash `dba6183` — IDSSE play_team direct path + Metrica Player-prefix strip on `stg_metrica__passes`.
- Memory `feedback_no_pr_decomposition_proposals` — single branch, single PR.
- Memory `feedback_lead_with_best_practice` — fail-fast staging rebuild.
- Memory `reference_lakebase_synced_table_auto_evolution` — additive auto-evolve on refresh.
