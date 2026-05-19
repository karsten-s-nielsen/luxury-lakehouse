# TC-2: `is_goalkeeper` Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace three provider-specific GK heuristics in `fct_tracking_frames` with a single source of truth derived from the tracking_context staging layer via silly-kicks `derive_goalkeepers()`.

**Architecture:** New ephemeral dbt intermediate model `int_tracking_goalkeepers` extracts GK `(match_key, player_key)` pairs from `stg_spadl__tracking_context`. `fct_tracking_frames` drops per-provider `is_goalkeeper` from its staging union and derives it via a LEFT JOIN to the intermediate. Three singular tests guard correctness.

**Tech Stack:** dbt (SQL only — no Python changes)

**Spec:** `docs/superpowers/specs/2026-05-18-tc2-is-goalkeeper-migration-design.md`

---

### Task 1: Create `int_tracking_goalkeepers` ephemeral model

**Files:**
- Create: `dbt_project/models/intermediate/int_tracking_goalkeepers.sql`

- [ ] **Step 1: Create the ephemeral model**

```sql
-- int_tracking_goalkeepers.sql
-- Extracts distinct GK player identities per match from tracking_context staging.
-- Single source of truth for GK identification across all tracking providers,
-- powered by silly-kicks derive_goalkeepers() 3-tier identification.
--
-- Grain: one row per (match_key, player_key) for each GK in a match.
-- Expected: ~2 rows/match (IDSSE/SkillCorner), 1 row/match (Metrica — home only).
-- GK substitution matches may have 3 rows.
--
-- Uses INNER JOINs for dimension resolution — a valid GK native ID with no
-- dim_players entry is silently dropped. warn_unresolved_gk_player_ids.sql
-- guards against this.

with gks as (

    select distinct
        data_source,
        native_match_id,
        defending_gk_player_id_native as player_id_native
    from {{ ref('stg_spadl__tracking_context') }}
    where defending_gk_player_id_native is not null

)

select
    dm.match_key,
    dp.player_key
from gks
inner join {{ ref('dim_matches') }} dm
    on  dm.provider = gks.data_source
   and dm.native_match_id = gks.native_match_id
inner join {{ ref('dim_players') }} dp
    on  dp.provider = gks.data_source
   and dp.native_player_id = gks.player_id_native
```

Write this file to `dbt_project/models/intermediate/int_tracking_goalkeepers.sql`.

The `dbt_project.yml` already sets `intermediate: +materialized: ephemeral` (line 44), so no config block needed.

- [ ] **Step 2: Verify dbt can parse the new model**

Run:
```bash
cd dbt_project && uv run dbt compile --select int_tracking_goalkeepers
```

Expected: compilation succeeds, no errors. The compiled SQL shows the CTE with the JOINs to `dim_matches` and `dim_players`.

- [ ] **Step 3: Commit**

```bash
git add dbt_project/models/intermediate/int_tracking_goalkeepers.sql
git commit -m "feat(tc2): add int_tracking_goalkeepers ephemeral model

Extracts GK player (match_key, player_key) pairs from
stg_spadl__tracking_context. Single source of truth for
is_goalkeeper across all tracking providers.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Modify `fct_tracking_frames.sql` to use the intermediate

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`

- [ ] **Step 1: Remove `is_goalkeeper` from the `tracking` CTE**

In `fct_tracking_frames.sql`, the `tracking` CTE (lines 38-71) unions three staging models. Each SELECT lists 15 columns including `is_goalkeeper`. Remove `is_goalkeeper` from all three SELECTs, reducing to 14 columns.

Change lines 45-48 from:
```sql
    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider, is_goalkeeper,
        x, y, ball_x, ball_y
    from {{ ref('stg_metrica__tracking') }}
```
to:
```sql
    select
        tracking_id, match_id, period, frame, timestamp_seconds,
        frame_rate, player_id, team, team_id, source_provider,
        x, y, ball_x, ball_y
    from {{ ref('stg_metrica__tracking') }}
```

Apply the same change to the `stg_idsse__tracking` SELECT (lines 54-57) and the `stg_skillcorner__tracking` SELECT (lines 63-66). Remove `is_goalkeeper,` from each.

Update the comment at line 42-44 to note the column count change:
```sql
    -- PR 7 (ADR-011): tracking staging now surfaces team_id per Q1 (IDSSE
    -- real DFL TeamId from PR 5a; Metrica synthesized via dim_teams pattern;
    -- SkillCorner via home_team_id/away_team_id CASE). The 14-column shared
    -- schema excludes is_goalkeeper — TC-2 derives it from
    -- int_tracking_goalkeepers (silly-kicks derive_goalkeepers() via TC-1).
```

- [ ] **Step 2: Add GK JOIN and replace `is_goalkeeper` in the `final` CTE**

In the `final` CTE (lines 132-192), add the LEFT JOIN to `int_tracking_goalkeepers` after the existing `dim_players` join (line 190):

```sql
    left join {{ ref('dim_players') }} dp
        on  dp.provider = wl.source_provider
       and dp.native_player_id = cast(wl.player_id as string)
    left join {{ ref('int_tracking_goalkeepers') }} gk
        on  gk.match_key = dm.match_key
       and gk.player_key = dp.player_key
```

Replace line 148:
```sql
        wl.is_goalkeeper,
```
with:
```sql
        gk.player_key is not null                          as is_goalkeeper,
```

- [ ] **Step 3: Verify dbt can compile the modified mart**

Run:
```bash
cd dbt_project && uv run dbt compile --select fct_tracking_frames
```

Expected: compilation succeeds. The compiled SQL shows the `int_tracking_goalkeepers` CTE inlined (ephemeral materialization) and the LEFT JOIN in the `final` CTE.

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/marts/fct_tracking_frames.sql
git commit -m "feat(tc2): derive is_goalkeeper from int_tracking_goalkeepers

Drop per-provider GK heuristics (jersey-#1, position_name) from
the tracking CTE. Derive is_goalkeeper via LEFT JOIN to
int_tracking_goalkeepers in the final CTE. Fixes Metrica
(wrong on 6/6 matches) and SkillCorner (6/10 had has_gk=False).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Create singular tests

**Files:**
- Create: `dbt_project/tests/assert_tracking_frames_gk_count_by_provider.sql`
- Create: `dbt_project/tests/assert_idsse_gk_parity.sql`
- Create: `dbt_project/tests/assert_unresolved_gk_player_ids.sql`

- [ ] **Step 1: Create the GK count per provider test**

```sql
-- assert_tracking_frames_gk_count_by_provider.sql
-- TC-2: Every match must have at least 2 GKs for IDSSE/SkillCorner,
-- and at least 1 for any provider. Metrica is allowed 1 due to
-- de-identified data (home GK only).
--
-- Counts GKs without pre-filtering — a match where is_goalkeeper = false
-- for ALL players still produces a group with n_gks = 0.
-- Returns rows that FAIL the expectation — 0 rows = all pass.

with match_gk_counts as (

    select
        match_key,
        data_source,
        count(distinct case when is_goalkeeper then player_key end) as n_gks
    from {{ ref('fct_tracking_frames') }}
    group by match_key, data_source

)

select *
from match_gk_counts
where
    (data_source in ('idsse', 'skillcorner') and n_gks < 2)
    or n_gks = 0
```

Write to `dbt_project/tests/assert_tracking_frames_gk_count_by_provider.sql`.

- [ ] **Step 2: Create the IDSSE GK parity test**

```sql
-- assert_idsse_gk_parity.sql
-- TC-2: The set of GK (match_key, player_key) pairs identified by
-- int_tracking_goalkeepers must match the set identified by
-- stg_idsse__tracking's per-frame is_goalkeeper flag.
-- A non-empty result indicates the GK substitution temporal
-- regression (spec §4 H1) is material for this match set.

with from_intermediate as (

    select distinct gk.match_key, gk.player_key
    from {{ ref('int_tracking_goalkeepers') }} gk
    inner join {{ ref('dim_matches') }} dm
        on dm.match_key = gk.match_key
    where dm.provider = 'idsse'

),

from_staging as (

    select distinct dm.match_key, dp.player_key
    from {{ ref('stg_idsse__tracking') }} st
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = 'idsse'
       and dm.native_match_id = cast(st.match_id as string)
    inner join {{ ref('dim_players') }} dp
        on  dp.provider = 'idsse'
       and dp.native_player_id = cast(st.player_id as string)
    where st.is_goalkeeper = true

)

-- Symmetric difference: rows in one set but not the other.
select match_key, player_key, 'intermediate_only' as source
from from_intermediate
except
select match_key, player_key, 'intermediate_only'
from from_staging

union all

select match_key, player_key, 'staging_only' as source
from from_staging
except
select match_key, player_key, 'staging_only'
from from_intermediate
```

Write to `dbt_project/tests/assert_idsse_gk_parity.sql`.

- [ ] **Step 3: Create the unresolved GK player IDs warning test**

```sql
-- assert_unresolved_gk_player_ids.sql
-- TC-2: Guards against silent drops from the INNER JOIN in
-- int_tracking_goalkeepers. Any defending_gk_player_id_native
-- that cannot resolve to a dim_players entry is flagged.
{{ config(severity='warn') }}

select distinct
    tc.data_source,
    tc.defending_gk_player_id_native
from {{ ref('stg_spadl__tracking_context') }} tc
left join {{ ref('dim_players') }} dp
    on  dp.provider = tc.data_source
   and dp.native_player_id = tc.defending_gk_player_id_native
where tc.defending_gk_player_id_native is not null
  and dp.player_key is null
```

Write to `dbt_project/tests/assert_unresolved_gk_player_ids.sql`.

- [ ] **Step 4: Verify all three tests compile**

Run:
```bash
cd dbt_project && uv run dbt compile --select test_type:singular
```

Expected: all three new tests appear in compiled output without errors.

- [ ] **Step 5: Commit**

```bash
git add dbt_project/tests/assert_tracking_frames_gk_count_by_provider.sql dbt_project/tests/assert_idsse_gk_parity.sql dbt_project/tests/assert_unresolved_gk_player_ids.sql
git commit -m "test(tc2): add GK count, IDSSE parity, and unresolved ID tests

Three singular tests:
- assert_tracking_frames_gk_count_by_provider: 2 GKs for
  IDSSE/SkillCorner, >=1 for Metrica
- assert_idsse_gk_parity: intermediate vs staging GK set match
- assert_unresolved_gk_player_ids: warn on dim_players gaps

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: Final validation

- [ ] **Step 1: Full dbt compile to validate the model graph**

Run:
```bash
cd dbt_project && uv run dbt compile --select fct_tracking_frames+
```

Expected: `fct_tracking_frames`, `fct_physical_stats`, `fct_tracking_avg_positions`, `fct_tracking_shape_timeline` all compile without errors. The `int_tracking_goalkeepers` CTE is inlined into `fct_tracking_frames`.

- [ ] **Step 2: Verify no ruff/pyright regressions (no Python changes, but confirm)**

Run:
```bash
uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/
```

Expected: 0 violations (no Python files changed, just confirming clean baseline).

- [ ] **Step 3: Squash commits into one**

Per project convention (single commit on feature branches), squash the 3 task commits into one:

```bash
git reset --soft HEAD~2 && git commit --amend -m "$(cat <<'EOF'
feat(tc2): derive is_goalkeeper from tracking_context staging

Replace three per-provider GK heuristics in fct_tracking_frames
with a single source of truth: int_tracking_goalkeepers (ephemeral),
which extracts GK (match_key, player_key) pairs from
stg_spadl__tracking_context (silly-kicks derive_goalkeepers()).

Fixes Metrica (jersey-#1 wrong on 6/6 matches) and SkillCorner
(6/10 old matches had has_gk=False). IDSSE parity preserved.

Three singular tests: GK count per provider, IDSSE staging parity,
unresolved GK player ID warning.

Post-merge: dbt run --full-refresh --select fct_tracking_frames+

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Post-Merge Runbook (not part of the implementation — executed after PR merges)

1. **Full-refresh on Databricks:**
   ```
   dbt run --full-refresh --select fct_tracking_frames+
   ```
   Cascades to: `fct_physical_stats`, `fct_tracking_avg_positions`, `fct_tracking_shape_timeline`.

2. **Run singular tests on live data:**
   ```
   dbt test --select test_type:singular
   ```
   Verify all three new tests pass.

3. **Synced table refresh:** Either wait for the daily `lakebase-grants.yml` run or manually refresh `fct_tracking_frames_synced` via `uv run --extra sdk python scripts/maintain_synced_tables.py --skip-refresh` + Databricks UI refresh.
