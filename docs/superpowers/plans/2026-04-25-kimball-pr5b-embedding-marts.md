# Kimball PR 5b — Player-side embedding mart migrations Implementation Plan

> **For agentic workers:** This plan is executed inline (per `feedback_no_approval_asks_in_plan_execution` and `feedback_agent_tool_requires_per_call_approval`) — no subagent dispatch. Use checkbox (`- [ ]`) syntax for tracking. Single commit at end of plan per `feedback_single_commit_squash` (do NOT commit per task).

**Goal:** Add `player_key BIGINT` Kimball surrogate to six embedding/percentile marts; add `match_key BIGINT` to `fct_player_embeddings` so the dim_matches bridge that PR 5a's CI-triage added to `fct_player_embeddings_season` + `_season_360` can be retired; add Taipy consumer dual-read plumbing; document the 2026-07-22 dual-column window on five HF dataset cards.

**Architecture:** Additive contract changes only. Legacy `canonical_player_id` STAYS verbatim everywhere (Hyrum's Law — 57-file consumer cascade). Surrogate joins use the existing `generate_player_key` macro (PR 5a). HF dataset payloads are NOT modified — only cards. Synced tables auto-evolve on refresh (no PK changes). Single commit per branch.

**Tech Stack:** dbt 1.10–1.12 / Databricks Spark SQL, pyright, ruff, pytest, Taipy, HuggingFace Hub.

**Spec source:** `docs/superpowers/specs/2026-04-24-kimball-pr5-design.md` §2 "In scope — PR 5b" (lines 204–234), §8 ship criteria (lines 417–425), §9 risks (lines 432–434).

---

## File map

**Modify (dbt marts):**
- `dbt_project/models/marts/fct_player_embeddings.sql` — add `match_key`, `player_key`
- `dbt_project/models/marts/fct_player_embeddings_season.sql` — add `player_key`, retire dim_matches bridge
- `dbt_project/models/marts/fct_player_embeddings_career.sql` — add `player_key`
- `dbt_project/models/marts/fct_player_embeddings_season_360.sql` — add `player_key`, retire dim_matches bridge
- `dbt_project/models/marts/fct_player_embeddings_career_360.sql` — add `player_key`
- `dbt_project/models/marts/fct_player_percentiles.sql` — add `player_key` (pulled from `fct_player_stats`)
- `dbt_project/models/marts/_marts__models.yml` — six contracts updated (new column + warn-severity relationship)

**Modify (Taipy consumers):**
- `hf_taipy_app/src/state/shared.py` — add `resolve_player_identity()` helper + `_player_identity_map`
- `hf_taipy_app/src/queries/players.py` — accept optional `player_key` parameter (no behavior change)
- `hf_taipy_app/src/queries/tracking.py` — same
- `hf_taipy_app/src/state/player_similarity.py` — populate the new identity map alongside the existing `_ps_player_map`

**Modify (HF dataset cards):**
- `docs/huggingface/dataset-cards/football2vec-player-embeddings.md`
- `docs/huggingface/dataset-cards/football2vec-360-embeddings.md`
- `docs/huggingface/dataset-cards/football2vec-training-data.md`
- `docs/huggingface/dataset-cards/football2vec-360-training-data.md`
- `docs/huggingface/dataset-cards/football2vec-statsbomb-wyscout.md`

**Tests (new + extended):**
- `src/tests/test_marts_player_key_contracts.py` — invariants on the six marts after build (new)

**Out of scope (per spec line 225):** training/export scripts (`scripts/train_football2vec_*`, `src/ingestion/export_*`, `src/ingestion/player_embeddings_*`) untouched. HF dataset *payloads* untouched. PR 8 batches those.

---

## Task 1: Update `fct_player_embeddings` — add `match_key` + `player_key`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings.sql`

**Why two columns:** `match_key` retires the dim_matches bridge in `_season` + `_season_360`. `player_key` is the headline addition. Both resolve via LEFT JOIN against existing dims so existing rows survive (no INNER-JOIN drops).

- [ ] **Step 1: Replace mart with dual-key version**

```sql
-- fct_player_embeddings.sql
-- Per-match player embedding vectors for similarity search.
--
-- Dual-vector design:
--   - behavioral_vector (32-dim): football2vec Doc2Vec embedding capturing
--     playing style from event sequences (action type + pitch grid location tokens)
--   - stat_vector (13-dim): z-score normalized per-90 statistics capturing
--     output metrics
--
-- Grain: one row per player per match per data_source.
--
-- PR 5b (ADR-011) Kimball surrogate keys:
--   - player_key BIGINT FK → dim_players.player_key. Resolved via LEFT JOIN
--     on canonical_player_id (the legacy hash preserved by dim_players for
--     Hyrum's Law). LEFT JOIN, not INNER, so zero-row Metrica + offline
--     embedding rows survive.
--   - match_key BIGINT FK → dim_matches.match_key. LEFT JOIN on
--     (provider='statsbomb', try_cast(native_match_id as bigint) = match_id).
--     Retires the dim_matches bridge that PR 5a's CI-triage added to
--     fct_player_embeddings_season + _season_360.
--
-- Downstream: fct_player_embeddings_season and _career aggregate via
-- element-wise mean.

{{ config(
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge'
) }}

-- D62 (2026-04-15) introduced a 360-embeddings variant alongside the
-- original Doc2Vec (v2) embeddings, so stg_player_embeddings now partitions
-- dedup by (canonical_player_id, match_id, data_source) — both can coexist
-- for the same (player, match). This mart's surrogate must include
-- data_source too; otherwise two source rows collapse to one embedding_id
-- and the incremental MERGE aborts with DELTA_MULTIPLE_SOURCE_ROW_MATCHING.
select
    {{ dbt_utils.generate_surrogate_key(['e.canonical_player_id', 'e.match_id', 'e.data_source']) }} as embedding_id,
    e.canonical_player_id,
    dp.player_key,
    e.match_id,
    dm.match_key,
    e.data_source,
    e.behavioral_vector,
    e.stat_vector
from {{ ref('stg_player_embeddings') }} e
left join {{ ref('dim_players') }} dp
    on dp.canonical_player_id = e.canonical_player_id
left join {{ ref('dim_matches') }} dm
    on dm.provider = 'statsbomb'
   and try_cast(dm.native_match_id as bigint) = e.match_id
```

---

## Task 2: Update `fct_player_embeddings_season` — drop dim_matches bridge, add `player_key`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql`

**Bridge retirement:** the inner join chain `e → dim_matches → fct_match_summary` collapses to `e → fct_match_summary` because `fct_player_embeddings.match_key` is now populated by Task 1.

- [ ] **Step 1: Replace `embeddings_with_context` CTE + add player_key passthrough**

```sql
-- fct_player_embeddings_season.sql
-- Per player-competition-season aggregation of embedding vectors.
--
-- Joins match-level embeddings with fct_match_summary to derive
-- competition and season context. Both behavioral and stat vectors
-- are aggregated via element-wise mean. NULL stat_vectors are excluded
-- from the stat mean.
--
-- Grain: one row per player per competition per season.
--
-- PR 5b (ADR-011): retired the dim_matches bridge that PR 5a's CI-triage
-- added; fct_player_embeddings now carries match_key directly. Added
-- player_key passthrough.

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    -- D62 2026-04-15: explicitly exclude 360-enriched rows (144d) so they
    -- do not promote over v2's 128d embeddings. The 360 aggregates live
    -- in fct_player_embeddings_season_360 / _career_360 with their own
    -- dimensionally-homogeneous aggregation.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
    group by canonical_player_id
),

embeddings_with_context as (

    select
        e.canonical_player_id,
        e.player_key,
        e.match_id,
        e.match_key,
        e.data_source,
        e.behavioral_vector,
        e.stat_vector,
        m.competition_id,
        m.season_id
    from {{ ref('fct_player_embeddings') }} e
    inner join {{ ref('fct_match_summary') }} m
        on m.match_key = e.match_key
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    -- D62 2026-04-15: 360-enriched embeddings live in their own mart; exclude here.
    where e.data_source != 'football2vec_360'

),

grouped as (

    select
        canonical_player_id,
        any_value(player_key)                                 as player_key,
        competition_id,
        season_id,
        collect_list(behavioral_vector)                       as behavioral_vectors,
        filter(collect_list(stat_vector), v -> v is not null) as non_null_stat_vectors,
        count(*)                                              as matches_in_sample,
        collect_set(data_source)                              as data_sources
    from embeddings_with_context
    group by canonical_player_id, competition_id, season_id

)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'competition_id', 'season_id']) }}
        as embedding_season_id,
    canonical_player_id,
    player_key,
    competition_id,
    season_id,
    -- Element-wise mean of behavioral vectors (dimension derived from data)
    transform(
        sequence(0, size(behavioral_vectors[0]) - 1),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, vec) -> acc + vec[i],
            acc -> acc / size(behavioral_vectors)
        )
    ) as behavioral_vector,
    -- Element-wise mean of stat vectors (pre-filtered NULLs in CTE)
    case
        when size(non_null_stat_vectors) > 0
        then transform(
            sequence(0, 12),
            i -> aggregate(
                non_null_stat_vectors,
                cast(0.0 as double),
                (acc, vec) -> acc + coalesce(vec[i], 0.0),
                acc -> acc / size(non_null_stat_vectors)
            )
        )
        else null
    end as stat_vector,
    matches_in_sample,
    data_sources
from grouped
```

---

## Task 3: Update `fct_player_embeddings_career` — add `player_key`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql`

- [ ] **Step 1: Add player_key passthrough in the grouped CTE + final select**

```sql
-- fct_player_embeddings_career.sql
-- Career-level aggregation of player embedding vectors.
--
-- Aggregates all match-level embeddings across competitions and seasons
-- into a single career embedding per player. Both behavioral and stat
-- vectors are averaged element-wise. NULL stat_vectors are excluded
-- from the stat mean.
--
-- Grain: one row per player (canonical_player_id).
--
-- PR 5b (ADR-011): added player_key passthrough.

{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    -- D62 2026-04-15: explicitly exclude 360-enriched rows (144d) so they
    -- do not promote over v2's 128d embeddings. The 360 aggregates live
    -- in fct_player_embeddings_season_360 / _career_360 with their own
    -- dimensionally-homogeneous aggregation.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
    group by canonical_player_id
),

grouped as (

    select
        e.canonical_player_id,
        any_value(e.player_key)                                 as player_key,
        collect_list(e.behavioral_vector)                       as behavioral_vectors,
        filter(collect_list(e.stat_vector), v -> v is not null) as non_null_stat_vectors,
        count(*)                                                as total_matches,
        collect_set(e.data_source)                              as data_sources
    from {{ ref('fct_player_embeddings') }} e
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    -- D62 2026-04-15: 360-enriched embeddings live in their own mart; exclude here.
    where e.data_source != 'football2vec_360'
    group by e.canonical_player_id

)

select
    canonical_player_id,
    player_key,
    -- Element-wise mean of behavioral vectors (dimension derived from data)
    transform(
        sequence(0, size(behavioral_vectors[0]) - 1),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, vec) -> acc + vec[i],
            acc -> acc / size(behavioral_vectors)
        )
    ) as behavioral_vector,
    -- Element-wise mean of stat vectors (pre-filtered NULLs in CTE)
    case
        when size(non_null_stat_vectors) > 0
        then transform(
            sequence(0, 12),
            i -> aggregate(
                non_null_stat_vectors,
                cast(0.0 as double),
                (acc, vec) -> acc + coalesce(vec[i], 0.0),
                acc -> acc / size(non_null_stat_vectors)
            )
        )
        else null
    end as stat_vector,
    total_matches,
    data_sources
from grouped
```

---

## Task 4: Update `fct_player_embeddings_season_360` — drop bridge, add `player_key`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_season_360.sql`

- [ ] **Step 1: Replace match_context CTE + final select with player_key**

```sql
{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

-- PR 5b (ADR-011): retired dim_matches bridge (fct_player_embeddings now
-- carries match_key directly) and added player_key passthrough.

with match_context as (
    select
        pe.canonical_player_id,
        pe.player_key,
        pe.match_id,
        pe.match_key,
        pe.behavioral_vector,
        pe.data_source,
        ms.competition_id,
        ms.season_id
    from {{ ref('fct_player_embeddings') }} pe
    inner join {{ ref('fct_match_summary') }} ms
        on ms.match_key = pe.match_key
    where pe.data_source = 'football2vec_360'
),

grouped as (
    select
        canonical_player_id,
        any_value(player_key) as player_key,
        competition_id,
        season_id,
        collect_list(behavioral_vector) as behavioral_vectors,
        count(*) as total_matches,
        collect_set(data_source) as data_sources
    from match_context
    group by canonical_player_id, competition_id, season_id
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id', 'competition_id', 'season_id']) }} as embedding_season_360_id,
    canonical_player_id,
    player_key,
    cast(competition_id as bigint) as competition_id,
    cast(season_id as bigint) as season_id,
    transform(
        sequence(0, 143),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, arr) -> acc + arr[i]
        ) / size(behavioral_vectors)
    ) as behavioral_vector,
    total_matches,
    data_sources
from grouped
```

---

## Task 5: Update `fct_player_embeddings_career_360` — add `player_key`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings_career_360.sql`

- [ ] **Step 1: Add player_key passthrough**

```sql
{{ config(
    materialized='table',
    enabled=var('embeddings_enabled', false)
) }}

-- PR 5b (ADR-011): added player_key passthrough.

with grouped as (
    select
        canonical_player_id,
        any_value(player_key) as player_key,
        collect_list(behavioral_vector) as behavioral_vectors,
        count(*) as total_matches,
        collect_set(data_source) as data_sources
    from {{ ref('fct_player_embeddings') }}
    where data_source = 'football2vec_360'
    group by canonical_player_id
)

select
    {{ dbt_utils.generate_surrogate_key(['canonical_player_id']) }} as embedding_career_360_id,
    canonical_player_id,
    player_key,
    transform(
        sequence(0, 143),
        i -> aggregate(
            behavioral_vectors,
            cast(0.0 as double),
            (acc, arr) -> acc + arr[i]
        ) / size(behavioral_vectors)
    ) as behavioral_vector,
    total_matches,
    data_sources
from grouped
```

---

## Task 6: Update `fct_player_percentiles` — add `player_key` from `fct_player_stats`

**Files:**
- Modify: `dbt_project/models/marts/fct_player_percentiles.sql`

**Why this path:** `fct_player_stats` already has `player_key` (PR 5a, INNER JOIN to dim_players). Pulling through is simpler + consistent with the upstream Kimball lineage. No new dim_players join here.

- [ ] **Step 1: Propagate player_key through CTEs and final select**

Edit replacements (preserve everything else verbatim):

(a) `player_stats` CTE — add `ps.player_key` after `ps.player_id`:

```sql
player_stats as (

    select
        cast(ps.player_id as string) as player_id,
        ps.player_key,
        ps.competition_id,
        ps.season_id,
        pn.player_display_name,
        pn.position_group,
        ps.minutes_played,
        ps.xg_per_90,
        ps.goals_per_90,
        ps.passes_per_90,
        ps.progressive_passes_per_90,
        ps.pass_completion_pct,
        ps.vaep_per_90,
        ps.offensive_vaep_per_90,
        ps.defensive_vaep_per_90,
        ps.line_breaking_per_90
        {% if var('defcon_enabled', false) %}
        , ps.defcon_per_90
        {% else %}
        , cast(null as double) as defcon_per_90
        {% endif %}
    from {{ ref('fct_player_stats') }} ps
    left join player_names pn
        on cast(ps.player_id as string) = pn.player_id
        and pn._rn = 1
    where ps.competition_id is not null
      and ps.season_id is not null
      and pn.position_group is not null

),
```

(b) `enriched` CTE — add `s.player_key` after `s.player_id`:

```sql
enriched as (

    select
        s.player_id,
        s.player_key,
        s.competition_id,
        s.season_id,
        s.player_display_name,
        s.position_group,
        s.minutes_played,

        -- Core per-90 metrics
        s.xg_per_90,
        ...
```

(c) `percentiled` CTE — add `player_key` after `player_id`:

```sql
percentiled as (

    select
        player_id,
        player_key,
        competition_id,
        season_id,
        player_display_name,
        position_group,
        minutes_played,
        ...
```

(d) Final select — add `cast(player_key as bigint) as player_key,` after the `player_id` cast:

```sql
select
    cast(player_id as string)          as player_id,
    cast(player_key as bigint)         as player_key,
    cast(competition_id as int)        as competition_id,
    ...
```

---

## Task 7: Update `_marts__models.yml` — six contracts

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

Each entry gets a new `player_key` column with a warn-severity `relationships` test to `dim_players(player_key)` for the 90-day dual-column window. `fct_player_embeddings` also gets `match_key`. Existing `canonical_player_id` columns stay (legacy preserved).

- [ ] **Step 1: Add `match_key` + `player_key` to `fct_player_embeddings` (after `canonical_player_id` block, before `match_id`)**

Insert after the existing `canonical_player_id` block (around line 1116):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). LEFT JOIN on
          canonical_player_id; nullable for upstream rows whose dim_players
          row hasn't materialised yet. Coexists with canonical_player_id
          during the 2026-07-22 dual-column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
      - name: match_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_matches (PR 5b). LEFT JOIN on
          (provider='statsbomb', native_match_id). Nullable for matches
          not yet in dim_matches. Retires the PR 5a CI-triage dim_matches
          bridge in fct_player_embeddings_season + _season_360.
        data_tests:
          - relationships:
              to: ref('dim_matches')
              field: match_key
              config:
                severity: warn
```

- [ ] **Step 2: Add `player_key` to `fct_player_embeddings_season` (after `canonical_player_id` block)**

Insert after the `canonical_player_id` block (around line 1171):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). Passed through from
          fct_player_embeddings via any_value() in the season aggregate.
          Coexists with canonical_player_id during the 2026-07-22 dual-
          column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

- [ ] **Step 3: Add `player_key` to `fct_player_embeddings_career` (after `canonical_player_id` block)**

Insert after the `canonical_player_id` block (around line 1230):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). Passed through from
          fct_player_embeddings via any_value() in the career aggregate.
          Coexists with canonical_player_id during the 2026-07-22 dual-
          column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

- [ ] **Step 4: Add `player_key` to `fct_player_embeddings_career_360` (after `canonical_player_id` block)**

Insert after the `canonical_player_id` block (around line 3038):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). Passed through from
          fct_player_embeddings via any_value() in the 360 career aggregate.
          Coexists with canonical_player_id during the 2026-07-22 dual-
          column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

- [ ] **Step 5: Add `player_key` to `fct_player_embeddings_season_360` (after `canonical_player_id` block)**

Insert after the `canonical_player_id` block (around line 3087):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). Passed through from
          fct_player_embeddings via any_value() in the 360 season aggregate.
          Coexists with canonical_player_id during the 2026-07-22 dual-
          column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

- [ ] **Step 6: Add `player_key` to `fct_player_percentiles` (after `player_id` block)**

Insert after the `player_id` block (around line 2204):

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 5b). Pulled through from
          fct_player_stats which carries player_key via INNER JOIN to
          dim_players (PR 5a). Coexists with player_id during the
          2026-07-22 dual-column window.
        data_tests:
          - relationships:
              to: ref('dim_players')
              field: player_key
              config:
                severity: warn
```

---

## Task 8: Add `resolve_player_identity()` helper to `state/shared.py`

**Files:**
- Modify: `hf_taipy_app/src/state/shared.py`

**Behavior:** the helper returns a `(canonical_player_id, player_key)` tuple for a given player label. Reads via `_player_identity_map` populated lazily on first access. Forward-compat plumbing — query filters stay on `canonical_player_id` during PR 5b.

- [ ] **Step 1: Add the new identity map + helper near the existing `_player_map` block**

After `_player_map: dict[str, int] = {}` (around line 146), add:

```python
# PR 5b (ADR-011): player_key Kimball surrogate plumbing for the 2026-07-22
# dual-column window. _player_identity_map maps player label →
# (canonical_player_id, player_key). Populated lazily per player on first
# resolve_player_identity() call to avoid the up-front 15K-row dim_players
# scan that on_competition_change would otherwise need.
_player_identity_map: dict[str, tuple[str, int]] = {}
```

- [ ] **Step 2: Add the resolver function after `get_player_id` (around line 300)**

```python
def resolve_player_identity(label: str | None) -> tuple[str, int] | None:
    """Resolve player label to (canonical_player_id, player_key) for the
    2026-07-22 dual-column window (PR 5b, ADR-011).

    Forward-compat plumbing: callers that already know the legacy player_id
    INT can still use ``get_player_id``; this resolver exists so the same
    label can yield BOTH canonical_player_id (legacy) and player_key
    (Kimball BIGINT) without a second cascade roundtrip after PR 8 drops
    canonical_player_id from dataset payloads.

    Returns None for 'All', empty, or unknown labels. Cached per-process
    in ``_player_identity_map`` after first lookup.
    """
    if not label or label == _ALL_LABEL:
        return None
    cached = _player_identity_map.get(label)
    if cached is not None:
        return cached
    pid = _player_map.get(label)
    if pid is None:
        return None
    # Lazy single-row lookup — keeps on_competition_change cheap.
    from queries.common import execute_query, t

    dim_tbl = t("dim_players_synced")
    df = execute_query(
        f"SELECT canonical_player_id, player_key "  # noqa: S608
        f"FROM {dim_tbl} WHERE player_id = %s LIMIT 1",
        (int(pid),),
    )
    if df.empty:
        return None
    canonical = str(df.iloc[0]["canonical_player_id"])
    pkey = int(df.iloc[0]["player_key"])
    _player_identity_map[label] = (canonical, pkey)
    return (canonical, pkey)
```

- [ ] **Step 3: Export the helper in `__all__`**

Find the `__all__` list (around line 78) and add `"resolve_player_identity"` to the function-export section (just after `"get_player_id"` if alphabetical or end of callbacks group is fine). Concretely, insert before `"on_init"`:

```python
    "resolve_player_identity",
```

- [ ] **Step 4: Reset the cache on competition change**

In `on_competition_change` (around line 342), add `_player_identity_map` to the `global` declaration line:

```python
    global _team_map, _match_map, _player_map, _player_identity_map
```

And after the existing reset block (just below the `state.player_search_query = ""` line near line 372), add:

```python
    _player_identity_map = {}
```

Same for `on_team_change` (around line 401) — add `_player_identity_map` to its `global` line and reset to `{}` after the existing resets.

---

## Task 9: Add `player_key` parameter to embedding queries in `queries/players.py`

**Files:**
- Modify: `hf_taipy_app/src/queries/players.py`

**Behavior:** add an optional `player_key` parameter to `fetch_player_embedding_vector` + `search_similar_players`. When provided, the SQL filter prefers `player_key` (post-2026-07-22 path); when None, falls back to `canonical_player_id` (legacy path). Default callers pass None → no behavior change.

- [ ] **Step 1: Update `fetch_player_embedding_vector`**

Replace the existing function body (lines 156–179) with:

```python
@ttl_cache()
def fetch_player_embedding_vector(
    table: str,
    player_id: str,
    competition_id: int | None,
    player_key: int | None = None,
) -> pd.DataFrame:
    """Fetch the target player's embedding vectors.

    PR 5b dual-read: when ``player_key`` is provided, filter on the Kimball
    surrogate; otherwise fall back to ``canonical_player_id`` (the legacy
    path preserved through the 2026-07-22 dual-column window). Default
    None preserves existing behaviour.

    Expected columns: behavioral_vector, stat_vector.
    """
    validate_param_id(player_id)
    tbl = t(table)
    if player_key is not None:
        if competition_id is not None:
            return execute_query(
                f"SELECT behavioral_vector, stat_vector "  # noqa: S608
                f"FROM {tbl} WHERE player_key = %s "
                f"AND competition_id = %s",
                (int(player_key), competition_id),
            )
        return execute_query(
            f"SELECT behavioral_vector, stat_vector "  # noqa: S608
            f"FROM {tbl} WHERE player_key = %s",
            (int(player_key),),
        )
    if competition_id is not None:
        return execute_query(
            f"SELECT behavioral_vector, stat_vector "  # noqa: S608
            f"FROM {tbl} WHERE canonical_player_id = %s "
            f"AND competition_id = %s",
            (player_id, competition_id),
        )
    return execute_query(
        f"SELECT behavioral_vector, stat_vector "  # noqa: S608
        f"FROM {tbl} WHERE canonical_player_id = %s",
        (player_id,),
    )
```

- [ ] **Step 2: Update `search_similar_players` signature + exclusion clause**

Replace the existing function (lines 182–231) with:

```python
@ttl_cache()
def search_similar_players(
    table: str,
    vector_str: str,
    vector_col: str,
    vector_dim: int,
    total_col: str,
    player_id: str,
    min_matches: int,
    limit: int,
    competition_id: int | None,
    player_key: int | None = None,
) -> pd.DataFrame:
    """Run pgvector cosine distance query to find similar players.

    All arguments are primitives (str / int / int | None), so the cache
    key is deterministic without any normalization.  Caching avoids
    re-running the cosine-distance scan for the same target player and
    filter combination on every re-render.

    PR 5b dual-read: when ``player_key`` is provided, the self-exclusion
    clause uses ``e.player_key != %s``; otherwise falls back to
    ``e.canonical_player_id != %s`` (legacy path). Default None preserves
    existing behaviour.

    Expected columns: canonical_player_id, player_display_name,
    data_sources, <total_col>, distance.
    """
    if vector_col not in _ALLOWED_VECTOR_COLUMNS:
        msg = f"Invalid vector column: {vector_col}"
        raise ValueError(msg)
    if total_col not in _ALLOWED_COUNT_COLUMNS:
        msg = f"Invalid count column: {total_col}"
        raise ValueError(msg)

    tbl = t(table)
    dim_players_tbl = t("dim_players_synced")

    comp_filter = ""
    excl_clause = (
        "AND e.player_key != %s "
        if player_key is not None
        else "AND e.canonical_player_id != %s "
    )
    excl_value: Any = int(player_key) if player_key is not None else player_id

    params: list[Any] = [vector_str, min_matches, excl_value, limit]
    if competition_id is not None:
        comp_filter = "AND e.competition_id = %s "
        params = [vector_str, min_matches, competition_id, excl_value, limit]

    return execute_query(
        f"SELECT e.canonical_player_id, p.player_display_name, "  # noqa: S608
        f"  p.data_sources, "
        f"  e.{total_col}, "
        f"  e.{vector_col}::text::vector({vector_dim}) <=> %s::vector({vector_dim}) AS distance "
        f"FROM {tbl} e "
        f"JOIN {dim_players_tbl} p "
        f"  ON e.canonical_player_id = p.canonical_player_id "
        f"WHERE e.{total_col} >= %s " + comp_filter + excl_clause +
        "ORDER BY distance LIMIT %s",
        tuple(params),
    )
```

---

## Task 10: Wire dual-read into `state/player_similarity.py`

**Files:**
- Modify: `hf_taipy_app/src/state/player_similarity.py`

**Behavior:** populate a parallel `_ps_player_key_map` alongside `_ps_player_map`, and pass `player_key` to the embedding queries when available. Falls back to canonical_player_id when the helper returns None.

- [ ] **Step 1: Add the parallel key map after `_ps_player_map`**

Around line 137, after:

```python
_ps_player_map: dict[str, str] = {}  # label -> canonical_player_id
_ps_compare_map: dict[str, str] = {}  # label -> canonical_player_id for results
```

Add:

```python
_ps_player_key_map: dict[str, int] = {}  # label -> player_key (PR 5b dual-read)
```

- [ ] **Step 2: Populate the key map in `_load_player_list`**

Around line 264, replace the body of `_load_player_list` with:

```python
def _load_player_list(state: Any) -> None:
    """Reload the player dropdown from embedding table based on current filters."""
    global _ps_player_map, _ps_player_key_map
    comp_id = _resolve_competition_id(state)
    raw_table, count_col = _get_table_and_columns(comp_id)
    min_matches = int(state.ps_min_matches)

    try:
        players = fetch_embedding_players(comp_id, min_matches, raw_table, count_col)
        if not players:
            state.ps_warning_text = (
                "No players with embeddings found. Embeddings are available for players with enough match history."
            )
            return
        _ps_player_map = {label: pid for label, pid in players}
        # PR 5b: clear the parallel key map; populated lazily per selection
        # in on_ps_selected_player_change to avoid the up-front lookup of
        # the entire (~9000-player) candidate list.
        _ps_player_key_map = {}
        state.ps_player_lov = [label for label, _ in players]
    except Exception:
        logger.exception("Failed to load embedding players")
        _ps_player_map = {}
        _ps_player_key_map = {}
        state.ps_player_lov = []
```

- [ ] **Step 3: Lookup player_key in `_run_similarity_search` and pass through**

Around line 465 (`_run_similarity_search`), replace the function body's vector-fetch and similarity-search calls:

Before the `try:` block (after `state.ps_status_message = ...`), add:

```python
    # PR 5b dual-read: look up player_key for the selected canonical_player_id
    # so the pgvector query filters on the Kimball surrogate when available.
    player_key = _ps_player_key_map.get(player_label)
    if player_key is None:
        try:
            from queries.common import execute_query, t

            dim_tbl = t("dim_players_synced")
            df_pk = execute_query(
                f"SELECT player_key FROM {dim_tbl} "  # noqa: S608
                f"WHERE canonical_player_id = %s LIMIT 1",
                (player_id,),
            )
            if not df_pk.empty:
                player_key = int(df_pk.iloc[0]["player_key"])
                _ps_player_key_map[player_label] = player_key
        except Exception:
            logger.debug(
                "player_key lookup failed for %s — falling back to canonical_player_id",
                player_label,
                exc_info=True,
            )
            player_key = None
```

Then inside the `try:`, change the `fetch_player_embedding_vector` call (around line 491) to pass `player_key`:

```python
        target_result = fetch_player_embedding_vector(raw_table, player_id, comp_id, player_key=player_key)
```

And the `search_similar_players` call (around line 521) to pass `player_key`:

```python
        results = search_similar_players(
            table=raw_table,
            vector_str=vector_str,
            vector_col=vector_col,
            vector_dim=vector_dim,
            total_col=total_col,
            player_id=player_id,
            min_matches=int(state.ps_min_matches),
            limit=limit,
            competition_id=comp_id,
            player_key=player_key,
        )
```

---

## Task 11: Add a no-op pass-through in `queries/tracking.py`

**Files:**
- Modify: `hf_taipy_app/src/queries/tracking.py`

**Why minimal:** the tracking queries currently filter by raw `player_id` (int) and `match_id` (str), not `canonical_player_id`. Per spec line 220 ("dual-read in the similarity query") only `state/player_similarity.py` needs the active dual-read; tracking gets a docstring annotation noting forward-compat plumbing.

- [ ] **Step 1: Add a single-line PR 5b annotation comment near `fetch_physical_stats`**

In `fetch_physical_stats` (around line 200), after the docstring, before the `tbl = t(...)` line, add:

```python
    # PR 5b note: this query joins dim_players_synced on canonical_player_id
    # already; player_key adoption here happens in PR 8 alongside the
    # fct_physical_stats Kimball migration (see PR 7 spec).
```

No behaviour change. Documents the scope boundary.

---

## Task 12: HF dataset card — `football2vec-player-embeddings.md` dual-column stanza

**Files:**
- Modify: `docs/huggingface/dataset-cards/football2vec-player-embeddings.md`

- [ ] **Step 1: Add the stanza after `## Data Fields` section, before `## Use Cases`**

Insert after line 102 (the per-match table block ends):

```markdown
## Schema Migration — Dual-Column Window (2026-04-25 → 2026-07-22)

PR 5b of the Kimball migration (ADR-011) adds the BIGINT surrogate `player_key` to the underlying `fct_player_embeddings*` marts. **This dataset payload is NOT yet modified** — the parquet files continue to ship `canonical_player_id` only. PR 8 (planned 2026-07-22) will add `player_key` to the payloads in a backwards-compatible way and announce a sunset for `canonical_player_id`.

Recommended consumer behaviour during this window:

- **No change required.** Continue to read `canonical_player_id` from this dataset.
- If you maintain your own join to a `dim_players` clone, you may pre-compute `player_key = xxhash64(provider || '|' || cast(player_id as string))` to align with the lakehouse Kimball convention ahead of the payload change.
- After 2026-07-22 the dataset will carry both columns for at least one HF dataset version, then `canonical_player_id` will be deprecated. Migrate at your convenience inside that window.

If you depend on this dataset and need extra notice before the column drop, open an issue on the lakehouse repo.
```

---

## Task 13: HF dataset card — `football2vec-360-embeddings.md`

**Files:**
- Modify: `docs/huggingface/dataset-cards/football2vec-360-embeddings.md`

- [ ] **Step 1: Add the same stanza after Data Fields, before Use Cases**

Same stanza as Task 12 — adapt only the mart-name reference to `fct_player_embeddings_career_360 / _season_360 / fct_player_embeddings (data_source='football2vec_360')` if the card mentions specific mart names; otherwise the canonical stanza is identical.

(The card's existing structure mirrors `football2vec-player-embeddings.md` — verify section ordering at edit time and place the new H2 immediately after the `## Data Fields` H2's last subsection.)

---

## Task 14: HF dataset card — `football2vec-training-data.md`

**Files:**
- Modify: `docs/huggingface/dataset-cards/football2vec-training-data.md`

- [ ] **Step 1: Add the dual-column stanza after Data Fields**

Same stanza pattern. Note: training data card may reference upstream tables differently — adapt the lead sentence: "PR 5b of the Kimball migration (ADR-011) adds the BIGINT surrogate `player_key` to the upstream `fct_player_embeddings*` marts. The training-data export script (`src/ingestion/export_embeddings_training_data.py`) continues to read `canonical_player_id` only; payload changes ship in PR 8."

---

## Task 15: HF dataset card — `football2vec-360-training-data.md`

**Files:**
- Modify: `docs/huggingface/dataset-cards/football2vec-360-training-data.md`

- [ ] **Step 1: Add the dual-column stanza after Data Fields**

Same as Task 14 with 360 specifics.

---

## Task 16: HF dataset card — `football2vec-statsbomb-wyscout.md`

**Files:**
- Modify: `docs/huggingface/dataset-cards/football2vec-statsbomb-wyscout.md`

- [ ] **Step 1: Add the dual-column stanza after Data Fields**

This is the v1 model dataset; adapt the lead sentence: "PR 5b of the Kimball migration (ADR-011) adds the BIGINT surrogate `player_key` to the underlying lakehouse marts that produce embeddings using this model. This dataset's payload is unchanged."

---

## Task 17: New invariants test — `test_marts_player_key_contracts.py`

**Files:**
- Create: `src/tests/test_marts_player_key_contracts.py`

**What it tests:** for each of the 6 marts, the live Lakebase synced table must have `player_key` populated (not 100% NULL). Skip-on-no-warehouse-creds for local CI air-gap, like the existing live tests.

- [ ] **Step 1: Create the test file**

```python
"""PR 5b live invariants — player_key must be populated on six embedding marts.

This is the post-deploy gate that ensures the warn-severity dbt
relationships test isn't masking a 100%-NULL column. dbt's relationships
test compares non-NULL values to dim_players; an all-NULL column trivially
passes. This test asserts non-NULL-rate >= 99% on each of the six marts.

Skips when DATABRICKS_* env vars are absent (air-gapped CI). Otherwise
runs against dev_gold via the standard SQL warehouse connection.
"""

from __future__ import annotations

import os

import pytest

_MARTS = (
    "fct_player_embeddings",
    "fct_player_embeddings_season",
    "fct_player_embeddings_career",
    "fct_player_embeddings_season_360",
    "fct_player_embeddings_career_360",
    "fct_player_percentiles",
)


def _databricks_env_or_skip() -> tuple[str, str, str]:
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if not (host and token and http_path):
        pytest.skip("Databricks env not configured — live test skipped")
    return host, token, http_path


@pytest.mark.parametrize("mart", _MARTS)
def test_player_key_is_populated(mart: str) -> None:
    """Each PR 5b mart must have at least 99% non-NULL player_key."""
    host, token, http_path = _databricks_env_or_skip()
    from databricks import sql

    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) AS total, count(player_key) AS non_null FROM {table}"  # noqa: S608
        )
        row = cur.fetchone()
        assert row is not None, f"empty result on {table}"
        total = int(row[0])
        non_null = int(row[1])

    if total == 0:
        # Mart unbuilt (var('embeddings_enabled', false) on the embedding marts)
        # — skip rather than fail. The post-deploy step explicitly enables the
        # embedding marts; a 0-row count means the gate hasn't run yet.
        pytest.skip(f"{mart} has zero rows — embeddings_enabled may be off")

    rate = non_null / total
    assert rate >= 0.99, (
        f"{mart}: player_key non-NULL rate {rate:.4f} below 0.99 threshold "
        f"(total={total}, non_null={non_null}). Investigate dim_players join."
    )
```

---

## Task 18: Local CI gates

**Files:** none modified.

- [ ] **Step 1: Run ruff lint**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/
```

Expected: zero violations. Fix any new ones inline before proceeding.

- [ ] **Step 2: Run ruff format check**

```bash
uv run ruff format --check src/ scripts/ hf_taipy_app/src/
```

Expected: clean. If files reformatted, run `uv run ruff format src/ scripts/ hf_taipy_app/src/` and re-stage.

- [ ] **Step 3: Run pyright**

```bash
uv run pyright src/ hf_taipy_app/src/
```

Expected: zero errors. The new `resolve_player_identity` and `player_key` parameter additions must type-check.

- [ ] **Step 4: Run unit tests**

```bash
uv run pytest src/tests/ -v --ignore=src/tests/test_marts_player_key_contracts.py
```

(Excluding the new live test which requires warehouse creds — included in Task 19's live-CI gate.)

Expected: all green. The HF parity test will skip without HF_TOKEN; that's fine locally.

- [ ] **Step 5: dbt parse**

```bash
uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse --project-dir dbt_project --profiles-dir dbt_project
```

Expected: parses with no errors. Catches YAML/SQL contract mismatches before live CI.

---

## Task 19: Single commit + push + open PR (USER GIT GATE)

**Files:** all changes from Tasks 1–17.

- [ ] **Step 1: Stage and review diff**

```bash
git status
git diff --stat
git diff
```

Confirm only the PR 5b files are touched. Halt and ask the user before committing if anything looks off.

- [ ] **Step 2: Pause for user approval to commit + push + open PR**

Per `feedback_no_commits_without_approval`, surface the diff to the user with a clear "ready to commit?" — do NOT auto-commit. Wait for explicit approval.

- [ ] **Step 3: After approval — commit, push, open PR**

```bash
git add dbt_project/models/marts/fct_player_embeddings.sql \
        dbt_project/models/marts/fct_player_embeddings_season.sql \
        dbt_project/models/marts/fct_player_embeddings_career.sql \
        dbt_project/models/marts/fct_player_embeddings_season_360.sql \
        dbt_project/models/marts/fct_player_embeddings_career_360.sql \
        dbt_project/models/marts/fct_player_percentiles.sql \
        dbt_project/models/marts/_marts__models.yml \
        hf_taipy_app/src/state/shared.py \
        hf_taipy_app/src/state/player_similarity.py \
        hf_taipy_app/src/queries/players.py \
        hf_taipy_app/src/queries/tracking.py \
        docs/huggingface/dataset-cards/football2vec-player-embeddings.md \
        docs/huggingface/dataset-cards/football2vec-360-embeddings.md \
        docs/huggingface/dataset-cards/football2vec-training-data.md \
        docs/huggingface/dataset-cards/football2vec-360-training-data.md \
        docs/huggingface/dataset-cards/football2vec-statsbomb-wyscout.md \
        src/tests/test_marts_player_key_contracts.py \
        docs/superpowers/plans/2026-04-25-kimball-pr5b-embedding-marts.md
git commit -m "$(cat <<'EOF'
feat(kimball-pr5b): player_key on six embedding/percentile marts + dual-read plumbing

- fct_player_embeddings gains match_key (retires PR 5a CI-triage dim_matches
  bridge in _season + _season_360) + player_key (LEFT JOIN dim_players)
- fct_player_embeddings_season/_career/_season_360/_career_360 gain
  player_key passthrough via any_value()
- fct_player_percentiles pulls player_key from fct_player_stats (PR 5a)
- _marts__models.yml: six warn-severity relationships → dim_players(player_key)
  for the 2026-07-22 dual-column window
- Taipy: resolve_player_identity() helper in state/shared.py + dual-read
  on fetch_player_embedding_vector + search_similar_players + ps_ state
  module; query filters fall back to canonical_player_id when player_key
  is None (forward-compat plumbing only — no behaviour change in PR 5b)
- 5 HF dataset cards documented for the dual-column window (cards only;
  payloads ship in PR 8)
- New live invariant test asserts player_key non-NULL >= 99% on the six
  marts post-deploy

Spec: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2 (PR 5b)
Plan: docs/superpowers/plans/2026-04-25-kimball-pr5b-embedding-marts.md
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push -u origin kimball-pr5b-embedding-marts
gh pr create --title "feat(kimball-pr5b): player_key on six embedding marts + dual-read" --body "$(cat <<'EOF'
## Summary
- Add `player_key` BIGINT to six embedding/percentile marts (PR 5b of ADR-011)
- Add `match_key` to `fct_player_embeddings`; retire dim_matches bridge in `_season` + `_season_360`
- Add `resolve_player_identity()` helper + dual-read plumbing in Taipy
- Document 2026-07-22 dual-column window on five HF dataset cards
- New live invariant test on player_key non-NULL rate

## Scope
- Marts: 6 modified
- Contracts: 6 column additions, all warn-severity relationships during the dual-column window
- Consumers: 4 Taipy files (no behaviour change — forward-compat plumbing only)
- HF cards: 5 (cards only; payloads ship in PR 8)

## Test plan
- [ ] Local: `uv run ruff check`, `uv run ruff format --check`, `uv run pyright`, `uv run pytest src/tests/ -v --ignore=src/tests/test_marts_player_key_contracts.py`
- [ ] Local: `uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks dbt parse`
- [ ] Live CI (`.github/workflows/dbt-live-ci.yml`): `state:modified+` builds 6 modified marts + transitive downstream; all green; warn-severity relationships pass
- [ ] Post-merge deploy: `uv run python scripts/refresh_synced_tables.py --tables fct_player_embeddings_synced fct_player_embeddings_season_synced fct_player_embeddings_career_synced fct_player_embeddings_season_360_synced fct_player_embeddings_career_360_synced fct_player_percentiles_synced` (additive auto-evolve)
- [ ] Post-merge deploy: `uv run python scripts/maintain_synced_tables.py --skip-refresh` for grants
- [ ] Post-merge deploy: `uv run pytest src/tests/test_marts_player_key_contracts.py -v` green
- [ ] Post-merge deploy: `uv run python scripts/publish_hf_cards.py --kind dataset --name <each card>.md` × 5
- [ ] Taipy E2E on dev Space: Player-Similarity loads, similar-player search returns same neighbour set as pre-PR (dual-read fallback path), no log errors

## Risks
- `fct_player_embeddings` is incremental MERGE; the column additions are additive but the first build after merge needs `--full-refresh` to populate `match_key` + `player_key` on existing rows. The live-CI Job uses `--full-refresh` on `state:modified+` selectors that touch incremental marts; verify it does so for `fct_player_embeddings`.
- HF dataset card text is propagated to HF Hub by `scripts/publish_hf_cards.py --kind dataset --name <card>.md` after merge. Card filename basenames must match HF repo basenames (test_hf_publish_parity.py guards this).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Task 20: Live dbt CI gate (USER WAITS ON CI)

**Files:** none modified locally.

The PR triggers `.github/workflows/dbt-live-ci.yml` which runs `state:modified+` on Databricks serverless. Six marts + transitive downstream should build green.

- [ ] **Step 1: Watch the run**

```bash
gh pr checks --watch
```

Expected: all green within ~15-20 min (PR 5a precedent). Live CI surfaces any latent bugs in `state:modified+` cascade per `reference_live_ci_surfaces_latent_bugs`.

- [ ] **Step 2: If failures — triage per `reference_live_ci_surfaces_latent_bugs` playbook**

Compile errors in PR 5b scope: fix in PR. Compile errors in adjacent unmigrated marts: fix in PR (cheap; column-rename swap). Data-test failures outside PR scope: warn-severity flip with YAML pointer to closing PR. **Do NOT skip the gate** — failures are diagnostic signal.

---

## Task 21: Post-merge deploy (USER GIT GATE — separate approval)

After live-CI green and explicit user approval to merge + deploy:

- [ ] **Step 1: User merges PR (squash-merge per `feedback_single_commit_squash`)**

This is a user action. Proceed only after the user confirms the merge.

- [ ] **Step 2: Refresh synced tables (additive auto-evolve)**

```bash
uv run python scripts/refresh_synced_tables.py --tables fct_player_embeddings_synced fct_player_embeddings_season_synced fct_player_embeddings_career_synced fct_player_embeddings_season_360_synced fct_player_embeddings_career_360_synced fct_player_percentiles_synced
```

Plan-approved per `reference_lakebase_synced_table_auto_evolution` for additive columns.

- [ ] **Step 3: Apply Lakebase grants + indexes**

```bash
uv run python scripts/maintain_synced_tables.py --skip-refresh
```

Includes Step 0.5 grants per ADR-005.

- [ ] **Step 4: Run live invariant test**

```bash
uv run pytest src/tests/test_marts_player_key_contracts.py -v
```

Expected: 6 green (one per mart). Failures here mean the dim_players LEFT JOIN dropped rows — investigate.

- [ ] **Step 5: Push 5 HF dataset cards**

```bash
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-player-embeddings.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-360-embeddings.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-training-data.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-360-training-data.md
uv run python scripts/publish_hf_cards.py --kind dataset --name football2vec-statsbomb-wyscout.md
```

Each emits a commit URL via `upload_hf_readme` log line. Verify all 5 succeed.

- [ ] **Step 6: Memory entry capture**

Update `project_kimball_migration_cycle.md` to mark PR 5b shipped + deployed; bump `project_kimball_pr5a_shipped.md` cross-reference.

---

## Self-Review Checklist (run before Task 19)

- [ ] **Spec coverage:** every bullet in spec §2 "PR 5b" maps to a task above:
  - 6 mart `player_key` additions → Tasks 1–6
  - YAML contract updates → Task 7
  - `resolve_player_identity` helper → Task 8
  - `queries/players.py` + `state/player_similarity.py` dual-read → Tasks 9–10
  - `queries/tracking.py` annotation → Task 11
  - dim_matches bridge retirement → folded into Tasks 1, 2, 4
  - 5 HF cards → Tasks 12–16
  - Synced-table refresh → Task 21 step 2
  - HF dataset card parity test → Task 18 step 4 (existing test) + manual verify after Task 21 step 5
- [ ] **Placeholders:** none. Every code block is real text the executor pastes verbatim.
- [ ] **Type consistency:** `player_key` is BIGINT in dim, BIGINT in marts, `int` in Python signatures, `int | None` in optional parameters. Consistent across Tasks 1–11.
- [ ] **Hyrum's Law:** `canonical_player_id` is preserved verbatim everywhere. Confirmed in Tasks 1–6.
- [ ] **DELTA_MULTIPLE_SOURCE_ROW_MATCHING guard:** Task 1's incremental mart change is additive (LEFT JOIN columns), but live-CI must use `--full-refresh` for the first build — flagged in Task 19's PR description Risks section.
- [ ] **No commits without approval:** Task 19 explicitly pauses for user before commit + push + PR. Task 21 explicitly pauses for user before merge + deploy.

---

**End of plan.**
