{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='line_breaking_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['match_key'],
    tags=['marts', 'output_mart'],
    tblproperties={
        'delta.enableChangeDataFeed': 'true',
    }
) }}
-- fct_line_breaking_results.sql
-- Gold-layer line-breaking detection results per pass event.
--
-- PR 2 (ADR-011) migrates the native `match_id` column to the Kimball
-- surrogate `match_key` (BIGINT FK to dim_matches). The upstream
-- `stg_line_breaking__results.match_id` has also been normalised as
-- part of this PR — IDSSE rows no longer carry the legacy `idsse_`
-- namespace prefix — so the join against dim_matches on
-- (provider, native_match_id) is direct.
--
-- data_source → dim_matches.provider mapping:
--   statsbomb_360    → statsbomb
--   metrica_tracking → metrica
--   idsse_tracking   → idsse
--
-- PR 7 (ADR-011 close-out): adds Kimball surrogate FKs team_key + player_key
-- (the passer's team / player). int_unified_passes is the source of truth
-- for the per-event native_team_id + native_player_id; LEFT JOIN to it on
-- (event_id, data_source) keeps line-breaking rows that lack a matching
-- pass event (defensive — should be 0 in practice).

with line_breaking_raw as (

    select
        event_id,
        cast(match_id as string)                        as native_match_id,
        case data_source
            when 'statsbomb_360'    then 'statsbomb'
            when 'metrica_tracking' then 'metrica'
            when 'idsse_tracking'   then 'idsse'
            else data_source
        end                                             as provider,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source
    from {{ ref('stg_line_breaking__results') }}

),

passes_for_keys as (

    -- PR 7: surface (match_key, event_id, data_source, native_*) from
    -- int_unified_passes for team_key + player_key resolution. The JOIN
    -- key MUST include match_key — IDSSE / Metrica event_id strings are
    -- per-match, not globally unique, so JOINing on (event_id, data_source)
    -- alone multiplies line_breaking_raw rows when the same event_id
    -- string repeats across matches with different teams/players. Adding
    -- match_key matches int_unified_passes' own pass identity grain
    -- (the same (match_key, event_id, data_source) triple that
    -- fct_passes.pass_id surrogates over).
    select
        match_key,
        event_id,
        data_source,
        native_team_id,
        native_player_id
    from {{ ref('int_unified_passes') }}

),

keyed as (

    select
        lb.event_id,
        dm.match_key,
        dt.team_key,
        dp.player_key,
        lb.is_line_breaking,
        lb.lines_broken,
        lb.line_breaking_type,
        lb.data_source
    from line_breaking_raw lb
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = lb.provider
       and dm.native_match_id = lb.native_match_id
    left join passes_for_keys p
        on  p.match_key = dm.match_key
       and p.event_id = lb.event_id
       and p.data_source = lb.provider
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = lb.provider
       and dt.native_team_id = p.native_team_id
    left join {{ ref('dim_players') }} dp
        on  dp.provider = lb.provider
       and dp.native_player_id = p.native_player_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['event_id']) }}    as line_breaking_id,
        event_id,
        match_key,
        team_key,
        player_key,
        is_line_breaking,
        lines_broken,
        line_breaking_type,
        data_source

    from keyed

)

select * from final
