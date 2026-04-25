-- dim_players.sql
-- Conformed player dimension (ADR-011 PR 5a).
--
-- Grain: one row per (provider, native_player_id).
--
-- Surrogates:
--   - player_key BIGINT — new Kimball surrogate via generate_player_key macro.
--   - canonical_player_id STRING — legacy hash preserved for Hyrum's Law compat
--     (57 downstream files + HF datasets reference it). Same value per SB player
--     as the pre-PR-5a dim_players output (`dbt_utils.generate_surrogate_key(
--     ['player_id', "'statsbomb'"])`) so existing consumers are non-breaking.
--   - canonical_player_key BIGINT — xref-resolved canonical pointer (preference
--     SB > WS > IDSSE); self-pointer when no xref match.
--
-- Metrica players: sample data is anonymised ("Player11"–"Player25" per side,
-- no names). Cross-provider entity resolution is UNREACHABLE by name matching →
-- Metrica rows stay siloed permanently. Documented design choice, not deferral.
-- Future Metrica subscription data (is_anonymized=false) has real names and
-- becomes xref-eligible through the same generate_entity_xref.py pipeline.

{{ config(
    materialized='table',
    meta={'contains_pii': False}
) }}

with statsbomb_players as (

    -- stg_statsbomb__lineups is one row per (match, player), so the same
    -- player_id appears across every match they played. GROUP BY collapses
    -- to one row per native_player_id, with MAX picking a stable
    -- representative value for per-match-varying attrs (nickname, position).
    -- This is required for unique(player_key) / unique(native_player_id)
    -- to hold on the downstream dim_players.
    select
        cast(player_id as string)                       as native_player_id,
        max(player_id)                                  as player_id_legacy,
        max(player_name)                                as player_name,
        coalesce(max(player_nickname), max(player_name)) as player_display_name,
        max(position_name)                              as primary_position,
        'statsbomb'                                     as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null
    group by cast(player_id as string)

),

wyscout_players_raw as (

    -- stg_wyscout__players is natively one row per player (Figshare roster),
    -- but group-by guards against future multi-row grains (e.g. per-season).
    select
        cast(player_id as string)                       as native_player_id,
        max(player_id)                                  as player_id_legacy,
        max(player_name)                                as player_name,
        coalesce(max(short_name), max(player_name))     as player_display_name,
        max(position_name)                              as primary_position,
        'wyscout'                                       as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        max(birth_date)                                 as birth_date,
        max(nationality)                                as nationality
    from {{ ref('stg_wyscout__players') }}
    where player_id is not null
    group by cast(player_id as string)

),

idsse_players as (

    -- stg_tracking__player_metadata is one row per (match, player) — same
    -- player appears in multiple matches. Group to one row per native_player_id
    -- with MAX picking a stable display name for the few cases of minor
    -- name variation across matches.
    select
        cast(player_id as string)                       as native_player_id,
        cast(null as int)                               as player_id_legacy,
        max(player_display_name)                        as player_name,
        max(player_display_name)                        as player_display_name,
        cast(null as string)                            as primary_position,
        'idsse'                                         as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_tracking__player_metadata') }}
    where provider = 'idsse'
      and player_id is not null
    group by cast(player_id as string)

),

metrica_anon_players as (

    select distinct
        native_player_id,
        cast(null as int)                               as player_id_legacy,
        concat('Metrica ', match_id, ' ', initcap(side), ' ', player_key_in_map) as player_name,
        player_key_in_map                               as player_display_name,
        cast(null as string)                            as primary_position,
        'metrica'                                       as provider,
        true                                            as is_synthesized,
        true                                            as is_anonymized,
        'metrica_anonymized'                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = true
      and native_player_id is not null

),

metrica_real_players as (

    -- Forward-compat zero-row branch for future subscription data.
    select distinct
        native_player_id,
        cast(null as int)                               as player_id_legacy,
        cast(null as string)                            as player_name,
        player_key_in_map                               as player_display_name,
        cast(null as string)                            as primary_position,
        'metrica'                                       as provider,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = false
      and native_player_id is not null

),

unioned as (

    select * from statsbomb_players
    union all
    select * from wyscout_players_raw
    union all
    select * from idsse_players
    union all
    select * from metrica_anon_players
    union all
    select * from metrica_real_players

),

-- Preserve historical canonical_player_id values: SB rows retain the same
-- hash computed pre-PR-5a (dbt_utils.generate_surrogate_key(['player_id', 'statsbomb'])),
-- WS rows use the equivalent WS-side hash. IDSSE + Metrica use
-- (native_player_id, provider) — new surrogate, no prior consumer.
canonical as (

    select
        u.*,
        {{ generate_player_key('u.provider', 'u.native_player_id') }} as player_key,
        case
            when u.provider = 'statsbomb' then
                {{ dbt_utils.generate_surrogate_key(['u.player_id_legacy', "'statsbomb'"]) }}
            when u.provider = 'wyscout' then
                {{ dbt_utils.generate_surrogate_key(['u.player_id_legacy', "'wyscout'"]) }}
            else
                {{ dbt_utils.generate_surrogate_key(['u.native_player_id', 'u.provider']) }}
        end as canonical_player_id
    from unioned u

),

{%- if var('entity_resolution_enabled', false) %}

xref as (

    -- canonical_player_key resolution: preference SB > WS > IDSSE.
    -- Metrica rows always self-point (anonymised; never in xref).
    select
        t.player_key,
        t.provider,
        t.native_player_id,
        coalesce(
            (select {{ generate_player_key('x.source_b', 'x.player_id_b') }}
                from {{ ref('int_player_xref') }} x
                where x.source_a = t.provider and x.player_id_a = t.native_player_id
                order by
                    case x.source_b
                        when 'statsbomb' then 1 when 'wyscout' then 2
                        when 'idsse' then 3 when 'metrica' then 4
                    end,
                    x.confidence desc
                limit 1),
            (select {{ generate_player_key('x.source_a', 'x.player_id_a') }}
                from {{ ref('int_player_xref') }} x
                where x.source_b = t.provider and x.player_id_b = t.native_player_id
                order by
                    case x.source_a
                        when 'statsbomb' then 1 when 'wyscout' then 2
                        when 'idsse' then 3 when 'metrica' then 4
                    end,
                    x.confidence desc
                limit 1),
            t.player_key
        ) as canonical_player_key,
        (select x.player_id_b
            from {{ ref('int_player_xref') }} x
            where x.source_a = t.provider and x.player_id_a = t.native_player_id
              and x.source_b = 'statsbomb'
            order by x.confidence desc limit 1) as statsbomb_player_id_side_b,
        (select x.player_id_a
            from {{ ref('int_player_xref') }} x
            where x.source_b = t.provider and x.player_id_b = t.native_player_id
              and x.source_a = 'statsbomb'
            order by x.confidence desc limit 1) as statsbomb_player_id_side_a,
        (select x.confidence
            from {{ ref('int_player_xref') }} x
            where (x.source_a = t.provider and x.player_id_a = t.native_player_id)
               or (x.source_b = t.provider and x.player_id_b = t.native_player_id)
            order by x.confidence desc limit 1) as match_confidence
    from canonical t

),

final as (

    select
        c.player_key,
        c.canonical_player_id,
        xr.canonical_player_key,
        c.provider,
        c.native_player_id,
        c.player_id_legacy                             as player_id,
        c.player_name,
        c.player_display_name,
        c.primary_position,
        pm.position_group,
        case when c.provider = 'statsbomb' then c.player_id_legacy end as statsbomb_player_id,
        case when c.provider = 'wyscout' then c.player_id_legacy end   as wyscout_player_id,
        case when c.provider = 'idsse' then c.native_player_id end     as idsse_player_id,
        xr.match_confidence,
        cast(null as int)                              as match_layer,
        c.birth_date,
        c.nationality,
        c.is_synthesized,
        c.is_anonymized,
        c.synthesis_reason,
        c.provider                                     as data_sources
    from canonical c
    left join xref xr
        on  xr.player_key = c.player_key
       and xr.provider = c.provider
       and xr.native_player_id = c.native_player_id
    left join {{ ref('position_mapping') }} pm
        on c.primary_position = pm.position_name

)

{%- else %}

final as (

    select
        c.player_key,
        c.canonical_player_id,
        c.player_key                                   as canonical_player_key,
        c.provider,
        c.native_player_id,
        c.player_id_legacy                             as player_id,
        c.player_name,
        c.player_display_name,
        c.primary_position,
        pm.position_group,
        case when c.provider = 'statsbomb' then c.player_id_legacy end as statsbomb_player_id,
        case when c.provider = 'wyscout' then c.player_id_legacy end   as wyscout_player_id,
        case when c.provider = 'idsse' then c.native_player_id end     as idsse_player_id,
        cast(null as double)                           as match_confidence,
        cast(null as int)                              as match_layer,
        c.birth_date,
        c.nationality,
        c.is_synthesized,
        c.is_anonymized,
        c.synthesis_reason,
        c.provider                                     as data_sources
    from canonical c
    left join {{ ref('position_mapping') }} pm
        on c.primary_position = pm.position_name

)

{%- endif %}

select * from final
