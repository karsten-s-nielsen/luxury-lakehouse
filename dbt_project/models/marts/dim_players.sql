-- dim_players.sql
-- Player dimension table combining StatsBomb and Wyscout data.
--
-- Cross-source entity resolution (Phase 14) maps players across sources.
-- Players matched across sources share a canonical_player_id; unmatched
-- players retain their source-native ID.
--
-- Grain: one row per unique canonical_player_id.

with statsbomb_players as (

    select
        player_id,
        player_name,
        player_nickname,
        position_name                                       as primary_position,
        'statsbomb'                                         as data_source,
        row_number() over (
            partition by player_id
            order by match_id desc
        )                                                   as rn

    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null

),

sb_deduped as (

    select * from statsbomb_players where rn = 1

),

{% if var('entity_resolution_enabled', false) %}
wyscout_players as (

    select
        player_id,
        player_name,
        short_name,
        position_name                                       as primary_position,
        birth_date,
        nationality,
        'wyscout'                                           as data_source

    from {{ ref('stg_wyscout__players') }}

),

xref as (

    select * from {{ ref('int_player_xref') }}

),
{% else %}
xref as (

    -- Entity resolution not yet run — empty xref
    select
        cast(null as int) as statsbomb_player_id,
        cast(null as int) as wyscout_player_id,
        cast(null as double) as confidence,
        cast(null as int) as match_layer,
        cast(null as string) as resolution_type
    where 1 = 0

),
{% endif %}

-- StatsBomb players enriched with Wyscout cross-reference
sb_enriched as (

    select
        {{ dbt_utils.generate_surrogate_key(['sb.player_id', "'statsbomb'"]) }}
                                                            as canonical_player_id,
        sb.player_id                                        as player_id,
        sb.player_name,
        coalesce(sb.player_nickname, sb.player_name)        as player_display_name,
        sb.primary_position,
        sb.data_source,
        -- Cross-source IDs
        sb.player_id                                        as statsbomb_player_id,
        xref.wyscout_player_id,
        xref.confidence                                     as match_confidence,
        xref.match_layer,
        -- Enrich from Wyscout if matched
        {% if var('entity_resolution_enabled', false) %}
        ws.birth_date,
        ws.nationality
        {% else %}
        cast(null as string)                                as birth_date,
        cast(null as string)                                as nationality
        {% endif %}

    from sb_deduped sb
    left join xref
        on sb.player_id = xref.statsbomb_player_id
    {% if var('entity_resolution_enabled', false) %}
    left join wyscout_players ws
        on xref.wyscout_player_id = ws.player_id
    {% endif %}

),

{% if var('entity_resolution_enabled', false) %}
-- Wyscout-only players (no StatsBomb match)
ws_unmatched as (

    select
        {{ dbt_utils.generate_surrogate_key(['ws.player_id', "'wyscout'"]) }}
                                                            as canonical_player_id,
        ws.player_id,
        ws.player_name,
        coalesce(ws.short_name, ws.player_name)             as player_display_name,
        ws.primary_position,
        ws.data_source,
        -- Cross-source IDs
        cast(null as int)                                   as statsbomb_player_id,
        ws.player_id                                        as wyscout_player_id,
        cast(null as double)                                as match_confidence,
        cast(null as int)                                   as match_layer,
        ws.birth_date,
        ws.nationality

    from wyscout_players ws
    left join xref
        on ws.player_id = xref.wyscout_player_id
    where xref.wyscout_player_id is null

),

combined as (

    select * from sb_enriched
    union all
    select * from ws_unmatched

),
{% else %}
combined as (

    select * from sb_enriched

),
{% endif %}

final as (

    select
        canonical_player_id,
        player_id,
        player_name,
        player_display_name,
        primary_position,
        pm.position_group,
        statsbomb_player_id,
        wyscout_player_id,
        match_confidence,
        match_layer,
        birth_date,
        nationality,
        case
            when statsbomb_player_id is not null and wyscout_player_id is not null
                then 'statsbomb,wyscout'
            when statsbomb_player_id is not null then 'statsbomb'
            else 'wyscout'
        end                                                 as data_sources

    from combined c
    left join {{ ref('position_mapping') }} pm
        on c.primary_position = pm.position_name

)

select * from final
