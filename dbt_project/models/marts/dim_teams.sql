-- dim_teams.sql
-- Conformed team dimension unifying StatsBomb, Wyscout, IDSSE, and Metrica.
--
-- PRIMARY KEY: team_key (BIGINT surrogate, xxhash64 of provider|native_team_id
--              via generate_team_key macro per ADR-011 PR 5a).
-- UNIQUE: (provider, native_team_id).
--
-- Grain: one row per (provider, native_team_id).
--
-- Synthesis rules (PR 5a):
--   - StatsBomb + Wyscout real teams: is_synthesized=false, team_id legacy populated.
--   - IDSSE: real DFL TeamId (DFL-CLU-XXXXXX) from stg_idsse__home_away_teams.
--   - Wyscout fallback rows (parse-failure on teams_data_parsed):
--       is_synthesized=true, synthesis_reason='wyscout_unresolved_teamsdata'.
--       Current data: 0 fallback rows (parse success 100%).
--   - Metrica anonymised: is_synthesized=true, synthesis_reason='metrica_anonymized'.
--   - Metrica subscription (future is_anonymized=false): real-identity path ready.
--
-- canonical_team_key: xref-resolved canonical pointer via int_team_xref
--   (preference order statsbomb > wyscout > idsse), or self when no match.
--
-- team_name populated for Wyscout real rows via new stg_wyscout__teams
-- (PR 5a closes the pre-existing teams.json ingestion gap).

with statsbomb_teams as (

    select distinct
        'statsbomb'                                     as provider,
        cast(team_id as string)                         as native_team_id,
        -- Upstream stg_statsbomb__events.team_id is BIGINT; pre-PR-5a dim_teams
        -- was BIGINT too, and dim_teams_synced (Lakebase) uses team_id as the
        -- primary key. Changing a synced-table PK column type is not
        -- supported (CHANGED_PRIMARY_KEY_COLUMN_TYPE error). Keep as BIGINT.
        team_id                                         as team_id_legacy,
        team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null

),

wyscout_real_teams as (

    select distinct
        'wyscout'                                       as provider,
        cast(e.team_id as string)                       as native_team_id,
        -- BIGINT passthrough (same rationale as statsbomb_teams — PK invariant).
        e.team_id                                       as team_id_legacy,
        wt.team_name                                    as team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_wyscout__events') }} e
    left join {{ ref('stg_wyscout__teams') }} wt
        on e.team_id = wt.team_id
    where e.team_id is not null

),

wyscout_synth_teams as (

    -- Fallback rows produced by stg_wyscout__home_away_teams when
    -- teams_data_parsed is NULL/empty. Zero rows on current data.
    select distinct
        'wyscout'                                       as provider,
        hat.native_team_id                              as native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        cast(null as string)                            as team_name,
        true                                            as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        hat.synthesis_reason                            as synthesis_reason
    from {{ ref('stg_wyscout__home_away_teams') }} hat
    where hat.is_synthesized = true

),

idsse_teams as (

    -- PR 7 hotfix #3: stg_idsse__home_away_teams was deleted; subsumed by
    -- int_tracking__match_side_team_bridge filtered to source_provider='idsse'.
    select
        'idsse'                                         as provider,
        hat.team_id                                     as native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        max(pm.team_display_name)                       as team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('int_tracking__match_side_team_bridge') }} hat
    left join {{ ref('stg_tracking__player_metadata') }} pm
        on  pm.provider = 'idsse'
       and pm.match_id = concat('idsse_', hat.match_id)  -- bronze tracking_player_metadata.match_id keeps idsse_ prefix
       and pm.team_side = hat.side
    where hat.source_provider = 'idsse'
      and hat.team_id is not null
    group by hat.team_id

),

metrica_anon_teams as (

    select distinct
        'metrica'                                       as provider,
        native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        concat('Metrica ', match_id, ' ', initcap(side)) as team_name,
        true                                            as is_synthesized,
        true                                            as is_anonymized,
        'metrica_anonymized'                            as synthesis_reason
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = true
      and native_team_id is not null

),

metrica_real_teams as (

    -- Forward-compat path for future subscription data. Zero rows today.
    select distinct
        'metrica'                                       as provider,
        native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        cast(null as string)                            as team_name,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = false
      and native_team_id is not null

),

skillcorner_teams as (

    -- PR 7 (ADR-011 close-out): SkillCorner onboarded into dim_teams.
    -- Real SkillCorner team_ids from bronze.skillcorner_tracking via
    -- home_team_id/away_team_id passthroughs (surfaced in stg_skillcorner__tracking
    -- post-PR-7 team_id derivation). No team_name in bronze (SkillCorner
    -- broadcast tracking doesn't carry team names); is_anonymized=true so
    -- consumers know name is unavailable.
    select distinct
        'skillcorner'                                   as provider,
        cast(team_id as string)                         as native_team_id,
        cast(null as bigint)                            as team_id_legacy,
        concat('SkillCorner Team ', team_id)            as team_name,
        false                                           as is_synthesized,
        true                                            as is_anonymized,
        'skillcorner_no_team_names_in_bronze'           as synthesis_reason
    from {{ ref('stg_skillcorner__tracking') }}
    where team_id is not null

),

unioned as (

    select * from statsbomb_teams
    union all
    select * from wyscout_real_teams
    union all
    select * from wyscout_synth_teams
    union all
    select * from idsse_teams
    union all
    select * from metrica_anon_teams
    union all
    select * from metrica_real_teams
    union all
    select * from skillcorner_teams

),

with_keys as (

    select
        {{ generate_team_key('provider', 'native_team_id') }} as team_key,
        u.*
    from unioned u

),

{%- if var('entity_resolution_enabled', false) %}

-- canonical_team_key resolution via xref. Preference: statsbomb > wyscout > idsse > metrica.
-- Metrica rows stay siloed (anonymised; unreachable by name-matching generator).
xref as (

    select
        t.team_key,
        t.provider,
        t.native_team_id,
        coalesce(
            (select {{ generate_team_key('x.source_b', 'x.team_id_b') }}
                from {{ ref('int_team_xref') }} x
                where x.source_a = t.provider and x.team_id_a = t.native_team_id
                order by
                    case x.source_b
                        when 'statsbomb' then 1 when 'wyscout' then 2
                        when 'idsse' then 3 when 'metrica' then 4
                    end,
                    x.confidence desc
                limit 1),
            (select {{ generate_team_key('x.source_a', 'x.team_id_a') }}
                from {{ ref('int_team_xref') }} x
                where x.source_b = t.provider and x.team_id_b = t.native_team_id
                order by
                    case x.source_a
                        when 'statsbomb' then 1 when 'wyscout' then 2
                        when 'idsse' then 3 when 'metrica' then 4
                    end,
                    x.confidence desc
                limit 1),
            t.team_key
        ) as canonical_team_key
    from with_keys t

),

final as (

    select
        wk.team_key,
        wk.provider,
        wk.native_team_id,
        wk.team_id_legacy                              as team_id,
        wk.team_name,
        xr.canonical_team_key,
        wk.is_synthesized,
        wk.is_anonymized,
        wk.synthesis_reason,
        -- Legacy `data_source` retained alongside new `team_data_source` during
        -- the coordinated ADR-011 dual-column window (sunset 2026-07-22 per PR 8).
        -- Additive-only schema change — Lakebase synced-table auto-evolution
        -- handles the new cols; PR 8 drops `data_source` in one coordinated pass.
        wk.provider                                    as data_source,
        wk.provider                                    as team_data_source
    from with_keys wk
    left join xref xr
        on  xr.team_key = wk.team_key
       and xr.provider = wk.provider
       and xr.native_team_id = wk.native_team_id

)

{%- else %}

-- Entity resolution disabled: canonical_team_key is self-pointer for all rows.
final as (

    select
        wk.team_key,
        wk.provider,
        wk.native_team_id,
        wk.team_id_legacy                              as team_id,
        wk.team_name,
        wk.team_key                                    as canonical_team_key,
        wk.is_synthesized,
        wk.is_anonymized,
        wk.synthesis_reason,
        -- Legacy `data_source` retained alongside new `team_data_source` during
        -- the coordinated ADR-011 dual-column window (sunset 2026-07-22 per PR 8).
        wk.provider                                    as data_source,
        wk.provider                                    as team_data_source
    from with_keys wk

)

{%- endif %}

select * from final
