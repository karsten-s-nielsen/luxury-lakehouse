-- dim_competitions.sql
-- Conformed competition dimension. Unifies StatsBomb + Wyscout + IDSSE
-- competitions. Metrica is intentionally absent — its open-data is
-- anonymised and carries no competition metadata.
--
-- PRIMARY KEY: competition_key (BIGINT surrogate, deterministic hash of
-- (provider, native_competition_id) via `generate_competition_key` macro).
-- Added in PR 2 of the Kimball migration (ADR-011) following the
-- same pattern as `dim_matches.match_key`.
--
-- Grain: one row per (provider, native_competition_id).
--
-- StatsBomb and Wyscout use SEPARATE integer competition-id spaces
-- for the same leagues (e.g. La Liga is 11 in StatsBomb, 795 in
-- Wyscout). Both spaces are preserved as independent rows with
-- different competition_keys so fact tables from either source can
-- JOIN without NULL competition names. IDSSE uses DFL-COM-XXXXXX
-- strings; no collision with SB/WS numeric space.
--
-- Legacy `competition_id` column (INT) is retained for backward
-- compatibility with unmigrated fact tables (fct_action_values,
-- fct_shots, fct_defcon_*, etc.). NULL for IDSSE rows (native IDs
-- are non-numeric). Will be dropped in PR 8 once all consumers
-- migrate to competition_key.

with statsbomb_competitions as (

    select distinct
        'statsbomb'                        as provider,
        cast(competition_id as string)     as native_competition_id,
        competition_id                     as competition_id_legacy,
        competition_name

    from {{ ref('stg_statsbomb__matches') }}
    where competition_id is not null

),

wyscout_competitions as (

    select distinct
        'wyscout'                          as provider,
        cast(competition_id as string)     as native_competition_id,
        competition_id                     as competition_id_legacy,
        competition_name

    from {{ ref('stg_wyscout__matches') }}
    where competition_id is not null

),

idsse_competitions as (

    -- DFL competitions from stg_idsse__matches. As of the current IDSSE
    -- Bundesliga open-data cut, two competitions ship:
    --   'DFL-COM-000001' -> 1. Bundesliga
    --   'DFL-COM-000002' -> 2. Bundesliga
    -- The mapping is stable for the published dataset; future IDSSE
    -- cuts may introduce more competitions.
    select distinct
        'idsse'                            as provider,
        competition_id                     as native_competition_id,
        cast(null as int)                  as competition_id_legacy,
        case competition_id
            when 'DFL-COM-000001' then '1. Bundesliga (DFL)'
            when 'DFL-COM-000002' then '2. Bundesliga (DFL)'
            else competition_id
        end                                as competition_name

    from {{ ref('stg_idsse__matches') }}
    where competition_id is not null

),

metrica_competitions as (

    -- PR 5a pseudo-competition: Metrica sample data has no competition
    -- metadata. Synthesise 'metrica-sample' so fct_passes.competition_key
    -- resolves non-NULL for Metrica rows and the Pass Map competition
    -- filter cascade shows "Metrica Sample Dataset".
    -- Ref: TODO #32, docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2.
    select distinct
        'metrica'                          as provider,
        'metrica-sample'                   as native_competition_id,
        cast(null as int)                  as competition_id_legacy,
        'Metrica Sample Dataset'           as competition_name

    from {{ ref('stg_metrica__matches') }}
    where native_match_id is not null

),

all_competitions as (

    select * from statsbomb_competitions
    union all
    select * from wyscout_competitions
    union all
    select * from idsse_competitions
    union all
    select * from metrica_competitions

),

deduped as (

    -- When the same (provider, native_competition_id) appears twice
    -- (shouldn't, but be defensive), keep the first row arbitrarily.
    -- StatsBomb vs Wyscout collisions on competition_id_legacy alone
    -- are NOT filtered here because their provider differs, so the
    -- grain key (provider, native_competition_id) is already unique.
    select *
    from (
        select
            all_competitions.*,
            row_number() over (
                partition by provider, native_competition_id
                order by competition_name
            ) as _rn
        from all_competitions
    )
    where _rn = 1

),

-- Seed data with enrichment attributes (INT-keyed; covers SB + WS only)
seed_metadata as (

    select
        competition_id,
        competition_name                    as seed_competition_name,
        country,
        gender

    from {{ ref('competition_metadata') }}

),

final as (

    select
        {{ generate_competition_key('d.provider', 'd.native_competition_id') }} as competition_key,
        d.provider,
        d.native_competition_id,
        d.competition_id_legacy                 as competition_id,
        coalesce(s.seed_competition_name, d.competition_name) as competition_name,
        coalesce(
            s.country,
            case
                when d.provider = 'idsse' then 'Germany'
                when d.provider = 'metrica' then cast(null as string)  -- Anonymised; no real country
            end
        ) as country,
        coalesce(
            s.gender,
            case
                when d.provider = 'idsse' then 'male'
                when d.provider = 'metrica' then cast(null as string)  -- Anonymised; no real gender
            end
        ) as gender

    from deduped d
    left join seed_metadata s
        on d.competition_id_legacy = s.competition_id

)

select * from final
