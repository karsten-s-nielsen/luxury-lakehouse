-- stg_wyscout__teams.sql
-- Wyscout team roster (Figshare teams.json, Pappalardo et al. 2019, article 7765310).
--
-- 142 teams across 7 competitions (2017/18 season). PR 5a addition —
-- closes the pre-existing team-name coverage gap for dim_teams Wyscout rows.
--
-- Area is a nested JSON struct (name, id, alpha2code, alpha3code) — parsed
-- here with get_json_object. Keeps full JSON passthrough for lineage.

with source as (

    select * from {{ source('wyscout', 'wyscout_teams') }}

),

final as (

    select
        cast(wyId as int)                                as team_id,
        officialName                                     as official_name,
        name                                             as team_name,
        city,
        get_json_object(area, '$.name')                  as area_name,
        get_json_object(area, '$.alpha3code')            as area_alpha3,
        get_json_object(area, '$.alpha2code')            as area_alpha2,
        type                                             as team_type,

        -- Bronze passthroughs surfaced for Kimball completeness. Names intentionally
        -- retain the bronze camelCase so consumers can cross-reference the Figshare
        -- JSON schema without additional mapping.
        cast(wyId as bigint)                             as wyId,
        officialName                                     as officialName,
        city                                             as city_raw,
        area                                             as area,
        type                                             as type,
        _ingested_at                                     as _ingested_at,

        'wyscout'                                        as data_source

    from source
    where wyId is not null

)

select * from final
