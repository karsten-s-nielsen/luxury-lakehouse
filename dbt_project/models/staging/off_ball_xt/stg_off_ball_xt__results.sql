-- stg_off_ball_xt__results.sql
-- Clean and deduplicate Off-Ball xT results from the bronze layer.
--
-- Dedup: ROW_NUMBER partitioned by (player_id, match_id, source_provider),
-- latest _ingested_at wins. source_provider is written at bronze ingestion
-- time (PR-1.5 fix) — no more derivation from match_id patterns.

with source as (

    select * from {{ source('off_ball_xt', 'off_ball_xt_results') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by player_id, match_id, source_provider
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    -- PR 7 hotfix #3 followup: derive Metrica synth player_id by JOINing
    -- stg_metrica__team_players. Bronze off_ball_xt Metrica player_id is the
    -- bare map key ('5', '11', ...); dim_players uses synth form
    -- 'metrica_<match>_<side>_<map_key>'. team_players has both forms keyed on
    -- (match_id, player_key_in_map), so the JOIN converts bare → synth. IDSSE /
    -- SkillCorner bypass via fallback (their bronze player_id is already in
    -- dim-compatible form).
    select
        coalesce(mtp.native_player_id, cast(deduplicated.player_id as string)) as player_id,
        -- Strip the `idsse_` prefix at staging boundary so mart-side JOINs to
        -- dim_matches.native_match_id match. Legacy data may still have the
        -- prefix; new ingestion writes clean match_id without prefix.
        regexp_replace(cast(deduplicated.match_id as string), '^idsse_', '') as match_id,
        cast(total_off_ball_xt as double)  as total_off_ball_xt,
        cast(avg_off_ball_xt as double)    as avg_off_ball_xt,
        cast(frames_sampled as int)        as frames_sampled,

        -- source_provider: read directly from bronze (written at ingestion).
        -- Legacy fallback for rows ingested before PR-1.5: derive from match_id
        -- pattern. NULL-safe: if source_provider is NULL, apply legacy derivation.
        coalesce(
            deduplicated.source_provider,
            case
                when deduplicated.match_id like 'idsse_%'        then 'idsse'
                when deduplicated.match_id like 'Sample_Game_%'  then 'metrica'
                else 'skillcorner'
            end
        )                                  as source_provider

    from deduplicated
    -- LEFT JOIN per-match-side team_players for Metrica synth player_id resolution.
    -- Active only on Metrica rows (Sample_Game_*). For IDSSE/SkillCorner, mtp.*
    -- is NULL and the COALESCE falls through to the bronze player_id unchanged.
    left join {{ ref('stg_metrica__team_players') }} mtp
        on  deduplicated.match_id = mtp.match_id
       and cast(deduplicated.player_id as string) = mtp.player_key_in_map
    where _row_num = 1

)

select * from cleaned
