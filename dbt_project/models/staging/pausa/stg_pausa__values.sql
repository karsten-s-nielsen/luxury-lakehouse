-- stg_pausa__values.sql
-- Clean and deduplicate PAUSA values from the bronze layer, and resolve the
-- Kimball-surrogate `pass_id` that joins to fct_passes.
--
-- Dedup: ROW_NUMBER partitioned by (pass_id), latest _ingested_at wins.
-- Enabled by pausa_enabled toggle.
--
-- PR 7 (ADR-013 second application): source repointed from
-- pausa_gold.fct_pausa_values (Python writer direct-write) to bronze.pausa_values
-- (Python writer raw output). The gold mart fct_pausa_values is built by
-- dbt with contract: enforced: true and inherits Kimball FKs via INNER JOIN
-- to fct_passes on pass_id.
--
-- PR 7 hotfix: the bronze writer emits the **native** prefixed identifier
-- (e.g. `idsse_J03WMX_18226500000865` = `<provider>_<native_match_id>_<event_id>`).
-- fct_passes.pass_id is the dbt-side surrogate
-- `dbt_utils.generate_surrogate_key([match_key, event_id, data_source])`
-- (= `md5(match_key || '-' || event_id || '-' || data_source)`).
-- Without this staging-side surrogate computation the mart's
-- `INNER JOIN fct_passes ON pass_id = pass_id` evaluates to 0 rows.
-- Stage parses the prefixed id, joins dim_matches on
-- (provider, native_match_id) to resolve match_key, then recomputes the
-- identical surrogate via the same dbt_utils macro fct_passes uses. The
-- `relationships` schema test on `fct_pausa_values.pass_id → fct_passes.pass_id`
-- is the structural guard that catches this drift on first build.
--
-- Native components are preserved as `native_pass_id` / `native_match_id` /
-- `event_id` / `data_source` for downstream traceability and consumers that
-- still need the provider-native form (HF dataset republish path).

{{ config(enabled=var('pausa_enabled', false)) }}

with source as (

    select * from {{ source('pausa', 'pausa_values') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by pass_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

parsed as (

    -- Split the native prefixed pass_id into (data_source, native_match_id, event_id).
    -- Format: `<provider>_<native_match_id>_<event_id>`. `native_match_id` may
    -- contain underscores in principle, but PAUSA today is IDSSE-only and
    -- IDSSE match keys are 6-char alphanumeric (e.g. `J03WMX`). Splitting on
    -- the FIRST two underscores is sufficient and tolerates underscore-bearing
    -- event_ids if any provider ever introduces them. `match_id` from the bronze
    -- writer is itself prefixed (e.g. `idsse_J03WMX`); strip the prefix to get
    -- the native form that dim_matches stores.
    select
        cast(pass_id as string)                                 as native_pass_id,
        split_part(cast(pass_id as string), '_', 1)             as data_source,
        regexp_extract(cast(pass_id as string), '^[^_]+_([^_]+)_(.+)$', 1) as native_match_id,
        regexp_extract(cast(pass_id as string), '^[^_]+_([^_]+)_(.+)$', 2) as event_id,
        cast(match_id as string)                                as match_id_prefixed,
        cast(player_id as string)                               as player_id,
        cast(team as string)                                    as team,
        cast(period as int)                                     as period,
        cast(timestamp_seconds as double)                       as timestamp_seconds,
        cast(frame_id as int)                                   as frame_id,
        cast(temporal_judgment as double)                       as temporal_judgment,
        cast(spatial_selection as double)                       as spatial_selection,
        cast(pausa_score as double)                             as pausa_score,
        cast(actual_obso as double)                             as actual_obso,
        cast(peak_obso as double)                               as peak_obso,
        cast(optimal_obso as double)                            as optimal_obso,
        cast(receiver_x as double)                              as receiver_x,
        cast(receiver_y as double)                              as receiver_y

    from deduplicated
    where _row_num = 1

),

keyed as (

    -- Resolve match_key via dim_matches lookup, then compute the surrogate
    -- pass_id with the SAME dbt_utils.generate_surrogate_key recipe fct_passes
    -- uses ([match_key, event_id, data_source]). INNER JOIN means rows with
    -- no matching dim_matches entry are dropped — those would have failed the
    -- mart's INNER JOIN to fct_passes anyway, so dropping at staging is the
    -- earliest correct surface.
    select
        {{ dbt_utils.generate_surrogate_key([
            'dm.match_key',
            'p.event_id',
            'p.data_source',
        ]) }}                                                   as pass_id,
        p.native_pass_id,
        p.native_match_id,
        p.event_id,
        p.data_source,
        -- `match_id` preserves the bronze writer's prefixed form
        -- (e.g. `idsse_J03WMX`) for parity with the obso-pausa-values HF
        -- dataset's existing match_id column. Downstream consumers that
        -- want the unprefixed native key use `native_match_id` instead.
        p.match_id_prefixed                                     as match_id,
        p.player_id,
        p.team,
        p.period,
        p.timestamp_seconds,
        p.frame_id,
        p.temporal_judgment,
        p.spatial_selection,
        p.pausa_score,
        p.actual_obso,
        p.peak_obso,
        p.optimal_obso,
        p.receiver_x,
        p.receiver_y
    from parsed p
    inner join {{ ref('dim_matches') }} dm
        on dm.provider = p.data_source
       and dm.native_match_id = p.native_match_id

)

select * from keyed
