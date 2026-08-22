-- stg_action_defensive.sql
-- Staging view for the per-action defending-team defensive-credit aggregate (spec §7.5, Task 17d;
-- ADR-013 writer-fed). Source: bronze.action_defensive_credit, written by
-- ingestion.defensive_credit_writer. Deduplicates by (data_source, match_id, action_id), latest
-- _ingested_at wins; renames match_id -> native_match_id for the Kimball-side resolution in
-- fct_action_defensive.

with source as (

    select * from {{ source('defensive_credit', 'action_defensive_credit') }}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by data_source, match_id, action_id
            order by _ingested_at desc
        ) as _row_num
    from source

),

cleaned as (

    select
        cast(data_source as string)            as data_source,
        cast(match_id as string)               as native_match_id,
        cast(period_id as bigint)              as period_id,
        cast(action_id as bigint)              as action_id,
        cast(defensive_credit_net as double)   as defensive_credit_net,
        cast(defensive_credit_plus as double)  as defensive_credit_plus,
        cast(defensive_credit_minus as double) as defensive_credit_minus,
        cast(n_defensive_credits as bigint)    as n_defensive_credits

    from deduplicated
    where _row_num = 1

)

select * from cleaned
