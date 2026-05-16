-- stg_skillcorner__events.sql
-- Minimal staging passthrough for SkillCorner dynamic events.
--
-- The bronze table preserves all ~294 source columns from the events CSV
-- per bronze-completeness. This staging model is a simple source reference
-- for downstream consumers that need to query events directly.

with source as (

    select * from {{ source('skillcorner', 'skillcorner_events') }}

)

select * from source
