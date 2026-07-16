-- ADR-030 / M13-ownership guard. bronze.spadl_action_context must have exactly one row per
-- (data_source, match_id, action_id). AC-1's work-unit ownership guarantees this; if it ever
-- breaks, the staging dedup would silently collapse the dups and NO mart-level test would see it.
-- This is the ONLY layer where an ownership regression is visible. See ADR-068 / spec review-2.
{{ config(severity='error') }}

select data_source, match_id, action_id, count(*) as n
from {{ source('action_context', 'spadl_action_context') }}
group by 1, 2, 3
having count(*) > 1
