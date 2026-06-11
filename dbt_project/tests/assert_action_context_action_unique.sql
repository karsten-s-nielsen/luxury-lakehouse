-- M13 single-owner invariant (ADR-040 amendment, 2026-06-11): the action-context
-- pipeline assigns every SPADL action to exactly ONE frame batch, so bronze must
-- hold exactly one row per (data_source, match_id, period_id, action_id).
-- Duplicates mean the dispatch ownership map de-aligned (e.g. a frames-side
-- absolute clock — the SkillCorner P2 class produced 2 duplicate action_ids in
-- period 1 before the dispatch re-base). The local driver fails loud pre-write;
-- this test is the net for the distributed Spark path, where the per-batch UDFs
-- cannot see each other's output.
select
    data_source,
    match_id,
    period_id,
    action_id,
    count(*) as n_rows
from {{ source('action_context', 'spadl_action_context') }}
group by data_source, match_id, period_id, action_id
having count(*) > 1
