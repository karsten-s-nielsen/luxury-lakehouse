-- fct_xg_predictions_v2.sql
-- BACK-COMPAT VIEW (Task 2.4 / §C2). Historically the gold v2 xG mart (Deep
-- Sets set encoder + MC dropout CIs, ADR-013 INNER JOIN fct_shots on shot_id).
-- Superseded by fct_shot_xg (canonical-SPADL xg_model_v3), which is now the
-- single source of pre-shot xG for ALL providers. To keep existing consumers
-- (Taipy pages, the fct_xg_predictions_v2_synced Lakebase table) working
-- unchanged, v2 is redefined as a VIEW that projects fct_shot_xg back into the
-- EXACT legacy schema. Valid in this delivery because xg_model_v3 is
-- context-aware for StatsBomb (SB-360 included), so the values are
-- equal-or-better than the legacy v2 model — no downgrade (spec N1).
--
-- Legacy schema (fact-checked live 2026-07-07):
--   shot_id STRING, match_key LONG, competition_key LONG, competition_id INT,
--   team_key LONG, player_key LONG, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE,
--   xg_ci_upper DOUBLE.
--
-- shot_id is reconstructed via the bridge:
--   fct_shot_xg.(match_key, action_id) -> fct_action_values.original_event_id
--   -> fct_shots.event_id / shot_id.
-- 1:1-ness of that bridge is guarded by assert_xg_v2_view_shot_id_1to1.sql.
-- Restricted to data_source IN ('statsbomb', 'wyscout') — the event-only
-- providers the legacy v2 model ever scored (legacy coverage).
--
-- Gated on 'xg_v3_enabled' (was 'xg_v2_enabled'): the view now projects
-- fct_shot_xg, so it can only exist when the v3 mart does.

{{ config(
    materialized='view',
    enabled=var('xg_v3_enabled', false),
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

select
    cast(s.shot_id as string)          as shot_id,
    cast(s.match_key as bigint)        as match_key,
    cast(s.competition_key as bigint)  as competition_key,
    cast(s.competition_id as int)      as competition_id,
    cast(s.team_key as bigint)         as team_key,
    cast(s.player_key as bigint)       as player_key,
    cast(sx.xg as double)              as xg_set_encoder,
    cast(sx.xg_ci_low as double)       as xg_ci_lower,
    cast(sx.xg_ci_high as double)      as xg_ci_upper

from {{ ref('fct_shot_xg') }} sx
-- (match_key, action_id) -> native original_event_id.
inner join {{ ref('fct_action_values') }} av
    on sx.match_key = av.match_key
   and sx.action_id = av.action_id
-- native event id -> fct_shots surrogate key + legacy Kimball columns.
inner join {{ ref('fct_shots') }} s
    on av.original_event_id = s.event_id
   and av.data_source = s.data_source
where sx.data_source in ('statsbomb', 'wyscout')
