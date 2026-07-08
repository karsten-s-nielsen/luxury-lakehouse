-- fct_shot_xg.sql
-- Gold-layer canonical-SPADL pre-shot xG (xg_model_v3, two-mode gate + OOF
-- calibrator). One row per shot, keyed (match_key, action_id) — joinable to
-- fct_action_values / fct_action_context on that key. Replaces
-- fct_xg_predictions_v2 (now a view over this mart; see §C2 / Task 2.4).
--
-- ADR-013 (ML-inference-outputs pattern): the Python scorer emits only the
-- native shot key + predictions to bronze.xg_shot_predictions; this mart
-- resolves the Kimball surrogates (competition_key / team_key / player_key)
-- via an INNER JOIN to the identity fact fct_action_values on the shot key
-- (match_key, action_id). action_id is per-match, never global — the join is
-- ALWAYS on the (match_key, action_id) pair.
--
-- Enabled via var 'xg_v3_enabled' (default false; flipped on per-run in the
-- Databricks job config when the v3 scoring pipeline is scheduled).

{{ config(
    materialized='table',
    enabled=var('xg_v3_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

select
    p.match_key,
    p.action_id,
    p.data_source,
    p.xg,
    p.xg_ci_low,
    p.xg_ci_high,
    p.scoring_mode,
    p.ood_flag,
    -- Kimball surrogates inherited from the identity fact via INNER JOIN on the
    -- (match_key, action_id) shot key (ADR-013). Always populated — a shot with
    -- no fct_action_values row is caught by assert_shot_xg_key_in_action_values.
    av.competition_key,
    av.team_key,
    av.player_key

from {{ ref('stg_xg__shot_predictions') }} p
inner join {{ ref('fct_action_values') }} av
    on p.match_key = av.match_key
   and p.action_id = av.action_id
