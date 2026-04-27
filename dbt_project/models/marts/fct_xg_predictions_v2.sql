-- fct_xg_predictions_v2.sql
-- Gold-layer v2 xG predictions (Deep Sets set encoder with MC dropout
-- confidence intervals). Keys inherited via INNER JOIN to fct_shots on
-- shot_id per ADR-013 (consumer-side ML inference output pattern;
-- counterpart to ADR-012 producer-side weight delivery).
--
-- First mart applying ADR-013. See
-- docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md.
--
-- Disabled by default (var xg_v2_enabled=false); flipped on per-run
-- in the Databricks job config when the v2 scoring pipeline is
-- scheduled. See workflow-cards/wf-xg-v2.yaml.

{{ config(
    materialized='table',
    enabled=var('xg_v2_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,
    -- PR 7 (ADR-011 close-out): pull-through team_key + player_key from
    -- fct_shots; ADR-013 INNER JOIN already inherits all surrogate FKs.
    s.team_key,
    s.player_key,
    p.xg_set_encoder,
    p.xg_ci_lower,
    p.xg_ci_upper

from {{ ref('stg_xg__predictions_v2') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
