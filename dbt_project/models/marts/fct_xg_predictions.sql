-- fct_xg_predictions.sql
-- Gold-layer v1 xG predictions (logistic + calibrated XGBoost). Keys
-- inherited via INNER JOIN to fct_shots on shot_id per ADR-013. One row
-- per scored shot.
--
-- Disabled by default (var xg_model_enabled=false); flipped on per-run
-- in the Databricks workflow config when the v1 scoring pipeline is
-- scheduled. See workflow-cards/wf-xg.yaml.

{{ config(
    materialized='table',
    enabled=var('xg_model_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    s.match_key,
    s.competition_key,
    s.competition_id,
    p.xg_logistic,
    p.xg_gradient_boosted

from {{ ref('stg_xg__predictions') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
