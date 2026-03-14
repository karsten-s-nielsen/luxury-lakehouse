-- fct_xg_predictions.sql
-- Custom xG model predictions joined to fct_shots for referential integrity.
--
-- Contains logistic regression baseline and calibrated XGBoost xG values.
-- INNER JOIN to fct_shots ensures only valid shots are included.
-- One row per scored shot.

{{ config(
    materialized='table',
    enabled=var('xg_model_enabled', false),
    liquid_clustered_by=['match_id'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    p.match_id,
    p.competition_id,
    p.xg_logistic,
    p.xg_gradient_boosted

from {{ ref('stg_xg__predictions') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
