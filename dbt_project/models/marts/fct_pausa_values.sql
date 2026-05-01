-- fct_pausa_values.sql
-- Gold-layer per-pass PAUSA decomposition: temporal judgment, spatial
-- selection, and composite PAUSA score. Keys inherited via INNER JOIN to
-- fct_passes on pass_id per ADR-013 (consumer-side ML inference output
-- pattern; second application after PR 3 fct_xg_predictions_v2).
--
-- See docs/superpowers/adrs/ADR-013-ml-inference-outputs-dbt-mart.md.
--
-- Disabled by default (var pausa_enabled=false); flipped on per-run in the
-- Databricks job config when the PAUSA scoring pipeline is scheduled. See
-- workflow-cards/wf-obso-pausa.yaml.
--
-- Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying
-- Optimal Pass Timing Beyond Speed." MIT Sloan 2026.

{{ config(
    materialized='table',
    enabled=var('pausa_enabled', false),
    liquid_clustered_by=['match_key'],
    on_schema_change='fail',
    contract={'enforced': true},
    tags=['marts', 'output_mart']
) }}

select
    p.pass_id,
    p.match_id,
    fp.match_key,
    fp.team_key,
    fp.passer_player_key                        as player_key,
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

from {{ ref('stg_pausa__values') }} p
inner join {{ ref('fct_passes') }} fp on p.pass_id = fp.pass_id
