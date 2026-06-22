{{ config(
    materialized='table',
    enabled=var('goalkeeper_enabled', false),
    tags=['marts', 'output_mart']
) }}
-- fct_gk_defensive_line.sql
-- Per-(defending GK, competition) defensive-line + shape aggregate for the tracking cohort.
-- Keyed on defending_gk_player_key (the line IN FRONT OF that keeper) — NOT team_key, which is
-- the in-possession/attacking team (B2). Small (a few hundred rows); precomputed so the page
-- never live-scans fct_action_context (S3).
--
-- avg_line_height_m = the defending back line's average DISTANCE FROM ITS OWN GOAL (metres; higher
-- = higher line). silly-kicks' `defensive_line_x` is an ABSOLUTE home-LTR coordinate (compute_
-- defensive_line: home team defends x=0 so its line is low-x; away team defends x=105 so its line
-- is high-x), so the raw value is NOT comparable across teams and is bimodal home-vs-away. We
-- normalise per action to own-goal distance: orientation is recovered from back_line_high_x, which
-- is max(x) for an x=0 defender but min(x) for an x=105 defender — so back_line_high_x > defensive_
-- line_x ⟺ the team defends x=0. (Verified: 99.6% of WC actions defend x=105.)
select
    {{ dbt_utils.generate_surrogate_key(['a.defending_gk_player_key', 'm.competition_key', 'a.data_source']) }}
                                                      as gk_defensive_line_id,
    a.defending_gk_player_key                         as gk_player_key,
    m.competition_key,
    a.data_source,
    avg(
        case
            when a.back_line_high_x is not null and a.back_line_high_x > a.defensive_line_x
                then a.defensive_line_x
            else 105.0 - a.defensive_line_x
        end
    )                                                 as avg_line_height_m,
    avg(a.team_shape_team_width_defending)            as avg_width,
    avg(a.compactness_x)                              as avg_compactness,
    count(*)                                          as n_actions
from {{ ref('fct_action_context') }} a
join {{ ref('dim_matches') }} m on m.match_key = a.match_key
where a.defending_gk_player_key is not null
  and a.defensive_line_x is not null
  and a.data_source in ('gradientsports', 'idsse', 'skillcorner')
group by a.defending_gk_player_key, m.competition_key, a.data_source
