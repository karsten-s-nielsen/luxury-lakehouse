{{ config(enabled=var('goalkeeper_enabled', false), severity='error') }}
-- ADR-018-style resolve test locking the B2 keying: every gk_player_key in the
-- defensive-line mart must resolve against dim_players.player_key. A row keyed on the
-- attacking team_key (the B2 bug) would not resolve as a goalkeeper player_key.
select l.gk_player_key
from {{ ref('fct_gk_defensive_line') }} l
left join {{ ref('dim_players') }} p on p.player_key = l.gk_player_key
where p.player_key is null
