-- assert_idsse_spadl_set_piece_player_attributed.sql
-- Guard: IDSSE set-piece actions with play_player / foul_fouler in the
-- DFL XML must have player_id_native populated after SPADL adapter
-- qualifier resolution.
--
-- Fouls use foul_fouler (100% available); ThrowIn/FreeKick/GoalKick/
-- CornerKick use play_player (~98-100% available).  A small residual
-- (3-4 rows) may lack play_player in the DFL XML — that is acceptable.
-- This test alerts on systemic regressions (>5 rows per action type).

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
    severity='warn',
) }}

select action_type, count(*) as null_player_count
from {{ ref('stg_spadl__action_values') }}
where data_source = 'idsse'
  and action_type in ('throw_in', 'freekick_short', 'foul', 'goalkick', 'corner_short')
  and (player_id_native is null or player_id_native = '')
group by action_type
having count(*) > 5
