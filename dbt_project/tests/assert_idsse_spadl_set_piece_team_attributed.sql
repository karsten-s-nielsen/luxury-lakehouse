-- assert_idsse_spadl_set_piece_team_attributed.sql
-- Guard: IDSSE set-piece and foul actions must have team_id_native populated.
--
-- DFL XML stores team attribution for ThrowIn/FreeKick/GoalKick/CornerKick
-- in play_team/throwin_team qualifiers, and for Foul in foul_team_fouler.
-- The SPADL adapter resolves these into the generic team column before
-- silly-kicks conversion, so team_id_native should be non-NULL for all
-- these action types.  This test returns rows that violate the invariant.

{{ config(
    tags=['post_deploy_only'],
    enabled=var('include_post_deploy_tests', false),
) }}

select action_type, count(*) as null_team_count
from {{ ref('stg_spadl__action_values') }}
where data_source = 'idsse'
  and action_type in ('throw_in', 'freekick_short', 'foul', 'goalkick', 'corner_short')
  and (team_id_native is null or team_id_native = '')
group by action_type
