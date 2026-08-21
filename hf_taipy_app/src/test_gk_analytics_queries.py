"""GK Analytics query builders — SQL-builder unit tests (no DB)."""

from queries.gk_analytics import (
    GK_TRACKING_PROVIDERS,
    build_distribution_profile_sql,
    build_gk_competition_lov_sql,
    build_gk_keeper_lov_sql,
    build_goals_prevented_sql,
    build_line_context_sql,
    build_sweeper_stats_sql,
)


def test_provider_gate():
    assert GK_TRACKING_PROVIDERS == ("gradientsports", "idsse", "skillcorner")


def test_competition_lov_restricted_to_tracking_providers():
    sql, params = build_gk_competition_lov_sql()
    assert "fct_gk_tracking_stats_synced" in sql and "dim_competitions_synced" in sql
    assert "data_source IN (" in sql and "n_keepers" in sql
    assert params == GK_TRACKING_PROVIDERS


def test_keeper_lov_per_competition_no_canonical():
    sql, params = build_gk_keeper_lov_sql(competition_key=77)
    assert "fct_gk_tracking_stats_synced" in sql and "dim_players_synced" in sql
    assert "dm.competition_key = %s" in sql and params == (77,)
    assert "canonical_player_key" not in sql and "SELECT *" not in sql


def test_distribution_profile_sql_action_grain_two_axes():
    sql, params = build_distribution_profile_sql(competition_key=5)
    # action-grain profile (NOT the degenerate preset columns), floored at n>=20; re-homed onto xt_gk_v2.
    assert "fct_gk_tracking_actions_synced" in sql
    assert "share_adds_threat" in sql and "mean_progress_m" in sql and "mean_completion" in sql
    assert "a.xt_gk_v2 > 0" in sql and "a.xt_gk_v2 IS NOT NULL" in sql and "HAVING COUNT(*) >= 20" in sql
    # composite breakdown: the 4 additive v2 terms surface for the value tile.
    assert all(c in sql for c in ("mean_v2_position", "mean_v2_pev", "mean_v2_retention_loss", "mean_v2_dzv"))
    assert "dist_xt_gk_counter_mean" not in sql  # the dead preset columns are gone
    assert "m.competition_key = %s" in sql and "SELECT *" not in sql and params == (5,)


def test_sweeper_stats_sql_per_competition():
    sql, params = build_sweeper_stats_sql(competition_key=5)
    assert "reachable_area_mean_m2" in sql and "n_defended_actions" in sql
    assert "dm.competition_key = %s" in sql and params == (5,)


def test_line_context_sql_reads_mart_filtered_by_competition():
    sql, params = build_line_context_sql(competition_key=9)
    # B2/S3: small per-defending-GK mart, NOT a live scan of fct_action_context.
    assert "fct_gk_defensive_line_synced" in sql
    assert "fct_action_context_synced" not in sql and "team_key" not in sql
    assert "competition_key = %s" in sql and params == (9,)


def test_goals_prevented_sql_reads_pre_aggregated_pooled_mart_by_competition():
    sql, params = build_goals_prevented_sql(competition_key=3)
    assert "fct_gk_shot_stopping_pooled_synced" in sql
    assert "goals_prevented_ci_low" in sql and "goals_prevented_ci_high" in sql and "low_sample" in sql
    assert "competition_key = %s" in sql and "SUM(" not in sql.upper() and params == (3,)
