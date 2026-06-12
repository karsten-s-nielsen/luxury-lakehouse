"""GK tracking queries — SQL-builder unit tests (no DB)."""

from queries.gk_tracking import (
    GK_TRACKING_PROVIDERS,
    build_gk_actions_sql,
    build_gk_lov_sql,
    build_gk_pool_stats_sql,
    build_gk_stats_sql,
    build_scene_frame_sql,
)


def test_provider_gate_constant():
    assert GK_TRACKING_PROVIDERS == ("gradientsports", "idsse", "skillcorner")
    assert "metrica" not in GK_TRACKING_PROVIDERS  # owner decision: anonymized players


def test_lov_sql_gates_providers_and_limits():
    sql, params = build_gk_lov_sql()
    assert "fct_gk_tracking_stats_synced" in sql and "dim_players_synced" in sql
    # review N2: derive the expected placeholder string from the constant — adding a provider
    # later must not require editing this assertion (single-source property of M4)
    expected = f"data_source IN ({', '.join(['%s'] * len(GK_TRACKING_PROVIDERS))})"
    assert expected in sql and params == GK_TRACKING_PROVIDERS
    assert "LIMIT 500" in sql


def test_actions_sql_filters_gk_and_limits():
    sql, params = build_gk_actions_sql(gk_player_key="abc", family="distribution")
    assert "fct_gk_tracking_actions_synced" in sql
    assert "gk_was_distributing" in sql and "xt_gk IS NOT NULL" in sql
    assert params[-1] == "abc" and "LIMIT 2000" in sql
    assert "SELECT *" not in sql  # A4: explicit column list only


def test_actions_sql_defense_family_keys_on_defending_gk():
    sql, _params = build_gk_actions_sql(gk_player_key="abc", family="defense")
    assert "defending_gk_player_key = %s" in sql


def test_actions_sql_rejects_unknown_family():
    import pytest

    with pytest.raises(ValueError):
        build_gk_actions_sql(gk_player_key="abc", family="nope")


def test_stats_sql_single_gk_and_explicit_columns():
    sql, params = build_gk_stats_sql(gk_player_key="abc")
    assert "gk_player_key = %s" in sql and "LIMIT 500" in sql
    assert "SELECT *" not in sql  # review M3
    assert params[-1] == "abc"


def test_pool_stats_sql_aggregates_all_gks():
    # review H2: the Tab 1 bump chart + every "vs sample" delta come from THIS query
    sql, params = build_gk_pool_stats_sql(min_distributions=10)
    assert "GROUP BY s.gk_player_key" in sql and "LIMIT 500" in sql
    assert "dist_xt_gk_counter_mean" in sql and "NULLIF(SUM(s.n_distributions), 0)" in sql
    assert params == (*GK_TRACKING_PROVIDERS, 10)


def test_scene_frame_sql_bounds():
    sql, params = build_scene_frame_sql(match_key=42, period=2, frame=123)
    assert "fct_tracking_frames_synced" in sql and "LIMIT 60" in sql
    assert params == (42, 2, 123)


def test_freshness_sql_reads_own_tables():
    # observability O2: the page's freshness badge reflects the page's OWN marts
    from queries.gk_tracking import build_gk_freshness_sql

    sql, params = build_gk_freshness_sql()
    assert "fct_gk_tracking_stats_synced" in sql and params == GK_TRACKING_PROVIDERS
