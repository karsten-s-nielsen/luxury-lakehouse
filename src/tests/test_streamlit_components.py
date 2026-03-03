"""Tests for streamlit_app.components (pitch + charts) visualization functions."""

from __future__ import annotations

import matplotlib.figure
import numpy as np
import pandas as pd

from streamlit_app.components.charts import plot_match_comparison_bars, plot_player_radar
from streamlit_app.components.pitch import (
    plot_heatmap,
    plot_pass_map,
    plot_pass_network,
    plot_physics_pitch_control,
    plot_pitch_control,
    plot_shot_map,
)
from streamlit_app.pages.pass_network import _build_network


class TestPlotShotMap:
    """Test shot map visualization."""

    def test_returns_figure_with_data(self) -> None:
        shots = pd.DataFrame(
            {
                "location_x": [100.0, 105.0, 110.0],
                "location_y": [40.0, 35.0, 45.0],
                "statsbomb_xg": [0.1, 0.5, 0.8],
                "is_goal": [0, 0, 1],
            }
        )
        fig = plot_shot_map(shots, title="Test Shots")
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        shots = pd.DataFrame({"location_x": [], "location_y": [], "statsbomb_xg": [], "is_goal": []})
        fig = plot_shot_map(shots)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_handles_all_goals(self) -> None:
        shots = pd.DataFrame(
            {
                "location_x": [110.0],
                "location_y": [40.0],
                "statsbomb_xg": [0.9],
                "is_goal": [1],
            }
        )
        fig = plot_shot_map(shots)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_handles_no_goals(self) -> None:
        shots = pd.DataFrame(
            {
                "location_x": [100.0, 105.0],
                "location_y": [40.0, 35.0],
                "statsbomb_xg": [0.1, 0.2],
                "is_goal": [0, 0],
            }
        )
        fig = plot_shot_map(shots)
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotPassMap:
    """Test pass map visualization."""

    def test_returns_figure_with_data(self) -> None:
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0],
                "start_y": [40.0, 30.0],
                "end_x": [60.0, 80.0],
                "end_y": [40.0, 35.0],
                "is_complete": [1, 0],
                "is_progressive": [1, 0],
            }
        )
        fig = plot_pass_map(passes)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        passes = pd.DataFrame(
            {
                "start_x": [],
                "start_y": [],
                "end_x": [],
                "end_y": [],
                "is_complete": [],
                "is_progressive": [],
            }
        )
        fig = plot_pass_map(passes)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_no_progressive_highlight(self) -> None:
        passes = pd.DataFrame(
            {
                "start_x": [30.0],
                "start_y": [40.0],
                "end_x": [60.0],
                "end_y": [40.0],
                "is_complete": [1],
                "is_progressive": [0],
            }
        )
        fig = plot_pass_map(passes, highlight_progressive=False)
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotPlayerRadar:
    """Test radar chart visualization."""

    def test_returns_figure_single_player(self) -> None:
        players = [{"goals": 0.5, "xg": 0.4, "passes": 50.0}]
        fig = plot_player_radar(
            players,
            metrics=["goals", "xg", "passes"],
            labels=["Goals/90", "xG/90", "Passes/90"],
            ranges=[(0, 1.5), (0, 1.5), (0, 80)],
        )
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_multiple_players(self) -> None:
        players = [
            {"goals": 0.5, "xg": 0.4, "passes": 50.0},
            {"goals": 0.8, "xg": 0.6, "passes": 30.0},
        ]
        fig = plot_player_radar(
            players,
            metrics=["goals", "xg", "passes"],
            labels=["Goals/90", "xG/90", "Passes/90"],
            ranges=[(0, 1.5), (0, 1.5), (0, 80)],
        )
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotMatchComparisonBars:
    """Test match comparison bar chart."""

    def test_returns_figure(self) -> None:
        fig = plot_match_comparison_bars(
            home_vals=[15.0, 5.0, 2.1],
            away_vals=[10.0, 3.0, 1.5],
            labels=["Shots", "SOT", "xG"],
            home_name="Team A",
            away_name="Team B",
        )
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_handles_single_metric(self) -> None:
        fig = plot_match_comparison_bars(
            home_vals=[3.0],
            away_vals=[1.0],
            labels=["Goals"],
        )
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotPitchControl:
    """Test pitch control visualization with Voronoi tessellation."""

    def test_returns_figure_with_data(self) -> None:
        players = pd.DataFrame(
            {
                "x": [20.0, 40.0, 60.0, 80.0, 100.0, 30.0, 50.0, 70.0, 90.0, 110.0],
                "y": [40.0, 30.0, 50.0, 20.0, 60.0, 60.0, 70.0, 10.0, 40.0, 50.0],
                "team": ["home"] * 5 + ["away"] * 5,
                "player_id": [f"p{i}" for i in range(10)],
            }
        )
        fig = plot_pitch_control(players, ball_x=60.0, ball_y=40.0)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        players = pd.DataFrame({"x": [], "y": [], "team": [], "player_id": []})
        fig = plot_pitch_control(players)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_with_velocity_arrows(self) -> None:
        players = pd.DataFrame(
            {
                "x": [30.0, 50.0, 70.0, 90.0],
                "y": [40.0, 30.0, 50.0, 20.0],
                "team": ["home", "home", "away", "away"],
                "player_id": ["p1", "p2", "p3", "p4"],
                "velocity_x": [1.0, -0.5, 0.3, -0.8],
                "velocity_y": [0.5, 0.2, -0.4, 0.1],
            }
        )
        fig = plot_pitch_control(players, show_velocity=True)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_with_ball_position(self) -> None:
        players = pd.DataFrame(
            {
                "x": [30.0, 50.0, 70.0, 90.0],
                "y": [40.0, 30.0, 50.0, 20.0],
                "team": ["home", "home", "away", "away"],
                "player_id": ["p1", "p2", "p3", "p4"],
            }
        )
        fig = plot_pitch_control(players, ball_x=60.0, ball_y=40.0)
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotHeatmap:
    """Test heat map visualization."""

    def test_returns_figure_with_data(self) -> None:
        actions = pd.DataFrame({"x": [30.0, 50.0, 70.0, 100.0], "y": [40.0, 20.0, 60.0, 35.0]})
        fig = plot_heatmap(actions, title="Test Heat Map")
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        actions = pd.DataFrame({"x": [], "y": []})
        fig = plot_heatmap(actions)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_custom_bins(self) -> None:
        actions = pd.DataFrame({"x": [30.0, 50.0, 70.0], "y": [40.0, 20.0, 60.0]})
        fig = plot_heatmap(actions, bins=(6, 4))
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_custom_cmap(self) -> None:
        actions = pd.DataFrame({"x": [30.0, 50.0], "y": [40.0, 20.0]})
        fig = plot_heatmap(actions, cmap="YlOrRd")
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotPassNetwork:
    """Test pass network visualization."""

    def test_returns_figure_with_data(self) -> None:
        nodes = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "player_display_name": ["A", "B", "C"],
                "avg_x": [30.0, 50.0, 70.0],
                "avg_y": [40.0, 30.0, 50.0],
                "pass_count": [10, 8, 12],
            }
        )
        edges = pd.DataFrame(
            {
                "passer_id": [1, 2],
                "receiver_id": [2, 3],
                "pair_count": [5, 3],
                "avg_start_x": [30.0, 50.0],
                "avg_start_y": [40.0, 30.0],
                "avg_end_x": [50.0, 70.0],
                "avg_end_y": [30.0, 50.0],
            }
        )
        fig = plot_pass_network(nodes, edges)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_nodes(self) -> None:
        nodes = pd.DataFrame(columns=pd.Index(["player_id", "player_display_name", "avg_x", "avg_y", "pass_count"]))
        edges = pd.DataFrame(
            columns=pd.Index(
                [
                    "passer_id",
                    "receiver_id",
                    "pair_count",
                    "avg_start_x",
                    "avg_start_y",
                    "avg_end_x",
                    "avg_end_y",
                ]
            )
        )
        fig = plot_pass_network(nodes, edges)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_edges(self) -> None:
        nodes = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_display_name": ["A", "B"],
                "avg_x": [30.0, 60.0],
                "avg_y": [40.0, 40.0],
                "pass_count": [5, 5],
            }
        )
        edges = pd.DataFrame(
            columns=pd.Index(
                [
                    "passer_id",
                    "receiver_id",
                    "pair_count",
                    "avg_start_x",
                    "avg_start_y",
                    "avg_end_x",
                    "avg_end_y",
                ]
            )
        )
        fig = plot_pass_network(nodes, edges)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_single_edge(self) -> None:
        nodes = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_display_name": ["Player A", "Player B"],
                "avg_x": [30.0, 60.0],
                "avg_y": [40.0, 40.0],
                "pass_count": [3, 3],
            }
        )
        edges = pd.DataFrame(
            {
                "passer_id": [1],
                "receiver_id": [2],
                "pair_count": [3],
                "avg_start_x": [30.0],
                "avg_start_y": [40.0],
                "avg_end_x": [60.0],
                "avg_end_y": [40.0],
            }
        )
        fig = plot_pass_network(nodes, edges)
        assert isinstance(fig, matplotlib.figure.Figure)


class TestBuildNetwork:
    """Test pass network construction logic."""

    def test_builds_nodes_and_edges(self) -> None:
        passes = pd.DataFrame(
            {
                "player_id": [1, 1, 1, 2, 2],
                "pass_recipient_id": [2, 2, 2, 3, 3],
                "passer_name": ["A", "A", "A", "B", "B"],
                "receiver_name": ["B", "B", "B", "C", "C"],
                "start_x": [30.0, 32.0, 28.0, 50.0, 52.0],
                "start_y": [40.0, 42.0, 38.0, 30.0, 32.0],
                "end_x": [50.0, 48.0, 52.0, 70.0, 68.0],
                "end_y": [30.0, 32.0, 28.0, 50.0, 48.0],
            }
        )
        nodes, edges = _build_network(passes, min_pair_count=1)
        assert len(nodes) == 3
        assert len(edges) == 2

    def test_min_pair_count_filters_edges(self) -> None:
        passes = pd.DataFrame(
            {
                "player_id": [1, 1, 1, 2],
                "pass_recipient_id": [2, 2, 2, 3],
                "passer_name": ["A", "A", "A", "B"],
                "receiver_name": ["B", "B", "B", "C"],
                "start_x": [30.0, 32.0, 28.0, 50.0],
                "start_y": [40.0, 42.0, 38.0, 30.0],
                "end_x": [50.0, 48.0, 52.0, 70.0],
                "end_y": [30.0, 32.0, 28.0, 50.0],
            }
        )
        _nodes, edges = _build_network(passes, min_pair_count=3)
        # Only 1->2 has 3 passes; 2->3 has 1
        assert len(edges) == 1
        assert edges.iloc[0]["passer_id"] == 1
        assert edges.iloc[0]["receiver_id"] == 2

    def test_empty_passes(self) -> None:
        passes = pd.DataFrame(
            columns=pd.Index(
                [
                    "player_id",
                    "pass_recipient_id",
                    "passer_name",
                    "receiver_name",
                    "start_x",
                    "start_y",
                    "end_x",
                    "end_y",
                ]
            )
        )
        nodes, edges = _build_network(passes)
        assert len(nodes) == 0
        assert len(edges) == 0


class TestPlotPhysicsPitchControl:
    """Test physics-based pitch control visualization."""

    def _sample_players(self, with_velocity: bool = False) -> pd.DataFrame:
        data: dict[str, list[object] | list[str]] = {
            "x": [20.0, 40.0, 60.0, 80.0, 100.0, 30.0, 50.0, 70.0, 90.0, 110.0],
            "y": [40.0, 30.0, 50.0, 20.0, 60.0, 60.0, 70.0, 10.0, 40.0, 50.0],
            "team": ["home"] * 5 + ["away"] * 5,
            "player_id": [f"p{i}" for i in range(10)],
        }
        if with_velocity:
            data["velocity_x"] = [1.0, -0.5, 0.3, -0.8, 0.0, -1.0, 0.5, -0.3, 0.8, 0.0]
            data["velocity_y"] = [0.5, 0.2, -0.4, 0.1, 0.0, -0.5, -0.2, 0.4, -0.1, 0.0]
        return pd.DataFrame(data)

    def _sample_surface(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        grid_x = np.linspace(0, 120, 50)
        grid_y = np.linspace(0, 80, 32)
        surface = np.random.default_rng(42).random((32, 50))
        return grid_x, grid_y, surface

    def test_returns_figure_with_data(self) -> None:
        players = self._sample_players()
        grid_x, grid_y, surface = self._sample_surface()
        fig = plot_physics_pitch_control(players, surface, grid_x, grid_y)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        players = pd.DataFrame({"x": [], "y": [], "team": [], "player_id": []})
        grid_x, grid_y, surface = self._sample_surface()
        fig = plot_physics_pitch_control(players, surface, grid_x, grid_y)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_with_velocity_arrows(self) -> None:
        players = self._sample_players(with_velocity=True)
        grid_x, grid_y, surface = self._sample_surface()
        fig = plot_physics_pitch_control(players, surface, grid_x, grid_y, show_velocity=True)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_with_ball_position(self) -> None:
        players = self._sample_players()
        grid_x, grid_y, surface = self._sample_surface()
        fig = plot_physics_pitch_control(players, surface, grid_x, grid_y, ball_x=60.0, ball_y=40.0)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_colorbar_present(self) -> None:
        players = self._sample_players()
        grid_x, grid_y, surface = self._sample_surface()
        fig = plot_physics_pitch_control(players, surface, grid_x, grid_y)
        # The figure should have more than 1 axes (pitch + colorbar)
        assert len(fig.get_axes()) > 1
