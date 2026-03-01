"""Tests for streamlit_app.components (pitch + charts) visualization functions."""

from __future__ import annotations

import matplotlib.figure
import pandas as pd

from streamlit_app.components.charts import plot_match_comparison_bars, plot_player_radar
from streamlit_app.components.pitch import plot_pass_map, plot_pitch_control, plot_shot_map


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
