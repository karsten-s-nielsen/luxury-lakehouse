"""Tests for streamlit_app.components (pitch + charts) visualization functions."""

from __future__ import annotations

import matplotlib.figure
import numpy as np
import pandas as pd

from streamlit_app.components.charts import (
    plot_match_comparison_bars,
    plot_physical_bars,
    plot_player_radar,
    plot_ppda_bars,
)
from streamlit_app.components.pitch import (
    categorize_passes,
    plot_heatmap,
    plot_pass_map,
    plot_pass_network_interactive,
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

    def test_legend_with_player_names(self) -> None:
        players = [
            {"goals": 0.5, "xg": 0.4, "passes": 50.0},
            {"goals": 0.8, "xg": 0.6, "passes": 30.0},
        ]
        fig = plot_player_radar(
            players,
            metrics=["goals", "xg", "passes"],
            labels=["Goals/90", "xG/90", "Passes/90"],
            ranges=[(0, 1.5), (0, 1.5), (0, 80)],
            player_names=["Player A", "Player B"],
        )
        assert isinstance(fig, matplotlib.figure.Figure)
        ax = fig.axes[0]
        legend = ax.get_legend()
        assert legend is not None
        texts = [t.get_text() for t in legend.get_texts()]
        assert texts == ["Player A", "Player B"]


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


class TestPlotPassNetworkInteractive:
    """Test interactive Plotly pass network visualization."""

    def test_returns_plotly_figure_with_data(self) -> None:
        import plotly.graph_objects as go

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
            }
        )
        fig = plot_pass_network_interactive(nodes, edges)
        assert isinstance(fig, go.Figure)

    def test_returns_figure_with_empty_nodes(self) -> None:
        import plotly.graph_objects as go

        nodes = pd.DataFrame(columns=pd.Index(["player_id", "player_display_name", "avg_x", "avg_y", "pass_count"]))
        edges = pd.DataFrame(columns=pd.Index(["passer_id", "receiver_id", "pair_count"]))
        fig = plot_pass_network_interactive(nodes, edges)
        assert isinstance(fig, go.Figure)

    def test_returns_figure_with_empty_edges(self) -> None:
        import plotly.graph_objects as go

        nodes = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_display_name": ["A", "B"],
                "avg_x": [30.0, 60.0],
                "avg_y": [40.0, 40.0],
                "pass_count": [5, 5],
            }
        )
        edges = pd.DataFrame(columns=pd.Index(["passer_id", "receiver_id", "pair_count"]))
        fig = plot_pass_network_interactive(nodes, edges)
        assert isinstance(fig, go.Figure)

    def test_single_edge(self) -> None:
        import plotly.graph_objects as go

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
            }
        )
        fig = plot_pass_network_interactive(nodes, edges)
        assert isinstance(fig, go.Figure)


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


class TestPlotPassMapLineBreaking:
    """Test line-breaking pass visualization on pass map."""

    def _make_passes_with_lb(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "start_x": [30.0, 50.0, 40.0, 60.0],
                "start_y": [40.0, 30.0, 50.0, 20.0],
                "end_x": [60.0, 80.0, 70.0, 90.0],
                "end_y": [40.0, 35.0, 50.0, 30.0],
                "is_complete": [1, 1, 0, 1],
                "is_progressive": [1, 0, 0, 1],
                "is_line_breaking": [0, 0, 0, 1],
            }
        )

    def test_line_breaking_highlight_enabled(self) -> None:
        passes = self._make_passes_with_lb()
        fig = plot_pass_map(passes, highlight_line_breaking=True)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_line_breaking_highlight_disabled(self) -> None:
        passes = self._make_passes_with_lb()
        fig = plot_pass_map(passes, highlight_line_breaking=False)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_no_line_breaking_column(self) -> None:
        """Pass map without is_line_breaking column should work normally."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0],
                "start_y": [40.0],
                "end_x": [60.0],
                "end_y": [40.0],
                "is_complete": [1],
                "is_progressive": [1],
            }
        )
        fig = plot_pass_map(passes, highlight_line_breaking=True)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_all_line_breaking(self) -> None:
        """All passes are line-breaking — all should render in gold."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0],
                "start_y": [40.0, 30.0],
                "end_x": [60.0, 80.0],
                "end_y": [40.0, 35.0],
                "is_complete": [1, 1],
                "is_progressive": [1, 0],
                "is_line_breaking": [1, 1],
            }
        )
        fig = plot_pass_map(passes, highlight_line_breaking=True)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_empty_passes_with_lb_column(self) -> None:
        passes = pd.DataFrame(
            {
                "start_x": [],
                "start_y": [],
                "end_x": [],
                "end_y": [],
                "is_complete": [],
                "is_progressive": [],
                "is_line_breaking": [],
            }
        )
        fig = plot_pass_map(passes, highlight_line_breaking=True)
        assert isinstance(fig, matplotlib.figure.Figure)


class TestCategorizePasses:
    """Test pass categorization ensures incomplete passes are never progressive or line-breaking."""

    def test_incomplete_progressive_stays_incomplete(self) -> None:
        """A pass that is incomplete but is_progressive=1 must be categorized as incomplete."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0],
                "start_y": [40.0, 30.0],
                "end_x": [60.0, 80.0],
                "end_y": [40.0, 35.0],
                "is_complete": [0, 1],
                "is_progressive": [1, 1],
            }
        )
        incomplete, _complete, prog, _lb = categorize_passes(passes)
        assert len(incomplete) == 1, "Incomplete progressive pass must stay in incomplete group"
        assert len(prog) == 1, "Only the complete progressive pass should be in prog group"
        assert int(prog.iloc[0]["is_complete"]) == 1

    def test_incomplete_line_breaking_stays_incomplete(self) -> None:
        """A pass that is incomplete but is_line_breaking=1 must be categorized as incomplete."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0],
                "start_y": [40.0, 30.0],
                "end_x": [60.0, 80.0],
                "end_y": [40.0, 35.0],
                "is_complete": [0, 1],
                "is_progressive": [0, 0],
                "is_line_breaking": [1, 1],
            }
        )
        incomplete, _complete, _prog, lb = categorize_passes(passes)
        assert len(incomplete) == 1, "Incomplete line-breaking pass must stay in incomplete group"
        assert len(lb) == 1, "Only the complete line-breaking pass should be in lb group"
        assert int(lb.iloc[0]["is_complete"]) == 1

    def test_incomplete_both_progressive_and_line_breaking(self) -> None:
        """A pass that is incomplete with both flags set must be categorized as incomplete."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0],
                "start_y": [40.0],
                "end_x": [60.0],
                "end_y": [40.0],
                "is_complete": [0],
                "is_progressive": [1],
                "is_line_breaking": [1],
            }
        )
        incomplete, _complete, prog, lb = categorize_passes(passes)
        assert len(incomplete) == 1
        assert len(prog) == 0
        assert len(lb) == 0

    def test_all_complete_passes_categorized_correctly(self) -> None:
        """Complete passes should be split by line-breaking > progressive > complete."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0, 70.0],
                "start_y": [40.0, 30.0, 20.0],
                "end_x": [60.0, 80.0, 100.0],
                "end_y": [40.0, 35.0, 30.0],
                "is_complete": [1, 1, 1],
                "is_progressive": [0, 1, 1],
                "is_line_breaking": [0, 0, 1],
            }
        )
        incomplete, complete, prog, lb = categorize_passes(passes)
        assert len(incomplete) == 0
        assert len(complete) == 1  # Non-progressive, non-LB
        assert len(prog) == 1  # Progressive but not LB
        assert len(lb) == 1  # Line-breaking (supersedes progressive)

    def test_no_progressive_in_incomplete_group(self) -> None:
        """Regression: verify no pass in incomplete group has is_progressive=1 in output."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 40.0, 50.0, 60.0],
                "start_y": [40.0, 40.0, 40.0, 40.0],
                "end_x": [60.0, 70.0, 80.0, 90.0],
                "end_y": [40.0, 40.0, 40.0, 40.0],
                "is_complete": [0, 0, 1, 1],
                "is_progressive": [1, 0, 1, 0],
                "is_line_breaking": [0, 0, 0, 0],
            }
        )
        incomplete, _complete, prog, _lb = categorize_passes(passes)
        assert len(incomplete) == 2, "Both incomplete passes should be in incomplete group"
        assert len(prog) == 1, "Only the complete progressive pass should be progressive"
        # The incomplete pass with is_progressive=1 must NOT appear in prog
        assert all(int(row["is_complete"]) == 1 for _, row in prog.iterrows())

    def test_no_line_breaking_in_incomplete_group(self) -> None:
        """Regression: verify no pass in incomplete group has is_line_breaking=1 in output."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 40.0, 50.0, 60.0],
                "start_y": [40.0, 40.0, 40.0, 40.0],
                "end_x": [60.0, 70.0, 80.0, 90.0],
                "end_y": [40.0, 40.0, 40.0, 40.0],
                "is_complete": [0, 0, 1, 1],
                "is_progressive": [0, 0, 0, 0],
                "is_line_breaking": [1, 0, 1, 0],
            }
        )
        incomplete, _complete, _prog, lb = categorize_passes(passes)
        assert len(incomplete) == 2, "Both incomplete passes should be in incomplete group"
        assert len(lb) == 1, "Only the complete line-breaking pass should be line-breaking"
        assert all(int(row["is_complete"]) == 1 for _, row in lb.iterrows())

    def test_highlight_flags_off_groups_all_as_complete_or_incomplete(self) -> None:
        """When highlights are off, only complete and incomplete groups should have passes."""
        passes = pd.DataFrame(
            {
                "start_x": [30.0, 50.0],
                "start_y": [40.0, 30.0],
                "end_x": [60.0, 80.0],
                "end_y": [40.0, 35.0],
                "is_complete": [1, 0],
                "is_progressive": [1, 1],
                "is_line_breaking": [1, 1],
            }
        )
        incomplete, complete, prog, lb = categorize_passes(
            passes, highlight_progressive=False, highlight_line_breaking=False
        )
        assert len(incomplete) == 1
        assert len(complete) == 1
        assert len(prog) == 0
        assert len(lb) == 0


# ---------------------------------------------------------------------------
# Movement Analysis chart tests
# ---------------------------------------------------------------------------


class TestPlotPhysicalBars:
    """Test physical performance bar chart."""

    def test_returns_figure_with_data(self) -> None:
        data = pd.DataFrame(
            {
                "player_id": ["p1", "p2", "p3"],
                "total_distance_km": [10.5, 11.2, 9.8],
            }
        )
        fig = plot_physical_bars(data, "total_distance_km", "Distance (km)")
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        data = pd.DataFrame({"player_id": pd.Series(dtype=str), "total_distance_km": pd.Series(dtype=float)})
        fig = plot_physical_bars(data, "total_distance_km", "Distance (km)")
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_custom_title(self) -> None:
        data = pd.DataFrame({"player_id": ["p1"], "sprint_distance_m": [500.0]})
        fig = plot_physical_bars(data, "sprint_distance_m", "Sprint (m)", title="Sprint Distance")
        assert isinstance(fig, matplotlib.figure.Figure)


class TestPlotPpdaBars:
    """Test PPDA bar chart."""

    def test_returns_figure_with_data(self) -> None:
        data = pd.DataFrame(
            {
                "match_id": [1, 2],
                "home_ppda": [8.5, 12.3],
                "away_ppda": [10.1, 7.8],
                "home_team_name": ["Team A", "Team C"],
                "away_team_name": ["Team B", "Team D"],
            }
        )
        fig = plot_ppda_bars(data)
        assert isinstance(fig, matplotlib.figure.Figure)

    def test_returns_figure_with_empty_data(self) -> None:
        data = pd.DataFrame(
            {
                "match_id": pd.Series(dtype=int),
                "home_ppda": pd.Series(dtype=float),
                "away_ppda": pd.Series(dtype=float),
                "home_team_name": pd.Series(dtype=str),
                "away_team_name": pd.Series(dtype=str),
            }
        )
        fig = plot_ppda_bars(data)
        assert isinstance(fig, matplotlib.figure.Figure)
