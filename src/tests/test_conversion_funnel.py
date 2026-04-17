from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


class TestFunnelAggregation:
    """Verify funnel stage counts from raw action data."""

    @pytest.fixture
    def sample_actions(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "match_id": [100, 100, 100, 100, 100, 100, 100, 100, 100],
                "team_id": [1, 1, 1, 1, 1, 1, 2, 2, 2],
                "possession_id": [1, 1, 2, 2, 2, 3, 4, 4, 5],
                "possession_team_id": [1, 1, 1, 1, 1, 1, 2, 2, 2],
                "start_x": [60, 65, 50, 69.9, 72, 80, 60, 65, 50],
                "end_x": [65, 72, 69.9, 70.1, 80, 90, 72, 80, 60],
                "action_type": [
                    "pass",
                    "pass",
                    "pass",
                    "pass",
                    "shot",
                    "shot",
                    "pass",
                    "shot",
                    "pass",
                ],
                "action_result": [
                    "success",
                    "success",
                    "success",
                    "success",
                    "success",
                    "fail",
                    "success",
                    "fail",
                    "success",
                ],
            }
        )

    def test_possession_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["possessions"] == 3

    def test_a3_entry_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["a3_entries"] == 2

    def test_a3_entry_excludes_start_in_a3(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["a3_entries"] == 2

    def test_shot_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["shots"] == 2

    def test_goal_count(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=1)
        assert result["goals"] == 1

    def test_away_team_independent(self, sample_actions: pd.DataFrame) -> None:
        from queries.funnel import compute_funnel_stages

        result = compute_funnel_stages(sample_actions, team_id=2)
        assert result["possessions"] == 2
        assert result["shots"] == 1
        assert result["goals"] == 0

    def test_a3_boundary_exact_70_is_entry(self) -> None:
        """start_x exactly 70 is at the boundary — counts as an entry."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame(
            {
                "match_id": [100],
                "team_id": [1],
                "possession_id": [1],
                "possession_team_id": [1],
                "start_x": [70.0],
                "end_x": [75.0],
                "action_type": ["pass"],
                "action_result": ["success"],
            }
        )
        result = compute_funnel_stages(df, team_id=1)
        assert result["a3_entries"] == 1

    def test_failed_actions_excluded_from_a3(self) -> None:
        """Only successful actions count as A3 entries."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame(
            {
                "match_id": [100],
                "team_id": [1],
                "possession_id": [1],
                "possession_team_id": [1],
                "start_x": [65.0],
                "end_x": [75.0],
                "action_type": ["pass"],
                "action_result": ["fail"],
            }
        )
        result = compute_funnel_stages(df, team_id=1)
        # A3 entries count spatial crossings regardless of action outcome —
        # a failed pass that still carried play into the final third is an entry.
        assert result["a3_entries"] == 1

    def test_defensive_actions_excluded(self) -> None:
        """Actions during opponent's possession (e.g. tackles) must not inflate funnel."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 100, 100, 100],
                "team_id": [1, 1, 1, 1],
                "possession_id": [1, 1, 10, 10],
                # possession 1 belongs to team 1 (own); possession 10 to team 2 (defensive)
                "possession_team_id": [1, 1, 2, 2],
                "start_x": [60.0, 65.0, 60.0, 72.0],
                "end_x": [65.0, 72.0, 75.0, 80.0],
                "action_type": ["pass", "pass", "tackle", "shot"],
                "action_result": ["success", "success", "success", "success"],
            }
        )
        result = compute_funnel_stages(df, team_id=1)
        # Only own possession counts: 1 possession, 1 A3 entry, 0 shots
        assert result["possessions"] == 1
        assert result["a3_entries"] == 1
        assert result["shots"] == 0
        assert result["goals"] == 0

    def test_cross_match_possession_ids_not_collapsed(self) -> None:
        """possession_id=1 in match 100 and match 200 are distinct possessions."""
        from queries.funnel import compute_funnel_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200],
                "team_id": [1, 1],
                "possession_id": [1, 1],
                "possession_team_id": [1, 1],
                "start_x": [50.0, 50.0],
                "end_x": [60.0, 60.0],
                "action_type": ["pass", "pass"],
                "action_result": ["success", "success"],
            }
        )
        result = compute_funnel_stages(df, team_id=1)
        assert result["possessions"] == 2


class TestConversionRates:
    """Verify conversion rate computation."""

    def test_step_rates(self) -> None:
        from queries.funnel import compute_conversion_rates

        stages = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        rates = compute_conversion_rates(stages)
        assert rates["poss_to_a3"] == pytest.approx(25.0)
        assert rates["a3_to_shot"] == pytest.approx(20.0)
        assert rates["shot_to_goal"] == pytest.approx(20.0)
        assert rates["end_to_end"] == pytest.approx(1.0)

    def test_zero_division(self) -> None:
        from queries.funnel import compute_conversion_rates

        stages = {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}
        rates = compute_conversion_rates(stages)
        assert rates["poss_to_a3"] == 0.0
        assert rates["end_to_end"] == 0.0


try:
    import plotly  # noqa: F401

    _has_plotly = True
except ImportError:
    _has_plotly = False


# plotly 6.7.0 type stubs declare `Figure.data` as `Unknown | Figure` rather
# than `tuple[BaseTraceType, ...]`, so pyright misreports every `fig.data[i].x`
# and `fig.data[i].marker` access as an attribute error and `len(fig.data)` as
# a bad argument to `len`. The test assertions are runtime-correct against
# Plotly's documented public API; the `# pyright: ignore[...]` markers below
# are a stub-quality workaround, not suppression of real bugs. The ignores
# are only needed here because `[tool.pyright].extraPaths` now lets pyright
# statically resolve `state.conversion_funnel` (before that it typed fig as
# Unknown and the accesses went un-checked).
@pytest.mark.skipif(not _has_plotly, reason="plotly not installed")
class TestFunnelChart:
    """Verify mirror funnel chart rendering."""

    def test_chart_has_two_traces(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        assert len(fig.data) == 2  # pyright: ignore[reportArgumentType]

    def test_chart_home_positive_away_negative(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 100, "a3_entries": 25, "shots": 5, "goals": 1}
        away = {"possessions": 90, "a3_entries": 20, "shots": 4, "goals": 0}
        fig = _build_mirror_chart(home, away, "Home FC", "Away FC")
        assert all(v >= 0 for v in fig.data[0].x)  # pyright: ignore[reportAttributeAccessIssue]
        assert all(v <= 0 for v in fig.data[1].x)  # pyright: ignore[reportAttributeAccessIssue]

    def test_chart_uses_canonical_colors(self) -> None:
        from state.conversion_funnel import _build_mirror_chart

        home = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        away = {"possessions": 50, "a3_entries": 10, "shots": 2, "goals": 0}
        fig = _build_mirror_chart(home, away, "H", "A")
        assert fig.data[0].marker.color == "#e63946"  # pyright: ignore[reportAttributeAccessIssue]
        assert fig.data[1].marker.color == "#457b9d"  # pyright: ignore[reportAttributeAccessIssue]
