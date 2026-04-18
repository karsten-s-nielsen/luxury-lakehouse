from __future__ import annotations

import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


# ---------------------------------------------------------------------------
# TestRollupStages — V01 straddler + Wyscout regression guard for the
# driver-side rollup that consumes fct_funnel_stages_agg_synced rows.
# ---------------------------------------------------------------------------


class TestRollupStages:
    """Verify rollup_stages() semantics against straddler + Wyscout inputs."""

    def test_empty_rows(self) -> None:
        from queries.funnel import rollup_stages

        result = rollup_stages(pd.DataFrame(), gs_filtered=True)
        assert result == {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}

        result_all = rollup_stages(pd.DataFrame(), gs_filtered=False)
        assert result_all == {"possessions": 0, "a3_entries": 0, "shots": 0, "goals": 0}

    def test_sb_only_gs_filtered(self) -> None:
        """gs_filtered=True → possessions is sum of pos_in_gs."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200],
                "team_id": [1, 1],
                "pos_in_gs": [10, 5],
                "pos_in_match": [15, 8],  # ignored at gs_filtered=True
                "a3_entries": [4, 2],
                "shots": [1, 1],
                "goals": [0, 1],
                "wy_match_flag": [0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["possessions"] == 15
        assert result["a3_entries"] == 6
        assert result["shots"] == 2
        assert result["goals"] == 1

    def test_sb_only_gs_all(self) -> None:
        """gs_filtered=False → possessions is groupby((match,team)).first(pos_in_match).sum()."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200],
                "team_id": [1, 1],
                "pos_in_gs": [10, 5],
                "pos_in_match": [15, 8],
                "a3_entries": [4, 2],
                "shots": [1, 1],
                "goals": [0, 1],
                "wy_match_flag": [0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 15 + 8

    def test_straddler_gs_all_deduped(self) -> None:
        """V01 regression guard — a straddler spans multiple gs rows.

        Same match appears on three gs rows, each with pos_in_gs=5 but
        pos_in_match=12. At gs_filtered=False, possessions must be 12
        (NOT 15 — that would double-count the straddler).
        """
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 100, 100],
                "team_id": [1, 1, 1],
                "pos_in_gs": [5, 5, 5],
                "pos_in_match": [12, 12, 12],
                "a3_entries": [2, 1, 2],
                "shots": [1, 0, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [0, 0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 12, "straddler must dedup to pos_in_match=12"

    def test_straddler_gs_filtered_not_deduped(self) -> None:
        """Same straddler rows — gs_filtered=True sums pos_in_gs = 15."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 100, 100],
                "team_id": [1, 1, 1],
                "pos_in_gs": [5, 5, 5],
                "pos_in_match": [12, 12, 12],
                "a3_entries": [2, 1, 2],
                "shots": [1, 0, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [0, 0, 0],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["possessions"] == 15

    def test_wy_match_deduped_across_gs(self) -> None:
        """A Wyscout match with wy_match_flag=1 on all 3 gs rows → counted ONCE."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [300, 300, 300],
                "team_id": [5, 5, 5],
                "pos_in_gs": [0, 0, 0],
                "pos_in_match": [0, 0, 0],
                "a3_entries": [4, 2, 3],
                "shots": [1, 1, 0],
                "goals": [0, 0, 0],
                "wy_match_flag": [1, 1, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        # 0 SB possessions + 1 Wyscout match = 1 synthetic possession
        assert result["possessions"] == 1
        assert result["a3_entries"] == 9

    def test_wy_mixed_sb(self) -> None:
        """2 SB matches (pos_in_match=15, 8) + 1 Wyscout match → 15+8+1 = 24."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 200, 300],
                "team_id": [1, 1, 1],
                "pos_in_gs": [10, 5, 0],
                "pos_in_match": [15, 8, 0],
                "a3_entries": [4, 2, 3],
                "shots": [1, 1, 0],
                "goals": [0, 1, 0],
                "wy_match_flag": [0, 0, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=False)
        assert result["possessions"] == 15 + 8 + 1

    def test_stage_sums_independent_of_wyscout(self) -> None:
        """a3/shots/goals are simple sums regardless of wy_match_flag."""
        from queries.funnel import rollup_stages

        df = pd.DataFrame(
            {
                "match_id": [100, 300],
                "team_id": [1, 1],
                "pos_in_gs": [10, 0],
                "pos_in_match": [15, 0],
                "a3_entries": [4, 3],
                "shots": [1, 2],
                "goals": [0, 1],
                "wy_match_flag": [0, 1],
            }
        )
        result = rollup_stages(df, gs_filtered=True)
        assert result["a3_entries"] == 7
        assert result["shots"] == 3
        assert result["goals"] == 1


# ---------------------------------------------------------------------------
# TestFetchFunnelAggSQL — captures the SQL + params emitted by
# fetch_funnel_agg under each of the four filter combinations.
# Protects against LIMIT re-introduction (the original D58 correctness bug).
# ---------------------------------------------------------------------------


def _capture_execute_query() -> tuple[list[tuple[str, tuple]], MagicMock]:
    """Return (captured_calls, patched_execute_query).

    captured_calls is appended to inside the mock; each entry is
    (sql_string, params_tuple).
    """
    calls: list[tuple[str, tuple]] = []

    def _mock(sql: str, params: tuple) -> pd.DataFrame:
        calls.append((sql, params))
        return pd.DataFrame()

    return calls, MagicMock(side_effect=_mock)


def _patch_db(mod: Any, mock_exec: MagicMock) -> ExitStack:
    """Stack patches for execute_query + t on the funnel module.

    The funnel module calls `t(<table_name>)` (which requires live
    lakebase settings from the environment) before dispatching through
    `execute_query`. To keep these tests purely SQL-shape assertions we
    substitute both: `t` returns the bare table name, and `execute_query`
    is the capturing mock. Returns a context manager that applies both
    patches for the duration of a `with` block.
    """
    stack = ExitStack()
    stack.enter_context(patch.object(mod, "execute_query", mock_exec))
    stack.enter_context(patch.object(mod, "t", lambda name, schema=None: name))
    return stack


class TestFetchFunnelAggSQL:
    """Capture SQL + params for every supported combination of filter args."""

    def test_single_match_no_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state=None)

        sql, params = calls[0]
        where = sql.split(" WHERE ", 1)[1]
        assert "match_id = %s" in where
        # `competition_id` / `game_state` appear in the SELECT column list regardless,
        # so scope the absence check to the WHERE clause.
        assert "competition_id" not in where
        assert "game_state" not in where
        assert params == (3888713,)

    def test_single_match_with_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state="Drawing")

        sql, params = calls[0]
        where = sql.split(" WHERE ", 1)[1]
        assert "match_id = %s" in where
        assert "game_state = %s" in where
        # Lowercased at the query-module boundary
        assert params == (3888713, "drawing")

    def test_season_no_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state=None)

        sql, params = calls[0]
        where = sql.split(" WHERE ", 1)[1]
        assert "competition_id = %s" in where
        assert "(team_id = %s OR opponent_team_id = %s)" in where
        assert "game_state" not in where
        assert params == (11, 217, 217)

    def test_season_with_gs(self) -> None:
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state="Winning")

        sql, params = calls[0]
        where = sql.split(" WHERE ", 1)[1]
        assert "competition_id = %s" in where
        assert "(team_id = %s OR opponent_team_id = %s)" in where
        assert "game_state = %s" in where
        assert params == (11, 217, 217, "winning")

    def test_game_state_all_is_treated_as_no_filter(self) -> None:
        """game_state='All' is a sentinel for 'no filter' — must NOT emit a clause."""
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217, match_id=None, game_state="All")

        sql, _params = calls[0]
        where = sql.split(" WHERE ", 1)[1]
        assert "game_state" not in where

    def test_no_row_truncation_clause(self) -> None:
        """V10 correctness guard — no code path may truncate rows.

        The mart is bounded to ~12,145 rows total; truncation would reintroduce
        the 2026-04-17 silent-truncation bug that under-reported A3/shots/goals
        by >50 % for prolific teams. Guards against LIMIT, FETCH FIRST, and
        OFFSET (the three row-limiting constructs in Lakebase Postgres).
        """
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_funnel_agg(11, 217)
            mod.fetch_funnel_agg(11, 217, match_id=3888713)
            mod.fetch_funnel_agg(11, 217, game_state="Drawing")
            mod.fetch_funnel_agg(11, 217, match_id=3888713, game_state="Drawing")

        for sql, _ in calls:
            upper = sql.upper()
            assert "LIMIT" not in upper, f"LIMIT found in emitted SQL: {sql}"
            assert "FETCH FIRST" not in upper, f"FETCH FIRST found in emitted SQL: {sql}"
            assert "OFFSET" not in upper, f"OFFSET found in emitted SQL: {sql}"


class TestFetchMatchMetaSingle:
    """fetch_match_meta must use LIMIT 1 (single-match lookup only)."""

    def test_limit_is_1_not_200(self) -> None:
        """Old implementation had LIMIT 200 (season-mode artifact) — must be 1 now.

        Also verifies the post-A7 signature: fetch_match_meta(comp_id, match_id) —
        the old team_id param was removed because it did not affect the SQL.
        """
        from queries import funnel as mod

        calls, mock_exec = _capture_execute_query()
        with _patch_db(mod, mock_exec):
            mod.fetch_match_meta(11, 3888713)

        sql, params = calls[0]
        assert "LIMIT 1" in sql
        assert "LIMIT 200" not in sql
        assert "LIMIT 500000" not in sql
        assert params == (11, 3888713)


# ---------------------------------------------------------------------------
# TestConversionRates (unchanged — pre-existing, kept for regression coverage)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# TestFunnelChart (unchanged — pre-existing, kept for regression coverage)
# ---------------------------------------------------------------------------


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
