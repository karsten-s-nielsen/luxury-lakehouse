"""Tests for Match Summary rendering helpers (spec §5.3, §5.4, §5.5)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd
import pytest

# match_summary_render imports plotly at module level. CI installs only the
# default extra (no taipy-app), which omits plotly — skip the whole module
# cleanly rather than failing collection. Matches the conversion-funnel test
# pattern that guards other plotly-dependent tests.
pytest.importorskip("plotly", reason="plotly is only installed with taipy-app extra")

import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from state.match_summary_render import (
    build_xg_race_figure,
    render_delta_table_html,
    render_moments_html,
)


class _ScatterXY(NamedTuple):
    """Narrowed (x, y) coordinates from a Plotly Scatter trace.

    Plotly's `Scatter.x` / `Scatter.y` are typed as a wide union
    (`str | tuple | numpy.ndarray | None | ...`) which pyright cannot index
    with `[-1]` or apply `len()` to. This NamedTuple is the post-narrow
    return type used by `_named_scatter()`.
    """

    x: list[Any]
    y: list[Any]


def _named_scatter(fig: go.Figure, name: str) -> _ScatterXY:
    """Locate the unique Scatter trace named ``name`` and return its (x, y) lists.

    The pyright narrowing path is: ``Figure.data`` is a wide union of
    ~50 trace classes; ``isinstance(t, go.Scatter)`` narrows individual
    traces; the explicit ``not isinstance(.., str)`` + ``is not None``
    asserts narrow the ``.x`` / ``.y`` attributes from Plotly's wide
    ``str|tuple|ndarray|None`` union to a plain list for unambiguous
    indexing at call sites.

    Asserts there is EXACTLY one matching trace so test failures point at
    the trace-identification problem, not a downstream `IndexError`.
    """
    matches = [t for t in fig.data if isinstance(t, go.Scatter) and t.name == name]
    assert len(matches) == 1, f"Expected exactly 1 Scatter trace named {name!r}, got {len(matches)}"
    s = matches[0]
    x, y = s.x, s.y
    assert x is not None and not isinstance(x, str), f"trace.x narrow failed: {type(x).__name__}"
    assert y is not None and not isinstance(y, str), f"trace.y narrow failed: {type(y).__name__}"
    return _ScatterXY(x=list(x), y=list(y))


# ── Fixtures ────────────────────────────────────────────────────────────────


def _sample_decisive() -> pd.DataFrame:
    """Three decisive actions sorted desc by |VAEP|: save (0.35) > shot (0.18) > miss (-0.04)."""
    return pd.DataFrame(
        {
            "minute": [84, 23, 67],
            "second": [40, 12, 5],
            "period": [2, 1, 2],
            "player_id": [300, 100, 200],
            "player_name": ["Sánchez", "Palmer", "Saka"],
            "team_id": [10, 10, 20],
            "team_name": ["Chelsea", "Chelsea", "Arsenal"],
            "action_type": ["keeper_save", "shot", "shot"],
            "action_result": ["success", "success", "fail"],
            "vaep_value": [0.35, 0.18, -0.04],
            "offensive_value": [0.0, 0.18, 0.0],
            "defensive_value": [0.35, 0.0, -0.04],
        }
    )


def _no_red_cards() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "minute",
            "second",
            "period",
            "player_id",
            "player_name",
            "team_id",
            "team_name",
            "card_name",
        ]
    )


def _one_red_card() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "minute": [58],
            "second": [20],
            "period": [2],
            "player_id": [999],
            "player_name": ["Saliba"],
            "team_id": [20],
            "team_name": ["Arsenal"],
            "card_name": ["Red Card"],
        }
    )


def _sample_shots() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "minute": [5, 23, 45, 67, 72, 84],
            "second": [10, 12, 30, 5, 15, 40],
            "period": [1, 1, 1, 2, 2, 2],
            "team_id": [10, 10, 10, 20, 20, 10],
            "team_name": ["Chelsea", "Chelsea", "Chelsea", "Arsenal", "Arsenal", "Chelsea"],
            "xg": [0.03, 0.12, 0.20, 0.48, 0.08, 0.30],
            "is_goal": [False, True, False, False, False, True],
        }
    )


def _sample_stats_home() -> dict[str, float]:
    return {
        "xG": 2.4,
        "Progressive passes": 47,
        "Shots": 16,
        "PPDA (lower = more press)": 8.2,
        "Possession %": 58,
        "Pass completion %": 87,
    }


def _sample_stats_away() -> dict[str, float]:
    return {
        "xG": 0.8,
        "Progressive passes": 28,
        "Shots": 9,
        "PPDA (lower = more press)": 14.5,
        "Possession %": 42,
        "Pass completion %": 83,
    }


# ── Row 1: Big Story moments HTML ───────────────────────────────────────────


def test_moments_html_contains_hero_card() -> None:
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    assert "Sánchez" in html
    assert "ll-big-story-hero" in html


def test_moments_html_contains_two_secondary_cards() -> None:
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    assert "Palmer" in html
    assert "Saka" in html
    # Count actual secondary card DIVs — not CSS rules that mention the class.
    assert html.count('"ll-moment-card ll-moment-card-secondary"') == 2


def test_moments_html_auto_includes_red_card() -> None:
    html = render_moments_html(_sample_decisive(), _one_red_card(), scope_plain="Chelsea vs Arsenal")
    assert "Saliba" in html
    assert "58&#x27;" in html or "58'" in html  # HTML-escaped or raw
    assert "ll-moment-card-red-card" in html


def test_moments_html_empty_decisive_renders_empty_state() -> None:
    empty = pd.DataFrame(columns=_sample_decisive().columns)
    html = render_moments_html(empty, _no_red_cards(), scope_plain="X vs Y")
    assert "No decisive actions" in html


def test_moments_html_caveat_mentions_off_ball_and_vaep() -> None:
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="Chelsea vs Arsenal")
    assert "VAEP" in html
    assert "Off-ball" in html or "off-ball" in html


def test_moments_html_shot_action_label_maps_to_scored_or_missed() -> None:
    html = render_moments_html(_sample_decisive(), _no_red_cards(), scope_plain="x")
    # Palmer's shot with result=success → "Shot, scored"
    assert "Shot, scored" in html
    # Saka's shot with result=fail → "Shot, missed / saved"
    assert "missed" in html.lower()


def test_moments_html_only_two_secondary_even_with_many_decisive() -> None:
    """Hero + 2 secondary cap at 3 decisive cards (plus optional red cards)."""
    many = _sample_decisive().copy()
    # Duplicate rows so we have 5
    many = pd.concat([many, _sample_decisive()], ignore_index=True)
    assert len(many) == 6
    html = render_moments_html(many, _no_red_cards(), scope_plain="x")
    # Expect exactly 1 hero card DIV + 2 secondary card DIVs (not CSS rules).
    assert html.count('"ll-moment-card ll-big-story-hero"') == 1
    assert html.count('"ll-moment-card ll-moment-card-secondary"') == 2


# ── Row 2: Plotly xG race ──────────────────────────────────────────────────


def test_xg_race_figure_has_expected_traces() -> None:
    """2 cumulative lines + 2 shot-tick traces + decisive rings + goal stars = ≥6."""
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    assert len(list(fig.data)) >= 6


def test_xg_race_figure_has_halftime_line() -> None:
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    shapes = fig.layout.shapes or ()
    halftime = [s for s in shapes if s.type == "line" and s.x0 == 45]
    assert len(halftime) >= 1, "Expected a half-time divider at minute 45"


def test_xg_race_cumulative_matches_team_total() -> None:
    shots = _sample_shots()
    fig = build_xg_race_figure(
        shots=shots,
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    home_total = shots.loc[shots["team_id"] == 10, "xg"].sum()
    home = _named_scatter(fig, "Chelsea xG")
    assert home.y[-1] == pytest.approx(home_total, abs=1e-6)


def test_xg_race_red_card_marker_present_when_red_card_issued() -> None:
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_one_red_card(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    red_traces = [t for t in fig.data if "red card" in str(getattr(t, "name", "")).lower()]
    assert len(red_traces) >= 1


def test_xg_race_empty_red_cards_produces_no_red_trace() -> None:
    fig = build_xg_race_figure(
        shots=_sample_shots(),
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    red_traces = [t for t in fig.data if "red card" in str(getattr(t, "name", "")).lower()]
    assert red_traces == []


def test_xg_race_lines_extend_to_common_end_minute() -> None:
    """Regression test for 2026-04-20 user report (Man City 0-1 Man United).

    When one team stops shooting earlier than the other (ManU last shot at 54',
    ManC last shot at 91'), both cumulative-xG traces must extend to a common
    right-edge x — not just stop at each team's last data point. Verify the
    last x-value on each team's trace is the same, and is >= the latest shot
    minute.
    """
    shots_asymmetric = pd.DataFrame(
        {
            # Home (Chelsea, team 10): 2 shots, last at minute 50
            # Away (Arsenal, team 20): 3 shots, last at minute 88
            "minute": [10, 50, 23, 67, 88],
            "second": [10, 5, 12, 15, 40],
            "period": [1, 2, 1, 2, 2],
            "team_id": [10, 10, 20, 20, 20],
            "team_name": ["Chelsea", "Chelsea", "Arsenal", "Arsenal", "Arsenal"],
            "xg": [0.10, 0.20, 0.15, 0.25, 0.30],
            "is_goal": [0, 0, 0, 0, 0],
        },
    )
    fig = build_xg_race_figure(
        shots=shots_asymmetric,
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    home_trace = _named_scatter(fig, "Chelsea xG")
    away_trace = _named_scatter(fig, "Arsenal xG")
    # Both traces must terminate at the same x value so the chart doesn't look truncated.
    assert home_trace.x[-1] == away_trace.x[-1], (
        f"Home line ends at {home_trace.x[-1]}, away line ends at {away_trace.x[-1]} — "
        "should be the same common end-of-match anchor."
    )
    # End anchor must be at or beyond the last shot minute (88 in this fixture).
    assert home_trace.x[-1] >= 88
    # Home's final cumulative xG (flat tail) must equal its total xG of shots (0.30 = 0.10 + 0.20).
    assert home_trace.y[-1] == pytest.approx(0.30, abs=1e-6)
    # Away's final cumulative xG must equal its total (0.70 = 0.15 + 0.25 + 0.30).
    assert away_trace.y[-1] == pytest.approx(0.70, abs=1e-6)


def test_xg_race_goal_trace_uses_boolean_filter_not_label_index() -> None:
    """Regression test for production bug 2026-04-19.

    Lakebase returns ``is_goal`` as int 0/1 (NOT bool). The previous
    ``shots.loc[shots["is_goal"]]`` pattern did LABEL indexing on an int
    series, producing a row per shot instead of per goal (17 "goals" for a
    1-goal match). The fix uses ``.astype(bool)``; this test uses an
    integer-typed fixture to prevent regression.
    """
    shots_int_goal = pd.DataFrame(
        {
            "minute": [5, 23, 45, 67, 72, 84],
            "second": [10, 12, 30, 5, 15, 40],
            "period": [1, 1, 1, 2, 2, 2],
            "team_id": [10, 10, 10, 20, 20, 10],
            "team_name": ["Chelsea", "Chelsea", "Chelsea", "Arsenal", "Arsenal", "Chelsea"],
            "xg": [0.03, 0.12, 0.20, 0.48, 0.08, 0.30],
            # INT dtype, not bool — matches Lakebase behavior
            "is_goal": [0, 1, 0, 0, 0, 1],
        },
    )
    fig = build_xg_race_figure(
        shots=shots_int_goal,
        decisive=_sample_decisive(),
        red_cards=_no_red_cards(),
        home_team_id=10,
        home_team_name="Chelsea",
        away_team_id=20,
        away_team_name="Arsenal",
        home_color="#5a9999",
        away_color="#a55555",
    )
    goal = _named_scatter(fig, "Goals")
    # Exactly 2 goal markers for 2 is_goal=1 rows — not 6 (the shot count).
    assert len(goal.x) == 2
    # Goal minutes are 23 (Chelsea) and 84 (Chelsea).
    assert sorted(int(m) for m in goal.x) == [23, 84]


# ── Row 3: ranked delta table ───────────────────────────────────────────────


def test_delta_table_rows_sorted_by_abs_delta() -> None:
    html = render_delta_table_html(
        home_stats=_sample_stats_home(),
        away_stats=_sample_stats_away(),
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={"xG": 1.3, "Possession %": 50, "Pass completion %": 82},
    )
    # Progressive passes has max |delta| = 19, so it should be the top row
    idx_progressive = html.index("Progressive passes")
    idx_pass_completion = html.index("Pass completion")
    assert idx_progressive < idx_pass_completion


def test_delta_table_top_row_has_gold_star() -> None:
    html = render_delta_table_html(
        home_stats=_sample_stats_home(),
        away_stats=_sample_stats_away(),
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={},
    )
    assert "\u2605" in html or "ll-delta-star" in html


def test_delta_table_ppda_direction_label_is_inverted() -> None:
    """Chelsea PPDA 8.2 < Arsenal PPDA 14.5 → Chelsea pressed higher."""
    html = render_delta_table_html(
        home_stats=_sample_stats_home(),
        away_stats=_sample_stats_away(),
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={},
    )
    assert "Chelsea pressed higher" in html


def test_delta_table_handles_missing_league_avgs() -> None:
    html = render_delta_table_html(
        home_stats=_sample_stats_home(),
        away_stats=_sample_stats_away(),
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={},
    )
    assert "Chelsea" in html and "Arsenal" in html
    assert "league avg" not in html.lower()


def test_delta_table_league_avg_annotation_rendered_when_present() -> None:
    html = render_delta_table_html(
        home_stats=_sample_stats_home(),
        away_stats=_sample_stats_away(),
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={"xG": 1.3},
    )
    assert "league avg 1.3" in html


def test_delta_table_xg_direction_uses_created_more() -> None:
    html = render_delta_table_html(
        home_stats={"xG": 2.4},
        away_stats={"xG": 0.8},
        home_name="Chelsea",
        away_name="Arsenal",
        league_avgs={},
    )
    assert "Chelsea created more" in html
