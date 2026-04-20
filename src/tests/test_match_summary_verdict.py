"""Tests for Match Summary verdict derivation (spec §8)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from state.match_summary_verdict import derive_verdict

# (home_xg, away_xg, home_score, away_score, expected_phrase, detail_contains)
VERDICT_CASES = [
    # Fully merited — winner xG >= loser xG + 0.5 (no stronger rule applies)
    (2.4, 1.8, 2, 1, "Fully merited", "0.6"),
    (1.8, 2.4, 1, 2, "Fully merited", "0.6"),  # symmetric — away winner
    # Fair result — |xG delta| < 0.3
    (1.2, 1.0, 1, 0, "Fair result", "0.2"),
    (1.0, 1.2, 0, 1, "Fair result", "0.2"),
    # Fortunate — winner xG < loser xG, but gap < 1.5
    (0.6, 1.2, 1, 0, "Fortunate", "0.6"),
    # Smash & grab — loser xG >= winner xG + 1.5
    (0.4, 2.1, 1, 0, "Smash & grab", "1.7"),
    (2.1, 0.4, 0, 1, "Smash & grab", "1.7"),
    # Flattered by scoreline — winner xG >= 2 * winner goals (score 1-0, xG 2.5)
    # Home wins 1-0 with home_xg 2.5 → winner_xg=2.5, winner_goals=1; 2.5 >= 2*1.
    (2.5, 0.4, 1, 0, "Flattered by scoreline", "2.1"),
    # 0-0 with tiny xG gap — Fair result
    (0.3, 0.5, 0, 0, "Fair result", "0.2"),
    # 0-0 with large xG gap — still Fair result, detail carries the nuance
    (2.1, 0.4, 0, 0, "Fair result", "1.7"),
]


@pytest.mark.parametrize(
    ("home_xg", "away_xg", "home_score", "away_score", "phrase", "detail_fragment"),
    VERDICT_CASES,
)
def test_derive_verdict_vocabulary(
    home_xg: float,
    away_xg: float,
    home_score: int,
    away_score: int,
    phrase: str,
    detail_fragment: str,
) -> None:
    """Each canonical case resolves to the expected phrase + detail contains xG gap."""
    result_phrase, result_detail = derive_verdict(home_xg, away_xg, home_score, away_score)
    assert result_phrase == phrase, (
        f"Expected '{phrase}' for xG {home_xg}-{away_xg}, score {home_score}-{away_score}, "
        f"got '{result_phrase}' with detail '{result_detail}'"
    )
    assert detail_fragment in result_detail, f"Expected detail to contain '{detail_fragment}', got '{result_detail}'"


def test_resolution_order_smash_and_grab_wins_over_fortunate() -> None:
    """Loser xG 2.1 vs winner 0.4 (gap 1.7): Smash & grab applies AND winner<loser.
    Smash & grab must win (higher priority)."""
    phrase, _ = derive_verdict(0.4, 2.1, 1, 0)
    assert phrase == "Smash & grab"


def test_resolution_order_flattered_wins_over_fully_merited() -> None:
    """xG 4.2 vs 0.5, score 2-0 — Fully merited (gap >= 0.5) AND Flattered
    (winner xG 4.2 >= 2*2=4). Flattered has higher priority."""
    phrase, _ = derive_verdict(4.2, 0.5, 2, 0)
    assert phrase == "Flattered by scoreline"


def test_detail_always_has_xg_gap_number() -> None:
    """Detail annotation must carry a numeric xG-gap value."""
    _, detail = derive_verdict(2.4, 0.8, 2, 1)
    assert "1.6" in detail


def test_pure_function_no_side_effects() -> None:
    """Calling twice with the same inputs yields identical outputs."""
    a = derive_verdict(2.4, 0.8, 2, 1)
    b = derive_verdict(2.4, 0.8, 2, 1)
    assert a == b


def test_draw_with_dominant_team_uses_fair_result_but_detail_flags_gap() -> None:
    """0-0 draw where one side dominated xG is still Fair result, but
    detail surfaces which side was dominant."""
    phrase, detail = derive_verdict(2.1, 0.4, 0, 0)
    assert phrase == "Fair result"
    assert "Home" in detail
    assert "1.7" in detail


def test_flattered_requires_winner_goals_positive() -> None:
    """A 0-0 scoreline never routes to 'Flattered by scoreline' (no winner)."""
    phrase, _ = derive_verdict(3.5, 0.4, 0, 0)
    assert phrase == "Fair result"  # draw short-circuit wins
