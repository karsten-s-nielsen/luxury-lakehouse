"""Match Summary verdict derivation — pure function per spec §8.

Maps ``(home_xg, away_xg, home_score, away_score)`` to one of seven phrases
plus a compact xG-gap detail string.

Resolution order (first match wins):
    1. Draw → always Fair result (detail carries xG gap nuance).
    2. Defensive masterclass — loser xG < 0.5.
    3. Detect comeback flag from goals timeline.
    4. Smash & grab    — loser xG ≥ winner xG + 1.5.
    5. Flattered       — winner xG >= 2 * winner goals AND winner scored > 0.
    6. Fortunate       — winner xG < loser xG (winning against xG, but gap < 1.5).
    7. Fair result     — |xG Δ| < 0.3.
    8. Fully merited   — default for clear wins where none of the above apply.

Comeback win is a prefix applied to verdicts 4-8 when the eventual winner
trailed (strictly behind) at any point during the match.

Pure function: no Taipy state, no DB access, fully deterministic.
"""

from __future__ import annotations

_SMASH_AND_GRAB_GAP = 1.5
_FAIR_RESULT_GAP = 0.3
_FLATTERED_XG_RATIO = 2.0
_DEFENSIVE_MASTERCLASS_XG = 0.5


def _detect_comeback(goals: list[tuple[int, str]], home_score: int, away_score: int) -> bool:
    """Return True if the eventual winner trailed (strictly behind) at any point."""
    winner = "home" if home_score > away_score else "away"
    running_home, running_away = 0, 0
    for _minute, side in sorted(goals, key=lambda g: g[0]):
        if side == "home":
            running_home += 1
        else:
            running_away += 1
        if winner == "home" and running_home < running_away:
            return True
        if winner == "away" and running_away < running_home:
            return True
    return False


def derive_verdict(
    home_xg: float,
    away_xg: float,
    home_score: int,
    away_score: int,
    goals: list[tuple[int, str]] | None = None,
) -> tuple[str, str]:
    """Return (phrase, detail) for the Match Summary "Our Verdict" tile.

    Parameters
    ----------
    goals : list of (minute, "home"|"away") tuples, or None.
        When provided, enables comeback detection. Each tuple represents
        a goal scored by the named side at the given minute.
    """
    xg_gap = abs(home_xg - away_xg)
    higher_xg_label = "Home" if home_xg > away_xg else ("Away" if away_xg > home_xg else "Even")
    detail = f"{higher_xg_label} +{xg_gap:.1f} xG gap (Home {home_xg:.1f} vs Away {away_xg:.1f})"

    if home_score == away_score:
        return "Fair result", detail

    if home_score > away_score:
        winner_xg, loser_xg, winner_goals = home_xg, away_xg, home_score
    else:
        winner_xg, loser_xg, winner_goals = away_xg, home_xg, away_score

    # Defensive masterclass — dominant narrative when opponent barely threatened
    if loser_xg < _DEFENSIVE_MASTERCLASS_XG:
        return "Defensive masterclass", detail

    # Comeback detection
    comeback = False
    if goals is not None:
        comeback = _detect_comeback(goals, home_score, away_score)

    # Base verdict resolution
    if loser_xg >= winner_xg + _SMASH_AND_GRAB_GAP:
        base = "Smash & grab"
    elif winner_goals > 0 and winner_xg >= _FLATTERED_XG_RATIO * winner_goals:
        base = "Flattered by scoreline"
    elif winner_xg < loser_xg:
        base = "Fortunate"
    elif xg_gap < _FAIR_RESULT_GAP:
        base = "Fair result"
    else:
        base = "Fully merited"

    if comeback:
        return f"Comeback win — {base}", detail
    return base, detail
