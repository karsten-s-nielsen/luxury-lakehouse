"""Match Summary verdict derivation — pure function per spec §8.

Maps ``(home_xg, away_xg, home_score, away_score)`` to one of five phrases
(Smash & grab, Flattered by scoreline, Fortunate, Fair result, Fully merited)
plus a compact xG-gap detail string.

Resolution order (first match wins):
    1. Draw → always Fair result (detail carries xG gap nuance).
    2. Smash & grab    — loser xG ≥ winner xG + 1.5.
    3. Flattered       — winner xG >= 2 * winner goals AND winner scored > 0.
    4. Fortunate       — winner xG < loser xG (winning against xG, but gap < 1.5).
    5. Fair result     — |xG Δ| < 0.3.
    6. Fully merited   — default for clear wins where none of the above apply.

Pure function: no Taipy state, no DB access, fully deterministic.
"""

from __future__ import annotations

_SMASH_AND_GRAB_GAP = 1.5
_FAIR_RESULT_GAP = 0.3
_FLATTERED_XG_RATIO = 2.0


def derive_verdict(
    home_xg: float,
    away_xg: float,
    home_score: int,
    away_score: int,
) -> tuple[str, str]:
    """Return (phrase, detail) for the Match Summary "Our Verdict" tile.

    See module docstring for resolution rules and spec §8 for the user-facing
    vocabulary.
    """
    xg_gap = abs(home_xg - away_xg)
    higher_xg_label = "Home" if home_xg > away_xg else "Away"
    detail = f"{higher_xg_label} +{xg_gap:.1f} xG gap (Home {home_xg:.1f} vs Away {away_xg:.1f})"

    if home_score == away_score:
        # Draws: no winner/loser, and the numeric gap goes into `detail`
        # so tight draws (fair) and dominated draws (wasted) both render
        # as "Fair result" with a meaningful annotation.
        return "Fair result", detail

    if home_score > away_score:
        winner_xg, loser_xg, winner_goals = home_xg, away_xg, home_score
    else:
        winner_xg, loser_xg, winner_goals = away_xg, home_xg, away_score

    if loser_xg >= winner_xg + _SMASH_AND_GRAB_GAP:
        return "Smash & grab", detail
    if winner_goals > 0 and winner_xg >= _FLATTERED_XG_RATIO * winner_goals:
        return "Flattered by scoreline", detail
    if winner_xg < loser_xg:
        return "Fortunate", detail
    if xg_gap < _FAIR_RESULT_GAP:
        return "Fair result", detail
    return "Fully merited", detail
