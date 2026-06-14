"""Cross-provider frame-orientation golden — the always-on guard the idsse-only goldens lacked.

The metrica/skillcorner bimodality and the GradientSports extra-time flip (ADR-053) went
undetected because every golden recomputed ONLY idsse (which `convert_to_frames` already emits
home-LTR). This test recomputes a committed real slice for EACH tracking provider through the
production `_convert_tracking_batch` (which applies `correct_frames_to_home_ltr`) and asserts the
canonical orientation invariant: the home goalkeeper defends LOW x (home attacks +x) in every
period — so the away GK is at HIGH x. It is fast (conversion only, no enrichment; no Spark) and
runs in the default suite.

It fails on the pre-ADR-053 code: GS `10517_p3` (extra-time, home GK at HIGH x) and the
metrica/skillcorner builders (absolute, ~50/50 mirrored) both violate the invariant.
"""

from __future__ import annotations

import pytest

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
)
from analytics.action_context.pipeline import _convert_tracking_batch
from analytics.action_context.work_unit import WorkUnit

_ROOT = "src/tests/fixtures/action_context"
_PITCH_MID_X = 52.5

# One committed tracking slice per provider. gradientsports/10517_p3 is EXTRA TIME (period 3) —
# the exact GS provider-flip case ADR-053 fixes; skillcorner covers the lakehouse absolute-builder
# path (`_bronze_skillcorner_to_frames`, sibling of metrica's builder → same net); idsse covers the
# already-LTR convert_to_frames path (net is a no-op).
#
# metrica (Sample_Game_1_p2) is intentionally NOT here: that committed slice's home_players JSON
# omits the GK jersey ("1"), so the builder yields no goalkeeper rows and the GK-anchored invariant
# can't be evaluated (a fixture-extraction quirk — production metrica resolves GKs, validated by the
# live recompute: P1/P2 0-low/all-high). Re-extracting it would churn test_differential's metrica
# value-golden; the metrica builder's orientation is covered transitively by skillcorner.
_FIXTURES = [
    ("idsse", "J03WMXmini", 1),
    ("skillcorner", "1886347", 2),
    ("gradientsports", "10517", 3),
]


@pytest.mark.parametrize(("provider", "match_id", "period"), _FIXTURES, ids=[f[0] for f in _FIXTURES])
def test_home_gk_defends_low_x(provider: str, match_id: str, period: int) -> None:
    from silly_kicks.tracking._id_compat import ids_match

    wu = WorkUnit(provider=provider, match_id=match_id, period=period)
    frames = ParquetFrameSource(_ROOT).frames(wu).frames
    actions = ParquetActionsSource(_ROOT).actions(wu)
    meta = ParquetMatchMetadataSource(_ROOT).metadata(wu)

    out = _convert_tracking_batch(provider, frames, actions, meta)
    players = out[~out["is_ball"].astype(bool)]
    gk = players[players["is_goalkeeper"].astype(bool)]
    assert not gk.empty, f"{provider}: no goalkeeper rows in converted frames"

    is_home = ids_match(gk["team_id"], meta.home_team_id).fillna(False)
    home_gk_x = gk[is_home]["x"].median()
    away_gk_x = gk[~is_home]["x"].median()

    # Canonical home-LTR: home defends the x=0 goal (GK low), attacks +x; away GK at the x=105 goal.
    assert home_gk_x < _PITCH_MID_X < away_gk_x, (
        f"{provider} {match_id}_p{period}: frame mis-oriented — "
        f"home_gk_x={home_gk_x:.1f}, away_gk_x={away_gk_x:.1f} "
        f"(home GK must defend LOW x; see ADR-053 / correct_frames_to_home_ltr)"
    )

    # Direction labels must agree with the corrected geometry (home attacks ltr, away rtl).
    home_mask = ids_match(players["team_id"], meta.home_team_id).fillna(False)
    home_dirs = set(players[home_mask]["team_attacking_direction"].dropna())
    away_dirs = set(players[~home_mask]["team_attacking_direction"].dropna())
    assert home_dirs <= {"ltr"}, f"{provider}: home direction labels {home_dirs} (expected ltr)"
    assert away_dirs <= {"rtl"}, f"{provider}: away direction labels {away_dirs} (expected rtl)"
