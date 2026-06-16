"""Cross-provider frame-Y-IDENTITY golden — guards the absolute-builder y-handedness.

The sibling `test_frame_orientation_golden.py` asserts the home GK defends LOW x — an **x-based**
invariant. A single-axis y-mirror (`y -> 68 - y`) is OUTSIDE the orientation family (ADR-053's
`correct_frames_to_home_ltr` is a 180deg point-reflection, never a single-axis flip), so the
orientation golden is structurally BLIND to it. This golden closes that gap: it recomputes a committed
real slice through the production `_convert_tracking_batch` and asserts the acting player's tracked
frame-y matches the SPADL action `start_y` at the action instant — restricted to OFF-CENTRE actions
(`|start_y - 34| > 8`) where a y-mirror is large and unambiguous (the error `|68 - 2y|` is zero at the
y=34 centre line, which is exactly why the bug hid).

Origin: the silly-kicks kloppy-tracking-y inversion (ADR-031, their numbering). Gate C established the
lakehouse builds SkillCorner/Metrica frames via its OWN `convert.py` builders, NOT the silly-kicks
kloppy gateway that PR-S94 fixed — so those builders need an independent guard. The SkillCorner builder
(`_bronze_skillcorner_to_frames`, `y = y_center + 34`, no flip) was verified y-correct
(action<->player d_identity = 0.000 m vs d_yflip = 31.5 m on `1886347_p2`).

Metrica (`_bronze_metrica_to_frames`, `y = (1 - y01) * 68`, flips) was CONFIRMED Y-INVERTED on a live
full-match check (Sample_Game_1 P1, n=346 acting-player + 331 ball: y-mirror wins 335/346, d_yflip
median 0.19 m vs d_identity 43.4 m; identical PRE- and POST-`correct_frames_to_home_ltr`, so the
inversion reaches production). The builder's `(1 - y01)` flip is wrong — the correct map is `y01 * 68`.
Metrica is therefore NOT in `_FIXTURES` yet: it is added (RED -> GREEN) by the builder-fix PR, which
also recomputes Metrica AC + retrains Metrica-dependent models.

Fast (conversion only, no enrichment, no Spark); runs in the default suite. Fails on a y-mirror
regression in either absolute builder.
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.action_context.convert import _SKILLCORNER_PERIOD_START_SECONDS
from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
)
from analytics.action_context.pipeline import _convert_tracking_batch
from analytics.action_context.work_unit import WorkUnit

_ROOT = "src/tests/fixtures/action_context"
_PITCH_MID_Y = 34.0
_OFF_CENTRE = 8.0  # |start_y - 34| > 8 — where a y-mirror is large
_X_ALIGN = 3.0  # only score actions whose acting player x matches (orientation already aligned)
_MAX_GAP_S = 0.20  # nearest action<->frame time tolerance
_Y_IDENTITY_MAX = 1.5  # m — acting-player frame-y must sit within this of action start_y
_Y_FLIP_MIN = 10.0  # m — the mirror must be far (proves the golden is sensitive to a y-flip)

# One committed tracking slice per absolute-builder provider. metrica is intentionally pending
# (its committed slice's action<->frame association is too sparse to localize; closed separately by a
# live-data check — see module docstring).
_FIXTURES = [
    ("skillcorner", "1886347", 2),
]


def _rebase_frame_time(provider: str, frames):
    """Frame ``time_seconds`` -> action-comparable clock. SkillCorner frames carry the CONTINUOUS
    broadcast clock (P2 = 2700+) while SPADL actions are period-relative (silly-kicks 4.20.1); the
    production DISPATCH layer subtracts the period start, so mirror that here."""
    if provider == "skillcorner":
        return frames["time_seconds"] - frames["period_id"].map(_SKILLCORNER_PERIOD_START_SECONDS).fillna(0.0)
    return frames["time_seconds"]


@pytest.mark.parametrize(("provider", "match_id", "period"), _FIXTURES, ids=[f[0] for f in _FIXTURES])
def test_acting_player_frame_y_matches_action_off_centre(provider: str, match_id: str, period: int) -> None:
    wu = WorkUnit(provider=provider, match_id=match_id, period=period)
    frames = ParquetFrameSource(_ROOT).frames(wu).frames
    actions = ParquetActionsSource(_ROOT).actions(wu)
    meta = ParquetMatchMetadataSource(_ROOT).metadata(wu)

    out = _convert_tracking_batch(provider, frames, actions, meta)
    players = out[~out["is_ball"].astype(bool)].dropna(subset=["x", "y"]).copy()
    players["player_id"] = players["player_id"].astype("string")
    players["_t"] = _rebase_frame_time(provider, players)

    acts = actions.copy()
    acts["player_id_native"] = acts["player_id_native"].astype("string")
    home = acts[acts["team_id_native"] == acts["home_team_id_native"]]
    off = home[(home["start_y"] - _PITCH_MID_Y).abs() > _OFF_CENTRE]

    by_player = dict(tuple(players.groupby(["period_id", "player_id"])))
    y_err: list[float] = []
    y_flip: list[float] = []
    for _, ac in off.iterrows():
        g = by_player.get((ac["period_id"], ac["player_id_native"]))
        if g is None or g.empty:
            continue
        gt = g["_t"].to_numpy(dtype=float)
        k = int(np.abs(gt - float(ac["time_seconds"])).argmin())
        if abs(gt[k] - float(ac["time_seconds"])) > _MAX_GAP_S:
            continue
        px = float(g["x"].to_numpy(dtype=float)[k])
        py = float(g["y"].to_numpy(dtype=float)[k])
        sx, sy = float(ac["start_x"]), float(ac["start_y"])
        if abs(sx - px) >= _X_ALIGN:  # skip orientation-mismatched samples — this golden tests Y only
            continue
        y_err.append(abs(sy - py))
        y_flip.append(abs(sy - (68.0 - py)))

    assert len(y_err) >= 3, f"{provider} {match_id}_p{period}: too few localizable off-centre actions ({len(y_err)})"

    med_err = sorted(y_err)[len(y_err) // 2]
    med_flip = sorted(y_flip)[len(y_flip) // 2]
    assert med_err < _Y_IDENTITY_MAX, (
        f"{provider} {match_id}_p{period}: acting-player frame-y does NOT match action start_y "
        f"(median |Δy|={med_err:.2f} m, n={len(y_err)}) — Y-MIRROR regression in the absolute builder "
        f"(see test_frame_y_identity_golden docstring / ADR-053)"
    )
    # Sensitivity guard: a y-mirror would have made med_err large and med_flip ~0. Assert the mirror is
    # demonstrably far, so this test actually has teeth against a future flip.
    assert med_flip > _Y_FLIP_MIN, (
        f"{provider} {match_id}_p{period}: y-flip distance unexpectedly small "
        f"(median={med_flip:.2f} m) — golden lost its sensitivity to a y-mirror"
    )
