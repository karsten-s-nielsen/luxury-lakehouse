"""Pure freeze-frame normalization port (shared by xG v3 training-export and scoring).

Normalizes a player set to [0,1] fractional pitch position + role flags, oriented so
the shooter always attacks toward high x. Coordinate-invariant by construction: dividing
by the system's own pitch dims yields identical fractional positions across conventions.
NEVER rescales to StatsBomb units.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt


@dataclass(frozen=True)
class PitchDims:
    length: float
    width: float


SPADL_PITCH = PitchDims(105.0, 68.0)


def normalize_freeze_frame(
    players: npt.NDArray[np.floating[Any]],
    pitch: PitchDims,
    *,
    shooter_attacks_high_x: bool,
) -> npt.NDArray[np.floating[Any]]:
    """(N,4) [x, y, is_keeper, is_teammate] metric coords -> (N,4) [x_norm, y_norm, is_keeper, is_teammate].

    x_norm/y_norm in [0,1]; when ``shooter_attacks_high_x`` is False the frame is
    point-reflected (x->L-x, y->W-y) so the shooter attacks high x in every sample.
    """
    if players.shape[0] == 0:
        return np.empty((0, 4), dtype=np.float64)
    xy = players[:, :2].astype(np.float64)
    flags = players[:, 2:4].astype(np.float64)
    if not shooter_attacks_high_x:
        xy = np.column_stack([pitch.length - xy[:, 0], pitch.width - xy[:, 1]])
    norm = np.column_stack([xy[:, 0] / pitch.length, xy[:, 1] / pitch.width])
    return np.column_stack([norm, flags])
