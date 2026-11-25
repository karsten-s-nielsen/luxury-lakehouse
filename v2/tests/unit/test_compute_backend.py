"""The compute backend: numba must be active, and numba must match numpy within the tolerance (perf spec §7.1)."""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

from typing import Any

import pytest
from silly_kicks.tracking.pitch_control import (  # pyright: ignore[reportMissingTypeStubs]
    _spearman,  # pyright: ignore[reportPrivateUsage] - the backend flag; pinned silly-kicks 4.139.1
)
from trackingkit import AWAY, HOME, canonical_frame

from lakehouse.compute.backend import BACKEND_TOLERANCE, numba_active
from lakehouse.compute.pitch_control import pitch_control_values

_KEYS = ["canonical_match_key", "period", "frame", "canonical_team_key"]


def test_numba_backend_is_active() -> None:
    assert numba_active() is True


def test_numba_matches_numpy_within_tolerance(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = canonical_frame()
    compiled: Any = pitch_control_values(frame, home_team_id=HOME, away_team_id=AWAY).to_pandas()
    monkeypatch.setattr(_spearman, "_HAS_NUMBA", False)
    fallback: Any = pitch_control_values(frame, home_team_id=HOME, away_team_id=AWAY).to_pandas()

    compiled = compiled.sort_values(_KEYS).reset_index(drop=True)
    fallback = fallback.sort_values(_KEYS).reset_index(drop=True)
    assert compiled[_KEYS].equals(fallback[_KEYS])
    assert float((compiled["control_fraction"] - fallback["control_fraction"]).abs().max()) <= BACKEND_TOLERANCE
