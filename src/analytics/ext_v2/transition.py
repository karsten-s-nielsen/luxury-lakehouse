"""Transition models for the ExT v2 reproduction harness.

Phase 0 implements ``SinghTransitionMatrix`` — an independent reimplementation
of v1's ``analytics.expected_threat._build_transition_matrix`` (combined with
the upstream filtering + zone-binning steps that v1 inlines into
``compute_expected_threat_grid``). The Phase 0 stop condition (per design spec
§6) requires byte-equivalent output to v1 on identical inputs.

Phases 1-4 add ``KDESmoothedTransition`` and ``KNNTransition`` subclasses of
``TransitionModel`` that the harness can drop in via Optuna axes without
disturbing Phase 0's contract.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

SINGH_MOVE_TYPES: Final[frozenset[str]] = frozenset(
    {
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "dribble",
        "goalkick",
        "clearance",
    }
)
"""SPADL move types contributing to Singh's transition matrix.

Mirrors v1's ``analytics.expected_threat._MOVE_TYPES``; the parity test in
``test_transition.py::TestParityWithV1`` enforces equality.
"""

REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "type_name",
    "result_name",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
)


@dataclass(frozen=True)
class GridSpec:
    """Pitch-grid binning specification.

    Mirrors the grid fields of v1's ``ExpectedThreatParams`` so the Phase 0
    parity test can assert equality. Phase 1+ may extend with non-uniform
    binning if KDE smoothing motivates it.
    """

    n_zones_x: int = 12
    n_zones_y: int = 8
    pitch_length: float = 105.0  # SPADL coordinates
    pitch_width: float = 68.0

    @property
    def n_zones(self) -> int:
        return self.n_zones_x * self.n_zones_y


def _assign_zones(
    x: np.ndarray,
    y: np.ndarray,
    grid: GridSpec,
) -> np.ndarray:
    """Map ``(x, y)`` SPADL coordinates to flat zone indices.

    Mirror of v1 ``_assign_zones`` with grid carried by ``GridSpec``. The
    parity test in ``test_transition.py::TestParityWithV1`` enforces
    numerical equality with v1 across grid resolutions.
    """
    zone_x = np.clip(
        (x / grid.pitch_length * grid.n_zones_x).astype(int),
        0,
        grid.n_zones_x - 1,
    )
    zone_y = np.clip(
        (y / grid.pitch_width * grid.n_zones_y).astype(int),
        0,
        grid.n_zones_y - 1,
    )
    return zone_x * grid.n_zones_y + zone_y


class TransitionModel(ABC):
    """Abstract base for ExT v2 transition models.

    Concrete implementations:

    - ``SinghTransitionMatrix`` (Phase 0): row-normalized counts.
    - ``KDESmoothedTransition`` (Phase 1): KDE-smoothed counts.
    - ``KNNTransition`` (Phase 2+): KNN over (source, [context]) → destination.
    """

    @abstractmethod
    def fit(self, actions: pd.DataFrame) -> TransitionModel: ...

    @property
    @abstractmethod
    def matrix(self) -> np.ndarray: ...


class SinghTransitionMatrix(TransitionModel):
    """Singh-2018 transition matrix: row-normalized counts of successful moves.

    The matrix is built only from rows whose ``type_name`` is in
    ``SINGH_MOVE_TYPES`` AND ``result_name == "success"`` — the same filter v1
    applies inline in ``compute_expected_threat_grid``.
    """

    def __init__(self, grid: GridSpec | None = None) -> None:
        self.grid: Final = grid if grid is not None else GridSpec()
        self._matrix: np.ndarray | None = None

    def fit(self, actions: pd.DataFrame) -> SinghTransitionMatrix:
        missing = [col for col in REQUIRED_COLUMNS if col not in actions.columns]
        if missing:
            msg = f"actions missing required columns: {missing}"
            raise ValueError(msg)

        type_names = actions["type_name"].to_numpy()
        result_names = actions["result_name"].to_numpy()
        is_move = np.fromiter(
            (t in SINGH_MOVE_TYPES for t in type_names),
            dtype=bool,
            count=len(type_names),
        )
        is_success = result_names == "success"
        mask = is_move & is_success

        start_x = np.asarray(actions["start_x"], dtype=np.float64)[mask]
        start_y = np.asarray(actions["start_y"], dtype=np.float64)[mask]
        end_x = np.asarray(actions["end_x"], dtype=np.float64)[mask]
        end_y = np.asarray(actions["end_y"], dtype=np.float64)[mask]

        start_zones = _assign_zones(start_x, start_y, self.grid)
        end_zones = _assign_zones(end_x, end_y, self.grid)

        n = self.grid.n_zones
        transition = np.zeros((n, n), dtype=np.float64)
        np.add.at(transition, (start_zones, end_zones), 1.0)
        row_sums = transition.sum(axis=1, keepdims=True)
        row_sums = np.maximum(row_sums, 1.0)
        self._matrix = transition / row_sums
        return self

    @property
    def matrix(self) -> np.ndarray:
        if self._matrix is None:
            msg = "SinghTransitionMatrix.fit() must be called before .matrix"
            raise RuntimeError(msg)
        return self._matrix
