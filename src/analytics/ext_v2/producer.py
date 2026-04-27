"""xT producers for the ExT v2 reproduction harness.

Phase 0 implements ``SinghProducer`` — an independent reimplementation of
``analytics.expected_threat.compute_expected_threat_grid`` that composes
``SinghTransitionMatrix`` + value iteration + per-zone shot/goal/move-
probability aggregation into an ``XTGrid``. The Phase 0 stop condition (per
design spec §6) requires byte-equivalent ``XTGrid.values`` to v1 on
identical inputs.

Phases 1-4 add ``KDESmoothedProducer`` and ``KNNProducer`` subclasses of
``Producer`` that replace the transition-matrix construction step.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Final

import numpy as np
import pandas as pd

from analytics.expected_threat import XTGrid
from analytics.ext_v2.transition import (
    REQUIRED_COLUMNS,
    SINGH_MOVE_TYPES,
    GridSpec,
    SinghTransitionMatrix,
    _assign_zones,
)
from analytics.ext_v2.value_iteration import iterate

if TYPE_CHECKING:
    # Import KDESmoothedTransition only for type checking — runtime uses
    # lazy import inside KDESmoothedProducer.fit() to avoid the circular
    # dep chain (kde.py imports transition.py; producer.py also imports
    # transition.py and would create a cycle if it imported kde.py at
    # module level).
    from analytics.ext_v2.kde import KdeKernel, KDESmoothedTransition

_SHOT_TYPES: Final[frozenset[str]] = frozenset({"shot", "shot_penalty", "shot_freekick"})
"""SPADL shot types contributing to the shot/goal probability per zone.

Mirrors v1's ``analytics.expected_threat._SHOT_TYPES``.
"""


class Producer(ABC):
    """Abstract base for ExT v2 xT producers.

    Concrete implementations:

    - ``SinghProducer`` (Phase 0): Singh-2018 transition + value iteration.
    - ``KDESmoothedProducer`` (Phase 1): KDE-smoothed transition.
    - ``KNNProducer`` (Phase 2+): KNN-based transition.
    """

    @abstractmethod
    def fit(self, actions: pd.DataFrame, *, competition_id: str | None = None) -> Producer: ...

    @property
    @abstractmethod
    def xt_grid(self) -> XTGrid: ...

    @property
    @abstractmethod
    def transition_matrix(self) -> np.ndarray: ...


class SinghProducer(Producer):
    """Singh-2018 xT producer.

    Pipeline mirrors v1's ``compute_expected_threat_grid`` end-to-end:

    1. Filter actions; classify shots vs moves; mark successes.
    2. Bin start positions to flat zone indices via ``GridSpec``.
    3. Per-zone counts: total actions, shots, goals, successful moves.
    4. Per-zone probabilities: shot, goal-conditional-on-shot, move.
    5. Build row-stochastic transition matrix via ``SinghTransitionMatrix``.
    6. Run value iteration to fixed point.
    7. Reshape to ``(n_zones_x, n_zones_y)`` and wrap in ``XTGrid``.
    """

    def __init__(
        self,
        grid: GridSpec | None = None,
        *,
        max_iterations: int = 100,
        tolerance: float = 1e-5,
    ) -> None:
        self.grid: Final = grid if grid is not None else GridSpec()
        self.max_iterations: Final = max_iterations
        self.tolerance: Final = tolerance
        self._transition_model: SinghTransitionMatrix | None = None
        self._xt_grid: XTGrid | None = None

    def fit(
        self,
        actions: pd.DataFrame,
        *,
        competition_id: str | None = None,
    ) -> SinghProducer:
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
        is_shot = np.fromiter(
            (t in _SHOT_TYPES for t in type_names),
            dtype=bool,
            count=len(type_names),
        )
        is_success = result_names == "success"
        successful_moves = is_move & is_success
        successful_shots = is_shot & is_success

        start_x = np.asarray(actions["start_x"], dtype=np.float64)
        start_y = np.asarray(actions["start_y"], dtype=np.float64)
        start_zones = _assign_zones(start_x, start_y, self.grid)

        n_zones = self.grid.n_zones
        total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
        shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
        goals_per_zone = np.bincount(start_zones[successful_shots], minlength=n_zones).astype(np.float64)
        succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

        safe_total = np.maximum(total_per_zone, 1.0)
        shot_prob = shots_per_zone / safe_total
        goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
        move_prob = succ_moves_per_zone / safe_total

        # Composition: delegate transition-matrix construction.
        self._transition_model = SinghTransitionMatrix(grid=self.grid).fit(actions)

        xt_flat, _iters = iterate(
            shot_prob,
            goal_prob,
            move_prob,
            self._transition_model.matrix,
            max_iterations=self.max_iterations,
            tolerance=self.tolerance,
        )

        self._xt_grid = XTGrid(
            values=xt_flat.reshape(self.grid.n_zones_x, self.grid.n_zones_y),
            pitch_length=self.grid.pitch_length,
            pitch_width=self.grid.pitch_width,
            coord_system="spadl",
            competition_id=competition_id,
        )
        return self

    @property
    def xt_grid(self) -> XTGrid:
        if self._xt_grid is None:
            msg = "SinghProducer.fit() must be called before .xt_grid"
            raise RuntimeError(msg)
        return self._xt_grid

    @property
    def transition_matrix(self) -> np.ndarray:
        if self._transition_model is None:
            msg = "SinghProducer.fit() must be called before .transition_matrix"
            raise RuntimeError(msg)
        return self._transition_model.matrix


class KDESmoothedProducer(Producer):
    """KDE-smoothed Singh xT producer (Phase 1).

    Mirrors ``SinghProducer`` end-to-end except the transition step:
    swap ``SinghTransitionMatrix`` for ``KDESmoothedTransition``. Per-zone
    shot/goal/move probabilities and value iteration are unchanged.

    Per spec section 10.3: KDE library is sklearn.KernelDensity; per-source-
    zone smoothing; per-row Silverman with global multiplier when
    adaptive=True.
    """

    def __init__(
        self,
        grid: GridSpec | None = None,
        *,
        kernel: KdeKernel = "gaussian",
        bandwidth: float = 1.0,
        adaptive: bool = False,
        max_iterations: int = 100,
        tolerance: float = 1e-5,
    ) -> None:
        self.grid: Final = grid if grid is not None else GridSpec()
        self.kernel: Final = kernel
        self.bandwidth: Final = bandwidth
        self.adaptive: Final = adaptive
        self.max_iterations: Final = max_iterations
        self.tolerance: Final = tolerance
        self._transition_model: KDESmoothedTransition | None = None
        self._xt_grid: XTGrid | None = None

    def fit(
        self,
        actions: pd.DataFrame,
        *,
        competition_id: str | None = None,
    ) -> KDESmoothedProducer:
        # Import here to avoid a circular import (kde.py imports from transition.py
        # which is also imported by producer.py).
        from analytics.ext_v2.kde import KDESmoothedTransition

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
        is_shot = np.fromiter(
            (t in _SHOT_TYPES for t in type_names),
            dtype=bool,
            count=len(type_names),
        )
        is_success = result_names == "success"
        successful_moves = is_move & is_success
        successful_shots = is_shot & is_success

        start_x = np.asarray(actions["start_x"], dtype=np.float64)
        start_y = np.asarray(actions["start_y"], dtype=np.float64)
        start_zones = _assign_zones(start_x, start_y, self.grid)

        n_zones = self.grid.n_zones
        total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
        shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
        goals_per_zone = np.bincount(start_zones[successful_shots], minlength=n_zones).astype(np.float64)
        succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

        safe_total = np.maximum(total_per_zone, 1.0)
        shot_prob = shots_per_zone / safe_total
        goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
        move_prob = succ_moves_per_zone / safe_total

        # Composition: swap Singh for KDE.
        self._transition_model = KDESmoothedTransition(
            grid=self.grid,
            kernel=self.kernel,
            bandwidth=self.bandwidth,
            adaptive=self.adaptive,
        ).fit(actions)

        xt_flat, _iters = iterate(
            shot_prob,
            goal_prob,
            move_prob,
            self._transition_model.matrix,
            max_iterations=self.max_iterations,
            tolerance=self.tolerance,
        )

        self._xt_grid = XTGrid(
            values=xt_flat.reshape(self.grid.n_zones_x, self.grid.n_zones_y),
            pitch_length=self.grid.pitch_length,
            pitch_width=self.grid.pitch_width,
            coord_system="spadl",
            competition_id=competition_id,
        )
        return self

    @property
    def xt_grid(self) -> XTGrid:
        if self._xt_grid is None:
            msg = "KDESmoothedProducer.fit() must be called before .xt_grid"
            raise RuntimeError(msg)
        return self._xt_grid

    @property
    def transition_matrix(self) -> np.ndarray:
        if self._transition_model is None:
            msg = "KDESmoothedProducer.fit() must be called before .transition_matrix"
            raise RuntimeError(msg)
        return self._transition_model.matrix
