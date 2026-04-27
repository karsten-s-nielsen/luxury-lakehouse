"""KDE-smoothed transition for the ExT v2 reproduction harness (Phase 1).

Per-source-zone 2D ``sklearn.neighbors.KernelDensity`` wrapper with optional
Silverman-scaled per-row bandwidth. Locked design decisions in
``docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md`` §10.3.

The class is a drop-in subclass of ``TransitionModel`` — composition with
``value_iteration.iterate`` and ``XTGrid`` wrap is unchanged from Phase 0,
so a Phase 0 ``SinghProducer`` swapped to a ``KDESmoothedProducer`` is the
only producer-layer change.
"""

from __future__ import annotations

from typing import Final, Literal

import numpy as np
import pandas as pd
from sklearn.neighbors import KernelDensity

from analytics.ext_v2.transition import (
    REQUIRED_COLUMNS,
    SINGH_MOVE_TYPES,
    GridSpec,
    TransitionModel,
    _assign_zones,
)

KdeKernel = Literal["gaussian", "epanechnikov", "tophat"]


def silverman_2d(n: int, sigma: float) -> float:
    """2D Silverman's rule of thumb bandwidth.

    For d-dimensional KDE with Gaussian kernel:
        h = (4 / (d + 2))^(1 / (d + 4)) * n^(-1 / (d + 4)) * sigma

    With d=2 this simplifies to h = n^(-1/6) * sigma (the leading constant
    (4/4)^(1/6) = 1). See Silverman 1986 §4.3.

    Args:
        n: Sample size for this row. Must be >= 1.
        sigma: Isotropic sigma proxy — by convention
            ``sqrt((var_x + var_y) / 2)`` from per-row destination positions.

    Returns:
        Per-row bandwidth.

    Raises:
        ValueError: if ``n < 1``.
    """
    if n < 1:
        msg = f"n must be >= 1 for Silverman bandwidth (got {n}); use row-mean fallback for n=0"
        raise ValueError(msg)
    return float(n ** (-1 / 6) * sigma)


class KDESmoothedTransition(TransitionModel):
    """KDE-smoothed transition matrix — per-source-zone 2D KDE on destinations.

    Per spec §10.3 Q2: for each source zone ``s``, fit a 2D
    ``KernelDensity`` over ``(end_x, end_y)`` of successful Singh-move
    events whose ``start_zone == s``. Evaluate the fitted KDE at each of
    the ``n_zones`` destination-zone centres (B1 point evaluation), exp
    the log-density, row-normalize → row ``s`` of the transition matrix.

    Per spec §10.3 Q3: when ``adaptive=True``, the per-row bandwidth is
    ``h_s = bandwidth * silverman_2d(n_s)`` where
    ``silverman_2d(n) = n^(-1/6) * sigma_s`` and ``sigma_s`` is the
    isotropic ``sqrt((var_x + var_y) / 2)`` of that row's destination
    positions. When ``adaptive=False``, all rows share ``h = bandwidth``.

    Edge cases:

    - Zero-event source zone (``n_s == 0``): row falls back to mean of all
      other rows. Materially affects NLL only when holdout passes start in
      a zone with zero train events — vanishingly rare on real data.
    """

    def __init__(
        self,
        grid: GridSpec | None = None,
        *,
        kernel: KdeKernel = "gaussian",
        bandwidth: float = 1.0,
        adaptive: bool = False,
    ) -> None:
        self.grid: Final = grid if grid is not None else GridSpec()
        self.kernel: Final = kernel
        self.bandwidth: Final = bandwidth
        self.adaptive: Final = adaptive
        self._matrix: np.ndarray | None = None

    def fit(self, actions: pd.DataFrame) -> KDESmoothedTransition:
        missing = [col for col in REQUIRED_COLUMNS if col not in actions.columns]
        if missing:
            msg = f"actions missing required columns: {missing}"
            raise ValueError(msg)

        # Filter to Singh successful moves (mirror SinghTransitionMatrix).
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

        n_zones = self.grid.n_zones
        # Destination-zone centre coordinates in SPADL space (point evaluation per Q2).
        cell_w = self.grid.pitch_length / self.grid.n_zones_x
        cell_h = self.grid.pitch_width / self.grid.n_zones_y
        zone_centres = np.empty((n_zones, 2), dtype=np.float64)
        for dz in range(n_zones):
            dzx = dz // self.grid.n_zones_y
            dzy = dz % self.grid.n_zones_y
            zone_centres[dz, 0] = (dzx + 0.5) * cell_w
            zone_centres[dz, 1] = (dzy + 0.5) * cell_h

        matrix = np.zeros((n_zones, n_zones), dtype=np.float64)
        end_xy = np.column_stack([end_x, end_y])
        for s in range(n_zones):
            row_mask = start_zones == s
            n_s = int(row_mask.sum())
            if n_s == 0:
                # Zero-event fallback lands in Task B7 (kept zero here so it's testable).
                continue
            if self.adaptive:
                row_dest = end_xy[row_mask]
                var_x = float(row_dest[:, 0].var())
                var_y = float(row_dest[:, 1].var())
                sigma_s = float(np.sqrt((var_x + var_y) / 2.0))
                if sigma_s == 0.0:
                    sigma_s = 1e-6  # degenerate single-point row; tiny epsilon
                h_s = self.bandwidth * silverman_2d(n_s, sigma_s)
            else:
                h_s = self.bandwidth
            kde = KernelDensity(kernel=self.kernel, bandwidth=h_s)
            kde.fit(end_xy[row_mask])
            log_density = kde.score_samples(zone_centres)
            density = np.exp(log_density)
            row_sum = density.sum()
            if row_sum > 0:
                matrix[s] = density / row_sum

        # Zero-event source zones: fall back to mean of populated rows.
        # Per spec §10.3 Q3.
        row_sums = matrix.sum(axis=1)
        zero_rows = row_sums == 0
        populated_rows = matrix[~zero_rows]
        if populated_rows.shape[0] > 0:
            fallback_row = populated_rows.mean(axis=0)
            # The mean of row-stochastic rows is row-stochastic — but defensive
            # re-normalization absorbs floating-point drift.
            fb_sum = fallback_row.sum()
            if fb_sum > 0:
                fallback_row = fallback_row / fb_sum
            matrix[zero_rows] = fallback_row
        else:
            # Pathological case: every source zone is zero-event. Uniform fallback.
            matrix[:] = 1.0 / n_zones

        self._matrix = matrix
        return self

    @property
    def matrix(self) -> np.ndarray:
        if self._matrix is None:
            msg = "KDESmoothedTransition.fit() must be called before .matrix"
            raise RuntimeError(msg)
        return self._matrix
