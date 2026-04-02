"""Off-Ball Scoring Opportunity (OBSO) value surface computation.

OBSO = PPCF x Transition(ball -> cell) x EPV(cell)

Computes a continuous value surface indicating the scoring opportunity at
each point on the pitch for the team in possession, accounting for pitch
control, ball transition probabilities, and expected possession value.

References:
    Spearman (2018). "Beyond Expected Goals." MIT Sloan.
    Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
    Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.pitch_control import (
    PitchControlParams,
    compute_pitch_control_at_points,
)

# ---------------------------------------------------------------------------
# Synthetic grid fallbacks
# ---------------------------------------------------------------------------


def _make_synthetic_reachability_grid(ny: int = 100, nx: int = 64) -> np.ndarray:
    """Gaussian distance decay proxy for ball reachability.

    Used as fallback when trained grids are not available.
    Shape: (ny, nx) — OBSO convention.
    """
    y = np.linspace(0, 1, ny)
    x = np.linspace(0, 1, nx)
    yy, xx = np.meshgrid(y, x, indexing="ij")
    center_y, center_x = 0.5, 0.5
    dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)
    return np.exp(-(dist**2) / (2 * 0.3**2))


def _make_synthetic_epv_grid(ny: int = 50, nx: int = 32) -> np.ndarray:
    """Linear ramp proxy for EPV. Shape: (ny, nx)."""
    x = np.linspace(0.01, 0.3, nx)
    return np.tile(x, (ny, 1))


def get_default_grids(
    reachability: np.ndarray | None = None,
    epv: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Return reachability and EPV grids, falling back to synthetic defaults.

    Both grids use (ny, nx) shape convention matching compute_obso_surface().
    When pre-loaded arrays are provided, they are used directly. Otherwise,
    synthetic proxy grids are generated (pure computation, no I/O).

    Args:
        reachability: Pre-loaded reachability grid, or None for synthetic.
        epv: Pre-loaded EPV grid, or None for synthetic.

    Returns:
        (reachability_grid, epv_grid) — both (ny, nx) shaped.
    """
    if reachability is None:
        reachability = _make_synthetic_reachability_grid()
    if epv is None:
        epv = _make_synthetic_epv_grid()

    return reachability, epv


# ---------------------------------------------------------------------------
# Grid interpolation
# ---------------------------------------------------------------------------


def interpolate_grid(grid: np.ndarray, target_shape: tuple[int, int]) -> np.ndarray:
    """Resize a grid to match PPCF grid dimensions via bilinear interpolation.

    Uses numpy-only bilinear interpolation (no scipy dependency at runtime).

    Args:
        grid: Source 2D array of shape (src_rows, src_cols).
        target_shape: Desired output shape (target_rows, target_cols).

    Returns:
        Interpolated 2D array of shape ``target_shape``.
    """
    src_rows, src_cols = grid.shape
    tgt_rows, tgt_cols = target_shape

    if (src_rows, src_cols) == target_shape:
        return grid.copy()

    # Build target coordinate grids mapping to source indices
    row_coords = np.linspace(0, src_rows - 1, tgt_rows)
    col_coords = np.linspace(0, src_cols - 1, tgt_cols)
    col_grid, row_grid = np.meshgrid(col_coords, row_coords)

    # Floor/ceil indices
    r0 = np.clip(np.floor(row_grid).astype(int), 0, src_rows - 2)
    r1 = r0 + 1
    c0 = np.clip(np.floor(col_grid).astype(int), 0, src_cols - 2)
    c1 = c0 + 1

    # Fractional parts
    dr = row_grid - r0
    dc = col_grid - c0

    # Bilinear interpolation
    result = (
        grid[r0, c0] * (1 - dr) * (1 - dc)
        + grid[r1, c0] * dr * (1 - dc)
        + grid[r0, c1] * (1 - dr) * dc
        + grid[r1, c1] * dr * dc
    )
    return result


# ---------------------------------------------------------------------------
# OBSO surface computation
# ---------------------------------------------------------------------------


def compute_obso_surface(
    ppcf_grid: np.ndarray,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    ball_position: tuple[float, float],
    grid_x: np.ndarray,
    grid_y: np.ndarray,
) -> np.ndarray:
    """Compute OBSO surface: PPCF x Transition(ball -> cell) x EPV(cell).

    The transition grid gives P(ball reaches cell | ball at ball_position).
    This requires interpolating the pre-computed transition grid from the
    ball position to the target grid shape.

    The EPV grid gives the expected possession value at each cell if the
    team gains control there.

    Args:
        ppcf_grid: (ny, nx) pitch control probabilities in [0, 1].
        transition_grid: (tr, tc) pre-computed ball transition probabilities.
        epv_grid: (er, ec) pre-computed expected possession value grid.
        ball_position: (x, y) ball coordinates in StatsBomb 120x80 space.
        grid_x: (nx,) x-coordinates of PPCF grid cells.
        grid_y: (ny,) y-coordinates of PPCF grid cells.

    Returns:
        (ny, nx) OBSO surface with values in [0, 1].
    """
    ny, nx = ppcf_grid.shape

    # Interpolate static grids to match PPCF dimensions
    transition_interp = interpolate_grid(transition_grid, (ny, nx))
    epv_interp = interpolate_grid(epv_grid, (ny, nx))

    # Shift transition grid based on ball position:
    # The transition grid is centered on the ball — weight cells by distance
    # from ball position as a proxy for ball reachability.
    # For a proper implementation, this would index into a pre-computed
    # transition model conditioned on ball position.  Here we use the
    # interpolated grid directly, modulated by Gaussian distance decay from
    # the ball to approximate transition likelihood.
    ball_x, ball_y = ball_position
    xx, yy = np.meshgrid(grid_x, grid_y)
    # Gaussian decay: sigma scaled to ~1/4 pitch length for reasonable spread
    sigma_x = 30.0  # StatsBomb units
    sigma_y = 20.0
    distance_weight = np.exp(-((xx - ball_x) ** 2) / (2.0 * sigma_x**2) - (yy - ball_y) ** 2 / (2.0 * sigma_y**2))

    # Combine: transition probability conditioned on ball position
    effective_transition = transition_interp * distance_weight
    # Normalize so max transition = 1 (probabilities relative to best target)
    max_trans = np.max(effective_transition)
    if max_trans > 1e-10:
        effective_transition = effective_transition / max_trans

    # OBSO = PPCF x Transition x EPV
    obso = ppcf_grid * effective_transition * epv_interp

    return np.clip(obso, 0.0, 1.0)


# ---------------------------------------------------------------------------
# Pass-level OBSO metrics (PAUSA inputs)
# ---------------------------------------------------------------------------


def compute_pass_obso(
    ghost_frames: list[pd.DataFrame],
    event_frame_idx: int,
    target_position: tuple[float, float],
    teammate_positions: np.ndarray,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    params: PitchControlParams | None = None,
    grid_nx: int = 104,
    grid_ny: int = 68,
) -> dict[str, float]:
    """Compute PAUSA-relevant OBSO metrics for one pass.

    Evaluates OBSO at the target position across all ghost frames to
    determine temporal judgment (when to pass) and spatial selection
    (where to pass).

    Args:
        ghost_frames: List of ghost-trajectory DataFrames (from
            ``generate_ghost_trajectories``).
        event_frame_idx: Index into ``ghost_frames`` for the actual event
            frame (typically ``int(window_before_s * frame_rate)``).
        target_position: (x, y) of the actual pass target in StatsBomb
            120x80 coordinates.
        teammate_positions: (n_teammates, 2) array of teammate positions
            in StatsBomb 120x80 coordinates at the event frame.
        transition_grid: Pre-computed ball transition probability grid.
        epv_grid: Pre-computed expected possession value grid.
        params: Pitch control parameters.
        grid_nx: Number of grid cells along x-axis for PPCF computation.
        grid_ny: Number of grid cells along y-axis for PPCF computation.

    Returns:
        Dictionary with keys:
            - ``actual_obso``: OBSO at target position at event frame.
            - ``peak_obso``: Maximum OBSO at target position across all
              ghost frames.
            - ``optimal_obso``: Maximum OBSO across all teammate positions
              at the event frame.
    """
    if params is None:
        params = PitchControlParams()

    target_arr = np.array([list(target_position)])

    # --- actual_obso: OBSO at target at the event frame ---
    event_df = ghost_frames[event_frame_idx]
    event_ppcf_at_target = compute_pitch_control_at_points(event_df, target_arr, params)
    actual_obso = float(event_ppcf_at_target[0])

    # Modulate by transition and EPV at target cell
    transition_interp = interpolate_grid(transition_grid, (grid_ny, grid_nx))
    epv_interp = interpolate_grid(epv_grid, (grid_ny, grid_nx))

    # Map target to grid indices
    tx_idx = int(np.clip(target_position[0] / params.sb_length * (grid_nx - 1), 0, grid_nx - 1))
    ty_idx = int(np.clip(target_position[1] / params.sb_width * (grid_ny - 1), 0, grid_ny - 1))
    trans_at_target = float(transition_interp[ty_idx, tx_idx])
    epv_at_target = float(epv_interp[ty_idx, tx_idx])
    actual_obso = float(np.clip(actual_obso * trans_at_target * epv_at_target, 0.0, 1.0))

    # --- peak_obso: max OBSO at target across all ghost frames ---
    peak_obso = actual_obso
    for i, frame_df in enumerate(ghost_frames):
        if i == event_frame_idx:
            continue
        ppcf_val = compute_pitch_control_at_points(frame_df, target_arr, params)
        frame_obso = float(np.clip(float(ppcf_val[0]) * trans_at_target * epv_at_target, 0.0, 1.0))
        if frame_obso > peak_obso:
            peak_obso = frame_obso

    # --- optimal_obso: max OBSO across teammate positions at event frame ---
    optimal_obso = actual_obso
    if len(teammate_positions) > 0:
        tm_ppcf = compute_pitch_control_at_points(event_df, teammate_positions, params)
        for j in range(len(teammate_positions)):
            tm_x_idx = int(np.clip(teammate_positions[j, 0] / params.sb_length * (grid_nx - 1), 0, grid_nx - 1))
            tm_y_idx = int(np.clip(teammate_positions[j, 1] / params.sb_width * (grid_ny - 1), 0, grid_ny - 1))
            tm_trans = float(transition_interp[tm_y_idx, tm_x_idx])
            tm_epv = float(epv_interp[tm_y_idx, tm_x_idx])
            tm_obso = float(np.clip(float(tm_ppcf[j]) * tm_trans * tm_epv, 0.0, 1.0))
            if tm_obso > optimal_obso:
                optimal_obso = tm_obso

    return {
        "actual_obso": actual_obso,
        "peak_obso": peak_obso,
        "optimal_obso": optimal_obso,
    }
