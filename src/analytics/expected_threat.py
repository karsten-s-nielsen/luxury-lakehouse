"""Expected Threat (xT) grid computation via Markov chain value iteration.

Replaces the static 12x8 Karun Singh seed with data-driven transition
probabilities computed from SPADL pass/shot events.

Reference: Karun Singh (2018) "Introducing Expected Threat (xT)"
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


@dataclass(frozen=True)
class ExpectedThreatParams:
    """Configuration for xT grid computation."""

    n_zones_x: int = 12
    n_zones_y: int = 8
    pitch_length: float = 105.0  # SPADL coordinates
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-5


# SPADL action types
_MOVE_TYPES = frozenset(
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
_SHOT_TYPES = frozenset({"shot", "shot_penalty", "shot_freekick"})


def _assign_zones(
    x: np.ndarray,
    y: np.ndarray,
    params: ExpectedThreatParams,
) -> np.ndarray:
    """Map (x, y) coordinates to flat zone indices."""
    zone_x = np.clip(
        (x / params.pitch_length * params.n_zones_x).astype(int),
        0,
        params.n_zones_x - 1,
    )
    zone_y = np.clip(
        (y / params.pitch_width * params.n_zones_y).astype(int),
        0,
        params.n_zones_y - 1,
    )
    return zone_x * params.n_zones_y + zone_y


def _build_transition_matrix(
    start_zones: np.ndarray,
    end_zones: np.ndarray,
    n_zones: int,
) -> np.ndarray:
    """Build row-normalized transition matrix from zone-to-zone moves."""
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)

    row_sums = transition.sum(axis=1, keepdims=True)
    row_sums = np.maximum(row_sums, 1.0)
    return transition / row_sums


def _value_iteration_numpy(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iterations: int,
    tolerance: float,
) -> tuple[np.ndarray, int]:
    """Pure NumPy value iteration.

    Returns (xT vector, iterations used).
    """
    xt = np.zeros_like(shot_prob)
    for i in range(max_iterations):
        xt_new = shot_prob * goal_prob + move_prob * (transition @ xt)
        delta = float(np.max(np.abs(xt_new - xt)))
        xt = xt_new
        if delta < tolerance:
            return xt, i + 1
    return xt, max_iterations


if _USE_JAX:

    def _value_iteration_jax(
        shot_prob: np.ndarray,
        goal_prob: np.ndarray,
        move_prob: np.ndarray,
        transition: np.ndarray,
        max_iterations: int,
        tolerance: float,
    ) -> tuple[np.ndarray, int]:
        """JAX-accelerated value iteration for dense grids."""
        s = jnp.asarray(shot_prob)
        g = jnp.asarray(goal_prob)
        m = jnp.asarray(move_prob)
        t_mat = jnp.asarray(transition)

        @jax.jit
        def _step(xt: jax.Array) -> jax.Array:
            return s * g + m * (t_mat @ xt)

        xt = jnp.zeros_like(s)
        for i in range(max_iterations):
            xt_new = _step(xt)
            delta = float(jnp.max(jnp.abs(xt_new - xt)))
            xt = xt_new
            if delta < tolerance:
                return np.asarray(xt), i + 1
        return np.asarray(xt), max_iterations


def compute_expected_threat_grid(
    actions_df: pd.DataFrame,
    params: ExpectedThreatParams | None = None,
) -> np.ndarray:
    """Compute an xT grid from SPADL action data via Markov chain value iteration.

    Args:
        actions_df: SPADL actions with columns: type_name, result_name,
            start_x, start_y, end_x, end_y. Coordinates in SPADL 105x68m.
        params: Grid and convergence parameters. Defaults if None.

    Returns:
        np.ndarray of shape (n_zones_x, n_zones_y) with xT values.
        Grid orientation: [0, 0] = own-goal bottom-left, [11, 7] = opponent
        goal top-right. Matches the dbt seed CSV layout.
    """
    if params is None:
        params = ExpectedThreatParams()

    n_zones = params.n_zones_x * params.n_zones_y

    # Classify events
    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_shot = np.array([t in _SHOT_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    # Assign zones
    start_x = np.asarray(actions_df["start_x"], dtype=np.float64)
    start_y = np.asarray(actions_df["start_y"], dtype=np.float64)
    end_x = np.asarray(actions_df["end_x"], dtype=np.float64)
    end_y = np.asarray(actions_df["end_y"], dtype=np.float64)

    start_zones = _assign_zones(start_x, start_y, params)
    end_zones = _assign_zones(end_x, end_y, params)

    # Per-zone counts
    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(start_zones[is_shot & is_success], minlength=n_zones).astype(np.float64)

    # Successful moves — failed moves lose possession (xT=0)
    successful_moves = is_move & is_success
    succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

    # Probabilities per zone
    safe_total = np.maximum(total_per_zone, 1.0)
    shot_prob = shots_per_zone / safe_total
    goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
    # Use successful move probability — failed moves contribute xT=0 implicitly
    move_prob = succ_moves_per_zone / safe_total

    # Transition matrix (successful moves only)
    transition = _build_transition_matrix(
        start_zones[successful_moves],
        end_zones[successful_moves],
        n_zones,
    )

    # Value iteration
    use_jax = _USE_JAX and n_zones > 200  # JAX overhead not worth it for small grids
    if use_jax:
        xt_flat, _iters = _value_iteration_jax(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            params.max_iterations,
            params.tolerance,
        )
    else:
        xt_flat, _iters = _value_iteration_numpy(
            shot_prob,
            goal_prob,
            move_prob,
            transition,
            params.max_iterations,
            params.tolerance,
        )

    return xt_flat.reshape(params.n_zones_x, params.n_zones_y)


def grid_to_dataframe(
    grid: np.ndarray,
    competition_id: str | None = None,
) -> pd.DataFrame:
    """Convert an xT grid array to a DataFrame matching the dbt seed schema.

    Args:
        grid: (n_zones_x, n_zones_y) array of xT values.
        competition_id: Optional competition identifier.

    Returns:
        DataFrame with columns: zone_x, zone_y, xt_value
        (and optionally competition_id).
    """
    n_x, n_y = grid.shape
    rows: list[dict[str, object]] = []
    for zx in range(n_x):
        for zy in range(n_y):
            row: dict[str, object] = {
                "zone_x": zx,
                "zone_y": zy,
                "xt_value": round(float(grid[zx, zy]), 5),
            }
            if competition_id is not None:
                row["competition_id"] = competition_id
            rows.append(row)
    return pd.DataFrame(rows)


def validate_xt_grid(grid: np.ndarray, params: ExpectedThreatParams | None = None) -> None:
    """Validate computed xT grid meets data quality requirements."""
    if params is None:
        params = ExpectedThreatParams()
    expected_shape = (params.n_zones_x, params.n_zones_y)
    if grid.shape != expected_shape:
        msg = f"Grid shape {grid.shape} != expected {expected_shape}"
        raise ValueError(msg)
    if grid.min() < 0.001 or grid.max() > 0.50:
        msg = f"Grid values out of expected range [0.001, 0.50]: min={grid.min():.4f}, max={grid.max():.4f}"
        raise ValueError(msg)
    row_means = grid.mean(axis=1)
    if not np.all(np.diff(row_means) >= -0.01):
        msg = "Grid row means not approximately monotonically increasing left-to-right"
        raise ValueError(msg)
