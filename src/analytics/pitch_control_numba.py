"""Numba JIT evaluation kernels for pitch control (D24).

Mirrors _tti_numpy and _influence_numpy from pitch_control.py for
benchmarking. This file is an evaluation artifact — it may be removed
if Numba does not demonstrate sufficient speedup.
"""

from __future__ import annotations

import math

import numba  # type: ignore[import-untyped]
import numpy as np


@numba.njit(cache=True)
def tti_numba(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    reaction_time: float,
    max_acceleration: float,
) -> np.ndarray:
    """Compute time-to-intercept for all players to all targets.

    Parameters
    ----------
    player_pos_m : (n_players, 2) positions in metres.
    player_vel_m : (n_players, 2) velocities in m/s.
    target_m : (n_targets, 2) target positions in metres.
    reaction_time : Reaction time in seconds.
    max_acceleration : Maximum acceleration in m/s².

    Returns
    -------
    (n_players, n_targets) TTI array in seconds.
    """
    n_players = player_pos_m.shape[0]
    n_targets = target_m.shape[0]
    result = np.empty((n_players, n_targets), dtype=np.float64)

    for i in range(n_players):
        for j in range(n_targets):
            dx = target_m[j, 0] - player_pos_m[i, 0]
            dy = target_m[j, 1] - player_pos_m[i, 1]
            dist = math.sqrt(dx * dx + dy * dy)

            if dist < 1e-10:
                result[i, j] = reaction_time
                continue

            # Project velocity onto displacement direction
            v_proj = (player_vel_m[i, 0] * dx + player_vel_m[i, 1] * dy) / dist

            discriminant = v_proj * v_proj + 2.0 * max_acceleration * dist
            if discriminant < 0:
                result[i, j] = 1e6  # unreachable
            else:
                result[i, j] = reaction_time + (-v_proj + math.sqrt(discriminant)) / max_acceleration

    return result


@numba.njit(cache=True)
def influence_numba(
    team_tti: np.ndarray,
    opponent_min_tti: np.ndarray,
    sigma: float,
) -> np.ndarray:
    """Compute summed team influence via logistic sigmoid over TTI difference.

    Mirrors ``_influence_numpy`` — returns (n_targets,) summed influence,
    not per-player influence.

    Parameters
    ----------
    team_tti : (n_players, n_targets) TTI array.
    opponent_min_tti : (n_targets,) minimum opponent TTI per target.
    sigma : Sigmoid width parameter.

    Returns
    -------
    (n_targets,) array of summed team influence values.
    """
    k = math.pi / math.sqrt(3.0) / sigma
    n_players = team_tti.shape[0]
    n_targets = team_tti.shape[1]
    result = np.zeros(n_targets, dtype=np.float64)

    for i in range(n_players):
        for j in range(n_targets):
            exponent = -k * (opponent_min_tti[j] - team_tti[i, j])
            if exponent > 50.0:
                exponent = 50.0
            elif exponent < -50.0:
                exponent = -50.0
            result[j] += 1.0 / (1.0 + math.exp(exponent))

    return result
