"""Value-iteration primitive for the ExT v2 reproduction harness.

Implements undiscounted fixed-point Bellman value iteration over a transition
matrix. Singh-2018's recursion:

    xT(s) = P_shot(s) * P_goal(s) + P_move(s) * sum_d T(s, d) * xT(d)

iterated until the maximum per-cell change is below ``tolerance``.

This is an independent reimplementation of v1's
``analytics.expected_threat._value_iteration_numpy``. The Phase 0 stop
condition (per design spec §6) requires the v2 producer to match the v1
producer to numerical tolerance on identical inputs; that match is enforced
by ``src/tests/test_ext_v2/test_value_iteration.py``.

Phases 1-4 plug KDE-smoothed and KNN-derived transition matrices into this
same primitive.
"""

from __future__ import annotations

import numpy as np


def iterate(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    *,
    max_iterations: int = 100,
    tolerance: float = 1e-5,
) -> tuple[np.ndarray, int]:
    """Run undiscounted Bellman value iteration to fixed point.

    Args:
        shot_prob: Per-zone P(action is shot | start in zone), shape (n_zones,).
        goal_prob: Per-zone P(goal | shot from zone), shape (n_zones,).
        move_prob: Per-zone P(action is successful move | start in zone),
            shape (n_zones,).
        transition: Row-stochastic transition matrix, shape (n_zones, n_zones).
            ``transition[s, d]`` is P(end in d | move was made from s).
        max_iterations: Hard upper bound on iterations. Returns the partial
            value if not converged within this budget.
        tolerance: Convergence threshold on max-cell-change between iterations.

    Returns:
        ``(xt, iterations_used)`` — ``xt`` shape (n_zones,); ``iterations_used``
        is the count of completed iterations (≤ ``max_iterations``).
    """
    xt = np.zeros_like(shot_prob)
    for i in range(max_iterations):
        xt_new = shot_prob * goal_prob + move_prob * (transition @ xt)
        delta = float(np.max(np.abs(xt_new - xt)))
        xt = xt_new
        if delta < tolerance:
            return xt, i + 1
    return xt, max_iterations
