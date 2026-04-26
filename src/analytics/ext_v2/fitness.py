"""Held-out NLL fitness for the ExT v2 reproduction harness.

Per design spec §5.1::

    NLL = -mean(log P(actual_destination | source))

For Phase 0, ``P`` is the producer's transition matrix. The fitness function
bins each holdout pass's start and end positions to grid cells via
``GridSpec``, looks up ``T[start_zone, end_zone]``, and averages the
negative log probabilities.

Unobserved transitions (``T[s, d] == 0``) are clipped to ``eps`` (default
``1e-10``) before the log to avoid ``-inf``. Phases 1-2 (KDE smoothing,
KNN) eliminate the unobserved-pair problem at its source; Phase 0 inherits
v1's discrete-bin scarcity and uses eps clipping as a defensive measure.
"""

from __future__ import annotations

from typing import Final, Protocol

import numpy as np
import pandas as pd

from analytics.ext_v2.transition import GridSpec, _assign_zones

NLL_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "start_x",
    "start_y",
    "end_x",
    "end_y",
)


class _ProducerProtocol(Protocol):
    """Minimal producer interface fitness consumes."""

    @property
    def transition_matrix(self) -> np.ndarray: ...


def compute_holdout_nll(
    producer: _ProducerProtocol,
    holdout: pd.DataFrame,
    *,
    grid: GridSpec,
    eps: float = 1e-10,
) -> float:
    """Compute mean negative log-likelihood of holdout passes under the producer's transition matrix.

    Args:
        producer: Object exposing a ``transition_matrix`` property of shape
            ``(grid.n_zones, grid.n_zones)``.
        holdout: DataFrame with at least ``start_x, start_y, end_x, end_y``
            columns.
        grid: Pitch-grid binning spec used to map positions to zones.
        eps: Floor for ``T[s, d]`` before the log; protects against
            ``log(0)`` for unobserved (start, end) pairs in the training set.

    Returns:
        Mean ``-log T[start_zone, end_zone]`` across holdout rows. ``NaN``
        if the holdout is empty.
    """
    missing = [col for col in NLL_REQUIRED_COLUMNS if col not in holdout.columns]
    if missing:
        msg = f"holdout missing required columns: {missing}"
        raise ValueError(msg)
    if holdout.empty:
        return float("nan")

    start_x = np.asarray(holdout["start_x"], dtype=np.float64)
    start_y = np.asarray(holdout["start_y"], dtype=np.float64)
    end_x = np.asarray(holdout["end_x"], dtype=np.float64)
    end_y = np.asarray(holdout["end_y"], dtype=np.float64)

    start_zones = _assign_zones(start_x, start_y, grid)
    end_zones = _assign_zones(end_x, end_y, grid)

    transition = producer.transition_matrix
    probs = transition[start_zones, end_zones]
    log_probs = np.log(np.maximum(probs, eps))
    return float(-np.mean(log_probs))


def compute_holdout_nll_per_competition(
    producer: _ProducerProtocol,
    holdout: pd.DataFrame,
    *,
    grid: GridSpec,
    eps: float = 1e-10,
) -> dict[str, float]:
    """Group holdout by ``competition_id`` and compute NLL per group.

    Empty groups (no holdout passes for a competition) are absent from the
    returned dict — callers can take the union with the producer's
    competition list to detect those cases.

    Returns:
        Mapping ``competition_id`` (as ``str``) → mean NLL. Empty if
        ``holdout`` itself is empty.
    """
    if "competition_id" not in holdout.columns:
        msg = "holdout missing required column: competition_id"
        raise ValueError(msg)
    if holdout.empty:
        return {}

    out: dict[str, float] = {}
    for comp_id, group in holdout.groupby("competition_id"):
        nll = compute_holdout_nll(producer, group, grid=grid, eps=eps)
        out[str(comp_id)] = nll
    return out
