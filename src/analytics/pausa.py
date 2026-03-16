"""PAUSA scoring — Passing Ability Under Spatiotemporal Awareness.

Pure-Python (pandas/numpy) scoring functions for the PAUSA metric.
No Spark dependency — designed to be called from ``applyInPandas`` UDFs
on executors or directly in unit tests.

PAUSA decomposes pass quality into two orthogonal components:
- **Temporal judgment**: Was the pass released at the peak OBSO moment?
  ``temporal = actual_obso / peak_obso``
- **Spatial selection**: Was the target the best available receiver?
  ``spatial = actual_obso / optimal_obso``
- **PAUSA composite**: ``pausa = temporal * spatial``

All values are clamped to [0, 1]. Division by zero (peak=0 or optimal=0)
yields 0 — if there was no scoring opportunity, the pass cannot be evaluated.

Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying
Optimal Pass Timing Beyond Speed." MIT Sloan 2026.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_pausa_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Compute PAUSA temporal judgment, spatial selection, and composite score.

    Args:
        df: DataFrame with columns ``actual_obso``, ``peak_obso``,
            ``optimal_obso``. Additional columns are passed through unchanged.

    Returns:
        Input DataFrame augmented with ``temporal_judgment``,
        ``spatial_selection``, and ``pausa_score`` columns.
    """
    logger.debug("compute_pausa_scores: input shape %s", df.shape)
    if df.empty:
        logger.debug("compute_pausa_scores: early exit — empty DataFrame")
        result = df.copy()
        result["temporal_judgment"] = pd.Series(dtype=np.float64)
        result["spatial_selection"] = pd.Series(dtype=np.float64)
        result["pausa_score"] = pd.Series(dtype=np.float64)
        return result

    actual = df["actual_obso"].to_numpy(dtype=np.float64)
    peak = df["peak_obso"].to_numpy(dtype=np.float64)
    optimal = df["optimal_obso"].to_numpy(dtype=np.float64)

    # Safe divide: where denominator is zero, result is 0.0.
    # Use np.divide with out= and where= to avoid RuntimeWarning on zero denominators.
    temporal = np.zeros_like(actual, dtype=np.float64)
    np.divide(actual, peak, out=temporal, where=peak > 0)

    spatial = np.zeros_like(actual, dtype=np.float64)
    np.divide(actual, optimal, out=spatial, where=optimal > 0)

    # Clamp to [0, 1] — handles numerical noise where actual slightly exceeds peak/optimal
    temporal = np.clip(temporal, 0.0, 1.0)
    spatial = np.clip(spatial, 0.0, 1.0)

    pausa = temporal * spatial

    result = df.copy()
    result["temporal_judgment"] = temporal
    result["spatial_selection"] = spatial
    result["pausa_score"] = pausa

    return result
