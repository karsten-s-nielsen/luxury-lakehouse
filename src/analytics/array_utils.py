"""Shared array utility helpers for analytics modules."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _col_f64(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract a DataFrame column as a float64 numpy array (pyright-safe)."""
    return np.asarray(df[col], dtype=np.float64)
