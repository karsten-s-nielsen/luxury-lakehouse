"""The pitch-control compute backend (perf spec §7.1, lever N).

silly-kicks runs its Spearman TTI and influence kernels compiled with numba when numba imports, and falls back to a
numpy broadcast path when it does not (`silly_kicks/tracking/pitch_control/_spearman.py:23-28`). The fallback is
silent, so the lakehouse checks the backend itself: a test fails when numba is not active in the run environment, and
the run logs the backend once per compute phase.

This module is the ONE place that reads the private flag `_spearman._HAS_NUMBA` (pinned to silly-kicks 4.139.1). A
public silly-kicks accessor would replace it (handoff `D:\\Development\\_handoffs\\2026-11-06-sk-tracking-batch-gaps-
handoff.md`, item 1).

The numpy and numba paths use a different floating-point operation order, so they are not bit-identical: switching
backend is a *backend change* (spec §4), gated by `BACKEND_TOLERANCE` on `control_fraction`.
"""

from __future__ import annotations

from typing import Final

from silly_kicks.tracking.pitch_control import (  # pyright: ignore[reportMissingTypeStubs]
    _spearman,  # pyright: ignore[reportPrivateUsage] - the backend flag; pinned silly-kicks 4.139.1
)

__all__ = ["BACKEND_TOLERANCE", "numba_active"]

#: The largest accepted absolute difference of `control_fraction` between the numba and numpy backends (spec §7.1).
BACKEND_TOLERANCE: Final[float] = 1e-9


def numba_active() -> bool:
    """True when silly-kicks runs its pitch-control kernels compiled with numba (not the numpy fallback)."""
    return bool(_spearman._HAS_NUMBA)  # pyright: ignore[reportPrivateUsage]
