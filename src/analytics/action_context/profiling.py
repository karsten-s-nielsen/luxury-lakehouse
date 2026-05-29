"""Non-invasive per-function profiler for the action-context pipeline (spec D.1).

Wraps ``run_work_unit`` in ``cProfile`` and returns the top callees by cumulative time, so the
30-min-per-IDSSE-half timeout can be attributed to specific silly-kicks enrichment steps
(L1: the per-batch ``add_*`` calls + the per-group ``pd.DataFrame(actions_records)`` rebuild)
WITHOUT editing the verbatim-moved ``enrich.py`` chain. Pure stdlib; no pyspark.
"""

from __future__ import annotations

import cProfile
import os
import pstats
from dataclasses import dataclass
from io import StringIO
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class StepTiming:
    """One profiled callee: function label, cumulative seconds, call count."""

    label: str
    cumulative_s: float
    ncalls: int


def profile_callable(fn: Callable[[], Any], *, top: int = 30) -> tuple[float, list[StepTiming]]:
    """Run ``fn`` under cProfile; return (wall_seconds, top callees by cumulative time)."""
    profiler = cProfile.Profile()
    profiler.enable()
    profiler.runcall(fn)
    profiler.disable()

    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream).sort_stats("cumulative")
    total = stats.total_tt  # type: ignore[attr-defined]

    timings: list[StepTiming] = []
    for func, (ncalls, _nc, _tt, ct, _callers) in stats.stats.items():  # type: ignore[attr-defined]
        filename, _lineno, name = func
        basename = os.path.basename(filename)
        timings.append(StepTiming(label=f"{name} ({basename})", cumulative_s=ct, ncalls=ncalls))
    timings.sort(key=lambda t: t.cumulative_s, reverse=True)
    return total, timings[:top]
