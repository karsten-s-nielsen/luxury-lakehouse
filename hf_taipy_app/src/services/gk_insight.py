"""Pure GK-insight domain functions (GK insight-views redesign, spec §0 E).

No Taipy / DB / pandas-required imports — stdlib + numpy only. Mirrors the
services/ghost_grid.py port discipline: pure, the caller supplies all data. Every
function returns a frozen dataclass or a primitive and is unit-tested in isolation.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np

Tercile = Literal["low", "mid", "high"]
CommandPos = Literal["upper", "mid", "lower"]
_TERCILE_RANK = {"low": 0, "mid": 1, "high": 2}


# ---------------------------------------------------------------------------
# Reference band (IQR + min-cohort gate)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReferenceBand:
    median: float
    q1: float
    q3: float
    n: int


def reference_band(values: Sequence[float], *, min_cohort: int = 8) -> ReferenceBand | None:
    """IQR band over a provider cohort. Returns None when fewer than ``min_cohort``
    finite members remain (caller renders the value without a band)."""
    arr = np.asarray(list(values), dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < min_cohort:
        return None
    q1, med, q3 = (float(x) for x in np.percentile(arr, [25, 50, 75]))
    return ReferenceBand(median=med, q1=q1, q3=q3, n=int(arr.size))


# ---------------------------------------------------------------------------
# Cohort values: one value per GK, sub-floor excluded
# ---------------------------------------------------------------------------
def cohort_values(rows, *, floor: float) -> list[float]:
    """rows: iterable of (gk_id, value, weight). Volume-weight each GK's rows to ONE value,
    drop GKs whose total weight < ``floor``, return the per-GK values (unit == displayed keeper)."""
    num: dict = defaultdict(float)
    den: dict = defaultdict(float)
    for gk, v, w in rows:
        if v is None or w is None:
            continue
        v, w = float(v), float(w)
        if not (np.isfinite(v) and np.isfinite(w) and w > 0):
            continue
        num[gk] += v * w
        den[gk] += w
    return [num[g] / den[g] for g in den if den[g] >= floor]


# ---------------------------------------------------------------------------
# Tercile position (within-cohort 33/67)
# ---------------------------------------------------------------------------
def tercile_position(value: float, cohort: Sequence[float], *, lower_is_better: bool = False) -> Tercile:
    """Classify ``value`` into low/mid/high by the cohort's 33rd/67th percentiles.
    ``lower_is_better`` flips the labels (e.g. closing time). Degenerate cohorts -> 'mid'."""
    arr = np.asarray(list(cohort), dtype=float)
    arr = arr[np.isfinite(arr)]
    if value is None or not np.isfinite(value) or arr.size < 3:
        return "mid"
    p33, p67 = (float(x) for x in np.percentile(arr, [33.33, 66.67]))
    if p33 == p67:
        return "mid"
    raw: Tercile = "low" if value < p33 else ("high" if value > p67 else "mid")
    flip: dict[Tercile, Tercile] = {"low": "high", "high": "low", "mid": "mid"}
    return flip[raw] if lower_is_better else raw


# ---------------------------------------------------------------------------
# Sweeping-command composite
# ---------------------------------------------------------------------------
def sweeping_command(*, reach: Tercile, pc: Tercile, closing: Tercile) -> CommandPos:
    """Composite of the three sweeper terciles (each already oriented better=high).
    Mean rank -> upper / mid / lower."""
    mean = (_TERCILE_RANK[reach] + _TERCILE_RANK[pc] + _TERCILE_RANK[closing]) / 3.0
    if mean >= 1.5:
        return "upper"
    if mean <= 0.5:
        return "lower"
    return "mid"


# ---------------------------------------------------------------------------
# Verdict templater
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Verdict:
    phrase: str
    detail: str


def distribution_quadrant(
    *, share_adds: float, progress_m: float, n: int, share_median: float | None, progress_median: float | None
) -> Verdict:
    """Threat-by-style quadrant read for the distribution profile (ADR-060 redesign). Threat = share of
    distributions that add xT-GK v2 (>0; the headline that actually varies — xT-GK v2 is signed and
    mostly negative); style = forward progression (long-direct vs short-safe). Descriptive cohort
    positioning, never a rank."""
    if n < 20:
        return Verdict("Indicative only — small sample", "too few distributions to characterise")
    if share_median is None or progress_median is None:
        return Verdict("Cohort too small", "no peer band to position against")
    high_threat = share_adds >= share_median
    direct = progress_m >= progress_median
    if high_threat and direct:
        return Verdict("Proactive distributor", "adds threat at volume, playing forward")
    if high_threat:
        return Verdict("Proactive recycler", "adds threat while keeping distribution short")
    if direct:
        return Verdict("Risk without reward", "plays direct but rarely adds threat")
    return Verdict("Secure recycler", "safe and short; rarely adds threat")


def defensive_verdict(*, command: CommandPos, n_defended: int) -> Verdict:
    """Spec §11a defensive — descriptive sweeping-command read. Command-only (NOT line-coupled): the
    per-keeper defensive-line height barely varies across a tournament (cohort terciles ~2 m apart),
    so the old command-by-line "underused sweeper / wrong system" verdict asserted a mismatch on noise.
    Line height is now shown descriptively (avg distance from own goal), not bucketed into deep/high."""
    if n_defended < 30:
        return Verdict("Indicative only — small sample", "too few defended actions")
    if command == "upper":
        return Verdict("Commands his box", "upper-cohort sweeping command (reach / pitch-control / closing)")
    if command == "lower":
        return Verdict("Line-keeper profile", "lower-cohort sweeping command — stays close to his line")
    return Verdict("Typical box-keeper", "mid-cohort sweeping command")
