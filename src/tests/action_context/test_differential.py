"""Differential: the frozen golden (real IDSSE anchor) vs the legacy oracles (spec C.2).

Operates on the committed ``golden.parquet`` (the validated ``run_work_unit`` output for
IDSSE J03WMX p1, 30 batches) so it runs fast in CI. ``test_e2e.py`` regenerates the golden
from the real pipeline and is the slow pre-commit gate.

Findings encoded in ``oracle_map`` (all root-caused on real data):
  - ~58 geometric tracking-context columns match the legacy oracle within tolerance.
  - DAS matches within a loosened tolerance (per-batch vs whole-match ball-carrier).
  - 4 threat-weighted columns are a KNOWN divergence: AC-1 uses the persisted global xT grid
    (ADR-013) while the oracle fit ExpectedThreat per match — different threat surface.
  - elastic (3) is range-checked only: the legacy elastic oracle has an IDSSE frame-origin
    bug (frame~=25*ts, 0-based) that silly-kicks 3.25.0 fixed, so it is not a valid target.
  - OBSO/PAUSA join on the linked frame_id (pass_id is an event-UUID hash, not action_id).
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd

from tests.action_context.oracle_map import (
    ORACLE_JOIN,
    build_oracle_specs,
    invariant_range,
)

# Determinism: single-threaded BLAS so float reductions are reproducible.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

PROVIDER = "idsse"
# Fraction of joined, both-non-null rows that must fall within tolerance for a non-divergent
# column. < 1.0 tolerates the ~2 boundary actions whose frame-link differs from the oracle.
_MIN_WITHIN_FRAC = 0.90


def test_golden_has_no_boundary_duplication(golden_df: pd.DataFrame) -> None:
    """M13: each (match_id, action_id, period_id) appears exactly once (single-owner de-dup)."""
    dupes = golden_df.groupby(["match_id", "action_id", "period_id"]).size()
    offenders = dupes[dupes > 1]
    assert offenders.empty, f"duplicate action rows: {offenders.to_dict()}"


def _join(golden: pd.DataFrame, oracle: pd.DataFrame, ac_col: str, oracle_col: str, oracle_name: str) -> pd.DataFrame:
    ac_key, or_key = ORACLE_JOIN[oracle_name]
    left = golden[[ac_key, ac_col]].copy()
    left["_jk"] = pd.to_numeric(left[ac_key], errors="coerce").astype("Int64")
    right = oracle[[or_key, oracle_col]].rename(columns={oracle_col: "_oracle"}).copy()
    right["_jk"] = pd.to_numeric(right[or_key], errors="coerce").astype("Int64")
    return left.merge(right[["_jk", "_oracle"]].dropna(subset=["_jk"]), on="_jk", how="inner")


def test_differential_against_legacy_oracles(
    golden_df: pd.DataFrame,
    oracle_tracking_context: pd.DataFrame,
    oracle_pausa: pd.DataFrame,
) -> None:
    oracles = {
        "tracking_context": oracle_tracking_context,
        "pausa": oracle_pausa,
    }
    specs = build_oracle_specs(list(golden_df.columns), list(oracle_tracking_context.columns))
    assert specs, "no oracle specs built"

    failures: list[str] = []
    checked_against_oracle = 0

    for s in specs:
        if s.oracle is None:  # invariant-only range check
            lo, hi = invariant_range(s.ac_col)
            if s.kind == "categorical":
                continue
            v = pd.to_numeric(golden_df[s.ac_col], errors="coerce").dropna()
            if lo is not None and not bool((v >= lo - 1e-9).all()):
                failures.append(f"{s.ac_col}: below {lo} (min={v.min()})")
            if hi is not None and not bool((v <= hi + 1e-9).all()):
                failures.append(f"{s.ac_col}: above {hi} (max={v.max()})")
            continue

        if PROVIDER not in s.providers:
            continue
        if s.known_divergence:
            continue  # xT-grid / DAS / elastic divergences — reported elsewhere, not asserted

        oracle = oracles[s.oracle]
        if s.oracle_col is None or s.oracle_col not in oracle.columns:
            continue
        merged = _join(golden_df, oracle, s.ac_col, s.oracle_col, s.oracle)
        both = merged.dropna(subset=[s.ac_col, "_oracle"])
        if both.empty:
            continue  # no overlap on this slice (e.g. OBSO/PAUSA passes outside the 30-batch window)

        if s.kind in ("float", "int"):
            a = pd.to_numeric(both[s.ac_col], errors="coerce").to_numpy(dtype=float)
            o = pd.to_numeric(both["_oracle"], errors="coerce").to_numpy(dtype=float)
            within = int(np.sum(np.abs(a - o) <= (s.atol + s.rtol * np.abs(o))))
            frac = within / len(a)
            checked_against_oracle += 1
            if frac < _MIN_WITHIN_FRAC:
                failures.append(f"{s.ac_col}: only {within}/{len(a)} within tol (maxd={np.abs(a - o).max():.4g})")
        else:  # bool / categorical exact
            eq = int((both[s.ac_col].astype(str) == both["_oracle"].astype(str)).sum())
            checked_against_oracle += 1
            if eq / len(both) < _MIN_WITHIN_FRAC:
                failures.append(f"{s.ac_col}: only {eq}/{len(both)} equal")

    assert checked_against_oracle >= 40, f"too few columns checked against oracle: {checked_against_oracle}"
    assert not failures, "differential divergences beyond tolerance:\n  " + "\n  ".join(failures)
