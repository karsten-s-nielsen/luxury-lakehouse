"""Haloed frame-window build seam — pure-core tests (ADR-089). No Spark: build_unit_inputs is pure pandas.

Neutrality (the load-bearing property): the union of per-window ``(core+halo)`` builds, halo trimmed,
is value-identical to the whole-unit build — including a ≤max_gap detection gap straddling a window
boundary (non-vacuous; full pipeline, smoothing+velocity ON). Halo sufficiency: H is enough, H-1 is not.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from analytics.action_context import frame_windows as fw
from analytics.action_context.unit_inputs import build_unit_inputs
from analytics.action_context.work_unit import FrameBundle

# Reuse the committed-fixture loaders the tracking-marts oracle uses.
from tests.tracking_marts import _oracle

# T6 convert-feasibility gate: run the neutrality proof per SEAM-TARGET frame-bearing provider —
# build_unit_inputs == convert_to_frames + preprocess, so a passing per-provider neutrality IS the
# empirical proof that THAT provider's convert is window-safe (orientation per-unit-constant, no
# whole-sequence op). METRICA is EXCLUDED (owner 2026-10-07: 3 ancient matches, no value) — AND its
# rebase is window-unsafe (unit_inputs.py:19 period_min from the passed set) AND it does not OOM at
# whole-unit grain (fit-profile: raw 77 MB + built 452 MB < 1 GB), so it keeps the whole-unit build and
# is never a seam target. gradientsports' committed fixture is actions-only (no frames) -> the GS seam is
# covered by the Docker dispatch + live (ADR-089). _FRAME_COL mirrors ingestion._frame_col (GS -> frame_num).
_PROVIDERS = ("idsse", "skillcorner")
_FRAME_COL_BY_PROVIDER = {"idsse": "frame", "metrica": "frame", "skillcorner": "frame", "gradientsports": "frame_num"}
_TARGET = 150  # small window so a ~hundreds-of-frames fixture spans several windows (+ halo boundaries).


def _frame_col(provider: str) -> str:
    return _FRAME_COL_BY_PROVIDER[provider]


def _ctx(provider: str):
    wu = _oracle.work_unit(provider)
    grid, xt_l, xt_w = _oracle.ParquetXtSource(_oracle.FIXTURE_ROOT).grid()
    meta = _oracle.metadata(provider)
    raw = _oracle.ParquetFrameSource(_oracle.FIXTURE_ROOT).frames(wu).frames.copy()

    def build_fn(frames_subset: pd.DataFrame) -> pd.DataFrame:
        return build_unit_inputs(
            wu,
            frame_bundle=FrameBundle(tier="tracking", frames=frames_subset),
            actions_df=_oracle.ParquetActionsSource(_oracle.FIXTURE_ROOT).actions(wu),
            meta=meta,
            xt_grid_data=grid,
            xt_l=xt_l,
            xt_w=xt_w,
        ).frames

    return wu, raw, build_fn


def _sort(df: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ("frame_id", "period_id", "player_id", "is_ball") if c in df.columns]
    return df.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _windowed_union(provider: str, raw: pd.DataFrame, build_fn, *, target: int = _TARGET) -> pd.DataFrame:
    fcol = _frame_col(provider)
    assigned = fw.assign_window_ids(raw, provider, target_window_frames=target, frame_col=fcol)
    cores = [fw.build_windowed(grp, build_fn, frame_col=fcol) for _, grp in assigned.groupby(fw._WINDOW_COL, sort=True)]
    return pd.concat(cores, ignore_index=True)


# ── T1: halo ───────────────────────────────────────────────────────────────
def test_halo_frames_equals_composed_reach_formula() -> None:
    for p, hz in (("gradientsports", 10.0), ("idsse", 25.0), ("skillcorner", 10.0), ("metrica", 25.0)):
        cfg = fw._resolve_sk_cfg(p)
        wf = max(round(cfg.sg_window_seconds * hz) | 1, cfg.sg_poly_order + 2)
        if wf % 2 == 0:
            wf += 1
        expected = (wf - 1) + math.ceil(cfg.max_gap_seconds * hz)
        assert fw.halo_frames(p, hz) == expected >= 1


def test_halo_sentinel_fires_on_sk_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    drifted = fw._SkCfg(99.0, 3, 0.48)  # changed sg_window_seconds
    monkeypatch.setattr(fw, "_resolve_sk_cfg", lambda p: drifted if p == "idsse" else fw._SkCfg(0.333, 3, 0.5))
    with pytest.raises(AssertionError, match="PreprocessConfig drift"):
        fw._assert_sk_config_pinned()


# ── T2 + T6: neutrality per FRAME-BEARING provider (convert window-safety gate) ──────────────────────
@pytest.mark.parametrize("provider", _PROVIDERS)
def test_windowed_build_equals_whole_unit_gapfree(provider: str) -> None:
    _wu, raw, build_fn = _ctx(provider)
    assert raw[_frame_col(provider)].nunique() > _TARGET, f"{provider} fixture must span >1 window (else vacuous)"
    assert_frame_equal(
        _sort(_windowed_union(provider, raw, build_fn)), _sort(build_fn(raw)), check_dtype=False, check_like=False
    )


def _raw_with_boundary_gap(provider: str, raw: pd.DataFrame) -> pd.DataFrame:
    """One player's x/y NaN'd over a ≤max_gap run straddling the window-1 boundary (engages the interp
    reach, so the FULL H = window_frames-1 + max_gap is binding — not just the smooth∘velocity reach)."""
    fcol = _frame_col(provider)
    frames_sorted = np.sort(raw[fcol].unique())
    cfg = fw._resolve_sk_cfg(provider)
    gap = max(1, math.floor(cfg.max_gap_seconds * 25.0) - 1)  # strictly < max_gap frames
    gap_frames = set(frames_sorted[_TARGET - gap // 2 : _TARGET + (gap - gap // 2)])
    one_player = raw["player_id"].dropna().iloc[0]
    hole = raw[fcol].isin(gap_frames) & (raw["player_id"] == one_player)
    assert hole.any(), "planted gap hit no rows"
    out = raw.copy()
    for c in ("x", "y"):
        if c in out.columns:
            out.loc[hole, c] = np.nan
    return out


@pytest.mark.parametrize("provider", _PROVIDERS)
def test_windowed_build_equals_whole_unit_with_bounded_gap(provider: str) -> None:
    """Non-vacuous: a ≤max_gap NaN detection gap straddling a window boundary — the halo reconstructs the
    same velocity windowed as whole-unit (full pipeline: interpolate -> smooth -> velocity), per provider."""
    _wu, raw, build_fn = _ctx(provider)
    raw_gap = _raw_with_boundary_gap(provider, raw)
    assert_frame_equal(
        _sort(_windowed_union(provider, raw_gap, build_fn)),
        _sort(build_fn(raw_gap)),
        check_dtype=False,
        check_like=False,
    )


def test_halo_is_load_bearing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-vacuity (HFW-SPEC-03): the neutrality tests do NOT pass because windowing is a no-op — with
    NO halo (halo=0) the per-window builds truncate the savgol stencil at every boundary, so the union
    DIFFERS from the whole-unit build. (Full H restores byte-identity — the tests above.) Proves the halo
    is genuinely required; does not assert H is minimal (H is sized for the worst-case gap)."""
    _wu, raw, build_fn = _ctx("idsse")
    whole = _sort(build_fn(raw))
    monkeypatch.setattr(fw, "halo_frames", lambda p, hz=25.0: 0)
    no_halo = _sort(_windowed_union("idsse", raw, build_fn))
    with pytest.raises(AssertionError):
        assert_frame_equal(no_halo, whole, check_dtype=False, check_like=False)
