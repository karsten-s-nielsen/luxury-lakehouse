"""Offline branch coverage for ``TrackingMartsProcessor.build_fit_probe`` (ADR-087 Probe M).

Spark/bronze-free: ``_prepare_stage1`` + ``windowed_build_sdf`` are patched so only the probe's control
flow is exercised — the no-inputs guard, the non-vacuous empty-build guard, and the success return."""

from __future__ import annotations

import logging

import pytest

import ingestion.tracking_marts_processor as tmp
from analytics.action_context.work_unit import WorkUnit

_U = WorkUnit(provider="idsse", match_id="J", period=1)


def _proc() -> tmp.TrackingMartsProcessor:
    # Bypass __init__ (which loads the xt grid from Spark); build_fit_probe only needs _spark/_logger.
    proc = tmp.TrackingMartsProcessor.__new__(tmp.TrackingMartsProcessor)
    proc._spark = object()  # type: ignore[attr-defined]
    proc._logger = logging.getLogger("t")  # type: ignore[attr-defined]
    return proc


class _Built:
    def __init__(self, n: int) -> None:
        self._n = n

    def count(self) -> int:
        return self._n


def test_build_fit_probe_raises_when_no_build_inputs(monkeypatch) -> None:
    proc = _proc()
    monkeypatch.setattr(proc, "_prepare_stage1", lambda unit: None)
    with pytest.raises(RuntimeError, match="no build inputs"):
        proc.build_fit_probe(_U, 0)


def _prep() -> object:
    return tmp._Stage1Prep(
        period=1,
        raw_sdf=object(),
        build_udf=object(),
        resolved_actions=None,
        xg_preds=None,
        meta=None,
        comp=None,
        season=None,
        access_tier=None,
    )


def test_build_fit_probe_raises_on_empty_build(monkeypatch) -> None:
    proc = _proc()
    monkeypatch.setattr(proc, "_prepare_stage1", lambda unit: _prep())
    monkeypatch.setattr("analytics.action_context.tracking_frame_spill.built_frame_struct_type", lambda p: object())
    monkeypatch.setattr("ingestion.frame_window_dispatch.windowed_build_sdf", lambda *a, **k: _Built(0))
    with pytest.raises(RuntimeError, match="built 0 rows"):
        proc.build_fit_probe(_U, 0)


def test_build_fit_probe_ok_returns_zero_and_builds_only_the_window(monkeypatch) -> None:
    proc = _proc()
    monkeypatch.setattr(proc, "_prepare_stage1", lambda unit: _prep())
    monkeypatch.setattr("analytics.action_context.tracking_frame_spill.built_frame_struct_type", lambda p: object())
    seen: dict[str, object] = {}

    def _wbs(spark, raw_sdf, provider, build_fn, schema, *, target_window_frames, only_window_id=None):
        seen["only_window_id"] = only_window_id
        return _Built(42)

    monkeypatch.setattr("ingestion.frame_window_dispatch.windowed_build_sdf", _wbs)
    assert proc.build_fit_probe(_U, 7) == 0
    assert seen["only_window_id"] == 7  # the probe bounds the build to the one selected window
