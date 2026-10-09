"""AC preflight two-probe canary fold (ADR-087 amendment): scope-aware + Probe-C-only (no Probe M).

The AC drain is NOT two-stage (direct ``mapInPandas``), so it has no window-build OOM class and runs
NO Probe M — the AC preflight passes ``window_counts=None`` to ``select_canary_probes``. This locks that
the AC canary is bounded (smallest unit per in-scope provider), scope-aware, and never selects Probe M.
"""

from __future__ import annotations

import logging
import types
from typing import Any

import pytest

from analytics.action_context.work_unit import WorkUnit


def test_parse_providers_scope_inclusion_and_validation() -> None:
    from ingestion.action_context import _parse_providers_scope

    assert _parse_providers_scope("") is None  # empty = all
    assert _parse_providers_scope(None) is None
    assert _parse_providers_scope("idsse,skillcorner") == ("idsse", "skillcorner")
    with pytest.raises(SystemExit, match="Unknown --providers"):
        _parse_providers_scope("idsse,nope")


def test_ac_unit_size_grain() -> None:
    """IDSSE units size by (match, period); match-grain providers (period None) by the match roll-up."""
    from ingestion.action_context import _ac_unit_size

    by_unit = {("idsse", "J", 1): 10, ("idsse", "J", 2): 7}
    by_match = {("idsse", "J"): 17, ("skillcorner", "S"): 40}
    assert _ac_unit_size(by_unit, by_match, WorkUnit(provider="idsse", match_id="J", period=1)) == 10
    assert _ac_unit_size(by_unit, by_match, WorkUnit(provider="skillcorner", match_id="S", period=None)) == 40
    assert _ac_unit_size(by_unit, by_match, WorkUnit(provider="idsse", match_id="MISSING", period=1)) is None


def test_ac_preflight_probe_c_only_and_scope(monkeypatch) -> None:
    """main_preflight builds Probe-C-only probes (window_counts=None → no Probe M) on the smallest unit
    per in-scope provider, scope-filtered, before the enqueue."""
    import ingestion.action_context as ac
    import ingestion.bootstrap as boot
    import ingestion.drain_adapters as da

    class _Args:
        catalog = "cat"
        schema = "bronze"
        provider = None
        max_units = None
        providers = "idsse,skillcorner"
        run_id = "R1"
        ghost_gk_backend = None

    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: _Args())
    monkeypatch.setattr(ac, "configure_logging", lambda name: logging.getLogger("t"))
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(boot, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(ac, "_force_full_rematerialize_on_grid_change", lambda *a, **k: None)
    monkeypatch.setattr(ac, "_resolve_backend_or_exit", lambda *a, **k: "fft-cic")
    monkeypatch.setattr(ac, "timed_check", lambda *a, **k: types.SimpleNamespace(count=2))

    class _Sink:
        def __init__(self, *a, **k) -> None: ...

        def ensure_tables(self) -> None: ...

    monkeypatch.setattr(da, "DeltaUnitEventSink", _Sink)
    monkeypatch.setattr(da, "SparkGameProcessor", lambda *a, **k: object())

    units = [
        WorkUnit(provider="idsse", match_id="J", period=1),
        WorkUnit(provider="skillcorner", match_id="S", period=None),
    ]
    monkeypatch.setattr(ac._ActionContextGuard, "discover_units", lambda self, spark, catalog, schema: units)
    monkeypatch.setattr(
        ac,
        "_compute_ac_unit_sizes",
        lambda spark, catalog, units: ({("idsse", "J", 1): 5}, {("idsse", "J"): 5, ("skillcorner", "S"): 9}),
    )

    captured: dict[str, Any] = {}
    import analytics.action_context.canary as canary_mod

    orig_select = canary_mod.select_canary_probes  # bind BEFORE patching (no recursion)

    def _capture_select(sized_units, window_counts=None):
        captured["window_counts"] = window_counts
        captured["providers"] = sorted({u.provider for u in sized_units})
        captured["sizes"] = {u.provider: u.n_frames for u in sized_units}
        return orig_select(sized_units, window_counts)

    def _stop_run_canary(processor, probes, logger) -> None:
        captured["memory_window"] = probes.memory_window
        captured["correctness"] = [(u.provider, u.match_id) for u in probes.correctness_units]
        raise RuntimeError("STOP-after-canary")  # short-circuit before the enqueue

    monkeypatch.setattr(canary_mod, "select_canary_probes", _capture_select)
    monkeypatch.setattr(canary_mod, "run_canary", _stop_run_canary)

    with pytest.raises(RuntimeError, match="STOP-after-canary"):
        ac.main_preflight()

    assert captured["window_counts"] is None  # AC passes NO window counts → no Probe M
    assert captured["memory_window"] is None  # ⇒ Probe M never selected
    assert captured["providers"] == ["idsse", "skillcorner"]
    assert captured["sizes"] == {"idsse": 5, "skillcorner": 9}  # action-count sizes attached
    assert set(captured["correctness"]) == {("idsse", "J"), ("skillcorner", "S")}  # smallest per provider
