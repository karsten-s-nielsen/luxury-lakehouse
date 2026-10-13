"""Unit tests for ``ingestion.tracking_marts_processor.TrackingMartsProcessor`` (rev3 two-stage, ADR-037).

``process(unit)`` is orchestration over the executor-distributed two-stage dispatch (build-once → spill →
six per-mart scoring passes). These tests fake every Spark/pyspark seam — the ``__init__`` loads (xT grid,
comp/season, struct factories, completion model), the driver reads (``read_unit_actions`` /
``read_raw_trk_sdf`` / ``resolve_unit_meta`` / ``_read_xg_preds`` / ``_resolve_enrichment_identity``), and the
dispatch functions (``run_stage1_build_and_spill`` / ``build_stage2_mart_sdf`` / ``write_delta_table`` /
``cleanup_spill``) — so NO Spark is touched, and assert the orchestration contract:

* the correct per-mart specs are built (six with gkdv enabled, five without);
* each mart is written with the identical per-unit ``replaceWhere``; the returned count sums the writes;
* a per-mart failure is attributed and re-raised as a combined unit failure (drain rolls it forward), while
  the OTHER marts still run (per-mart isolation);
* the spill is cleaned up in a ``finally`` even when a mart fails;
* ``dry_run`` materializes (``.count()``) but writes nothing and returns 0; an empty unit returns 0.

The REAL two-stage Spark dispatch (build/spill/score mapInPandas) is validated by the pure dispatch-closure
tests + the pyspark Docker test (``tests/tracking_marts/``) + the live Part-B recompute.
"""

from __future__ import annotations

import types
from typing import Any

import pandas as pd
import pytest

import analytics.action_context.enrich as ac_enrich
import analytics.action_context.tracking_frame_spill as spill_mod
import ingestion.tracking_marts_dispatch as dispatch
import ingestion.tracking_marts_driver as driver
import ingestion.utils as utils
from analytics.action_context.work_unit import WorkUnit
from ingestion import tracking_marts_processor as tmp
from ingestion.tracking_marts_processor import (
    AGG_TABLE,
    GK_DECISION_TABLE,
    GKDV_OBS_TABLE,
    LONG_TABLE,
    OFF_BALL_TABLE,
    REST_DEFENSE_TABLE,
    TrackingMartsProcessor,
)


class _Meta:
    home_team_id = "HOME"


#: Per-mart fake row counts the faked ``write_delta_table`` returns (so ``total`` is deterministic).
_MART_ROWS = {
    OFF_BALL_TABLE: 5,
    AGG_TABLE: 3,
    LONG_TABLE: 2,
    GK_DECISION_TABLE: 7,
    REST_DEFENSE_TABLE: 4,
    GKDV_OBS_TABLE: 6,
}


class _FakeSdf:
    """Stands in for the lazy Stage-2 Spark DataFrame; ``.count()`` is the dry-run canary path."""

    def __init__(self, table: str) -> None:
        self.table = table
        self.counted = False

    def count(self) -> int:
        self.counted = True
        return _MART_ROWS[self.table]


def _make_processor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    gkdv_enabled: bool = True,
    actions: pd.DataFrame | None = None,
    write_raises: set[str] | None = None,
    stage1_raises: bool = False,
    score_window_frames: int | None = None,
) -> tuple[TrackingMartsProcessor, dict[str, Any]]:
    """Build a processor with every Spark seam faked. Returns (processor, record) where ``record`` captures
    stage1 calls, per-mart ``build_stage2`` tables, writes, dry-run counts, and cleanup."""
    rec: dict[str, Any] = {
        "writes": [],
        "stage2": [],
        "stage2_windows": [],
        "stage1": 0,
        "cleanup": 0,
        "sdfs": [],
        "stage2a": None,
        "stage2a_restricted": None,
        "scorer_args": None,
        "scorer_kwargs": None,
    }
    acts = pd.DataFrame({"period_id": [2, 2], "access_tier": ["public", "public"]}) if actions is None else actions

    # ── __init__ seams (module-level in tmp) ──
    monkeypatch.setattr(tmp, "ac_xt_grid", lambda spark, catalog, schema: ([[0.0]], 1, 1))
    monkeypatch.setattr(
        tmp, "_build_comp_season_lookup", lambda spark, catalog, providers: {("idsse", "M1"): ("C1", "2023")}
    )
    monkeypatch.setattr(tmp, "_off_ball_struct_type", lambda: "OFF_SCHEMA")
    monkeypatch.setattr(tmp, "_dc_struct_type", lambda cols, types: "DC_SCHEMA")
    monkeypatch.setattr(tmp, "_gkdv_obs_struct_type", lambda: "GKDV_SCHEMA")
    monkeypatch.setattr(tmp, "_rd_struct_type", lambda: "RD_SCHEMA")
    monkeypatch.setattr(tmp, "_gk_decision_struct_type", lambda: "GKD_SCHEMA")
    monkeypatch.setattr(tmp, "_bundled_completion_model", lambda: object())

    # ── process() driver reads ──
    monkeypatch.setattr(tmp, "resolve_unit_meta", lambda spark, catalog, provider, match_id: _Meta())
    monkeypatch.setattr(tmp, "_read_xg_preds", lambda spark, catalog, provider, match_id: pd.DataFrame())
    monkeypatch.setattr(driver, "read_unit_actions", lambda spark, catalog, provider, match_id: acts)
    monkeypatch.setattr(driver, "read_raw_trk_sdf", lambda spark, catalog, provider, match_id, period: "RAW_SDF")
    monkeypatch.setattr(ac_enrich, "_resolve_enrichment_identity", lambda a, *, provider, match_id_native: a)
    monkeypatch.setattr(spill_mod, "built_frame_struct_type", lambda provider: "BUILT_SCHEMA")

    # ── dispatch seam ──
    monkeypatch.setattr(dispatch, "spill_path", lambda catalog, schema, unit: "/vol/spill/u")
    monkeypatch.setattr(dispatch, "make_build_udf", lambda *a, **k: lambda raw: raw)

    def _fake_scorers(*a, **k):
        rec["scorer_args"] = a
        rec["scorer_kwargs"] = k
        return _scorers_for(k.get("gkdv_enabled", True))

    monkeypatch.setattr(dispatch, "make_mart_scorers", _fake_scorers)

    def _fake_stage1(spark, unit, raw_sdf, build_udf, built_schema, spill_dir):
        if stage1_raises:
            raise RuntimeError("stage1 boom")
        rec["stage1"] += 1
        return spill_dir

    monkeypatch.setattr(dispatch, "run_stage1_build_and_spill", _fake_stage1)

    def _fake_stage2a(spark, spill_dir, resolved_actions, provider, *, gkdv_enabled, target_window_frames):
        # Stage-2a reads the spill on the driver; faked here. gkdv-only invariants (carrier / stride) are
        # None when disabled, mirroring precompute_stage2a's need_carrier gating.
        rec["stage2a"] = {"gkdv_enabled": gkdv_enabled, "target_window_frames": target_window_frames}
        inv = types.SimpleNamespace(goal_map="GOAL_MAP", carrier=("CARRIER" if gkdv_enabled else None))
        out = types.SimpleNamespace(
            restricted_actions=resolved_actions,
            invariants=inv,
            scored_frame_keys=("SCORED_FRAME_KEYS" if gkdv_enabled else None),
            action_window={},
        )
        rec["stage2a_restricted"] = out.restricted_actions
        return out

    monkeypatch.setattr(dispatch, "precompute_stage2a", _fake_stage2a)

    def _fake_stage2(spark, unit, spill_dir, mart, *, action_window=None, target_window_frames=None):
        rec["stage2"].append(mart.table)
        rec["stage2_windows"].append(target_window_frames)
        sdf = _FakeSdf(mart.table)
        rec["sdfs"].append(sdf)
        return sdf

    monkeypatch.setattr(dispatch, "build_stage2_mart_sdf", _fake_stage2)

    def _fake_cleanup(spark, spill_dir):
        rec["cleanup"] += 1

    monkeypatch.setattr(dispatch, "cleanup_spill", _fake_cleanup)

    raises = write_raises or set()

    def _fake_write(sdf, catalog, schema, table, *, replace_where, logger):
        if table in raises:
            raise ValueError(f"{table} write boom")
        rec["writes"].append({"table": table, "where": replace_where})
        return _MART_ROWS[table]

    monkeypatch.setattr(utils, "write_delta_table", _fake_write)

    proc = TrackingMartsProcessor(
        spark=object(),
        catalog="cat",
        schema="bronze",
        gkdv_enabled=gkdv_enabled,
        score_window_frames=score_window_frames,
    )
    return proc, rec


def _scorers_for(gkdv_enabled: bool) -> dict[str, Any]:
    keys = ["off_ball_runs", "action_defensive", "defensive_credit_attributions", "gk_decision", "rest_defense"]
    if gkdv_enabled:
        keys.append("gkdv_observations")
    return {k: (lambda built: built) for k in keys}


_UNIT = WorkUnit(provider="idsse", match_id="M1", period=2)
_WHERE = "data_source = 'idsse' AND match_id = 'M1' AND period_id = 2"


def test_process_builds_six_specs_and_writes_each_with_replace_where(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True)
    total = proc.process(_UNIT)

    assert rec["stage1"] == 1
    assert rec["stage2"] == [
        OFF_BALL_TABLE,
        AGG_TABLE,
        LONG_TABLE,
        GK_DECISION_TABLE,
        REST_DEFENSE_TABLE,
        GKDV_OBS_TABLE,
    ]
    assert [w["table"] for w in rec["writes"]] == rec["stage2"]
    assert {w["where"] for w in rec["writes"]} == {_WHERE}
    assert total == sum(_MART_ROWS.values())
    assert rec["cleanup"] == 1  # finally


def test_process_five_specs_when_gkdv_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=False)
    proc.process(_UNIT)
    assert GKDV_OBS_TABLE not in rec["stage2"]
    assert len(rec["stage2"]) == 5
    assert GKDV_OBS_TABLE not in [w["table"] for w in rec["writes"]]


def test_process_threads_stage2a_invariants_into_scorers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stage-2a runs once (after the spill) and its whole-unit invariants + restricted actions are injected
    into ``make_mart_scorers`` — the ADR-090 wiring the windowed dispatch relies on (gkdv-only invariants
    gate on ``gkdv_enabled``)."""
    acts = pd.DataFrame({"period_id": [2, 2], "access_tier": ["public", "public"], "action_id": [10, 11]})
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True, actions=acts)
    proc.process(_UNIT)

    assert rec["stage2a"] == {"gkdv_enabled": True, "target_window_frames": dispatch._TM_SCORE_WINDOW_FRAMES}
    # The scorer's 3rd positional arg is the SAME object Stage-2a returned as restricted_actions (identity),
    # proving the coverage-restricted actions thread from Stage-2a into make_mart_scorers.
    assert rec["scorer_args"][2] is rec["stage2a_restricted"]
    assert rec["scorer_kwargs"]["goal_map"] == "GOAL_MAP"
    assert rec["scorer_kwargs"]["carrier"] == "CARRIER"
    assert rec["scorer_kwargs"]["scored_frame_keys"] == "SCORED_FRAME_KEYS"


def test_process_gkdv_disabled_injects_no_carrier_or_stride(monkeypatch: pytest.MonkeyPatch) -> None:
    """With gkdv off, Stage-2a resolves no carrier / stride survivors (``need_carrier=False``), so the
    scorers receive ``carrier=None`` + ``scored_frame_keys=None`` (only the GoalMap is injected)."""
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=False)
    proc.process(_UNIT)
    assert rec["stage2a"]["gkdv_enabled"] is False
    assert rec["scorer_kwargs"]["goal_map"] == "GOAL_MAP"
    assert rec["scorer_kwargs"]["carrier"] is None
    assert rec["scorer_kwargs"]["scored_frame_keys"] is None


def test_process_attributes_mart_failure_and_rolls_unit_forward(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True, write_raises={LONG_TABLE})
    with pytest.raises(RuntimeError, match=f"idsse:M1:2 failed.*{LONG_TABLE}"):
        proc.process(_UNIT)
    # the OTHER five marts still ran + wrote (per-mart isolation); spill cleaned up despite the failure.
    assert len(rec["stage2"]) == 6
    assert LONG_TABLE not in [w["table"] for w in rec["writes"]]
    assert len([w for w in rec["writes"]]) == 5
    assert rec["cleanup"] == 1


def test_process_cleanup_runs_even_when_stage1_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True, stage1_raises=True)
    with pytest.raises(RuntimeError):
        proc.process(_UNIT)
    assert rec["stage2"] == []  # never reached Stage 2
    assert rec["cleanup"] == 1  # finally still cleaned the spill


def test_process_empty_unit_returns_zero_and_skips_stage1(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, actions=pd.DataFrame({"period_id": [], "access_tier": []}))
    assert proc.process(_UNIT) == 0
    assert rec["stage1"] == 0
    assert rec["stage2"] == []


def test_process_dry_run_materializes_but_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True)
    total = proc.process(_UNIT, dry_run=True)
    assert total == 0
    assert rec["writes"] == []  # nothing persisted
    assert all(sdf.counted for sdf in rec["sdfs"])  # every mart forced to run (canary)
    assert len(rec["stage2"]) == 6
    assert rec["cleanup"] == 1


def test_process_passes_injected_score_window_to_both_dispatch_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """ADR-090 runtime override: a ``score_window_frames=4000`` worker feeds 4000 as ``target_window_frames``
    to BOTH ``precompute_stage2a`` (Stage-2a routing) AND every ``build_stage2_mart_sdf`` (Stage-2b dispatch).
    The SAME value on both is the cross-engine window-id agreement the windowed scoring relies on (R2)."""
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True, score_window_frames=4000)
    proc.process(_UNIT)

    assert rec["stage2a"]["target_window_frames"] == 4000
    assert rec["stage2_windows"] == [4000] * 6  # six marts, each dispatched at the injected window


def test_process_defaults_to_baked_window_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """No override (``score_window_frames=None``) => both dispatch calls use the baked
    ``_TM_SCORE_WINDOW_FRAMES`` (the daily-run default path is unchanged)."""
    proc, rec = _make_processor(monkeypatch, gkdv_enabled=True, score_window_frames=None)
    proc.process(_UNIT)

    assert rec["stage2a"]["target_window_frames"] == dispatch._TM_SCORE_WINDOW_FRAMES
    assert rec["stage2_windows"] == [dispatch._TM_SCORE_WINDOW_FRAMES] * 6


def test_score_window_default_is_unchanged() -> None:
    """Change-detector (spec §5 test 4): lowering the DEFAULT is a SEPARATE approval (§4.6 fit-profile);
    this cycle only adds the override. If this fails, someone moved the baked default — that needs its own
    sign-off, not this cycle. The constructor's override defaults OFF (None => resolve to the baked value)."""
    import inspect

    assert dispatch._TM_SCORE_WINDOW_FRAMES == 8000
    default = inspect.signature(TrackingMartsProcessor.__init__).parameters["score_window_frames"].default
    assert default is None


def test_gkdv_enabled_is_the_shipped_default() -> None:
    """gkdv is RE-ENABLED (sk 4.128 adoption, ADR-082 amendment): the module constant is True AND the
    constructor's ``gkdv_enabled`` defaults to it, so an un-parameterized worker scores gkdv."""
    import inspect

    assert tmp.GKDV_ENABLED is True
    default = inspect.signature(TrackingMartsProcessor.__init__).parameters["gkdv_enabled"].default
    assert default is tmp.GKDV_ENABLED
