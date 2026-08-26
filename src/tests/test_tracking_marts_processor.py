"""Unit tests for ``ingestion.tracking_marts_processor.TrackingMartsProcessor`` (Task 4, ADR-037).

The processor is orchestration: build one unit's inputs ONCE, run the four tracking-grain scorers, write
each result with the per-unit ``replaceWhere``. These tests fake every Spark/pyspark seam (the xT-grid +
comp/season loads, the struct-type factories, ``read_and_build_unit_inputs``, ``_read_xg_preds``,
``resolve_unit_meta``, and ``_write``) so NO Spark is touched, and assert the orchestration contract:

* all three scorers run on the SAME oriented ``(actions, frames, xt)``;
* all four bronze tables are written with the identical per-unit ``replaceWhere``;
* the returned count is the sum across the four writes;
* a per-scorer exception is attributed and re-raised as a combined unit failure (drain rolls it forward),
  while the OTHER scorers still write (per-scorer isolation).

The real ``_write`` (``spark.createDataFrame`` + ``write_delta_table``) is validated live in Part B.
"""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.action_context.unit_inputs import UnitInputs
from analytics.action_context.work_unit import WorkUnit
from ingestion import tracking_marts_processor as tmp
from ingestion.tracking_marts_processor import _GKDV_OBS_COLUMNS, TrackingMartsProcessor


class _Meta:
    home_team_id = "HOME"


def _gkdv_frame(match_id: str) -> pd.DataFrame:
    """A score_gkdv_unit-shaped frame (the 9 columns it stamps, minus the processor-added match_id)."""
    return pd.DataFrame(
        {
            "data_source": ["idsse"],
            "game_id": [match_id],
            "competition_id": ["C1"],
            "season_id": ["2023"],
            "player_id": ["gk"],
            "period_id": [2],
            "frame_id": [7],
            "delta_das": [0.1],
            "delta_threat_suppression": [0.2],
        }
    )


def _make_processor(monkeypatch, *, inputs, capture, gkdv_enabled: bool = True):
    """Construct a processor with every Spark/pyspark seam faked; capture writes into ``capture``.

    ``gkdv_enabled`` defaults True here so the existing four-write contract tests keep exercising the gkdv
    scoring path (the perf project depends on it). The SHIPPED default is False (gkdv gated off) — see
    ``test_gkdv_gated_off_is_the_shipped_default`` and the ``gkdv_enabled=False`` skip tests.
    """
    monkeypatch.setattr(tmp, "ac_xt_grid", lambda spark, catalog, schema: ([[0.0]], 1, 1))
    monkeypatch.setattr(
        tmp, "_build_comp_season_lookup", lambda spark, catalog, providers: {("idsse", "M1"): ("C1", "2023")}
    )
    monkeypatch.setattr(tmp, "_off_ball_struct_type", lambda: "OFF_SCHEMA")
    monkeypatch.setattr(tmp, "_dc_struct_type", lambda cols, types: ("DC_SCHEMA", tuple(cols)))
    monkeypatch.setattr(tmp, "_gkdv_obs_struct_type", lambda: "GKDV_SCHEMA")
    monkeypatch.setattr(tmp, "resolve_unit_meta", lambda spark, catalog, provider, match_id: _Meta())
    monkeypatch.setattr(tmp, "read_and_build_unit_inputs", lambda spark, catalog, unit, **kw: inputs)
    monkeypatch.setattr(tmp, "_read_xg_preds", lambda spark, catalog, provider, match_id: pd.DataFrame())
    monkeypatch.setattr(tmp, "attach_xg", lambda actions, xg_preds: actions)  # passthrough (same actions object)

    proc = TrackingMartsProcessor(spark=object(), catalog="cat", schema="bronze", gkdv_enabled=gkdv_enabled)

    def _fake_write(pdf, schema, table, where):
        capture.append({"table": table, "where": where, "rows": len(pdf), "schema": schema})
        return len(pdf)

    monkeypatch.setattr(proc, "_write", _fake_write)
    return proc


def test_process_runs_three_scorers_on_same_inputs_and_writes_four_tables(monkeypatch) -> None:
    actions = pd.DataFrame({"a": [1, 2]})
    frames = pd.DataFrame({"f": [1, 2, 3]})
    inputs = UnitInputs(actions=actions, frames=frames, xt="XT")
    capture: list[dict] = []
    seen: dict[str, tuple] = {}

    def _obr(a, f, xt):
        seen["obr"] = (id(a), id(f), xt)
        return pd.DataFrame({"x": range(5)})

    def _agg(a, f, xt):
        seen["agg"] = (id(a), id(f), xt)
        return pd.DataFrame({"x": range(3)})

    def _long(a, f, xt):
        seen["long"] = (id(a), id(f), xt)
        return pd.DataFrame({"x": range(2)})

    def _gkdv(frames_arg, home_team_id, xt, *, data_source, match_id, competition_id, season_id, want_threat=True):
        seen["gkdv"] = (id(frames_arg), xt, data_source, match_id, competition_id, season_id, home_team_id)
        return _gkdv_frame(match_id)

    proc = _make_processor(monkeypatch, inputs=inputs, capture=capture)
    monkeypatch.setattr(tmp, "compute_off_ball_runs", _obr)
    monkeypatch.setattr(tmp, "compute_action_defensive_credit", _agg)
    monkeypatch.setattr(tmp, "compute_defensive_credit_long", _long)
    monkeypatch.setattr(tmp, "score_gkdv_unit", _gkdv)

    unit = WorkUnit(provider="idsse", match_id="M1", period=2)
    total = proc.process(unit)

    # (a) all three (actions, frames) scorers ran on the SAME oriented inputs; gkdv on the same frames.
    assert seen["obr"][0] == seen["agg"][0] == seen["long"][0] == id(actions)
    assert seen["obr"][1] == seen["agg"][1] == seen["long"][1] == id(frames)
    assert seen["gkdv"][0] == id(frames)
    assert seen["obr"][2] == seen["agg"][2] == seen["long"][2] == seen["gkdv"][1] == "XT"
    # gkdv got home_team_id from resolve_unit_meta and (comp, season) from the lookup.
    assert seen["gkdv"][2:7] == ("idsse", "M1", "C1", "2023", "HOME")

    # (b) four writes to the four bronze tables, each with the SAME per-unit replaceWhere.
    where = "data_source = 'idsse' AND match_id = 'M1' AND period_id = 2"
    assert [c["table"] for c in capture] == [
        "off_ball_runs",
        "action_defensive_credit",
        "defensive_credit_attributions",
        "gkdv_observations",
    ]
    assert {c["where"] for c in capture} == {where}

    # (c) summed row count across all four writes.
    assert total == 5 + 3 + 2 + 1

    # gkdv write carries the intermediate schema in the canonical column order.
    gkdv_write = capture[-1]
    assert gkdv_write["schema"] == "GKDV_SCHEMA"


def test_process_gkdv_write_has_full_columns_including_match_id(monkeypatch) -> None:
    """The processor stamps match_id and selects the canonical _GKDV_OBS_COLUMNS order before writing."""
    inputs = UnitInputs(actions=pd.DataFrame({"a": [1]}), frames=pd.DataFrame({"f": [1]}), xt="XT")
    captured_pdf: dict[str, pd.DataFrame] = {}

    def _make(monkeypatch):
        proc = _make_processor(monkeypatch, inputs=inputs, capture=[])
        monkeypatch.setattr(tmp, "compute_off_ball_runs", lambda a, f, xt: pd.DataFrame({"x": [1]}))
        monkeypatch.setattr(tmp, "compute_action_defensive_credit", lambda a, f, xt: pd.DataFrame({"x": [1]}))
        monkeypatch.setattr(tmp, "compute_defensive_credit_long", lambda a, f, xt: pd.DataFrame({"x": [1]}))
        monkeypatch.setattr(tmp, "score_gkdv_unit", lambda *a, **k: _gkdv_frame(k["match_id"]))

        def _capture_write(pdf, schema, table, where):
            if table == "gkdv_observations":
                captured_pdf["pdf"] = pdf
            return len(pdf)

        monkeypatch.setattr(proc, "_write", _capture_write)
        return proc

    proc = _make(monkeypatch)
    proc.process(WorkUnit(provider="idsse", match_id="M1", period=2))

    written = captured_pdf["pdf"]
    assert list(written.columns) == list(_GKDV_OBS_COLUMNS)
    assert (written["match_id"] == "M1").all()
    assert (written["game_id"] == "M1").all()  # game_id == match_id


def test_process_attributes_scorer_failure_and_rolls_unit_forward(monkeypatch) -> None:
    inputs = UnitInputs(actions=pd.DataFrame({"a": [1]}), frames=pd.DataFrame({"f": [1]}), xt="XT")
    capture: list[dict] = []
    proc = _make_processor(monkeypatch, inputs=inputs, capture=capture)

    def _boom(a, f, xt):
        raise ValueError("obr exploded")

    monkeypatch.setattr(tmp, "compute_off_ball_runs", _boom)
    monkeypatch.setattr(tmp, "compute_action_defensive_credit", lambda a, f, xt: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(tmp, "compute_defensive_credit_long", lambda a, f, xt: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(tmp, "score_gkdv_unit", lambda *a, **k: _gkdv_frame(k["match_id"]))

    unit = WorkUnit(provider="idsse", match_id="M1", period=2)
    with pytest.raises(RuntimeError) as excinfo:
        proc.process(unit)

    msg = str(excinfo.value)
    assert "off_ball_runs" in msg and "obr exploded" in msg
    assert "idsse:M1:2" in msg

    # Per-scorer isolation: the failing scorer skipped its write, the others still wrote (unit rolls forward).
    tables = [c["table"] for c in capture]
    assert "off_ball_runs" not in tables
    assert "action_defensive_credit" in tables
    assert "defensive_credit_attributions" in tables
    assert "gkdv_observations" in tables


def test_process_empty_unit_returns_zero_and_writes_nothing(monkeypatch) -> None:
    capture: list[dict] = []
    proc = _make_processor(monkeypatch, inputs=None, capture=capture)
    # No scorer should be reached; make them explode if called.
    monkeypatch.setattr(tmp, "compute_off_ball_runs", lambda *a, **k: (_ for _ in ()).throw(AssertionError("called")))

    total = proc.process(WorkUnit(provider="idsse", match_id="M1", period=2))

    assert total == 0
    assert capture == []


# ── gkdv gated off (ADR-082 amendment): default-off ships off_ball_runs + defensive_credit only ──


def test_gkdv_gated_off_is_the_shipped_default() -> None:
    """gkdv is gated off by default pending its perf project: the module constant is False AND the
    constructor's ``gkdv_enabled`` defaults to it, so an un-parameterized worker (the shipped path) never
    scores gkdv. Asserted on the signature so no Spark seam is needed."""
    import inspect

    assert tmp.GKDV_ENABLED is False
    default = inspect.signature(TrackingMartsProcessor.__init__).parameters["gkdv_enabled"].default
    assert default is tmp.GKDV_ENABLED


def test_gkdv_gated_off_skips_scoring_and_writes_only_three_tables(monkeypatch) -> None:
    """With gkdv gated off, ``score_gkdv_unit`` is NEVER called, no gkdv_observations write happens, and
    the unit still succeeds with the two shipping surfaces (off_ball_runs + defensive_credit)."""
    inputs = UnitInputs(actions=pd.DataFrame({"a": [1]}), frames=pd.DataFrame({"f": [1]}), xt="XT")
    capture: list[dict] = []
    proc = _make_processor(monkeypatch, inputs=inputs, capture=capture, gkdv_enabled=False)
    monkeypatch.setattr(tmp, "compute_off_ball_runs", lambda a, f, xt: pd.DataFrame({"x": range(5)}))
    monkeypatch.setattr(tmp, "compute_action_defensive_credit", lambda a, f, xt: pd.DataFrame({"x": range(3)}))
    monkeypatch.setattr(tmp, "compute_defensive_credit_long", lambda a, f, xt: pd.DataFrame({"x": range(2)}))
    # gkdv scoring AND its meta lookup must be unreachable when gated off.
    monkeypatch.setattr(tmp, "score_gkdv_unit", lambda *a, **k: (_ for _ in ()).throw(AssertionError("gkdv scored")))
    monkeypatch.setattr(
        tmp, "resolve_unit_meta", lambda *a, **k: (_ for _ in ()).throw(AssertionError("gkdv meta resolved"))
    )

    total = proc.process(WorkUnit(provider="idsse", match_id="M1", period=2))

    assert [c["table"] for c in capture] == [
        "off_ball_runs",
        "action_defensive_credit",
        "defensive_credit_attributions",
    ]
    assert "gkdv_observations" not in [c["table"] for c in capture]
    assert total == 5 + 3 + 2
    # The gkdv-only (comp, season) warehouse lookup is skipped when gated off.
    assert proc._comp_season == {}


def test_gkdv_gated_off_cannot_fail_the_unit_even_if_scoring_would_raise(monkeypatch) -> None:
    """A gated-off gkdv is inert: even a score_gkdv_unit that WOULD raise is never invoked, so a unit whose
    off_ball + defensive scorers succeed completes without the combined RuntimeError."""
    inputs = UnitInputs(actions=pd.DataFrame({"a": [1]}), frames=pd.DataFrame({"f": [1]}), xt="XT")
    capture: list[dict] = []
    proc = _make_processor(monkeypatch, inputs=inputs, capture=capture, gkdv_enabled=False)
    monkeypatch.setattr(tmp, "compute_off_ball_runs", lambda a, f, xt: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(tmp, "compute_action_defensive_credit", lambda a, f, xt: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(tmp, "compute_defensive_credit_long", lambda a, f, xt: pd.DataFrame({"x": [1]}))

    def _boom(*a, **k):
        raise ValueError("gkdv exploded")

    monkeypatch.setattr(tmp, "score_gkdv_unit", _boom)

    # No RuntimeError: gkdv never runs, so it never contributes a combined-failure attribution.
    total = proc.process(WorkUnit(provider="idsse", match_id="M1", period=2))
    assert total == 3
    assert [c["table"] for c in capture] == [
        "off_ball_runs",
        "action_defensive_credit",
        "defensive_credit_attributions",
    ]
