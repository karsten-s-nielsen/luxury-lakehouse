"""Task 5 — the planner's SPADL leg is PERIOD grain, in BOTH sibling planners (§0a, the pair rule).

`_find_tracking_new_period_pairs` and `_find_idsse_new_period_pairs` are welded siblings: they run
the same three-way join, and an edit to one that is not mirrored in the other is a defect. So every
test here is PARAMETRIZED over both — a fix that lands in only one function fails the suite.

## Why the fake models `cast()` for real (W2)

The pre-existing planner fakes (`test_action_context_enrichment.py::_MockDF`) take rows that are
ALREADY keyed `_mid`/`_period` — `select()` is a no-op passthrough, so the `.cast(...)` calls in the
planner are never executed. That is fine for testing join COMPOSITION, and useless for testing the
one failure mode this edit can actually have:

> if `spadl.period_id` and `tracking.period` disagree in ENCODING or DTYPE, the new period-grain
> join silently matches NOTHING, the planner enumerates ZERO units, the drain does nothing, and the
> D8 gate still reports COMPLETE (it is structurally blind to an under-enumerating planner — the M2
> diagnostic re-runs this same function, so `enqueued == 0` AND `remaining == 0`).

So the fake below takes RAW bronze column names (`match_id_native`, `period_id`, `period`,
`data_source`) and executes `select`/`cast`/`alias`/`filter` with real semantics — including Spark's
"an uncastable value becomes NULL", which is exactly how a silent zero-match would arise. This is the
repo's ADR-019 canonical-id class.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

_TRACKING = "cat.bronze.provider_tracking"
_SPADL = "cat.bronze.spadl_actions"
_RESULTS = "cat.bronze.spadl_action_context"

# ── a Spark fake that actually EXECUTES select / cast / alias / filter ───────────────


def _cast_string(value: Any) -> Any:
    return None if value is None else str(value)


def _cast_bigint(value: Any) -> Any:
    """Spark semantics: an UNCASTABLE value becomes NULL (it does not raise).

    This is the whole point of the fake — a NULL join key matches nothing, which is precisely how a
    period-encoding mismatch would silently empty the planner.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_CASTS = {"string": _cast_string, "bigint": _cast_bigint}


class _Eq:
    """The predicate produced by ``F.col(x) == literal``."""

    def __init__(self, col: _Col, value: Any) -> None:
        self._col = col
        self._value = value

    def matches(self, row: dict[str, Any]) -> bool:
        return self._col.evaluate(row) == self._value


class _Col:
    def __init__(self, name: str, cast_type: str | None = None, alias_name: str | None = None) -> None:
        self.name = name
        self.cast_type = cast_type
        self.alias_name = alias_name

    def cast(self, cast_type: str) -> _Col:
        return _Col(self.name, cast_type, self.alias_name)

    def alias(self, alias_name: str) -> _Col:
        return _Col(self.name, self.cast_type, alias_name)

    @property
    def out(self) -> str:
        return self.alias_name or self.name

    def evaluate(self, row: dict[str, Any]) -> Any:
        value = row.get(self.name)
        return _CASTS[self.cast_type](value) if self.cast_type else value

    def __eq__(self, other: object) -> _Eq:  # pyright: ignore[reportIncompatibleMethodOverride]
        """Spark Columns build a PREDICATE from ``==``; they do not compare."""
        return _Eq(self, other)

    def __hash__(self) -> int:
        return id(self)


class _FakeDF:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = list(rows)

    def select(self, *cols: _Col) -> _FakeDF:
        return _FakeDF([{c.out: c.evaluate(row) for c in cols} for row in self._rows])

    def filter(self, pred: _Eq) -> _FakeDF:
        return _FakeDF([row for row in self._rows if pred.matches(row)])

    def distinct(self) -> _FakeDF:
        seen: set[tuple[Any, ...]] = set()
        out: list[dict[str, Any]] = []
        for row in self._rows:
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                out.append(row)
        return _FakeDF(out)

    def join(self, other: _FakeDF, on: str | list[str], how: str) -> _FakeDF:
        keys = [on] if isinstance(on, str) else list(on)
        right = {tuple(r[k] for k in keys) for r in other._rows}
        out = [
            row
            for row in self._rows
            if (tuple(row[k] for k in keys) in right) is (how == "inner")  # inner keeps hits, left_anti misses
        ]
        return _FakeDF(out)

    def collect(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSpark:
    def __init__(self, tables: dict[str, _FakeDF]) -> None:
        self._tables = tables

    def table(self, name: str) -> _FakeDF:
        return self._tables.get(name, _FakeDF([]))


@pytest.fixture
def _fake_pyspark(monkeypatch: pytest.MonkeyPatch):
    """Inject a ``pyspark.sql.functions`` whose ``col`` builds the real ``_Col`` above."""
    functions = MagicMock()
    functions.col = _Col
    sql_module = MagicMock()
    sql_module.functions = functions
    monkeypatch.setitem(sys.modules, "pyspark", MagicMock())
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)


# ── the pair rule (§0a): every case runs against BOTH sibling planners ───────────────

_PLANNERS = [("tracking", "skillcorner"), ("idsse", "idsse")]


def _enumerate(which: str, spark: _FakeSpark, provider: str) -> list[tuple[str, int]]:
    from ingestion.action_context import (
        _find_idsse_new_period_pairs,
        _find_tracking_new_period_pairs,
    )

    if which == "tracking":
        return _find_tracking_new_period_pairs(spark, _TRACKING, _SPADL, _RESULTS, provider)  # type: ignore[arg-type]
    return _find_idsse_new_period_pairs(spark, _TRACKING, _SPADL, _RESULTS)  # type: ignore[arg-type]


def _tables(
    *,
    tracking: list[dict[str, Any]],
    spadl: list[dict[str, Any]],
    results: list[dict[str, Any]] | None = None,
) -> dict[str, _FakeDF]:
    return {
        _TRACKING: _FakeDF(tracking),
        _SPADL: _FakeDF(spadl),
        _RESULTS: _FakeDF(results or []),
    }


@pytest.mark.usefixtures("_fake_pyspark")
@pytest.mark.parametrize(("which", "provider"), _PLANNERS)
def test_period_WITH_actions_IS_still_enumerated(which: str, provider: str) -> None:  # noqa: N802
    """W2 — the POSITIVE case, and the one that catches a SILENTLY DEAD planner.

    A join that matches NOTHING passes the zero-action test trivially. Without this assertion, a
    period-grain join that reconciles no rows at all ships green: the drain does nothing, and D8
    reports COMPLETE because it re-runs this same function to compute `remaining`.
    """
    spark = _FakeSpark(
        _tables(
            tracking=[
                {"match_id": "m1", "period": 1},
                {"match_id": "m1", "period": 2},
                {"match_id": "m2", "period": 1},
            ],
            spadl=[
                {"match_id_native": "m1", "period_id": 1, "data_source": provider},
                {"match_id_native": "m1", "period_id": 2, "data_source": provider},
                {"match_id_native": "m2", "period_id": 1, "data_source": provider},
            ],
            results=[{"match_id": "m1", "period_id": 1, "data_source": provider}],  # already processed
        )
    )
    assert sorted(_enumerate(which, spark, provider)) == [("m1", 2), ("m2", 1)]


@pytest.mark.usefixtures("_fake_pyspark")
@pytest.mark.parametrize(("which", "provider"), _PLANNERS)
def test_period_with_frames_but_ZERO_actions_is_NOT_enumerated(which: str, provider: str) -> None:  # noqa: N802
    """THE BEHAVIOUR CHANGE. A (match, period) with tracking frames but ZERO spadl actions in that
    period is enumerated today (the SPADL leg is MATCH grain), processed to `_empty_result()` (0
    rows), never lands in results -> RE-ENUMERATED FOREVER. Latent (measured live: 0), not safe.
    """
    spark = _FakeSpark(
        _tables(
            tracking=[
                {"match_id": "m1", "period": 1},
                {"match_id": "m1", "period": 2},  # frames, but NO actions in period 2
            ],
            spadl=[{"match_id_native": "m1", "period_id": 1, "data_source": provider}],
        )
    )
    assert sorted(_enumerate(which, spark, provider)) == [("m1", 1)]


@pytest.mark.usefixtures("_fake_pyspark")
@pytest.mark.parametrize(("which", "provider"), _PLANNERS)
def test_period_encoding_variants_still_join(which: str, provider: str) -> None:
    """W2 — THE ACTUAL FAILURE MODE, and the one the live 374-count gate is the only other guard for.

    `spadl.period_id` and `tracking.period` must reconcile across dtype/encoding variants (string
    vs int match ids, string vs int periods). Both sides are `.cast(...)` in the planner; this
    asserts the casts actually reconcile rather than silently producing a join that matches nothing.
    """
    spark = _FakeSpark(
        _tables(
            tracking=[
                {"match_id": 1552423, "period": 1},  # int match id, int period
                {"match_id": 1552423, "period": 2},
            ],
            spadl=[
                {"match_id_native": "1552423", "period_id": "1", "data_source": provider},  # STRINGS
                {"match_id_native": "1552423", "period_id": "2", "data_source": provider},
            ],
            results=[{"match_id": "1552423", "period_id": 1, "data_source": provider}],
        )
    )
    assert sorted(_enumerate(which, spark, provider)) == [("1552423", 2)]


@pytest.mark.usefixtures("_fake_pyspark")
@pytest.mark.parametrize(("which", "provider"), _PLANNERS)
def test_other_providers_actions_do_not_satisfy_this_providers_period(which: str, provider: str) -> None:
    """The `data_source` filter must survive the grain change: another provider's actions in the same
    period must not make this provider's zero-action period look non-empty."""
    spark = _FakeSpark(
        _tables(
            tracking=[{"match_id": "m1", "period": 1}, {"match_id": "m1", "period": 2}],
            spadl=[
                {"match_id_native": "m1", "period_id": 1, "data_source": provider},
                {"match_id_native": "m1", "period_id": 2, "data_source": "some_other_provider"},
            ],
        )
    )
    assert sorted(_enumerate(which, spark, provider)) == [("m1", 1)]


# ── the enqueue round-trip (spec §11) ────────────────────────────────────────────────


def _preflight_with_queue(monkeypatch: pytest.MonkeyPatch, queue_cls: type) -> None:
    import argparse

    import ingestion.action_context as ac
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.guards import FilterResult

    ns = argparse.Namespace(catalog="cat", schema="bronze", provider=None, max_units=None, run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(ac, "timed_check", lambda g, s, c, sc: FilterResult(workflow_id="x", count=3))
    monkeypatch.setattr(ac, "_force_full_rematerialize_on_grid_change", lambda *a, **k: None)
    units = [WorkUnit(provider="skillcorner", match_id=f"m{i}", period=1) for i in range(3)]
    monkeypatch.setattr(ac._ActionContextGuard, "discover_units", lambda self, s, c, sc: units)
    monkeypatch.setattr(ac, "_set_task_value", lambda key, value, log: None)

    class _FakeSink:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def ensure_tables(self) -> None:
            pass

    monkeypatch.setattr(q, "DeltaUnitEventSink", _FakeSink)
    monkeypatch.setattr(q, "DeltaWorkQueue", queue_cls)  # patched at SOURCE (function-local import)
    ac.main_preflight()


class _RoundTripQueue:
    """A queue whose persisted count is settable — `persisted` short-changes the enqueue."""

    persisted: int | None = None  # None => honest (returns what it was given)

    def __init__(self, *a: object, **k: object) -> None:
        self._n = 0

    def ensure_table(self) -> None:
        pass

    def prune(self, *a: object, **k: object) -> int:
        return 0

    def enqueue(self, run_id: str, assignments: list) -> None:
        self._n = len(assignments)

    def count_for_run(self, run_id: str) -> int:
        short = type(self).persisted
        return self._n if short is None else short


def test_enqueue_round_trip_count_is_asserted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §11 — planner discovered N, `enqueue` persisted M < N.

    The D8 gate compares queue rows against unit events: BOTH would be self-consistently SHORT, so a
    partial enqueue is invisible to every downstream check. Preflight is the only place that holds
    both numbers, so it is the only place that can assert them.
    """

    class _Short(_RoundTripQueue):
        persisted = 2  # 3 assigned, 2 landed

    with pytest.raises(RuntimeError, match="round-trip"):
        _preflight_with_queue(monkeypatch, _Short)


def test_enqueue_round_trip_passes_when_every_unit_landed(monkeypatch: pytest.MonkeyPatch) -> None:
    """The positive half — the assert must not fire on a healthy enqueue."""
    _preflight_with_queue(monkeypatch, _RoundTripQueue)  # no raise
