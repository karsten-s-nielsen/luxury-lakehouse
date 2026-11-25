"""The shared per-match bronze write loop (`compute.base.run_per_match_bronze`; plan Task 4/DRY extraction).

The four compute steps (spadl_actions, pitch_control, shape_graphs, line_breaking) differ only in their match-discovery
query and their per-match `produce` body; the loop that tallies matches/rows/empty/failed, records a per-match failure
through the `ErrorSink` (and keeps going), and writes+swaps one bronze table is identical. This test drives that loop
on a fake writer + fake catalog across the four per-match outcomes (write / empty / already-done / raise) and asserts
the tally, the recorded failure, and that only the producing matches were written.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from lakehouse.compute.base import MatchBronze, SkipReason, run_per_match_bronze

if TYPE_CHECKING:
    from lakehouse.bronze.writers import BronzeWriter

_CLOCK = datetime(2026, 1, 1, tzinfo=UTC)


class _Row:
    def __init__(self, row: list[Any] | None) -> None:
        self._row = row

    def fetchone(self) -> list[Any] | None:
        return self._row


class _Conn:
    def execute(self, sql: str, params: Any = None) -> _Row:
        return _Row([False])  # the only query the loop runs is the ops_matches visibility lookup (bool_or: not private)


class _Writer:
    def __init__(self) -> None:
        self.swaps: list[int] = []

    def write_table(self, put: Any, *, match_key: int, **_: Any) -> tuple[str, int]:
        # The writer contract (writer._check_rows): the data carries a match_key column, all equal to the arg.
        assert "match_key" in put.data.column_names
        assert set(put.data.column("match_key").to_pylist()) == {match_key}
        return ("written", match_key)

    def swap(self, match_key: int, parts: Any, tables: Any, *, also: Any) -> None:
        also()
        self.swaps.append(match_key)


def _cat() -> Any:
    return type("_Cat", (), {"conn": _Conn(), "storage": None})()


def _bronze(n: int) -> MatchBronze:
    table = pa.table({"canonical_match_key": pa.array([0] * n, pa.int64())})
    return MatchBronze(data=table, source_hash="h" * 16)


def test_loop_tallies_writes_empties_skips_and_failures() -> None:
    writer = _Writer()
    errors: list[dict[str, Any]] = []

    def produce(row: int, match_key: int) -> MatchBronze | SkipReason:
        if match_key == 1:
            return _bronze(2)  # writes 2 rows
        if match_key == 2:
            return SkipReason.EMPTY  # counted empty, not written
        if match_key == 3:
            return SkipReason.DONE  # skip-guard hit, silent
        raise ValueError("boom")  # match 4: recorded, loop continues

    outcome = run_per_match_bronze(
        _cat(),
        step="demo",
        output_table="demo_table",
        schema=pa.schema([pa.field("canonical_match_key", pa.int64())]),
        matches=[1, 2, 3, 4],
        match_key_of=lambda r: int(r),
        produce=produce,
        run_id="r1",
        now=lambda: _CLOCK,
        error=lambda **kw: errors.append(kw),  # type: ignore[arg-type]
        writer=cast("BronzeWriter", writer),
    )

    assert outcome.step == "demo"
    assert outcome.matches == 4
    assert outcome.rows == 2  # only match 1 wrote, 2 rows
    assert outcome.empty_matches == 1  # match 2
    assert outcome.failed_matches == 1  # match 4
    assert writer.swaps == [1]  # only the producing match was swapped in (3 was DONE, silent)
    assert len(errors) == 1 and errors[0]["kind"] == "compute_failed"
    assert "match_key=4" in errors[0]["message"]


# --- perf plan Task 6: deadlines, durable progress, deferred ("not reached") matches (perf spec §7.5) ----------------

_SCHEMA = pa.schema([pa.field("canonical_match_key", pa.int64())])


def _control(*, soft_at: int | None = None, stop_after: int | None = None, written: list[int] | None = None) -> Any:
    """A control whose clock is the number of `may_start_match` calls so far: the soft deadline passes at call
    `soft_at`, and `should_stop` turns True after `stop_after` checks."""
    from datetime import timedelta

    from lakehouse.compute.control import ComputeControl

    ticks = {"t": 0, "checks": 0}

    def now() -> datetime:
        return _CLOCK + timedelta(seconds=ticks["t"])

    soft = None if soft_at is None else _CLOCK + timedelta(seconds=soft_at)
    control = ComputeControl(
        soft_deadline=soft,
        hard_deadline=None,
        now=now,
        stop=lambda: stop_after is not None and ticks["checks"] >= stop_after,
        on_written=lambda: written.append(1) if written is not None else None,
        check_every=1,
    )

    class _Ticking:
        def __getattr__(self, name: str) -> Any:
            return getattr(control, name)

        def may_start_match(self) -> bool:
            result = control.may_start_match()
            ticks["t"] += 1
            return result

        def should_stop(self) -> bool:
            ticks["checks"] += 1
            return control.should_stop()

    return _Ticking()


def _loop(produce: Any, control: Any, errors: list[dict[str, Any]], writer: Any, matches: list[int]) -> Any:
    def record(**kw: Any) -> None:
        errors.append(kw)

    return run_per_match_bronze(
        _cat(),
        step="demo",
        output_table="demo_table",
        schema=_SCHEMA,
        matches=matches,
        match_key_of=lambda r: int(r),
        produce=produce,
        run_id="r1",
        now=lambda: _CLOCK,
        error=record,
        writer=cast("BronzeWriter", writer),
        control=control,
    )


def test_a_write_calls_on_written() -> None:
    written: list[int] = []

    def produce(row: int, mk: int) -> MatchBronze:
        return _bronze(1)

    outcome = _loop(produce, _control(written=written), [], _Writer(), [1, 2, 3])
    assert outcome.rows == 3
    assert len(written) == 3  # the run's checkpoint policy sees one deferrable unit per written match


def test_past_soft_deadline_starts_no_match_and_counts_not_reached() -> None:
    errors: list[dict[str, Any]] = []
    produced: list[int] = []

    def produce(row: int, mk: int) -> MatchBronze:
        produced.append(mk)
        return _bronze(1)

    outcome = _loop(produce, _control(soft_at=2), errors, _Writer(), [1, 2, 3, 4, 5])
    assert produced == [1, 2]  # matches 3..5 are not started once the soft deadline has passed
    assert outcome.deferred_matches == 3
    assert outcome.failed_matches == 0
    assert len(errors) == 1
    assert errors[0]["kind"] == "compute_deferred" and errors[0]["severity"] == "warning"
    assert "skip state unchecked" in errors[0]["message"]


def test_abandoned_match_writes_nothing_and_counts_not_reached() -> None:
    from lakehouse.compute.control import ComputeAbandoned

    writer, errors = _Writer(), list[dict[str, Any]]()

    def produce(row: int, mk: int) -> MatchBronze:
        if mk == 2:
            raise ComputeAbandoned("hard deadline")
        return _bronze(1)

    outcome = _loop(produce, _control(), errors, writer, [1, 2])
    assert writer.swaps == [1]  # match 2 wrote nothing
    assert outcome.deferred_matches == 1 and outcome.failed_matches == 0
    assert [e["kind"] for e in errors] == ["compute_deferred"]


def test_spadl_step_honours_the_soft_deadline() -> None:
    import duckdb

    from lakehouse.compute.spadl_actions import SPADL_ACTIONS_GRADIENTSPORTS

    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    conn.execute("CREATE TABLE lake.main.gradientsports_events (match_key BIGINT)")
    conn.execute("INSERT INTO lake.main.gradientsports_events VALUES (1), (2)")
    errors: list[dict[str, Any]] = []

    def record(**kw: Any) -> None:
        errors.append(kw)

    cat: Any = type("_Cat", (), {"conn": conn, "storage": None})()
    outcome = SPADL_ACTIONS_GRADIENTSPORTS(
        cat, run_id="r1", caps=cast(Any, None), now=lambda: _CLOCK, error=record, control=_control(soft_at=0)
    )
    assert outcome.matches == 0 and outcome.deferred_matches == 2  # no SPADL match started past the soft deadline
    assert [e["kind"] for e in errors] == ["compute_deferred"]
