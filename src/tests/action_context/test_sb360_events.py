"""Task 6 — sb360 emits unit events (it is otherwise UNGATED).

sb360 is **never enqueued** (``action_context.py`` ``_find_sb360_new_ids`` / ADR-058): it EXITS the
per-match drain and runs as ONE distributed cogroup job with its OWN terraform task. So the queue
says NOTHING about statsbomb, and a queue-only completeness gate leaves it completely unchecked.

THE sb360 SEAM (plan §0c) is the most defect-prone part of this design — a defect landed here in
SIX consecutive review rounds (P2 -> V2 -> W3 -> X1/X2 -> Y1/Y2), never the same one twice. The
constant: **a second producer with a different lifecycle silently falls outside every rule written
while looking at the first one.** So each of §0c's four questions gets its own test here:

1. (X2) the early-return on "no new matches" — the COMMON daily case — must STILL emit
   ``slice_completed``, or the gate reports DRAIN_FAILED every quiet day.
2. (X2b/Y1) terraform must pass ``--run-id`` AND argparse must accept it — a producer/consumer PAIR.
3. (W3) every sb360 event carries the ``SB360_WORKER_ID`` sentinel...
4. (X1/Y2) ...IMPORTED from ``analytics.action_context.drain`` — the one home BOTH the producer
   (ingestion) and the consumer (the pure gate in analytics) can read. Never the literal ``-1``.

Plus §0d: every event write must go through the append-guarded sink — ``write_delta_table``
DEFAULTS to ``mode="overwrite"``, so a direct write from the sb360 path would silently WIPE the
event log. Guarded, and the guard is proven to FAIL on a planted violation (§0b).
"""

from __future__ import annotations

import argparse
import ast
import inspect
import re
from pathlib import Path
from typing import Any, ClassVar

import pytest

from analytics.action_context.drain import SB360_WORKER_ID
from analytics.action_context.work_unit import WorkUnit
from tests._delta_write_ast import write_delta_table_calls, writes_without_append

_MAIN_TF = Path(__file__).resolve().parents[3] / "terraform" / "modules" / "workflows" / "main.tf"

# Pre-compiled at MODULE level (CLAUDE.md: never `re.compile`/`re.findall` with a raw pattern string
# inside a function body).
_DEPENDS_ON_RE = re.compile(r'depends_on\s*\{\s*task_key\s*=\s*"([^"]+)"')
_TF_FLAG_RE = re.compile(r'"(--[a-z0-9-]+)"')


# ── fakes ──────────────────────────────────────────────────────────────


class _RecordingSink:
    """Records every sb360 event in call order. Constructed by ``main_statsbomb`` itself."""

    instances: ClassVar[list[_RecordingSink]] = []

    def __init__(self, spark: object, catalog: str, logger: object = None) -> None:
        self.catalog = catalog
        self.calls: list[tuple[Any, ...]] = []
        _RecordingSink.instances.append(self)

    @property
    def write_failures(self) -> int:
        return 0

    def ensure_tables(self) -> None:
        # PREFLIGHT's method (9 tables + `CREATE OR REPLACE VIEW`). sb360 must NEVER call it — the
        # two tasks overlap, and concurrent CREATE-OR-REPLACE on one view is a metastore race.
        self.calls.append(("ensure_tables",))

    def ensure_own_table(self, worker_id: int) -> None:
        self.calls.append(("ensure_own_table", worker_id))

    def units_started(self, run_id: str, worker_id: int, units: list[WorkUnit]) -> None:
        for u in units:
            self.calls.append(("running", run_id, worker_id, u.provider, u.match_id, u.period))

    def unit_finished(
        self,
        run_id: str,
        worker_id: int,
        unit: WorkUnit,
        *,
        state: str,
        rows_written: int | None,
        error: str | None,
    ) -> None:
        self.calls.append((state, run_id, worker_id, unit.provider, unit.match_id, unit.period, rows_written))

    def flush_terminals(self) -> None:
        self.calls.append(("flush",))

    def slice_completed(self, run_id: str, worker_id: int) -> None:
        self.calls.append(("slice_completed", run_id, worker_id))


def _boom(*a: object, **k: object) -> Any:
    raise AssertionError("must not be called on this path")


def _drive_main_statsbomb(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ids: list[str],
    rows_by_match: dict[str, int] | None = None,
    run_id: str = "JOBRUN42",
    written: int = 0,
) -> _RecordingSink:
    """Run ``main_statsbomb`` with every Spark seam faked; return the sink it constructed."""
    import ingestion.action_context as ac
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs
    import ingestion.guards as guards

    _RecordingSink.instances.clear()
    ns = argparse.Namespace(catalog="cat", schema="bronze", max_units="", run_id=run_id)
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(guards, "ensure_table", lambda *a, **k: None)
    monkeypatch.setattr(q, "DeltaUnitEventSink", _RecordingSink)
    monkeypatch.setattr(ac, "_find_sb360_new_ids", lambda *a, **k: list(ids))
    if ids:
        monkeypatch.setattr(ac, "_load_xt_grid_from_delta", lambda *a, **k: ([[0.0]], 1, 1))
        monkeypatch.setattr(ac, "_process_statsbomb_matches", lambda *a, **k: written)
        monkeypatch.setattr(ac, "_sb360_rows_by_match", lambda *a, **k: dict(rows_by_match or {}))
    else:
        # the no-work path must not touch the xT grid or the cogroup job at all
        monkeypatch.setattr(ac, "_load_xt_grid_from_delta", _boom)
        monkeypatch.setattr(ac, "_process_statsbomb_matches", _boom)
        monkeypatch.setattr(ac, "_sb360_rows_by_match", _boom)

    ac.main_statsbomb()

    assert len(_RecordingSink.instances) == 1, "main_statsbomb must construct exactly one event sink"
    return _RecordingSink.instances[0]


# ── W3 / X1 / Y2: the shared sentinel ──────────────────────────────────


def test_sb360_emits_the_SHARED_sentinel_worker_id(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802 -- W3
    """W3 — the PRODUCER half of the contract (the consumer half is the gate's rule 0).

    If sb360 emitted ``worker_id = 0`` (the obvious default), the gate would look for the sentinel,
    find none, and return DRAIN_FAILED on EVERY run — a gate that cries wolf permanently, from day
    one. Every sb360 event — ``running``, terminal, ``slice_completed`` — carries the sentinel.
    """
    sink = _drive_main_statsbomb(monkeypatch, ids=["3788741", "3788746"], rows_by_match={"3788741": 12})

    events = [c for c in sink.calls if c[0] not in ("ensure_tables", "ensure_own_table", "flush")]
    assert events, "sb360 emitted no events at all"
    for call in events:
        assert call[2] == SB360_WORKER_ID, f"sb360 event {call[0]!r} did not carry the sentinel worker id"
    assert SB360_WORKER_ID == -1


def test_sb360_sentinel_is_IMPORTED_from_analytics_not_REDEFINED() -> None:  # noqa: N802 -- X1/Y2
    """X1/Y2 — the sentinel's HOME is load-bearing, and a redefinition is how the drift returns.

    The pure gate lives in ``analytics`` and ``analytics`` CANNOT import ``ingestion``
    (.importlinter ``analytics-isolation``). So the constant must live in
    ``analytics.action_context.drain``, which BOTH sides can read. A producer-side redefinition
    (``SB360_WORKER_ID = -1`` in ingestion) type-checks, passes every behavioural test, and silently
    re-opens the drift W3 was raised to kill.
    """
    from ingestion import action_context as ac

    assert ac.SB360_WORKER_ID is SB360_WORKER_ID
    assert _sentinel_redefinitions(Path(ac.__file__).read_text(encoding="utf-8")) == []


def _sentinel_redefinitions(src: str) -> list[str]:
    """Every module-level ASSIGNMENT to ``SB360_WORKER_ID`` (it must only ever be IMPORTED)."""
    return [
        f"line {node.lineno}: SB360_WORKER_ID is ASSIGNED, not imported"
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name) and t.id == "SB360_WORKER_ID"
    ]


def test_sentinel_guard_FAILS_on_a_planted_redefinition() -> None:  # noqa: N802 -- §0b
    """§0b: plant the exact defect (the producer defines its own ``-1``) and prove the guard fires."""
    planted = "SB360_WORKER_ID = -1\n\n\ndef main_statsbomb() -> None:\n    pass\n"
    assert _sentinel_redefinitions(planted)
    assert _sentinel_redefinitions("from analytics.action_context.drain import SB360_WORKER_ID\n") == []


# ── the three event kinds ──────────────────────────────────────────────


def test_sb360_emits_running_terminal_and_slice_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``running`` per DISCOVERED match (``period`` NULL — sb360 is match-grain, it exits the
    per-period drain), a terminal per match, and exactly one ``slice_completed``.

    ORDER is the guarantee: every ``running`` precedes the cogroup job, so a job that OOMs leaves
    its in-flight matches visible (``running``, no terminal) rather than invisible.
    """
    ids = ["3788741", "3788746"]
    sink = _drive_main_statsbomb(monkeypatch, ids=ids, rows_by_match={"3788741": 12, "3788746": 34}, written=46)

    running = [c for c in sink.calls if c[0] == "running"]
    assert [(c[3], c[4], c[5]) for c in running] == [("statsbomb", "3788741", None), ("statsbomb", "3788746", None)]

    terminals = [c for c in sink.calls if c[0] == "succeeded"]
    assert [(c[4], c[6]) for c in terminals] == [("3788741", 12), ("3788746", 34)]

    # every running precedes every terminal; the slice closes exactly once, after the flush
    assert max(sink.calls.index(c) for c in running) < min(sink.calls.index(c) for c in terminals)
    assert sink.calls[-2:] == [("flush",), ("slice_completed", "JOBRUN42", SB360_WORKER_ID)]
    assert [c for c in sink.calls if c[0] == "slice_completed"] == [("slice_completed", "JOBRUN42", SB360_WORKER_ID)]


def test_sb360_terminal_is_derived_from_what_LANDED_not_from_intent() -> None:  # noqa: N802
    """The driver never observes per-match completion (ONE cogroup job, ADR-058), so terminals are
    derived POST-HOC from the rows that actually landed.

    A discovered match that wrote ZERO rows (the UDF's empty / no-frame path) still gets a terminal:
    ``succeeded, rows_written=0``. It must NOT be left terminal-less — a missing terminal means
    "never ran", and the gate RAISES on that (the silent-skip class).
    """
    from ingestion.action_context import _sb360_terminals

    assert _sb360_terminals(["a", "b", "c"], {"a": 12, "c": 0}) == [("a", 12), ("b", 0), ("c", 0)]


# ── X2: the no-work run (THE COMMON DAILY CASE) ────────────────────────


def test_sb360_with_NO_matches_STILL_emits_slice_completed(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """X2 — this is P4, for sb360, and it is the COMMON case.

    ``main_statsbomb`` early-returns on ``if not ids:`` BEFORE any processing. On any run with no
    new sb360 matches — the ordinary daily shape — statsbomb would emit NOTHING: no ``running``, no
    terminals, no ``slice_completed``. The gate's rule 0 expects the sentinel's ``slice_completed``
    unconditionally (the sb360 task is unconditional in the DAG), so it would return DRAIN_FAILED
    **every quiet day** — crying wolf on the most common run there is, which is the muting failure
    P4 was raised to prevent.
    """
    sink = _drive_main_statsbomb(monkeypatch, ids=[])

    assert sink.calls == [
        ("ensure_own_table", SB360_WORKER_ID),
        ("slice_completed", "JOBRUN42", SB360_WORKER_ID),
    ]


def test_sb360_slice_completed_is_emitted_BEFORE_the_early_return() -> None:  # noqa: N802 -- X2
    """Source-level backstop for X2: the emit must sit BEFORE the ``return`` in the short-circuit.

    An emit placed after the ``return`` is dead code that no fake can catch — and this exact
    inversion is what a future "tidy-up" edit would produce.
    """
    from ingestion import action_context as ac

    src = inspect.getsource(ac.main_statsbomb)
    short_circuit = src[src.index("if not ids:") :]
    short_circuit = short_circuit[: short_circuit.index("\n", short_circuit.index("return"))]
    assert "slice_completed" in short_circuit
    assert short_circuit.index("slice_completed") < short_circuit.rindex("return")


def test_sb360_creates_ONLY_ITS_OWN_table_and_NEVER_the_view(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """THE CONCURRENCY RACE (2026-07-13 review). The design claimed "preflight is a SINGLE writer" —
    it is not. sb360 does NOT depend on ``preflight_action_context`` (asserted below, from the real
    main.tf), so the two tasks OVERLAP; and ``ensure_tables()`` issues, besides 9 idempotent
    ``CREATE TABLE IF NOT EXISTS``, one ``CREATE OR REPLACE VIEW``. Two tasks concurrently replacing
    the SAME view is a metastore race that can throw — and both tasks run at ``max_retries = 0``.

    sb360 still cannot inherit preflight's creation (no dependency ⇒ no ordering) and its
    ``slice_completed`` is FAIL-LOUD, so it must create SOMETHING: exactly its own ``_sb360`` table,
    via the narrow ``ensure_own_table``. Preflight owns the view, alone.
    """
    sink = _drive_main_statsbomb(monkeypatch, ids=[])

    assert sink.calls[0] == ("ensure_own_table", SB360_WORKER_ID), "sb360 must create its own table before it writes"
    assert not [c for c in sink.calls if c[0] == "ensure_tables"], (
        "sb360 called ensure_tables() -- that issues CREATE OR REPLACE VIEW, and this task runs "
        "CONCURRENTLY with preflight (no depends_on). Use ensure_own_table()."
    )

    # The premise, asserted rather than assumed (§0c question 4). If sb360 ever gains a preflight
    # dependency, the race disappears and this split can be re-derived.
    upstream = _sb360_depends_on(_MAIN_TF.read_text(encoding="utf-8"))
    assert upstream, "the sb360 task has no depends_on — the parse is wrong, not the DAG"
    assert "preflight_action_context" not in upstream


def test_ensure_own_table_touches_ONLY_that_workers_table_and_no_view() -> None:  # noqa: N802
    """The adapter half of the same contract, at the SQL level: the narrow method must not emit a
    view DDL, and must not create the other eight workers' tables."""
    from ingestion.action_context_queue import DeltaUnitEventSink, event_table_for_worker

    class _SqlSpy:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> None:
            self.statements.append(statement)

    spy = _SqlSpy()
    DeltaUnitEventSink(spy, "cat").ensure_own_table(SB360_WORKER_ID)  # type: ignore[arg-type]

    joined = " ".join(spy.statements)
    assert "CREATE OR REPLACE VIEW" not in joined.upper(), "ensure_own_table must NEVER touch the view"
    assert event_table_for_worker(SB360_WORKER_ID) in joined
    assert event_table_for_worker(0) not in joined, "ensure_own_table must create ONLY its own table"

    # The negative control: preflight's method DOES create the view (and every table).
    full = _SqlSpy()
    DeltaUnitEventSink(full, "cat").ensure_tables()  # type: ignore[arg-type]
    assert "CREATE OR REPLACE VIEW" in " ".join(full.statements).upper()


# ── Y1 / X2b: terraform passes --run-id AND argparse accepts it (a PAIR) ─


def _sb360_task_block(tf: str) -> str:
    start = tf.index('task_key        = "compute_action_context_statsbomb"')
    return tf[start : tf.index("\n  # ──", start)]


def _sb360_depends_on(tf: str) -> set[str]:
    """The sb360 task's REAL upstreams — parsed from its ``depends_on`` blocks, not grepped from the
    block's text (a comment mentioning a task name is not a dependency)."""
    block = _sb360_task_block(tf)
    return set(_DEPENDS_ON_RE.findall(block))


def test_terraform_sb360_task_passes_run_id() -> None:
    """X2b — sb360's events must carry the run the gate verifies. Its task previously took only
    ``--catalog``/``--schema``/``--max-units``.

    ``{{job.run_id}}`` (NOT the preflight task value): sb360 does NOT depend on
    ``preflight_action_context``, and a Databricks task value is only resolvable from an UPSTREAM
    task. ``{{job.run_id}}`` is the identical value — it is exactly what preflight itself is passed
    (main.tf), and ``_resolve_run_id`` returns it verbatim.
    """
    block = _sb360_task_block(_MAIN_TF.read_text(encoding="utf-8"))
    assert '"--run-id", "{{job.run_id}}"' in block


def test_main_statsbomb_ACCEPTS_the_flag_terraform_passes(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802 -- Y1
    """Y1 — terraform passing a flag and Python accepting it are a PRODUCER/CONSUMER PAIR (§0a).

    Adding the flag to main.tf alone makes the task die on ``unrecognized arguments`` on the FIRST
    run after apply, and the gate then piles DRAIN_FAILED on top of it. So: every ``--flag`` in the
    sb360 task's terraform parameters must be declared by ``main_statsbomb``'s argparse.
    """
    import ingestion.action_context as ac

    captured: list[list[tuple[str, dict[str, Any]]]] = []

    def _capture(description: str, extra_args: list[tuple[str, dict[str, Any]]] | None = None) -> argparse.Namespace:
        captured.append(list(extra_args or []))
        return argparse.Namespace(catalog="cat", schema="bronze", max_units="", run_id="R")

    monkeypatch.setattr(ac, "parse_ingestion_args", _capture)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(ac, "_find_sb360_new_ids", lambda *a, **k: [])
    monkeypatch.setattr(ac, "_load_xt_grid_from_delta", _boom)
    import ingestion.action_context_queue as q
    import ingestion.bootstrap as bs
    import ingestion.guards as guards

    _RecordingSink.instances.clear()
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(guards, "ensure_table", lambda *a, **k: None)
    monkeypatch.setattr(q, "DeltaUnitEventSink", _RecordingSink)

    ac.main_statsbomb()

    declared = {flag for flag, _kw in captured[0]} | {"--catalog", "--schema"}  # the latter two are always added
    tf_flags = set(_TF_FLAG_RE.findall(_sb360_task_block(_MAIN_TF.read_text(encoding="utf-8"))))
    assert tf_flags, "the sb360 terraform task passes no flags — the parity check would be vacuous"
    assert tf_flags <= declared, f"terraform passes flags main_statsbomb does not accept: {sorted(tf_flags - declared)}"


def test_sb360_refuses_an_EMPTY_run_id(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """Fail LOUD, not quietly-under-a-different-run-id: sb360's events are only evidence if they
    carry the run the gate verifies. An empty ``--run-id`` (a terraform regression) would otherwise
    file them under a run nobody reads, and the gate's DRAIN_FAILED *report* would not raise."""
    with pytest.raises(SystemExit, match="run-id"):
        _drive_main_statsbomb(monkeypatch, ids=[], run_id="")


# ── §0d: every event write goes through the append-guarded sink ─────────


def test_sb360_event_writes_go_through_the_APPEND_guarded_sink() -> None:  # noqa: N802 -- §0d
    """§0d — ``write_delta_table`` DEFAULTS to ``mode="overwrite"`` (utils.py). The spike proved the
    consequence: 392 default-mode "appends" left ONE row in the table, which would make the gate
    accuse a healthy drain on every run.

    ``DeltaUnitEventSink`` passes ``mode="append"`` and is guarded (test_unit_event_sink.py). The
    sb360-side hole is a DIFFERENT one: the sb360 path could write its events DIRECTLY, bypassing
    the sink and its guard. So: ``main_statsbomb`` makes NO ``write_delta_table`` call at all — its
    only persistence seam is the sink — and the sink's writes are append.
    """
    from ingestion.action_context import main_statsbomb
    from ingestion.action_context_queue import DeltaUnitEventSink

    assert write_delta_table_calls(inspect.getsource(main_statsbomb)) == [], (
        "main_statsbomb writes Delta directly — every event write must go through DeltaUnitEventSink, "
        "whose mode='append' is guarded (§0d: the default is a silent overwrite)"
    )
    sink_src = inspect.getsource(DeltaUnitEventSink)
    assert write_delta_table_calls(sink_src), "guard is vacuous — the sink makes no write_delta_table call"
    assert writes_without_append(sink_src) == []


def test_append_guard_FAILS_on_a_planted_direct_event_write() -> None:  # noqa: N802 -- §0b
    """§0b: plant the natural (silently destructive) sb360-side write and prove the guard rejects it.

    This is the shape a future edit takes when it wants "just one more event" without plumbing the
    sink: a bare ``write_delta_table(...)`` that takes the ``mode="overwrite"`` default and wipes
    the log.
    """
    planted = (
        "def main_statsbomb() -> None:\n"
        "    sdf = spark.createDataFrame(rows, schema=_event_struct())\n"
        "    write_delta_table(sdf, catalog, 'observability', 'action_context_unit_events_sb360')\n"
    )
    assert write_delta_table_calls(planted), "guard missed a direct Delta write in the sb360 path"
    assert writes_without_append(planted), "guard missed the silently-destructive mode='overwrite' DEFAULT"
