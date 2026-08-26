"""Task 5 — the tracking-marts preflight skip-guard: PURE, CROSS-RUN, ``succeeded``-only (§3.1, G2).

"Done" = a unit has a ``succeeded`` terminal in ``tracking_marts_unit_events`` under ANY ``run_id`` (the
events read is deliberately NOT filtered to the current run — a per-run read would leave every unit
always-open and the chronic timeout would go unfixed silently). A ``failed`` / ``timed_out`` (or never-run)
unit stays OPEN.

The load-bearing DISCRIMINATING case is the cross-run one: a naive "unit A done => skip {B, C}" bug passes
a same-run test but fails when A's ``succeeded`` terminal is from a DIFFERENT run. The pure evidence reader
(``succeeded_keys_from_events``) takes ``(provider, match_id, period, state)`` rows WITHOUT ``run_id`` —
that absence IS the cross-run property.
"""

from __future__ import annotations

from analytics.action_context.work_unit import WorkUnit
from ingestion.tracking_marts_drain import (
    _N_TRACKING_MARTS_WORKERS,
    open_units,
    succeeded_keys_from_events,
)

_A = WorkUnit(provider="idsse", match_id="A", period=1)
_B = WorkUnit(provider="idsse", match_id="B", period=1)
_C = WorkUnit(provider="skillcorner", match_id="C", period=2)
_UNIVERSE = [_A, _B, _C]


def _key(u: WorkUnit) -> tuple[str, str, int | None]:
    return (u.provider, u.match_id, u.period)


def test_n_workers_is_eight() -> None:
    assert _N_TRACKING_MARTS_WORKERS == 8


def test_succeeded_terminal_under_a_DIFFERENT_run_is_still_done() -> None:  # noqa: N802
    """The load-bearing cross-run case. The evidence reader has NO ``run_id`` column, so a ``succeeded``
    terminal from a prior run marks the unit done — exactly what a per-run bug would fail to see."""
    # A succeeded (in some prior run), B FAILED, C never ran. The reader sees no run_id at all.
    events = [
        ("idsse", "A", 1, "succeeded"),
        ("idsse", "B", 1, "failed"),
    ]
    done = succeeded_keys_from_events(events)
    assert done == frozenset({("idsse", "A", 1)})

    still_open = open_units(_UNIVERSE, done, full=False)
    assert _key(_A) not in {_key(u) for u in still_open}  # A skipped (succeeded — cross-run)
    assert {_key(u) for u in still_open} == {_key(_B), _key(_C)}  # B (failed) + C (never ran) stay OPEN


def test_failed_or_timed_out_only_unit_stays_open() -> None:
    """Only ``succeeded`` counts as done. A unit whose ONLY terminals are ``failed`` / ``timed_out`` is
    re-enumerated (it wrote zero rows and never will until it succeeds)."""
    events = [
        ("idsse", "A", 1, "failed"),
        ("idsse", "A", 1, "timed_out"),
        ("skillcorner", "C", 2, "timed_out"),
    ]
    done = succeeded_keys_from_events(events)
    assert done == frozenset()  # nothing succeeded
    assert {_key(u) for u in open_units(_UNIVERSE, done, full=False)} == {_key(_A), _key(_B), _key(_C)}


def test_full_returns_the_whole_universe_regardless_of_terminals() -> None:
    """``--full`` bypasses the subtraction entirely — every unit is re-enumerated even if it succeeded."""
    done = succeeded_keys_from_events([("idsse", "A", 1, "succeeded"), ("idsse", "B", 1, "succeeded")])
    assert done == frozenset({("idsse", "A", 1), ("idsse", "B", 1)})
    assert open_units(_UNIVERSE, done, full=True) == _UNIVERSE  # all three, done-set ignored


def test_running_events_do_not_mark_a_unit_done() -> None:
    """A ``running`` event (unit started, no terminal) is NOT done — an OOM-killed unit must re-enumerate."""
    done = succeeded_keys_from_events([("idsse", "A", 1, "running")])
    assert done == frozenset()
    assert _key(_A) in {_key(u) for u in open_units(_UNIVERSE, done, full=False)}
