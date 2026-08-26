"""Task 1 — the drain adapters generalize over TWO orthogonal axes:

* ``drain_name`` — the table *name* namespace (``action_context`` default, ``tracking_marts`` new).
* ``include_sb360`` — the worker *topology* (does this drain have the unconditional sb360 extra worker?).

The AC drain must be byte-identical at the defaults; a no-sb360 drain (tracking-marts, the FIRST such
drain) must NOT get a phantom ``*_sb360`` event table (G1, review #1).
"""

from __future__ import annotations

from ingestion.drain_adapters import (
    DeltaUnitEventSink,
    DeltaWorkQueue,
    event_table_for_worker,
    event_table_names,
    event_view_sql,
)


def test_default_drain_name_is_action_context_unchanged() -> None:
    assert event_table_for_worker(0) == "action_context_unit_events_w0"
    names = event_table_names()  # AC default -> INCLUDES sb360 (byte-identical)
    assert names[0] == "action_context_unit_events_w0"
    assert names[-1].endswith("_sb360")
    assert "action_context_unit_events" in event_view_sql("cat")


def test_tracking_marts_drain_name_namespaces_tables_without_sb360() -> None:
    assert event_table_for_worker(0, drain_name="tracking_marts") == "tracking_marts_unit_events_w0"
    names = event_table_names(drain_name="tracking_marts", include_sb360=False)
    assert names[0] == "tracking_marts_unit_events_w0"
    assert not any(n.endswith("_sb360") for n in names)  # G1: no phantom sb360 worker
    view = event_view_sql("cat", drain_name="tracking_marts", include_sb360=False)
    assert "tracking_marts_unit_events" in view
    assert "_sb360" not in view


def test_workqueue_table_name_follows_drain_name() -> None:
    # __init__ does no Spark work — safe to construct with spark=None and read the FQN.
    assert DeltaWorkQueue(None, "cat")._table == "cat.observability.action_context_work_queue"  # type: ignore[arg-type]
    assert (
        DeltaWorkQueue(None, "cat", drain_name="tracking_marts")._table  # type: ignore[arg-type]
        == "cat.observability.tracking_marts_work_queue"
    )


def test_sink_stores_the_two_axes() -> None:
    sink = DeltaUnitEventSink(None, "cat", drain_name="tracking_marts", include_sb360=False)  # type: ignore[arg-type]
    assert sink._drain_name == "tracking_marts"
    assert sink._include_sb360 is False
    default = DeltaUnitEventSink(None, "cat")  # type: ignore[arg-type]
    assert default._drain_name == "action_context" and default._include_sb360 is True
