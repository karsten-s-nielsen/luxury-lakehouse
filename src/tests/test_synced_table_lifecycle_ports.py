"""The thin hexagonal ports + heal outcome enum + HealPorts composition (spec M1 / review B)."""

from __future__ import annotations

from ingestion.synced_table_heal import HealOutcome, HealPorts
from ingestion.synced_table_lifecycle import (
    PostgresGhostPort,
    SyncedTableReaderPort,
    SyncedTableWriterPort,
    WarehousePort,
)


def test_reader_port_is_read_only() -> None:
    for m in ("get_synced_table_status", "get_pipeline_id", "latest_failed_events"):
        assert hasattr(SyncedTableReaderPort, m), m
    # The reader port carries NO destructive op — this is what gives detection its type guarantee (P2).
    for destructive in ("sdk_delete", "create_synced_table", "drop_pg_ghost", "ensure_cdf"):
        assert not hasattr(SyncedTableReaderPort, destructive), destructive


def test_writer_port_methods() -> None:
    for m in ("create_synced_table", "sdk_delete", "trigger_refresh", "wait_until_online"):
        assert hasattr(SyncedTableWriterPort, m), m


def test_ghost_and_warehouse_ports() -> None:
    assert hasattr(PostgresGhostPort, "drop_pg_ghost")
    assert hasattr(WarehousePort, "ensure_cdf")


def test_heal_ports_composition() -> None:
    assert {f for f in HealPorts.__dataclass_fields__} == {"reader", "writer", "ghost", "warehouse"}


def test_heal_outcome_members() -> None:
    assert {o.name for o in HealOutcome} == {"HEALED", "UNHEALABLE", "HEAL_FAILED", "SKIPPED_PREFLIGHT"}


def test_wait_until_online_returns_early_on_terminal_failure_states(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """wait_until_online must return IMMEDIATELY on terminal failure states — including
    SYNCED_TABLE_ONLINE_PIPELINE_FAILED, the strand signature (table still serving, DLT given up).
    Before 2026-06-10 that state was missing from the early-return set, so a genuine strand burned
    the caller's entire timeout (600-1800s) before being reported, even though DLT had already
    settled on failure ~13 min in (ADR-041 re-characterisation)."""
    from ingestion.synced_table_lifecycle import SdkWriterAdapter

    for terminal in ("SYNCED_TABLE_OFFLINE", "SYNCED_TABLE_OFFLINE_FAILED", "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"):
        adapter = SdkWriterAdapter.__new__(SdkWriterAdapter)  # no real WorkspaceClient needed
        monkeypatch.setattr(adapter, "_status", lambda fqn, s=terminal: s, raising=False)
        sleeps: list[int] = []
        monkeypatch.setattr("ingestion.synced_table_lifecycle.time.sleep", lambda s, _sink=sleeps: _sink.append(s))
        assert adapter.wait_until_online("cat.sch.tbl", timeout_s=600) == terminal
        assert sleeps == [], f"{terminal}: expected immediate return, but the wait slept {sleeps}"


def test_wait_until_online_returns_online_on_settled_state(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """The settled state maps to the canonical SYNCED_TABLE_ONLINE return value."""
    from ingestion.synced_table_lifecycle import SdkWriterAdapter

    adapter = SdkWriterAdapter.__new__(SdkWriterAdapter)
    monkeypatch.setattr(adapter, "_status", lambda fqn: "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE", raising=False)
    assert adapter.wait_until_online("cat.sch.tbl", timeout_s=600) == "SYNCED_TABLE_ONLINE"


def test_wait_until_online_survives_transient_status_errors(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A poll loop with minutes of budget must NOT die because one poll failed.

    2026-06-10: a single SDK DeadlineExceeded from the control-plane GET killed the heal e2e in
    setup (and the same transient class failed the maintenance pipeline-id lookup earlier the same
    day). The wait must record the unreadable poll and keep polling; timeout_s still bounds it.
    """
    from ingestion.synced_table_lifecycle import SdkWriterAdapter

    adapter = SdkWriterAdapter.__new__(SdkWriterAdapter)
    calls = {"n": 0}

    def _flaky_status(fqn: str) -> str:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise TimeoutError("request failed")  # stand-in for databricks DeadlineExceeded
        return "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"

    monkeypatch.setattr(adapter, "_status", _flaky_status, raising=False)
    monkeypatch.setattr("ingestion.synced_table_lifecycle.time.sleep", lambda s: None)
    assert adapter.wait_until_online("cat.sch.tbl", timeout_s=600) == "SYNCED_TABLE_ONLINE"
    assert calls["n"] == 3  # two failed polls tolerated, third succeeded


# ---------------------------------------------------------------------------
# latest_failed_events: the in-flight-retry blindness (2026-07-28)
# ---------------------------------------------------------------------------
#
# DLT retries a failed update with growing backoff. Scoped to the LITERALLY newest update, the
# classifier saw a RUNNING retry with no errors and reported "no strand" while the strand was real
# — turning a detected strand into SKIPPED_PREFLIGHT (heal-e2e run 30384832625). In production
# detect and heal are separate jobs minutes apart, so the window is far wider than the e2e's.


class _FakeEvent:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def as_dict(self) -> dict:
        return self._payload


def _evt(update_id: str, *, error: bool) -> _FakeEvent:
    payload: dict = {"origin": {"update_id": update_id}, "event_type": "update_progress"}
    if error:
        payload["error"] = {"exceptions": [{"message": "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE XXKST"}]}
    return _FakeEvent(payload)


def _reader_with(events: list, states: dict[str, str]):  # type: ignore[no-untyped-def]
    """SdkReaderAdapter over fake events + a fixed update_id -> state map."""
    from types import SimpleNamespace

    from ingestion.synced_table_lifecycle import SdkReaderAdapter

    class _Pipelines:
        def list_pipeline_events(self, pipeline_id: str, max_results: int = 250) -> list:
            return events

        def get_update(self, pipeline_id: str, update_id: str):  # type: ignore[no-untyped-def]
            state = states[update_id]
            if state == "__raise__":
                raise RuntimeError("update unreadable")
            return SimpleNamespace(update=SimpleNamespace(state=state))

    adapter = SdkReaderAdapter.__new__(SdkReaderAdapter)
    # Private attribute by design: the adapter's whole job is to wrap a WorkspaceClient, so testing
    # it offline means substituting exactly that. object.__setattr__ keeps pyright out of it.
    object.__setattr__(adapter, "_ws", SimpleNamespace(pipelines=_Pipelines()))
    return adapter


def test_latest_failed_events_looks_past_an_in_flight_retry() -> None:
    """THE regression. A RUNNING retry must not mask the failure it is retrying."""
    from ingestion.synced_table_heal import is_checkpoint_mismatch_failure

    events = [_evt("u2", error=False), _evt("u1", error=True)]  # newest-first
    reader = _reader_with(events, {"u2": "RUNNING", "u1": "FAILED"})

    assert reader.latest_failed_events("p") != [], "in-flight retry masked the stranded update"
    assert is_checkpoint_mismatch_failure(reader, "p") is True


def test_latest_failed_events_does_not_resurrect_a_stale_failure() -> None:
    """P9 preserved: a SUCCESSFUL newest update stops the walk — the older failure stays buried."""
    events = [_evt("u2", error=False), _evt("u1", error=True)]
    reader = _reader_with(events, {"u2": "COMPLETED", "u1": "FAILED"})

    assert reader.latest_failed_events("p") == [], "walked past a concluded update into stale history"


def test_latest_failed_events_reports_a_failed_newest_update() -> None:
    """The ordinary case must be unchanged."""
    events = [_evt("u1", error=True)]
    reader = _reader_with(events, {"u1": "FAILED"})

    assert reader.latest_failed_events("p") != []


def test_latest_failed_events_treats_unreadable_state_as_concluded() -> None:
    """Fail-safe: if the state cannot be read, behave exactly as before the amendment (use that
    update). Never MORE destructive on doubt."""
    events = [_evt("u2", error=False), _evt("u1", error=True)]
    reader = _reader_with(events, {"u2": "__raise__", "u1": "FAILED"})

    assert reader.latest_failed_events("p") == [], "unreadable state must not widen the search"


def test_latest_failed_events_treats_an_unknown_state_as_concluded() -> None:
    """Fail-safe on UNRECOGNISED states, not just unreadable ones.

    The first cut of this amendment used a terminal-state denylist, so anything it did not
    recognise counted as in-flight and got skipped. Under a MagicMock every update looked
    in-flight and the walk fell off the end returning [] — breaking the pre-existing P9 scoping
    test. A future SDK enum value would have done the same thing in production, silently widening
    a search that gates a destructive heal. The allowlist inverts that.
    """
    events = [_evt("u2", error=True), _evt("u1", error=True)]
    reader = _reader_with(events, {"u2": "SOME_FUTURE_SDK_STATE", "u1": "FAILED"})

    out = reader.latest_failed_events("p")
    assert {e["origin"]["update_id"] for e in out} == {"u2"}, "unknown state must not be skipped"


def test_latest_failed_events_bounds_the_walk_back() -> None:
    """A pathological stream of in-flight updates must not march back through history."""
    from ingestion.synced_table_lifecycle import _MAX_INFLIGHT_UPDATES_SKIPPED

    n = _MAX_INFLIGHT_UPDATES_SKIPPED + 2
    events = [_evt(f"u{i}", error=False) for i in range(n)] + [_evt("old", error=True)]
    states = {f"u{i}": "RUNNING" for i in range(n)}
    states["old"] = "FAILED"
    reader = _reader_with(events, states)

    assert reader.latest_failed_events("p") == [], "walk was not bounded"
