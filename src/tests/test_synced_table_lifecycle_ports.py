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
