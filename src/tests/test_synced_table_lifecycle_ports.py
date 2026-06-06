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
