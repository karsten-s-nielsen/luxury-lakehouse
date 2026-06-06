"""maintain_synced_tables runs the heal step BEFORE grants + indexes (spec P5).

The order is load-bearing: a recreated synced table is a new PG table that needs grants + indexes
reapplied, so heal must precede them. A future refactor that reorders the passes would silently
reopen the grant/index outage — this test locks the sequence.
"""

from __future__ import annotations

import sys

import pytest


def test_heal_runs_before_grants_and_indexes(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.maintain_synced_tables as m

    order: list[str] = []

    def _record(name: str, cmd: list[str], dry_run: bool) -> tuple[bool, float]:
        order.append(name)
        return True, 0.0

    monkeypatch.setattr(m, "_run_step", _record)
    monkeypatch.setattr(sys, "argv", ["maintain_synced_tables", "--dry-run"])

    assert m.main() == 0
    assert "heal_synced_tables" in order, order
    heal_i = order.index("heal_synced_tables")
    assert heal_i < order.index("grant_synced_table_permissions"), order
    assert heal_i < order.index("create_indexes"), order
