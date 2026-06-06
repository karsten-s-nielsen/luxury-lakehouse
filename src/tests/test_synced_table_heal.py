"""verify-before-destroy heal_synced_table over composed thin ports (spec H1/L1/L5/L7/M4)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from ingestion.synced_table_heal import HealOutcome, HealPorts, heal_synced_table, run_heal_pass


@dataclass
class _FakePorts:
    """One fake implementing all four ports, with per-sub-op failure injection (L5)."""

    fail_on: set[str] = field(default_factory=set)
    create_already_exists: bool = False
    mismatch_present: bool = True
    online_after: bool = True
    calls: list[str] = field(default_factory=list)

    def _maybe_raise(self, op: str) -> None:
        if op in self.fail_on:
            raise RuntimeError(f"{op} failed")

    # ReaderPort
    def latest_failed_events(self, pipeline_id: str) -> list:
        return [{"error": {"exceptions": [{"message": "SQLSTATE: XXKST"}]}}] if self.mismatch_present else []

    def get_pipeline_id(self, fqn: str) -> str:
        return "pid"

    def get_synced_table_status(self, fqn: str) -> str:
        return "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"

    # WarehousePort
    def ensure_cdf(self, source_fqn: str) -> None:
        self.calls.append("ensure_cdf")
        self._maybe_raise("ensure_cdf")

    # WriterPort
    def sdk_delete(self, fqn: str) -> bool:
        self.calls.append("sdk_delete")
        self._maybe_raise("sdk_delete")
        return True

    def create_synced_table(self, config, catalog: str, schema: str) -> None:
        self.calls.append("create")
        if self.create_already_exists:
            raise RuntimeError("Table already exists")
        self._maybe_raise("create")

    def trigger_refresh(self, pipeline_id: str) -> None:
        self.calls.append("trigger")

    def wait_until_online(self, fqn: str, timeout_s: int = 1800) -> str:
        return "SYNCED_TABLE_ONLINE" if self.online_after else "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"

    # PostgresGhostPort
    def drop_pg_ghost(self, schema: str, table: str) -> None:
        self.calls.append("drop_pg_ghost")
        self._maybe_raise("drop_pg_ghost")


def _ports(f: _FakePorts) -> HealPorts:
    return HealPorts(reader=f, writer=f, ghost=f, warehouse=f)


@dataclass(frozen=True)
class _Cfg:
    name: str = "fct_x_synced"
    source_table: str = "fct_x"
    scheduling_policy: str = "TRIGGERED"
    schema_override: str | None = None


def test_happy_path_heals_in_order() -> None:
    f = _FakePorts()
    assert heal_synced_table(_ports(f), _Cfg(), "cat", "dev_gold") == HealOutcome.HEALED
    assert f.calls == ["ensure_cdf", "sdk_delete", "drop_pg_ghost", "create", "trigger"]


def test_preflight_abort_does_not_delete() -> None:
    f = _FakePorts(mismatch_present=False)
    assert heal_synced_table(_ports(f), _Cfg(), "cat", "dev_gold") == HealOutcome.SKIPPED_PREFLIGHT
    assert "sdk_delete" not in f.calls and "drop_pg_ghost" not in f.calls


def test_already_exists_is_heal_failed_not_healed() -> None:
    f = _FakePorts(create_already_exists=True)
    assert heal_synced_table(_ports(f), _Cfg(), "cat", "dev_gold") == HealOutcome.HEAL_FAILED


def test_ghost_drop_failure_after_delete_is_heal_failed() -> None:
    f = _FakePorts(fail_on={"drop_pg_ghost"})
    assert heal_synced_table(_ports(f), _Cfg(), "cat", "dev_gold") == HealOutcome.HEAL_FAILED
    assert "sdk_delete" in f.calls  # delete happened; partial state surfaced (M4)


def test_not_online_after_is_heal_failed() -> None:
    f = _FakePorts(online_after=False)
    assert heal_synced_table(_ports(f), _Cfg(), "cat", "dev_gold") == HealOutcome.HEAL_FAILED


# ---------------------------------------------------------------------------------- run_heal_pass
class _FakeState:
    def __init__(self) -> None:
        self.healed: list[tuple[str, datetime]] = []

    def mark_healed(self, table_name: str, event_at: datetime) -> None:
        self.healed.append((table_name, event_at))


_NOW = datetime(2026, 6, 5, 12, 0, 0)
_CONFIGS = {"fct_x_synced": _Cfg()}


def test_heal_pass_healed_marks_state() -> None:
    f = _FakePorts()
    state = _FakeState()
    out = run_heal_pass(_ports(f), ["fct_x_synced"], _CONFIGS, "cat", "dev_gold", state, now=_NOW, enabled=True)
    assert out == {"fct_x_synced": HealOutcome.HEALED}
    assert state.healed == [("fct_x_synced", _NOW)]


def test_heal_pass_failed_logs_error_and_does_not_mark(caplog) -> None:
    f = _FakePorts(fail_on={"create"})
    state = _FakeState()
    with caplog.at_level(logging.ERROR):
        out = run_heal_pass(_ports(f), ["fct_x_synced"], _CONFIGS, "cat", "dev_gold", state, now=_NOW, enabled=True)
    assert out == {"fct_x_synced": HealOutcome.HEAL_FAILED}
    assert state.healed == []
    assert any(r.levelno >= logging.ERROR for r in caplog.records)


def test_heal_pass_killswitch_off_does_nothing() -> None:
    f = _FakePorts()
    state = _FakeState()
    out = run_heal_pass(_ports(f), ["fct_x_synced"], _CONFIGS, "cat", "dev_gold", state, now=_NOW, enabled=False)
    assert out == {}
    assert f.calls == []  # no destructive op when disabled
    assert state.healed == []


class _BrokenState:
    """mark_healed always fails — strand-state recording is best-effort, must NOT undo a real heal."""

    def mark_healed(self, table_name: str, event_at: datetime) -> None:
        raise RuntimeError("warehouse INSERT failed")


def test_heal_pass_mark_healed_failure_does_not_fail_a_real_heal(caplog) -> None:
    f = _FakePorts()  # heals successfully
    with caplog.at_level(logging.ERROR):
        out = run_heal_pass(
            _ports(f), ["fct_x_synced"], _CONFIGS, "cat", "dev_gold", _BrokenState(), now=_NOW, enabled=True
        )
    assert out == {"fct_x_synced": HealOutcome.HEALED}  # the heal stands; recording failure is non-fatal
    assert "drop_pg_ghost" in f.calls and "create" in f.calls  # the destructive heal actually ran
    assert any(r.levelno >= logging.ERROR for r in caplog.records)  # surfaced, not silently swallowed


def test_heal_entry_is_pyspark_free() -> None:
    """The heal runs in the GitHub Actions maintenance env (no pyspark). Regression guard for the
    2026-06-06 `ModuleNotFoundError: No module named 'pyspark'` crash: heal_synced_tables built its
    strand-state via SparkStrandStateBackend(get_spark_session()) which is unavailable there.

    AST-based (not raw-text) so the explanatory comments that mention 'pyspark' don't trip it."""
    import ast
    import pathlib

    import ingestion.heal_synced_tables as heal_entry

    tree = ast.parse(pathlib.Path(heal_entry.__file__).read_text(encoding="utf-8"))
    imports: set[str] = set()
    identifiers: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
        elif isinstance(node, ast.Name):
            identifiers.add(node.id)
        elif isinstance(node, ast.Attribute):
            identifiers.add(node.attr)

    assert not any("pyspark" in m for m in imports), "heal entry must not import pyspark (none in maintenance env)"
    assert "get_spark_session" not in identifiers, "heal entry must not call get_spark_session (no Spark in maint env)"
    assert "SparkStrandStateBackend" not in identifiers, "use WarehouseStrandStateBackend in the heal path"
