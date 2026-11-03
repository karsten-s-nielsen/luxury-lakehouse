"""The dependency view: run-step layer edges plus the dbt silver→gold sub-DAG (spec §7.4, test 11; plan Task 4)."""

from __future__ import annotations

from pathlib import Path

from lakehouse.observability import (
    TaskRow,
    dbt_manifest_key,
    dbt_model_dag,
    dependency_view,
    write_task_rows,
)

_FIXTURE = Path(__file__).parent.parent / "fixtures" / "dbt" / "manifest_min.json"


class _FakeStorage:
    """A dict-backed Storage good enough for `write_task_rows`/`read_task_rows` and the manifest read."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put(self, root: str, key: str, data: bytes | Path) -> None:
        self.objects[(root, key)] = data if isinstance(data, bytes) else Path(data).read_bytes()

    def get(self, root: str, key: str) -> bytes:
        return self.objects[(root, key)]

    def exists(self, root: str, key: str) -> bool:
        return (root, key) in self.objects


def _row(task: str, layer: str, started_at: str) -> TaskRow:
    return TaskRow(
        run_id="r1",
        task=task,
        parent_task="lakehouse.run",
        layer=layer,
        started_at=started_at,
        duration_seconds=1.0,
        row_count=1,
        state="ok",
        visibility="public",
        execution_context="local",
        flavour=None,
        cost_usd=0.0,
        cost_error=None,
    )


def test_dbt_model_dag_silver_to_gold() -> None:
    assert dbt_model_dag(_FIXTURE) == [("stg_statsbomb__matches", "dim_matches")]


def test_dependency_topology() -> None:
    storage = _FakeStorage()
    rows = [
        _row("lakehouse.run.write", "bronze", "2026-10-28T00:00:00+00:00"),
        _row("lakehouse.run.resolve", "resolution", "2026-10-28T00:00:01+00:00"),
        _row("lakehouse.run.dbt", "dbt", "2026-10-28T00:00:02+00:00"),
    ]
    write_task_rows(storage, "r1", rows)
    storage.put("lake", dbt_manifest_key("r1"), _FIXTURE.read_bytes())

    view = dependency_view(storage, "r1")
    assert view.run_edges == [("bronze", "resolution"), ("resolution", "dbt")]
    assert view.dbt_edges == [("stg_statsbomb__matches", "dim_matches")]
    assert view.note is None


def test_dependency_topology_missing_manifest() -> None:
    storage = _FakeStorage()
    write_task_rows(
        storage,
        "r2",
        [
            _row("lakehouse.run.write", "bronze", "2026-10-28T00:00:00+00:00"),
            _row("lakehouse.run.resolve", "resolution", "2026-10-28T00:00:01+00:00"),
        ],
    )
    # No manifest retained (no dbt step): empty sub-DAG + a visible note, never a crash (Review Focus 5).
    view = dependency_view(storage, "r2")
    assert view.run_edges == [("bronze", "resolution")]
    assert view.dbt_edges == []
    assert view.note is not None and "manifest" in view.note
