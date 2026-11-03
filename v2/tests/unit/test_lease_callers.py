"""Every `Lease.acquire` caller stops before any write when the run anchor or the acquisition marker cannot be stored
(T3 spec D5, owner decisions 12 and 13; plan round 2 Task 2, Reviewer B T3-PLAN-08).

The five callers are the run and the re-parse (`runs/run.py`), `sweep_now`, restore and relocate. A failed marker
store releases the won generation and raises `LeaseMarkerError`; the caller then writes no catalog, data, raw or run
object beyond the generation object and the anchor that `acquire` itself stored."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from lakekit import lake_roots
from runkit import FakeParser, active, save_settings

from lakehouse.catalog.cleanup import sweep_now
from lakehouse.catalog.relocate import relocate
from lakehouse.catalog.restore import restore
from lakehouse.config.model import load_defaults
from lakehouse.errors import LeaseMarkerError
from lakehouse.runs.run import reparse_matches, run_ingestion
from lakehouse.storage.keys import Root, RootMap
from lakehouse.storage.lease import anchor_key, lease_key, marker_key
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake


class _FailingMarker(LocalStorage):
    def __init__(self, roots: RootMap) -> None:
        super().__init__(roots)

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        if key == marker_key(1):
            raise OSError("no space left on device")
        return super().put_if_absent(root, key, data)


def _keys(storage: LocalStorage) -> set[str]:
    return {
        info.key
        for root in ("lake", "restricted")
        for prefix in ("runs/", "catalog/", "data/", "raw/")
        for info in storage.list(root, prefix)  # type: ignore[arg-type]
    }


def _run(storage: LocalStorage, work: Path) -> Any:
    return run_ingestion(storage=storage, trigger="cli", adapters={}, parsers={}, work_dir=work, dbt=None)


def _reparse(storage: LocalStorage, work: Path) -> Any:
    return reparse_matches(
        storage=storage, parsers={"idsse": FakeParser("idsse")}, target="all", work_dir=work, dbt=None
    )


def _sweep(storage: LocalStorage, work: Path) -> Any:
    return sweep_now(storage, settings=load_defaults("laptop"), work_dir=work)


def _restore(storage: LocalStorage, work: Path) -> Any:
    return restore(storage, target_seq=1, operator="ops", work_dir=work)


def _relocate(storage: LocalStorage, work: Path) -> Any:
    return relocate(storage, old_roots=storage.roots, new_roots=storage.roots, work_dir=work)


CALLERS: dict[str, Callable[[LocalStorage, Path], Any]] = {
    "run_ingestion": _run,
    "reparse_matches": _reparse,
    "sweep_now": _sweep,
    "restore": _restore,
    "relocate": _relocate,
}


@pytest.mark.parametrize("caller", sorted(CALLERS))
def test_failed_marker_store_stops_the_caller_before_any_write(tmp_path: Path, caller: str) -> None:
    roots = lake_roots(tmp_path)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "boot.json")
    save_settings(LocalStorage(roots), [active("idsse_public")])
    storage = _FailingMarker(roots)
    with pytest.raises(LeaseMarkerError, match="generation 1"):
        CALLERS[caller](storage, tmp_path / "work")
    assert _keys(storage) == {lease_key(1), anchor_key(1)}
