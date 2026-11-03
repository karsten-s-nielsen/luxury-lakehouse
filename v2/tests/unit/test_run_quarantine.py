"""Quarantine-and-continue (robust public ingest): a corrupt, unparseable PUBLIC source artifact is a visible,
non-fatal `quarantined` outcome, so the run still completes. StatsBomb open-data ships NUL-corrupted 360 files
(e.g. match 3845506); one bad upstream file must not red the whole scheduled ingest, but it must be surfaced
(ops_errors warning + a `quarantined` match step), never silently dropped. A corrupt PRIVATE/pining artifact
stays fatal (owner-tier data is held to the strict bar)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from lakekit import LakeKit, lake_roots
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.parsers.base import ParsedMatch
from lakehouse.raw.archive import Delivery
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, exit_code, run_ingestion
from lakehouse.storage.local import LocalStorage


class CorruptParser(FakeParser):
    """Raises a real `json.JSONDecodeError` for any match whose events bytes contain `corrupt` (a malformed file)."""

    def parse(self, entry_: Any, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:  # type: ignore[override]
        if any(b"corrupt" in d.decoded for d in deliveries.values()):
            json.loads(b'{"x": ]')  # a genuine json.JSONDecodeError, as loads_exact raises on a corrupt source file
        return super().parse(entry_, deliveries, run_id=run_id)


def _kit(tmp_path: Path) -> LakeKit:
    roots = lake_roots(tmp_path)
    (tmp_path / "lake").mkdir(parents=True, exist_ok=True)
    (tmp_path / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LakeKit(base=tmp_path, roots=roots, storage=LocalStorage(roots))


def _adapter(module_id: str, channel: str, **files_for: bytes) -> FakeAdapter:
    entries = [entry(module_id, n, visibility="public") for n in files_for]  # type: ignore[arg-type]
    files = {(n, "events"): body for n, body in files_for.items()}
    return FakeAdapter(module_id, "gradientsports", channel, entries, files)  # type: ignore[arg-type]


def _run(kit: LakeKit, adapter: FakeAdapter) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},  # type: ignore[misc]
        parsers={adapter.provider: CorruptParser(adapter.provider)},  # type: ignore[misc]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )


def _rows(kit: LakeKit, table: str) -> list[dict[str, Any]]:
    with CatalogReader(kit.storage).session() as conn:
        return read_table(conn, table)


def test_public_corrupt_json_is_quarantined_and_the_run_completes(tmp_path: Path) -> None:
    kit = _kit(tmp_path)
    save_settings(kit.storage, [active("gradientsports_public")])
    result = _run(kit, _adapter("gradientsports_public", "public", good=b"line good", bad=b"corrupt"))

    assert result.status == "ok"
    assert exit_code(result) == 0
    assert result.counts.quarantined == 1
    assert result.counts.failed == 0
    assert result.counts.added == 1  # the good match still ingested

    steps = {s["native_match_id"]: s["outcome"] for s in _rows(kit, "ops_match_steps")}
    assert steps["bad"] == "quarantined"
    assert steps["good"] == "added"

    errors = _rows(kit, "ops_errors")
    quarantine = [e for e in errors if e["native_match_id"] == "bad"]
    assert len(quarantine) == 1
    assert quarantine[0]["kind"] == "source_corrupt"
    assert quarantine[0]["severity"] == "warning"


def test_pining_corrupt_json_still_fails_the_run(tmp_path: Path) -> None:
    kit = _kit(tmp_path)
    save_settings(kit.storage, [active("gradientsports_pining")])
    result = _run(kit, _adapter("gradientsports_pining", "pining", bad=b"corrupt"))

    assert result.status == "failed"
    assert exit_code(result) == 1
    assert result.counts.failed == 1
    assert result.counts.quarantined == 0
