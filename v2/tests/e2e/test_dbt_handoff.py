"""The dbt hand-off inside a run (spec §10.3, §17.2, §23.6 "dbt hand-off"; plan Task 23): dbt builds while the run
holds the lease, after resolution; the run then cleans up, writes its history and makes its run-end commit, and the
published version holds the dbt models."""

from __future__ import annotations

import json

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.runs.dbt import run_results_key
from lakehouse.runs.history import read_table
from lakehouse.runs.run import run_ingestion


def test_dbt_builds_during_the_run_and_the_run_commits(kit: LakeKit, capsys: pytest.CaptureFixture[str]) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    adapter = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"idsse_public": adapter},
        parsers={"idsse": FakeParser("idsse")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )
    assert result.status == "ok", result.errors
    steps = [json.loads(line)["msg"] for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    run_steps = [s for s in steps if s in ("lakehouse.run.resolve", "lakehouse.run.dbt", "lakehouse.run.cleanup")]
    assert run_steps == ["lakehouse.run.resolve", "lakehouse.run.dbt", "lakehouse.run.cleanup"]
    assert read_manifest(kit.storage, read_current(kit.storage).seq).kind == "run_end"
    with CatalogReader(kit.storage).session() as conn:
        kinds = dict(
            conn.execute(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = 'lake'"
            ).fetchall()
        )
        [run] = [r for r in read_table(conn, "ops_runs") if r["run_id"] == result.run_id]
    assert {kinds[f"dim_{d}"] for d in ("players", "teams", "competitions", "seasons", "matches")} == {"BASE TABLE"}
    assert kinds["tracking_cross_provider"] == "VIEW"
    assert run["status"] == "ok"  # the history row written after dbt is in the run-end commit
    assert kit.storage.exists("lake", run_results_key(result.run_id))
