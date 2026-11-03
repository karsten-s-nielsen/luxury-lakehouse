"""The run's steps (spec §9, §12.3, §17, §23.6, §29.13, §29.14; plan Task 21). Fake adapters and parsers
(`tests/support/runkit.py`); `tests/e2e/test_run_local.py` runs the real ones."""

from __future__ import annotations

import hashlib
import json
import signal
import sys
from collections.abc import Mapping
from types import FrameType
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, fake_table, save_settings, with_entry

from lakehouse.catalog.publish import listed_sequences
from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CheckpointConfig, ModuleConfig
from lakehouse.config.store import ConfigStore
from lakehouse.ids.keys import match_key
from lakehouse.lineage.model import read_lineage
from lakehouse.resolve.decisions import AcceptRequest, DecisionStore, applied_requests
from lakehouse.runs.dbt import DbtNodeResult, DbtResult
from lakehouse.runs.history import read_table
from lakehouse.runs.progress import read_progress
from lakehouse.runs.run import RunResult, exit_code, run_ingestion
from lakehouse.storage.lease import Lease, write_skip_record


def _idsse(entries: list[Any], files: Mapping[tuple[str, str], bytes]) -> FakeAdapter:
    return FakeAdapter("idsse_public", "idsse", "public", entries, files)


def _run(kit: LakeKit, *adapters: FakeAdapter, trigger: str = "cli", **kwargs: Any) -> RunResult:
    parsers = {a.provider: FakeParser(a.provider) for a in adapters}
    kwargs.setdefault("dbt", None)  # dbt runs in its own tests (test_dbt_materialisation, e2e/test_dbt_handoff)
    return run_ingestion(
        storage=kit.storage,
        trigger=trigger,  # type: ignore[arg-type]
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers=parsers,  # type: ignore[arg-type]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        **kwargs,
    )


def _rows(kit: LakeKit, table: str) -> list[dict[str, Any]]:
    with CatalogReader(kit.storage).session() as conn:
        return read_table(conn, table)


def _bronze(kit: LakeKit, table: str) -> list[tuple[Any, ...]]:
    with CatalogReader(kit.storage).session() as conn:
        return conn.execute(f'SELECT match_key, line FROM lake.main."{table}" ORDER BY match_key, line').fetchall()  # noqa: S608 - a checked bronze table name


def _two(
    kit: LakeKit, modules: list[ModuleConfig] | None = None, *, checkpoint: CheckpointConfig | None = None
) -> FakeAdapter:
    save_settings(kit.storage, modules or [active("idsse_public")], checkpoint=checkpoint)
    return _idsse(
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )


def test_new_matches_are_written_and_committed_per_match(kit: LakeKit) -> None:
    adapter = _two(kit, checkpoint=CheckpointConfig(matches=1))  # M = 1: one publish per match (T3 spec D1)
    result = _run(kit, adapter)
    assert (result.status, exit_code(result)) == ("ok", 0)
    assert (result.counts.added, result.counts.failed) == (2, 0)
    wpy, wmx = match_key("idsse", "J03WPY"), match_key("idsse", "J03WMX")
    assert _bronze(kit, fake_table("idsse")) == sorted([(wpy, "a"), (wpy, "b"), (wmx, "c")])
    matches = {m["native_match_id"]: m for m in _rows(kit, "ops_matches")}
    assert set(matches) == {"J03WPY", "J03WMX"}
    assert matches["J03WPY"]["visibility"] == "public"
    assert {f["bronze_table"] for f in _rows(kit, "ops_match_files")} == {fake_table("idsse")}
    with CatalogReader(kit.storage).session() as conn:
        lineage = read_lineage(conn)
    assert {(r.channel_id, r.artifact, r.change_signal) for r in lineage} == {
        ("J03WPY", "events", "s1"),
        ("J03WMX", "events", "s1"),
    }
    assert len(listed_sequences(kit.storage)) >= 3  # one in-run commit per match and the run-end commit
    assert {c["artifact"] for c in _rows(kit, "ops_coordinate_systems")} == {"events"}


def test_unchanged_match_is_not_fetched(kit: LakeKit) -> None:
    adapter = _two(kit)
    _run(kit, adapter)
    adapter.fetched.clear()
    result = _run(kit, adapter)
    assert adapter.fetched == []
    assert (result.counts.unchanged, result.counts.added) == (2, 0)


def test_changed_signal_refetches_and_replaces(kit: LakeKit) -> None:
    adapter = _two(kit)
    _run(kit, adapter)
    with_entry(adapter, "J03WPY", artifacts=entry("idsse_public", "J03WPY", signals={"events": "s2"}).artifacts)
    adapter.files[("J03WPY", "events")] = b"z"
    adapter.fetched.clear()
    result = _run(kit, adapter)
    assert adapter.fetched == [("J03WPY", "events")]
    assert result.counts.changed == 1
    wpy = match_key("idsse", "J03WPY")
    assert [r for r in _bronze(kit, fake_table("idsse")) if r[0] == wpy] == [(wpy, "z")]


def test_new_signal_same_content_is_not_reparsed(kit: LakeKit) -> None:
    """§17.3: the change signal decides the download; the content hash decides the re-parse."""
    adapter = _two(kit)
    _run(kit, adapter)
    files_before = {f["path"] for f in _rows(kit, "ops_match_files")}
    with_entry(adapter, "J03WPY", artifacts=entry("idsse_public", "J03WPY", signals={"events": "s2"}).artifacts)
    result = _run(kit, adapter)
    assert result.counts.unchanged == 2
    assert {f["path"] for f in _rows(kit, "ops_match_files")} == files_before
    with CatalogReader(kit.storage).session() as conn:
        signals = {r.change_signal for r in read_lineage(conn) if r.channel_id == "J03WPY"}
    assert "s2" in signals  # the new signal is recorded, so the next run does not download again


def test_empty_filter_downloads_nothing(kit: LakeKit) -> None:
    """§23.6: an active module with an empty filter downloads nothing and shows a warning row."""
    save_settings(kit.storage, [ModuleConfig(module_id="idsse_public", active=True, filter=[])])
    adapter = _idsse([entry("idsse_public", "J03WPY")], {("J03WPY", "events"): b"a"})
    result = _run(kit, adapter)
    assert (adapter.listed, adapter.fetched) == (0, [])
    assert result.status == "ok"
    assert [e["kind"] for e in _rows(kit, "ops_errors")] == ["empty_filter"]


def test_unmapped_field_fails_match_continues_run(kit: LakeKit) -> None:
    """§12.3: the match fails loudly, the other matches commit, the run ends failed with exit code 1."""
    adapter = _two(kit)
    adapter.files[("J03WPY", "events")] = b"bad"
    result = _run(kit, adapter)
    assert (result.status, exit_code(result)) == ("failed", 1)
    assert (result.counts.added, result.counts.failed) == (1, 1)
    assert [(e.native_match_id, e.kind) for e in result.errors] == [("J03WPY", "unmapped_field")]
    assert [m["native_match_id"] for m in _rows(kit, "ops_matches")] == ["J03WMX"]
    assert _rows(kit, "ops_errors")[0]["message"] == "unmapped fields in events: bad"


def test_saved_config_only(kit: LakeKit) -> None:
    """The run reads the saved config once: a draft saved after the run starts is not seen."""
    adapter = _two(kit)
    store = ConfigStore(kit.storage)

    def save_a_narrower_filter() -> None:
        settings, version = store.load()
        narrowed = [
            m.model_copy(update={"filter": []}) if m.module_id == "idsse_public" else m for m in settings.modules
        ]
        store.save(settings.model_copy(update={"modules": narrowed}), expected_version=version, operator="other")

    adapter.on_list = save_a_narrower_filter
    result = _run(kit, adapter)
    assert result.counts.added == 2


def test_progress_object_updates(kit: LakeKit) -> None:
    adapter = _two(kit)
    result = _run(kit, adapter)
    progress = read_progress(kit.storage, result.run_id)
    assert (progress.status, progress.step, progress.done) == ("ok", "done", 2)


def test_trigger_recorded(kit: LakeKit) -> None:
    result = _run(kit, _two(kit), trigger="button")
    [run] = [r for r in _rows(kit, "ops_runs") if r["run_id"] == result.run_id]
    assert (run["trigger"], run["status"], run["added"]) == ("button", "ok", 2)
    assert [m["module_id"] for m in _rows(kit, "ops_module_runs")] == ["idsse_public"]
    assert {s["step_id"] for s in _rows(kit, "ops_match_steps")} == {
        f"{result.run_id}.idsse_public.J03WPY",
        f"{result.run_id}.idsse_public.J03WMX",
    }


def test_every_run_resolves(kit: LakeKit) -> None:
    """Spec §15.3: resolution runs on every run, before the run-end commit; it reads the review requests from the
    config side and records the ones it applied in the catalog (plan Task 22)."""
    store = DecisionStore(kit.storage)
    store.add(AcceptRequest(request_id="req-1", entity=None, batch_filter=None, requested_by="ops", requested_at=NOW))
    result = _run(kit, _two(kit))
    assert result.status == "ok"
    with CatalogReader(kit.storage).session() as conn:
        assert applied_requests(conn) == {"req-1"}
        tables = {
            name
            for (name,) in conn.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'"
            ).fetchall()
        }
    assert {"gold_player_links", "gold_team_links", "gold_match_links", "ops_review_queue"} <= tables
    assert len(store.load().accept_requests) == 1  # the run wrote no config-side object


def test_table_from_several_deliveries_keeps_each_source_hash(kit: LakeKit) -> None:
    """A bronze table built from several deliveries (as IDSSE's `idsse_files`, one row per file) keeps each row's
    own `_source_hash`; the file registry then names no single source hash (spec §11.2, §13.1)."""
    save_settings(kit.storage, [active("idsse_public")])
    listed = entry("idsse_public", "J03WPY", signals={"events": "s1", "metadata": "s1"})
    adapter = _idsse([listed], {("J03WPY", "events"): b"a\nb", ("J03WPY", "metadata"): b"m"})
    result = _run(kit, adapter)
    assert result.status == "ok", result.errors
    with CatalogReader(kit.storage).session() as conn:
        hashes = conn.execute(
            f'SELECT artifact, _source_hash FROM lake.main."{fake_table("idsse")}" ORDER BY artifact, line'  # noqa: S608 - a checked bronze table name
        ).fetchall()
    events, metadata = hashlib.sha256(b"a\nb").hexdigest(), hashlib.sha256(b"m").hexdigest()
    assert hashes == [("events", events), ("events", events), ("metadata", metadata)]
    [registered] = [f for f in _rows(kit, "ops_match_files") if f["bronze_table"] == fake_table("idsse")]
    assert registered["source_hash"] is None  # per row, not per file


def test_dbt_runs_after_resolution_and_a_failure_fails_the_run(kit: LakeKit) -> None:
    """Spec §17.2: dbt runs once per run, after resolution; a failed build is a visible error and fails the run,
    which still writes its history and its run-end commit."""
    calls: list[tuple[str, bool]] = []

    def failing_dbt(cat: Any, *, run_id: str, caps: Any) -> DbtResult:
        tables = {
            n
            for (n,) in cat.conn.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'"
            ).fetchall()
        }
        calls.append((run_id, "ops_entity_records" in tables))
        return DbtResult(False, [DbtNodeResult("model.lakehouse.dim_players", "error", "boom")], "runs/dbt/x.json")

    result = _run(kit, _two(kit), dbt=failing_dbt)
    assert calls == [(result.run_id, True)]  # resolution's records exist when dbt starts
    assert (result.status, exit_code(result)) == ("failed", 1)
    [error] = [e for e in result.errors if e.kind == "dbt_failed"]
    assert "model.lakehouse.dim_players" in error.message and "boom" not in error.message
    [run] = [r for r in _rows(kit, "ops_runs") if r["run_id"] == result.run_id]
    assert run["status"] == "failed"


def test_held_lease_skips_the_run(kit: LakeKit) -> None:
    adapter = _two(kit)
    held = Lease.acquire(kit.storage, holder_id="other", run_id="other-run", clock=lambda: NOW)
    assert held is not None
    result = _run(kit, adapter)
    assert (result.status, exit_code(result)) == ("skipped", 3)
    assert adapter.listed == 0
    assert kit.storage.exists("lake", f"runs/skipped/{result.run_id}.json")


def test_skip_records_folded_into_history(kit: LakeKit) -> None:
    """§23.6 "Skip record": the next lease holder writes one `ops_runs` row with status `skipped`, the skip run id
    and its reason, and only then removes the record."""
    write_skip_record(
        kit.storage, run_id="skipped-run", reason="a run is in progress", blocking_run_id="x", clock=lambda: NOW
    )
    _run(kit, _two(kit))
    [skipped] = [r for r in _rows(kit, "ops_runs") if r["run_id"] == "skipped-run"]
    assert (skipped["status"], skipped["reason"], skipped["trigger"]) == ("skipped", "a run is in progress", None)
    assert not kit.storage.exists("lake", "runs/skipped/skipped-run.json")


def test_private_match_files_go_to_the_restricted_root(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    adapter = _idsse([entry("idsse_public", "J03WPY", visibility="private")], {("J03WPY", "events"): b"a"})
    _run(kit, adapter)
    assert {f["root"] for f in _rows(kit, "ops_match_files")} == {"restricted"}
    assert [m["visibility"] for m in _rows(kit, "ops_matches")] == ["private"]


def test_360_only_skips_download(kit: LakeKit) -> None:
    """§23.6 "StatsBomb 360 option": with the option on, a match without 360 data is not fetched, events included."""
    save_settings(kit.storage, [active("statsbomb_public", statsbomb_360_only=True)])
    adapter = FakeAdapter(
        "statsbomb_public",
        "statsbomb",
        "public",
        [
            entry("statsbomb_public", "1", signals={"events": "e1", "three_sixty": "t1"}, has_360=True),
            entry("statsbomb_public", "2", signals={"events": "e2"}, has_360=False),
            entry("statsbomb_public", "3", signals={"events": "e3"}, has_360=None),
        ],
        {("1", "events"): b"x", ("1", "three_sixty"): b"f", ("2", "events"): b"y", ("3", "events"): b"z"},
    )
    result = _run(kit, adapter)
    assert {native for native, _ in adapter.fetched} == {"1"}
    assert result.counts.added == 1


def test_ops_matches_has_360_frames(kit: LakeKit) -> None:
    """§11.3, §29.13: `has_360_frames` is true exactly when the match has 360 bronze rows; null for other
    providers; a listed `has_360` that disagrees gives a `has_360_mismatch` warning row."""
    save_settings(kit.storage, [active("statsbomb_public"), active("idsse_public")])
    statsbomb = FakeAdapter(
        "statsbomb_public",
        "statsbomb",
        "public",
        [
            entry("statsbomb_public", "1", signals={"events": "e1", "three_sixty": "t1"}, has_360=True),
            entry("statsbomb_public", "2", signals={"events": "e2"}, has_360=True),  # says 360, has none
        ],
        {("1", "events"): b"x", ("1", "three_sixty"): b"f", ("2", "events"): b"y"},
    )
    idsse = _idsse([entry("idsse_public", "J03WPY")], {("J03WPY", "events"): b"a"})
    _run(kit, statsbomb, idsse)
    frames = {m["native_match_id"]: m["has_360_frames"] for m in _rows(kit, "ops_matches")}
    assert frames == {"1": True, "2": False, "J03WPY": None}
    warnings = [(e["native_match_id"], e["kind"], e["severity"]) for e in _rows(kit, "ops_errors")]
    assert warnings == [("2", "has_360_mismatch", "warning")]


def test_adapter_warnings_become_rows(kit: LakeKit) -> None:
    adapter = _two(kit)
    adapter.warnings.append("hf tree: a file is not a known kind")
    _run(kit, adapter)
    assert [(e["kind"], e["message"]) for e in _rows(kit, "ops_errors")] == [
        ("adapter_warning", "hf tree: a file is not a known kind")
    ]


def test_list_metadata_mismatch_is_a_warning(kit: LakeKit) -> None:
    """§29.1: a pining entry whose list fields differ from its metadata commits with the metadata values and
    writes a `list_metadata_mismatch` warning row."""
    save_settings(kit.storage, [active("idsse_pining")])
    listed = entry("idsse_pining", "J03WPY", competition=("C1", "S1"))
    adapter = FakeAdapter("idsse_pining", "idsse", "pining", [listed], {("J03WPY", "events"): b"a"})

    class Renaming(FakeParser):
        def parse(self, entry: Any, deliveries: Any, *, run_id: str) -> Any:
            parsed = super().parse(entry, deliveries, run_id=run_id)
            competitions = parsed.names.competitions.set_column(3, "season_name", pa_array(["S2"]))
            return type(parsed)(
                tables=parsed.tables,
                coordinate_systems=parsed.coordinate_systems,
                names=type(parsed.names)(
                    players=parsed.names.players, teams=parsed.names.teams, competitions=competitions
                ),
            )

    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={"idsse_pining": adapter},
        parsers={"idsse": Renaming("idsse")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )
    assert result.status == "ok"
    [warning] = _rows(kit, "ops_errors")
    assert (warning["kind"], warning["severity"]) == ("list_metadata_mismatch", "warning")
    assert [m["season_name"] for m in _rows(kit, "ops_matches")] == ["S2"]  # the metadata values commit


def pa_array(values: list[str]) -> Any:
    import pyarrow as pa

    return pa.array(values, pa.string())


def test_stop_request_ends_the_run_at_a_safe_point(kit: LakeKit) -> None:
    """A stop request (SIGTERM) is honoured between matches: committed matches stay, the run ends `failed` with
    reason `terminated`, and the lease is released."""
    adapter = _two(kit)
    result = _run(kit, adapter, stop=lambda: len(adapter.fetched) >= 1)  # asked to stop once the first match is in
    assert result.status == "failed"
    assert [e.kind for e in result.errors] == ["terminated"]
    assert result.counts.added == 1
    [run] = [r for r in _rows(kit, "ops_runs") if r["run_id"] == result.run_id]
    assert (run["status"], run["reason"]) == ("failed", "terminated")
    assert Lease.acquire(kit.storage, holder_id="next", run_id="next-run", clock=lambda: NOW) is not None


def test_steps_log_source_rows_and_timing(kit: LakeKit, capsys: pytest.CaptureFixture[str]) -> None:
    """Every step logs one JSON line with its source, its row or byte count where it has one, and its time (§21).
    M = 1 gives each match write its own publish, so each commit span carries a `catalog_seq` (T3 spec D1)."""
    _run(kit, _two(kit, checkpoint=CheckpointConfig(matches=1)))
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    steps = [line for line in lines if str(line["msg"]).startswith("lakehouse.run")]
    assert all(isinstance(line["elapsed_ms"], int) and line["status"] == "ok" for line in steps)
    by_name: dict[str, list[dict[str, Any]]] = {}
    for line in steps:
        by_name.setdefault(line["msg"], []).append(line)
    assert sorted(line["rows"] for line in by_name["lakehouse.run.write"]) == [1, 2]  # one line, then two lines
    assert {line["source"] for line in by_name["lakehouse.run.write"]} == {"idsse_public"}
    assert sorted(line["bytes"] for line in by_name["lakehouse.run.fetch"]) == [1, 3]
    assert {(line["provider"], line["channel"]) for line in by_name["lakehouse.run.fetch"]} == {("idsse", "public")}
    assert [line["rows"] for line in by_name["lakehouse.run.list"]] == [2]
    seqs = [line["catalog_seq"] for line in by_name["lakehouse.run.commit"]]
    assert seqs == sorted(seqs) and len(seqs) >= 3  # one per match and the run end, in order
    [lease] = by_name["lakehouse.run.lease"]
    assert lease["lease_generation"] >= 1


def test_sigterm_releases_lease(kit: LakeKit) -> None:
    """A real SIGTERM during a match (plan Task 21): the run's handler sets the stop flag, the match in hand still
    commits whole, the run stops before the next match, ends `failed` with reason `terminated`, releases the lease,
    and puts the old handler back."""
    adapter = _two(kit)
    reached: list[int] = []

    def fallback(signum: int, frame: FrameType | None) -> None:  # only if the run installed no handler
        reached.append(signum)

    def terminate() -> None:
        if len(adapter.fetched) == 1:
            signal.raise_signal(signal.SIGTERM)

    adapter.on_fetch = terminate
    old = signal.signal(signal.SIGTERM, fallback)
    try:
        result = _run(kit, adapter)
        assert signal.getsignal(signal.SIGTERM) is fallback
    finally:
        signal.signal(signal.SIGTERM, old)
    assert reached == []
    assert result.status == "failed"
    assert [e.kind for e in result.errors] == ["terminated"]
    assert result.counts.added == 1
    assert len(adapter.fetched) == 1  # the second match was never fetched
    assert len(_rows(kit, "ops_matches")) == 1  # the match in hand committed whole
    [run] = [r for r in _rows(kit, "ops_runs") if r["run_id"] == result.run_id]
    assert (run["status"], run["reason"]) == ("failed", "terminated")
    assert Lease.acquire(kit.storage, holder_id="next", run_id="next-run", clock=lambda: NOW) is not None


@pytest.mark.parametrize("trigger", ["schedule", "button", "cli"])
def test_run_ids_are_stable_and_valid(kit: LakeKit, trigger: str) -> None:
    result = _run(kit, _two(kit), trigger=trigger)
    assert result.run_id.replace("-", "").replace("_", "").replace(".", "").isalnum()


def test_run_logs_peak_memory(kit: LakeKit, capsys: pytest.CaptureFixture[str]) -> None:
    """§26 item 9: every run logs one `run memory` line with its peak RSS in MiB, for `hf jobs logs` to read on the
    HF Job. The number is present on a platform with stdlib `resource` (Linux, macOS) and `null` where there is
    none (Windows)."""
    result = _run(kit, _two(kit))
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    memory = [line for line in lines if line["msg"] == "run memory"]
    assert len(memory) == 1, "a run logs exactly one peak-memory line"
    [line] = memory
    assert line["run_id"] == result.run_id
    if sys.platform == "win32":
        assert line["peak_rss_mb"] is None
    else:
        assert isinstance(line["peak_rss_mb"], int | float) and line["peak_rss_mb"] > 0
