"""Visibility in the run: full pining listings, excluded and inactive-module matches (spec §8.1, §9, §29.13, §29.14;
plan Task 21)."""

from __future__ import annotations

from typing import Any

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, fake_table, save_settings, with_entry

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CompetitionClause, ModuleConfig
from lakehouse.errors import HttpStatusError
from lakehouse.ids.keys import match_key
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, exit_code, run_ingestion

KEY = match_key("gradientsports", "3812")


def _pining(*natives: str, visibility: str = "public") -> FakeAdapter:
    entries = [entry("gradientsports_pining", n, visibility=visibility, competition=("38", "2022")) for n in natives]
    files = {(n, "events"): f"line {n}".encode() for n in natives}
    return FakeAdapter("gradientsports_pining", "gradientsports", "pining", entries, files)


def _run(kit: LakeKit, *adapters: FakeAdapter) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers={a.provider: FakeParser(a.provider) for a in adapters},  # type: ignore[misc]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,  # dbt runs in its own tests
    )


def _rows(kit: LakeKit, table: str) -> list[dict[str, Any]]:
    with CatalogReader(kit.storage).session() as conn:
        return read_table(conn, table)


def _lake_objects_of(kit: LakeKit, key: int) -> list[str]:
    bronze = [i.key for i in kit.storage.list("lake", "data/bronze/") if i.key.rsplit("/", 1)[-1].startswith(f"{key}-")]
    return bronze


def _raw_lake(kit: LakeKit) -> list[str]:
    return [i.key for i in kit.storage.list("lake", "raw/")]


def _ingest_then(kit: LakeKit, adapter: FakeAdapter, *, raw_archive: bool = True) -> None:
    save_settings(kit.storage, [active("gradientsports_pining")], raw_archive=raw_archive)
    assert _run(kit, adapter).status == "ok"


def test_visibility_listing_is_full(kit: LakeKit) -> None:
    """Every listing of a pining module that holds ingested matches is the full listing (no `updatedSince`); a
    flip whose entry kept its old `updated_at` and artifact hashes is still moved."""
    adapter = _pining("3812")
    _ingest_then(kit, adapter)
    adapter.full_listings.clear()
    with_entry(adapter, "3812", visibility="private")
    adapter.fetched.clear()
    _run(kit, adapter)
    assert adapter.full_listings == ["full"]  # one list call, the full one
    assert adapter.listed == 0  # never the filtered listing
    assert adapter.fetched == []
    assert [m["visibility"] for m in _rows(kit, "ops_matches")] == ["private"]
    assert _lake_objects_of(kit, KEY) == []
    assert {f["root"] for f in _rows(kit, "ops_match_files")} == {"restricted"}


def test_visibility_listing_failure_visible(kit: LakeKit) -> None:
    """A 401 or 5xx on the visibility listing gives a `visibility_check_failed` row naming the module; the other
    modules still run; the run ends `failed` with exit code 1."""
    adapter = _pining("3812")
    _ingest_then(kit, adapter)
    save_settings(
        kit.storage, [ModuleConfig(module_id="gradientsports_pining", active=False, filter=[]), active("idsse_public")]
    )
    adapter.fail_listing = HttpStatusError(401, "https://pining.example.test/v1/gradientsports/matches")
    idsse = FakeAdapter(
        "idsse_public", "idsse", "public", [entry("idsse_public", "J03WPY")], {("J03WPY", "events"): b"a"}
    )
    result = _run(kit, adapter, idsse)
    assert (result.status, exit_code(result)) == ("failed", 1)
    [warning] = [e for e in _rows(kit, "ops_errors") if e["kind"] == "visibility_check_failed"]
    assert "gradientsports_pining" in warning["message"]
    assert result.counts.added == 1  # the idsse module ran


def test_excluded_match_turning_private_moves(kit: LakeKit) -> None:
    """§23.6, §29.14: an ingested public pining match that a narrowed filter excludes, flagged private later, is
    moved with no fetch, and no lake-root object of the match is left (bronze or pining raw)."""
    adapter = _pining("3812")
    _ingest_then(kit, adapter)
    assert _raw_lake(kit)
    save_settings(
        kit.storage,
        [active("gradientsports_pining", CompetitionClause(competition_id="99", seasons="all"))],
        raw_archive=True,
    )
    with_entry(adapter, "3812", visibility="private")
    adapter.fetched.clear()
    result = _run(kit, adapter)
    assert adapter.fetched == []
    assert result.counts.excluded == 1
    assert _lake_objects_of(kit, KEY) == []
    assert _raw_lake(kit) == []
    assert [m["visibility"] for m in _rows(kit, "ops_matches")] == ["private"]


def test_excluded_match_turning_public_moves(kit: LakeKit) -> None:
    adapter = _pining("3812", visibility="private")
    _ingest_then(kit, adapter)
    save_settings(kit.storage, [active("gradientsports_pining", CompetitionClause(competition_id="99", seasons="all"))])
    with_entry(adapter, "3812", visibility="public")
    adapter.fetched.clear()
    _run(kit, adapter)
    assert adapter.fetched == []
    assert {f["root"] for f in _rows(kit, "ops_match_files")} == {"lake"}
    assert _lake_objects_of(kit, KEY)


def test_inactive_pining_module_visibility_only(kit: LakeKit) -> None:
    """§29.14: an inactive pining module with ingested matches is listed once, nothing is fetched, and the match whose
    flag turned private moves; an inactive pining module with no ingested match is not listed; a direct public
    module is never listed for visibility."""
    adapter = _pining("3812", "3813")
    _ingest_then(kit, adapter)
    idsse_pining = FakeAdapter("idsse_pining", "idsse", "pining", [], {})
    public = FakeAdapter("idsse_public", "idsse", "public", [], {})
    save_settings(kit.storage, [ModuleConfig(module_id="gradientsports_pining", active=False, filter=[])])
    with_entry(adapter, "3812", visibility="private")
    adapter.full_listings.clear()
    adapter.fetched.clear()
    _run(kit, adapter, idsse_pining, public)
    assert (adapter.full_listings, adapter.fetched) == (["full"], [])
    assert (idsse_pining.full_listings, idsse_pining.listed) == ([], 0)
    assert (public.full_listings, public.listed) == ([], 0)
    flags = {m["native_match_id"]: m["visibility"] for m in _rows(kit, "ops_matches")}
    assert flags == {"3812": "private", "3813": "public"}


def test_inactive_pining_module_public_flip(kit: LakeKit) -> None:
    adapter = _pining("3812", visibility="private")
    _ingest_then(kit, adapter)
    save_settings(kit.storage, [ModuleConfig(module_id="gradientsports_pining", active=False, filter=[])])
    with_entry(adapter, "3812", visibility="public")
    adapter.fetched.clear()
    _run(kit, adapter)
    assert adapter.fetched == []
    assert {f["root"] for f in _rows(kit, "ops_match_files")} == {"lake"}


def test_narrowed_filter_keeps_data(kit: LakeKit) -> None:
    """§8.1, §29.13: after a clause is removed, the excluded match's bronze stays, the next run neither updates nor
    deletes it, and run history counts it as `excluded`."""
    adapter = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [
            entry("idsse_public", "J03WPY", competition=("C1", "S1")),
            entry("idsse_public", "J03WMX", competition=("C2", "S1")),
        ],
        {("J03WPY", "events"): b"a", ("J03WMX", "events"): b"b"},
    )
    save_settings(kit.storage, [active("idsse_public")])
    _run(kit, adapter)
    save_settings(kit.storage, [active("idsse_public", CompetitionClause(competition_id="C1", seasons="all"))])
    adapter.entries = [e for e in adapter.entries if e.competition_id == "C1"]  # the adapter lists the selected ones
    with_entry(adapter, "J03WPY", artifacts=entry("idsse_public", "J03WPY", signals={"events": "s9"}).artifacts)
    adapter.files[("J03WPY", "events")] = b"z"
    result = _run(kit, adapter)
    assert (result.counts.excluded, result.counts.changed) == (1, 1)
    with CatalogReader(kit.storage).session() as conn:
        lines = conn.execute(f'SELECT line FROM lake.main."{fake_table("idsse")}" ORDER BY line').fetchall()  # noqa: S608 - a checked bronze table name
    assert lines == [("b",), ("z",)]  # J03WMX's bronze stays, untouched
    [module_run] = [m for m in _rows(kit, "ops_module_runs") if m["run_id"] == result.run_id]
    assert module_run["excluded"] == 1
