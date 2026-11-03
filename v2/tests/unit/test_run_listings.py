"""Source-level listings in the run (spec §10.5; plan Task 21): one file per listing and per visibility, written
again only when the listing's content changes."""

from __future__ import annotations

import json
from typing import Any

from lakekit import LakeKit
from runkit import NOW, ListingAdapter, ListingParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.runs.run import run_ingestion


def _adapter(players: list[dict[str, str]]) -> ListingAdapter:
    return ListingAdapter(
        "skillcorner_public",
        "skillcorner",
        "public",
        [entry("skillcorner_public", "1899585")],
        {("1899585", "events"): b"a"},
        listings={"players": json.dumps(players).encode()},
    )


def _run(kit: LakeKit, adapter: ListingAdapter) -> Any:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},
        parsers={adapter.provider: ListingParser(adapter.provider)},
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,  # dbt runs in its own tests
    )


def _ids(kit: LakeKit) -> list[tuple[str, str]]:
    with CatalogReader(kit.storage).session() as conn:
        return conn.execute('SELECT id, visibility FROM lake.main."skillcorner_fake_players" ORDER BY id').fetchall()


def test_mixed_listing_lands_split_by_record_visibility(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("skillcorner_public")])
    adapter = _adapter([{"id": "1", "visibility": "public"}, {"id": "2", "visibility": "private"}, {"id": "3"}])
    result = _run(kit, adapter)
    assert result.status == "ok"
    assert _ids(kit) == [("1", "public"), ("2", "private"), ("3", "public")]
    lake = [i.key for i in kit.storage.list("lake", "data/bronze/skillcorner_fake_players/")]
    restricted = [i.key for i in kit.storage.list("restricted", "data/bronze/skillcorner_fake_players/")]
    assert (len(lake), len(restricted)) == (1, 1)


def test_unchanged_listing_is_not_written_again(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("skillcorner_public")])
    adapter = _adapter([{"id": "1", "visibility": "public"}])
    _run(kit, adapter)
    before = {i.key for i in kit.storage.list("lake", "data/bronze/skillcorner_fake_players/")}
    _run(kit, adapter)
    assert {i.key for i in kit.storage.list("lake", "data/bronze/skillcorner_fake_players/")} == before
    adapter.listings["players"] = json.dumps([{"id": "1"}, {"id": "4"}]).encode()
    _run(kit, adapter)
    assert _ids(kit) == [("1", "public"), ("4", "public")]  # the new content replaces the listing's rows


def test_listing_failure_is_recorded_and_the_run_goes_on(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("skillcorner_public")])
    adapter = _adapter([{"id": "1"}])
    adapter.listings["players"] = b"not json"
    result = _run(kit, adapter)
    assert result.status == "failed"
    assert [e.kind for e in result.errors] == ["listing_failed"]
    assert result.counts.added == 1  # the match still landed
