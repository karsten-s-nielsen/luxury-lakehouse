"""The offline re-parse from the raw archive (spec §13.2; plan Task 26): no source is called, only the matches whose
parser version changed (or the one named) are re-parsed, and lineage keeps the recorded delivery."""

from __future__ import annotations

from typing import Any

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, fake_table, save_settings, with_entry

from lakehouse.catalog.reader import CatalogReader
from lakehouse.ids.keys import match_key
from lakehouse.lineage.model import read_lineage
from lakehouse.raw.archive import raw_key
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, reparse_matches, run_ingestion


def _run(kit: LakeKit, adapters: list[FakeAdapter], parsers: dict[str, FakeParser]) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers=parsers,  # type: ignore[arg-type]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )


def _reparse(kit: LakeKit, parsers: dict[str, FakeParser], target: Any) -> RunResult:
    return reparse_matches(
        storage=kit.storage,
        parsers=parsers,  # type: ignore[arg-type]
        target=target,
        now=lambda: NOW,
        work_dir=kit.base / "work-reparse",
        dbt=None,
    )


def _bronze(kit: LakeKit, provider: str) -> list[tuple[Any, ...]]:
    with CatalogReader(kit.storage).session() as conn:
        return conn.execute(
            f'SELECT match_key, artifact, line, _source_hash FROM lake.main."{fake_table(provider)}" ORDER BY ALL'  # noqa: S608 - a checked bronze table name
        ).fetchall()


def _rows(kit: LakeKit, table: str) -> list[dict[str, Any]]:
    with CatalogReader(kit.storage).session() as conn:
        return read_table(conn, table)


def _two_modules(kit: LakeKit) -> list[FakeAdapter]:
    save_settings(kit.storage, [active("idsse_public"), active("skillcorner_public")], raw_archive=True)
    idsse = FakeAdapter(
        "idsse_public",
        "idsse",
        "public",
        [entry("idsse_public", "J03WPY"), entry("idsse_public", "J03WMX")],
        {("J03WPY", "events"): b"a\nb", ("J03WMX", "events"): b"c"},
    )
    skillcorner = FakeAdapter(
        "skillcorner_public",
        "skillcorner",
        "public",
        [entry("skillcorner_public", "1899585")],
        {("1899585", "events"): b"s"},
    )
    return [idsse, skillcorner]


def test_reparse_from_archive_offline(kit: LakeKit) -> None:
    adapters = _two_modules(kit)
    assert _run(kit, adapters, {"idsse": FakeParser("idsse"), "skillcorner": FakeParser("skillcorner")}).status == "ok"
    before = {p: _bronze(kit, p) for p in ("idsse", "skillcorner")}
    lineage_before = _lineage(kit)
    # every match is at the current parser version: nothing to re-parse
    current = {"idsse": FakeParser("idsse"), "skillcorner": FakeParser("skillcorner")}
    unchanged = _reparse(kit, current, "all")
    assert (unchanged.status, unchanged.counts.changed, unchanged.counts.unchanged) == ("ok", 0, 3)
    assert current["idsse"].parsed == [] and current["skillcorner"].parsed == []
    # a new IDSSE parser version: only the IDSSE matches are re-parsed, from the archive, with no source call
    newer = {"idsse": FakeParser("idsse", version="2"), "skillcorner": FakeParser("skillcorner")}
    fetched = [list(a.fetched) for a in adapters]
    result = _reparse(kit, newer, "all")
    assert (result.status, result.counts.changed, result.counts.unchanged) == ("ok", 2, 1)
    assert sorted(newer["idsse"].parsed) == ["J03WMX", "J03WPY"] and newer["skillcorner"].parsed == []
    assert [a.fetched for a in adapters] == fetched  # no source call: the re-parse takes no adapter at all
    versions = {(m["provider"], m["native_match_id"]): m["parser_version"] for m in _rows(kit, "ops_matches")}
    assert versions == {("idsse", "J03WPY"): "2", ("idsse", "J03WMX"): "2", ("skillcorner", "1899585"): "1"}
    assert {p: _bronze(kit, p) for p in ("idsse", "skillcorner")} == before  # the same bytes give the same bronze
    assert _lineage(kit) == lineage_before  # the recorded deliveries (wrapper, hashes) stay


def test_reparse_one_match_even_at_the_current_version(kit: LakeKit) -> None:
    adapters = _two_modules(kit)
    parsers = {"idsse": FakeParser("idsse"), "skillcorner": FakeParser("skillcorner")}
    _run(kit, adapters, parsers)
    again = {"idsse": FakeParser("idsse"), "skillcorner": FakeParser("skillcorner")}
    result = _reparse(kit, again, ("idsse", "J03WMX"))
    assert (result.counts.changed, result.counts.unchanged) == (1, 0)
    assert again["idsse"].parsed == ["J03WMX"]


def test_reparse_without_archive_is_a_visible_failure(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")], raw_archive=False)
    adapter = FakeAdapter(
        "idsse_public", "idsse", "public", [entry("idsse_public", "J03WPY")], {("J03WPY", "events"): b"a"}
    )
    _run(kit, [adapter], {"idsse": FakeParser("idsse")})
    result = _reparse(kit, {"idsse": FakeParser("idsse", version="2")}, "all")
    assert (result.status, result.counts.failed) == ("failed", 1)
    assert "no raw copy" in result.errors[0].message


def test_reparse_after_private_move(kit: LakeKit) -> None:
    """After a match's pining raw file moved to `restricted:raw/`, a re-parse reads it there and rebuilds identical
    bronze, still in the restricted root."""
    save_settings(kit.storage, [active("gradientsports_pining")], raw_archive=True)
    adapter = FakeAdapter(
        "gradientsports_pining",
        "gradientsports",
        "pining",
        [entry("gradientsports_pining", "3812", competition=("38", "2022"))],
        {("3812", "events"): b"line one\nline two"},
    )
    parsers = {"gradientsports": FakeParser("gradientsports")}
    _run(kit, [adapter], parsers)
    with_entry(adapter, "3812", visibility="private")
    assert _run(kit, [adapter], parsers).status == "ok"  # the move
    [row] = [r for r in _lineage(kit) if r[0] == match_key("gradientsports", "3812")]
    raw = raw_key(row[3])
    assert kit.storage.exists("restricted", raw) and not kit.storage.exists("lake", raw)
    before = _bronze(kit, "gradientsports")
    again = {"gradientsports": FakeParser("gradientsports")}
    result = _reparse(kit, again, ("gradientsports", "3812"))
    assert (result.status, result.counts.changed) == ("ok", 1) and again["gradientsports"].parsed == ["3812"]
    assert _bronze(kit, "gradientsports") == before
    key = match_key("gradientsports", "3812")
    assert not [
        i.key for i in kit.storage.list("lake", "data/bronze/") if i.key.rsplit("/", 1)[-1].startswith(f"{key}-")
    ]


def _lineage(kit: LakeKit) -> list[tuple[Any, ...]]:
    with CatalogReader(kit.storage).session() as conn:
        rows = read_lineage(conn)
    return sorted(
        (r.match_key, r.channel, r.artifact, r.content_sha256, r.delivery_sha256, r.wrapper, r.kept)
        for r in rows
        if r.kept
    )
