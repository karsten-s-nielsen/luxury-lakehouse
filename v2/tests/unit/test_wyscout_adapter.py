"""The Wyscout public adapter: figshare collection 4415000 (spec §7.1, §8.4; plan Task 19).

The figshare API answers are recorded (`figshare_collection.json`, `figshare_articles.json`, trimmed to the fields
the adapter reads). The two zip files are built here from the fixture slices, so their md5 and size replace the
recorded ones.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from requests.structures import CaseInsensitiveDict

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.errors import IntegrityError, UnexpectedPayloadError
from lakehouse.net.http import Expect, HttpResult
from lakehouse.sources import wyscout_public
from lakehouse.sources.base import ListingSource
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.wyscout_public import ARTICLE_URL, COLLECTION_URL, DOWNLOAD_URL, WyscoutPublicAdapter

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "wyscout"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
MATCHES_FILE = 14464622
EVENTS_FILE = 14464685
WC = CompetitionClause(competition_id="28", seasons=["10078"])


def _md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def _zip(members: Mapping[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _result(body: bytes, *, status: int = 200) -> HttpResult:
    return HttpResult(status=status, body=body, headers=CaseInsensitiveDict({}), final_url="")


class FakeFigshare:
    """Serves the recorded API answers and the files by id; counts the requests per URL."""

    def __init__(
        self,
        *,
        matches: bytes | None = None,
        events: bytes | None = None,
        collection: list[dict[str, Any]] | None = None,
    ) -> None:
        self.collection: list[dict[str, Any]] = collection or json.loads(_fixture("figshare_collection.json"))
        self.articles: dict[str, dict[str, Any]] = json.loads(_fixture("figshare_articles.json"))
        self.files: dict[int, bytes] = {}
        self.serve(
            MATCHES_FILE,
            matches
            if matches is not None
            else _zip(
                {
                    "matches_World_Cup.json": _fixture("matches_World_Cup_slice.json"),
                    "matches_England.json": _fixture("matches_cover.json"),
                }
            ),
        )
        self.serve(
            EVENTS_FILE,
            events if events is not None else _zip({"events_World_Cup.json": _fixture("events_World_Cup_slice.json")}),
        )
        for file_id, name in (
            (15073685, "competitions.json"),
            (15073697, "teams.json"),
            (15073721, "players_slice.json"),
            (15073868, "coaches_slice.json"),
            (21385245, "eventid2name.csv"),
            (21385239, "tags2name.csv"),
        ):
            self.serve(file_id, _fixture(name))
        self.calls: list[str] = []

    def serve(self, file_id: int, data: bytes, *, md5: str | None = None) -> None:
        """Serve `data` as the file, with its md5 and size in the article answer (or a stated md5)."""
        self.files[file_id] = data
        for article in self.articles.values():
            for file in article["files"]:
                if file["id"] == file_id:
                    file.update(size=len(data), computed_md5=md5 or _md5(data), supplied_md5=md5 or _md5(data))

    def get(self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        self.calls.append(url)
        if url.startswith(COLLECTION_URL + "?"):
            query = dict(part.split("=", 1) for part in url.split("?", 1)[1].split("&"))
            page, size = int(query["page"]), int(query["page_size"])
            return _result(json.dumps(self.collection[(page - 1) * size : page * size]).encode())
        for article_id, article in self.articles.items():
            if url == ARTICLE_URL.format(article_id=article_id):
                return _result(json.dumps(article).encode())
        for file_id, data in self.files.items():
            if url == DOWNLOAD_URL.format(file_id=file_id):
                assert len(data) <= max_bytes
                return _result(data)
        return _result(b"not found", status=404)


def _adapter(http: FakeFigshare, *clauses: Any, cache: MemoryCache | None = None) -> WyscoutPublicAdapter:
    module = ModuleConfig(module_id="wyscout_public", active=True, filter=list(clauses) or [AllClause()])
    return WyscoutPublicAdapter(http, module, cache or MemoryCache(), clock=lambda: NOW)


def test_listing_entries() -> None:
    http = FakeFigshare()
    entries = list(_adapter(http, WC).list_entries())
    assert [e.native_match_id for e in entries] == ["2058012", "2058017"]
    entry = entries[1]
    assert (entry.competition_id, entry.competition_name, entry.season_id, entry.season_name) == (
        "28",
        "World Cup",
        "10078",
        "2018",
    )
    assert entry.visibility == "public"
    assert entry.listed_at == NOW
    matches_md5 = _md5(http.files[MATCHES_FILE])
    assert entry.artifacts["matches"].key == f"{MATCHES_FILE}/matches_World_Cup.json"
    assert entry.artifacts["matches"].change_signal == f"{matches_md5}@v1"
    assert entry.artifacts["matches"].size == len(http.files[MATCHES_FILE])
    assert entry.artifacts["events"].key == f"{EVENTS_FILE}/events_World_Cup.json"
    assert entry.artifacts["events"].change_signal == f"{_md5(http.files[EVENTS_FILE])}@v1"
    assert DOWNLOAD_URL.format(file_id=EVENTS_FILE) not in http.calls  # the listing never reads events.zip


def test_all_clause_and_match_clause() -> None:
    assert [e.native_match_id for e in _adapter(FakeFigshare()).list_entries()] == [
        "2058012",
        "2058017",
        "2499985",
        "2500039",
    ]
    picked = list(_adapter(FakeFigshare(), MatchClause(match_ids=["2500039"])).list_entries())
    assert [(e.native_match_id, e.competition_name, e.season_id) for e in picked] == [
        ("2500039", "English first division", "181150")
    ]


def test_season_name_spans_the_match_years() -> None:
    """Wyscout gives no season name; the name is the year span of the season's match dates (`dateutc`)."""
    cover = _fixture("matches_cover.json").replace(b'"dateutc": "2018-02-10', b'"dateutc": "2017-08-12', 1)
    matches = _zip({"matches_England.json": cover})
    entries = list(_adapter(FakeFigshare(matches=matches)).list_entries())
    assert {e.season_name for e in entries} == {"2017/2018"}


def test_preview_counts() -> None:
    preview = _adapter(FakeFigshare(), WC).preview()
    assert [
        (t.competition_id, t.season_id, t.competition_name, t.season_name, t.match_count) for t in preview.tournaments
    ] == [
        ("28", "10078", "World Cup", "2018", 2),
        ("364", "181150", "English first division", "2018", 2),
    ]
    assert all(t.count_note is None for t in preview.tournaments)
    assert preview.fetched_at == NOW


def test_fetch_gives_the_competition_member() -> None:
    http = FakeFigshare()
    adapter = _adapter(http, WC)
    entries = list(adapter.list_entries())
    delivery = adapter.fetch(entries[0], "matches")
    assert delivery.decoded == _fixture("matches_World_Cup_slice.json")
    assert (delivery.provider, delivery.channel, delivery.native_match_id, delivery.artifact) == (
        "wyscout",
        "public",
        "2058012",
        "matches",
    )
    assert delivery.change_signal == entries[0].artifacts["matches"].change_signal
    assert delivery.wrapper == "none"
    events = adapter.fetch(entries[0], "events")
    assert events.decoded == _fixture("events_World_Cup_slice.json")
    again = adapter.fetch(entries[1], "events")
    assert again.content_sha256 == events.content_sha256
    assert again.native_match_id == "2058017"
    assert http.calls.count(DOWNLOAD_URL.format(file_id=EVENTS_FILE)) == 1  # one download serves every match


def test_files_cached_by_md5() -> None:
    cache = MemoryCache()
    http = FakeFigshare()
    list(_adapter(http, WC, cache=cache).list_entries())
    second = FakeFigshare()
    list(_adapter(second, WC, cache=cache).list_entries())
    assert DOWNLOAD_URL.format(file_id=MATCHES_FILE) in http.calls
    assert DOWNLOAD_URL.format(file_id=MATCHES_FILE) not in second.calls


def test_download_must_match_the_md5() -> None:
    http = FakeFigshare()
    http.serve(MATCHES_FILE, http.files[MATCHES_FILE], md5="0" * 32)
    with pytest.raises(IntegrityError, match="md5"):
        list(_adapter(http, WC).list_entries())


def test_fetch_after_a_source_change_fails() -> None:
    http = FakeFigshare()
    adapter = _adapter(http, WC)
    entry = next(iter(adapter.list_entries()))
    http.files[EVENTS_FILE] = _zip({"events_World_Cup.json": b"[]"})
    with pytest.raises(IntegrityError, match="changed since the listing"):
        adapter.fetch(entry, "events")


def test_missing_member_fails() -> None:
    http = FakeFigshare(events=_zip({"events_Spain.json": b"[]"}))
    adapter = _adapter(http, WC)
    entry = next(iter(adapter.list_entries()))
    with pytest.raises(IntegrityError, match=r"has no member events_World_Cup\.json"):
        adapter.fetch(entry, "events")


def test_member_size_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """A member larger than the cap (a zip bomb, or a file far beyond the measured sizes) is refused unread."""
    monkeypatch.setattr(wyscout_public, "MAX_MEMBER_BYTES", 1000)
    with pytest.raises(UnexpectedPayloadError, match=r"matches_England\.json is \d+ bytes, above 1000"):
        list(_adapter(FakeFigshare(), WC).list_entries())


def test_unknown_article_warns_and_known_extras_are_skipped() -> None:
    collection = json.loads(_fixture("figshare_collection.json"))
    collection.append({"id": 99, "title": "Shots", "url": "https://api.figshare.com/v2/articles/99"})
    adapter = _adapter(FakeFigshare(collection=collection), WC)
    list(adapter.list_entries())
    assert adapter.warnings == ["figshare collection 4415000: article 99 'Shots' is not a known Wyscout file"]


def test_a_missing_article_fails() -> None:
    collection = [a for a in json.loads(_fixture("figshare_collection.json")) if a["id"] != 7770422]
    with pytest.raises(IntegrityError, match="7770422"):
        list(_adapter(FakeFigshare(collection=collection), WC).list_entries())


def test_article_must_hold_the_expected_file() -> None:
    http = FakeFigshare()
    http.articles["7770422"]["files"][0]["name"] = "matches_v2.zip"
    with pytest.raises(UnexpectedPayloadError, match=r"matches\.zip"):
        list(_adapter(http, WC).list_entries())


def test_link_only_file_refused() -> None:
    http = FakeFigshare()
    http.articles["7770422"]["files"][0]["is_link_only"] = True
    with pytest.raises(UnexpectedPayloadError, match="link"):
        list(_adapter(http, WC).list_entries())


@pytest.mark.parametrize(
    ("kind", "name", "version"),
    [
        ("competitions", "competitions.json", 4),
        ("teams", "teams.json", 3),
        ("players", "players_slice.json", 3),
        ("coaches", "coaches_slice.json", 1),
        ("event_names", "eventid2name.csv", 1),
        ("tag_names", "tags2name.csv", 1),
    ],
)
def test_fetch_listing(kind: str, name: str, version: int) -> None:
    delivery = _adapter(FakeFigshare()).fetch_listing(kind)  # type: ignore[arg-type]
    assert delivery.decoded == _fixture(name)
    assert delivery.artifact == kind
    assert delivery.change_signal == f"{_md5(_fixture(name))}@v{version}"


def test_module_must_be_wyscout_public() -> None:
    module = ModuleConfig(module_id="idsse_public", active=True, filter=[AllClause()])
    with pytest.raises(ValueError, match="wyscout_public"):
        WyscoutPublicAdapter(FakeFigshare(), module, MemoryCache())


def test_collection_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """The collection API pages its article list; every page is read until a short one."""
    monkeypatch.setattr(wyscout_public, "PAGE_SIZE", 5)
    http = FakeFigshare()
    assert len(list(_adapter(http, WC).list_entries())) == 2
    assert sum(1 for url in http.calls if url.startswith(COLLECTION_URL)) == 3  # 12 articles: 5, 5 and 2


def test_one_adapter_serves_every_member() -> None:
    adapter = _adapter(FakeFigshare())
    entries = {e.native_match_id: e for e in adapter.list_entries()}
    world_cup = adapter.fetch(entries["2058017"], "matches")
    england = adapter.fetch(entries["2500039"], "matches")
    assert world_cup.decoded == _fixture("matches_World_Cup_slice.json")
    assert england.decoded == _fixture("matches_cover.json")
    assert england.native_match_id == "2500039"


def test_a_match_in_two_members_fails() -> None:
    slice_ = _fixture("matches_World_Cup_slice.json")
    matches = _zip({"matches_World_Cup.json": slice_, "matches_Copy.json": slice_})
    with pytest.raises(IntegrityError, match="in two members"):
        list(_adapter(FakeFigshare(matches=matches)).list_entries())


def test_two_files_of_the_same_name_fail() -> None:
    http = FakeFigshare()
    files = http.articles["7770422"]["files"]
    files.append(dict(files[0], id=1))
    with pytest.raises(UnexpectedPayloadError, match=r"holds 2 files named matches\.zip"):
        list(_adapter(http, WC).list_entries())


@pytest.mark.parametrize("compression", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_damaged_member_fails(compression: int) -> None:
    """A member whose bytes do not match its CRC-32 (or whose deflate stream is broken) fails as damaged."""
    body = _fixture("events_World_Cup_slice.json")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        archive.writestr("events_World_Cup.json", body)
    data = bytearray(buffer.getvalue())
    start = data.index(b"events_World_Cup.json") + len("events_World_Cup.json") + 200
    data[start : start + 40] = bytes(b ^ 0xFF for b in data[start : start + 40])
    http = FakeFigshare(events=bytes(data))
    adapter = _adapter(http, WC)
    entry = next(iter(adapter.list_entries()))
    with pytest.raises(IntegrityError, match="damaged"):
        adapter.fetch(entry, "events")


def test_listing_protocol() -> None:
    adapter = _adapter(FakeFigshare())
    assert isinstance(adapter, ListingSource)
    assert adapter.listing_kinds() == ("competitions", "teams", "players", "coaches", "event_names", "tag_names")
    with pytest.raises(ValueError, match="unknown Wyscout listing"):
        adapter.fetch_listing("referees")
