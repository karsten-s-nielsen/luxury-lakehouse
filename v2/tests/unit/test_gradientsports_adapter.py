"""The Gradient Sports public adapter: a public Google Drive folder (spec §7.1, §13.1, §17.3; plan Task 18).
All pages and files here are synthetic (`drivekit`); GS data has no licence (§23.4)."""

from __future__ import annotations

import bz2
import hashlib
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import pytest
from drivekit import DriveItem, confirm_page, folder_page
from requests.structures import CaseInsensitiveDict

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.errors import UnexpectedPayloadError
from lakehouse.net.http import Expect, HttpResult
from lakehouse.sources.base import ListingSource
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.gradientsports_public import (
    DOWNLOAD_URL,
    LISTING_URL,
    GradientSportsPublicAdapter,
    content_changed,
)

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
ROOT = "rootFolder00"
COMPETITIONS = (
    b"games,id,name\n\"[{'id': '9001', 'season': '2022'}, {'id': '9002', 'season': '2022'}]\",77,Invented Cup\n"
)
TRACKING = b'{"frameNum": 1, "period": 1}\n{"frameNum": 2, "period": 1}\n'


def _result(body: bytes, status: int = 200) -> HttpResult:
    return HttpResult(status=status, body=body, headers=CaseInsensitiveDict({}), final_url="")


class FakeDrive:
    """Serves folder pages by folder id and files by file id; large files answer first with the confirm page."""

    def __init__(self) -> None:
        self.folders: dict[str, bytes] = {}
        self.files: dict[str, bytes] = {}
        self.large: set[str] = set()
        self.after_confirm: dict[str, bytes] = {}
        self.calls: list[str] = []

    def get(self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        self.calls.append(url)
        parts = urlsplit(url)
        query = dict(parse_qsl(parts.query))
        if url.startswith(LISTING_URL):
            return _result(self.folders[query["id"]])
        assert url.startswith(DOWNLOAD_URL)
        file_id = query["id"]
        if file_id in self.large and "confirm" not in query:
            return _result(confirm_page(file_id))
        if "confirm" in query and file_id in self.after_confirm:
            body = self.after_confirm[file_id]
            if expect != "text" and body.lstrip().lower().startswith(b"<!doctype html"):
                raise UnexpectedPayloadError("HTML where binary data was expected")  # HttpClient's own rule
            return _result(body)
        return _result(self.files[file_id])


def _drive(*, today_file: bool = False) -> FakeDrive:
    drive = FakeDrive()
    drive.folders[ROOT] = folder_page(
        "root",
        [
            DriveItem("fEvents", "Event Data", "1/31/25", folder=True),
            DriveItem("fMeta", "Metadata", "1/31/25", folder=True),
            DriveItem("fRoster", "Rosters", "1/31/25", folder=True),
            DriveItem("fTracking", "Tracking Data", "1/31/25", folder=True),
            DriveItem("cCompetitions", "competitions.csv", "2/5/24"),
            DriveItem("cPlayers", "players.csv", "2/5/24"),
            DriveItem("dChangeLog", "PFF FC Change Log", "6/3/25", google_doc=True),
        ],
    )
    drive.folders["fEvents"] = folder_page(
        "Event Data",
        [
            DriveItem("dropA", "March 14, 2025", "5/1/25", folder=True),
            DriveItem("e9001", "9001.json", "6/3/25"),
            DriveItem("e9002", "9002.json", "6/3/25"),
            DriveItem("eSpec", "PFF FC Event Data Specification v2.5.pdf", "6/3/25"),
        ],
    )
    drive.folders["fMeta"] = folder_page(
        "Metadata", [DriveItem(f"m{m}", f"{m}.json", "2/5/25") for m in ("9001", "9002")]
    )
    drive.folders["fRoster"] = folder_page(
        "Rosters", [DriveItem(f"r{m}", f"{m}.json", "1/31/25") for m in ("9001", "9002")]
    )
    drive.folders["fTracking"] = folder_page(
        "Tracking Data",
        [
            DriveItem("t9001", "9001.jsonl.bz2", "4:15 PM" if today_file else "2/2/24"),
            DriveItem("t9002", "9002.jsonl.bz2", "8/28/24"),
            DriveItem("tSpec", "PFF FC Tracking Data Specification v2.2.pdf", "9/13/24"),
        ],
    )
    drive.files.update(
        {"cCompetitions": COMPETITIONS, "cPlayers": b"dob,firstName\n", "m9001": b"[{}]", "r9001": b"[]"}
    )
    drive.files.update({"e9001": b"[]", "m9002": b"[{}]", "r9002": b"[]", "e9002": b"[]"})
    drive.large.update({"t9001", "t9002"})
    drive.after_confirm.update({"t9001": bz2.compress(TRACKING), "t9002": bz2.compress(TRACKING)})
    return drive


def _adapter(drive: FakeDrive, *clauses: Any, cache: MemoryCache | None = None) -> GradientSportsPublicAdapter:
    module = ModuleConfig(module_id="gradientsports_public", active=True, filter=list(clauses) or [AllClause()])
    return GradientSportsPublicAdapter(drive, module, cache or MemoryCache(), clock=lambda: NOW, folder_id=ROOT)


def test_listing_entries() -> None:
    adapter = _adapter(_drive())
    entries = list(adapter.list_entries())
    assert [e.native_match_id for e in entries] == ["9001", "9002"]
    entry = entries[0]
    assert (entry.competition_id, entry.competition_name, entry.season_id, entry.season_name) == (
        "77",
        "Invented Cup",
        "2022",  # GS has no season id: the season name is the native season id (spec §14.1)
        "2022",
    )
    assert set(entry.artifacts) == {"metadata", "roster", "events", "tracking"}
    tracking = entry.artifacts["tracking"]
    assert (tracking.key, tracking.change_signal, tracking.expected_sha256, tracking.size) == (
        "t9001",
        "t9001@2024-02-02",
        None,
        None,
    )
    assert adapter.warnings == []  # spec PDFs, the change-log document and the dated drops are skipped by rule


def test_day_level_signal_of_a_file_changed_today() -> None:
    """Drive shows a time, not a date, for a file changed today; the signal then uses the listing day."""
    entry = next(iter(_adapter(_drive(today_file=True)).list_entries()))
    assert entry.artifacts["tracking"].change_signal == f"t9001@{date(2026, 10, 27).isoformat()}"


def test_unknown_entries_warn() -> None:
    drive = _drive()
    drive.folders["fMeta"] = folder_page(
        "Metadata",
        [*(DriveItem(f"m{m}", f"{m}.json", "2/5/25") for m in ("9001", "9002")), DriveItem("x", "notes.txt", "1/1/25")],
    )
    adapter = _adapter(drive)
    list(adapter.list_entries())
    assert any("notes.txt" in w for w in adapter.warnings)


def test_drive_confirm_page_followed_or_rejected() -> None:
    """Review focus 1: the virus-scan page is followed once, or rejected; it is never parsed as data."""
    drive = _drive()
    adapter = _adapter(drive)
    entry = next(iter(adapter.list_entries()))
    delivery = adapter.fetch(entry, "tracking")
    assert delivery.decoded == TRACKING
    followed = [c for c in drive.calls if "confirm=t" in c]
    assert len(followed) == 1 and "uuid=" in followed[0]
    drive.after_confirm["t9001"] = confirm_page("t9001")  # the confirm page again: not followed twice
    with pytest.raises(UnexpectedPayloadError):
        adapter.fetch(entry, "tracking")
    drive.large.discard("t9001")
    drive.files["t9001"] = b"<!DOCTYPE html><html><head><title>Quota exceeded</title></head></html>"
    with pytest.raises(UnexpectedPayloadError, match="not the confirm form"):
        adapter.fetch(entry, "tracking")
    drive.files["t9001"] = confirm_page("t9001").replace(
        b"https://drive.usercontent.google.com/download", b"https://example.invalid/download"
    )
    with pytest.raises(UnexpectedPayloadError, match="not the confirm form"):  # a form that posts elsewhere
        adapter.fetch(entry, "tracking")
    drive.files["t9001"] = confirm_page("someOtherFile")
    with pytest.raises(UnexpectedPayloadError, match="someOtherFile"):
        adapter.fetch(entry, "tracking")


def test_bz2_wrapper_recorded() -> None:
    entry = next(iter(_adapter(_drive()).list_entries()))
    delivery = _adapter(_drive()).fetch(entry, "tracking")
    assert delivery.wrapper == "bz2"
    assert delivery.decoded == TRACKING
    assert delivery.content_sha256 == hashlib.sha256(TRACKING).hexdigest()
    assert delivery.delivery_sha256 == hashlib.sha256(bz2.compress(TRACKING)).hexdigest()
    assert delivery.change_signal == "t9001@2024-02-02"


def test_content_hash_decides_reparse() -> None:
    """§17.3: the day-level signal only decides the download; the content hash decides the re-parse."""
    entry = next(iter(_adapter(_drive()).list_entries()))
    delivery = _adapter(_drive()).fetch(entry, "events")
    assert content_changed(delivery, None)
    assert not content_changed(delivery, delivery.content_sha256)
    assert content_changed(delivery, "0" * 64)


def test_listing_follows_the_clauses() -> None:
    drive = _drive()

    def ids(*clauses: Any) -> list[str]:
        return [e.native_match_id for e in _adapter(drive, *clauses).list_entries()]

    assert ids(CompetitionClause(competition_id="77", seasons=["2022"])) == ["9001", "9002"]
    assert ids(CompetitionClause(competition_id="77", seasons=["2018"])) == []
    assert ids(MatchClause(match_ids=["9002"])) == ["9002"]


def test_preview_counts() -> None:
    preview = _adapter(_drive()).preview()
    assert [(t.competition_id, t.competition_name, t.season_id, t.match_count) for t in preview.tournaments] == [
        ("77", "Invented Cup", "2022", 2)
    ]


def test_competitions_file_cached_by_signal() -> None:
    drive, cache = _drive(), MemoryCache()
    list(_adapter(drive, cache=cache).list_entries())
    list(_adapter(drive, cache=cache).list_entries())
    assert sum(1 for c in drive.calls if "id=cCompetitions" in c) == 1


def test_content_length_from_a_one_byte_range() -> None:
    """The listing has no sizes (§26 item 8): a 1-byte range request through the confirm form reads the total."""
    drive = _drive()
    seen: list[Mapping[str, str] | None] = []
    real_get = drive.get

    def ranged(url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        seen.append(headers)
        result = real_get(url, headers=headers, max_bytes=max_bytes, expect=expect)
        if headers and headers.get("Range") == "bytes=0-0" and not result.body.startswith(b"<!DOCTYPE"):
            total = len(result.body)
            return HttpResult(206, result.body[:1], CaseInsensitiveDict({"Content-Range": f"bytes 0-0/{total}"}), "")
        return result

    drive.get = ranged  # type: ignore[method-assign]
    assert _adapter(drive).content_length("t9001") == len(bz2.compress(TRACKING))
    assert _adapter(drive).content_length("cCompetitions") == len(COMPETITIONS)
    assert all(h is not None and h.get("Range") == "bytes=0-0" for h in seen)


def test_listing_protocol() -> None:
    drive = _drive()
    adapter = _adapter(drive)
    assert isinstance(adapter, ListingSource)
    assert adapter.listing_kinds() == ("competitions", "players")
    delivery = adapter.fetch_listing("players")
    assert delivery.decoded == drive.files["cPlayers"]
    assert (delivery.artifact, delivery.native_match_id) == ("players", "")
    with pytest.raises(ValueError, match="unknown Gradient Sports listing"):
        adapter.fetch_listing("rosters")
