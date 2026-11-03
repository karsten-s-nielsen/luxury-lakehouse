"""The IDSSE public adapter: HF `pysport/idsse-data` (spec §7.1, §8.4; plan Task 16)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from requests.structures import CaseInsensitiveDict

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.errors import IntegrityError
from lakehouse.net.http import Expect, HttpResult
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.idsse_public import RESOLVE_BASE, TREE_URL, IdssePublicAdapter

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "idsse"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
MATCHES = ("J03WMX", "J03WN1", "J03WOH", "J03WOY", "J03WPY", "J03WQQ", "J03WR9")
PY_EVENTS = "DFL_03_02_events_raw_DFL-COM-000002_DFL-MAT-J03WPY.xml"
PY_POSITIONS = "DFL_04_03_positions_raw_observed_DFL-COM-000002_DFL-MAT-J03WPY.xml"


def _blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own blob hash


def _result(body: bytes, *, status: int = 200, headers: Mapping[str, str] | None = None) -> HttpResult:
    return HttpResult(status=status, body=body, headers=CaseInsensitiveDict(dict(headers or {})), final_url="")


class FakeHttp:
    """Serves the recorded HF tree and the fixture files; counts the requests per URL."""

    def __init__(self, tree: list[dict[str, Any]], files: Mapping[str, bytes]) -> None:
        self.pages: dict[str, HttpResult] = {TREE_URL: _result(json.dumps(tree).encode())}
        self.files = dict(files)
        self.calls: list[str] = []

    def get(self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        self.calls.append(url)
        if url in self.pages:
            return self.pages[url]
        path = url.removeprefix(RESOLVE_BASE)
        if path not in self.files:
            return _result(b"not found", status=404)
        body = self.files[path]
        assert len(body) <= max_bytes
        return _result(body)


def _recorded_tree() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "hf_tree.json").read_text(encoding="utf-8"))


def _metadata_files() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for entry in _recorded_tree():
        if "matchinformation" in entry["path"]:
            files[entry["path"]] = (FIXTURES / f"matchinformation_{entry['path'][-10:-4]}.xml").read_bytes()
    return files


def _module(*clauses: Any) -> ModuleConfig:
    return ModuleConfig(module_id="idsse_public", active=True, filter=list(clauses) or [AllClause()])


def _adapter(http: FakeHttp, *clauses: Any, cache: MemoryCache | None = None) -> IdssePublicAdapter:
    return IdssePublicAdapter(http, _module(*clauses), cache or MemoryCache(), clock=lambda: NOW)


def test_listing_entries() -> None:
    tree = _recorded_tree()
    entries = list(_adapter(FakeHttp(tree, _metadata_files())).list_entries())
    assert [e.native_match_id for e in entries] == list(MATCHES)
    by_id = {e.native_match_id: e for e in entries}
    py = by_id["J03WPY"]
    assert (py.competition_id, py.competition_name, py.season_id, py.season_name) == (
        "DFL-COM-000002",
        "2. Bundesliga",
        "DFL-SEA-0001K6",
        "2022/2023",
    )
    assert (by_id["J03WMX"].competition_id, by_id["J03WMX"].competition_name) == ("DFL-COM-000001", "Bundesliga")
    assert (py.module_id, py.visibility, py.listed_at) == ("idsse_public", "public", NOW)
    assert set(py.artifacts) == {"metadata", "events", "positions"}
    recorded = {e["path"]: e for e in tree}
    positions = py.artifacts["positions"]
    assert positions.key == PY_POSITIONS
    assert positions.change_signal == "845202c29ad93d696653436da426a7cdaefb848298e07b7343de6529581671e1"  # LFS sha256
    assert (positions.expected_sha256, positions.size) == (positions.change_signal, 371688499)
    events = py.artifacts["events"]
    assert (events.change_signal, events.expected_sha256) == (recorded[PY_EVENTS]["oid"], None)  # git blob SHA


def test_listing_follows_the_clauses() -> None:
    http = FakeHttp(_recorded_tree(), _metadata_files())

    def ids(*clauses: Any) -> list[str]:
        return [e.native_match_id for e in _adapter(http, *clauses).list_entries()]

    assert ids(CompetitionClause(competition_id="DFL-COM-000001", seasons="all")) == ["J03WMX", "J03WN1"]
    assert len(ids(CompetitionClause(competition_id="DFL-COM-000002", seasons=["DFL-SEA-0001K6"]))) == 5
    assert ids(CompetitionClause(competition_id="DFL-COM-000002", seasons=["DFL-SEA-0001ZZ"])) == []
    assert ids(MatchClause(match_ids=["J03WPY"])) == ["J03WPY"]


def test_preview_counts() -> None:
    preview = _adapter(FakeHttp(_recorded_tree(), _metadata_files())).preview()
    rows = [
        (t.competition_id, t.competition_name, t.season_id, t.season_name, t.match_count, t.count_note)
        for t in preview.tournaments
    ]
    assert rows == [
        ("DFL-COM-000001", "Bundesliga", "DFL-SEA-0001K6", "2022/2023", 2, None),
        ("DFL-COM-000002", "2. Bundesliga", "DFL-SEA-0001K6", "2022/2023", 5, None),
    ]
    assert preview.fetched_at == NOW


def test_metadata_cached_by_blob_oid() -> None:
    http, cache = FakeHttp(_recorded_tree(), _metadata_files()), MemoryCache()
    list(_adapter(http, cache=cache).list_entries())
    first = len(http.calls)
    list(_adapter(http, cache=cache).list_entries())
    assert http.calls[first:] == [TREE_URL]  # the tree again, and no metadata file


def _fixture_tree(events: bytes, positions: bytes) -> list[dict[str, Any]]:
    tree = [e for e in _recorded_tree() if "J03WPY" in e["path"]]
    for entry in tree:
        if entry["path"] == PY_EVENTS:
            entry.update(oid=_blob_sha(events), size=len(events))
        elif entry["path"] == PY_POSITIONS:
            entry.update(size=134, lfs={"oid": hashlib.sha256(positions).hexdigest(), "size": len(positions)})
    return tree


def test_fetch_checks_lfs_sha256() -> None:
    events, positions = (
        (FIXTURES / "events_J03WPY_slice.xml").read_bytes(),
        (FIXTURES / "positions_J03WPY_slice.xml").read_bytes(),
    )
    files = {**_metadata_files(), PY_EVENTS: events, PY_POSITIONS: positions}
    http = FakeHttp(_fixture_tree(events, positions), files)
    adapter = _adapter(http)
    entry = next(iter(adapter.list_entries()))
    delivery = adapter.fetch(entry, "positions")
    assert (
        delivery.content_sha256 == hashlib.sha256(positions).hexdigest() == entry.artifacts["positions"].change_signal
    )
    assert (delivery.provider, delivery.channel, delivery.artifact, delivery.visibility) == (
        "idsse",
        "public",
        "positions",
        "public",
    )
    http.files[PY_POSITIONS] = positions.replace(b'X="0.57"', b'X="0.58"', 1)
    with pytest.raises(IntegrityError, match="SHA-256"):
        adapter.fetch(entry, "positions")


def test_fetch_checks_git_blob_sha() -> None:
    events, positions = (
        (FIXTURES / "events_J03WPY_slice.xml").read_bytes(),
        (FIXTURES / "positions_J03WPY_slice.xml").read_bytes(),
    )
    files = {**_metadata_files(), PY_EVENTS: events, PY_POSITIONS: positions}
    http = FakeHttp(_fixture_tree(events, positions), files)
    adapter = _adapter(http)
    entry = next(iter(adapter.list_entries()))
    assert adapter.fetch(entry, "events").decoded == events
    http.files[PY_EVENTS] = events.replace(b"KickOff", b"KickOfF", 1)  # same size, other bytes
    with pytest.raises(IntegrityError, match="blob"):
        adapter.fetch(entry, "events")


def test_tree_pages_are_followed() -> None:
    tree = _recorded_tree()
    second = "https://huggingface.co/api/datasets/pysport/idsse-data/tree/main?recursive=true&cursor=abc"
    http = FakeHttp(tree[:10], _metadata_files())
    http.pages[TREE_URL] = _result(json.dumps(tree[:10]).encode(), headers={"Link": f'<{second}>; rel="next"'})
    http.pages[second] = _result(json.dumps(tree[10:]).encode())
    entries = list(_adapter(http).list_entries())
    assert [e.native_match_id for e in entries] == list(MATCHES)
    assert all(set(e.artifacts) == {"metadata", "events", "positions"} for e in entries)  # page 2 files


def test_unknown_files_and_missing_metadata_warn() -> None:
    tree = [e for e in _recorded_tree() if "J03WOH" not in e["path"] or "matchinformation" not in e["path"]]
    tree.append(
        {"type": "file", "path": "DFL_05_01_other_DFL-COM-000002_DFL-MAT-J03WPY.xml", "size": 10, "oid": "0" * 40}
    )
    adapter = _adapter(FakeHttp(tree, _metadata_files()))
    ids = [e.native_match_id for e in adapter.list_entries()]
    assert "J03WOH" not in ids
    assert len(ids) == 6
    assert any("DFL_05_01_other" in w for w in adapter.warnings)
    assert any("J03WOH" in w and "metadata" in w for w in adapter.warnings)


def test_competition_must_match_the_file_name() -> None:
    tree = _recorded_tree()
    for entry in tree:
        if entry["path"].endswith("J03WPY.xml"):
            entry["path"] = entry["path"].replace("DFL-COM-000002", "DFL-COM-000001")
    files = {
        p.replace("DFL-COM-000002_DFL-MAT-J03WPY", "DFL-COM-000001_DFL-MAT-J03WPY"): b
        for p, b in _metadata_files().items()
    }
    with pytest.raises(IntegrityError, match="J03WPY"):
        list(_adapter(FakeHttp(tree, files)).list_entries())
