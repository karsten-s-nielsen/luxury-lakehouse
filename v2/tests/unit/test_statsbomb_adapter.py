"""The StatsBomb public adapter (spec §7.1, §8.4, §29.13; plan Task 15)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.errors import IntegrityError
from lakehouse.sources.base import ListingSource
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.github import TreeEntry
from lakehouse.sources.statsbomb_public import StatsBombPublicAdapter

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "statsbomb"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def _blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own blob hash


class FakeGitHub:
    """Serves files by repository path; `tree` gives their real git blob SHAs."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.raw_calls: list[str] = []
        self.tree_calls = 0

    def tree(self, repo: str, ref: str, *, lfs: Callable[[str], bool] | None = None) -> list[TreeEntry]:
        self.tree_calls += 1
        return [
            TreeEntry(path=p, blob_sha=_blob_sha(d), size=len(d), lfs_oid=None, lfs_size=None)
            for p, d in sorted(self.files.items())
        ]

    def raw(self, repo: str, ref: str, path: str) -> bytes:
        assert repo == "statsbomb/open-data"
        self.raw_calls.append(path)
        return self.files[path]


def _files(*, three_sixty_for: tuple[int, ...] = (9100001, 9100002)) -> dict[str, bytes]:
    files = {
        "data/competitions.json": (FIXTURES / "competitions.json").read_bytes(),
        "data/matches/43/106.json": (FIXTURES / "matches_43_106.json").read_bytes(),
        "data/matches/43/3.json": (FIXTURES / "matches_43_3.json").read_bytes(),
        "data/matches/9/281.json": (FIXTURES / "matches_9_281.json").read_bytes(),
    }
    for match in (9100001, 9100002, 9100003, 9100011, 9100021):
        files[f"data/events/{match}.json"] = (FIXTURES / "events_9100001.json").read_bytes()
        files[f"data/lineups/{match}.json"] = (FIXTURES / "lineups_9100001.json").read_bytes()
    for match in three_sixty_for:
        files[f"data/three-sixty/{match}.json"] = (FIXTURES / "three_sixty_9100001.json").read_bytes()
    return files


def _module(*clauses: object, only_360: bool = False) -> ModuleConfig:
    return ModuleConfig(
        module_id="statsbomb_public",
        active=True,
        filter=list(clauses),  # type: ignore[arg-type]
        statsbomb_360_only=only_360,
    )


def _adapter(
    github: FakeGitHub, module: ModuleConfig | None = None, cache: MemoryCache | None = None
) -> StatsBombPublicAdapter:
    return StatsBombPublicAdapter(
        github,
        module or _module(CompetitionClause(competition_id="43", seasons=["106"])),
        cache or MemoryCache(),
        clock=lambda: NOW,
    )


def test_listing_entries() -> None:
    github = FakeGitHub(_files())
    entries = list(_adapter(github).list_entries())
    assert [e.native_match_id for e in entries] == ["9100001", "9100002", "9100003"]
    first = entries[0]
    assert (first.competition_id, first.competition_name, first.season_id, first.season_name) == (
        "43",
        "FIFA World Cup",
        "106",
        "2022",
    )
    assert first.visibility == "public"
    assert first.listed_at == NOW
    assert first.artifacts["events"].change_signal == _blob_sha(github.files["data/events/9100001.json"])
    assert set(first.artifacts) == {"events", "lineups", "three_sixty"}
    assert set(entries[2].artifacts) == {"events", "lineups"}


def test_listing_follows_the_clauses() -> None:
    github = FakeGitHub(_files())
    both = _module(CompetitionClause(competition_id="43", seasons="all"))
    assert {e.native_match_id for e in _adapter(github, both).list_entries()} == {
        "9100001",
        "9100002",
        "9100003",
        "9100011",
    }
    one = _module(MatchClause(match_ids=["9100021"]))
    assert [e.native_match_id for e in _adapter(github, one).list_entries()] == ["9100021"]
    every = _module(AllClause())
    assert len(list(_adapter(github, every).list_entries())) == 5


def test_has_360_from_tree() -> None:
    entries = list(_adapter(FakeGitHub(_files())).list_entries())
    assert {e.native_match_id: e.has_360 for e in entries} == {"9100001": True, "9100002": True, "9100003": False}


def test_360_status_mismatch_warns() -> None:
    # 9100002 says "available" but has no three-sixty file; 9100003 says "unscheduled" but has one
    github = FakeGitHub(_files(three_sixty_for=(9100001, 9100003)))
    adapter = _adapter(github)
    list(adapter.list_entries())
    assert adapter.warnings == [
        "StatsBomb match 9100002: match_status_360 is 'available' but the repository has no 360 file",
        "StatsBomb match 9100003: match_status_360 is 'unscheduled' but the repository has a 360 file",
    ]
    preview = adapter.preview()
    assert any("FIFA World Cup 2022" in note and "2 matches" in note for note in preview.notes)


def test_preview_counts() -> None:
    preview = _adapter(FakeGitHub(_files())).preview()
    by_id = {(t.competition_id, t.season_id): t for t in preview.tournaments}
    assert len(preview.tournaments) == 7  # one per competition-season of competitions.json
    assert by_id[("43", "106")].match_count == 3
    assert by_id[("43", "3")].match_count == 1
    missing = by_id[("9001", "281")]  # its match list is not in the repository
    assert missing.match_count is None
    assert missing.count_note is not None
    assert preview.fetched_at == NOW


def test_preview_360_share() -> None:
    matches = json.loads((FIXTURES / "matches_43_106.json").read_bytes())
    sixty_four = [dict(matches[0], match_id=9200000 + i) for i in range(64)]
    files = _files()
    files["data/matches/43/106.json"] = json.dumps(sixty_four).encode()
    for match in sixty_four:
        files[f"data/three-sixty/{match['match_id']}.json"] = b"[]"
    preview = _adapter(FakeGitHub(files)).preview()
    wc22 = next(t for t in preview.tournaments if (t.competition_id, t.season_id) == ("43", "106"))
    assert wc22.match_count_360 == 64
    assert wc22.share_360_text() == "64 matches · 64 with 360 (100 %)"
    unreadable = next(t for t in preview.tournaments if (t.competition_id, t.season_id) == ("9002", "281"))
    assert unreadable.match_count_360 is None
    assert unreadable.share_360_note is not None
    assert unreadable.share_360_note.startswith("360 share not available")
    assert unreadable.share_360_text() == unreadable.share_360_note  # never a guessed number


def test_preview_competition_available_360() -> None:
    preview = _adapter(FakeGitHub(_files())).preview()
    by_id = {(t.competition_id, t.season_id): t for t in preview.tournaments}
    assert by_id[("9", "281")].competition_available_360 == datetime(2025, 11, 15, 23, 17, 41, 827093)
    assert by_id[("43", "106")].available_360_text() == "360 data since 2026-05-04"
    assert by_id[("43", "3")].competition_available_360 is None
    assert by_id[("43", "3")].available_360_text() == "no 360 data for this season"


def test_match_lists_cached_by_blob_sha() -> None:
    github = FakeGitHub(_files())
    cache = MemoryCache()
    _adapter(github, cache=cache).preview()
    first = [p for p in github.raw_calls if p.startswith("data/matches/")]
    assert first
    github.raw_calls.clear()
    _adapter(github, cache=cache).preview()  # unchanged blob SHAs: no match list download
    assert [p for p in github.raw_calls if p.startswith("data/matches/")] == []


def test_fetch_checks_the_blob_sha() -> None:
    github = FakeGitHub(_files())
    adapter = _adapter(github)
    entry = next(iter(adapter.list_entries()))
    delivery = adapter.fetch(entry, "events")
    assert delivery.decoded == github.files["data/events/9100001.json"]
    assert delivery.change_signal == entry.artifacts["events"].change_signal
    assert (delivery.provider, delivery.channel, delivery.visibility) == ("statsbomb", "public", "public")
    github.files["data/events/9100001.json"] = b"[]"  # the bytes changed after the listing
    with pytest.raises(IntegrityError, match="blob"):
        adapter.fetch(entry, "events")
    with pytest.raises(ValueError, match="three_sixty"):
        adapter.fetch(next(e for e in adapter.list_entries() if e.native_match_id == "9100003"), "three_sixty")


def test_listing_protocol() -> None:
    """Spec §10.5: the competitions file and the match list of every selected tournament are source listings."""
    adapter = _adapter(FakeGitHub(_files()))
    assert isinstance(adapter, ListingSource)
    assert adapter.listing_kinds() == ("competitions", "matches/43/106")
    delivery = adapter.fetch_listing("matches/43/106")
    assert delivery.decoded == (FIXTURES / "matches_43_106.json").read_bytes()
    assert (delivery.artifact, delivery.native_match_id, delivery.channel) == ("matches/43/106", "", "public")
    assert delivery.change_signal == _blob_sha(delivery.decoded)
    with pytest.raises(ValueError, match="unknown StatsBomb listing"):
        adapter.fetch_listing("lineups")
