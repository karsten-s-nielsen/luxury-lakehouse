"""The SkillCorner public adapter: GitHub `SkillCorner/opendata` with Git LFS tracking (spec §7.1, §8.4; Task 17)."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.errors import IntegrityError
from lakehouse.sources.base import ListingSource
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.github import TreeEntry
from lakehouse.sources.skillcorner_public import REPO, SkillCornerPublicAdapter

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "skillcorner"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
TRACKING_OID = "90b6289ec2181192d1793ec6240fbbe8225ff0219c7956710c6278c31a661100"  # the recorded LFS pointer


def _blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own blob hash


class FakeGitHub:
    """Serves files by path; tracking files are LFS objects behind a pointer, as in the real repository."""

    def __init__(self, files: dict[str, bytes], lfs: dict[str, bytes]) -> None:
        self.files, self.lfs = files, lfs
        self.raw_calls: list[str] = []
        self.lfs_calls: list[str] = []
        self.oid_of = {path: hashlib.sha256(data).hexdigest() for path, data in lfs.items()}

    def tree(self, repo: str, ref: str, *, lfs: Callable[[str], bool] | None = None) -> list[TreeEntry]:
        assert repo == REPO
        entries = [TreeEntry(p, _blob_sha(d), len(d), None, None) for p, d in sorted(self.files.items())]
        for path, data in sorted(self.lfs.items()):
            pointer = f"version https://git-lfs.github.com/spec/v1\noid sha256:{self.oid_of[path]}\nsize {len(data)}\n"
            selected = lfs is not None and lfs(path)
            entries.append(
                TreeEntry(
                    path,
                    _blob_sha(pointer.encode()),
                    len(pointer),
                    self.oid_of[path] if selected else None,
                    len(data) if selected else None,
                )
            )
        return entries

    def raw(self, repo: str, ref: str, path: str) -> bytes:
        self.raw_calls.append(path)
        return self.files[path]

    def lfs_object(self, repo: str, oid: str, size: int) -> bytes:
        self.lfs_calls.append(oid)
        return next(data for path, data in self.lfs.items() if self.oid_of[path] == oid)


def _repository() -> tuple[dict[str, bytes], dict[str, bytes]]:
    files: dict[str, bytes] = {"data/matches.json": (FIXTURES / "matches.json").read_bytes(), "README.md": b"x"}
    lfs: dict[str, bytes] = {}
    for match in ("1899585", "1874553"):
        base = f"data/matches/{match}/{match}"
        files[f"{base}_match.json"] = (FIXTURES / f"match_{match}.json").read_bytes()
        files[f"{base}_dynamic_events.csv"] = (FIXTURES / "dynamic_events_1899585_slice.csv").read_bytes()
        files[f"{base}_phases_of_play.csv"] = (FIXTURES / "phases_of_play_1899585_slice.csv").read_bytes()
        lfs[f"{base}_tracking_extrapolated.jsonl"] = (
            FIXTURES / "tracking_1899585_slice.jsonl"
        ).read_bytes() + match.encode()
    return files, lfs


def _adapter(github: FakeGitHub, *clauses: Any, cache: MemoryCache | None = None) -> SkillCornerPublicAdapter:
    module = ModuleConfig(module_id="skillcorner_public", active=True, filter=list(clauses) or [AllClause()])
    return SkillCornerPublicAdapter(github, module, cache or MemoryCache(), clock=lambda: NOW)


def test_listing_entries() -> None:
    github = FakeGitHub(*_repository())
    entries = list(_adapter(github).list_entries())
    assert [e.native_match_id for e in entries] == ["1874553", "1899585"]
    entry = entries[1]
    assert (entry.competition_id, entry.competition_name, entry.season_id, entry.season_name) == (
        "61",
        "A-League",
        "95",
        "2024/2025",
    )
    assert (entry.module_id, entry.visibility, entry.listed_at) == ("skillcorner_public", "public", NOW)
    assert set(entry.artifacts) == {"match", "tracking", "dynamic_events", "phases_of_play"}
    match_file = github.files["data/matches/1899585/1899585_match.json"]
    assert entry.artifacts["match"].change_signal == _blob_sha(match_file)
    assert entry.artifacts["match"].expected_sha256 is None


def test_lfs_pointer_signal() -> None:
    """The tracking change signal is the LFS pointer's `oid sha256` (§7.1), the SHA-256 of the stored file."""
    github = FakeGitHub(*_repository())
    entry = next(e for e in _adapter(github).list_entries() if e.native_match_id == "1899585")
    tracking = entry.artifacts["tracking"]
    path = "data/matches/1899585/1899585_tracking_extrapolated.jsonl"
    assert tracking.change_signal == tracking.expected_sha256 == github.oid_of[path]
    assert tracking.size == len(github.lfs[path])
    recorded = (FIXTURES / "github_tree.json").read_text(encoding="utf-8")
    assert TRACKING_OID not in recorded  # the tree holds only the pointer's blob SHA; the oid needs the pointer read


def test_fetch_tracking_uses_lfs_and_checks_sha256() -> None:
    github = FakeGitHub(*_repository())
    adapter = _adapter(github)
    entry = next(e for e in adapter.list_entries() if e.native_match_id == "1899585")
    delivery = adapter.fetch(entry, "tracking")
    assert github.lfs_calls == [entry.artifacts["tracking"].expected_sha256]
    assert delivery.content_sha256 == entry.artifacts["tracking"].expected_sha256
    path = "data/matches/1899585/1899585_tracking_extrapolated.jsonl"
    original = github.lfs[path]
    github.lfs[path] = original.replace(b'"frame": 0,', b'"frame": 9,', 1)
    github.oid_of[path] = hashlib.sha256(original).hexdigest()  # the server keeps the old oid
    with pytest.raises(IntegrityError, match="SHA-256"):
        adapter.fetch(entry, "tracking")


def test_fetch_small_file_checks_blob_sha() -> None:
    github = FakeGitHub(*_repository())
    adapter = _adapter(github)
    entry = next(e for e in adapter.list_entries() if e.native_match_id == "1899585")
    assert (
        adapter.fetch(entry, "phases_of_play").decoded
        == github.files["data/matches/1899585/1899585_phases_of_play.csv"]
    )
    github.files["data/matches/1899585/1899585_phases_of_play.csv"] += b"\n"
    with pytest.raises(IntegrityError, match="blob"):
        adapter.fetch(entry, "phases_of_play")


def test_listing_follows_the_clauses() -> None:
    github = FakeGitHub(*_repository())

    def ids(*clauses: Any) -> list[str]:
        return [e.native_match_id for e in _adapter(github, *clauses).list_entries()]

    assert len(ids(CompetitionClause(competition_id="61", seasons=["95"]))) == 2
    assert ids(CompetitionClause(competition_id="61", seasons=["94"])) == []
    assert ids(MatchClause(match_ids=["1899585"])) == ["1899585"]


def test_preview_counts() -> None:
    preview = _adapter(FakeGitHub(*_repository())).preview()
    assert [
        (t.competition_id, t.competition_name, t.season_id, t.season_name, t.match_count) for t in preview.tournaments
    ] == [("61", "A-League", "95", "2024/2025", 2)]


def test_match_files_cached_by_blob_sha() -> None:
    github, cache = FakeGitHub(*_repository()), MemoryCache()
    list(_adapter(github, cache=cache).list_entries())
    first = len(github.raw_calls)
    list(_adapter(github, cache=cache).list_entries())
    assert github.raw_calls[first:] == []


def test_unknown_files_and_missing_match_file_warn() -> None:
    files, lfs = _repository()
    files["data/matches/1899585/1899585_bodypose.jsonl"] = b"{}"
    del files["data/matches/1874553/1874553_match.json"]
    adapter = _adapter(FakeGitHub(files, lfs))
    assert [e.native_match_id for e in adapter.list_entries()] == ["1899585"]
    assert any("1899585_bodypose.jsonl" in w for w in adapter.warnings)
    assert any("1874553" in w and "match.json" in w for w in adapter.warnings)


def test_listing_protocol() -> None:
    files, lfs = _repository()
    adapter = _adapter(FakeGitHub(files, lfs))
    assert isinstance(adapter, ListingSource)
    assert adapter.listing_kinds() == ("matches",)
    delivery = adapter.fetch_listing("matches")
    assert delivery.decoded == files["data/matches.json"]
    assert (delivery.artifact, delivery.native_match_id) == ("matches", "")
    with pytest.raises(ValueError, match="unknown SkillCorner listing"):
        adapter.fetch_listing("players")
