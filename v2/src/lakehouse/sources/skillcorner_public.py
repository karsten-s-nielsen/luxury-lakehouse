"""The SkillCorner public module: GitHub `SkillCorner/opendata` (spec §7.1, §8.4; plan Task 17).

One tree call lists every file with its git blob SHA. The tracking files are Git LFS objects: the tree holds only
their pointer, so the adapter reads each pointer (133 bytes) to get the LFS `oid sha256` and size, which are the
change signal and the expected SHA-256 of the file. A tracking download goes through the LFS batch API and uses
SkillCorner's bandwidth quota (§26 item 7), so it happens only on a fetch, never on a listing. The competition and
season come from each match.json, read once and cached by its blob SHA.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, cast

from lakehouse.config.model import ModuleConfig
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import IntegrityError, UnexpectedPayloadError
from lakehouse.exact import loads_exact
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected
from lakehouse.sources.cache import ListingCache
from lakehouse.sources.github import TreeEntry

__all__ = ["REPO", "SkillCornerPublicAdapter"]

REPO: Final = "SkillCorner/opendata"
REF: Final = "master"
_MATCH_FILE: Final = re.compile(
    r"^data/matches/(?P<folder>\d+)/(?P<match>\d+)_(?P<kind>[a-z_]+)\.(?P<ext>json|jsonl|csv)$"
)
_KINDS: Final[dict[str, str]] = {
    "match.json": "match",
    "tracking_extrapolated.jsonl": "tracking",
    "dynamic_events.csv": "dynamic_events",
    "phases_of_play.csv": "phases_of_play",
}
_TRACKING_SUFFIX: Final = "_tracking_extrapolated.jsonl"


class GitHubLike(Protocol):
    def tree(self, repo: str, ref: str, *, lfs: Callable[[str], bool] | None = None) -> list[TreeEntry]: ...

    def raw(self, repo: str, ref: str, path: str) -> bytes: ...

    def lfs_object(self, repo: str, oid: str, size: int) -> bytes: ...


@dataclass(frozen=True)
class _Match:
    match_id: str
    files: dict[str, TreeEntry]


_MATCH_LIST: Final = "data/matches.json"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own blob hash


class SkillCornerPublicAdapter:
    """The `ChannelAdapter` of module `skillcorner_public`. `warnings` collects files and matches it cannot list."""

    def __init__(
        self,
        github: GitHubLike,
        module: ModuleConfig,
        cache: ListingCache,
        *,
        clock: Callable[[], datetime] = _utc_now,
        ref: str = REF,
    ) -> None:
        if module.module_id != "skillcorner_public":
            raise ValueError("SkillCornerPublicAdapter serves the skillcorner_public module only")
        self._github = github
        self._module = module
        self._cache = cache
        self._clock = clock
        self._ref = ref
        self.warnings: list[str] = []

    @property
    def module_id(self) -> Literal["skillcorner_public"]:
        return "skillcorner_public"

    @property
    def provider(self) -> Literal["skillcorner"]:
        return "skillcorner"

    @property
    def channel(self) -> Literal["public"]:
        return "public"

    def listing_kinds(self) -> tuple[str, ...]:
        """The source listing (§10.5): `data/matches.json`, the repository's match list."""
        return ("matches",)

    def fetch_listing(self, kind: str) -> Delivery:
        if kind != "matches":
            raise ValueError(f"unknown SkillCorner listing {kind!r}")
        entry = next((e for e in self._github.tree(REPO, self._ref) if e.path == _MATCH_LIST), None)
        if entry is None:
            raise FileNotFoundError(f"{REPO} has no {_MATCH_LIST}")
        data = self._github.raw(REPO, self._ref, entry.path)
        if _git_blob_sha(data) != entry.blob_sha:
            raise IntegrityError(f"{entry.path} does not match its git blob SHA {entry.blob_sha}")
        return Delivery.received(
            provider="skillcorner",
            channel="public",
            native_match_id="",
            artifact=kind,
            raw=data,
            visibility="public",
            change_signal=entry.blob_sha,
            fetched_at=self._clock(),
        )

    def _matches(self) -> list[_Match]:
        found: dict[str, _Match] = {}
        for entry in self._github.tree(REPO, self._ref, lfs=lambda path: path.endswith(_TRACKING_SUFFIX)):
            if not entry.path.startswith("data/matches/"):
                continue
            match = _MATCH_FILE.match(entry.path)
            kind = _KINDS.get(f"{match.group('kind')}.{match.group('ext')}") if match else None
            if match is None or kind is None or match.group("folder") != match.group("match"):
                self.warnings.append(f"{REPO}: {entry.path} is not a known SkillCorner match file; it is not listed")
                continue
            match_id = canonical_native_match_id("skillcorner", "public", match.group("match"))
            found.setdefault(match_id, _Match(match_id, {})).files[kind] = entry
        return [found[key] for key in sorted(found)]

    def _match_file(self, match: _Match) -> dict[str, Any] | None:
        entry = match.files.get("match")
        if entry is None:
            self.warnings.append(f"{REPO}: {match.match_id} has no match.json; it is not listed")
            return None
        data = self._cache.get(entry.blob_sha)
        if data is None:
            data = self._github.raw(REPO, self._ref, entry.path)
            if _git_blob_sha(data) != entry.blob_sha:
                raise IntegrityError(f"{entry.path} does not match its git blob SHA {entry.blob_sha}")
            self._cache.put(entry.blob_sha, data)
        payload = loads_exact(data)
        if not isinstance(payload, dict):
            raise UnexpectedPayloadError(f"{entry.path} must hold a JSON object")
        return cast("dict[str, Any]", payload)

    def _listed(self) -> Iterator[tuple[_Match, dict[str, str]]]:
        self.warnings = []
        for match in self._matches():
            payload = self._match_file(match)
            if payload is None:
                continue
            edition = cast("dict[str, Any]", payload.get("competition_edition") or {})
            competition = cast("dict[str, Any]", edition.get("competition") or {})
            season = cast("dict[str, Any]", edition.get("season") or {})
            yield (
                match,
                {
                    "competition_id": str(competition.get("id", "")),
                    "competition_name": str(competition.get("name", "")),
                    "season_id": str(season.get("id", "")),
                    "season_name": str(season.get("name", "")),
                },
            )

    def list_entries(self) -> Iterator[SourceEntry]:
        """Every match of the tournaments that the module's clauses select."""
        wanted = selected_match_ids(self._module)
        listed_at = self._clock()
        for match, names in self._listed():
            if not tournament_selected(self._module, names["competition_id"], names["season_id"]):
                continue
            if wanted is not None and match.match_id not in wanted:
                continue
            artifacts: dict[str, ArtifactRef] = {}
            for artifact, entry in sorted(match.files.items()):
                if artifact == "tracking" and entry.lfs_oid is None:
                    raise IntegrityError(f"{entry.path} is not a Git LFS pointer: the tracking file has no SHA-256")
                artifacts[artifact] = ArtifactRef(
                    key=entry.path,
                    change_signal=entry.lfs_oid or entry.blob_sha,
                    expected_sha256=entry.lfs_oid,
                    size=entry.lfs_size if entry.lfs_size is not None else entry.size,
                )
            yield SourceEntry(
                module_id="skillcorner_public",
                native_match_id=match.match_id,
                competition_id=names["competition_id"] or None,
                competition_name=names["competition_name"] or None,
                season_id=names["season_id"] or None,
                season_name=names["season_name"] or None,
                visibility="public",
                artifacts=artifacts,
                listed_at=listed_at,
            )

    def preview(self) -> PreviewResult:
        """One tournament per competition-season, with the exact match count (the tree lists every match)."""
        groups: dict[tuple[str, str], tuple[dict[str, str], int]] = {}
        for _, names in self._listed():
            key = (names["competition_id"], names["season_id"])
            first, count = groups.get(key, (names, 0))
            groups[key] = (first, count + 1)
        tournaments = [
            PreviewTournament(
                competition_id=competition,
                competition_name=names["competition_name"],
                season_id=season,
                season_name=names["season_name"],
                match_count=count,
                count_note=None,
            )
            for (competition, season), (names, count) in sorted(groups.items())
        ]
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=list(self.warnings))

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"SkillCorner match {entry.native_match_id} has no {artifact} artifact")
        if ref.expected_sha256 is not None:
            if ref.size is None:
                raise IntegrityError(f"{ref.key}: the listing gave no LFS size")
            data = self._github.lfs_object(REPO, ref.expected_sha256, ref.size)
            if len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.expected_sha256:
                raise IntegrityError(f"{ref.key} does not match its LFS SHA-256 {ref.expected_sha256}")
        else:
            data = self._github.raw(REPO, self._ref, ref.key)
            if _git_blob_sha(data) != ref.change_signal:
                raise IntegrityError(
                    f"{ref.key} changed since the listing: it no longer matches blob {ref.change_signal}"
                )
        return Delivery.received(
            provider="skillcorner",
            channel="public",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=data,
            visibility="public",
            change_signal=ref.change_signal,
            fetched_at=self._clock(),
        )
