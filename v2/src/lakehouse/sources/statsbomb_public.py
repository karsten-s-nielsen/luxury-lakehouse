"""The StatsBomb public module: GitHub `statsbomb/open-data` (spec §7.1, §8.4, §29.13).

One tree call lists every file with its git blob SHA, which is the change signal. A match has 360 data exactly when
the tree holds `data/three-sixty/<match_id>.json`; a `match_status_360` that disagrees is reported as a warning and a
preview note, never trusted over the tree. Match lists are read per competition-season and cached by their blob SHA,
so a repeated preview downloads nothing that did not change. Every fetched file is checked against its blob SHA.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, cast

from lakehouse.config.model import ModuleConfig
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import HttpStatusError, IntegrityError
from lakehouse.exact import loads_exact
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected
from lakehouse.sources.cache import ListingCache
from lakehouse.sources.github import TreeEntry

__all__ = ["REPO", "StatsBombPublicAdapter"]

REPO: Final = "statsbomb/open-data"
REF: Final = "master"
_ARTIFACT_FOLDERS: Final = {"events": "events", "lineups": "lineups", "three_sixty": "three-sixty"}


class GitHubLike(Protocol):
    def tree(self, repo: str, ref: str, *, lfs: Callable[[str], bool] | None = None) -> list[TreeEntry]: ...

    def raw(self, repo: str, ref: str, path: str) -> bytes: ...


_LISTING_MATCHES: Final = re.compile(r"^matches/\d+/\d+$")


def _git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own object hash


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _path(artifact: str, match_id: str) -> str:
    if artifact not in _ARTIFACT_FOLDERS:
        raise ValueError(f"unknown StatsBomb artifact {artifact!r}")
    return f"data/{_ARTIFACT_FOLDERS[artifact]}/{match_id}.json"


class StatsBombPublicAdapter:
    """The `ChannelAdapter` of module `statsbomb_public`. `warnings` collects the 360 status disagreements."""

    def __init__(
        self,
        github: GitHubLike,
        module: ModuleConfig,
        cache: ListingCache,
        *,
        clock: Callable[[], datetime] = _utc_now,
        ref: str = REF,
    ) -> None:
        if module.module_id != "statsbomb_public":
            raise ValueError("StatsBombPublicAdapter serves the statsbomb_public module only")
        self._github = github
        self._module = module
        self._cache = cache
        self._clock = clock
        self._ref = ref
        self._tree: dict[str, TreeEntry] | None = None
        self.warnings: list[str] = []

    @property
    def module_id(self) -> Literal["statsbomb_public"]:
        return "statsbomb_public"

    @property
    def provider(self) -> Literal["statsbomb"]:
        return "statsbomb"

    @property
    def channel(self) -> Literal["public"]:
        return "public"

    def _files(self) -> dict[str, TreeEntry]:
        if self._tree is None:
            self._tree = {entry.path: entry for entry in self._github.tree(REPO, self._ref)}
        return self._tree

    def _read(self, path: str, *, cached: bool) -> bytes:
        entry = self._files().get(path)
        if entry is None:
            raise FileNotFoundError(f"{REPO} has no {path}")
        if cached:
            hit = self._cache.get(entry.blob_sha)
            if hit is not None:
                return hit
        data = self._github.raw(REPO, self._ref, path)
        if _git_blob_sha(data) != entry.blob_sha:
            raise IntegrityError(f"{path} does not match its git blob SHA {entry.blob_sha}")
        if cached:
            self._cache.put(entry.blob_sha, data)
        return data

    def _json_list(self, path: str) -> list[dict[str, Any]]:
        payload = loads_exact(self._read(path, cached=True))
        if not isinstance(payload, list):
            raise IntegrityError(f"{path} must hold a JSON list")
        return cast("list[dict[str, Any]]", payload)

    def _competitions(self) -> list[dict[str, Any]]:
        return self._json_list("data/competitions.json")

    def _matches(self, competition_id: int, season_id: int) -> list[dict[str, Any]]:
        return self._json_list(f"data/matches/{competition_id}/{season_id}.json")

    def _has_360(self, match_id: str) -> bool:
        return _path("three_sixty", match_id) in self._files()

    def _check_360_status(self, match: dict[str, Any], match_id: str) -> str | None:
        status = match.get("match_status_360")
        present = self._has_360(match_id)
        if (status == "available") != present:
            where = "has a 360 file" if present else "has no 360 file"
            return f"StatsBomb match {match_id}: match_status_360 is {status!r} but the repository {where}"
        return None

    def listing_kinds(self) -> tuple[str, ...]:
        """The source listings (§10.5): `competitions` and the match list `matches/<c>/<s>` of each selected
        tournament."""
        kinds = ["competitions"]
        files = self._files()
        for row in self._competitions():
            competition, season = str(row["competition_id"]), str(row["season_id"])
            path = f"data/matches/{competition}/{season}.json"
            if tournament_selected(self._module, competition, season) and path in files:
                kinds.append(f"matches/{competition}/{season}")
        return tuple(dict.fromkeys(kinds))

    def fetch_listing(self, kind: str) -> Delivery:
        """One listing file, checked against its git blob SHA (cached by it)."""
        if kind == "competitions":
            path = "data/competitions.json"
        elif _LISTING_MATCHES.match(kind):
            path = f"data/{kind}.json"
        else:
            raise ValueError(f"unknown StatsBomb listing {kind!r}")
        data = self._read(path, cached=True)
        return Delivery.received(
            provider="statsbomb",
            channel="public",
            native_match_id="",
            artifact=kind,
            raw=data,
            visibility="public",
            change_signal=self._files()[path].blob_sha,
            fetched_at=self._clock(),
        )

    def list_entries(self) -> Iterator[SourceEntry]:
        """Every match of the tournaments that the module's clauses select."""
        files = self._files()
        wanted = selected_match_ids(self._module)
        self.warnings = []
        listed_at = self._clock()
        for row in self._competitions():
            competition, season = str(row["competition_id"]), str(row["season_id"])
            if not tournament_selected(self._module, competition, season):
                continue
            try:
                matches = self._matches(int(row["competition_id"]), int(row["season_id"]))
            except FileNotFoundError:
                continue  # a competition-season without a match list holds no matches
            for match in matches:
                match_id = canonical_native_match_id("statsbomb", "public", match["match_id"])
                if wanted is not None and match_id not in wanted:
                    continue
                warning = self._check_360_status(match, match_id)
                if warning is not None:
                    self.warnings.append(warning)
                artifacts: dict[str, ArtifactRef] = {}
                for artifact in ("events", "lineups", "three_sixty"):
                    entry = files.get(_path(artifact, match_id))
                    if entry is not None:
                        artifacts[artifact] = ArtifactRef(
                            key=entry.path, change_signal=entry.blob_sha, expected_sha256=None, size=entry.size
                        )
                yield SourceEntry(
                    module_id="statsbomb_public",
                    native_match_id=match_id,
                    competition_id=competition,
                    competition_name=str(row["competition_name"]),
                    season_id=season,
                    season_name=str(row["season_name"]),
                    visibility="public",
                    artifacts=artifacts,
                    listed_at=listed_at,
                    has_360=self._has_360(match_id),
                )

    def preview(self) -> PreviewResult:
        """One tournament per competition-season, with the exact 360 share where the match list is readable."""
        tournaments: list[PreviewTournament] = []
        notes: list[str] = []
        for row in self._competitions():
            available = row.get("match_available_360")
            base: dict[str, Any] = {
                "competition_id": str(row["competition_id"]),
                "competition_name": str(row["competition_name"]),
                "season_id": str(row["season_id"]),
                "season_name": str(row["season_name"]),
                "competition_available_360": datetime.fromisoformat(available) if available else None,
            }
            try:
                matches = self._matches(int(row["competition_id"]), int(row["season_id"]))
            except (FileNotFoundError, IntegrityError, HttpStatusError) as exc:
                reason = f"the match list could not be read ({type(exc).__name__})"
                tournaments.append(
                    PreviewTournament(
                        **base,
                        match_count=None,
                        count_note=f"match count not available: {reason}",
                        match_count_360=None,
                        share_360_note=f"360 share not available: {reason}",
                    )
                )
                continue
            ids = [canonical_native_match_id("statsbomb", "public", m["match_id"]) for m in matches]
            disagreements = [w for m, i in zip(matches, ids, strict=True) if (w := self._check_360_status(m, i))]
            if disagreements:
                notes.append(
                    f"{base['competition_name']} {base['season_name']}: {len(disagreements)} matches disagree on "
                    "360 data (match_status_360 against the three-sixty files)"
                )
            tournaments.append(
                PreviewTournament(
                    **base,
                    match_count=len(matches),
                    count_note=None,
                    match_count_360=sum(1 for i in ids if self._has_360(i)),
                )
            )
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=notes)

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"StatsBomb match {entry.native_match_id} has no {artifact} artifact")
        data = self._github.raw(REPO, self._ref, ref.key)
        if _git_blob_sha(data) != ref.change_signal:
            raise IntegrityError(f"{ref.key} changed since the listing: it no longer matches blob {ref.change_signal}")
        return Delivery.received(
            provider="statsbomb",
            channel="public",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=data,
            visibility="public",
            change_signal=ref.change_signal,
            fetched_at=self._clock(),
        )
