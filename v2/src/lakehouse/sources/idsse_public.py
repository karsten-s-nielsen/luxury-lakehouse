"""The IDSSE public module: HF dataset `pysport/idsse-data` (spec §7.1, §8.4; plan Task 16).

One tree call lists every file with its git blob SHA and, for the Git LFS positions files, the LFS SHA-256. The
change signal is the LFS SHA-256 where the file is in LFS, else the git blob SHA; a fetch checks the downloaded bytes
against it. The file names give the match and competition (`DFL_<kind>_..._DFL-COM-<id>_DFL-MAT-<id>.xml`); the
competition and season names come from each match's matchinformation file (12 KB), read once and cached by its blob
SHA. The adapter reads only the `General` element of that file; the parser maps the rest.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, cast
from urllib.parse import quote

import defusedxml
import defusedxml.ElementTree

from lakehouse.config.model import ModuleConfig
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import HttpStatusError, IntegrityError, UnexpectedPayloadError
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.net.http import Expect, HttpResult, safe_url
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected
from lakehouse.sources.cache import ListingCache

__all__ = ["REPO", "RESOLVE_BASE", "TREE_URL", "IdssePublicAdapter"]

REPO: Final = "pysport/idsse-data"
REVISION: Final = "main"
TREE_URL: Final = f"https://huggingface.co/api/datasets/{REPO}/tree/{REVISION}?recursive=true"
RESOLVE_BASE: Final = f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/"
MAX_TREE_BYTES: Final = 16 * 1024 * 1024
MAX_METADATA_BYTES: Final = 4 * 1024 * 1024
_FILE: Final = re.compile(
    r"^DFL_(?P<kind>\d{2}_\d{2})_[a-z_]+_(?P<competition>DFL-COM-[A-Z0-9]+)_DFL-MAT-(?P<match>[A-Z0-9]+)\.xml$"
)
_KINDS: Final[dict[str, str]] = {"02_01": "metadata", "03_02": "events", "04_03": "positions"}
_NEXT: Final = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class HttpGetter(Protocol):
    def get(
        self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect
    ) -> HttpResult: ...


@dataclass(frozen=True)
class _HfFile:
    path: str
    size: int
    blob_sha: str
    lfs_sha256: str | None
    lfs_size: int | None


@dataclass(frozen=True)
class _Match:
    match_id: str
    competition_id: str
    files: dict[str, _HfFile]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()  # noqa: S324 - git's own blob hash


class IdssePublicAdapter:
    """The `ChannelAdapter` of module `idsse_public`. `warnings` collects files and matches it cannot list."""

    def __init__(
        self,
        http: HttpGetter,
        module: ModuleConfig,
        cache: ListingCache,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if module.module_id != "idsse_public":
            raise ValueError("IdssePublicAdapter serves the idsse_public module only")
        self._http = http
        self._module = module
        self._cache = cache
        self._clock = clock
        self.warnings: list[str] = []

    @property
    def module_id(self) -> Literal["idsse_public"]:
        return "idsse_public"

    @property
    def provider(self) -> Literal["idsse"]:
        return "idsse"

    @property
    def channel(self) -> Literal["public"]:
        return "public"

    def _get(self, url: str, *, max_bytes: int, expect: Expect) -> HttpResult:
        result = self._http.get(url, max_bytes=max_bytes, expect=expect)
        if result.status != 200:
            raise HttpStatusError(result.status, safe_url(url))
        return result

    def _tree(self) -> list[_HfFile]:
        files: list[_HfFile] = []
        url: str | None = TREE_URL
        while url is not None:
            result = self._get(url, max_bytes=MAX_TREE_BYTES, expect="json")
            payload = json.loads(result.body)
            if not isinstance(payload, list):
                raise UnexpectedPayloadError(f"the HF tree of {REPO} is not a JSON list")
            for item in cast("list[dict[str, Any]]", payload):
                if item.get("type") != "file":
                    continue
                lfs = cast("dict[str, Any] | None", item.get("lfs"))
                files.append(
                    _HfFile(
                        path=str(item["path"]),
                        size=int(item["size"]),
                        blob_sha=str(item["oid"]),
                        lfs_sha256=str(lfs["oid"]) if lfs else None,
                        lfs_size=int(lfs["size"]) if lfs else None,
                    )
                )
            following = _NEXT.search(result.headers.get("Link", ""))
            url = following.group(1) if following else None
        return files

    def _matches(self) -> list[_Match]:
        """The matches of the tree, sorted by id; unknown DFL file names become warnings."""
        found: dict[str, _Match] = {}
        for file in self._tree():
            name = file.path.rsplit("/", 1)[-1]
            match = _FILE.match(name)
            if match is None or match.group("kind") not in _KINDS:
                if name.startswith("DFL_"):
                    self.warnings.append(f"{REPO}: {file.path} is not a known IDSSE file kind; it is not listed")
                continue
            match_id = canonical_native_match_id("idsse", "public", match.group("match"))
            entry = found.setdefault(match_id, _Match(match_id, match.group("competition"), {}))
            if entry.competition_id != match.group("competition"):
                raise IntegrityError(f"{REPO}: the files of {match_id} name two competitions")
            entry.files[_KINDS[match.group("kind")]] = file
        return [found[key] for key in sorted(found)]

    def _general(self, match: _Match) -> dict[str, str] | None:
        """The `General` attributes of the match's matchinformation file, or `None` (with a warning) without one."""
        file = match.files.get("metadata")
        if file is None:
            self.warnings.append(f"{REPO}: {match.match_id} has no metadata file; it is not listed")
            return None
        data = self._cache.get(file.blob_sha)
        if data is None:
            data = self._get(RESOLVE_BASE + quote(file.path), max_bytes=MAX_METADATA_BYTES, expect="binary").body
            if _git_blob_sha(data) != file.blob_sha:
                raise IntegrityError(f"{file.path} does not match its git blob SHA {file.blob_sha}")
            self._cache.put(file.blob_sha, data)
        try:
            general = defusedxml.ElementTree.fromstring(data, forbid_dtd=True).find("MatchInformation/General")
        except (defusedxml.DefusedXmlException, defusedxml.ElementTree.ParseError) as exc:
            raise UnexpectedPayloadError(f"{file.path} is not readable matchinformation XML: {exc}") from exc
        if general is None:
            raise UnexpectedPayloadError(f"{file.path} has no MatchInformation/General element")
        values = dict(general.attrib)
        if values.get("CompetitionId") != match.competition_id:
            raise IntegrityError(
                f"{REPO}: {match.match_id} is filed under {match.competition_id}, but its metadata names "
                f"{values.get('CompetitionId')!r}"
            )
        return values

    def _listed(self) -> Iterator[tuple[_Match, dict[str, str]]]:
        self.warnings = []
        for match in self._matches():
            general = self._general(match)
            if general is not None:
                yield match, general

    def list_entries(self) -> Iterator[SourceEntry]:
        """Every match of the tournaments that the module's clauses select."""
        wanted = selected_match_ids(self._module)
        listed_at = self._clock()
        for match, general in self._listed():
            season = general.get("SeasonId", "")
            if not tournament_selected(self._module, match.competition_id, season):
                continue
            if wanted is not None and match.match_id not in wanted:
                continue
            yield SourceEntry(
                module_id="idsse_public",
                native_match_id=match.match_id,
                competition_id=match.competition_id,
                competition_name=general.get("CompetitionName"),
                season_id=season or None,
                season_name=general.get("Season"),
                visibility="public",
                artifacts={
                    artifact: ArtifactRef(
                        key=file.path,
                        change_signal=file.lfs_sha256 or file.blob_sha,
                        expected_sha256=file.lfs_sha256,
                        size=file.lfs_size if file.lfs_size is not None else file.size,
                    )
                    for artifact, file in sorted(match.files.items())
                },
                listed_at=listed_at,
            )

    def preview(self) -> PreviewResult:
        """One tournament per competition-season, with the exact match count (the tree lists every match)."""
        groups: dict[tuple[str, str], tuple[dict[str, str], int]] = {}
        for match, general in self._listed():
            key = (match.competition_id, general.get("SeasonId", ""))
            first, count = groups.get(key, (general, 0))
            groups[key] = (first, count + 1)
        tournaments = [
            PreviewTournament(
                competition_id=competition,
                competition_name=general.get("CompetitionName", competition),
                season_id=season,
                season_name=general.get("Season", season),
                match_count=count,
                count_note=None,
            )
            for (competition, season), (general, count) in sorted(groups.items())
        ]
        notes = list(self.warnings)
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=notes)

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"IDSSE match {entry.native_match_id} has no {artifact} artifact")
        if ref.size is None or ref.change_signal is None:
            raise IntegrityError(f"{ref.key}: the listing gave no size or change signal")
        data = self._get(RESOLVE_BASE + quote(ref.key), max_bytes=ref.size, expect="binary").body
        if ref.expected_sha256 is not None:
            if len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.expected_sha256:
                raise IntegrityError(
                    f"{ref.key} changed since the listing: it no longer matches LFS SHA-256 {ref.expected_sha256}"
                )
        elif _git_blob_sha(data) != ref.change_signal:
            raise IntegrityError(f"{ref.key} changed since the listing: it no longer matches blob {ref.change_signal}")
        return Delivery.received(
            provider="idsse",
            channel="public",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=data,
            visibility="public",
            change_signal=ref.change_signal,
            fetched_at=self._clock(),
        )
