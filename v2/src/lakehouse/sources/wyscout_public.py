"""The Wyscout public module: figshare collection 4415000 (spec §7.1, §8.4, §29.15; plan Task 19).

The collection (Pappalardo and Massucco 2019, CC BY 4.0) holds 12 articles with one file each. The adapter reads
eight of them: the matches and events (one zip file each, with one member per competition), the competitions, teams,
players and coaches, and the two id-to-name mappings. Skipped by rule, with no warning: the data paper, the plotting
code, PlayeRanks (a derived metric) and the referees file, which is truncated at the source (owner, §29.15). Another
article becomes a warning, and a missing known article fails the listing.

The change signal of a file is `<md5>@v<article version>` from the article API; a download is checked against the
size and md5 of that answer, and cached by md5 (a changed file has a new key). A match's delivery is its
competition's member of the zip (`matches_<name>.json`, `events_<name>.json`): the parser takes the match from it.
The listing reads only matches.zip (0.6 MB) and competitions.json; events.zip (77 MB) is read at the first fetch, and
the extracted member serves every match of its competition.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import json
import re
import zipfile
import zlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, cast

from lakehouse.config.model import ModuleConfig
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import HttpStatusError, IntegrityError, UnexpectedPayloadError
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.net.http import Expect, HttpResult, safe_url
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected
from lakehouse.sources.cache import ListingCache

__all__ = [
    "ARTICLE_URL",
    "COLLECTION_ID",
    "COLLECTION_URL",
    "DOWNLOAD_URL",
    "MAX_MEMBER_BYTES",
    "WyscoutPublicAdapter",
]

COLLECTION_ID: Final = 4415000
API: Final = "https://api.figshare.com/v2"
COLLECTION_URL: Final = f"{API}/collections/{COLLECTION_ID}/articles"
ARTICLE_URL: Final = API + "/articles/{article_id}"
# The ndownloader host, not figshare.com/ndownloader (which answers 202; IRIS and v1 note the same).
DOWNLOAD_URL: Final = "https://ndownloader.figshare.com/files/{file_id}"
PAGE_SIZE: Final = 100
MAX_API_BYTES: Final = 4 * 1024 * 1024
MAX_FILE_BYTES: Final = 256 * 1024 * 1024  # events.zip is 77,323,413 bytes
MAX_MEMBER_BYTES = 768 * 1024 * 1024  # a member above this is refused unread (a zip bomb, or far beyond the sizes)

Artifact = Literal["matches", "events"]
_LISTING_KINDS: Final = ("competitions", "teams", "players", "coaches", "event_names", "tag_names")
ListingKind = Literal["competitions", "teams", "players", "coaches", "event_names", "tag_names"]
# kind -> (figshare article id, file name)
_ARTICLES: Final[dict[str, tuple[int, str]]] = {
    "matches": (7770422, "matches.zip"),
    "events": (7770599, "events.zip"),
    "competitions": (7765316, "competitions.json"),
    "teams": (7765310, "teams.json"),
    "players": (7765196, "players.json"),
    "coaches": (8082650, "coaches.json"),
    "event_names": (11743836, "eventid2name.csv"),
    "tag_names": (11743818, "tags2name.csv"),
}
# The data paper, the plotting code, PlayeRanks and the truncated referees file (spec §29.15).
_SKIPPED: Final = frozenset({11473374, 11473365, 9361148, 8082665})
_MEMBER: Final = re.compile(r"^matches_(?P<name>[A-Za-z_]+)\.json$")
_MD5: Final = re.compile(r"^[0-9a-f]{32}$")
_SIGNAL: Final = re.compile(r"^(?P<md5>[0-9a-f]{32})@v(?P<version>\d+)$")


class HttpGetter(Protocol):
    def get(
        self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect
    ) -> HttpResult: ...


@dataclass(frozen=True)
class _File:
    file_id: int
    name: str
    size: int
    md5: str
    version: int

    @property
    def signal(self) -> str:
        return f"{self.md5}@v{self.version}"


@dataclass(frozen=True)
class _Match:
    match_id: str
    competition_id: str
    season_id: str
    year: int
    member: str


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def _season_name(years: list[int]) -> str:
    """The year span of a season's match dates (Wyscout gives no season name): `2018` or `2017/2018`."""
    first, last = min(years), max(years)
    return str(first) if first == last else f"{first}/{last}"


def _member(data: bytes, file: str, name: str) -> bytes:
    """One member of a zip file. A member above the size cap is refused unread. zipfile never returns more than the
    header's `file_size` and then checks the CRC-32, so a header that understates the size fails as damaged."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise UnexpectedPayloadError(f"{file} is not a zip file: {exc}") from exc
    with archive:
        try:
            info = archive.getinfo(name)
        except KeyError as exc:
            raise IntegrityError(f"{file} has no member {name}") from exc
        if info.file_size > MAX_MEMBER_BYTES:
            raise UnexpectedPayloadError(f"{file}: {name} is {info.file_size} bytes, above {MAX_MEMBER_BYTES}")
        try:
            return archive.read(info)
        except (zipfile.BadZipFile, zlib.error) as exc:
            raise IntegrityError(f"{file}: {name} is damaged: {exc}") from exc


def _member_names(data: bytes, file: str) -> list[str]:
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            return sorted(archive.namelist())
    except zipfile.BadZipFile as exc:
        raise UnexpectedPayloadError(f"{file} is not a zip file: {exc}") from exc


class WyscoutPublicAdapter:
    """The `ChannelAdapter` of module `wyscout_public`. `warnings` collects articles and members it does not list."""

    def __init__(
        self,
        http: HttpGetter,
        module: ModuleConfig,
        cache: ListingCache,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if module.module_id != "wyscout_public":
            raise ValueError("WyscoutPublicAdapter serves the wyscout_public module only")
        self._http = http
        self._module = module
        self._cache = cache
        self._clock = clock
        self._members: dict[str, tuple[tuple[str, str], Delivery]] = {}
        self.warnings: list[str] = []

    @property
    def module_id(self) -> Literal["wyscout_public"]:
        return "wyscout_public"

    @property
    def provider(self) -> Literal["wyscout"]:
        return "wyscout"

    @property
    def channel(self) -> Literal["public"]:
        return "public"

    def _get(self, url: str, *, max_bytes: int, expect: Expect) -> HttpResult:
        result = self._http.get(url, max_bytes=max_bytes, expect=expect)
        if result.status != 200:
            raise HttpStatusError(result.status, safe_url(url))
        return result

    def _api(self, url: str) -> object:
        return json.loads(self._get(url, max_bytes=MAX_API_BYTES, expect="json").body)

    def _check_collection(self) -> None:
        """Every known article must be in the collection; an unknown one becomes a warning."""
        seen: dict[int, str] = {}
        page = 1
        while True:
            items = self._api(f"{COLLECTION_URL}?page={page}&page_size={PAGE_SIZE}")
            if not isinstance(items, list):
                raise UnexpectedPayloadError(f"figshare collection {COLLECTION_ID}: the article list is not a list")
            listed = cast("list[dict[str, Any]]", items)
            for item in listed:
                seen[int(item["id"])] = str(item.get("title"))
            if len(listed) < PAGE_SIZE:
                break
            page += 1
        known = {article for article, _ in _ARTICLES.values()}
        missing = sorted(known - set(seen))
        if missing:
            raise IntegrityError(f"figshare collection {COLLECTION_ID} no longer holds articles {missing}")
        for article, title in sorted(seen.items()):
            if article not in known and article not in _SKIPPED:
                self.warnings.append(
                    f"figshare collection {COLLECTION_ID}: article {article} {title!r} is not a known Wyscout file"
                )

    def _file(self, kind: str) -> _File:
        """The file of one known article, from the article API."""
        article_id, name = _ARTICLES[kind]
        answer = self._api(ARTICLE_URL.format(article_id=article_id))
        if not isinstance(answer, dict):
            raise UnexpectedPayloadError(f"figshare article {article_id} is not a JSON object")
        article = cast("dict[str, Any]", answer)
        files = [f for f in cast("list[dict[str, Any]]", article.get("files") or []) if f.get("name") == name]
        if len(files) != 1:
            raise UnexpectedPayloadError(f"figshare article {article_id} holds {len(files)} files named {name}")
        file = files[0]
        if file.get("is_link_only"):
            raise UnexpectedPayloadError(f"figshare article {article_id}: {name} is a link, not a file")
        md5 = str(file.get("computed_md5"))
        version = article.get("version")
        if not _MD5.match(md5) or not isinstance(version, int) or not isinstance(file.get("size"), int):
            raise UnexpectedPayloadError(f"figshare article {article_id}: {name} has no md5, size or version")
        return _File(file_id=int(file["id"]), name=name, size=int(file["size"]), md5=md5, version=version)

    def _download(self, file_id: int, md5: str, size: int, *, listed: bool) -> bytes:
        """The bytes of one figshare file, checked against the size and md5 of the article answer."""
        cached = self._cache.get(md5)
        if cached is not None:
            return cached
        data = self._get(DOWNLOAD_URL.format(file_id=file_id), max_bytes=size, expect="binary").body
        if len(data) != size or _md5(data) != md5:
            if listed:
                raise IntegrityError(
                    f"figshare file {file_id} changed since the listing: it no longer matches md5 {md5}"
                )
            raise IntegrityError(f"figshare file {file_id} does not match its md5 {md5} and size {size}")
        self._cache.put(md5, data)
        return data

    def _listed(self) -> tuple[list[_Match], dict[str, str], _File, _File]:
        """Every match of matches.zip (sorted by id), the competition names, and the two zip files."""
        self.warnings = []
        self._check_collection()
        matches_file, events_file = self._file("matches"), self._file("events")
        names_file = self._file("competitions")
        names_data = self._download(names_file.file_id, names_file.md5, names_file.size, listed=False)
        competitions = {str(c["wyId"]): str(c["name"]) for c in cast("list[dict[str, Any]]", json.loads(names_data))}
        data = self._download(matches_file.file_id, matches_file.md5, matches_file.size, listed=False)
        found: dict[str, _Match] = {}
        for member in _member_names(data, matches_file.name):
            shape = _MEMBER.match(member)
            if shape is None:
                self.warnings.append(f"{matches_file.name}: member {member} is not a competition file; not listed")
                continue
            records = json.loads(_member(data, matches_file.name, member))
            for record in cast("list[dict[str, Any]]", records):
                match_id = canonical_native_match_id("wyscout", "public", int(record["wyId"]))
                if match_id in found:
                    raise IntegrityError(f"{matches_file.name}: match {match_id} is in two members")
                found[match_id] = _Match(
                    match_id=match_id,
                    competition_id=str(record["competitionId"]),
                    season_id=str(record["seasonId"]),
                    year=int(str(record["dateutc"])[:4]),
                    member=shape.group("name"),
                )
        return [found[key] for key in sorted(found, key=int)], competitions, matches_file, events_file

    def list_entries(self) -> Iterator[SourceEntry]:
        """Every match of the tournaments that the module's clauses select."""
        matches, competitions, matches_file, events_file = self._listed()
        seasons = _season_names(matches)
        wanted = selected_match_ids(self._module)
        listed_at = self._clock()
        for match in matches:
            if not tournament_selected(self._module, match.competition_id, match.season_id):
                continue
            if wanted is not None and match.match_id not in wanted:
                continue
            yield SourceEntry(
                module_id="wyscout_public",
                native_match_id=match.match_id,
                competition_id=match.competition_id,
                competition_name=competitions.get(match.competition_id),
                season_id=match.season_id,
                season_name=seasons[(match.competition_id, match.season_id)],
                visibility="public",
                artifacts={
                    kind: ArtifactRef(
                        key=f"{file.file_id}/{kind}_{match.member}.json",
                        change_signal=file.signal,
                        expected_sha256=None,
                        size=file.size,
                    )
                    for kind, file in (("events", events_file), ("matches", matches_file))
                },
                listed_at=listed_at,
            )

    def preview(self) -> PreviewResult:
        """One tournament per competition-season, with the exact match count (matches.zip holds every match)."""
        matches, competitions, _, _ = self._listed()
        seasons = _season_names(matches)
        counts: dict[tuple[str, str], int] = {}
        for match in matches:
            key = (match.competition_id, match.season_id)
            counts[key] = counts.get(key, 0) + 1
        tournaments = [
            PreviewTournament(
                competition_id=competition,
                competition_name=competitions.get(competition, competition),
                season_id=season,
                season_name=seasons[(competition, season)],
                match_count=count,
                count_note=None,
            )
            for (competition, season), count in sorted(counts.items(), key=lambda item: (int(item[0][0]), item[0][1]))
        ]
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=list(self.warnings))

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        """The competition member that holds the match; one extraction serves every match of that member."""
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"Wyscout match {entry.native_match_id} has no {artifact} artifact")
        signal = _SIGNAL.match(ref.change_signal or "")
        file_id, _, member = ref.key.partition("/")
        if signal is None or ref.size is None or not file_id.isdigit() or not member:
            raise IntegrityError(f"{ref.key}: the listing gave no file, member, size or change signal")
        held = self._members.get(artifact)
        if held is None or held[0] != (signal.group("md5"), member):
            data = self._download(int(file_id), signal.group("md5"), ref.size, listed=True)
            delivery = Delivery.received(
                provider="wyscout",
                channel="public",
                native_match_id=entry.native_match_id,
                artifact=artifact,
                raw=_member(data, _ARTICLES[artifact][1], member),
                visibility="public",
                change_signal=ref.change_signal,
                fetched_at=self._clock(),
            )
            self._members[artifact] = ((signal.group("md5"), member), delivery)
            return delivery
        return dataclasses.replace(held[1], native_match_id=entry.native_match_id, fetched_at=self._clock())

    def listing_kinds(self) -> tuple[str, ...]:
        """The source listings (§10.5): the competitions, teams, players and coaches files and the two mappings."""
        return _LISTING_KINDS

    def fetch_listing(self, kind: str) -> Delivery:
        """One source-level listing file (spec §10.5); a listing belongs to no match (`native_match_id` "")."""
        if kind not in _LISTING_KINDS:
            raise ValueError(f"unknown Wyscout listing {kind!r}")
        file = self._file(kind)
        return Delivery.received(
            provider="wyscout",
            channel="public",
            native_match_id="",
            artifact=kind,
            raw=self._download(file.file_id, file.md5, file.size, listed=False),
            visibility="public",
            change_signal=file.signal,
            fetched_at=self._clock(),
        )


def _season_names(matches: list[_Match]) -> dict[tuple[str, str], str]:
    years: dict[tuple[str, str], list[int]] = {}
    for match in matches:
        years.setdefault((match.competition_id, match.season_id), []).append(match.year)
    return {key: _season_name(values) for key, values in years.items()}
