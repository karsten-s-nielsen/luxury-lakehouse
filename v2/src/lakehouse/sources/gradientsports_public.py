"""The Gradient Sports public module: a public Google Drive folder (spec §7.1, §13.1, §17.3, §29.15; plan Task 18).

Drive's public `embeddedfolderview` page lists each folder's items with their file id and last-modified day; it gives
no size and no checksum. The root folder holds the subfolders `Event Data`, `Metadata`, `Rosters` and `Tracking Data`
(one file per match, named `<match>.json` or `<match>.jsonl.bz2`) and the listings `competitions.csv` and
`players.csv`. Skipped by rule, with no warning: the specification PDFs, Google documents (the change log), and the
dated drop subfolders of `Event Data` (the owner chose the current root files, §29.15). Any other item becomes a
warning. The change signal is `<file id>@<day>` (a day-level date, so weak); the run compares the content hash after
download (`content_changed`, §17.3).

A download of a large file first answers with Drive's virus-scan page. The adapter follows its confirm form once,
after checking that the form names the requested file; any other HTML page, or a second confirm page, raises
`UnexpectedPayloadError` and is never parsed as data.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import io
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from typing import Final, Literal, Protocol, cast
from urllib.parse import urlencode

from lakehouse.config.model import ModuleConfig
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import HttpStatusError, UnexpectedPayloadError
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.net.http import Expect, HttpResult, safe_url
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected
from lakehouse.sources.cache import ListingCache

__all__ = ["DOWNLOAD_URL", "FOLDER_ID", "LISTING_URL", "GradientSportsPublicAdapter", "content_changed"]

FOLDER_ID: Final = "1_a_q1e9CXeEPJ3GdCv_3-rNO3gPqacfa"
LISTING_URL: Final = "https://drive.google.com/embeddedfolderview"
DOWNLOAD_URL: Final = "https://drive.usercontent.google.com/download"
MAX_LISTING_BYTES: Final = 4 * 1024 * 1024
MAX_FILE_BYTES: Final = 2 * 1024**3  # Drive gives no size; GS tracking files are about 50 MB compressed
_FOLDERS: Final[dict[str, str]] = {
    "Event Data": "events",
    "Metadata": "metadata",
    "Rosters": "roster",
    "Tracking Data": "tracking",
}
_NAMES: Final[dict[str, re.Pattern[str]]] = {
    "events": re.compile(r"^(\d+)\.json$"),
    "metadata": re.compile(r"^(\d+)\.json$"),
    "roster": re.compile(r"^(\d+)\.json$"),
    "tracking": re.compile(r"^(\d+)\.jsonl\.bz2$"),
}
_LISTINGS: Final = frozenset({"competitions.csv", "players.csv"})
_LISTING_FILES: Final = {"competitions": "competitions.csv", "players": "players.csv"}
_DAY: Final = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{2})$")
_TIME: Final = re.compile(r"^\d{1,2}:\d{2}\s?[AP]M$")
_HTML_START: Final = (b"<!doctype html", b"<html")
_TOTAL: Final = re.compile(r"^bytes \d+-\d+/(\d+)$")


class HttpGetter(Protocol):
    def get(
        self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect
    ) -> HttpResult: ...


@dataclass(frozen=True)
class _Item:
    item_id: str
    title: str
    modified: str
    folder: bool
    google_doc: bool


class _FolderPage(HTMLParser):
    """The items of an `embeddedfolderview` page."""

    def __init__(self) -> None:
        super().__init__()
        self.items: list[_Item] = []
        self._id: str | None = None
        self._href = ""
        self._title: list[str] = []
        self._modified: list[str] = []
        self._capture: Literal["title", "modified", None] = None
        self._in_modified = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name: value or "" for name, value in attrs}
        classes = values.get("class", "").split()
        if tag == "div" and "flip-entry" in classes and values.get("id", "").startswith("entry-"):
            self._flush()
            self._id = values["id"].removeprefix("entry-")
        elif tag == "a" and self._id is not None and not self._href:
            self._href = values.get("href", "")
        elif tag == "div" and "flip-entry-title" in classes:
            self._capture = "title"
        elif tag == "div" and "flip-entry-last-modified" in classes:
            self._in_modified = True
        elif tag == "div" and self._in_modified and not classes and not self._modified:
            self._capture = "modified"

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._capture is not None:
            self._capture = None

    def handle_data(self, data: str) -> None:
        if self._capture == "title":
            self._title.append(data)
        elif self._capture == "modified":
            self._modified.append(data)

    def _flush(self) -> None:
        if self._id is not None:
            self.items.append(
                _Item(
                    item_id=self._id,
                    title="".join(self._title).strip(),
                    modified="".join(self._modified).strip(),
                    folder="/drive/folders/" in self._href,
                    google_doc="docs.google.com" in self._href,
                )
            )
        self._id, self._href, self._title, self._modified, self._in_modified = None, "", [], [], False

    def close(self) -> None:
        super().close()
        self._flush()


class _ConfirmForm(HTMLParser):
    """The `download-form` of Drive's virus-scan page: its action and hidden inputs."""

    def __init__(self) -> None:
        super().__init__()
        self.action: str | None = None
        self.fields: dict[str, str] = {}
        self._inside = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {name: value or "" for name, value in attrs}
        if tag == "form" and values.get("id") == "download-form":
            self.action, self._inside = values.get("action"), True
        elif tag == "input" and self._inside and values.get("type") == "hidden" and values.get("name"):
            self.fields[values["name"]] = values.get("value", "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self._inside = False


@dataclass
class _Match:
    match_id: str
    files: dict[str, _Item] = field(default_factory=dict[str, _Item])


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _is_html(body: bytes) -> bool:
    return body[:64].lstrip().lower().startswith(_HTML_START)


def content_changed(delivery: Delivery, known_content_sha256: str | None) -> bool:
    """Whether a downloaded file must be re-parsed: its content hash differs from the one in lineage (§17.3)."""
    return delivery.content_sha256 != known_content_sha256


class GradientSportsPublicAdapter:
    """The `ChannelAdapter` of module `gradientsports_public`. `warnings` collects items it cannot list."""

    def __init__(
        self,
        http: HttpGetter,
        module: ModuleConfig,
        cache: ListingCache,
        *,
        clock: Callable[[], datetime] = _utc_now,
        folder_id: str = FOLDER_ID,
    ) -> None:
        if module.module_id != "gradientsports_public":
            raise ValueError("GradientSportsPublicAdapter serves the gradientsports_public module only")
        self._http = http
        self._module = module
        self._cache = cache
        self._clock = clock
        self._folder_id = folder_id
        self.warnings: list[str] = []

    @property
    def module_id(self) -> Literal["gradientsports_public"]:
        return "gradientsports_public"

    @property
    def provider(self) -> Literal["gradientsports"]:
        return "gradientsports"

    @property
    def channel(self) -> Literal["public"]:
        return "public"

    def _result(
        self, url: str, *, max_bytes: int, expect: Expect, headers: Mapping[str, str] | None = None
    ) -> HttpResult:
        result = self._http.get(url, headers=headers, max_bytes=max_bytes, expect=expect)
        if result.status not in (200, 206):
            raise HttpStatusError(result.status, safe_url(url))
        return result

    def _get(self, url: str, *, max_bytes: int, expect: Expect) -> bytes:
        return self._result(url, max_bytes=max_bytes, expect=expect).body

    def _folder(self, folder_id: str) -> list[_Item]:
        page = _FolderPage()
        body = self._get(f"{LISTING_URL}?{urlencode({'id': folder_id})}", max_bytes=MAX_LISTING_BYTES, expect="text")
        page.feed(body.decode("utf-8"))
        page.close()
        return page.items

    def _day(self, item: _Item) -> str:
        match = _DAY.match(item.modified)
        if match is not None:
            month, day, year = (int(part) for part in match.groups())
            return date(2000 + year, month, day).isoformat()
        if _TIME.match(item.modified):
            return self._clock().date().isoformat()  # Drive shows a time for an item changed today
        raise UnexpectedPayloadError(f"Drive item {item.title!r} has an unknown last-modified text {item.modified!r}")

    def _signal(self, item: _Item) -> str:
        return f"{item.item_id}@{self._day(item)}"

    def _scan(self) -> tuple[list[_Match], dict[str, _Item]]:
        self.warnings = []
        matches: dict[str, _Match] = {}
        listings: dict[str, _Item] = {}
        for item in self._folder(self._folder_id):
            if item.folder and item.title in _FOLDERS:
                artifact = _FOLDERS[item.title]
                for child in self._folder(item.item_id):
                    found = _NAMES[artifact].match(child.title)
                    if found is not None and not child.folder:
                        match_id = canonical_native_match_id("gradientsports", "public", found.group(1))
                        matches.setdefault(match_id, _Match(match_id)).files[artifact] = child
                    elif not self._skipped(child, artifact):
                        self.warnings.append(
                            f"GS Drive {item.title}/{child.title} is not a known file; it is not listed"
                        )
            elif item.title in _LISTINGS and not item.folder:
                listings[item.title] = item
            elif not self._skipped(item, None):
                self.warnings.append(f"GS Drive {item.title} is not a known item; it is not listed")
        return [matches[key] for key in sorted(matches, key=lambda m: (len(m), m))], listings

    @staticmethod
    def _skipped(item: _Item, artifact: str | None) -> bool:
        """Items skipped by rule: specification PDFs, Google documents, and the dated drops of `Event Data`."""
        if item.google_doc or item.title.lower().endswith(".pdf"):
            return True
        return item.folder and artifact == "events"

    def _competitions(self, listings: Mapping[str, _Item]) -> dict[str, tuple[str, str, str]]:
        """match id -> (competition id, competition name, season), from `competitions.csv`."""
        item = listings.get("competitions.csv")
        if item is None:
            self.warnings.append("GS Drive has no competitions.csv: matches are listed without competition")
            return {}
        key = hashlib.sha256(self._signal(item).encode()).hexdigest()  # the cache takes hex keys
        data = self._cache.get(key)
        if data is None:
            data = self.download(item.item_id)
            self._cache.put(key, data)
        found: dict[str, tuple[str, str, str]] = {}
        for row in csv.DictReader(io.StringIO(data.decode("utf-8"), newline="")):
            games = ast.literal_eval(row["games"])  # a Python-literal list of dicts in the source CSV
            if not isinstance(games, list):
                raise UnexpectedPayloadError("competitions.csv: the games field is not a list")
            for game in cast("list[object]", games):
                if not isinstance(game, dict):
                    raise UnexpectedPayloadError("competitions.csv: a games entry is not a dict")
                entry = cast("dict[str, object]", game)
                found[str(entry["id"])] = (str(row["id"]), str(row["name"]), str(entry["season"]))
        return found

    def _listed(self) -> Iterator[tuple[_Match, tuple[str, str, str] | None]]:
        matches, listings = self._scan()
        competitions = self._competitions(listings)
        for match in matches:
            yield match, competitions.get(match.match_id)

    def list_entries(self) -> Iterator[SourceEntry]:
        """Every match of the tournaments that the module's clauses select."""
        wanted = selected_match_ids(self._module)
        listed_at = self._clock()
        for match, competition in self._listed():
            competition_id, name, season = competition or ("", "", "")
            if not tournament_selected(self._module, competition_id, season):
                continue
            if wanted is not None and match.match_id not in wanted:
                continue
            yield SourceEntry(
                module_id="gradientsports_public",
                native_match_id=match.match_id,
                competition_id=competition_id or None,
                competition_name=name or None,
                season_id=season or None,  # GS has no season id; the season name stands for it (spec §14.1)
                season_name=season or None,
                visibility="public",
                artifacts={
                    artifact: ArtifactRef(
                        key=item.item_id, change_signal=self._signal(item), expected_sha256=None, size=None
                    )
                    for artifact, item in sorted(match.files.items())
                },
                listed_at=listed_at,
            )

    def preview(self) -> PreviewResult:
        """One tournament per competition-season, with the exact match count (the folders list every match)."""
        groups: dict[tuple[str, str], tuple[str, int]] = {}
        for _, competition in self._listed():
            competition_id, name, season = competition or ("", "", "")
            first, count = groups.get((competition_id, season), (name, 0))
            groups[(competition_id, season)] = (first, count + 1)
        tournaments = [
            PreviewTournament(
                competition_id=competition_id,
                competition_name=name,
                season_id=season,
                season_name=season,
                match_count=count,
                count_note=None,
            )
            for (competition_id, season), (name, count) in sorted(groups.items())
        ]
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=list(self.warnings))

    def listing_kinds(self) -> tuple[str, ...]:
        """The source listings (§10.5): `competitions.csv` and `players.csv` of the root folder."""
        return tuple(sorted(_LISTING_FILES))

    def fetch_listing(self, kind: str) -> Delivery:
        if kind not in _LISTING_FILES:
            raise ValueError(f"unknown Gradient Sports listing {kind!r}")
        _, listings = self._scan()
        item = listings.get(_LISTING_FILES[kind])
        if item is None:
            raise FileNotFoundError(f"the GS Drive folder has no {_LISTING_FILES[kind]}")
        return Delivery.received(
            provider="gradientsports",
            channel="public",
            native_match_id="",
            artifact=kind,
            raw=self.download(item.item_id),
            visibility="public",
            change_signal=self._signal(item),
            fetched_at=self._clock(),
        )

    def download(self, file_id: str) -> bytes:
        """The bytes of one Drive file, through the virus-scan confirm form when Drive shows it."""
        return self._fetch(file_id, max_bytes=MAX_FILE_BYTES).body

    def content_length(self, file_id: str) -> int:
        """The size of one Drive file from a 1-byte range request (the listing gives no sizes, §26 item 8)."""
        result = self._fetch(file_id, max_bytes=MAX_LISTING_BYTES, headers={"Range": "bytes=0-0"})
        if result.status == 206:
            found = _TOTAL.match(result.headers.get("Content-Range", ""))
            if found is None:
                raise UnexpectedPayloadError(f"Drive answered the range request for {file_id} without a total size")
            return int(found.group(1))
        return len(result.body)

    def _fetch(self, file_id: str, *, max_bytes: int, headers: Mapping[str, str] | None = None) -> HttpResult:
        url = f"{DOWNLOAD_URL}?{urlencode({'id': file_id, 'export': 'download'})}"
        first = self._result(url, max_bytes=max_bytes, expect="text", headers=headers)
        body = first.body
        if not _is_html(body):
            return first
        form = _ConfirmForm()
        form.feed(body.decode("utf-8", errors="replace"))
        form.close()
        if form.action != DOWNLOAD_URL or "confirm" not in form.fields:
            raise UnexpectedPayloadError(f"Drive answered {file_id} with an HTML page that is not the confirm form")
        if form.fields.get("id") != file_id:
            raise UnexpectedPayloadError(f"Drive's confirm form names {form.fields.get('id')!r}, not {file_id!r}")
        params = {name: form.fields[name] for name in ("id", "export", "confirm", "uuid") if name in form.fields}
        return self._result(
            f"{DOWNLOAD_URL}?{urlencode(params)}", max_bytes=max_bytes, expect="binary", headers=headers
        )

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"GS match {entry.native_match_id} has no {artifact} artifact")
        return Delivery.received(
            provider="gradientsports",
            channel="public",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=self.download(ref.key),
            visibility="public",
            change_signal=ref.change_signal,
            fetched_at=self._clock(),
        )
