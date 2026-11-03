"""The pining channel: one adapter for the four pining modules (spec §7.2, §9, §17.3, §29.1, §29.6, §29.13; plan
Task 20).

`GET {base}/{provider}/matches` lists a provider's entries for the token's tier; each artifact is fetched by its bare
key at `{base}/{provider}/matches/{id}/{key}`, which redirects to a presigned S3 URL (the HTTP client drops the bearer
header on that cross-host hop). Visibility comes only from each entry's own flag, never from the token. The change
signal of an artifact is its `artifact_meta.sha256` (the SHA-256 of the bytes as stored and served), so an
unchanged artifact is never downloaded (`artifact_unchanged`), and a download whose bytes differ from it fails
loudly; an entry without `artifact_meta` falls back to `updated_at` and the content hash. A 401 is retried with
backoff (the owner token rotates with no overlap); a 404 on an artifact is `ArtifactUnavailableError`.

The pining artifact keys are role keys (pining ADR 0008); the adapter names each artifact as the provider's parser
does (IDSSE `tracking` is `positions`, StatsBomb `freeze_frames` and `roster` are `three_sixty` and `lineups`,
SkillCorner's legacy public keys carry the match id, and its format 2 Parquet set gets its own names). An unknown key
becomes a warning and is not fetched.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Literal, Protocol, cast
from urllib.parse import quote

from pydantic import SecretStr

from lakehouse.config.model import ModuleConfig, ModuleId
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import ArtifactUnavailableError, HttpStatusError, IntegrityError, UnexpectedPayloadError
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.ids.native import Provider
from lakehouse.net.http import Expect, HttpResult, safe_url
from lakehouse.raw.archive import Delivery
from lakehouse.sources.base import PreviewResult, PreviewTournament, selected_match_ids, tournament_selected

__all__ = [
    "PINING_PROVIDERS",
    "PUBLIC_TOKEN",
    "ArtifactUnavailableError",
    "PiningAdapter",
    "PiningProvider",
    "PiningToken",
    "artifact_unchanged",
    "token_from_env",
    "updated_since_text",
]

# The documented public token of the pining API (README), not a secret.
PUBLIC_TOKEN: Final = "test-token-pining-for-the-data"  # noqa: S105  # pragma: allowlist secret
TokenTier = Literal["public", "list", "owner"]
ListingKind = Literal["matches", "players"]
MAX_LISTING_BYTES: Final = 64 * 1024 * 1024
MAX_ARTIFACT_BYTES: Final = 2 * 1024 * 1024 * 1024  # used only when an entry has no artifact_meta size
RETRY_401_S: Final = (2.0, 4.0, 8.0, 16.0, 32.0)
NO_COMPETITION: Final = "competition not given by source"
PUBLIC_MARKER: Final = "private pining tournaments are not listed here: needs the list-only token"
_ID: Final = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")
_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class PiningProvider:
    """One provider served through the pining API. `artifacts` maps each pining role key to the name the provider's
    parser gives that artifact (`<pining role> -> <parser artifact>`).

    This registry is the ADAPTER side only: one entry here adds the provider to the shared `PiningAdapter`. It is not
    the single edit site for a new pining provider. The parser side keeps its own list (`parsers.pining.PROVIDERS`),
    because the adapter and parser packages are independent (import-linter); a parity test asserts the two sets agree.
    The provider also needs its `Provider` literal and native-id formats (ids/native.py), its canonical match-id case
    (ids/channels.py), its `<provider>_pining` ModuleId (config/model.py), its artifact parser
    (`wiring.default_parsers`), its config default, and its `DISPLAY_NAMES` label (services/modules.py). See
    `docs/adding-a-provider.md` for the full edit-site list. SkillCorner is the exception: its keys differ by
    `format_version`, so it declares no `artifacts` here and `PiningAdapter._artifact_name` resolves them.
    """

    provider: Provider
    module_id: ModuleId
    artifacts: Mapping[str, str] = field(default_factory=dict[str, str])


# The pining ADAPTER registry: the providers the shared PiningAdapter serves (plan Task 20). The parser side keeps
# its own `parsers.pining.PROVIDERS`; `test_pining_registries_agree_on_providers` asserts the two sets match.
PINING_PROVIDERS: Final[tuple[PiningProvider, ...]] = (
    PiningProvider("idsse", "idsse_pining", {"metadata": "metadata", "events": "events", "tracking": "positions"}),
    PiningProvider(
        "statsbomb",
        "statsbomb_pining",
        {"events": "events", "freeze_frames": "three_sixty", "roster": "lineups", "metadata": "metadata"},
    ),
    PiningProvider(
        "gradientsports",
        "gradientsports_pining",
        {"metadata": "metadata", "roster": "roster", "events": "events", "tracking": "tracking"},
    ),
    PiningProvider("skillcorner", "skillcorner_pining"),  # keys resolved by _artifact_name (format_version)
)
_MODULES: Final[dict[str, ModuleId]] = {p.provider: p.module_id for p in PINING_PROVIDERS}
# pining role key -> the provider parser's artifact name (SkillCorner handled by _artifact_name, so it has none here)
_ARTIFACTS: Final[dict[str, dict[str, str]]] = {p.provider: dict(p.artifacts) for p in PINING_PROVIDERS if p.artifacts}
_SKILLCORNER_LEGACY: Final = {  # the public tier's id-prefixed keys `<id>_<role>`
    "match": "match",
    "tracking_extrapolated": "tracking",
    "dynamic_events": "dynamic_events",
    "phases_of_play": "phases_of_play",
}
_SKILLCORNER_V2: Final = {  # `format_version` 2, pining ADR 0011
    "metadata": "match",
    "tracking": "tracking_parquet",
    "events": "dynamic_events_parquet",
    "physical": "physical",
}


class HttpGetter(Protocol):
    def get(
        self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect
    ) -> HttpResult: ...


@dataclass(frozen=True)
class PiningToken:
    """A pining bearer token and its tier. The tier names what the token may see; it never decides visibility."""

    value: SecretStr
    tier: TokenTier

    @classmethod
    def of(cls, value: str, tier: TokenTier) -> PiningToken:
        return cls(SecretStr(value), tier)

    def __repr__(self) -> str:
        return f"PiningToken(tier={self.tier!r}, value='**********')"

    __str__ = __repr__


def token_from_env(env: Mapping[str, str]) -> PiningToken:
    """The owner token (`PINING_FOR_THE_DATA_TOKEN`), else the list-only token (`PINING_LIST_TOKEN`), else the
    documented public token."""
    if env.get("PINING_FOR_THE_DATA_TOKEN"):
        return PiningToken.of(env["PINING_FOR_THE_DATA_TOKEN"], "owner")
    if env.get("PINING_LIST_TOKEN"):
        return PiningToken.of(env["PINING_LIST_TOKEN"], "list")
    return PiningToken.of(PUBLIC_TOKEN, "public")


def updated_since_text(when: datetime) -> str:
    """`updatedSince` as the API requires it (`^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$`), always in UTC."""
    if when.tzinfo is None:
        raise ValueError("updatedSince needs a time with a time zone")
    return when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def artifact_unchanged(ref: ArtifactRef, known_delivery_sha256: str | None) -> bool:
    """Whether the stored copy already has the artifact's bytes: its `artifact_meta.sha256` equals the
    `delivery_sha256` in lineage (§17.3). Without `artifact_meta` the artifact is always downloaded."""
    return ref.expected_sha256 is not None and ref.expected_sha256 == known_delivery_sha256


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _text(entry: Mapping[str, Any], name: str) -> str | None:
    value = entry.get(name)
    if value is not None and not isinstance(value, str):
        raise UnexpectedPayloadError(f"pining entry field {name} is not a string")
    return value


class PiningAdapter:
    """The `ChannelAdapter` of one pining module. `warnings` collects artifact keys it does not know."""

    def __init__(
        self,
        provider: Provider,
        *,
        http: HttpGetter,
        base_url: str,
        token: PiningToken,
        module: ModuleConfig,
        clock: Callable[[], datetime] = _utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if provider not in _MODULES:
            raise ValueError(f"pining serves no provider {provider!r}")
        if not base_url.startswith("https://"):
            raise ValueError("the pining base URL must use https")
        if module.module_id != _MODULES[provider]:
            raise ValueError(f"the {provider} pining adapter serves the {_MODULES[provider]} module only")
        self._provider: Provider = provider
        self._http = http
        self._base = base_url.rstrip("/")
        self._token = token
        self._module = module
        self._clock = clock
        self._sleep = sleep
        self.warnings: list[str] = []

    def __repr__(self) -> str:
        return f"PiningAdapter(provider={self._provider!r}, base_url={self._base!r}, token={self._token!r})"

    @property
    def module_id(self) -> ModuleId:
        return _MODULES[self._provider]

    @property
    def provider(self) -> Provider:
        return self._provider

    @property
    def channel(self) -> Literal["pining"]:
        return "pining"

    def _get(self, url: str, *, max_bytes: int, expect: Expect) -> HttpResult:
        """One API call; a 401 is retried with backoff while the owner token rotates."""
        headers = {"Authorization": f"Bearer {self._token.value.get_secret_value()}"}
        result = self._http.get(url, headers=headers, max_bytes=max_bytes, expect=expect)
        for wait in RETRY_401_S:
            if result.status != 401:
                return result
            self._sleep(wait)
            result = self._http.get(url, headers=headers, max_bytes=max_bytes, expect=expect)
        if result.status != 401:
            return result
        raise IntegrityError(f"HTTP 401 from {safe_url(url)} after {len(RETRY_401_S)} retries: the token was refused")

    def _listing(self, kind: ListingKind, query: str = "") -> HttpResult:
        url = f"{self._base}/{self._provider}/{kind}" + query
        result = self._get(url, max_bytes=MAX_LISTING_BYTES, expect="json")
        if result.status != 200:
            raise HttpStatusError(result.status, safe_url(url))
        return result

    def _entries(self, updated_since: datetime | None) -> list[dict[str, Any]]:
        query = f"?updatedSince={updated_since_text(updated_since)}" if updated_since is not None else ""
        payload: object = json.loads(self._listing("matches", query).body)
        listing = cast("dict[str, Any]", payload) if isinstance(payload, dict) else {}
        if listing.get("provider") != self._provider:
            raise UnexpectedPayloadError(f"the pining {self._provider} match list names another provider")
        entries = listing.get("matches")
        if not isinstance(entries, list):
            raise UnexpectedPayloadError(f"the pining {self._provider} match list has no matches list")
        return cast("list[dict[str, Any]]", entries)

    def _artifact_name(self, entry: Mapping[str, Any], key: str) -> str | None:
        if self._provider != "skillcorner":
            return _ARTIFACTS[self._provider].get(key)
        if entry.get("format_version") == 2:
            return _SKILLCORNER_V2.get(key)
        prefix = f"{entry['id']}_"
        return _SKILLCORNER_LEGACY.get(key.removeprefix(prefix)) if key.startswith(prefix) else None

    def _artifacts(self, entry: Mapping[str, Any]) -> dict[str, ArtifactRef]:
        meta = cast("Mapping[str, Mapping[str, Any]]", entry.get("artifact_meta") or {})
        refs: dict[str, ArtifactRef] = {}
        for key in sorted(cast("Mapping[str, str]", entry.get("artifacts") or {})):
            name = self._artifact_name(entry, key)
            if name is None:
                self.warnings.append(
                    f"{self.module_id}: {entry['id']} has artifact {key!r}, which v2 does not know; not fetched"
                )
                continue
            stored = meta.get(key)
            if stored is not None and (
                not _SHA256.match(str(stored.get("sha256"))) or not isinstance(stored.get("size"), int)
            ):
                raise UnexpectedPayloadError(f"{self.module_id}: {entry['id']} {key}: artifact_meta is malformed")
            refs[name] = ArtifactRef(
                key=key,
                change_signal=str(stored["sha256"]) if stored is not None else _text(entry, "updated_at"),
                expected_sha256=str(stored["sha256"]) if stored is not None else None,
                size=int(stored["size"]) if stored is not None else None,
            )
        return refs

    def _entry(self, entry: Mapping[str, Any], listed_at: datetime) -> SourceEntry:
        raw_id = entry.get("id")
        if not isinstance(raw_id, str) or not _ID.match(raw_id):
            raise UnexpectedPayloadError(f"{self.module_id}: an entry has an invalid id")
        flag = entry.get("visibility", "public")
        if flag not in ("public", "private"):
            raise UnexpectedPayloadError(f"{self.module_id}: {raw_id} has visibility {flag!r}")
        season_id = _text(entry, "season_id")
        if season_id is None and self._provider == "gradientsports":
            season_id = _text(entry, "season_name")  # GS has no season id; its season value is v2's id (§14.1)
        artifacts = self._artifacts(entry)
        return SourceEntry(
            module_id=self.module_id,
            native_match_id=canonical_native_match_id(self._provider, "pining", raw_id),
            competition_id=_text(entry, "competition_id"),
            competition_name=_text(entry, "competition_name"),
            season_id=season_id,
            season_name=_text(entry, "season_name"),
            visibility=flag,
            artifacts=artifacts,
            listed_at=listed_at,
            has_360=("three_sixty" in artifacts) if self._provider == "statsbomb" else None,
            channel_match_id=raw_id,
        )

    def _listed(self, updated_since: datetime | None = None) -> list[SourceEntry]:
        self.warnings = []
        listed_at = self._clock()
        return [self._entry(entry, listed_at) for entry in self._entries(updated_since)]

    def list_all_entries(self) -> list[SourceEntry]:
        """Every entry, whatever the module's clauses, from one full listing without `updatedSince`: the visibility
        listing of §29.14 (a flip is never missed, whatever pining does with `updated_at`)."""
        return self._listed()

    def list_entries(self, *, updated_since: datetime | None = None) -> Iterator[SourceEntry]:
        """The entries of the tournaments that the module's clauses select. `updated_since` is a polling hint only
        (§17.3); a visibility listing is always full (Task 21)."""
        wanted = selected_match_ids(self._module)
        for entry in self._listed(updated_since):
            if not tournament_selected(self._module, entry.competition_id or "", entry.season_id or ""):
                continue
            if wanted is not None and entry.native_match_id not in wanted:
                continue
            yield entry

    def preview(self) -> PreviewResult:
        """One tournament per competition-season from the one list call; entries without competition fields form
        the "competition not given by source" group (§29.1). StatsBomb counts the entries with freeze frames."""
        groups: dict[tuple[str, str], list[SourceEntry]] = {}
        for entry in self._listed():
            groups.setdefault((entry.competition_id or "", entry.season_id or ""), []).append(entry)
        tournaments = [
            PreviewTournament(
                competition_id=competition,
                competition_name=members[0].competition_name or (competition if competition else NO_COMPETITION),
                season_id=season,
                season_name=members[0].season_name or season,
                match_count=len(members),
                count_note=None,
                match_count_360=sum(1 for e in members if e.has_360) if self._provider == "statsbomb" else None,
            )
            for (competition, season), members in sorted(groups.items())
        ]
        notes = list(self.warnings)
        if self._token.tier == "public":
            notes.append(PUBLIC_MARKER)
        return PreviewResult(tournaments=tournaments, fetched_at=self._clock(), notes=notes)

    def fetch(self, entry: SourceEntry, artifact: str) -> Delivery:
        """One artifact by its bare key; the received bytes must match `artifact_meta` where the entry has it."""
        ref = entry.artifacts.get(artifact)
        if ref is None:
            raise ValueError(f"{self.module_id} match {entry.native_match_id} has no {artifact} artifact")
        channel_id = entry.channel_match_id or entry.native_match_id
        url = f"{self._base}/{self._provider}/matches/{quote(channel_id)}/{quote(ref.key)}"
        result = self._get(url, max_bytes=ref.size if ref.size is not None else MAX_ARTIFACT_BYTES, expect="binary")
        if result.status == 404:
            raise ArtifactUnavailableError(404, safe_url(url), "artifact not available with this token")
        if result.status != 200:
            raise HttpStatusError(result.status, safe_url(url))
        data = result.body
        if ref.expected_sha256 is not None and (
            len(data) != ref.size or hashlib.sha256(data).hexdigest() != ref.expected_sha256
        ):
            raise IntegrityError(
                f"{safe_url(url)}: the received bytes do not match artifact_meta {ref.expected_sha256}"
            )
        return Delivery.received(
            provider=self._provider,
            channel="pining",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=data,
            visibility=entry.visibility,
            change_signal=ref.change_signal,
            fetched_at=self._clock(),
        )

    def listing_kinds(self) -> tuple[str, ...]:
        """The source listings (§10.5): the match list and the player list."""
        return ("matches", "players")

    def fetch_listing(self, kind: str) -> Delivery:
        """The match or player list as a source-level listing (§10.5). Its raw file is private when any record is
        flagged private (the content decides, not the token)."""
        if kind not in ("matches", "players"):
            raise ValueError(f"unknown pining listing {kind!r}")
        data = self._listing(kind).body
        payload: object = json.loads(data)
        listing = cast("dict[str, Any]", payload) if isinstance(payload, dict) else {}
        records = cast("list[dict[str, Any]]", listing.get(kind) or [])
        private = any(record.get("visibility") == "private" for record in records)
        return Delivery.received(
            provider=self._provider,
            channel="pining",
            native_match_id="",
            artifact=kind,
            raw=data,
            visibility="private" if private else "public",
            change_signal=None,
            fetched_at=self._clock(),
        )
