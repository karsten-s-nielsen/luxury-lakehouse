"""The pining channel adapter (spec §7.2, §9, §17.3, §29.1, §29.13; plan Task 20).

The public-tier listings are recorded (`tests/fixtures/pining/*_public.json`); owner-tier entries are synthetic
(invented ids and names on the observed field set), because private entries never enter the repository (§23.4).
"""

from __future__ import annotations

import ast
import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from piningkit import BASE, OWNER, FakePining, adapter, owner_entry, sha

from lakehouse.config.model import AllClause, CompetitionClause, MatchClause, ModuleConfig
from lakehouse.entries import ArtifactRef
from lakehouse.errors import IntegrityError, UnexpectedPayloadError
from lakehouse.sources import pining
from lakehouse.sources.base import ListingSource
from lakehouse.sources.pining import (
    PUBLIC_TOKEN,
    ArtifactUnavailableError,
    PiningAdapter,
    PiningToken,
    artifact_unchanged,
    token_from_env,
    updated_since_text,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "pining"


def _recorded(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_bytes())


def _adapter(
    http: FakePining, *clauses: Any, token: PiningToken = OWNER, sleeps: list[float] | None = None
) -> PiningAdapter:
    return adapter(http, *clauses, token=token, sleep=(sleeps if sleeps is not None else []).append)


def _idsse(**kwargs: Any) -> FakePining:
    return FakePining("idsse", _recorded("idsse_matches_public.json")["matches"], {}, **kwargs)


def test_token_from_env_tiers() -> None:
    assert token_from_env({}).tier == "public"
    assert token_from_env({}).value.get_secret_value() == PUBLIC_TOKEN
    assert token_from_env({"PINING_LIST_TOKEN": "list-token-value-x"}).tier == "list"  # pragma: allowlist secret
    both = {"PINING_LIST_TOKEN": "list-token-value-x", "PINING_FOR_THE_DATA_TOKEN": "owner-token-value-x"}
    assert token_from_env(both).tier == "owner"
    assert token_from_env({"PINING_FOR_THE_DATA_TOKEN": ""}).tier == "public"


def test_token_repr_masked() -> None:
    """Task 1 `test_secret_reprs_masked`, the PiningToken half."""
    token = PiningToken.of("owner-token-value-for-tests", "owner")  # pragma: allowlist secret - a test value
    for text in (repr(token), str(token)):
        assert "owner-token-value" not in text
        assert "owner" in text


def test_updated_since_format() -> None:
    """The API's regex: `^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$`; a local non-UTC clock still gives UTC."""
    local = datetime(2026, 5, 1, 2, 30, 15, 123456, tzinfo=timezone(timedelta(hours=2)))
    assert updated_since_text(local) == "2026-05-01T00:30:15Z"
    with pytest.raises(ValueError, match="time zone"):
        updated_since_text(datetime(2026, 5, 1))  # a naive time is the case under test
    http = _idsse()
    list(_adapter(http).list_entries(updated_since=local))
    assert http.calls[0][0] == f"{BASE}/idsse/matches?updatedSince=2026-05-01T00:30:15Z"


def test_listing_entries_public_idsse() -> None:
    http = _idsse()
    entries = list(_adapter(http, token=token_from_env({})).list_entries())
    assert [e.native_match_id for e in entries] == [
        "J03WMX",
        "J03WN1",
        "J03WOH",
        "J03WOY",
        "J03WPY",
        "J03WQQ",
        "J03WR9",
    ]
    entry = entries[0]
    assert entry.channel_match_id == "DFL-MAT-J03WMX"
    assert (entry.competition_id, entry.competition_name, entry.season_id, entry.season_name) == (
        "DFL-COM-000001",
        "Bundesliga",
        "DFL-SEA-0001K6",
        "2022/2023",
    )
    assert entry.module_id == "idsse_pining"
    assert set(entry.artifacts) == {"metadata", "events", "positions"}  # the IDSSE parser's names
    tracking = entry.artifacts["positions"]
    recorded = _recorded("idsse_matches_public.json")["matches"][0]["artifact_meta"]["tracking"]
    assert (tracking.key, tracking.expected_sha256, tracking.size) == ("tracking", recorded["sha256"], recorded["size"])
    assert tracking.change_signal == recorded["sha256"]
    assert all(call[1].get("Authorization") == f"Bearer {PUBLIC_TOKEN}" for call in http.calls)


def test_bare_artifact_key_used() -> None:
    data = b"<PutDataRequest/>"
    entry = owner_entry("DFL-MAT-J03WMX", {"metadata": data}, visibility="public")
    http = FakePining("idsse", [entry], {("DFL-MAT-J03WMX", "metadata"): data})
    adapter = _adapter(http)
    listed = next(iter(adapter.list_entries()))
    delivery = adapter.fetch(listed, "metadata")
    assert http.calls[-1][0] == f"{BASE}/idsse/matches/DFL-MAT-J03WMX/metadata"  # the key, not metadata.json
    assert delivery.decoded == data
    assert (delivery.channel, delivery.native_match_id, delivery.artifact) == ("pining", "J03WMX", "metadata")


def test_visibility_from_entry_flag() -> None:
    """§23.6: an owner token with a public-flagged entry gives public; the tier never sets visibility."""
    entries = [
        owner_entry("1001", {"metadata": b"{}"}, visibility="public"),
        owner_entry("1002", {"metadata": b"{}"}, visibility="private"),
        {k: v for k, v in owner_entry("1003", {"metadata": b"{}"}).items() if k != "visibility"},
    ]
    http = FakePining("gradientsports", entries, {})
    assert [e.visibility for e in _adapter(http).list_entries()] == ["public", "private", "public"]
    entries.append(owner_entry("1004", {"metadata": b"{}"}, visibility="secret"))
    with pytest.raises(UnexpectedPayloadError, match="visibility"):
        list(_adapter(http).list_entries())


def test_no_visibility_code_reads_the_token_tier() -> None:
    """An AST check: the functions that set visibility never reference `.tier`."""
    tree = ast.parse(inspect.getsource(pining))
    setters = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(isinstance(n, ast.keyword) and n.arg == "visibility" for n in ast.walk(node))
    ]
    assert setters, "no function sets visibility"
    for function in setters:
        assert not any(isinstance(n, ast.Attribute) and n.attr == "tier" for n in ast.walk(function)), function.name


def test_public_token_nothing_private() -> None:
    http = _idsse()
    adapter = _adapter(http, token=token_from_env({}))
    entries = list(adapter.list_entries())
    assert {e.visibility for e in entries} == {"public"}
    preview = adapter.preview()
    assert [t.match_count for t in preview.tournaments] == [2, 5]
    assert preview.notes == ["private pining tournaments are not listed here: needs the list-only token"]
    owner = _adapter(_idsse()).preview()
    assert owner.notes == []


def test_artifact_meta_skip() -> None:
    data = b'[{"id": "1001"}]'
    entry = owner_entry("1001", {"metadata": data})
    http = FakePining("gradientsports", [entry], {("1001", "metadata"): data})
    adapter = _adapter(http)
    listed = next(iter(adapter.list_entries()))
    ref = listed.artifacts["metadata"]
    assert artifact_unchanged(ref, sha(data))  # equal hash: no download
    assert not artifact_unchanged(ref, sha(b"older bytes"))  # changed hash: download
    assert not artifact_unchanged(ref, None)  # never downloaded
    delivery = adapter.fetch(listed, "metadata")
    assert delivery.delivery_sha256 == ref.expected_sha256
    http.files[("1001", "metadata")] = b'[{"id": "1002"}]'  # same size, other bytes
    with pytest.raises(IntegrityError, match="artifact_meta"):
        adapter.fetch(listed, "metadata")


def test_without_artifact_meta_falls_back_to_content_hash() -> None:
    data = b'[{"id": "1001"}]'
    entry = {k: v for k, v in owner_entry("1001", {"metadata": data}).items() if k != "artifact_meta"}
    http = FakePining("gradientsports", [entry], {("1001", "metadata"): data})
    adapter = _adapter(http)
    listed = next(iter(adapter.list_entries()))
    ref = listed.artifacts["metadata"]
    assert (ref.expected_sha256, ref.size, ref.change_signal) == (None, None, "2026-10-20T10:00:00Z")
    assert not artifact_unchanged(ref, sha(data))  # no artifact_meta: always download, then compare content
    assert not artifact_unchanged(ref, None)  # no artifact_meta and never downloaded
    assert adapter.fetch(listed, "metadata").content_sha256 == sha(data)


def test_noop_updated_at_no_reparse() -> None:
    """§23.6: a new `updated_at` with the same artifact bytes changes no change signal, so nothing is downloaded."""
    data = b'[{"id": "1001"}]'
    first = owner_entry("1001", {"metadata": data})
    later = dict(first, updated_at="2026-10-27T09:00:00Z")
    refs: list[ArtifactRef] = []
    for entry in (first, later):
        refs.append(
            next(iter(_adapter(FakePining("gradientsports", [entry], {})).list_entries())).artifacts["metadata"]
        )
    assert refs[0] == refs[1]
    assert artifact_unchanged(refs[1], sha(data))


def test_competition_season_fields_used_and_missing_group() -> None:
    entries = [
        owner_entry(
            "1001", {"metadata": b"{}"}, competition_id="7", competition_name="Cup", season_id="9", season_name="2025"
        ),
        owner_entry("1002", {"metadata": b"{}"}),
    ]
    adapter = _adapter(FakePining("skillcorner", entries, {}))
    listed = list(adapter.list_entries())
    assert (listed[0].competition_id, listed[0].season_id, listed[0].season_name) == ("7", "9", "2025")
    assert (listed[1].competition_id, listed[1].competition_name, listed[1].season_id) == (None, None, None)
    groups = {
        (t.competition_id, t.competition_name, t.season_name): t.match_count for t in adapter.preview().tournaments
    }
    assert groups == {("7", "Cup", "2025"): 1, ("", "competition not given by source", ""): 1}
    narrowed = _adapter(FakePining("skillcorner", entries, {}), CompetitionClause(competition_id="7", seasons="all"))
    assert [e.native_match_id for e in narrowed.list_entries()] == ["1001"]


def test_gs_season_name_only() -> None:
    """GS gives only `season_name` ("2022"); v2's GS season id is the season value (§14.1), as on the public channel."""
    entry = owner_entry("3812", {"metadata": b"{}"}, competition_id="38", competition_name="WC", season_name="2022")
    listed = next(iter(_adapter(FakePining("gradientsports", [entry], {})).list_entries()))
    assert (listed.competition_id, listed.season_id, listed.season_name) == ("38", "2022", "2022")
    other = owner_entry("3813", {"metadata": b"{}"}, competition_id="38", season_name="2022")
    sc = next(iter(_adapter(FakePining("skillcorner", [other], {})).list_entries()))
    assert (sc.season_id, sc.season_name) == (None, "2022")  # only GS maps a season value to its season id


def test_pining_statsbomb_has_360_from_artifacts() -> None:
    files = {"events": b"[]", "roster": b"[]", "metadata": b"{}"}
    entries = [
        owner_entry("3788741", {**files, "freeze_frames": b"[]"}, competition_id="55", season_id="43"),
        owner_entry("3788742", files, competition_id="55", season_id="43"),
    ]
    adapter = _adapter(FakePining("statsbomb", entries, {}))
    listed = list(adapter.list_entries())
    assert [e.has_360 for e in listed] == [True, False]
    assert set(listed[0].artifacts) == {"events", "lineups", "three_sixty", "metadata"}
    before = len(adapter._http.calls)  # type: ignore[attr-defined]
    tournament = adapter.preview().tournaments[0]
    assert (tournament.match_count, tournament.match_count_360) == (2, 1)
    assert len(adapter._http.calls) - before == 1  # type: ignore[attr-defined]  # one list call


def test_skillcorner_public_keys_map_to_parser_artifacts() -> None:
    http = FakePining("skillcorner", _recorded("skillcorner_matches_public.json")["matches"], {})
    entry = next(e for e in _adapter(http).list_entries() if e.native_match_id == "1899585")
    assert set(entry.artifacts) == {"match", "tracking", "dynamic_events", "phases_of_play"}
    assert entry.artifacts["tracking"].key == "1899585_tracking_extrapolated"


def test_skillcorner_format_2_keys() -> None:
    files = {"metadata": b"{}", "tracking": b"PAR1", "events": b"PAR1", "physical": b"PAR1"}
    entry = owner_entry("2000001", files, format_version=2)
    listed = next(iter(_adapter(FakePining("skillcorner", [entry], {})).list_entries()))
    assert set(listed.artifacts) == {"match", "tracking_parquet", "dynamic_events_parquet", "physical"}


def test_unknown_artifact_key_warns() -> None:
    entry = owner_entry("1001", {"metadata": b"{}", "heatmaps": b"{}"})
    adapter = _adapter(FakePining("gradientsports", [entry], {}))
    listed = next(iter(adapter.list_entries()))
    assert set(listed.artifacts) == {"metadata"}
    assert adapter.warnings == [
        "gradientsports_pining: 1001 has artifact 'heatmaps', which v2 does not know; not fetched"
    ]


def test_401_retry_during_rotation() -> None:
    http = _idsse()
    http.unauthorised = 2
    sleeps: list[float] = []
    assert len(list(_adapter(http, sleeps=sleeps).list_entries())) == 7
    assert sleeps == [2.0, 4.0]
    http.unauthorised = 99
    calls = len(http.calls)
    with pytest.raises(IntegrityError, match="401"):
        list(_adapter(http, sleeps=[]).list_entries())
    assert len(http.calls) - calls == 6  # the first call and 5 retries


def test_list_token_cannot_fetch_artifact() -> None:
    """The list tier sees private entries but gets the uniform 404 on artifacts (§29.6)."""
    data = b"{}"
    entry = owner_entry("1001", {"metadata": data})
    http = FakePining("gradientsports", [entry], {("1001", "metadata"): data})
    http.not_found.add(("1001", "metadata"))
    adapter = _adapter(http, token=PiningToken.of("list-token-value-x", "list"))  # pragma: allowlist secret
    listed = next(iter(adapter.list_entries()))
    assert listed.visibility == "private"
    with pytest.raises(ArtifactUnavailableError, match="not available with this token"):
        adapter.fetch(listed, "metadata")


def test_provider_and_ids_checked() -> None:
    http = FakePining("idsse", [], {})
    http.provider = "idsse"
    bad = FakePining("gradientsports", [owner_entry("../x", {"metadata": b"{}"})], {})
    with pytest.raises(UnexpectedPayloadError, match="id"):
        list(_adapter(bad).list_entries())
    with pytest.raises(ValueError, match="https"):
        PiningAdapter(
            "idsse",
            http=http,
            base_url="http://x.test/v1",
            token=OWNER,
            module=ModuleConfig(module_id="idsse_pining", active=True, filter=[AllClause()]),
        )


def test_pining_registries_agree_on_providers() -> None:
    """The adapter registry (`sources.pining.PINING_PROVIDERS`) and the listing-parser registry
    (`parsers.pining.PROVIDERS`) are separate lists by the adapter/parser independence contract (import-linter forbids
    either package importing the other), so a new pining provider must be added to BOTH. They must name the same
    provider set, and every adapter entry's module id must be `<provider>_pining` (review V2UI-15)."""
    from lakehouse.parsers import pining as parsers_pining

    adapter_providers = {entry.provider for entry in pining.PINING_PROVIDERS}
    assert adapter_providers == set(parsers_pining.PROVIDERS)
    assert all(entry.module_id == f"{entry.provider}_pining" for entry in pining.PINING_PROVIDERS)


def test_resolve_provider_maps_cover_every_provider() -> None:
    """Every per-provider resolve map is keyed by the full `Provider` literal, so a new provider that forgets one
    fails here rather than at runtime (review B-15). These maps are edit sites in docs/adding-a-provider.md."""
    from typing import get_args

    from lakehouse.ids.native import Provider
    from lakehouse.resolve import model as resolve_model
    from lakehouse.resolve import records as resolve_records

    providers = set(get_args(Provider))
    assert set(resolve_model.PROVIDER_RANK) == providers
    for name in ("_PLAYER_IDS", "_TEAM_IDS", "_COMPETITION_IDS", "_MATCH_IDS"):
        assert set(getattr(resolve_records, name)) == providers, f"resolve.records.{name} misses a provider"


def test_match_clause_uses_canonical_ids() -> None:
    adapter = _adapter(_idsse(), MatchClause(match_ids=["J03WPY"]))
    assert [e.native_match_id for e in adapter.list_entries()] == ["J03WPY"]


def test_fetch_listing_players() -> None:
    http = _idsse()
    delivery = _adapter(http).fetch_listing("players")
    assert json.loads(delivery.decoded) == {"provider": "idsse", "players": []}
    assert (delivery.artifact, delivery.native_match_id, delivery.channel) == ("players", "", "pining")


def test_listing_that_names_another_provider_fails() -> None:
    http = _idsse()
    http.listed_provider = "skillcorner"
    with pytest.raises(UnexpectedPayloadError, match="another provider"):
        list(_adapter(http).list_entries())


def test_listing_raw_file_private_when_a_record_is_private() -> None:
    """The listing's raw file takes the visibility of its content: private when any record is private."""
    entries = [owner_entry("1001", {"metadata": b"{}"}, visibility="public")]
    assert _adapter(FakePining("gradientsports", entries, {})).fetch_listing("matches").visibility == "public"
    entries.append(owner_entry("1002", {"metadata": b"{}"}))
    assert _adapter(FakePining("gradientsports", entries, {})).fetch_listing("matches").visibility == "private"


def test_list_all_entries_ignores_the_clauses() -> None:
    """The visibility listing (§29.14): every entry, whatever the module's clauses, and never `updatedSince`."""
    http = _idsse()
    entries = _adapter(http, MatchClause(match_ids=["J03WPY"])).list_all_entries()
    assert len(entries) == 7
    assert http.calls[-1][0] == f"{BASE}/idsse/matches"


def test_listing_protocol() -> None:
    adapter = _adapter(_idsse())
    assert isinstance(adapter, ListingSource)
    assert adapter.listing_kinds() == ("matches", "players")
