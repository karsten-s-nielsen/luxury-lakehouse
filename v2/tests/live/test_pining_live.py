"""Pining live tier (marker `live`; spec §7.2, §12.2, §23.6; plan Task 20).

The public-tier tests use the documented public token. The owner-tier tests need `PINING_FOR_THE_DATA_TOKEN` and the
list-tier test needs `PINING_LIST_TOKEN`; without them they skip with a visible reason. No test writes an id or a
value of a private entry anywhere: a private id is read at test time and used only in memory. Tests that download a
full tracking or event file also carry the marker `heavy` and run on the Mac Mini only.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.entries import SourceEntry
from lakehouse.ids.native import Provider
from lakehouse.net.http import HttpClient
from lakehouse.parsers.base import NamesBundle
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.inventory import unmapped_fields
from lakehouse.parsers.pining import PiningListingParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.runs.list_fields import list_fields_mismatch
from lakehouse.sources.pining import ArtifactUnavailableError, PiningAdapter, PiningToken, token_from_env

pytestmark = pytest.mark.live

BASE = "https://ozqgk9a3ji.execute-api.us-east-1.amazonaws.com/v1"  # the config default (spec §7.2)
PROVIDERS: tuple[Provider, ...] = ("gradientsports", "idsse", "skillcorner", "statsbomb")
PUBLIC_MINIMUM = {"gradientsports": 0, "idsse": 7, "skillcorner": 20, "statsbomb": 0}  # §7.2, 2026-10-26
PARSERS: dict[str, Any] = {
    "gradientsports": GradientSportsParser(),
    "idsse": IdsseParser(),
    "skillcorner": SkillCornerParser(),
    "statsbomb": StatsBombParser(),
}
METADATA = {"gradientsports": "metadata", "idsse": "metadata", "skillcorner": "match", "statsbomb": "metadata"}
_HTTP = HttpClient()


def _adapter(provider: Provider, token: PiningToken) -> PiningAdapter:
    module = ModuleConfig(module_id=f"{provider}_pining", active=True, filter=[AllClause()])  # type: ignore[arg-type]
    return PiningAdapter(provider, http=_HTTP, base_url=BASE, token=token, module=module)


def _owner() -> PiningToken:
    if not os.environ.get("PINING_FOR_THE_DATA_TOKEN"):
        pytest.skip("PINING_FOR_THE_DATA_TOKEN is not set: the owner-tier checks need the owner token")
    return token_from_env({"PINING_FOR_THE_DATA_TOKEN": os.environ["PINING_FOR_THE_DATA_TOKEN"]})


PUBLIC = token_from_env({})


@pytest.mark.parametrize("provider", PROVIDERS)
def test_public_tier_counts_and_flags(provider: Provider) -> None:
    adapter = _adapter(provider, PUBLIC)
    entries = list(adapter.list_entries())
    assert len(entries) >= PUBLIC_MINIMUM[provider]
    assert {e.visibility for e in entries} <= {"public"}
    assert adapter.warnings == []
    assert adapter.preview().notes[-1].startswith("private pining tournaments are not listed here")


@pytest.mark.parametrize("provider", PROVIDERS)
def test_public_entries_carry_the_new_fields(provider: Provider) -> None:
    """§7.2: `artifact_meta` and the competition and season fields (pining part one, landed 2026-10-26)."""
    raw = json.loads(_adapter(provider, PUBLIC).fetch_listing("matches").decoded)["matches"]
    if not raw:
        pytest.skip(f"the public tier lists no {provider} entry")
    assert all(set(e["artifact_meta"]) == set(e["artifacts"]) for e in raw)
    assert all("competition_id" in e and "season_name" in e for e in raw)


@pytest.mark.parametrize("provider", PROVIDERS)
@pytest.mark.parametrize("kind", ["matches", "players"])
def test_public_listings_mapped(provider: Provider, kind: str) -> None:
    delivery = _adapter(provider, PUBLIC).fetch_listing(kind)  # type: ignore[arg-type]
    parser = PiningListingParser(provider)
    assert unmapped_fields(parser.field_inventory(kind, delivery.decoded), parser.bronze_columns(kind)) == []  # type: ignore[arg-type]
    parser.listing(kind, delivery.decoded, run_id="live", source_hash=delivery.content_sha256)  # type: ignore[arg-type]


def _metadata_checks(provider: Provider, token: PiningToken) -> Iterator[tuple[SourceEntry, list[str]]]:
    adapter = _adapter(provider, token)
    for entry in adapter.list_entries():
        artifact = METADATA[provider]
        if artifact not in entry.artifacts:
            continue
        deliveries = {artifact: adapter.fetch(entry, artifact)}
        if provider == "statsbomb":  # the parser needs events for a match; the names need only the metadata
            row = StatsBombParser().table("metadata", deliveries[artifact].decoded).to_pylist()[0]
            fields = ("competition_id", "competition_name", "season_id", "season_name")
            names = NamesBundle(
                players=pa.table({}),
                teams=pa.table({}),
                competitions=pa.table(
                    {f: pa.array([None if row[f] is None else str(row[f])], pa.string()) for f in fields}
                ),
            )
            yield entry, list_fields_mismatch(entry, names)
            continue
        if provider == "gradientsports":
            deliveries["roster"] = adapter.fetch(entry, "roster")
        parsed = PARSERS[provider].parse(entry, deliveries, run_id="live")
        yield entry, list_fields_mismatch(entry, parsed.names)


@pytest.mark.parametrize("provider", ["idsse", "skillcorner"])
def test_list_fields_equal_metadata_live(provider: Provider) -> None:
    """§23.6 "Pining list fields" on every public entry: the list's competition and season equal the metadata."""
    results = list(_metadata_checks(provider, PUBLIC))
    assert results
    assert [(e.native_match_id, m) for e, m in results if m] == []


def test_idsse_pining_files_equal_hf_live() -> None:
    """The 7 public IDSSE entries are byte-identical to HF `pysport/idsse-data` (equal hashes, §7.3)."""
    tree = json.loads(
        _HTTP.get(
            "https://huggingface.co/api/datasets/pysport/idsse-data/tree/main?recursive=true",
            max_bytes=16 * 1024 * 1024,
            expect="json",
        ).body
    )
    files = {item["path"].rsplit("/", 1)[-1]: item for item in tree if item.get("type") == "file"}
    adapter = _adapter("idsse", PUBLIC)
    raw = {e["id"]: e for e in json.loads(adapter.fetch_listing("matches").decoded)["matches"]}
    for entry in adapter.list_entries():
        meta = raw[entry.channel_match_id or ""]["artifact_meta"]
        name = next(n for n in files if n.startswith("DFL_04_03") and n.endswith(f"{entry.native_match_id}.xml"))
        assert meta["tracking"]["sha256"] == files[name]["lfs"]["oid"]
        events = adapter.fetch(entry, "events").raw
        name = next(n for n in files if n.startswith("DFL_03_02") and n.endswith(f"{entry.native_match_id}.xml"))
        blob = hashlib.sha1(b"blob %d\x00" % len(events) + events).hexdigest()  # noqa: S324 - git's own blob hash
        assert blob == files[name]["oid"]


def test_private_entry_is_404_for_the_public_token() -> None:
    """§7.2 tiers: a private entry, read with the owner token, is a 404 for the public token (the uniform 404)."""
    owner = _owner()
    for provider in PROVIDERS:
        private = next((e for e in _adapter(provider, owner).list_entries() if e.visibility == "private"), None)
        if private is None:
            continue
        artifact = next(iter(private.artifacts))
        with pytest.raises(ArtifactUnavailableError):
            _adapter(provider, PUBLIC).fetch(private, artifact)
        return
    pytest.fail("the owner tier lists no private entry")


@pytest.mark.parametrize("provider", ["gradientsports", "skillcorner", "statsbomb"])
def test_owner_list_fields_equal_metadata_live(provider: Provider) -> None:
    """§23.6 "Pining list fields" over every owner-tier entry (metadata artifacts only, a few MB in all)."""
    results = list(_metadata_checks(provider, _owner()))
    assert results
    mismatched = [m for _, m in results if m]
    assert mismatched == [], f"{len(mismatched)} of {len(results)} entries differ"  # ids are not printed


def test_list_tier_sees_private_entries_but_no_artifact() -> None:
    """§29.6: the list-only tier lists private entries and gets the uniform 404 on every artifact."""
    if not os.environ.get("PINING_LIST_TOKEN"):
        pytest.skip("PINING_LIST_TOKEN is not set: the list-only tier is not available yet (pining work in progress)")
    token = token_from_env({"PINING_LIST_TOKEN": os.environ["PINING_LIST_TOKEN"]})
    for provider in PROVIDERS:
        private = next((e for e in _adapter(provider, token).list_entries() if e.visibility == "private"), None)
        if private is not None:
            with pytest.raises(ArtifactUnavailableError, match="not available with this token"):
                _adapter(provider, token).fetch(private, next(iter(private.artifacts)))
            return
    pytest.fail("the list tier lists no private entry")


@pytest.mark.heavy
@pytest.mark.parametrize("provider", PROVIDERS)
def test_owner_tier_parses(provider: Provider) -> None:
    """Every artifact of every owner-tier entry maps and parses (several GB; Mac Mini only)."""
    owner = _owner()
    adapter = _adapter(provider, owner)
    parser = PARSERS[provider]
    for entry in adapter.list_entries():
        deliveries = {artifact: adapter.fetch(entry, artifact) for artifact in entry.artifacts}
        for artifact, delivery in deliveries.items():
            inventory = parser.field_inventory(artifact, delivery.decoded)
            assert unmapped_fields(inventory, parser.bronze_columns(artifact)) == [], artifact
        parser.parse(entry, deliveries, run_id="live")
