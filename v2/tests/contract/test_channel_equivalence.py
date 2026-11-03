"""Channel equivalence (spec §7.3, §12.5, §23.6 "Channel equivalence"; plan Task 20).

Every pair of the Task 2 inventory overlaps (`tests/fixtures/channels/inventory.json`), so each pair has the same
test: a public delivery and a pining delivery of the same match give the same native id, the same `content_sha256`
where the files are equal, and identical canonical bronze (every column but `_ingested_at` and `_run_id`). Pining
deliveries go through `PiningAdapter`, so the artifact-name map and the wrapper removal are part of the proof.

Recorded evidence of equal and unequal files (`tests/fixtures/pining/*_public.json` against the public channels'
recorded listings, and the measurements of 2026-10-27 in the ledger):
- IDSSE: all 21 pining files of the 7 public entries are byte-identical to HF `pysport/idsse-data`.
- SkillCorner public tier: `match.json` and the tracking JSONL are byte-identical to GitHub opendata; the pining
  dynamic events and phases of play are an older version (294 instead of 322 event columns), which v2 also parses.
- StatsBomb `redistributed`: the decoded pining files are open-data values, not open-data bytes (they differ by git
  blob SHA), so the equal-values proof runs on the owner tier (`tests/live/test_pining_live.py`).
"""

from __future__ import annotations

import bz2
import gzip
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from piningkit import FakePining, adapter, owner_entry, sha

from lakehouse.entries import SourceEntry
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import MATCH_METADATA, StatsBombParser
from lakehouse.raw.archive import Delivery

ROOT = Path(__file__).resolve().parents[1] / "fixtures"
AUDIT = ("_ingested_at", "_run_id")
SKILLCORNER_TRACKING_OID = "90b6289ec2181192d1793ec6240fbbe8225ff0219c7956710c6278c31a661100"  # recorded LFS pointer


def _bytes(*parts: str) -> bytes:
    return ROOT.joinpath(*parts).read_bytes()


def _public(provider: str, entry: SourceEntry, files: Mapping[str, bytes]) -> dict[str, Delivery]:
    return {
        artifact: Delivery.received(
            provider=provider,  # type: ignore[arg-type]
            channel="public",
            native_match_id=entry.native_match_id,
            artifact=artifact,
            raw=raw,
            visibility="public",
            change_signal=None,
            fetched_at=entry.listed_at,
        )
        for artifact, raw in files.items()
    }


def _pining(
    provider: str, entry: dict[str, Any], files: Mapping[str, bytes]
) -> tuple[SourceEntry, dict[str, Delivery]]:
    """The pining entry as listed, and every artifact fetched through the adapter."""
    http = FakePining(provider, [entry], {(entry["id"], key): raw for key, raw in files.items()})
    pining = adapter(http)
    listed = next(iter(pining.list_entries()))
    return listed, {artifact: pining.fetch(listed, artifact) for artifact in listed.artifacts}


def _canonical(parsed: ParsedMatch) -> dict[str, list[dict[str, Any]]]:
    return {
        name: table.drop_columns([c for c in AUDIT if c in table.column_names]).to_pylist()
        for name, table in parsed.tables.items()
    }


# --- IDSSE ------------------------------------------------------------------------------------------------------


def _idsse_listing() -> list[dict[str, Any]]:
    return json.loads(_bytes("pining", "idsse_matches_public.json"))["matches"]


def test_idsse_ids_and_recorded_hashes() -> None:
    """The 7 pining ids map to the 7 HF match ids; metadata and positions hashes equal the HF copies."""
    tree = {item["path"].rsplit("/", 1)[-1]: item for item in json.loads(_bytes("idsse", "hf_tree.json"))}
    listing = _idsse_listing()
    entries = list(adapter(FakePining("idsse", listing, {})).list_entries())
    hf_ids = {name.rsplit("DFL-MAT-", 1)[1].removesuffix(".xml") for name in tree if name.startswith("DFL_")}
    assert {e.native_match_id for e in entries} == hf_ids
    for raw in listing:
        match = raw["id"].removeprefix("DFL-MAT-")
        meta = raw["artifact_meta"]
        assert meta["metadata"]["sha256"] == sha(_bytes("idsse", f"matchinformation_{match}.xml"))
        positions = next(i for n, i in tree.items() if n.startswith("DFL_04_03") and n.endswith(f"{match}.xml"))
        assert meta["tracking"]["sha256"] == positions["lfs"]["oid"]
        events = next(i for n, i in tree.items() if n.startswith("DFL_03_02") and n.endswith(f"{match}.xml"))
        assert meta["events"]["size"] == events["size"]


def test_idsse_identical_bronze() -> None:
    files = {
        "metadata": _bytes("idsse", "matchinformation_J03WPY.xml"),
        "events": _bytes("idsse", "events_J03WPY_slice.xml"),
        "tracking": _bytes("idsse", "positions_J03WPY_slice.xml"),
    }
    raw = next(e for e in _idsse_listing() if e["id"] == "DFL-MAT-J03WPY")
    entry = owner_entry(raw["id"], files, **{k: raw[k] for k in ("competition_id", "season_id", "visibility")})
    listed, deliveries = _pining("idsse", entry, files)
    public = _public(
        "idsse", listed, {"metadata": files["metadata"], "events": files["events"], "positions": files["tracking"]}
    )
    for artifact, delivery in deliveries.items():
        assert delivery.content_sha256 == public[artifact].content_sha256, artifact
    assert _canonical(IdsseParser().parse(listed, deliveries, run_id="pining")) == _canonical(
        IdsseParser().parse(listed, public, run_id="public")
    )


# --- SkillCorner --------------------------------------------------------------------------------------------------


def test_skillcorner_ids_and_recorded_hashes() -> None:
    listing = json.loads(_bytes("pining", "skillcorner_matches_public.json"))["matches"]
    tree = json.loads(_bytes("skillcorner", "github_tree.json"))["tree"]
    github_ids = {
        item["path"].split("/")[2]
        for item in tree
        if item["path"].startswith("data/matches/") and item["path"].count("/") == 2
    }
    entries = list(adapter(FakePining("skillcorner", listing, {})).list_entries())
    assert {e.native_match_id for e in entries} == github_ids
    by_id = {e["id"]: e["artifact_meta"] for e in listing}
    for match in ("1899585", "1874553"):
        assert by_id[match][f"{match}_match"]["sha256"] == sha(_bytes("skillcorner", f"match_{match}.json"))
    assert by_id["1899585"]["1899585_tracking_extrapolated"]["sha256"] == SKILLCORNER_TRACKING_OID
    sizes = {item["path"].rsplit("/", 1)[-1]: item["size"] for item in tree if "/1899585/" in item["path"]}
    assert by_id["1899585"]["1899585_dynamic_events"]["size"] != sizes["1899585_dynamic_events.csv"]  # older version


def test_skillcorner_legacy_public_identical_bronze() -> None:
    files = {
        "1899585_match": _bytes("skillcorner", "match_1899585.json"),
        "1899585_tracking_extrapolated": _bytes("skillcorner", "tracking_1899585_slice.jsonl"),
        "1899585_dynamic_events": _bytes("skillcorner", "dynamic_events_1899585_slice.csv"),
        "1899585_phases_of_play": _bytes("skillcorner", "phases_of_play_1899585_slice.csv"),
    }
    listed, deliveries = _pining("skillcorner", owner_entry("1899585", files, visibility="public"), files)
    public = _public(
        "skillcorner",
        listed,
        {key.removeprefix("1899585_").replace("tracking_extrapolated", "tracking"): raw for key, raw in files.items()},
    )
    assert set(deliveries) == set(public)
    assert _canonical(SkillCornerParser().parse(listed, deliveries, run_id="p")) == _canonical(
        SkillCornerParser().parse(listed, public, run_id="q")
    )


def test_skillcorner_format_2_identical_bronze() -> None:
    """§12.5: the same match as JSONL/CSV (public) and as format 2 Parquet (pining owner tier) gives the same bronze."""
    files = {
        "metadata": _bytes("skillcorner", "match_1899585.json"),
        "tracking": _bytes("skillcorner", "tracking_1899585_slice.parquet"),
        "events": _bytes("skillcorner", "dynamic_events_1899585_slice.parquet"),
    }
    listed, deliveries = _pining("skillcorner", owner_entry("1899585", files, format_version=2), files)
    assert set(deliveries) == {"match", "tracking_parquet", "dynamic_events_parquet"}
    public = _public(
        "skillcorner",
        listed,
        {
            "match": files["metadata"],
            "tracking": _bytes("skillcorner", "tracking_1899585_slice.jsonl"),
            "dynamic_events": _bytes("skillcorner", "dynamic_events_1899585_slice.csv"),
        },
    )
    pining = _canonical(SkillCornerParser().parse(listed, deliveries, run_id="p"))
    direct = _canonical(SkillCornerParser().parse(listed, public, run_id="q"))
    for rows in (*pining.values(), *direct.values()):
        for row in rows:
            row.pop("_source_hash", None)  # Parquet and JSONL are different files of the same data
    assert pining == direct


# --- StatsBomb ----------------------------------------------------------------------------------------------------


def test_statsbomb_redistributed_identical_bronze() -> None:
    """The open-data files and their pining owner-tier copy (`redistributed`: events and 360 gzipped, roster =
    lineups, plus the per-match metadata object) give the same events, lineups and 360 bronze, and a metadata row
    equal to the open-data match-list row."""
    events, lineups, frames = (
        _bytes("statsbomb", name)
        for name in ("events_9100001.json", "lineups_9100001.json", "three_sixty_9100001.json")
    )
    row = next(m for m in json.loads(_bytes("statsbomb", "matches_43_106.json")) if m["match_id"] == 9100001)
    metadata = {
        **row,
        **dict.fromkeys(("attendance", "behind_closed_doors", "neutral_ground", "collection_status", "play_status")),
    }
    files = {
        "events": gzip.compress(events, mtime=0),
        "freeze_frames": gzip.compress(frames, mtime=0),
        "roster": lineups,
        "metadata": json.dumps(metadata).encode(),
    }
    entry = owner_entry("9100001", files, provenance="redistributed", competition_id="43", season_id="106")
    listed, deliveries = _pining("statsbomb", entry, files)
    public = _public("statsbomb", listed, {"events": events, "lineups": lineups, "three_sixty": frames})
    for artifact in public:
        assert deliveries[artifact].content_sha256 == public[artifact].content_sha256, artifact
    assert deliveries["events"].wrapper == "gzip"
    parser = StatsBombParser()
    pining = _canonical(parser.parse(listed, deliveries, run_id="p"))
    meta = pining.pop(MATCH_METADATA)
    assert pining == _canonical(parser.parse(listed, public, run_id="q"))
    listing = parser.listing("matches", _bytes("statsbomb", "matches_43_106.json"), run_id="r", source_hash="h" * 64)
    listed_row = next(r for r in listing.to_pylist() if r["match_id"] == 9100001)
    shared = set(meta[0]) & set(listed_row) - {"_source_hash", *AUDIT}
    assert shared >= {"match_id", "home_team_managers", "competition_key", "season_key", "match_key"}
    assert {k: meta[0][k] for k in shared} == {k: listed_row[k] for k in shared}


# --- Gradient Sports ------------------------------------------------------------------------------------------------


def test_gradientsports_identical_bronze() -> None:
    """GS has no licence, so the pair is proven on the synthetic fixtures: the Drive files and their pining copy (the
    tracking file bz2-wrapped on both channels) give the same bronze."""
    files = {
        "metadata": _bytes("gradientsports", "metadata_9001.json"),
        "roster": _bytes("gradientsports", "roster_9001.json"),
        "events": _bytes("gradientsports", "events_9001.json"),
        "tracking": bz2.compress(_bytes("gradientsports", "tracking_9001.jsonl")),
    }
    listed, deliveries = _pining(
        "gradientsports", owner_entry("9001", files, competition_id="9900", season_name="2022"), files
    )
    public = _public("gradientsports", listed, files)
    for artifact in files:
        assert deliveries[artifact].content_sha256 == public[artifact].content_sha256, artifact
    assert _canonical(GradientSportsParser().parse(listed, deliveries, run_id="p")) == _canonical(
        GradientSportsParser().parse(listed, public, run_id="q")
    )
