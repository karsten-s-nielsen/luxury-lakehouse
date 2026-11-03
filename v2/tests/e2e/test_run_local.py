"""A full run on fixtures through a local lake (spec §17, §23.6; plan Tasks 21 and 23).

The five providers' real parsers read the repository's fixtures, served by fake adapters (no network), with their
source-level listings. The run writes bronze, resolves (links, queue, pointers) and builds the dbt dimensions. A
second run with a narrowed filter keeps the excluded match's dims and links (spec §8.1, §29.13).
"""

from __future__ import annotations

import bz2
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, ListingAdapter, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CompetitionClause, ModuleId
from lakehouse.ids.native import Provider
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PARSERS: dict[Provider, Any] = {
    "statsbomb": StatsBombParser(),
    "idsse": IdsseParser(),
    "skillcorner": SkillCornerParser(),
    "wyscout": WyscoutParser(),
    "gradientsports": GradientSportsParser(),
}
WYSCOUT_LISTINGS = {
    "competitions": "competitions.json",
    "teams": "teams.json",
    "players": "players_slice.json",
    "coaches": "coaches_slice.json",
    "event_names": "eventid2name.csv",
    "tag_names": "tags2name.csv",
}


def _adapter(
    module: ModuleId,
    provider: Provider,
    native: str,
    files: Mapping[str, bytes],
    *,
    competition: tuple[str, str],
    listings: Mapping[str, bytes],
    has_360: bool | None = None,
) -> ListingAdapter:
    listed = entry(
        module,
        native,
        signals=dict.fromkeys(files, "s1"),
        competition=competition,
        has_360=has_360,  # type: ignore[arg-type]
    )
    raw = {(native, artifact): data for artifact, data in files.items()}
    return ListingAdapter(module, provider, "public", [listed], raw, listings=listings)


def _read(folder: str, name: str) -> bytes:
    return (FIXTURES / folder / name).read_bytes()


def _adapters() -> dict[ModuleId, FakeAdapter]:
    statsbomb = _adapter(
        "statsbomb_public",
        "statsbomb",
        "9100001",
        {
            "events": _read("statsbomb", "events_9100001.json"),
            "lineups": _read("statsbomb", "lineups_9100001.json"),
            "three_sixty": _read("statsbomb", "three_sixty_9100001.json"),
        },
        competition=("43", "106"),
        listings={
            "competitions": _read("statsbomb", "competitions.json"),
            "matches/43/106": _read("statsbomb", "matches_43_106.json"),
        },
        has_360=True,
    )
    idsse = _adapter(
        "idsse_public",
        "idsse",
        "J03WPY",
        {
            "metadata": _read("idsse", "matchinformation_J03WPY.xml"),
            "events": _read("idsse", "events_J03WPY_slice.xml"),
            "positions": _read("idsse", "positions_J03WPY_slice.xml"),
        },
        competition=("DFL-COM-000002", "DFL-SEA-0001K6"),
        listings={},
    )
    skillcorner = _adapter(
        "skillcorner_public",
        "skillcorner",
        "1899585",
        {
            "match": _read("skillcorner", "match_1899585.json"),
            "tracking": _read("skillcorner", "tracking_1899585_slice.jsonl"),
            "dynamic_events": _read("skillcorner", "dynamic_events_1899585_slice.csv"),
            "phases_of_play": _read("skillcorner", "phases_of_play_1899585_slice.csv"),
        },
        competition=("61", "95"),
        listings={"matches": _read("skillcorner", "matches.json")},
    )
    wyscout = _adapter(
        "wyscout_public",
        "wyscout",
        "2058017",
        {
            "matches": _read("wyscout", "matches_World_Cup_slice.json"),
            "events": _read("wyscout", "events_World_Cup_slice.json"),
        },
        competition=("28", "10078"),
        listings={kind: _read("wyscout", name) for kind, name in WYSCOUT_LISTINGS.items()},
    )
    gradientsports = _adapter(
        "gradientsports_public",
        "gradientsports",
        "9001",
        {
            "metadata": _read("gradientsports", "metadata_9001.json"),
            "roster": _read("gradientsports", "roster_9001.json"),
            "events": _read("gradientsports", "events_9001.json"),
            "tracking": bz2.compress(_read("gradientsports", "tracking_9001.jsonl")),  # as the Drive folder delivers it
        },
        competition=("77", "2030"),
        listings={
            "competitions": _read("gradientsports", "competitions.csv"),
            "players": _read("gradientsports", "players.csv"),
        },
    )
    return {a.module_id: a for a in (statsbomb, idsse, skillcorner, wyscout, gradientsports)}


def _run(kit: LakeKit, adapters: Mapping[ModuleId, FakeAdapter]) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters=adapters,  # type: ignore[arg-type]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )


def _count(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    found = conn.execute(sql).fetchone()
    assert found is not None
    return int(found[0])


def test_full_run_gives_bronze_links_and_dims(kit: LakeKit) -> None:
    adapters = _adapters()
    save_settings(kit.storage, [active(module) for module in adapters])
    first = _run(kit, adapters)
    assert first.status == "ok", first.errors
    assert first.counts.added == 5
    with CatalogReader(kit.storage).session() as conn:
        for table in (
            "statsbomb_events",
            "statsbomb_three_sixty",
            "idsse_tracking_players",
            "skillcorner_tracking_players",
            "wyscout_events",
            "gradientsports_events",
            "gradientsports_tracking_players",
        ):
            assert _count(conn, f"SELECT count(*) FROM lake.main.{table}") > 0, table  # noqa: S608 - constant names
        for entity in ("player", "team", "competition", "match"):
            conn.execute(f"SELECT count(*) FROM lake.main.gold_{entity}_links").fetchone()  # noqa: S608 - constant names
        providers = {p for (p,) in conn.execute("SELECT DISTINCT provider FROM lake.main.dim_players").fetchall()}
        assert providers == set(PARSERS)
        for dim in ("dim_teams", "dim_competitions", "dim_seasons", "dim_matches"):
            assert _count(conn, f"SELECT count(*) FROM lake.main.{dim}") > 0, dim  # noqa: S608 - constant names
        assert _count(conn, "SELECT count(*) FROM lake.main.tracking_cross_provider") > 0
        # Task 21 carry (§11.3, §29.13): ops_matches and dim_matches agree with the 360 bronze rows
        statsbomb = conn.execute(
            "SELECT m.match_key, m.has_360_frames, d.has_360_frames, "
            "(SELECT count(*) FROM lake.main.statsbomb_three_sixty t WHERE t.match_key = m.match_key) > 0 "
            "FROM lake.main.ops_matches m JOIN lake.main.dim_matches d ON d.match_key = m.match_key "
            "WHERE m.provider = 'statsbomb'"
        ).fetchall()
        assert statsbomb and all(ops is dim is rows for _, ops, dim, rows in statsbomb)
        others = conn.execute(
            "SELECT has_360_frames FROM lake.main.dim_matches WHERE provider <> 'statsbomb'"
        ).fetchall()
        assert others and all(flag is None for (flag,) in others)
        idsse_match = _count(conn, "SELECT count(*) FROM lake.main.dim_matches WHERE native_match_id = 'J03WPY'")
        idsse_players = _count(conn, "SELECT count(*) FROM lake.main.dim_players WHERE provider = 'idsse'")
        links_before = {
            entity: _count(conn, f"SELECT count(*) FROM lake.main.gold_{entity}_links")  # noqa: S608 - constant names
            for entity in ("player", "team", "competition", "match")
        }
    assert idsse_match == 1
    # Task 21 carry (§8.1, §29.13): a narrowed filter keeps the excluded match's dims and links
    save_settings(
        kit.storage,
        [
            *(active(module) for module in adapters if module != "idsse_public"),
            active("idsse_public", CompetitionClause(competition_id="DFL-COM-999999", seasons="all")),
        ],
    )
    adapters["idsse_public"].entries = []  # the adapter lists only the selected matches
    second = _run(kit, adapters)
    assert second.status == "ok", second.errors
    assert second.counts.excluded == 1 and second.counts.added == 0
    with CatalogReader(kit.storage).session() as conn:
        assert _count(conn, "SELECT count(*) FROM lake.main.dim_matches WHERE native_match_id = 'J03WPY'") == 1
        assert _count(conn, "SELECT count(*) FROM lake.main.dim_players WHERE provider = 'idsse'") == idsse_players
        for entity, before in links_before.items():
            assert _count(conn, f"SELECT count(*) FROM lake.main.gold_{entity}_links") == before  # noqa: S608
        [run] = [r for r in read_table(conn, "ops_runs") if r["run_id"] == second.run_id]
    assert run["excluded"] == 1
