"""A full run on fixtures through a local lake (spec §17, §23.6; plan Tasks 21 and 23).

The five providers' real parsers read the repository's fixtures, served by fake adapters (no network), with their
source-level listings. The run writes bronze, resolves (links, queue, pointers) and builds the dbt dimensions. A
second run with a narrowed filter keeps the excluded match's dims and links (spec §8.1, §29.13).
"""

from __future__ import annotations

from collections.abc import Mapping

import duckdb
from computekit import PARSERS, fixture_adapters
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, active, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CompetitionClause, ModuleId
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion

_adapters = fixture_adapters  # the five providers' fixture adapters (tests/support/computekit.py)


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
            assert _count(conn, f"SELECT count(*) FROM lake.main.gold_{entity}_links") == before  # noqa: S608 - test constants, no outside input
        [run] = [r for r in read_table(conn, "ops_runs") if r["run_id"] == second.run_id]
    assert run["excluded"] == 1
