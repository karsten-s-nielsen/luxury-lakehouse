"""Live end-to-end on the public sources (markers `live` and `heavy`; plan Task 31, spec §17, §G2).

On a fresh local lake with the laptop defaults restricted to two matches per active module (a test-only filter), one
`run_ingestion` downloads, parses, resolves and builds the dbt dimensions. The test asserts bronze for every active
module, the dimensions, items or links in the review queue, and that a second run downloads nothing and re-parses
nothing. It carries `heavy` because it fetches real tracking, so it runs on the Mac Mini only (the AI-PC runs
`-m "live and not heavy"`, which deselects it; C7 keeps a full parse off the AI-PC)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from lakekit import LakeKit

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import MatchClause, ModuleConfig, Settings, load_defaults
from lakehouse.config.store import ConfigStore
from lakehouse.net.http import HttpClient
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion
from lakehouse.wiring import build_adapter, default_parsers

pytestmark = [pytest.mark.live, pytest.mark.heavy]

ACTIVE = ("statsbomb_public", "wyscout_public", "idsse_public", "skillcorner_public")
PROVIDERS = ("statsbomb", "wyscout", "idsse", "skillcorner")
PER_MODULE = 2


def _restrict_to_two(defaults: Settings, cache: Path, http: HttpClient) -> Settings:
    """List each active module and keep the first two matches, as a test-only match filter."""
    modules: list[ModuleConfig] = []
    for module in defaults.modules:
        if module.module_id in ACTIVE:
            adapter = build_adapter(module, settings=defaults, env={}, cache=cache, http=http)
            ids: list[str] = []
            for entry in adapter.list_entries():
                ids.append(entry.native_match_id)
                if len(ids) == PER_MODULE:
                    break
            assert len(ids) == PER_MODULE, module.module_id
            modules.append(module.model_copy(update={"filter": [MatchClause(match_ids=ids)]}))
        else:
            modules.append(module)  # Gradient Sports and the pining modules stay inactive
    return defaults.model_copy(update={"modules": modules})


def _count(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0]) if row else 0


def _run(kit: LakeKit, settings: Settings, cache: Path, work: Path) -> RunResult:
    http = HttpClient()
    adapters = {
        m.module_id: build_adapter(m, settings=settings, env={}, cache=cache, http=http)
        for m in settings.modules
        if m.active
    }
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters=adapters,  # type: ignore[arg-type]
        parsers=default_parsers(),
        work_dir=work,
        settings=settings,
        env={},
    )


def test_e2e_public_small(kit: LakeKit, tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    defaults = load_defaults("laptop")
    restricted = _restrict_to_two(defaults, cache, HttpClient())
    ConfigStore(kit.storage).save(restricted, expected_version=0, operator="e2e")

    first = _run(kit, restricted, cache, tmp_path / "work-1")
    assert first.status == "ok", [e.kind for e in first.errors]
    assert [e for e in first.errors if e.kind == "unmapped_field"] == []
    assert first.counts.added >= len(ACTIVE) * PER_MODULE  # every active module brought in its two matches

    with CatalogReader(kit.storage).session() as conn:
        tables = {
            t for (t,) in conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main'").fetchall()
        }
        for provider in PROVIDERS:  # bronze for every active module (at least one table per provider)
            assert any(t.startswith(provider) for t in tables), provider
        match_providers = {p for (p,) in conn.execute("SELECT DISTINCT provider FROM lake.main.dim_matches").fetchall()}
        assert set(PROVIDERS) <= match_providers, match_providers
        for dim in ("dim_players", "dim_teams", "dim_competitions", "dim_seasons", "dim_matches"):
            assert _count(conn, f"SELECT count(*) FROM lake.main.{dim}") > 0, dim  # noqa: S608 - constant names
        queue = _count(conn, "SELECT count(*) FROM lake.main.ops_review_queue")
        links = sum(
            _count(conn, f"SELECT count(*) FROM lake.main.gold_{e}_links")  # noqa: S608 - constant entity names
            for e in ("player", "team", "competition", "match")
        )
        assert queue + links > 0, "the review queue holds items or links"

    second = _run(kit, restricted, cache, tmp_path / "work-2")
    assert second.status == "ok", [e.kind for e in second.errors]
    assert (second.counts.added, second.counts.changed) == (0, 0), (
        "a second run downloads nothing and re-parses nothing"
    )
    with CatalogReader(kit.storage).session() as conn:
        [row] = [r for r in read_table(conn, "ops_runs") if r["run_id"] == second.run_id]
        assert row["status"] == "ok"
