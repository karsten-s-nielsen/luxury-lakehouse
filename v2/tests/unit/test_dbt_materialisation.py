"""The dbt project and the hand-off (spec §9, §10.3, §11.6, §14.3, §23.6; plan Task 23).

The manifest tests parse the project once (`dbt parse`, no connection). The build tests run `run_dbt` on a working
copy, as the run does: detach, dbt attaches, re-attach.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from lakekit import LakeKit

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.ids.keys import match_key, team_key
from lakehouse.resolve.decisions import DecisionStore, MustLink
from lakehouse.resolve.links import run_resolution
from lakehouse.resolve.model import RecordRef
from lakehouse.runs.dbt import (
    DbtResult,
    parse_manifest,
    project_dir,
    render_profile,
    run_dbt,
    run_results_key,
    tables_outside_allowlist,
)
from lakehouse.runs.history import MatchRow, replace_match

CAPS = load_defaults("laptop").caps
DIMS = {
    "dim_players": ("player_key", "native_player_id", "canonical_player_key"),
    "dim_teams": ("team_key", "native_team_id", "canonical_team_key"),
    "dim_competitions": ("competition_key", "native_competition_id", "canonical_competition_key"),
    "dim_seasons": ("season_key", "native_season_id", "canonical_season_key"),
    "dim_matches": ("match_key", "native_match_id", "canonical_match_key"),
}


@pytest.fixture(scope="module")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return parse_manifest(tmp_path_factory.mktemp("dbt-parse"), caps=CAPS)


def _models(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {node["name"]: node for node in manifest["nodes"].values() if node["resource_type"] == "model"}


def test_row_level_models_are_views(manifest: dict[str, Any], tmp_path: Path) -> None:
    """§23.6: only dimension, link, lineage and ops models are tables; a row-level table model makes it fail."""
    assert tables_outside_allowlist(manifest) == []
    models = _models(manifest)
    assert models["tracking_cross_provider"]["config"]["materialized"] == "view"
    assert {models[name]["config"]["materialized"] for name in DIMS} == {"table"}
    copy = tmp_path / "project"
    shutil.copytree(project_dir(), copy, ignore=shutil.ignore_patterns("target", "logs"))
    (copy / "models" / "gold" / "events_copy.sql").write_text(
        "{{ config(materialized='table') }}\nselect 1 as frame\n", encoding="utf-8"
    )
    (copy / "models" / "gold" / "frames_delta.sql").write_text(
        "{{ config(materialized='incremental', on_schema_change='fail') }}\nselect 1 as frame\n", encoding="utf-8"
    )
    widened = parse_manifest(tmp_path / "work", caps=CAPS, project=copy)
    assert tables_outside_allowlist(widened) == ["events_copy", "frames_delta"]


def test_gold_contracts_enforced(manifest: dict[str, Any]) -> None:
    gold = {name: node for name, node in _models(manifest).items() if node["fqn"][1] == "gold"}
    assert set(gold) == {*DIMS, "tracking_cross_provider"}
    for name, node in gold.items():
        assert node["config"]["contract"]["enforced"] is True, name
        assert all(column.get("data_type") for column in node["columns"].values()), name


def test_dims_keep_natural_keys(manifest: dict[str, Any]) -> None:
    models = _models(manifest)
    for name, (key, native, canonical) in DIMS.items():
        assert {key, "provider", native, canonical} <= set(models[name]["columns"]), name


def test_v1_dim_parity_shape(manifest: dict[str, Any]) -> None:
    columns = set(_models(manifest)["dim_players"]["columns"])
    assert {"player_key", "provider", "native_player_id", "canonical_player_key"} <= columns
    assert "canonical_player_id" not in columns  # the legacy v1 string is dropped (spec §14.3, guess 50)


def test_project_ships_in_the_wheel() -> None:
    """HF images install the wheel: the dbt project must travel inside it (`lakehouse/_dbt`)."""
    pyproject = (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text(encoding="utf-8")
    assert '[tool.hatch.build.targets.wheel.force-include]\n"dbt" = "lakehouse/_dbt"' in pyproject
    assert (project_dir() / "dbt_project.yml").is_file()


def test_profile_holds_the_attach_and_caps(kit: LakeKit) -> None:
    text = render_profile(catalog_path=Path("C:/work/lake.ducklake"), caps=CAPS, storage=kit.storage, env={})
    assert 'path: "ducklake:C:/work/lake.ducklake"' in text
    assert f"threads: {CAPS.threads}" in text and f'memory_limit: "{CAPS.memory_limit}"' in text
    assert "secrets:" not in text  # a local lake needs none


@pytest.fixture
def working(kit: LakeKit) -> Iterator[WorkingCatalog]:
    cat = kit.catalog(run_id="run-dbt")
    yield cat
    cat.close()


def _statsbomb_lake(cat: WorkingCatalog) -> tuple[int, int]:
    """Two StatsBomb matches in the match listing, 360 rows for the first only, and one IDSSE match."""
    conn = cat.conn
    conn.execute(
        "CREATE TABLE lake.main.statsbomb_matches (match_id BIGINT, match_date DATE, home_team_id BIGINT, "
        "home_team_name VARCHAR, home_team_country_name VARCHAR, away_team_id BIGINT, away_team_name VARCHAR, "
        "away_team_country_name VARCHAR, competition_id BIGINT, competition_name VARCHAR, "
        "competition_country_name VARCHAR, season_id BIGINT, season_name VARCHAR)"
    )
    conn.execute(
        "INSERT INTO lake.main.statsbomb_matches VALUES "
        "(3788741, DATE '2021-06-11', 909, 'Turkey', 'Turkey', 914, 'Italy', 'Italy', 55, 'UEFA Euro', 'Europe', 43, "
        "'2020'), (3788742, DATE '2021-06-12', 773, 'Wales', 'Wales', 1110, 'Switzerland', 'Switzerland', 55, "
        "'UEFA Euro', 'Europe', 43, '2020')"
    )
    conn.execute(
        "CREATE TABLE lake.main.idsse_match_information (native_match_id VARCHAR, general_kickoff_time TIMESTAMPTZ, "
        "general_home_team_id VARCHAR, general_home_team_name VARCHAR, general_guest_team_id VARCHAR, "
        "general_guest_team_name VARCHAR, general_competition_id VARCHAR, general_competition_name VARCHAR, "
        "general_season_id VARCHAR, "
        "general_season VARCHAR, environment_country VARCHAR)"
    )
    conn.execute(
        "INSERT INTO lake.main.idsse_match_information VALUES ('J03WMX', TIMESTAMPTZ '2022-08-05 18:30:00+00', "
        "'DFL-CLU-00000G', 'FC Bayern', 'DFL-CLU-000017', 'Frankfurt', 'DFL-COM-000001', 'Bundesliga', "
        "'DFL-SEA-0001K6', "
        "'2022/2023', 'Germany')"
    )
    with_360, without = match_key("statsbomb", "3788741"), match_key("statsbomb", "3788742")
    now = datetime(2026, 10, 27, tzinfo=UTC)
    for key, native, frames in ((with_360, "3788741", True), (without, "3788742", False)):
        row = MatchRow(
            match_key=key,
            provider="statsbomb",
            native_match_id=native,
            visibility="public",
            kept_channel="public",
            kept_reason="only copy",
            competition_id="55",
            competition_name=None,
            season_id="43",
            season_name=None,
            has_360_frames=frames,
            run_id=cat.run_id,
            updated_at=now,
        )
        replace_match(conn, row, [], [])
    return with_360, without


def test_dim_matches_has_360_frames(kit: LakeKit, working: WorkingCatalog) -> None:
    """§11.3, §29.13: true exactly for StatsBomb matches with 360 bronze rows, false for the others, null for other
    providers — the flag of ops_matches, the same source."""
    with_360, without = _statsbomb_lake(working)
    run_resolution(working.conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-dbt")
    result = run_dbt(working, run_id="run-dbt", caps=CAPS)
    assert result.success, [r for r in result.results if r.status not in ("success", "pass")]
    found = dict(
        working.conn.execute(
            "SELECT provider || ':' || native_match_id, has_360_frames FROM lake.main.dim_matches"
        ).fetchall()
    )
    assert found == {"statsbomb:3788741": True, "statsbomb:3788742": False, "idsse:J03WMX": None}
    assert {with_360, without} <= {
        k for (k,) in working.conn.execute("SELECT match_key FROM lake.main.dim_matches").fetchall()
    }


def test_dims_carry_the_canonical_pointer(kit: LakeKit, working: WorkingCatalog) -> None:
    """§14.3: a dimension row carries its cluster's canonical key — here an owner's must-link joins an IDSSE team to
    a StatsBomb team, and both rows point to the StatsBomb record (the higher rank)."""
    _statsbomb_lake(working)
    turkey, bayern = RecordRef("statsbomb", "909"), RecordRef("idsse", "DFL-CLU-00000G")
    DecisionStore(kit.storage).add(
        MustLink(entity="team", a=turkey, b=bayern, decided_by="karsten", decided_at=datetime.now(UTC), reason="test")
    )
    run_resolution(working.conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-dbt")
    assert run_dbt(working, run_id="run-dbt", caps=CAPS).success
    found = dict(
        working.conn.execute(
            "SELECT provider || ':' || native_team_id, canonical_team_key FROM lake.main.dim_teams"
        ).fetchall()
    )
    assert found["statsbomb:909"] == found["idsse:DFL-CLU-00000G"] == team_key("statsbomb", "909")
    assert found["statsbomb:914"] == team_key("statsbomb", "914")  # an unlinked team points to itself


def test_run_dbt_hands_off_and_keeps_run_results(kit: LakeKit, working: WorkingCatalog) -> None:
    """§10.3: the working copy is detached for dbt and attached again after; the run's dbt results are kept."""
    _statsbomb_lake(working)
    run_resolution(working.conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-dbt")
    result = run_dbt(working, run_id="run-dbt", caps=CAPS)
    assert isinstance(result, DbtResult) and result.success
    assert result.run_results_key == run_results_key("run-dbt") == "runs/dbt/run-dbt/run_results.json"
    stored = json.loads(kit.storage.get("lake", result.run_results_key))
    assert {r["unique_id"] for r in stored["results"]} == {r.unique_id for r in result.results}
    assert any(r.unique_id == "model.lakehouse.dim_players" and r.status == "success" for r in result.results)
    # attached again: the run goes on with the same connection
    kinds = dict(
        working.conn.execute(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = 'lake'"
        ).fetchall()
    )
    assert kinds["dim_matches"] == "BASE TABLE" and kinds["tracking_cross_provider"] == "VIEW"
    assert kinds["stg_idsse__tracking_players"] == "VIEW"
    assert working.conn.execute("SELECT count(*) FROM lake.main.tracking_cross_provider").fetchone() == (0,)
    working.commit("run_end")  # the commit after dbt publishes the dbt models too


def test_run_dbt_reports_a_failed_model(kit: LakeKit, working: WorkingCatalog, tmp_path: Path) -> None:
    copy = tmp_path / "project"
    shutil.copytree(project_dir(), copy, ignore=shutil.ignore_patterns("target", "logs"))
    (copy / "models" / "gold" / "dim_players.sql").write_text(
        "{{ config(materialized='table') }}\nselect no_such_column from lake.main.ops_entity_records\n",
        encoding="utf-8",
    )
    run_resolution(working.conn, storage=kit.storage, settings=load_defaults("laptop"), run_id="run-dbt")
    result = run_dbt(working, run_id="run-dbt", caps=CAPS, project=copy)
    assert not result.success
    [failed] = [r for r in result.results if r.unique_id == "model.lakehouse.dim_players"]
    assert failed.status == "error" and failed.message
    assert working.conn.execute("SELECT 1").fetchone() == (1,)  # attached again after a failure too
    assert kit.storage.exists("lake", result.run_results_key)
