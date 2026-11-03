"""A real offline run writes its observability object and its masked log copy (spec §7.1-§7.3, §9; plan Task 10,
Reviewer B OBS-B20). Before this cycle nothing was wired: a real run wrote no rows and no logs. `observe_run` is the
exact wiring the CLI `run`/`reparse` entrypoints use, so running it around `run_ingestion` exercises the production
path on the offline fixture lake."""

from __future__ import annotations

import bz2
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, ListingAdapter, active, entry, save_settings

from lakehouse.cli import observe_run
from lakehouse.config.model import ModuleId
from lakehouse.ids.native import Provider
from lakehouse.observability import read_logs, read_task_rows
from lakehouse.parsers.gradientsports import GradientSportsParser
from lakehouse.parsers.idsse import IdsseParser
from lakehouse.parsers.skillcorner import SkillCornerParser
from lakehouse.parsers.statsbomb import StatsBombParser
from lakehouse.parsers.wyscout import WyscoutParser
from lakehouse.runs.run import new_run_id, run_ingestion

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
SECRET = "hf_secret_token_value_123456"  # noqa: S105 - a test value carried by a known secret var
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


def _read(folder: str, name: str) -> bytes:
    return (FIXTURES / folder / name).read_bytes()


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
    listed = entry(module, native, signals=dict.fromkeys(files, "s1"), competition=competition, has_360=has_360)  # type: ignore[arg-type]
    raw = {(native, artifact): data for artifact, data in files.items()}
    return ListingAdapter(module, provider, "public", [listed], raw, listings=listings)


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
            "tracking": bz2.compress(_read("gradientsports", "tracking_9001.jsonl")),
        },
        competition=("77", "2030"),
        listings={
            "competitions": _read("gradientsports", "competitions.csv"),
            "players": _read("gradientsports", "players.csv"),
        },
    )
    return {a.module_id: a for a in (statsbomb, idsse, skillcorner, wyscout, gradientsports)}


def test_run_writes_observability_and_logs(kit: LakeKit) -> None:
    adapters = _adapters()
    save_settings(kit.storage, [active(module) for module in adapters])
    run_id = new_run_id(NOW)
    env = {"HF_TOKEN": SECRET, "LAKEHOUSE_EXECUTION_CONTEXT": "local"}

    with observe_run(kit.storage, run_id, env):
        logging.getLogger("lakehouse.run").warning("a run log mentioning the %s in a line", SECRET)
        result = run_ingestion(
            storage=kit.storage,
            trigger="cli",
            adapters=adapters,  # type: ignore[arg-type]
            parsers=PARSERS,
            now=lambda: NOW,
            run_id=run_id,
            work_dir=kit.base / "work",
            env=env,
        )
    assert result.status == "ok", result.errors

    rows = read_task_rows(kit.storage, run_id)
    assert rows, "the run wrote no observability rows (OBS-B20)"
    by_task = {row.task: row for row in rows}
    run_row = by_task["lakehouse.run"]
    assert run_row.execution_context == "local"
    assert run_row.cost_usd == 0.0 and run_row.cost_error is None  # a local run is free, never a silent error
    layers = {row.layer for row in rows}
    assert {"bronze", "resolution", "dbt"} <= layers  # the per-step layer attribute is set on every data step
    assert all(row.state in {"ok", "failed", "skipped"} for row in rows)

    logs = read_logs(kit.storage, run_id, limit=2000)
    assert logs, "the run collected no logs"
    assert any("in a line" in record.msg for record in logs)  # the run's own log line was collected
    assert all(SECRET not in record.msg for record in logs)  # every field of the collected copy is masked
