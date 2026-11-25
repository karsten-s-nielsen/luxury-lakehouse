"""gradientsports tracking reaches the gold views clean: raw positions only, one copy of each frame (owner 2026-11-08).

GS writes a frame record once per possession event, so one (period, frameNum) can appear several times with different
content (up to 16 copies on the real lake), and every record carries the raw AND the provider-smoothed player lists.
v2 bronze keeps both on purpose (the parser leaves the choice to silver); silver keeps only the raw rows (v1 scored
raw positions and let silly-kicks smooth them) and the FIRST record of each frame by `record_index` (v1 ADR-030
keep-first). The 2026-11-07/08 Mac run scored every GS frame with 44 player rows instead of 22 before this.

The fixture `tracking_9001.jsonl` already holds both cases: frame (1, 101) twice, and smoothed lists on every record.
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from computekit import PARSERS, fixture_adapters, read_fixture
from lakekit import LakeKit, lake_roots
from runkit import NOW, active, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

_MODULE = "gradientsports_public"
_GS = (
    "WHERE canonical_match_key IN "
    "(SELECT canonical_match_key FROM lake.main.dim_matches WHERE provider = 'gradientsports')"
)


def _fixture_frames() -> list[dict[str, Any]]:
    text = read_fixture("gradientsports", "tracking_9001.jsonl").decode("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    base = tmp_path_factory.mktemp("gs-clean")
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "boot.json")
    kit = LakeKit(base=base, roots=roots, storage=LocalStorage(roots))
    save_settings(kit.storage, [active(_MODULE)])
    adapters = fixture_adapters()
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={_MODULE: adapters[_MODULE]},  # type: ignore[index]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
    )
    with CatalogReader(kit.storage, env={}).session() as conn:
        return {
            "status": result.status,
            "errors": [f"{e.kind}: {e.message}" for e in result.errors],
            "frames": conn.execute(
                f"SELECT period, frame, canonical_player_key, x_mm, y_mm FROM lake.main.fct_tracking_frames {_GS}"  # noqa: S608 - test constants, no outside input
            ).fetchall(),
            "ball": conn.execute(
                f"SELECT period, frame, ball_x_mm, time_seconds FROM lake.main.fct_tracking_ball {_GS}"  # noqa: S608 - test constants, no outside input
            ).fetchall(),
        }


def test_the_run_ends_ok(lake: dict[str, Any]) -> None:
    assert lake["status"] == "ok", "\n".join(lake["errors"])


def test_each_player_appears_once_per_frame(lake: dict[str, Any]) -> None:
    keys = [(p, f, k) for p, f, k, _x, _y in lake["frames"]]
    assert keys and len(keys) == len(set(keys))


def test_frames_carry_the_raw_positions_of_the_first_record(lake: dict[str, Any]) -> None:
    records = _fixture_frames()
    first: dict[tuple[int, int], dict[str, Any]] = {}
    for record in records:
        first.setdefault((record["period"], record["frameNum"]), record)
    expected = sorted(
        round(float(p["x"]) * 1000)
        for record in first.values()
        for side in ("homePlayers", "awayPlayers")
        for p in cast("list[dict[str, Any]]", record.get(side) or [])
    )
    assert sorted(x for _p, _f, _k, x, _y in lake["frames"]) == expected


def test_each_frame_has_one_ball_row(lake: dict[str, Any]) -> None:
    keys = [(p, f) for p, f, _x, _t in lake["ball"]]
    assert keys and len(keys) == len(set(keys))
    assert len(keys) == len({(r["period"], r["frameNum"]) for r in _fixture_frames()})
