"""The owner-tier (RM-format) SkillCorner match file has no `match_periods`: the tracking computes still run.

The 2026-11-06 Mac clean-slate run failed every skillcorner `pitch_control` and `line_breaking` match: the frame
rate came only from `match_periods`, which this format does not carry, so `fct_match_params.frame_rate` was null
(smooth_frames: "single positional indexer is out-of-bounds"; merge_asof: "Merge keys contain null values on right
side"). The fixture match file is changed in memory only: `match_periods` is removed, as in the owner-tier payload.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from computekit import PARSERS, fixture_adapters, read_fixture
from lakekit import LakeKit, lake_roots
from runkit import NOW, active, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.compute.registry import REGISTERED_COMPUTE_STEPS
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

_MODULE = "skillcorner_public"
_IN_MATCH = "WHERE canonical_match_key IN (SELECT canonical_match_key FROM lake.main.dim_matches WHERE provider = ?)"


def _owner_format_match() -> bytes:
    match: dict[str, Any] = json.loads(read_fixture("skillcorner", "match_1899585.json"))
    del match["match_periods"]
    return json.dumps(match).encode("utf-8")


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    base: Path = tmp_path_factory.mktemp("owner")
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "boot.json")
    kit = LakeKit(base=base, roots=roots, storage=LocalStorage(roots))
    save_settings(kit.storage, [active(_MODULE)])
    adapters = fixture_adapters(skillcorner_match=_owner_format_match())
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={_MODULE: adapters[_MODULE]},  # type: ignore[index]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
        compute=REGISTERED_COMPUTE_STEPS,  # the excluded pitch_control + shape_graphs code stays tested (spec D10)
    )
    with CatalogReader(kit.storage, env={}).session() as conn:
        return {
            "status": result.status,
            "errors": [f"{e.kind}: {e.message}" for e in result.errors],
            "frame_rate": conn.execute(
                f"SELECT frame_rate FROM lake.main.fct_match_params {_IN_MATCH}",  # noqa: S608 - constant text
                ["skillcorner"],
            ).fetchall(),
            "pitch_control": conn.execute(
                f"SELECT count(*) FROM lake.main.pitch_control_values {_IN_MATCH}",  # noqa: S608 - constant text
                ["skillcorner"],
            ).fetchone(),
        }


def test_the_run_ends_ok(lake: dict[str, Any]) -> None:
    assert lake["status"] == "ok", "\n".join(lake["errors"])


def test_match_params_frame_rate_comes_from_the_tracking(lake: dict[str, Any]) -> None:
    assert lake["frame_rate"] == [(10.0,)]


def test_pitch_control_scores_the_owner_format_match(lake: dict[str, Any]) -> None:
    assert lake["pitch_control"] is not None
    assert lake["pitch_control"][0] > 0
