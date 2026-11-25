"""A skillcorner match without tracking frames does not fail the run (owner 2026-11-24, impl review r2 B-08).

`fct_match_params.frame_rate` has a dbt `not_null`, and a skillcorner match takes its rate from the tracking bronze.
An events-only skillcorner match (match + dynamic events, no tracking file) has no rate. Owner ruling: the rule
applies only to matches with tracking frames. A match with no frames has nothing to convert, so it has no converter
parameters: no `fct_match_params` row. Then neither the dbt build nor line_breaking (whose discovery joins
`fct_match_params`) fails on it.

One run ingests the skillcorner fixture with the default compute. Each test deletes the match's tracking rows on a
working copy (never published), so each test starts from the same lake.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from computekit import PARSERS, fixture_adapters
from lakekit import LakeKit, lake_roots
from runkit import NOW, active, save_settings

from lakehouse.catalog.working import WorkingCatalog
from lakehouse.compute import input_key as ik
from lakehouse.compute.registry import DEFAULT_COMPUTE_STEPS
from lakehouse.compute.tracking_steps import LINE_BREAKING
from lakehouse.config.model import ResourceCaps
from lakehouse.runs.dbt import run_dbt
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

_MODULE = "skillcorner_public"
CAPS = ResourceCaps(threads=1, memory_limit="1GB", arrow_cpus=1)


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> LakeKit:
    root: Path = tmp_path_factory.mktemp("events-only")
    roots = lake_roots(root)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=root / "boot.json")
    kit = LakeKit(base=root, roots=roots, storage=LocalStorage(roots))
    save_settings(kit.storage, [active(_MODULE)])  # type: ignore[arg-type]
    adapters = fixture_adapters()
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={_MODULE: adapters[_MODULE]},  # type: ignore[index]
        parsers=PARSERS,
        now=lambda: NOW,
        work_dir=kit.base / "work",
        compute=DEFAULT_COMPUTE_STEPS,
    )
    assert result.status == "ok", [f"{e.kind}: {e.message}" for e in result.errors]
    return kit


@pytest.fixture
def events_only(lake: LakeKit) -> Iterator[WorkingCatalog]:
    """A working copy whose skillcorner match keeps its match row and dynamic events but has no tracking rows."""
    cat = lake.catalog(run_id="run-events-only")
    try:
        conn = cat.conn
        assert conn.execute(ik.LINE_BREAKING_DISCOVERY_SQL).fetchall(), "the fixture match must have tracking first"
        conn.execute("DELETE FROM lake.main.skillcorner_tracking_frames")
        conn.execute("DELETE FROM lake.main.skillcorner_tracking_players")
        assert conn.execute("SELECT count(*) FROM lake.main.skillcorner_matches").fetchone() == (1,)
        assert conn.execute("SELECT count(*) FROM lake.main.fct_actions").fetchone() != (0,)
        yield cat
    finally:
        cat.close()


def test_an_events_only_match_has_no_converter_parameters(events_only: WorkingCatalog) -> None:
    rows = events_only.conn.execute("SELECT count(*) FROM lake.main.fct_match_params").fetchone()
    assert rows == (0,)


def test_the_dbt_build_passes_with_an_events_only_match(events_only: WorkingCatalog) -> None:
    built = run_dbt(events_only, run_id="run-events-only", caps=CAPS)
    failed = [r.unique_id for r in built.results if r.status in ("error", "fail", "runtime error")]
    assert built.success, failed


def test_line_breaking_does_not_visit_an_events_only_match(events_only: WorkingCatalog) -> None:
    errors: list[dict[str, Any]] = []

    def error(**kw: Any) -> None:
        errors.append(kw)

    outcome = LINE_BREAKING(
        events_only,
        run_id="run-events-only",
        caps=CAPS,
        now=lambda: datetime(2026, 11, 24, tzinfo=UTC),
        error=error,
    )
    assert (outcome.matches, outcome.failed_matches) == (0, 0)
    assert errors == []
