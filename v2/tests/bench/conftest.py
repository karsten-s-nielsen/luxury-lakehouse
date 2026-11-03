"""Bench fixtures (spec §10.9, §26 item 13; plan Task 17). The bench runs only on the Mac Mini (Task 30).

`LAKEHOUSE_BENCH_CACHE` names a folder that keeps the raw files between runs; the session fixture fetches them with
the v2 adapters, so the bench needs no spike data. `--bench-record` measures and writes `budget.json` instead of
checking against it.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from lakehouse.config.model import MatchClause, ModuleConfig
from lakehouse.net.http import HttpClient
from lakehouse.sources.cache import MemoryCache
from lakehouse.sources.github import GitHubClient
from lakehouse.sources.idsse_public import IdssePublicAdapter
from lakehouse.sources.skillcorner_public import SkillCornerPublicAdapter

BENCH_ARTIFACTS = {"idsse": ("metadata", "positions"), "skillcorner": ("match", "tracking")}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--bench-record", action="store_true", help="measure and write tests/bench/budget.json")


@pytest.fixture(scope="session")
def bench_record(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--bench-record"))


@pytest.fixture(scope="session")
def bench_cache() -> Path:
    folder = os.environ.get("LAKEHOUSE_BENCH_CACHE")
    if not folder:
        pytest.fail("set LAKEHOUSE_BENCH_CACHE to a folder for the bench raw files (Mac Mini, Task 30)")
    path = Path(folder)
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope="session")
def bench_files(bench_cache: Path) -> dict[str, dict[str, str]]:
    """Local paths of the raw files of J03WPY and 1899585, fetched once with the v2 adapters."""
    http = HttpClient()
    adapters = {
        "idsse": IdssePublicAdapter(
            http,
            ModuleConfig(module_id="idsse_public", active=True, filter=[MatchClause(match_ids=["J03WPY"])]),
            MemoryCache(),
        ),
        "skillcorner": SkillCornerPublicAdapter(
            GitHubClient(http, os.environ.get("GITHUB_TOKEN")),
            ModuleConfig(module_id="skillcorner_public", active=True, filter=[MatchClause(match_ids=["1899585"])]),
            MemoryCache(),
        ),
    }
    files: dict[str, dict[str, str]] = {}
    for provider, adapter in adapters.items():
        entry = next(iter(adapter.list_entries()))
        files[provider] = {}
        for artifact in BENCH_ARTIFACTS[provider]:
            ref = entry.artifacts[artifact]
            target = bench_cache / provider / f"{artifact}-{ref.change_signal}.raw"
            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(adapter.fetch(entry, artifact).raw)
            files[provider][artifact] = str(target)
    return files
