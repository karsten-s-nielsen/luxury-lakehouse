"""Shared fixtures: `kit`, a fresh local lake with both roots (see tests/support/lakekit.py). OS-marked tests
(`windows`, `macos`, `linux`) run only on their own OS and only with `LAKEHOUSE_OS_TESTS=1`, which CI sets: they
install real OS tasks, so a local run never does that unasked (plan Task 24)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Final

import pytest
from lakekit import LakeKit, lake_roots

from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake


@pytest.fixture
def kit(tmp_path: Path) -> LakeKit:
    roots = lake_roots(tmp_path)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "boot.json")
    return LakeKit(base=tmp_path, roots=roots, storage=LocalStorage(roots))


_OS_MARKERS: Final = {"windows": "win32", "macos": "darwin", "linux": "linux"}


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    opted = os.environ.get("LAKEHOUSE_OS_TESTS") == "1"
    for item in items:
        for marker, platform in _OS_MARKERS.items():
            if marker not in item.keywords:
                continue
            if not sys.platform.startswith(platform):
                item.add_marker(pytest.mark.skip(reason=f"needs a {marker} host"))
            elif not opted:
                item.add_marker(pytest.mark.skip(reason="installs a real OS task: set LAKEHOUSE_OS_TESTS=1 (CI does)"))
