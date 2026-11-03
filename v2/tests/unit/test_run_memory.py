"""The run's memory log tells the truth on macOS (audit T11, owner decision 2026-11-01).

`ru_maxrss` is the peak resident size. On macOS that also counts freed pages the kernel has not reclaimed yet: the T3
Task 14 run reported 15.7 GB where the true peak (phys_footprint) was 8.2 GB. The `run memory` line keeps
`peak_rss_mb` (its readers, §26 item 9) and adds `peak_footprint_mb`, the lifetime peak phys_footprint, on macOS.
"""

from __future__ import annotations

import logging
import sys

import pytest
from lakekit import LakeKit
from runkit import NOW, save_settings

from lakehouse.runs.memory import peak_footprint_mb, peak_rss_mb
from lakehouse.runs.run import run_ingestion


def test_peak_footprint_is_reported_on_macos_only() -> None:
    value = peak_footprint_mb()
    if sys.platform == "darwin":
        assert value is not None and value > 0
        rss = peak_rss_mb()
        assert rss is not None and value <= rss * 4  # the same order as the resident peak, not another unit
    else:
        assert value is None


def test_the_run_memory_line_names_both_measures(kit: LakeKit, caplog: pytest.LogCaptureFixture) -> None:
    save_settings(kit.storage, [])
    with caplog.at_level(logging.INFO):
        run_ingestion(
            storage=kit.storage,
            trigger="cli",
            adapters={},
            parsers={},
            now=lambda: NOW,
            work_dir=kit.base / "w",
            dbt=None,
        )
    [line] = [r for r in caplog.records if r.getMessage() == "run memory"]
    assert hasattr(line, "peak_rss_mb") and hasattr(line, "peak_footprint_mb")
    footprint = getattr(line, "peak_footprint_mb")  # noqa: B009 - a log record's dynamic attribute
    if sys.platform == "darwin":
        assert isinstance(footprint, float) and footprint > 0
    else:
        assert footprint is None
