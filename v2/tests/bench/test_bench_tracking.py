"""Parse and write one full tracking match per provider (spec §10.9, §23.4, §26 item 13; plan Task 17).

Markers `bench` and `heavy`: Mac Mini only (Task 30). It records MB per match, the parse time and the peak RSS of each
parse (sampled in a child process), and fails above `budget.json`. `--bench-record` writes the budget instead: the
measured values plus 25 % headroom (Task 30).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from benchkit import BENCH_MATCHES, BenchProvider, measure

pytestmark = [pytest.mark.bench, pytest.mark.heavy]

BUDGET = Path(__file__).with_name("budget.json")
HEADROOM = 1.25
CHECKED = ("parse_s", "peak_rss_mb", "mb_per_match")


@pytest.mark.parametrize("provider", ["idsse", "skillcorner"])
def test_bench_tracking(
    provider: BenchProvider,
    bench_files: dict[str, dict[str, str]],
    bench_record: bool,
    tmp_path: Path,
    benchmark: Any,
) -> None:
    measured = measure(provider, BENCH_MATCHES[provider], bench_files[provider], tmp_path / "out")
    benchmark.extra_info.update(measured)
    benchmark.pedantic(lambda: None, rounds=1, iterations=1)  # the timed work ran in the child; its numbers above
    if bench_record:
        budget: dict[str, Any] = json.loads(BUDGET.read_text(encoding="utf-8")) if BUDGET.exists() else {}
        budget[provider] = {name: round(measured[name] * HEADROOM, 3) for name in CHECKED} | {"measured": measured}
        BUDGET.write_text(json.dumps(budget, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return
    if not BUDGET.exists():
        pytest.fail("no tests/bench/budget.json yet: run the bench with --bench-record on the Mac Mini (Task 30)")
    limits = json.loads(BUDGET.read_text(encoding="utf-8"))[provider]
    over = {name: (measured[name], limits[name]) for name in CHECKED if measured[name] > limits[name]}
    assert not over, f"{provider} is over its budget (measured, limit): {over}"
