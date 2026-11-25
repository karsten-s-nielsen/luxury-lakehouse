"""The pinned pytest-benchmark works with the pinned pytest (perf plan Task 9 Step 7, 2026-11-23).

The bench tests (`tests/bench`, Mac Mini only) use the `benchmark` fixture. pytest-benchmark 5.3.0 builds that fixture
from `node._nodeid`, which pytest 9.2 removed, so every bench test errored at setup with
`AttributeError: 'Function' object has no attribute '_nodeid'` (found on the Mac). The bench tier runs only on the Mac,
so this guard runs the fixture in the normal tier: a pin pair that breaks it fails here, on any machine.
"""

from __future__ import annotations

from typing import Any


def test_benchmark_fixture_works_with_the_pinned_pytest(benchmark: Any) -> None:
    result = benchmark.pedantic(lambda: 1 + 1, rounds=1, iterations=1)
    assert result == 2
