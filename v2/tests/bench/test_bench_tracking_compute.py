"""line_breaking and the skip check on the largest real match per provider (perf plan Task 9; spec §10). Mac Mini only.

Markers `bench` and `heavy`. Each case runs `scripts/profile_tracking_compute.py --step line_breaking` as a child
process against the PUBLISHED catalog (read-only; the lake roots resolve as for any `lakehouse` command) and samples
the child's resident memory with psutil. It records, per provider, the line_breaking milliseconds per frame, the
child's peak RSS and the skip-check seconds (the exact input key, `skip_key` phase), and fails above the
`tracking_compute` section of `budget.json`. `--bench-record` writes that section instead: the measured values plus
25 % headroom, as `test_bench_tracking.py` does.

Amendment 2026-11-23 (calc-layer spec D10): pitch_control and shape_graphs are excluded from the default run, so they
are not measured here; the silly-kicks upgrade cycle owns their benchmarks. The idsse numbers cover the incomplete
idsse action set (D10 item 4).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Final

import psutil
import pytest

pytestmark = [pytest.mark.bench, pytest.mark.heavy]

V2_ROOT: Final = Path(__file__).resolve().parents[2]
SCRIPT: Final = V2_ROOT / "scripts" / "profile_tracking_compute.py"
BUDGET: Final = Path(__file__).with_name("budget.json")
SECTION: Final = "tracking_compute"
HEADROOM: Final = 1.25
CHECKED: Final = ("line_breaking_ms_per_frame", "peak_rss_mb", "skip_check_s")
_SAMPLE_S: Final = 0.2


def _profile_line_breaking(provider: str, out_dir: Path) -> dict[str, Any]:
    """Run the profiler for the provider's largest match in a child process; add the child's peak RSS in MB."""
    child = subprocess.Popen(  # noqa: S603 - a fixed argv: this interpreter, a repo script, fixed options
        [sys.executable, str(SCRIPT), "--provider", provider, "--step", "line_breaking", "--out", str(out_dir)],
        cwd=V2_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    watched = psutil.Process(child.pid)
    peak = 0
    while child.poll() is None:
        try:
            peak = max(peak, watched.memory_info().rss)
        except psutil.NoSuchProcess:
            break
        time.sleep(_SAMPLE_S)
    _stdout, stderr = child.communicate()
    if child.returncode != 0:
        raise RuntimeError(f"the {provider} profile child failed ({child.returncode}): {stderr[-2000:]}")
    (result_file,) = out_dir.glob(f"line_breaking-{provider}-*.json")
    profile: dict[str, Any] = json.loads(result_file.read_text(encoding="utf-8"))
    return {
        "match_key": profile["match_key"],
        "frames": profile["frames"],
        "total_s": profile["total_s"],
        "line_breaking_ms_per_frame": profile["total_s"] * 1000 / max(profile["frames"], 1),
        "skip_check_s": profile["phases"]["skip_key"],
        "peak_rss_mb": peak / 1e6,
    }


@pytest.mark.parametrize("provider", ["gradientsports", "idsse", "skillcorner"])
def test_bench_tracking_compute_budget(provider: str, bench_record: bool, tmp_path: Path, benchmark: Any) -> None:
    """The time column of the pytest-benchmark table is a placeholder (a no-op call, nanoseconds): the real numbers are
    measured in a child process and are in `extra_info` and `budget.json`."""
    measured = _profile_line_breaking(provider, tmp_path)
    benchmark.extra_info.update(measured)
    benchmark.pedantic(lambda: None, rounds=1, iterations=1)  # the timed work ran in the child; its numbers above
    budget: dict[str, Any] = json.loads(BUDGET.read_text(encoding="utf-8")) if BUDGET.exists() else {}
    if bench_record:
        section = budget.setdefault(SECTION, {})
        section[provider] = {name: round(measured[name] * HEADROOM, 3) for name in CHECKED} | {"measured": measured}
        BUDGET.write_text(json.dumps(budget, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return
    if provider not in budget.get(SECTION, {}):
        pytest.fail(f"no {SECTION}.{provider} budget yet: run the bench with --bench-record on the Mac Mini (Task 9)")
    limits = budget[SECTION][provider]
    over = {name: (measured[name], limits[name]) for name in CHECKED if measured[name] > limits[name]}
    assert not over, f"{provider} line_breaking is over its budget (measured, limit): {over}"
