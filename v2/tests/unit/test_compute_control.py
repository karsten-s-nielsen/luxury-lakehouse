"""The compute deadline and stop control (perf spec §7.5; plan Task 4)."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from lakehouse.compute.control import UNBOUNDED, ComputeControl
from lakehouse.config.model import ComputeConfig

V2_ROOT = Path(__file__).resolve().parents[2]
_TIMEOUT = re.compile(r"--timeout (\d+)")
START = datetime(2026, 11, 6, 3, 0, tzinfo=UTC)


def test_compute_budget_fits_the_deployed_timeout() -> None:
    """Hyrum's Law guard: the compute budget plus the startup allowance stays inside the scheduled job's timeout,
    as documented in deploy.md and drawn in architecture.dsl (both must agree)."""
    deploy = (V2_ROOT / "docs" / "ops" / "deploy.md").read_text(encoding="utf-8")
    line = next(ln for ln in deploy.splitlines() if ln.startswith("Scheduled job:"))
    match = _TIMEOUT.search(line)
    assert match is not None
    dsl = _TIMEOUT.search((V2_ROOT / "architecture.dsl").read_text(encoding="utf-8"))
    assert dsl is not None and dsl.group(1) == match.group(1)
    config = ComputeConfig()
    assert config.budget_seconds + config.startup_allowance_seconds <= int(match.group(1))


def _control(layer: int, at: datetime, *, stopped: bool = False) -> ComputeControl:
    return ComputeControl.for_layer(
        ComputeConfig(), started_at=START, layer=layer, now=lambda: at, stop=lambda: stopped, on_written=lambda: None
    )


def test_for_layer_deadlines() -> None:
    config = ComputeConfig()
    one = _control(1, START)
    two = _control(2, START)
    hard_two = START + timedelta(seconds=config.budget_seconds - config.finish_reserve_seconds)
    hard_one = hard_two - timedelta(seconds=config.layer2_reserve_seconds)
    assert one.hard_deadline == hard_one
    assert one.soft_deadline == hard_one - timedelta(seconds=config.worst_match_seconds)
    assert two.hard_deadline == hard_two
    assert two.soft_deadline == hard_two - timedelta(seconds=config.worst_match_seconds)
    assert one.check_every == config.check_every_frames


def test_may_start_match_and_should_stop() -> None:
    soft = _control(1, START).soft_deadline
    hard = _control(1, START).hard_deadline
    assert soft is not None and hard is not None
    early = _control(1, soft - timedelta(seconds=1))
    assert early.may_start_match() and not early.should_stop()
    between = _control(1, soft + timedelta(seconds=1))
    assert not between.may_start_match() and not between.should_stop()
    late = _control(1, hard + timedelta(seconds=1))
    assert not late.may_start_match() and late.should_stop()
    halted = _control(1, START, stopped=True)
    assert not halted.may_start_match() and halted.should_stop()


def test_unbounded_never_stops() -> None:
    assert UNBOUNDED.may_start_match() and not UNBOUNDED.should_stop()
    assert UNBOUNDED.soft_deadline is None and UNBOUNDED.hard_deadline is None
