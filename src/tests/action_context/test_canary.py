"""Unit tests for the in-preflight drain canary (ADR-087)."""

from __future__ import annotations

import logging

import pytest

from analytics.action_context.canary import run_canary, select_canary_units
from analytics.action_context.work_unit import WorkUnit


def _u(provider: str, mid: str, period: int | None = None) -> WorkUnit:
    return WorkUnit(provider=provider, match_id=mid, period=period)


class _OkProcessor:
    def __init__(self) -> None:
        self.seen: list[tuple[str, str, bool]] = []

    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        self.seen.append((unit.provider, unit.match_id, dry_run))
        return 0


class _FailProcessor:
    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        raise ValueError(f"boom {unit.match_id}")


def test_select_canary_units_single_first_unit() -> None:
    """A single representative unit (the first discovered) — a full-processor dry-run per provider would
    blow preflight's 600 s budget (ADR-087 amendment); one unit catches a global systemic defect."""
    units = [_u("skillcorner", "1", 1), _u("skillcorner", "2", 1), _u("gradientsports", "3", 1)]
    got = select_canary_units(units)
    assert len(got) == 1
    assert (got[0].provider, got[0].match_id) == ("skillcorner", "1")  # first discovered
    assert select_canary_units([]) == []


def test_run_canary_passes_and_uses_dry_run() -> None:
    proc = _OkProcessor()
    run_canary(proc, [_u("idsse", "J", 1), _u("idsse", "K", 1)], logging.getLogger("t"))
    assert proc.seen == [("idsse", "J", True)]  # one per provider, dry_run=True (no write)


def test_run_canary_raises_on_processor_failure() -> None:
    with pytest.raises(RuntimeError, match="canary FAILED on idsse:J:1"):
        run_canary(_FailProcessor(), [_u("idsse", "J", 1)], logging.getLogger("t"))
