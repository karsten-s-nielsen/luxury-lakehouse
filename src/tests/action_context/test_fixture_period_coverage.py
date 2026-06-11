"""Fixture parity rule (ADR-040 amendment): every tracking provider needs a period>=2 fixture.

Time-base bugs are structurally INVISIBLE in period 1 — absolute clock == period-relative
clock at offset 0 — so a provider whose only fixtures are period-1 can ship a period-2
data-loss bug with every test green. That is exactly how the SkillCorner dispatch
time-base bug (2026-06-11, ~90% of P2 actions silently dropped) survived: no SkillCorner
fixture existed at all, and the metrica fixtures were period-1. GS was the counterexample
that proved the rule: its period-3 fixture (10517_p3) is what validated the GS fix.

This test enforces: every tracking provider has at least one committed fixture unit
for a period >= 2. Adding a fifth tracking provider without one fails here.
"""

from __future__ import annotations

import re
from pathlib import Path

_FIXTURE_ROOT = Path(__file__).parent.parent / "fixtures" / "action_context"

# Tracking providers (event-only providers have no frame clock to misalign).
_TRACKING_PROVIDERS = ("gradientsports", "idsse", "metrica", "skillcorner")

_UNIT_DIR_RE = re.compile(r"_p(\d+)$")


def test_every_tracking_provider_has_a_period_ge_2_fixture() -> None:
    missing: list[str] = []
    for provider in _TRACKING_PROVIDERS:
        provider_dir = _FIXTURE_ROOT / provider
        periods: list[int] = []
        if provider_dir.is_dir():
            for unit_dir in provider_dir.iterdir():
                m = _UNIT_DIR_RE.search(unit_dir.name)
                if unit_dir.is_dir() and m:
                    periods.append(int(m.group(1)))
        if not any(p >= 2 for p in periods):
            missing.append(f"{provider} (periods present: {sorted(periods)})")
    assert not missing, (
        "Tracking providers without a period>=2 fixture (time-base bugs are invisible in "
        f"period 1 — see module docstring): {missing}. Extract one via "
        "scripts/extract_action_context_fixture.py --provider <p> --match-id <m> --period 2 --num-batches 2"
    )


def test_period_ge_2_fixtures_carry_frames_and_actions() -> None:
    """A period>=2 fixture only protects if it actually carries both streams."""
    for provider in _TRACKING_PROVIDERS:
        provider_dir = _FIXTURE_ROOT / provider
        if not provider_dir.is_dir():
            continue
        for unit_dir in provider_dir.iterdir():
            m = _UNIT_DIR_RE.search(unit_dir.name)
            if not (unit_dir.is_dir() and m and int(m.group(1)) >= 2):
                continue
            for required in ("frames.parquet", "actions.parquet", "meta.parquet"):
                assert (unit_dir / required).is_file(), f"{unit_dir.name}: period>=2 fixture missing {required}"
