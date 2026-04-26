"""Parity test: workflow card prose must not contradict code constants.

Bug 1 from the D66 ExT v2 spike (2026-04-25): `wf-xt-grids.yaml` claimed
"16x12 grid" while the code default in `ExpectedThreatParams` was 12x8 —
a single-source-of-truth violation. The fix scrubbed the YAML prose to
defer to the code constant, and this test enforces that any future
"NxM grid" claim in the workflow card matches the code default.

Pattern is general — extend to other workflow cards if similar
prose-vs-code drift surfaces.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from analytics.expected_threat import ExpectedThreatParams

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CARD_PATH = _REPO_ROOT / "workflow-cards" / "wf-xt-grids.yaml"

_GRID_CLAIM_PATTERN = re.compile(r"(\d+)x(\d+)\s+(?:grid|cells)", re.IGNORECASE)
"""Matches '12x8 grid', '24x16 cells', etc. Used to find resolution claims
in the workflow card prose that we need to keep in sync with
ExpectedThreatParams defaults. To opt out for a specific phrase (rare —
use sparingly), substitute the U+00D7 multiplication-sign character for
the ASCII 'x' in that phrase; the regex matches only ASCII 'x'."""


def _load_card_text() -> str:
    if not _CARD_PATH.exists():
        pytest.fail(f"Workflow card not found: {_CARD_PATH}")
    return _CARD_PATH.read_text(encoding="utf-8")


class TestWorkflowCardXTGridSSOT:
    """SSOT parity tests for `workflow-cards/wf-xt-grids.yaml`."""

    def test_yaml_frontmatter_parses(self) -> None:
        """Sanity: the workflow card YAML frontmatter parses correctly."""
        card_text = _load_card_text()
        parts = card_text.split("---", 2)
        assert len(parts) >= 2, "Workflow card must have YAML frontmatter delimited by '---'"
        frontmatter = yaml.safe_load(parts[1])
        assert frontmatter["id"] == "wf-xt-grids"
        assert frontmatter["status"] == "production"

    def test_grid_resolution_claims_match_code_defaults(self) -> None:
        """Any 'NxM grid' or 'NxM cells' phrasing must match ExpectedThreatParams.

        Guards against the 2026-04-25 drift (YAML said '16x12 grid', code
        default was 12x8). If new resolution-claim phrases are introduced
        that should NOT be parity-checked (e.g., describing the historical
        Singh seed at '12x8' while the current default has moved on),
        substitute the U+00D7 multiplication-sign character for ASCII 'x'
        in that phrase to opt out (regex matches only ASCII 'x').
        """
        card_text = _load_card_text()
        matches = _GRID_CLAIM_PATTERN.findall(card_text)

        defaults = ExpectedThreatParams()
        expected_x = str(defaults.n_zones_x)
        expected_y = str(defaults.n_zones_y)

        mismatches = [(x, y) for (x, y) in matches if not (x == expected_x and y == expected_y)]

        if mismatches:
            pytest.fail(
                f"Workflow card {_CARD_PATH.name} contains resolution claims "
                f"{mismatches!r} that do not match ExpectedThreatParams defaults "
                f"({expected_x}x{expected_y}). Either correct the prose to match the "
                f"code constant, or replace ASCII 'x' with the U+00D7 multiplication "
                f"sign to opt out of the parity check (use sparingly)."
            )
