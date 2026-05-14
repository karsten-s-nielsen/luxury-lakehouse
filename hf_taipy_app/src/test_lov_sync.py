"""Drift guard: _MATCH_REQUIRED_PAGE_NAMES in shared.py must equal _MATCH_REQUIRED_PAGES in template.py."""

from __future__ import annotations

from state.shared import _MATCH_REQUIRED_PAGE_NAMES
from template import _MATCH_REQUIRED_PAGES


def test_match_required_pages_in_sync() -> None:
    """The frozenset in shared.py must match the tuple in template.py."""
    assert set(_MATCH_REQUIRED_PAGES) == _MATCH_REQUIRED_PAGE_NAMES
