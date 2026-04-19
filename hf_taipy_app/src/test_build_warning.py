"""Unit tests for build_warning helper in filters.py."""

from __future__ import annotations

import pytest
from filters import build_warning


def test_build_warning_no_suggestions_fallback() -> None:
    """Empty suggestions list uses the canonical fallback sentence."""
    assert build_warning("actions", []) == "No actions found for this selection. Try adjusting your filters."


def test_build_warning_single_suggestion() -> None:
    """One suggestion renders without 'or'."""
    assert (
        build_warning("actions", ["choosing a different player"])
        == "No actions found for this selection. Try choosing a different player."
    )


def test_build_warning_two_suggestions_uses_or() -> None:
    """Two suggestions are joined with 'or'."""
    result = build_warning("actions", ["removing the team filter", "choosing a different player"])
    assert result == (
        "No actions found for this selection. Try removing the team filter or choosing a different player."
    )


def test_build_warning_three_suggestions_comma_and_or() -> None:
    """Three suggestions: comma between first two, 'or' before last."""
    result = build_warning("passes", ["broadening the match", "another team", "different player"])
    assert result == ("No passes found for this selection. Try broadening the match, another team or different player.")


def test_build_warning_empty_domain_raises() -> None:
    """Empty or whitespace domain is a caller error."""
    with pytest.raises(ValueError, match="domain must be non-empty"):
        build_warning("", ["x"])
    with pytest.raises(ValueError, match="domain must be non-empty"):
        build_warning("   ", ["x"])
