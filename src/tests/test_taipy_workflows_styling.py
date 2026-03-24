"""Tests for Taipy workflows page style callbacks and stat detail HTML helpers.

These are pure functions with no Taipy dependency — they return CSS class
names or RawHtml strings based on cell values.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add hf_taipy_app/src to path so we can import the state module
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from state.workflows import (
    RawHtml,
    wf_style_freshness,
    wf_style_runtime,
    wf_style_type,
)

# ---------------------------------------------------------------------------
# wf_style_type
# ---------------------------------------------------------------------------


class TestStyleType:
    """Cell class callback for the Type column."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("Train+Infer", "ll-cell-type-train"),
            ("Training", "ll-cell-type-train"),
            ("Inference", "ll-cell-type-train"),
            ("Grid Compute", "ll-cell-type-grid"),
            ("Heuristic", "ll-cell-type-heuristic"),
            ("Validation", "ll-cell-type-validation"),
            ("Augmentation", "ll-cell-type-augmentation"),
        ],
    )
    def test_known_types(self, value: str, expected: str) -> None:
        assert wf_style_type(None, value, 0, 0, "Type") == expected

    def test_unknown_type_returns_empty(self) -> None:
        assert wf_style_type(None, "UnknownType", 0, 0, "Type") == ""

    def test_em_dash_returns_empty(self) -> None:
        assert wf_style_type(None, "\u2014", 0, 0, "Type") == ""


# ---------------------------------------------------------------------------
# wf_style_runtime
# ---------------------------------------------------------------------------


class TestStyleRuntime:
    """Cell class callback for the Runtime column."""

    def test_db_only(self) -> None:
        assert wf_style_runtime(None, "DB", 0, 0, "Runtime") == "ll-cell-rt-db"

    def test_hf_only(self) -> None:
        assert wf_style_runtime(None, "HF", 0, 0, "Runtime") == "ll-cell-rt-hf"

    @pytest.mark.parametrize("value", ["HF + DB", "DB + HF"])
    def test_combined(self, value: str) -> None:
        assert wf_style_runtime(None, value, 0, 0, "Runtime") == "ll-cell-rt-both"

    def test_em_dash_returns_empty(self) -> None:
        assert wf_style_runtime(None, "\u2014", 0, 0, "Runtime") == ""


# ---------------------------------------------------------------------------
# wf_style_freshness
# ---------------------------------------------------------------------------


class TestStyleFreshness:
    """Cell class callback for the Freshness column."""

    def test_ok(self) -> None:
        assert wf_style_freshness(None, "OK", 0, 0, "Freshness") == "ll-cell-fresh-ok"

    def test_warning(self) -> None:
        assert wf_style_freshness(None, "Warning", 0, 0, "Freshness") == "ll-cell-fresh-warning"

    def test_stale(self) -> None:
        assert wf_style_freshness(None, "Stale", 0, 0, "Freshness") == "ll-cell-fresh-stale"

    def test_em_dash_returns_empty(self) -> None:
        assert wf_style_freshness(None, "\u2014", 0, 0, "Freshness") == ""


# ---------------------------------------------------------------------------
# _stat_detail_html
# ---------------------------------------------------------------------------


class TestStatDetailHtml:
    """RawHtml wrapper for stat card content-provider iframes."""

    def test_empty_input_returns_empty_rawhtml(self) -> None:
        from state.workflows import _stat_detail_html

        result = _stat_detail_html("")
        assert isinstance(result, RawHtml)
        assert len(result) == 0

    def test_nonempty_wraps_in_body(self) -> None:
        from state.workflows import _stat_detail_html

        result = _stat_detail_html('<span style="color:red">test</span>')
        assert isinstance(result, RawHtml)
        assert "<body" in str(result)
        assert "test</span>" in str(result)
        assert "background:transparent" in str(result)

    def test_html_escaping_not_applied_to_inner(self) -> None:
        """Inner HTML should preserve tags — it's intentionally raw."""
        from state.workflows import _stat_detail_html

        result = _stat_detail_html("<b>bold</b>")
        assert "<b>bold</b>" in str(result)
