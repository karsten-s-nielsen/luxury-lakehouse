"""Unit tests for build_scope_label_plain helper in filters.py.

HTML scope rendering is handled by page_template + PageConfig.scope_dims,
not by a build_scope_label helper — Taipy's `|text|raw|` escapes HTML,
so we emit per-dimension state vars via static Taipy markdown. The plain
variant is the only helper in filters.py; it serves image alt text and
screen-reader contexts.
"""

from __future__ import annotations

from filters import build_scope_label_plain


def test_build_scope_label_plain_empty() -> None:
    """Empty pairs list returns empty string."""
    assert build_scope_label_plain([]) == ""


def test_build_scope_label_plain_single_pair() -> None:
    """Single pair: 'label: value'."""
    assert build_scope_label_plain([("Competition", "England — PL")]) == "Competition: England — PL"


def test_build_scope_label_plain_multiple_pairs_joined_with_middle_dot() -> None:
    """Pairs joined with U+00B7 middle dot and surrounding spaces."""
    result = build_scope_label_plain(
        [
            ("Competition", "England — PL"),
            ("Team", "All teams"),
            ("Player", "Jens Lehmann"),
        ]
    )
    assert result == "Competition: England — PL \u00b7 Team: All teams \u00b7 Player: Jens Lehmann"


def test_build_scope_label_plain_has_no_html_tags() -> None:
    """Plain text only — safe for alt attributes."""
    result = build_scope_label_plain([("Competition", "England"), ("Team", "Arsenal")])
    assert "<" not in result
    assert ">" not in result
