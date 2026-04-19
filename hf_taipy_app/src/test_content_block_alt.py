"""Tests for ContentBlock.alt_var field."""

from __future__ import annotations

from page_template import ContentBlock, _build_content_block


def test_content_block_image_without_alt_var_falls_back_to_page_title() -> None:
    """When alt_var is empty, image label is the page_title (existing behaviour)."""
    block = ContentBlock("image", "hm_pass_bubbles")
    md = _build_content_block(block, page_title="Heat Map")
    assert "|image|label=Heat Map|" in md


def test_content_block_image_with_alt_var_binds_state() -> None:
    """When alt_var is set, image label binds to a state variable."""
    block = ContentBlock("image", "hm_pass_bubbles", alt_var="hm_pass_bubbles_alt")
    md = _build_content_block(block, page_title="Heat Map")
    assert "|image|label={hm_pass_bubbles_alt}|" in md


def test_content_block_alt_var_default_is_empty_string() -> None:
    """Unset alt_var defaults to empty string."""
    block = ContentBlock("image", "hm_pass_bubbles")
    assert block.alt_var == ""
