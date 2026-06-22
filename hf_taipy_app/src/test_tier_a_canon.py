"""Tier A page canon contract test.

Asserts the canonical patterns on the set of Tier A pages that have been
migrated (MIGRATED_TIER_A). Add a page to the set once its migration PR
is merged. Prevents the 'partial pattern application' consistency violation
from reopening.
"""

from __future__ import annotations

from pages.action_values import page_config as player_impact_config
from pages.conversion_funnel import page_config as conversion_funnel_config
from pages.gk_analytics import page_config as goalkeeper_config  # route "Goalkeeper-Analytics", new page
from pages.heat_map import page_config as heat_map_config
from pages.match_summary import page_config as match_summary_config
from pages.pass_map import page_config as pass_map_config
from pages.pass_network import page_config as pass_network_config
from pages.player_radar import page_config as player_comparison_config
from pages.shot_map import page_config as shot_map_config
from template import GLOSSARY, PAGE_TERMS

MIGRATED_TIER_A: dict[str, object] = {
    "Heat-Map": heat_map_config,
    "Match-Summary": match_summary_config,
    "Shot-Map": shot_map_config,
    "Pass-Map": pass_map_config,
    "Pass-Network": pass_network_config,
    "Player-Impact": player_impact_config,
    "Player-Comparison": player_comparison_config,
    "Goalkeeper-Analytics": goalkeeper_config,
    "Conversion-Funnel": conversion_funnel_config,
}


def test_migrated_tier_a_page_has_non_empty_glossary() -> None:
    """Every migrated Tier A page has at least one domain term in PAGE_TERMS."""
    for page_key in MIGRATED_TIER_A:
        terms = PAGE_TERMS.get(page_key, [])
        assert terms, f"PAGE_TERMS[{page_key!r}] is empty; Tier A migration requires at least one term"
        for t in terms:
            assert t in GLOSSARY, f"PAGE_TERMS[{page_key!r}] references undefined GLOSSARY key {t!r}"


def test_migrated_tier_a_page_has_scope_dims() -> None:
    """Migrated pages must declare the canonical scope_dims list."""
    for page_key, cfg in MIGRATED_TIER_A.items():
        scope_dims = getattr(cfg, "scope_dims", [])
        assert scope_dims, f"{page_key} has no scope_dims — canonical scope line not wired"
        for dim in scope_dims:
            assert dim.label, f"{page_key}: scope_dim has empty label"
            assert dim.value_var, f"{page_key}: scope_dim {dim.label!r} has empty value_var"


def test_migrated_tier_a_page_content_blocks_have_alt_var() -> None:
    """Every image ContentBlock on a migrated page must have alt_var set."""
    for page_key, cfg in MIGRATED_TIER_A.items():
        for row in getattr(cfg, "content", []):
            for block in row.blocks:
                if block.kind == "image":
                    assert block.alt_var, f"{page_key}: image ContentBlock with var={block.var!r} has empty alt_var"


def test_migrated_tier_a_page_has_warning_var() -> None:
    """Sanity: the page has warning_var wired (its refresh uses build_warning).

    Standard + dashboard pages carry warning_var on PageConfig.
    Sub-view pages (Player-Impact, Goalkeeper-Analytics) carry it on each SubView.
    """
    for page_key, cfg in MIGRATED_TIER_A.items():
        page_warning = getattr(cfg, "warning_var", "")
        sub_views = getattr(cfg, "sub_views", [])
        sub_view_warnings = [getattr(sv, "warning_var", "") for sv in sub_views]
        has_warning = bool(page_warning) or all(sub_view_warnings) if sub_views else bool(page_warning)
        assert has_warning, f"{page_key} has no warning_var (page or per-sub-view)"
