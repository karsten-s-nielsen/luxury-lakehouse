"""VAEP Adjusted page — config only, layout from page_template.

Standard layout: a per-player leaderboard showing RAW vs result-ADJUSTED VAEP side by side (with the
delta). The adjusted values come from silly-kicks VAEP.rate_adjusted, weighted by pass-success (xSuccess).
"""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    RequiredFilter,
    ScopeDim,
    build_page,
)

_NOTE = (
    "VAEP is in goals-added units (net change in scoring/conceding probability); HIGHER = more valuable. "
    "Raw is the standard VAEP; Adjusted applies silly-kicks' result-flip weighted by xSuccess (pass-success "
    "probability, 0-1) — so a good decision that failed by chance is valued nearer its intent. Δ VAEP = "
    "Adjusted - Raw. Season totals across all actions in scope (not per-90); ranked by adjusted VAEP. "
    "StatsBomb / Wyscout only (player surrogates resolve there in the current dual-column window)."
)

page_config = PageConfig(
    title="VAEP Adjusted",
    icon="tune",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Player action value with a result-adjustment — raw VAEP alongside VAEP.rate_adjusted, which "
        "re-weights each action by its pass-success probability (xSuccess) so unlucky good decisions are "
        "not undervalued."
    ),
    citations=[
        Citation("Decroos et al. (2019) — VAEP", "https://doi.org/10.1145/3292500.3330758"),
        Citation("Paul, Klemp & Memmert (2025) — result-adjusted action value"),
        Citation("Anzer & Bauer (2022) — Expected Passes"),
        Citation("silly-kicks — VAEP.rate_adjusted / XSuccessModel"),
    ],
    scope_dims=[
        ScopeDim("Competition", "va_scope_comp"),
        ScopeDim("Team", "va_scope_team"),
    ],
    required_filters=[RequiredFilter("selected_competition", "competition")],
    freshness_var="va_data_freshness",
    warning_var="va_warning_text",
    content=[
        ContentRow(
            [
                ContentBlock(
                    "table",
                    "va_leaderboard",
                    header="Raw vs adjusted VAEP — ranked by adjusted value",
                    table_page_size=25,
                    caption=_NOTE,
                )
            ]
        ),
    ],
)
page_md = build_page(page_config)
