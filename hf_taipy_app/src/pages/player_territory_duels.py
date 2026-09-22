"""Player Territory & Duels page — config only, layout from page_template.

Two ranked-leaderboard views (full width): Territory (net xT defended; v1 vs counterfactual side by side)
and Duels (latest carried-forward Glicko-2 rating). Both families are ranking-licensed.
"""

from __future__ import annotations

from page_template import (
    NAV_PLAYER_ANALYSIS,
    Citation,
    ContentBlock,
    ContentRow,
    PageConfig,
    ScopeDim,
    SubView,
    build_page,
)

_TERRITORY_NOTE = (
    "xT (Expected Threat) is in goal-probability units; all territory columns are HIGHER = better. "
    "xT Prevented (v1) counts threat prevented on completed/failed passes; xT Prevented Above Exp. (cf) "
    "is the counterfactual — threat prevented beyond what an average defender would in the same situations. "
    "The two methods are shown side by side for comparison. Ranked by net xT defended."
)
_DUELS_NOTE = (
    "Glicko-2 rating is a chess-style skill rating (≈1500 baseline; HIGHER = better) that carries forward "
    "across matches — the latest carried-forward value is shown, not a per-match average. Rating ± is the "
    "rating deviation (uncertainty; LOWER = more reliable). Ranked by rating."
)

page_config = PageConfig(
    title="Player Territory & Duels",
    icon="shield_person",
    nav_section=NAV_PLAYER_ANALYSIS,
    description=(
        "Two per-player defensive evaluations at the player-match grain — territorial dominance "
        "(expected threat conceded/prevented in a defender's zone) and ground-duel skill (Glicko-2 rating)."
    ),
    freshness_var="ptd_data_freshness",
    citations=[
        Citation("silly-kicks — territorial dominance (TF-54/TF-54b) & duel ratings (TF-55)"),
        Citation("Glickman (2012) — Example of the Glicko-2 system", "http://www.glicko.net/glicko/glicko2.pdf"),
        Citation("Expected Threat (xT) — action-value grid"),
    ],
    scope_dims=[
        ScopeDim("Competition", "ptd_scope_comp"),
        ScopeDim("Team", "ptd_scope_team"),
    ],
    sub_views=[
        SubView(
            condition='selected_sub_view == "Territory"',
            scale_notes=[_TERRITORY_NOTE],
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "table",
                            "ptd_territory_table",
                            header="Territorial dominance — ranked by net xT defended",
                            table_page_size=25,
                        )
                    ]
                ),
            ],
            warning_var="ptd_warning_text",
            empty_message="Select a competition to begin — no territory rows in scope yet.",
            empty_condition="len(ptd_territory_table) == 0",
        ),
        SubView(
            condition='selected_sub_view == "Duels"',
            scale_notes=[_DUELS_NOTE],
            content=[
                ContentRow(
                    [
                        ContentBlock(
                            "table",
                            "ptd_duels_table",
                            header="Ground-duel skill — ranked by Glicko-2 rating",
                            table_page_size=25,
                        )
                    ]
                ),
            ],
            warning_var="ptd_warning_text",
            empty_message="Select a competition to begin — no duel ratings in scope yet.",
            empty_condition="len(ptd_duels_table) == 0",
        ),
    ],
)
page_md = build_page(page_config)
