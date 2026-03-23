"""Domain glossary — provides tooltip definitions for soccer analytics terms.

Cognitive audit finding H20: 11+ domain-specific terms used without explanation.
This module provides a central glossary that can be rendered in the sidebar
and referenced by individual pages for st.metric help= parameters.
"""

from __future__ import annotations

import streamlit as st

# Canonical definitions — used for both sidebar glossary and st.metric help= text.
# Keep definitions concise (one sentence) and include a direction indicator where
# applicable (higher = better, lower = more aggressive, etc.).
GLOSSARY: dict[str, str] = {
    "xG (Expected Goals)": (
        "Probability of scoring from each shot's location and context. "
        "Higher = better chance. Sum over a match = team's expected output."
    ),
    "VAEP": (
        "Valuing Actions by Estimating Probabilities — how much each on-ball action "
        "changed the probability of scoring. Positive = helped, negative = hurt."
    ),
    "VAEP/90": "VAEP per 90 minutes played. Higher = more impactful player.",
    "PPDA": ("Passes Per Defensive Action — measures pressing intensity. Lower values = more aggressive pressing."),
    "xT (Expected Threat)": (
        "Probability that ball possession in a pitch zone leads to a goal within "
        "the next few actions. Data-driven Markov chain model."
    ),
    "Off-Ball xT": (
        "Cumulative expected threat gained from a player's off-ball movement — "
        "measures how well a player positions themselves to receive the ball in dangerous areas. "
        "Typical range: 0.001-0.01 per match."
    ),
    "DEFCON": (
        "Defensive Contribution framework (Kim et al. 2025) — quantifies how defenders "
        "affect an attacker's scoring probability via four credit categories."
    ),
    "Intercept": (
        "DEFCON credit for winning the ball. Higher = more successful interceptions. "
        "Typical match total per player: 0.1\u20131.5 credits."
    ),
    "Concede": (
        "DEFCON credit charged when a shot or goal occurs despite pressure. "
        "Lower is better. Typical match total per player: 0.0\u20130.5 credits."
    ),
    "Disturb": (
        "DEFCON credit for disrupting possession without winning the ball. "
        "Higher = more effective disruption. Typical match total: 0.1\u20131.0 credits."
    ),
    "Deter": (
        "DEFCON credit for preventing attacker progression through positioning. "
        "Higher = more effective deterrence. Typical match total: 0.1\u20132.0 credits."
    ),
    "Brier Score": (
        "Prediction calibration metric — lower is better. "
        "0.0 = perfect predictions, 0.25 = coin flip. Good models score < 0.10."
    ),
    "Cosine Distance": (
        "Similarity measure between player embedding vectors. "
        "0.0 = identical playing style, 1.0 = completely different."
    ),
    "Line-Breaking Pass": (
        "A pass that penetrates at least one defensive line, detected via "
        "Ward clustering on StatsBomb 360 freeze-frame defender positions."
    ),
    "Pitch Control": (
        "Physics-based model estimating which team controls each point on the pitch, "
        "based on player positions, velocities, and time-to-intercept."
    ),
    "SPADL": (
        "Soccer Player Action Description Language — unified event format converting "
        "vendor-specific event streams into 23 canonical action types (105x68m coordinates)."
    ),
    "Progressive Pass": (
        "A pass that moves the ball significantly closer to the opponent's goal — "
        "defined by a minimum distance threshold toward the goal line."
    ),
    "PAUSA": (
        "Passing Ability Under Spatiotemporal Awareness. Composite of temporal judgment "
        "\u00d7 spatial selection. Higher = better pass timing and target choice. "
        "(Lee et al., MIT Sloan 2026)"
    ),
    "Temporal Judgment": (
        "Was the pass released at the optimal moment? Ratio of actual OBSO at release "
        "to peak OBSO in the \u00b13s/+1s window. 1.0 = perfect timing."
    ),
    "Spatial Selection": (
        "Was the target location the best available? Ratio of actual OBSO at target "
        "to maximum OBSO across all receivers. 1.0 = optimal target."
    ),
    "OBSO": (
        "Off-Ball Scoring Opportunity. Continuous value surface: Pitch Control "
        "\u00d7 Ball Transition Probability \u00d7 Expected Possession Value. "
        "(Spearman 2018, Fernandez & Bornn 2018)"
    ),
}

# Per-page glossary terms — only these terms are shown when the user is on that page.
# Avoids showing DEFCON definitions on Shot Map or xG on Defensive Impact (Sweller extraneous load).
PAGE_TERMS: dict[str, list[str]] = {
    "shot-map": ["xG (Expected Goals)", "Brier Score"],
    "pass-map": ["Line-Breaking Pass", "Progressive Pass"],
    "heat-map": [],
    "pass-network": [],
    "match-summary": ["xG (Expected Goals)", "PPDA", "Progressive Pass"],
    "action-values": ["VAEP", "VAEP/90", "SPADL"],
    "player-radar": [
        "xG (Expected Goals)",
        "VAEP",
        "VAEP/90",
        "PPDA",
        "xT (Expected Threat)",
        "Off-Ball xT",
        "DEFCON",
        "Line-Breaking Pass",
        "Progressive Pass",
    ],
    "movement-analysis": ["PPDA", "xT (Expected Threat)", "Off-Ball xT", "Pitch Control"],
    "pitch-control": ["Pitch Control"],
    "defensive-valuation": ["DEFCON", "Intercept", "Concede", "Disturb", "Deter"],
    "player-similarity": ["Cosine Distance"],
    "pass-timing": ["PAUSA", "Temporal Judgment", "Spatial Selection", "OBSO"],
}

# Subset for st.metric help= parameters — keyed by the label as it appears in st.metric()
METRIC_HELP: dict[str, str] = {
    "Total xG": GLOSSARY["xG (Expected Goals)"],
    "xG": GLOSSARY["xG (Expected Goals)"],
    "Home xG": GLOSSARY["xG (Expected Goals)"],
    "Away xG": GLOSSARY["xG (Expected Goals)"],
    "xG / Shot": "Average expected goals per shot — higher indicates better shot quality.",
    "Brier Score": GLOSSARY["Brier Score"],
    "Conversion Rate": "Goals / Total Shots — percentage of shots that resulted in goals.",
    "Total VAEP": GLOSSARY["VAEP"],
    "VAEP/90": GLOSSARY["VAEP/90"],
    "Off. VAEP/90": "Offensive VAEP per 90 minutes — contribution to scoring probability.",
    "Def. VAEP/90": "Defensive VAEP per 90 minutes — contribution to preventing opponent scoring.",
    "Net Match VAEP": "Sum of all VAEP values in a match — positive = team created more than conceded.",
    "Avg Home PPDA": GLOSSARY["PPDA"],
    "Avg Away PPDA": GLOSSARY["PPDA"],
    "Avg Off-Ball xT": GLOSSARY["Off-Ball xT"],
    "Max Off-Ball xT": GLOSSARY["Off-Ball xT"],
    "Max Speed (km/h)": "Maximum player speed in km/h. Elite sprints reach ~35 km/h.",
    "Intercept": GLOSSARY["Intercept"],
    "Concede": GLOSSARY["Concede"],
    "Disturb": GLOSSARY["Disturb"],
    "Deter": GLOSSARY["Deter"],
    "Home Control": "Percentage of pitch area controlled by the home team at this moment.",
    "Away Control": "Percentage of pitch area controlled by the away team at this moment.",
    "Control at Ball": (
        "Pitch control at the ball's location. 0.0 = full away control, 1.0 = full home control. "
        "Values near 0.5 indicate contested space."
    ),
    "Avg Speed": "Average player speed in m/s. Typical match average: 1.5\u20132.5 m/s.",
    "Avg Speed (m/s)": "Average player speed in m/s. Typical match average: 1.5\u20132.5 m/s.",
    "Max Speed": "Maximum player speed in m/s. Elite sprints reach 9\u201310 m/s (~35 km/h).",
    "Max Speed (m/s)": "Maximum player speed in m/s. Elite sprints reach 9\u201310 m/s (~35 km/h).",
    "Avg Dist to Ball": "Average distance from players to the ball in meters.",
    "Cosine Distance": GLOSSARY["Cosine Distance"],
    "Completed Passes": "Number of passes that reached the intended recipient.",
    "Unique Connections": "Number of distinct passer-receiver pairs above the minimum threshold.",
    "Top Pair Count": "Number of passes between the most frequent passer-receiver pair.",
    "Most Active Zone": "The 3x3 pitch zone (e.g., 'Att Center') with the highest action count.",
    "Progressive": "Passes that move the ball significantly toward the opponent's goal.",
    "Line-Breaking": GLOSSARY["Line-Breaking Pass"],
    "Total Shots": "Total number of shots attempted.",
    "Goals": "Total number of goals scored.",
    "Positive Actions": "Actions with positive VAEP — contributed to scoring probability.",
    "Negative Actions": "Actions with negative VAEP — reduced scoring probability.",
    "Total Passes": "Number of passes attempted by the selected player or team.",
    "Completed": "Number of passes that reached the intended recipient.",
    "Completion %": "Percentage of attempted passes successfully completed.",
    "Total Actions": "Number of on-ball actions (passes, shots, dribbles, etc.) in the selected scope.",
    "Passes": "Number of pass actions in the selected scope.",
    "Shots": "Number of shot actions in the selected scope.",
    "Top Action Type": "The SPADL action type with the highest total VAEP contribution.",
    "Most Valuable Action": "The single action that contributed most to scoring probability in this match.",
    "Players": "Number of players visible in the current data scope.",
    "Avg Distance (km)": "Average total distance covered per player in kilometers.",
    "Matches": "Number of matches included in the current analysis.",
    "Score": "Match score - goals scored by each team.",
    # Radar spoke labels (F6/F12: Expert Blind Spot — no inline tooltip on chart axes)
    "Goals/90": "Goals scored per 90 minutes played.",
    "xG/90": "Expected goals per 90 minutes — shot quality independent of finishing.",
    "Passes/90": "Completed passes per 90 minutes played.",
    "Prog. Passes/90": "Progressive passes per 90 minutes — passes that advance the ball significantly toward goal.",
    "Pass %": "Pass completion percentage — completed passes / attempted passes.",
    "xG Over-perf": (
        "Goals scored minus expected goals (xG). Positive = scored more than expected. "
        "Can reflect finishing skill or luck over small samples."
    ),
    "LB Passes/90": "Line-breaking passes per 90 minutes — passes penetrating at least one defensive line.",
    "DEFCON/90": (
        "DEFCON pressure credits received per 90 minutes — how much defensive attention "
        "the attacker attracts. Higher = more pressured."
    ),
    "Dist/Min (m)": "Distance covered per minute in meters. Reflects work rate and activity level.",
    "Top Speed (m/s)": "Peak sprint speed in meters per second. Elite players reach 9-10 m/s.",
    "HSR Distance (m)": "High-Speed Running distance — meters covered above ~5.5 m/s threshold.",
    "Sprint Frames": "Number of tracking frames where the player exceeded sprint speed threshold (~7 m/s).",
    "Avg PAUSA": GLOSSARY["PAUSA"],
    "Avg Temporal Judgment": GLOSSARY["Temporal Judgment"],
    "Avg Spatial Selection": GLOSSARY["Spatial Selection"],
    "Median PAUSA": "Median PAUSA composite score across passes. Less sensitive to outliers than mean.",
    "Pass Count": "Number of passes evaluated for PAUSA scoring.",
    "Passes Above Median": (
        "Number of passes with PAUSA score above 0.5 — indicates consistency of high-quality passes."
    ),
}


def render_glossary_sidebar(page_url_path: str = "") -> None:
    """Render a context-filtered glossary in the sidebar.

    Only shows terms relevant to the current page (per PAGE_TERMS mapping).
    Falls back to the full glossary if the page has no mapping or the mapping is empty.
    This avoids Sweller extraneous load from showing DEFCON definitions on the Shot Map.
    """
    # Check if this page has a mapping (even if the list is empty — empty means "no terms needed")
    if page_url_path in PAGE_TERMS:
        terms = PAGE_TERMS[page_url_path]
        filtered = {k: v for k, v in GLOSSARY.items() if k in terms}
    else:
        # Unknown page or empty path — show full glossary as fallback
        filtered = GLOSSARY

    with st.sidebar:
        with st.expander("Glossary", expanded=False, icon=":material/help:"):
            if filtered:
                for term, definition in filtered.items():
                    st.markdown(f"**{term}**  \n{definition}")
            else:
                st.caption("No domain-specific terms on this page.")


def render_onboarding_sidebar() -> None:
    """Render a getting-started guide in the sidebar, accessible from all pages."""
    with st.sidebar:
        with st.expander("Getting Started", expanded=False, icon=":material/school:"):
            st.markdown(
                "**Suggested workflow:**\n\n"
                "1. **Shot Map** — shot locations and xG\n"
                "2. **Match Summary** — match overview with xG, passing, pressing\n"
                "3. **Player Comparison** — per-90 radar chart (1-3 players)\n"
                "4. **Player Similarity** — find comparable players by style\n"
                "5. **Action Values** — who contributed most? (VAEP)\n"
                "6. **Defensive Impact** — pressure on attackers (DEFCON)\n\n"
                "**Advanced pages** (tracking data, ~20 matches): "
                "Movement & Pressing, Pitch Control, Pass Timing (PAUSA), "
                "Defensive Impact.\n\n"
                "**How to start:** Use the sidebar filters to select a competition, "
                "then a team and match.\n\n"
                "Hover over **?** on any metric for an explanation. "
                "Use **Glossary** below for terms."
            )
