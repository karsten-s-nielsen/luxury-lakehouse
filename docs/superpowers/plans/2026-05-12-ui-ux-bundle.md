# UI/UX Bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single PR bundling five Taipy app UX items: VAEP three-axis labeling (U6), verdict vocabulary expansion (U7), lightbox CSP hardening (UI-4), accessibility/responsive fixes (UI-5), and redundant matplotlib.use removal (UI-3).

**Architecture:** All changes are Taipy-app-side — no Databricks workflows, no dbt models, no ingestion code. U6 is prose-only (glossary + help_text). U7 adds two verdicts to a pure function + caller wiring. UI-4 extracts inline JS to a static file and adds a CSP header. UI-5 fixes the loading spinner DOM strategy and adds missing loading texts + a CVD color test. UI-3 deletes two redundant lines.

**Tech Stack:** Python 3.10, Taipy 4.1.1, Flask (Taipy's internal instance), pytest, CSS

**Spec:** `docs/superpowers/specs/2026-05-12-ui-ux-bundle-design.md`

---

## File Map

| File | Action | Component |
|------|--------|-----------|
| `hf_taipy_app/src/template.py` | Modify (GLOSSARY + PAGE_TERMS + loading overlay markup) | U6, U7, UI-5 |
| `hf_taipy_app/src/pages/action_values.py` | Modify (help_text) | U6 |
| `hf_taipy_app/src/pages/movement_analysis.py` | Modify (help_text) | U6 |
| `hf_taipy_app/src/pages/pass_timing.py` | Modify (help_text) | U6 |
| `hf_taipy_app/src/pages/match_summary.py` | Modify (StatCard help_text) | U7 |
| `hf_taipy_app/src/state/match_summary_verdict.py` | Modify (add 2 verdicts + comeback detection) | U7 |
| `hf_taipy_app/src/state/match_summary.py` | Modify (goals extraction + verdict call reorder) | U7 |
| `hf_taipy_app/src/state/shared.py` | Modify (_LOADING_TEXTS) | UI-5 |
| `hf_taipy_app/src/main.py` | Modify (extract lightbox, add CSP) | UI-4 |
| `hf_taipy_app/static/lightbox.js` | Create (extracted JS) | UI-4 |
| `hf_taipy_app/Dockerfile` | Modify (add COPY static/) | UI-4 |
| `hf_taipy_app/src/style_v2.css` | Modify (loading overlay + mobile fix) | UI-5 |
| `hf_taipy_app/src/state/movement_analysis.py` | Modify (delete matplotlib.use) | UI-3 |
| `hf_taipy_app/src/state/pitch_control.py` | Modify (delete matplotlib.use) | UI-3 |
| `docs/huggingface/model-cards/vaep-model.md` | Modify (three-axis section) | U6 |
| `src/tests/test_match_summary_verdict.py` | Modify (new test cases) | U7 |
| `src/tests/test_cvd_color_accessibility.py` | Create (CVD perceptual distance test) | UI-5 |

---

### Task 1: U7 — Verdict expansion (TDD: tests first)

**Files:**
- Modify: `src/tests/test_match_summary_verdict.py`
- Modify: `hf_taipy_app/src/state/match_summary_verdict.py`

- [ ] **Step 1: Write failing tests for new verdicts**

Modify `src/tests/test_match_summary_verdict.py`:

First, update the existing test case on line 29 that will break under the new resolution order. The case `(2.5, 0.4, 1, 0)` has `loser_xg=0.4 < 0.5`, so it now routes to "Defensive masterclass" instead of "Flattered by scoreline". Replace line 29:

```python
    # Flattered by scoreline — winner xG >= 2 * winner goals AND loser xG >= 0.5
    (2.5, 0.6, 1, 0, "Flattered by scoreline", "1.9"),
```

Also update the existing Smash & grab cases on lines 25-26. The case `(0.4, 2.1, 1, 0)` has `loser_xg=0.4 < 0.5` (home wins with away_xg=2.1, but home is winner so loser_xg=away_xg=2.1... wait, no: home_score=1 > away_score=0, so home wins. winner_xg=home_xg=0.4, loser_xg=away_xg=2.1. loser_xg=2.1 >= 0.5, so Defensive masterclass does NOT fire). Actually this case is fine — loser_xg=2.1, not < 0.5. Only the line 29 case breaks.

Then add new test cases after line 33 (end of `VERDICT_CASES` list, before the closing `]`):

```python
    # Defensive masterclass — loser xG < 0.5
    (2.0, 0.3, 2, 0, "Defensive masterclass", "1.7"),
    (0.3, 2.0, 0, 2, "Defensive masterclass", "1.7"),  # symmetric — away winner
    # Defensive masterclass — loser xG exactly 0.5 does NOT trigger (must be strictly <)
    (2.0, 0.5, 2, 0, "Fully merited", "1.5"),
```

Add these new test functions after the existing `test_flattered_requires_winner_goals_positive` function (after line 97):

```python
def test_defensive_masterclass_trumps_smash_and_grab() -> None:
    """Loser xG 0.4 < 0.5: Defensive masterclass fires before Smash & grab,
    even though gap 1.6 > 1.5 would qualify for Smash & grab."""
    phrase, _ = derive_verdict(2.0, 0.4, 1, 0)
    assert phrase == "Defensive masterclass"


def test_comeback_prefix_basic() -> None:
    """Home trails 0-1, then wins 2-1 → Comeback win prefix."""
    goals = [(10, "away"), (30, "home"), (60, "home")]
    phrase, _ = derive_verdict(2.0, 1.0, 2, 1, goals=goals)
    assert phrase.startswith("Comeback win")
    assert "Fully merited" in phrase


def test_comeback_equalized_then_won_is_not_comeback() -> None:
    """Home leads 1-0, equalized 1-1, then wins 2-1. Never trailed → no comeback."""
    goals = [(10, "home"), (30, "away"), (60, "home")]
    phrase, _ = derive_verdict(2.0, 1.0, 2, 1, goals=goals)
    assert phrase == "Fully merited"
    assert "Comeback" not in phrase


def test_comeback_plus_fortunate() -> None:
    """Winner trailed AND winner xG < loser xG → Comeback win — Fortunate."""
    goals = [(10, "away"), (30, "home"), (60, "home")]
    phrase, _ = derive_verdict(0.8, 1.5, 2, 1, goals=goals)
    assert phrase == "Comeback win — Fortunate"


def test_defensive_masterclass_trumps_comeback() -> None:
    """Loser xG < 0.5 AND winner trailed → Defensive masterclass wins."""
    goals = [(10, "away"), (30, "home"), (60, "home")]
    phrase, _ = derive_verdict(2.0, 0.3, 2, 1, goals=goals)
    assert phrase == "Defensive masterclass"


def test_backward_compat_goals_none() -> None:
    """goals=None produces identical output to current behavior."""
    without = derive_verdict(2.0, 1.0, 2, 1)
    with_none = derive_verdict(2.0, 1.0, 2, 1, goals=None)
    assert without == with_none


def test_draw_still_fair_result_even_with_goals() -> None:
    """Draws always → Fair result regardless of goals timeline."""
    goals = [(10, "home"), (30, "away")]
    phrase, _ = derive_verdict(1.0, 1.0, 1, 1, goals=goals)
    assert phrase == "Fair result"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_match_summary_verdict.py -v`

Expected: New parametrize cases + new functions FAIL (derive_verdict doesn't accept `goals` yet). The updated line-29 case also fails (still returns "Flattered" under old code). Existing tests that don't touch the new resolution path continue to PASS. Total existing: 9 parametrized cases + 5 standalone functions = 14 tests.

- [ ] **Step 3: Implement verdict expansion**

Replace the entire contents of `hf_taipy_app/src/state/match_summary_verdict.py`:

```python
"""Match Summary verdict derivation — pure function per spec §8.

Maps ``(home_xg, away_xg, home_score, away_score)`` to one of seven phrases
plus a compact xG-gap detail string.

Resolution order (first match wins):
    1. Draw → always Fair result (detail carries xG gap nuance).
    2. Defensive masterclass — loser xG < 0.5.
    3. Detect comeback flag from goals timeline.
    4. Smash & grab    — loser xG ≥ winner xG + 1.5.
    5. Flattered       — winner xG >= 2 * winner goals AND winner scored > 0.
    6. Fortunate       — winner xG < loser xG (winning against xG, but gap < 1.5).
    7. Fair result     — |xG Δ| < 0.3.
    8. Fully merited   — default for clear wins where none of the above apply.

Comeback win is a prefix applied to verdicts 4-8 when the eventual winner
trailed (strictly behind) at any point during the match.

Pure function: no Taipy state, no DB access, fully deterministic.
"""

from __future__ import annotations

_SMASH_AND_GRAB_GAP = 1.5
_FAIR_RESULT_GAP = 0.3
_FLATTERED_XG_RATIO = 2.0
_DEFENSIVE_MASTERCLASS_XG = 0.5


def _detect_comeback(
    goals: list[tuple[int, str]], home_score: int, away_score: int
) -> bool:
    """Return True if the eventual winner trailed (strictly behind) at any point."""
    winner = "home" if home_score > away_score else "away"
    running_home, running_away = 0, 0
    for _minute, side in sorted(goals, key=lambda g: g[0]):
        if side == "home":
            running_home += 1
        else:
            running_away += 1
        if winner == "home" and running_home < running_away:
            return True
        if winner == "away" and running_away < running_home:
            return True
    return False


def derive_verdict(
    home_xg: float,
    away_xg: float,
    home_score: int,
    away_score: int,
    goals: list[tuple[int, str]] | None = None,
) -> tuple[str, str]:
    """Return (phrase, detail) for the Match Summary "Our Verdict" tile.

    Parameters
    ----------
    goals : list of (minute, "home"|"away") tuples, or None.
        When provided, enables comeback detection. Each tuple represents
        a goal scored by the named side at the given minute.
    """
    xg_gap = abs(home_xg - away_xg)
    higher_xg_label = "Home" if home_xg > away_xg else "Away"
    detail = f"{higher_xg_label} +{xg_gap:.1f} xG gap (Home {home_xg:.1f} vs Away {away_xg:.1f})"

    if home_score == away_score:
        return "Fair result", detail

    if home_score > away_score:
        winner_xg, loser_xg, winner_goals = home_xg, away_xg, home_score
    else:
        winner_xg, loser_xg, winner_goals = away_xg, home_xg, away_score

    # Defensive masterclass — dominant narrative when opponent barely threatened
    if loser_xg < _DEFENSIVE_MASTERCLASS_XG:
        return "Defensive masterclass", detail

    # Comeback detection
    comeback = False
    if goals is not None:
        comeback = _detect_comeback(goals, home_score, away_score)

    # Base verdict resolution
    if loser_xg >= winner_xg + _SMASH_AND_GRAB_GAP:
        base = "Smash & grab"
    elif winner_goals > 0 and winner_xg >= _FLATTERED_XG_RATIO * winner_goals:
        base = "Flattered by scoreline"
    elif winner_xg < loser_xg:
        base = "Fortunate"
    elif xg_gap < _FAIR_RESULT_GAP:
        base = "Fair result"
    else:
        base = "Fully merited"

    if comeback:
        return f"Comeback win — {base}", detail
    return base, detail
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_match_summary_verdict.py -v`

Expected: All tests PASS (12 parametrized cases + 12 standalone functions = 24 total).

---

### Task 2: U7 — Caller wiring + page config updates

**Files:**
- Modify: `hf_taipy_app/src/state/match_summary.py:183-185` (verdict call reorder)
- Modify: `hf_taipy_app/src/pages/match_summary.py:70-79` (StatCard help_text)
- Modify: `hf_taipy_app/src/template.py:252-332` (GLOSSARY + PAGE_TERMS)

- [ ] **Step 1: Add goals extraction + upgrade verdict call in ms_refresh**

In `hf_taipy_app/src/state/match_summary.py`, replace the existing `derive_verdict` call at lines 183-185 with goals extraction + a single verdict call. The old call is removed because the new single call handles both paths (goals=None produces the same result as the old call without the `goals` parameter):

Replace lines 183-185:

```python
    phrase, detail = derive_verdict(home_xg, away_xg, home_score, away_score)
    state.ms_verdict_phrase = phrase
    state.ms_verdict_detail = detail
```

With:

```python
    # Extract goal timeline for comeback detection (best-effort).
    # shots is defined at line 190 inside the try block. If the try block raised,
    # the except block called _clear_all(state) and returned — so this code only
    # runs when shots succeeded. The variable is always defined at this point.
    goals = None
    try:
        if not shots.empty:
            goal_rows = shots[shots["is_goal"].astype(bool)]
            if not goal_rows.empty:
                goals = [
                    (int(row["minute"]), "home" if row["team_id"] == home_team_id_raw else "away")
                    for _, row in goal_rows.iterrows()
                ]
    except Exception:
        logger.warning("Comeback detection failed for match_key=%s", match_key, exc_info=True)

    # Single verdict call — handles both goals=None (no comeback detection)
    # and goals=[...] (adds comeback prefix + defensive masterclass).
    phrase, detail = derive_verdict(home_xg, away_xg, home_score, away_score, goals=goals)
    state.ms_verdict_phrase = phrase
    state.ms_verdict_detail = detail
```

**Placement:** This block goes AFTER the try/except that fetches `shots` and `decisive` (line 200 `return`). The goals extraction references `shots`, which is guaranteed to be in scope here — the except branch at line 191-200 calls `_clear_all` + `return`, so execution only reaches this point when `shots` is defined.

- [ ] **Step 2: Update StatCard help_text**

In `hf_taipy_app/src/pages/match_summary.py`, replace lines 74-77:

```python
            help_text=(
                "Editorial interpretation of whether the scoreline reflected the run of play, "
                "by xG margin. Phrase set: Fully merited / Fair result / Fortunate / "
                "Smash & grab / Flattered by scoreline."
            ),
```

With:

```python
            help_text=(
                "Editorial interpretation of whether the scoreline reflected the run of play, "
                "by xG margin. Phrases: Fully merited / Fair result / Fortunate / "
                "Smash & grab / Flattered by scoreline / Defensive masterclass / "
                "Comeback win (prefix). See glossary for each phrase."
            ),
```

- [ ] **Step 3: Add GLOSSARY entries**

In `hf_taipy_app/src/template.py`, add these entries inside the `GLOSSARY` dict in alphabetical position:

"Comeback win" — alphabetically between "Bubble Map" area and "Cosine Distance" (Co < Co, but "Comb" < "Cosi"):

```python
    "Comeback win": (
        "The eventual winner trailed (strictly behind) at some point during the match, "
        "then came back to win. Applied as a prefix to the base verdict "
        "(e.g., 'Comeback win — Fortunate'). Note: own goals (~3-5% of all goals) may be "
        "misattributed due to SPADL recording conventions."
    ),
```

"Defensive masterclass" — alphabetically near the "DEFCON" entry:

```python
    "Defensive masterclass": (
        "The winning team conceded fewer than 0.5 expected goals — the opponent barely "
        "threatened. This verdict takes priority over all other non-draw verdicts."
    ),
```

- [ ] **Step 4: Update PAGE_TERMS for Match-Summary**

In `hf_taipy_app/src/template.py`, update the `"Match-Summary"` entry in `PAGE_TERMS` (lines 257-269) to add the two new verdict terms:

```python
    "Match-Summary": [
        "xG (Expected Goals)",
        "PPDA",
        "VAEP",
        "Progressive Pass",
        "Our Verdict",
        "Big Story",
        "Fully merited",
        "Fair result",
        "Fortunate",
        "Smash & grab",
        "Flattered by scoreline",
        "Defensive masterclass",
        "Comeback win",
    ],
```

- [ ] **Step 5: Verify existing tests still pass**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_match_summary_verdict.py -v`

Expected: All tests PASS.

---

### Task 3: U6 — VAEP three-axis glossary + help_text + model card

**Files:**
- Modify: `hf_taipy_app/src/template.py:37-250` (GLOSSARY)
- Modify: `hf_taipy_app/src/template.py:252-332` (PAGE_TERMS)
- Modify: `hf_taipy_app/src/pages/action_values.py:56-98` (help_text)
- Modify: `hf_taipy_app/src/pages/movement_analysis.py:92-103` (help_text)
- Modify: `hf_taipy_app/src/pages/pass_timing.py:52-68` (help_text)
- Modify: `docs/huggingface/model-cards/vaep-model.md:189` (new section)

- [ ] **Step 1: Add three-axis GLOSSARY entries**

In `hf_taipy_app/src/template.py`, add these entries inside the `GLOSSARY` dict in alphabetical position:

After "Cosine Distance" (before "DEFCON"):
```python
    "Decision Value": (
        "Was the implicit risk worth it? The composite VAEP score — net of Survival cost "
        "and Progression gain. Positive = net benefit, negative = net cost."
    ),
```

After "Passing Network" (before "Pass Connection" or wherever alphabetical):
```python
    "Progression": (
        "Was the state advanced? The offensive component of VAEP — change in own team's "
        "scoring probability. Maps to xT zone delta for territorial actions. "
        "Higher = more threatening."
    ),
```

After "Stretch Index" (before "Team Length" or wherever alphabetical):
```python
    "Survival": (
        "Did the action protect against conceding? The defensive component of VAEP — "
        "how much the action reduced opponent scoring probability. "
        "Positive = opponent became less likely to score (safer). "
        "Negative = opponent became more likely to score (riskier)."
    ),
```

- [ ] **Step 2: Update PAGE_TERMS for three-axis pages**

In `hf_taipy_app/src/template.py`, update these `PAGE_TERMS` entries:

For `"Player-Impact"` (line 270), append the three terms:
```python
    "Player-Impact": [
        "VAEP", "VAEP/90", "Off. VAEP/90", "Def. VAEP/90", "SPADL", "Percentile Rank",
        "Survival", "Progression", "Decision Value",
    ],
```

For `"Player-Comparison"` (lines 271-286), append the three terms:
```python
    "Player-Comparison": [
        "Goals/90",
        "xG (Expected Goals)",
        "xG Over-performance",
        "Passes/90",
        "Pass %",
        "Progressive Pass",
        "Line-Breaking Pass",
        "VAEP",
        "VAEP/90",
        "Off. VAEP/90",
        "Def. VAEP/90",
        "DEFCON",
        "DEFCON/90",
        "Percentile Rank",
        "Survival", "Progression", "Decision Value",
    ],
```

For `"Match-Summary"` — append `"Decision Value"` to the list already updated in Task 2 Step 4 (which added `"Defensive masterclass"` and `"Comeback win"`).

For `"Movement-Pressing"` (line 298), append `"Progression"`:
```python
    "Movement-Pressing": ["PPDA", "xT (Expected Threat)", "Off-Ball xT", "Pitch Control", "Progression"],
```

For `"Pass-Timing"` (line 300), append `"Survival"`, `"Progression"`, `"Decision Value"`:
```python
    "Pass-Timing": [
        "PAUSA", "Temporal Judgment", "Spatial Selection", "OBSO", "Passes with Value",
        "Survival", "Progression", "Decision Value",
    ],
```

- [ ] **Step 3: Update action_values.py help_text**

In `hf_taipy_app/src/pages/action_values.py`, update the VAEP-derived metrics' help_text (5 of 7 — "Total Actions" and "Top Action Type" are counts/labels, not VAEP axes):

Line 59 — Total VAEP:
```python
                    "Valuing Actions by Estimating Probabilities — how much each on-ball action changed "
                    "the probability of scoring. Positive = helped, negative = hurt. "
                    "Measures Decision Value (net of Survival and Progression).",
```

Line 86 — Positive Actions:
```python
                    "Actions with positive VAEP — contributed to scoring probability. "
                    "Reflects net Decision Value: Survival gain + Progression gain.",
```

Line 88 — Negative Actions (currently single-line):
```python
                Metric(
                    "Negative Actions",
                    "av_negative",
                    "Actions with negative VAEP — reduced scoring probability. "
                    "Reflects net Decision Value: Survival cost exceeded Progression gain.",
                ),
```

Line 92 — Net Match VAEP:
```python
                    "Sum of all VAEP values in a match — positive = team created more than conceded. "
                    "Aggregate Decision Value across all on-ball actions.",
```

Line 97 — Most Valuable Action:
```python
                    "The single action that contributed most to scoring probability in this match. "
                    "Highest absolute Decision Value.",
```

- [ ] **Step 4: Update movement_analysis.py help_text**

In `hf_taipy_app/src/pages/movement_analysis.py`, update lines 97 and 102:

Line 97 — Avg Off-Ball xT:
```python
                    "Avg Off-Ball xT",
                    "ma_oxt_avg",
                    "Average expected threat from off-ball movement. Measures Progression — "
                    "how much players' movement improved territorial position. Typical range: 0.001-0.01.",
```

Line 102 — Max Off-Ball xT:
```python
                    "Max Off-Ball xT",
                    "ma_oxt_max",
                    "Highest cumulative off-ball xT by any single player. Measures Progression. "
                    "Typical range: 0.001-0.01.",
```

- [ ] **Step 5: Update pass_timing.py help_text**

In `hf_taipy_app/src/pages/pass_timing.py`, update lines 56, 61, 66:

Line 56 — Avg PAUSA:
```python
            "Passing Ability Under Spatiotemporal Awareness. Composite of "
            "Survival (temporal judgment) and Progression (spatial selection). "
            "Measures Decision Value of pass timing. Higher = better. "
            "(Lee et al., MIT Sloan 2026)",
```

Line 61 — Avg Temporal Judgment:
```python
            "Was the pass released at the optimal moment? Ratio of actual OBSO at release "
            "to peak OBSO in the ±3s/+1s window. 1.0 = perfect timing. Measures Survival — "
            "releasing too early or late increases turnover risk.",
```

Line 66 — Avg Spatial Selection:
```python
            "Was the target location the best available? Ratio of actual OBSO at target "
            "to maximum OBSO across all receivers. 1.0 = optimal target. Measures Progression — "
            "better targets advance the ball into more threatening territory.",
```

- [ ] **Step 6: Add three-axis section to VAEP model card**

In `docs/huggingface/model-cards/vaep-model.md`, insert after line 189 (end of "Intended Use" section, before "## EU AI Act"):

```markdown

## Three-Axis Interpretation

VAEP's two probability deltas map naturally to three cognitive axes for interpretation, borrowing framing from García de Marina's xR framework (SOCCHUB 2026):

- **Survival** — the defensive component (`vaep_defensive`). How much did the action reduce the opponent's scoring probability? Positive = safer (opponent less likely to score). Negative = riskier (opponent more likely to score). Computed as `P(concedes)_before - P(concedes)_after` — the sign flip in the `vaep_defensive` computation ensures positive = good.

- **Progression** — the offensive component (`vaep_offensive`). How much did the action advance the team's own scoring probability? Positive = more threatening. Computed as `P(scores)_after - P(scores)_before`.

- **Decision Value** — the composite VAEP score (`vaep_value = vaep_offensive + vaep_defensive`). Was the implicit risk worth it? Positive = the action's Progression gain exceeded its Survival cost. Negative = the risk outweighed the benefit.

This is a labeling convention, not a new model. The underlying probabilities and computation are unchanged. The xR model itself (which introduces additional axes like xR-score) is not implemented here.
```

- [ ] **Step 7: Verify no tests broken**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_match_summary_verdict.py -v`

Expected: All tests PASS.

---

### Task 4: UI-4 — Lightbox CSP security hardening

**Files:**
- Modify: `hf_taipy_app/src/main.py:152-315`
- Create: `hf_taipy_app/static/lightbox.js`

- [ ] **Step 1: Create the static directory**

Run: `mkdir -p D:/Development/karstenskyt__luxury-lakehouse/hf_taipy_app/static`

- [ ] **Step 2: Extract lightbox JS to static file**

Read `hf_taipy_app/src/main.py` lines 163-305. Extract the JavaScript between `<script>` (line 163) and `</script>` (line 305) — everything between `(function(){` and `})();` inclusive. Write it to `hf_taipy_app/static/lightbox.js`.

The content starts at line 164 (`(function(){`) and ends at line 304 (`})();`). Copy those lines verbatim to the new file.

- [ ] **Step 3: Rewrite main.py lightbox injection + add CSP**

In `hf_taipy_app/src/main.py`, replace lines 155-313 (the lightbox comment, `_LIGHTBOX_SCRIPT` string, and `_inject_lightbox` function) with:

```python
    # Lightbox — click any content image to expand in a full-viewport overlay.
    # Extracted to static/lightbox.js for CSP compliance (CHI audit #5/#23).
    # Injected via after_request because Taipy markdown strips <script> tags.
    _LIGHTBOX_TAG = '<script src="/static/lightbox.js"></script>'

    # Content-Security-Policy — defense-in-depth hardening.
    # 'unsafe-inline' (script): required for AUTOSIZE_JS (workflows_dag.py:55-70),
    #   DAG rendering CDN loader (workflows_dag.py:290+), and Taipy framework scripts.
    # 'unsafe-eval': Taipy reactive expression engine uses eval() (expr_<hash> pattern).
    # 'unsafe-inline' (style): Taipy injects inline styles for component rendering.
    # https://unpkg.com: DAG page CDN scripts with SRI integrity hashes.
    _CSP = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "connect-src 'self' wss:; "
        "font-src 'self'"
    )

    @flask_app.after_request
    def _inject_lightbox_and_csp(response):  # type: ignore[no-untyped-def]
        if response.content_type and "text/html" in response.content_type:
            html = response.get_data(as_text=True)
            if "</body>" in html and "ll-lightbox-overlay" not in html:
                response.set_data(html.replace("</body>", _LIGHTBOX_TAG + "</body>"))
            response.headers["Content-Security-Policy"] = _CSP
        return response
```

- [ ] **Step 4: Set Flask static folder**

In `hf_taipy_app/src/main.py` line 152, update the Flask constructor:

```python
    flask_app = Flask("luxury-lakehouse-taipy", static_folder="../static")
```

The `static_folder` is relative to the Flask app module (`src/main.py`), so `../static` points to `hf_taipy_app/static/`.

- [ ] **Step 5: Add Dockerfile COPY for static directory**

In `hf_taipy_app/Dockerfile`, add a COPY line after line 16 (`COPY --chown=appuser:appuser workflow-cards/ workflow-cards/`):

```dockerfile
COPY --chown=appuser:appuser static/ static/
```

Without this, the Docker build on HF Spaces won't include `static/lightbox.js`, and the lightbox will 404 in production (the old inline `_LIGHTBOX_SCRIPT` is deleted, so there's no fallback).

- [ ] **Step 6: Verify lightbox still works**

This requires a staging deploy. For now, verify the JS file was created correctly:

Run: `head -5 D:/Development/karstenskyt__luxury-lakehouse/hf_taipy_app/static/lightbox.js`

Expected: First line should be `(function(){` (the IIFE opening).

Run: `tail -3 D:/Development/karstenskyt__luxury-lakehouse/hf_taipy_app/static/lightbox.js`

Expected: Last non-empty line should be `})();` (the IIFE closing).

---

### Task 5: UI-5 — Loading spinner fix + missing loading texts

**Files:**
- Modify: `hf_taipy_app/src/template.py:1129-1135` (loading overlay markup)
- Modify: `hf_taipy_app/src/style_v2.css:737-749` (loading overlay CSS)
- Modify: `hf_taipy_app/src/style_v2.css:1608-1647` (768px media query)
- Modify: `hf_taipy_app/src/state/shared.py:193-207` (_LOADING_TEXTS)

- [ ] **Step 1: Fix loading overlay markup**

In `hf_taipy_app/src/template.py`, replace lines 1129-1135:

**Important:** This block is inside an f-string in `build_root_page()` (line 1123: `f"""`). All Taipy `{var}` bindings are escaped as `{{var}}` in the source to survive the f-string. The "old" and "new" text below shows the **source-level** syntax.

```
<|part|render={{is_loading}}|class_name=ll-loading-overlay|
<|part|class_name=ll-loading-spinner|
<span class="material-symbols-outlined ll-spin">progress_activity</span>

<|{{loading_text}}|text|raw|>
|>
|>
```

With:

```
<|part|class_name={{"ll-loading-overlay ll-loading-visible" if is_loading else "ll-loading-overlay"}}|
<|part|class_name=ll-loading-spinner|
<span class="material-symbols-outlined ll-spin">progress_activity</span>

<|{{loading_text}}|text|raw|>
|>
|>
```

**Verify empirically before committing:** Run the Taipy app locally and confirm the loading overlay appears/disappears. The `class_name={{expr}}` syntax uses a Taipy binding expression (the `{{...}}` is f-string escaping → `{...}` in output → Taipy evaluates the Python expression). If it doesn't work, fall back to keeping `render={{is_loading}}` and switching to `display: none` / `display: flex` CSS transition approach instead of DOM removal.

- [ ] **Step 2: Fix loading overlay CSS**

In `hf_taipy_app/src/style_v2.css`, replace lines 737-749:

```css
.ll-loading-overlay {
    position: fixed;
    top: 0;
    left: 300px;  /* sidebar width */
    right: 0;
    bottom: 0;
    background: rgba(14, 17, 23, 0.7);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 1200;
    pointer-events: all;
}
```

With:

```css
.ll-loading-overlay {
    display: none;  /* hidden by default — no positioning overhead when hidden */
}
.ll-loading-visible {
    display: flex;
    position: fixed;
    top: 0;
    left: 300px;  /* sidebar width */
    right: 0;
    bottom: 0;
    background: rgba(14, 17, 23, 0.7);
    align-items: center;
    justify-content: center;
    z-index: 1200;
    pointer-events: all;
}
```

- [ ] **Step 3: Add mobile responsive fix**

In `hf_taipy_app/src/style_v2.css`, inside the `@media (max-width: 768px)` block (after line 1646, before the closing `}`), add:

```css
    /* Loading overlay: full width when sidebar stacks above content */
    .ll-loading-visible {
        left: 0;
    }
```

- [ ] **Step 4: Add missing _LOADING_TEXTS entries**

In `hf_taipy_app/src/state/shared.py`, add these entries to the `_LOADING_TEXTS` dict (after line 206, before the closing `}`):

```python
    "Tactical-Positions": "Loading tactical data...",
    "Goalkeeper-Analytics": "Loading goalkeeper data...",
    "Conversion-Funnel": "Loading funnel data...",
    "AI-ML-Workflows": "Loading workflow data...",
```

- [ ] **Step 5: Verify no syntax errors**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run python -c "import ast; ast.parse(open('hf_taipy_app/src/template.py').read()); ast.parse(open('hf_taipy_app/src/state/shared.py').read()); print('syntax OK')"`

Expected: "syntax OK" — no syntax errors in modified files.

---

### Task 6: UI-5 — CVD color accessibility test

**Files:**
- Create: `src/tests/test_cvd_color_accessibility.py`

- [ ] **Step 1: Write the CVD color test**

Create `src/tests/test_cvd_color_accessibility.py`:

```python
"""CVD (color-vision-deficiency) accessibility test for chart color palettes.

Asserts minimum perceptual distance between semantically-distinct color pairs
under deuteranopia and protanopia simulation. Uses CIELAB ΔE*ab distance —
pairs below JND threshold 20 are indistinguishable to CVD users.

No external dependency — uses hand-rolled sRGB→CIELAB conversion via the
standard D65 illuminant. The CVD simulation matrices are from Brettel et al.
(1997) / Viénot et al. (1999), the same source used by colorspacious.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

from render import AWAY_COLOR, DEFCON_COLORS, HOME_COLOR

# ── sRGB → linear RGB → XYZ → CIELAB pipeline ──────────────────────────────

def _hex_to_linear_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert #RRGGBB to linear RGB (0-1 range, gamma-decoded)."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16) / 255, int(h[2:4], 16) / 255, int(h[4:6], 16) / 255
    # sRGB gamma decode
    def _linearize(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4
    return _linearize(r), _linearize(g), _linearize(b)


def _linear_rgb_to_xyz(r: float, g: float, b: float) -> tuple[float, float, float]:
    """Linear sRGB to CIE XYZ (D65 illuminant)."""
    x = 0.4124564 * r + 0.3575761 * g + 0.1804375 * b
    y = 0.2126729 * r + 0.7151522 * g + 0.0721750 * b
    z = 0.0193339 * r + 0.1191920 * g + 0.9503041 * b
    return x, y, z


def _xyz_to_lab(x: float, y: float, z: float) -> tuple[float, float, float]:
    """CIE XYZ to CIELAB (D65 reference white)."""
    xn, yn, zn = 0.95047, 1.0, 1.08883
    def _f(t: float) -> float:
        return t ** (1 / 3) if t > 0.008856 else 7.787 * t + 16 / 116
    fx, fy, fz = _f(x / xn), _f(y / yn), _f(z / zn)
    l_star = 116 * fy - 16
    a_star = 500 * (fx - fy)
    b_star = 200 * (fy - fz)
    return l_star, a_star, b_star


# ── CVD simulation (Viénot et al. 1999 / Brettel et al. 1997) ──────────────

# Protanopia simulation matrix (applied in linear RGB space)
_PROTAN_MATRIX = [
    (0.152286, 1.052583, -0.204868),
    (0.114503, 0.786281, 0.099216),
    (-0.003882, -0.048116, 1.051998),
]

# Deuteranopia simulation matrix
_DEUTAN_MATRIX = [
    (0.367322, 0.860646, -0.227968),
    (0.280085, 0.672501, 0.047414),
    (-0.011820, 0.042940, 0.968881),
]


def _apply_cvd_matrix(
    matrix: list[tuple[float, float, float]], r: float, g: float, b: float
) -> tuple[float, float, float]:
    """Apply a 3x3 CVD simulation matrix to linear RGB."""
    r2 = matrix[0][0] * r + matrix[0][1] * g + matrix[0][2] * b
    g2 = matrix[1][0] * r + matrix[1][1] * g + matrix[1][2] * b
    b2 = matrix[2][0] * r + matrix[2][1] * g + matrix[2][2] * b
    return max(0, min(1, r2)), max(0, min(1, g2)), max(0, min(1, b2))


def _hex_to_lab_cvd(
    hex_color: str, matrix: list[tuple[float, float, float]]
) -> tuple[float, float, float]:
    """Convert hex color to CIELAB after CVD simulation."""
    r, g, b = _hex_to_linear_rgb(hex_color)
    r2, g2, b2 = _apply_cvd_matrix(matrix, r, g, b)
    x, y, z = _linear_rgb_to_xyz(r2, g2, b2)
    return _xyz_to_lab(x, y, z)


def _delta_e(lab1: tuple[float, float, float], lab2: tuple[float, float, float]) -> float:
    """CIE76 ΔE*ab — Euclidean distance in CIELAB space."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2)))


# ── Test data ───────────────────────────────────────────────────────────────

# Semantically-distinct color pairs that MUST be distinguishable
_CRITICAL_PAIRS: list[tuple[str, str, str, str]] = [
    ("HOME_COLOR", HOME_COLOR, "AWAY_COLOR", AWAY_COLOR),
    ("Intercept", DEFCON_COLORS["Intercept"], "Concede", DEFCON_COLORS["Concede"]),
    ("Intercept", DEFCON_COLORS["Intercept"], "Disturb", DEFCON_COLORS["Disturb"]),
    ("Intercept", DEFCON_COLORS["Intercept"], "Deter", DEFCON_COLORS["Deter"]),
    ("Concede", DEFCON_COLORS["Concede"], "Disturb", DEFCON_COLORS["Disturb"]),
    ("Concede", DEFCON_COLORS["Concede"], "Deter", DEFCON_COLORS["Deter"]),
    ("Disturb", DEFCON_COLORS["Disturb"], "Deter", DEFCON_COLORS["Deter"]),
]

# JND threshold — pairs below this ΔE are considered indistinguishable.
# WCAG-adjacent: typical JND for CVD users is ~10-15; we use 20 as a
# conservative threshold (some research suggests 25 for deuteranopia).
_JND_THRESHOLD = 20.0


@pytest.mark.parametrize(
    ("name_a", "color_a", "name_b", "color_b"),
    _CRITICAL_PAIRS,
    ids=[f"{p[0]}_vs_{p[2]}" for p in _CRITICAL_PAIRS],
)
@pytest.mark.parametrize(
    ("cvd_name", "cvd_matrix"),
    [("protanopia", _PROTAN_MATRIX), ("deuteranopia", _DEUTAN_MATRIX)],
)
def test_color_pair_distinguishable_under_cvd(
    name_a: str,
    color_a: str,
    name_b: str,
    color_b: str,
    cvd_name: str,
    cvd_matrix: list[tuple[float, float, float]],
) -> None:
    """Assert minimum perceptual distance between semantically-distinct colors
    under CVD simulation. Hard assert — failing pairs must be fixed before merge.
    Shape markers (WCAG 1.4.1) provide a secondary cue but do not excuse
    indistinguishable color pairs."""
    lab_a = _hex_to_lab_cvd(color_a, cvd_matrix)
    lab_b = _hex_to_lab_cvd(color_b, cvd_matrix)
    de = _delta_e(lab_a, lab_b)
    assert de >= _JND_THRESHOLD, (
        f"{name_a} ({color_a}) vs {name_b} ({color_b}) under {cvd_name}: "
        f"ΔE = {de:.1f} < {_JND_THRESHOLD} — colors are too similar for CVD users"
    )
```

- [ ] **Step 2: Run CVD tests**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_cvd_color_accessibility.py -v`

Expected: All tests PASS. If any fail, the color pair must be fixed before merge — update the color constant in `render.py` to maintain sufficient perceptual distance under CVD simulation. Shape markers provide a secondary WCAG 1.4.1 cue but do not excuse indistinguishable color pairs.

---

### Task 7: UI-3 — Remove redundant matplotlib.use("Agg")

**Files:**
- Modify: `hf_taipy_app/src/state/movement_analysis.py:27`
- Modify: `hf_taipy_app/src/state/pitch_control.py:31`

- [ ] **Step 1: Remove from movement_analysis.py**

In `hf_taipy_app/src/state/movement_analysis.py`, delete line 27:

```python
matplotlib.use("Agg")
```

Also remove the `matplotlib` import if it becomes unused. Check line ~15 for `import matplotlib` — if the only usage was `.use("Agg")`, delete the import too. If `matplotlib` is used elsewhere in the file (e.g., `matplotlib.figure`), keep the import.

- [ ] **Step 2: Remove from pitch_control.py**

In `hf_taipy_app/src/state/pitch_control.py`, delete line 31:

```python
matplotlib.use("Agg")
```

Same import cleanup as Step 1.

- [ ] **Step 3: Verify no import errors**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run python -c "import ast; ast.parse(open('hf_taipy_app/src/state/movement_analysis.py').read()); ast.parse(open('hf_taipy_app/src/state/pitch_control.py').read()); print('both OK')"`

Expected: "both OK"

---

### Task 8: Lint + type check + full test suite

**Files:** None (verification only)

- [ ] **Step 1: Run ruff check**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run ruff check hf_taipy_app/ src/tests/test_match_summary_verdict.py src/tests/test_cvd_color_accessibility.py`

Expected: No violations. Fix any issues found.

- [ ] **Step 2: Run ruff format check**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run ruff format --check hf_taipy_app/ src/tests/test_match_summary_verdict.py src/tests/test_cvd_color_accessibility.py`

Expected: All files formatted. If not, run `uv run ruff format` on the failing files.

- [ ] **Step 3: Run pyright on modified files**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pyright hf_taipy_app/src/state/match_summary_verdict.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/main.py`

Expected: No errors in basic mode.

- [ ] **Step 4: Run full verdict + CVD tests**

Run: `cd D:\Development\karstenskyt__luxury-lakehouse && uv run pytest src/tests/test_match_summary_verdict.py src/tests/test_cvd_color_accessibility.py -v`

Expected: All tests PASS.

---

### Task 9: Commit

**Files:** All modified/created files from Tasks 1-7

- [ ] **Step 1: Stage all changes**

```bash
git add \
  hf_taipy_app/src/template.py \
  hf_taipy_app/src/pages/action_values.py \
  hf_taipy_app/src/pages/movement_analysis.py \
  hf_taipy_app/src/pages/pass_timing.py \
  hf_taipy_app/src/pages/match_summary.py \
  hf_taipy_app/src/state/match_summary_verdict.py \
  hf_taipy_app/src/state/match_summary.py \
  hf_taipy_app/src/state/shared.py \
  hf_taipy_app/src/state/movement_analysis.py \
  hf_taipy_app/src/state/pitch_control.py \
  hf_taipy_app/src/main.py \
  hf_taipy_app/static/lightbox.js \
  hf_taipy_app/src/style_v2.css \
  hf_taipy_app/Dockerfile \
  docs/huggingface/model-cards/vaep-model.md \
  docs/superpowers/specs/2026-05-12-ui-ux-bundle-design.md \
  docs/superpowers/plans/2026-05-12-ui-ux-bundle.md \
  src/tests/test_match_summary_verdict.py \
  src/tests/test_cvd_color_accessibility.py
```

- [ ] **Step 2: Commit**

```bash
git commit -m "feat(taipy): UI/UX bundle — three-axis labels, verdict expansion, CSP, accessibility (#TBD)

U6: VAEP/xT three-axis UX labeling (Survival/Progression/Decision Value)
U7: Match Summary verdict expansion (Defensive masterclass + Comeback win)
UI-4: Lightbox CSP security hardening (static JS + Content-Security-Policy)
UI-5: Loading spinner DOM fix, mobile responsive, missing loading texts, CVD test
UI-3: Remove redundant matplotlib.use('Agg') calls

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```
