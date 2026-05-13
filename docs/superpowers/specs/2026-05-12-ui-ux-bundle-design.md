# UI/UX Bundle — U6 + U7 + UI-4 + UI-3/UI-5 Remnants

**Date:** 2026-05-12
**Status:** Shipped (2026-05-12)
**Scope:** Single PR — Taipy app UX polish, security hardening, accessibility fixes

---

## 1. U6 — VAEP/xT Three-Axis UX Labeling

### Goal

Decompose VAEP/xT displays into three labeled cognitive axes — **Survival**, **Progression**, **Decision Value** — without touching underlying models. Borrowed framing from Garcia de Marina (xR, SOCCHUB 2026); the xR model itself is NOT implemented.

### Changes

#### 1a. Three new GLOSSARY entries (`template.py`)

| Term | Definition |
|------|------------|
| **Survival** | Did the action protect against conceding? The defensive component of VAEP — how much the action reduced opponent scoring probability. Positive = opponent became less likely to score (safer). Negative = opponent became more likely to score (riskier). |
| **Progression** | Was the state advanced? The offensive component — change in own team's scoring probability. Maps to xT zone delta for territorial actions. Higher = more threatening. |
| **Decision Value** | Was the implicit risk worth it? The composite VAEP score — net of Survival cost and Progression gain. Positive = net benefit, negative = net cost. |

#### 1b. Update help_text on Metric/StatCard entries

**`action_values.py`** (7 metrics): Append three-axis mapping to help_text. Example for Total VAEP:
> "Valuing Actions by Estimating Probabilities — how much each on-ball action changed the probability of scoring. Positive = helped, negative = hurt. Measures Decision Value (net of Survival and Progression). Range varies by action; verify typical range against `fct_action_values.vaep_value` at implementation time."

**`movement_analysis.py`** (2 metrics — Avg/Max Off-Ball xT): Map to Progression axis:
> "Cumulative expected threat from off-ball movement. Measures Progression — how much the player's movement improved the team's territorial position. Typical range: 0.001-0.01 per match."

**`pass_timing.py`** (3 metrics — PAUSA, Temporal, Spatial): Map PAUSA to Decision Value, Temporal to Survival (timing = risk), Spatial to Progression (target quality):
> "Passing Ability Under Spatiotemporal Awareness. Composite of Survival (temporal judgment — was the pass released at the safe-yet-optimal moment?) and Progression (spatial selection — was the target the best available?). Higher = better. (Lee et al., MIT Sloan 2026)"

#### 1c. Update PAGE_TERMS (`template.py`)

Add `"Survival"`, `"Progression"`, `"Decision Value"` to the PAGE_TERMS lists for:
- `"Player-Impact"` (VAEP page)
- `"Player-Comparison"` (radar page — shows VAEP/90)
- `"Match-Summary"` (shows xG, which is related but distinct — add only Decision Value for the verdict context)
- `"Movement-Pressing"` (xT page — add Progression)
- `"Pass-Timing"` (PAUSA page)

#### 1d. VAEP model card prose section

Add a "Three-axis interpretation" section to `docs/huggingface/model-cards/vaep-model.md` after the existing "Intended Use" section. Prose-only — explains the framing, cites Garcia de Marina informally (no formal `Citation` or Appendix D entry per TODO item's explicit instruction — non-peer-reviewed SOCCHUB blog post).

### Not changed

No model code, no new Databricks workflows, no composite-score computation, no xR model implementation, no new `Citation(text, url)` in any PageConfig.

---

## 2. U7 — Match Summary Verdict Vocabulary Expansion

### Goal

Expand the 5-phrase verdict set with two cheap-first-pass additions: **Defensive masterclass** and **Comeback win**. Data already in hand — no new dbt models or ingestion changes.

### Current state

`match_summary_verdict.py` contains `derive_verdict(home_xg, away_xg, home_score, away_score)` — a pure function returning `(phrase, detail)`. Resolution order: Draw → Smash & grab → Flattered → Fortunate → Fair result → Fully merited.

### Changes

#### 2a. New verdict: Defensive masterclass

**Condition:** Winner conceded < 0.5 total xG (the opponent barely threatened the goal).

**Resolution position:** After the draw check, before Smash & grab. If the winner held opponents to < 0.5 xG, that narrative dominates regardless of the xG margin.

**Interaction with comeback:** A defensive masterclass that's also a comeback is possible but vanishingly unlikely (concede < 0.5 xG yet trail at some point). Defensive masterclass takes priority — if you held them to < 0.5 xG, that's the dominant narrative.

**Constants:**
```python
_DEFENSIVE_MASTERCLASS_XG = 0.5
```

**Threshold validation:** Before implementation, run a frequency query against `fct_match_summary` to verify how many matches would trigger `loser_xg < 0.5`. The 0.5 threshold should fire for ~5-15% of non-draw matches to be editorially meaningful but not ubiquitous. Adjust if the empirical frequency is outside this range.

#### 2b. New verdict: Comeback win (prefix)

**Condition:** The eventual winner trailed at some point during the match. Detected by reconstructing running score from the goals list.

**Implementation as prefix:** "Comeback win" is not a standalone verdict — it's a **prefix** applied to the base verdict. A match can be a comeback AND fortunate: `"Comeback win — Fortunate"`. A match can be a comeback AND fully merited: `"Comeback win — Fully merited"`.

**Data source:** `fetch_shots_timeline()` is already called in `ms_refresh()`. Extract `(minute, side)` tuples for goals (`is_goal == True`), where `side` is `"home"` or `"away"` — pre-classified by the caller using `home_team_id_raw` / `away_team_id_raw` from `fetch_match_summary`. This keeps `derive_verdict` free of provider-specific ID semantics.

**Own-goal limitation:** SPADL records `team_id` as the performing team (the shooter), and `fct_shots` has no `is_own_goal` column. An own goal is attributed to the shooting team, so comeback detection may misattribute goals. Per `int_running_score.sql:15`, own goals account for ~3-5% of all goals. This is accepted: comeback detection is editorial flavor, not a precision metric, and the edge case rarely flips a determination.

**Comeback detection algorithm:**

"Trailed" means the eventual winner was strictly behind at some point. Being equalized (e.g., 1-0 → 1-1 → 2-1 for home) does NOT count as a comeback — the home team was never behind.

```python
def _detect_comeback(goals: list[tuple[int, str]], home_score: int, away_score: int) -> bool:
    """Return True if the eventual winner trailed (strictly behind) at any point."""
    winner = "home" if home_score > away_score else "away"
    running_home, running_away = 0, 0
    for _minute, side in sorted(goals, key=lambda g: g[0]):
        if side == "home":
            running_home += 1
        else:
            running_away += 1
        # Check if winner was behind after this goal
        if winner == "home" and running_home < running_away:
            return True
        if winner == "away" and running_away < running_home:
            return True
    return False
```

#### 2c. Interface change

```python
def derive_verdict(
    home_xg: float,
    away_xg: float,
    home_score: int,
    away_score: int,
    goals: list[tuple[int, str]] | None = None,  # NEW — (minute, "home"|"away") per goal
) -> tuple[str, str]:
```

When `goals` is `None`, behaves exactly as today (backward-compatible). When provided, enables comeback detection.

#### 2d. New resolution order

```
1. Draw → "Fair result" (unchanged)
2. Winner conceded < 0.5 xG → "Defensive masterclass"
3. Detect comeback_flag from goals timeline
4. Smash & grab (loser xG >= winner xG + 1.5) → [comeback prefix +] "Smash & grab"
5. Flattered (winner xG >= 2x goals) → [comeback prefix +] "Flattered by scoreline"
6. Fortunate (winner xG < loser xG) → [comeback prefix +] "Fortunate"
7. Fair result (|xG delta| < 0.3) → [comeback prefix +] "Fair result"
8. Default → [comeback prefix +] "Fully merited"
```

#### 2e. Caller update (`match_summary.py`)

In `ms_refresh()`, the `derive_verdict` call currently runs at line 183 — **before** `fetch_shots_timeline` at line 190. Remove the existing call at line 183-185 and replace it with a single `derive_verdict` call AFTER the try/except block (after line 200), with best-effort goals extraction from the already-fetched `shots` variable:

```python
# Replace lines 183-185 with goals extraction + single verdict call.
# This block goes AFTER the try/except that fetches shots (line 200 return).
# shots is guaranteed in scope: the except branch _clear_all + returns.
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

# Single verdict call — handles goals=None (no comeback) and goals=[...] (comeback prefix).
phrase, detail = derive_verdict(home_xg, away_xg, home_score, away_score, goals=goals)
state.ms_verdict_phrase = phrase
state.ms_verdict_detail = detail
```

**No reordering impact:** The verdict call is no longer inside the shots try/except block. If the shots query fails, the except branch at lines 191-200 calls `_clear_all(state)` and `return`s — execution never reaches the verdict call. If shots succeeds, the verdict call runs once with best-effort goals extraction. No double-call, no wasted work.

Note: `goal_rows.iterrows()` over a goals-only subset is fine — max ~10 rows per match. Not a hot path. Uses `.astype(bool)` instead of `== True` because Lakebase returns integer 0/1 for boolean columns.

#### 2f. Help text update

Update the verdict StatCard's `help_text` to list all 7 phrases:
> "Editorial interpretation of whether the scoreline reflected the run of play, by xG margin. Phrases: Fully merited / Fair result / Fortunate / Smash & grab / Flattered by scoreline / Defensive masterclass / Comeback win (prefix). See glossary for each phrase."

Add 2 new GLOSSARY entries for "Defensive masterclass" and "Comeback win".

#### 2g. Tests

- Extend `test_match_summary_verdict.py` (or create if absent) with cases for:
  - Defensive masterclass: winner xG > loser xG, loser xG < 0.5
  - Comeback win prefix: goals show winner trailing then winning
  - Comeback + Fortunate: winner trailed AND winner xG < loser xG
  - Defensive masterclass trumps comeback: loser xG < 0.5 AND winner trailed
  - Backward compat: `goals=None` produces identical output to current behavior

### Deferred

Late winner, Against the run of play, Man-advantage winner, Won on set pieces — all need additional data plumbing or design work.

---

## 3. UI-4 — Lightbox CSP Security Hardening

### Goal

Move the 630-line lightbox IIFE from inline injection to a static JS file and add a Content-Security-Policy header. Closes CHI audit findings #5 (Critical) and #23 (Medium).

### Current state

- `_LIGHTBOX_SCRIPT` defined as a Python string in `main.py:163-305`
- Injected via `_inject_lightbox` Flask `after_request` hook (`main.py:307-313`)
- Replaces `</body>` with `<script>...</script></body>`
- Zero CSP headers anywhere in the app

### Changes

#### 3a. Extract to static file

Move the JS content (between `<script>` and `</script>` tags) from `_LIGHTBOX_SCRIPT` to `hf_taipy_app/static/lightbox.js`. Delete the `_LIGHTBOX_SCRIPT` string constant from `main.py`.

#### 3b. Flask static folder

Verify or set `static_folder="static"` on the Flask app constructor. The default Flask behavior serves `/static/` from a `static/` directory relative to the app module.

#### 3c. Rewrite `_inject_lightbox`

Instead of injecting the full `<script>...</script>` block, inject a `<script src>` tag:

```python
_LIGHTBOX_TAG = '<script src="/static/lightbox.js"></script>'

@flask_app.after_request
def _inject_lightbox(response):
    if response.content_type and "text/html" in response.content_type:
        html = response.get_data(as_text=True)
        if "</body>" in html and "ll-lightbox-overlay" not in html:
            response.set_data(html.replace("</body>", _LIGHTBOX_TAG + "</body>"))
    return response
```

#### 3d. Add CSP header

Add a `_set_csp` Flask `after_request` hook (or extend `_inject_lightbox`) for HTML responses:

```
Content-Security-Policy:
    default-src 'self';
    script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com;
    style-src 'self' 'unsafe-inline';
    img-src 'self' data: blob:;
    connect-src 'self' wss:;
    font-src 'self';
```

Rationale for each directive:
- `script-src 'self'` — our static JS (lightbox.js) + Taipy's own bundled scripts.
- `https://unpkg.com` — the AI-ML-Workflows DAG page loads three CDN scripts (cytoscape, dagre, cytoscape-dagre) from `unpkg.com` (`workflows_dag.py:290-298`). All three carry SRI `integrity` hashes, so even if unpkg were compromised, tampered content would be rejected by the browser's subresource integrity check. Bundling these as static files would eliminate the CDN dependency entirely but is a larger scope change deferred to a future PR.
- `'unsafe-inline'` — required for two remaining inline `<script>` injections (`AUTOSIZE_JS` in `workflows_dag.py:55-70`, DAG rendering in `workflows_dag.py:290+`) plus any Taipy framework inline scripts. Extracting the lightbox to a static file eliminates our largest inline script (~630 lines); full `'unsafe-inline'` removal is a future task requiring extraction of the remaining two injections + Taipy upstream audit. Documented as a framework constraint in a code comment.
- `'unsafe-eval'` — Taipy's reactive expression engine uses `eval()` internally (`expr_<hash>` pattern). Without this, Taipy breaks. Documented as a framework constraint in a code comment.
- `style-src 'unsafe-inline'` — Taipy injects inline styles extensively (component rendering). Cannot avoid without upstream Taipy changes. Documented as a framework constraint.
- `img-src data: blob:` — Matplotlib renders to base64 data URIs; Plotly uses blob URLs.
- `connect-src wss:` — Taipy WebSocket for state sync.
- `font-src 'self'` — Material Symbols loaded from same origin.

#### 3e. What this achieves

Blocks external script injection (`default-src 'self'` prevents loading scripts from arbitrary origins). The `unsafe-inline` (script + style) and `unsafe-eval` exceptions are pragmatic concessions: Taipy's framework architecture + two remaining inline injections require them. The lightbox extraction to a static file eliminates our largest inline script (~630 lines). Full `'unsafe-inline'` removal from `script-src` is a future task (requires extracting `AUTOSIZE_JS` and DAG rendering from `workflows_dag.py` + auditing Taipy's own inline script behavior). This is a defense-in-depth step, not defense-in-total.

---

## 4. UI-5 — Accessibility & Responsive Fixes

### 4a. Spinner never appears during filter changes (all pages)

**Root cause:** The loading overlay uses `render={{is_loading}}` in `template.py:1129`, which controls DOM *presence*. When `is_loading` flips `True → False` within a single synchronous callback (e.g., `on_team_change` → `_refresh_current_page`), Taipy either:
- Batches both updates and the client only sees `False`, or
- Sends `True` but the DOM mount round-trip isn't complete before `False` arrives.

**Fix:** Replace `render={{is_loading}}` with an always-present overlay hidden via CSS. Use Taipy's dynamic `class_name` binding to toggle visibility:

```
<|part|class_name=ll-loading-overlay {"ll-loading-visible" if is_loading else ""}|
```

CSS changes in `style_v2.css`:
```css
.ll-loading-overlay {
    display: none;  /* hidden by default — no positioning overhead when hidden */
}
.ll-loading-visible {
    display: flex;
    position: fixed;
    top: 0;
    left: 300px;
    right: 0;
    bottom: 0;
    background: rgba(14, 17, 23, 0.7);
    align-items: center;
    justify-content: center;
    z-index: 1200;
    pointer-events: all;
}
```

The element stays in the DOM at all times — toggling CSS `display` is synchronous on the client, no DOM mount/unmount round-trip.

**Verification:** Deploy to staging, select Competition → Team on Conversion Funnel (dashboard page), confirm spinner appears during the SQL query and disappears when content renders.

### 4b. Loading overlay off-screen on mobile

**Bug:** `.ll-loading-overlay` has `left: 300px` (sidebar width) but the 768px media query doesn't reset it. When the sidebar stacks above content, the overlay is 300px offset from left.

**Fix:** Add to the 768px media query in `style_v2.css`:

```css
@media (max-width: 768px) {
    .ll-loading-visible {
        left: 0;
    }
}
```

### 4c. Missing `_LOADING_TEXTS` entries (`shared.py`)

Add 4 missing pages to `_LOADING_TEXTS`:

```python
"Tactical-Positions": "Loading tactical data...",
"Goalkeeper-Analytics": "Loading goalkeeper data...",
"Conversion-Funnel": "Loading funnel data...",
"AI-ML-Workflows": "Loading workflow data...",
```

### 4d. CVD audit tooling

Integrate a CVD-capable color science library (e.g., `colour-science`, `colorspacious`, or a minimal hand-rolled CIELAB distance check) to programmatically check chart color palettes for color-vision-deficiency accessibility. Scope: add a test that imports every color constant from `render.py` (HOME_COLOR, AWAY_COLOR, etc.) and asserts minimum perceptual distance under deuteranopia/protanopia simulation. Fails if any pair of semantically-distinct colors (e.g., home vs. away) falls below the WCAG-recommended JND threshold.

**Dependencies:** TBD at implementation time — evaluate `colour-science` (actively maintained) vs `colorspacious` (last release 2018) vs minimal inline CIELAB math.

---

## 5. UI-3 Remnant — Redundant matplotlib.use("Agg")

Remove `matplotlib.use("Agg")` from:
- `movement_analysis.py:27`
- `pitch_control.py:31`

The canonical call lives in `render.py:13`, imported before either module. Both files already have comments confirming this.

---

## File Impact Summary

| Component | Files touched | Character |
|-----------|--------------|-----------|
| U6 | `template.py` (GLOSSARY + PAGE_TERMS), `action_values.py`, `movement_analysis.py`, `pass_timing.py`, `vaep-model.md` | Prose-only |
| U7 | `match_summary_verdict.py`, `match_summary.py`, `match_summary.py` page config help_text, `template.py` (GLOSSARY), test file | Pure function + wiring |
| UI-4 | `main.py`, new `static/lightbox.js` | Security plumbing |
| UI-5 | `template.py` (loading overlay markup), `style_v2.css`, `shared.py` (_LOADING_TEXTS), new CVD test | Accessibility |
| UI-3 | `movement_analysis.py`, `pitch_control.py` | 2-line deletion |
