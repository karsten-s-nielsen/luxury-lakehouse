# Taipy Cognitive Parity v2 — Puppeteer-Verified Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the 3 actionable findings from the Puppeteer-verified cognitive audit recheck.

**Architecture:** Three targeted state-file and template fixes. No infrastructure changes needed — the template layer from the prior round is sound.

**Tech Stack:** Taipy 4.1.1, pandas, Python

---

## Findings Classification

| Finding | Status | Action |
|---------|--------|--------|
| X5: PAUSA rankings 18-decimal precision | **Fix** | Round numeric columns to 3 dp |
| X6: DEFCON rankings blank cells for zero values | **Fix** | `fillna(0)` + round |
| X2: Loading text Taipy parser warning | **Fix** | Restructure template line |
| X4: Raw match IDs in Pass Timing | **Not a regression** — Streamlit has the same `COALESCE` fallback; IDSSE matches lack `fct_match_summary_synced` entries. Shared data gap, not a Taipy parity issue | No action |
| X3: Sidebar help icon positioning | **Framework limitation** — Taipy renders `<span>` as paragraph block, cannot inline into MUI label | No action |
| X7: Page transition flash | **Framework limitation** — Taipy SPA routing artifact | No action |
| X1: Warning box unverifiable | **Works by design** — confirmed amber on Pitch Control no-DB test | No action |
| Match Summary delta neutral | **Already handled** — both `ll-metric-delta` and `ll-metric-delta-inverse` use neutral gray CSS | No action |

---

## Task 1: Data Formatting + Template Warning Fix

**Files:**
- Modify: `taipy_spike/src/state/pass_timing.py:500-515`
- Modify: `taipy_spike/src/state/defensive_valuation.py:483-505`
- Modify: `taipy_spike/src/template.py:504-507`

### Fix 1: PAUSA Rankings — Round to 3 Decimal Places

- [ ] **Step 1: Add rounding after rename in `state/pass_timing.py`**

In the `_refresh_data` function, after the `display_df = rankings_df[...].rename(columns={...})` block (~line 514), add rounding before assigning to state:

```python
            # Round numeric columns for display
            for col in ["Avg PAUSA", "Avg Temporal", "Avg Spatial", "Median PAUSA"]:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(3)
            state.pt_rankings_data = display_df
```

Replace the existing `state.pt_rankings_data = display_df` line — don't duplicate it.

### Fix 2: DEFCON Rankings — Fill NaN with 0 and Round

- [ ] **Step 2: Update `_format_rankings_table` in `state/defensive_valuation.py`**

In `_format_rankings_table()` (~line 498-505), after `display = df.drop(columns=...).rename(columns=...)`, expand the rounding block to cover all numeric columns and fill NaN:

```python
    # Round and fill numeric columns for display (0 = no contribution, not blank)
    numeric_cols = ["Total Pressure", "Actions Faced", "Intercepted", "Shots Conceded", "Disturbed", "Deterred"]
    for col in numeric_cols:
        if col in display.columns:
            display[col] = display[col].fillna(0).round(2)

    return display
```

Replace the existing `for col in ["Total Pressure"]:` block — the new code covers all 6 numeric columns.

### Fix 3: Loading Text Template Warning

- [ ] **Step 3: Fix Taipy parser warning in `template.py`**

In `template.py` line 506, the loading overlay mixes HTML `<span>` and Taipy `<|...|>` on the same line, causing: `Missing leading pipe '|' in opening tag`.

Change lines 504-508 from:

```
<|part|render={{is_loading}}|class_name=ll-loading-overlay|
<|part|class_name=ll-loading-spinner|
<span class="material-symbols-outlined ll-spin">progress_activity</span> <|{{loading_text}}|text|raw|>
|>
|>
```

to:

```
<|part|render={{is_loading}}|class_name=ll-loading-overlay|
<|part|class_name=ll-loading-spinner|
<span class="material-symbols-outlined ll-spin">progress_activity</span>

<|{{loading_text}}|text|raw|>
|>
|>
```

The blank line separates the HTML span from the Taipy binding, so the parser sees `<|` at the start of a new block.

### Verification

- [ ] **Step 4: Run test_render.py**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse/taipy_spike && .venv/Scripts/python.exe src/test_render.py
```

Verify: no new warnings. The existing `Missing leading pipe` warning on the root page should disappear.

- [ ] **Step 5: Start app and verify via Puppeteer**

```bash
cd D:/Development/karstenskyt__luxury-lakehouse/taipy_spike && LAKEBASE_HOST="ep-spring-rain-d2i6lozx.database.us-east-1.cloud.databricks.com" LAKEBASE_ENDPOINT_NAME="projects/soccer-analytics-dev/branches/production/endpoints/primary" .venv/Scripts/python.exe src/main.py
```

Navigate to Pass Timing → verify rankings table shows 3 decimal places (e.g., `0.997` not `0.996774706430145`).

Navigate to Defensive Impact → verify rankings table shows `0` or `0.00` in credit columns instead of blank cells.

Check server logs for absence of `Missing leading pipe` warning.

- [ ] **Step 6: Commit**

```bash
git add taipy_spike/src/state/pass_timing.py taipy_spike/src/state/defensive_valuation.py taipy_spike/src/template.py
git commit -m "fix(taipy): data formatting — round PAUSA decimals, fill DEFCON blanks, fix template warning"
```
