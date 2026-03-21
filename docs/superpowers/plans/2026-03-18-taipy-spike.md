# Taipy Spike Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate Taipy on HF Spaces Docker SDK — WebSocket proxy, mplsoccer rendering, Lakebase psycopg2.

**Architecture:** Progressive spike in `taipy_spike/` directory. Phase A validates three technical unknowns with a minimal Shot Map. Phase B adds full Shot Map feature parity. Phase C proves multi-page routing. Each phase is a go/no-go gate.

**Tech Stack:** Taipy 4.1.1, Flask-SocketIO (gevent), mplsoccer, Plotly, psycopg2-binary, Pydantic, HF Spaces Docker SDK.

**Spec:** `docs/superpowers/specs/2026-03-18-taipy-spike-design.md`

---

## Task 0: Create HF Space and WebSocket Smoke Test

**Files:**
- Create: `taipy_spike/Dockerfile`
- Create: `taipy_spike/.dockerignore`
- Create: `taipy_spike/requirements.txt`
- Create: `taipy_spike/src/main.py`

This is the **Phase A pre-condition** — a separate gate before any business logic. If WebSocket fails here, the spike is over.

- [ ] **Step 1: Create the HF Space repo**

```bash
python -c "
from huggingface_hub import HfApi
api = HfApi()
api.create_repo('luxury-lakehouse/taipy-spike', repo_type='space', space_sdk='docker', private=True)
print('Created')
"
```

- [ ] **Step 2: Create scaffold directory**

```bash
mkdir -p taipy_spike/src/pages
```

- [ ] **Step 3: Write requirements.txt**

All dependencies pinned per spec:

```
taipy==4.1.1
psycopg2-binary==2.9.10
mplsoccer==1.6.1
plotly==6.6.0
pandas==2.2.3
numpy==1.26.4
pydantic==2.12.5
pydantic-settings==2.13.1
requests==2.32.5
databricks-sdk==0.50.0
gunicorn==23.0.0
gevent==24.11.1
gevent-websocket==0.10.1
```

- [ ] **Step 4: Write Dockerfile**

Per spec: `gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1` for proper WebSocket support.

```dockerfile
FROM python:3.10-slim@sha256:87579103010e46d38cae5e8d6c979f2cdbc9ef753daf603813c74cc7e995f6a7

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd -m -u 1000 appuser

WORKDIR /app
COPY --chown=appuser:appuser src/ src/

ENV PYTHONPATH=/app/src

USER appuser

EXPOSE 7860

HEALTHCHECK CMD curl --fail http://localhost:7860 || exit 1

CMD ["python", "src/main.py"]
```

Note: Taipy's `gui.run()` uses Flask-SocketIO internally. If the built-in server doesn't handle WebSocket on HF Spaces, switch the CMD to:
```
CMD ["gunicorn", "-k", "geventwebsocket.gunicorn.workers.GeventWebSocketWorker", "-w", "1", "-b", "0.0.0.0:7860", "src.main:gui"]
```
Both approaches are available since gunicorn+gevent are in requirements.txt.

- [ ] **Step 5: Write .dockerignore**

```
__pycache__
*.pyc
*.pyo
.git
.env
```

- [ ] **Step 6: Write minimal main.py (WebSocket smoke test)**

A bare Taipy app that proves WebSocket works on HF Spaces before any business logic:

```python
"""Taipy spike — WebSocket smoke test."""

from taipy.gui import Gui

message = "Taipy WebSocket test — if you see this, the proxy works!"
counter = 0


def on_button_click(state):
    state.counter += 1
    state.message = f"Button clicked {state.counter} times — WebSocket is live!"


page = """
# Taipy Spike — HF Spaces Smoke Test

<|{message}|text|>

<|Click me|button|on_action=on_button_click|>

Counter: <|{counter}|text|>
"""

if __name__ == "__main__":
    gui = Gui(page=page)
    gui.run(
        host="0.0.0.0",
        port=7860,
        title="Taipy Spike",
        dark_mode=True,
        use_reloader=False,
        async_mode="gevent",
    )
```

- [ ] **Step 7: Deploy smoke test to HF Spaces**

```bash
python -c "
from huggingface_hub import HfApi
api = HfApi()
api.upload_folder(
    folder_path='taipy_spike',
    repo_id='luxury-lakehouse/taipy-spike',
    repo_type='space',
)
print('Deployed')
"
```

- [ ] **Step 8: Add secrets to HF Space**

Go to `https://huggingface.co/spaces/luxury-lakehouse/taipy-spike/settings` and add:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `LAKEBASE_HOST`
- `LAKEBASE_ENDPOINT_NAME`
- `GOLD_SCHEMA`

(Same values as the Streamlit Space.)

- [ ] **Step 9: Verify WebSocket smoke test**

Open `https://huggingface.co/spaces/luxury-lakehouse/taipy-spike` in browser:
1. Page loads with the message text
2. Click the button → counter increments without full page reload
3. Open DevTools → Network tab → filter "WS" → confirm WebSocket frames (not XHR long-polling)

If WebSocket frames are NOT visible but the button still works (meaning Flask-SocketIO fell back to long-polling), this is a **partial pass** — Taipy works but without the performance benefit of WebSocket. Note the transport mode and proceed to Task 1 (the spike is still viable on long-polling; it's just slower).

**GO/NO-GO GATE:** If the page fails to load entirely (HF proxy rejects the connection, 502/504 errors, blank page), stop here and evaluate Panel. Do not proceed to Task 1.

---

## Task 1: Database Layer (Lakebase Connection)

**Files:**
- Create: `taipy_spike/src/config.py`
- Create: `taipy_spike/src/db.py`

- [ ] **Step 1: Copy config.py**

Copy `src/streamlit_app/config.py` to `taipy_spike/src/config.py`. The file is self-contained (Pydantic BaseSettings + lru_cache singleton). No changes needed to the file content.

- [ ] **Step 2: Copy db.py**

Copy `src/streamlit_app/db.py` to `taipy_spike/src/db.py`. Change one import:

```python
# Line 21: change
from streamlit_app.config import get_settings
# to
from config import get_settings
```

All other code is pure Python (psycopg2, requests, threading) — no Streamlit imports.

- [ ] **Step 3: Verify db.py works locally**

```bash
cd taipy_spike && uv run python -c "
import sys; sys.path.insert(0, 'src')
from db import execute_query, t
tbl = t('dim_competitions_synced')
df = execute_query(f'SELECT competition_id, competition_name FROM {tbl} LIMIT 5')
print(df)
"
```

Expected: DataFrame with competition names from Lakebase.

**GO/NO-GO GATE:** If the OAuth token flow fails, debug before proceeding. This is likely fixable since it's pure Python — no Taipy interaction.

---

## Task 2: Phase A — Shot Map (Minimal) + Visualization Comparison

**Files:**
- Create: `taipy_spike/src/pages/shot_map.py`
- Modify: `taipy_spike/src/main.py`

- [ ] **Step 1: Write shot_map.py with both rendering approaches**

Per spec, test both visualization approaches in Phase A:

**Approach 1:** `tgb.image` with base64-encoded PNG (high-DPI raster)
**Approach 2:** `tgb.part` with direct matplotlib Figure object

Implement both and display side-by-side so the user can visually compare. The measurable criterion: no visible rasterization blur on pitch line intersections at 1x browser zoom, 150 DPI minimum.

```python
"""Shot Map page — Phase A minimal: competition filter + pitch scatter."""

from __future__ import annotations

import base64
from io import BytesIO
from typing import Any

import pandas as pd
from mplsoccer import VerticalPitch

from db import execute_query, t


def _fetch_competitions() -> pd.DataFrame:
    tbl = t("dim_competitions_synced")
    return execute_query(
        f"SELECT DISTINCT competition_id, competition_name "  # noqa: S608
        f"FROM {tbl} ORDER BY competition_name LIMIT 50",
    )


def _fetch_shots(competition_id: int) -> pd.DataFrame:
    shots_tbl = t("fct_shots_synced")
    return execute_query(
        f"SELECT location_x, location_y, is_goal "  # noqa: S608
        f"FROM {shots_tbl} WHERE competition_id = %s "
        f"LIMIT 5000",
        (competition_id,),
    )


def _create_pitch_figure(shots: pd.DataFrame):
    """Create mplsoccer pitch figure (returns fig, ax)."""
    pitch = VerticalPitch(half=True, pitch_color="#1a1a2e", line_color="#e0e0e0")
    fig, ax = pitch.draw(figsize=(8, 10))

    if not shots.empty:
        goals = shots[shots["is_goal"] == True]  # noqa: E712
        misses = shots[shots["is_goal"] == False]  # noqa: E712

        if not misses.empty:
            pitch.scatter(
                misses["location_x"], misses["location_y"],
                ax=ax, color="#888888", s=40, alpha=0.5, zorder=2,
            )
        if not goals.empty:
            pitch.scatter(
                goals["location_x"], goals["location_y"],
                ax=ax, color="#f59e0b", s=100, alpha=0.9, zorder=3,
                edgecolors="#ffffff", linewidth=1,
            )
    return fig, ax


def _render_pitch_base64(shots: pd.DataFrame) -> str:
    """Approach 1: Render mplsoccer pitch to base64 PNG string at 150 DPI."""
    fig, _ = _create_pitch_figure(shots)
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf.seek(0)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return base64.b64encode(buf.read()).decode("utf-8")


def _render_pitch_figure(shots: pd.DataFrame):
    """Approach 2: Return raw matplotlib Figure for tgb.part."""
    fig, _ = _create_pitch_figure(shots)
    return fig


# --- State initialization ---
competitions_df: pd.DataFrame = pd.DataFrame()
selected_competition: str | None = None
competition_lov: list[str] = []
total_shots: int = 0
pitch_image: str = ""  # base64 PNG (approach 1)
pitch_figure = None  # matplotlib Figure (approach 2)


def initialize(state: Any) -> None:
    """Load competitions on app start."""
    df = _fetch_competitions()
    state.competitions_df = df
    state.competition_lov = [
        f"{r['competition_name']} ({r['competition_id']})"
        for _, r in df.iterrows()
    ]


def on_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    """Callback when competition dropdown changes."""
    if not var_value:
        return
    # Extract competition_id from "Name (id)" format
    comp_id = int(var_value.split("(")[-1].rstrip(")"))
    shots = _fetch_shots(comp_id)
    state.total_shots = len(shots)
    state.pitch_image = _render_pitch_base64(shots)
    state.pitch_figure = _render_pitch_figure(shots)


page_md = """
## Shot Map

<|{selected_competition}|selector|lov={competition_lov}|dropdown|label=Competition|on_change=on_competition_change|>

Total Shots: **<|{total_shots}|text|>**

### Approach 1: Base64 PNG (150 DPI)
<|{pitch_image}|image|label=Shot Map (base64)|>

### Approach 2: Direct Figure
<|{pitch_figure}|part|>

*Taipy spike — Phase A minimal viable test. Compare rendering quality above.*
"""
```

- [ ] **Step 2: Update main.py to use the shot map page**

```python
"""Taipy spike — Phase A: Shot Map with Lakebase + mplsoccer."""

from taipy.gui import Gui

from pages.shot_map import initialize, page_md

root_page = """
<|navbar|>

<|content|>

---
*Taipy spike — validating HF Spaces deployment*
"""

pages = {
    "/": root_page,
    "shot-map": page_md,
}

gui = Gui(pages=pages)


def on_init(state):
    initialize(state)


if __name__ == "__main__":
    gui.run(
        host="0.0.0.0",
        port=7860,
        title="Taipy Spike — Shot Map",
        dark_mode=True,
        use_reloader=False,
        async_mode="gevent",
        on_init=on_init,
    )
```

- [ ] **Step 3: Deploy Phase A to HF Spaces**

```bash
python -c "
from huggingface_hub import HfApi
api = HfApi()
api.upload_folder(
    folder_path='taipy_spike',
    repo_id='luxury-lakehouse/taipy-spike',
    repo_type='space',
)
print('Deployed')
"
```

- [ ] **Step 4: Verify Phase A**

Open `https://huggingface.co/spaces/luxury-lakehouse/taipy-spike`:
1. Competition dropdown populates from Lakebase (**proves psycopg2**)
2. Select a competition → both pitch renders appear (**proves mplsoccer**)
3. Compare Approach 1 (base64 PNG) vs Approach 2 (direct Figure) — pick the crisper one
4. Change competition → pitches update without full page reload (**proves callback model**)
5. DevTools → Network → check transport mode (WS frames or XHR polling)

**Decision:** Remove the losing rendering approach, keep the winner for Phase B.

**GO/NO-GO GATE:** If BOTH rendering approaches produce blur at 1x zoom with visible rasterization on pitch line intersections, evaluate Panel. If Lakebase connection fails, debug before proceeding.

---

## Task 3: Phase B — Full Shot Map Parity

**Files:**
- Modify: `taipy_spike/src/pages/shot_map.py`
- Create: `taipy_spike/src/style.css`

- [ ] **Step 1: Add team/player filters and xG model selector**

Extend `shot_map.py` with:
- Team dropdown (optional, populated after competition selection)
- Player dropdown (optional, populated after team selection)
- xG model radio selector: StatsBomb / Custom (Logistic) / Custom (XGBoost) — three options matching the existing Streamlit page
- Cascade: competition → team → player (changing parent resets children)

- [ ] **Step 2: Add 6 KPI metric cards with help tooltips**

Add metric bindings for: Total Shots, Goals, Total xG (with delta vs StatsBomb), Conversion Rate, xG/Shot (with delta), Brier Score (with delta, inverse color).

Inline help strings directly (no dependency on `components/glossary.py`):
```python
METRIC_HELP = {
    "Total Shots": "Number of shots in the selected scope.",
    "Goals": "Shots that resulted in a goal.",
    "Total xG": "Sum of expected goals. Higher = more dangerous shots. 1.0 = one goal expected.",
    "Conversion Rate": "Goals / Total Shots as a percentage.",
    "xG / Shot": "Average xG per shot. Higher = more dangerous average shot.",
    "Brier Score": "Prediction calibration (0 = perfect, 0.25 = coin flip). Lower is better.",
}
```

Use `tgb.metric` with tooltip via Taipy's markup.

- [ ] **Step 3: Add xG predictions join and Brier Score computation**

Port `_fetch_xg_predictions()` and `_compute_brier_score()` from `src/streamlit_app/pages/shot_map.py`. Join predictions to shots on `shot_id`. Compute comparison deltas when model != StatsBomb.

- [ ] **Step 4: Add pitch scatter with xG sizing and model coloring**

Port `plot_shot_map()` logic: scatter size proportional to xG value, color by goal/miss, amber accent for goals. Use the winning rendering approach from Phase A.

- [ ] **Step 5: Add dark theme CSS, amber accent, data freshness caption, citation footer**

Create `taipy_spike/src/style.css`:
```css
:root {
    --color-primary: #f59e0b;
}
```

Add data freshness caption (e.g., "Data refreshed every 10 minutes") and academic citation:
```
*Rathke, A. (2017). "An examination of expected goals and shot efficiency in soccer."*
```

- [ ] **Step 6: Deploy and verify Phase B**

Deploy to HF Spaces. Verify:
1. All three filters cascade correctly (competition → team → player)
2. xG model selector shows comparison deltas on Total xG, xG/Shot, Brier Score
3. All 6 metrics display with help tooltips
4. Pitch scatter sizes by xG, colors by goal
5. Dark theme with amber accent
6. Data freshness caption visible
7. Citation footer visible

**GO/NO-GO GATE:** Full feature parity with Streamlit Shot Map confirmed.

---

## Task 4: Phase C — Multi-Page + Cross-Page State

**Files:**
- Create: `taipy_spike/src/pages/match_summary.py`
- Modify: `taipy_spike/src/main.py`

- [ ] **Step 1: Write match_summary.py**

Port the Match Summary page (scorecard + metrics):
- Fetch match data from `fct_match_summary_synced`
- Display home/away scores, xG comparison, PPDA
- Reuse the competition filter from root state

- [ ] **Step 2: Add multi-page routing to main.py**

```python
pages = {
    "/": root_page,
    "shot-map": shot_map_page,
    "match-summary": match_summary_page,
}
```

Add `<|navbar|>` or `<|menu|>` element for sidebar navigation.

- [ ] **Step 3: Implement cross-page filter persistence**

Move `competition_id` and `competitions_df` to root-level state variables in `main.py`. Both pages read from root state. Competition selector appears on root page (persistent sidebar).

- [ ] **Step 4: Deploy and verify Phase C**

Deploy to HF Spaces. Verify:
1. Navigate between Shot Map and Match Summary via sidebar menu
2. Competition selection persists across navigation
3. **URL passthrough test (additional unknown from spec):** Navigate directly to `https://huggingface.co/spaces/luxury-lakehouse/taipy-spike/shot-map` — confirm it loads the Shot Map page, not a 404. If the HF proxy strips the path, evaluate Taipy's hash-based routing (`navigate(state, "shot-map")` with `#/shot-map` fragments) as a fallback.
4. No state loss on page switch

**GO/NO-GO GATE:** If URL routing fails (404 on direct navigation), try hash-based routing: Taipy supports `navigate(state, page_name)` which uses client-side routing via `window.location.hash`. If cross-page state fails, evaluate whether root-page state variables need explicit `State` object sharing. If neither fallback works, note the limitation for the migration plan (may require single-page with tab-based navigation instead of true multi-page).

---

## Task 5: Document Results and Next Steps

**Files:**
- Modify: `docs/superpowers/specs/2026-03-18-taipy-spike-design.md`

- [ ] **Step 1: Update spec with results**

Record pass/fail for each gate:
- WebSocket pre-condition: pass/fail (note transport mode: WS or long-polling)
- Phase A mplsoccer: pass/fail (note winning rendering approach)
- Phase A Lakebase: pass/fail
- Phase B feature parity: pass/fail (note any gaps)
- Phase C routing: pass/fail (note URL passthrough status)

Update the "Migration Path" section with revised effort estimates based on spike learnings.

- [ ] **Step 2: Document follow-on actions**

If the spike succeeds, the next actions are:
- **Step 0 from migration path:** Extract `db.py` and `config.py` into a shared internal package (`src/shared/` or similar) to eliminate the three-copy divergence problem before porting any more pages.
- Port remaining 10 pages one at a time.
- Add Taipy job management for training pipeline monitoring.

- [ ] **Step 3: Commit all spike code**

```bash
git add taipy_spike/ docs/superpowers/specs/2026-03-18-taipy-spike-design.md docs/superpowers/plans/2026-03-18-taipy-spike.md
git commit -m "spike: Taipy proof of concept — Phase A/B/C results"
```
