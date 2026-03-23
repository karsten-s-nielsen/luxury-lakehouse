# Taipy Spike — HF Spaces Proof of Concept

**Date:** 2026-03-18
**Branch:** `spike/taipy-proof-of-concept`
**Status:** Design approved

## Objective

Validate whether Taipy can replace Streamlit as the dashboard framework, deployed on HuggingFace Spaces Docker SDK. Three technical unknowns must pass before committing to migration:

1. **WebSocket proxy** — Taipy uses Flask-SocketIO (WebSocket) for client-server state sync. HF Spaces Docker reverse proxy must support WebSocket upgrade. **Known risk:** FastAPI WebSocket endpoints have returned HTTP 404 on HF Spaces (forum reports 2024-2025). Flask-SocketIO may fall back to long-polling if the `Upgrade: websocket` header is stripped by the proxy.
2. **mplsoccer rendering** — Pitch maps must render crisply via Taipy's visual element system, without the rasterization blur that plagues Streamlit's `st.pyplot`.
3. **Lakebase psycopg2** — The existing OAuth token + psycopg2 connection pool pattern must work inside Taipy callbacks.

## Progressive Phases

### Phase A — Minimal Viable Test

**Pre-condition: WebSocket smoke test.** Before writing any Taipy page code, deploy a minimal Flask-SocketIO echo server to the HF Space and confirm WebSocket upgrade succeeds via browser devtools (Network tab → WS frames visible). If the proxy strips the `Upgrade` header, the spike is over immediately — no Taipy page code needs to be written. The Dockerfile must use `async_mode='gevent'` and serve via `gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1`.

Once WebSocket is confirmed, prove the remaining unknowns:

- **Sidebar:** Competition dropdown populated from Lakebase `dim_competitions_synced`
- **Main area:** mplsoccer `Pitch().scatter()` of shots colored by goal/no-goal
- **One KPI card:** "Total Shots" count
- **Footer:** "Taipy spike — validating HF Spaces deployment"

Pass criteria:
- App loads on HF Spaces with WebSocket connection (not long-polling fallback)
- Filter interaction triggers callback (not full page rerun)
- Pitch map rendered at 150 DPI with no visible rasterization blur on pitch line intersections at 1x display zoom
- Data loads from Lakebase via psycopg2 pool

### Phase B — Faithful Shot Map Reproduction

Full parity with the Streamlit Shot Map page:

- All filters: competition, team (optional), player (optional)
- xG model selector (StatsBomb / Custom XGBoost) with comparison deltas on Total xG, xG/Shot, Brier Score
- 6 metric cards with help tooltips: Total Shots, Goals, Total xG, Conversion Rate, xG/Shot, Brier Score. Help tooltip strings are inlined directly (no dependency on `components/glossary.py`)
- Pitch scatter colored by xG model, sized by xG value
- Data freshness caption
- Academic citation footer (Rathke 2017)
- Dark theme with amber accent

### Phase C — Multi-Page + Cross-Page State

Prove multi-page routing and shared filter state:

- Add a second page (Match Summary — scorecard + metrics)
- Sidebar navigation menu between Shot Map and Match Summary
- Competition/match selection persists across page navigation via root-level state variables
- URL-based routing (`/shot-map`, `/match-summary`)

**Additional unknown:** HF Spaces reverse proxy URL path passthrough. Taipy's multi-page routing relies on the URL path reaching the Flask server — the proxy may strip or rewrite paths. Test: navigate to `/shot-map` by direct URL and confirm it loads the correct page without a 404.

## Architecture

```
taipy_spike/
├── Dockerfile              # python:3.10-slim@sha256:..., non-root, port 7860
├── requirements.txt        # taipy==4.1.1, psycopg2-binary==2.9.10, mplsoccer==1.6.1, etc.
├── src/
│   ├── main.py             # Taipy app entrypoint — gui.run(port=7860, host="0.0.0.0")
│   ├── db.py               # Self-contained copy of Lakebase connection layer
│   ├── config.py           # Self-contained copy of AppSettings (Pydantic)
│   └── pages/
│       ├── shot_map.py     # Shot Map page (Phase A → B)
│       └── match_summary.py  # Match Summary page (Phase C)
└── .dockerignore
```

### Database Layer

Copied from `src/streamlit_app/db.py` — self-contained, no import dependency on the Streamlit app. Same OAuth token management, psycopg2 `ThreadedConnectionPool`, and `execute_query()` helper. The `t()` table reference helper and `validate_table_name()` carry over unchanged.

**Note:** This is intentionally a throwaway copy. If the spike succeeds, the migration path (Step 0) must extract `db.py` and `config.py` into a shared internal package (`src/shared/` or similar) to eliminate the three-copy divergence problem (src/, hf_streamlit_app/, taipy_spike/).

### Taipy State Model

Replaces Streamlit's full-script rerun:

```python
# Root state variables (persist across pages)
competition_id: int | None = None
competitions_df: pd.DataFrame = pd.DataFrame()

# Page-level state (per-page callbacks update these)
shots_df: pd.DataFrame = pd.DataFrame()
total_shots: int = 0
pitch_figure: matplotlib.figure.Figure | None = None
```

Filter changes trigger `on_change` callbacks that update only the bound state variables. Only UI elements bound to changed variables re-render.

### Visualization Strategy

- **mplsoccer pitch maps:** Test two approaches in Phase A:
  1. `tgb.part(content="{pitch_figure}")` — direct matplotlib Figure object
  2. `tgb.image(content="{pitch_bytes}")` — base64-encoded PNG from `fig.savefig(buf, format='png', dpi=150)`
  Pick whichever produces crisper output. Measurable criterion: no visible rasterization blur on pitch line intersections at 1x browser zoom, 150 DPI minimum.
- **Plotly charts (Phase B):** `tgb.chart(figure="{plotly_fig}")` — native Taipy element.
- **KPI cards:** `tgb.metric(value="{total_shots}", label="Total Shots")` — available since Taipy 4.0. Pin to `taipy==4.1.1` to lock the API.

### Deployment

- HF Space: `luxury-lakehouse/taipy-spike` (Docker SDK, `cpu-basic`)
- Private visibility (unlisted — accessible by direct URL only)
- Secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `LAKEBASE_HOST`, `LAKEBASE_ENDPOINT_NAME`, `GOLD_SCHEMA`  <!-- pragma: allowlist secret -->
- Port: 7860 (HF Spaces requirement)
- Dockerfile: `gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1` for proper WebSocket support

### Dependency Pinning

All dependencies pinned to exact versions in `requirements.txt`:

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
gunicorn==23.0.0
gevent==24.11.1
gevent-websocket==0.10.1
```

## What We Explicitly Skip

- Job management / Scenario system (future, post-spike — this is Taipy's killer feature but not needed to validate the three unknowns)
- Full glossary sidebar component (Phase B adds inline help tooltip strings only)
- Cross-page filter persistence (Phase C)
- All 10 other Streamlit pages (only Shot Map + Match Summary)
- Gradio demo parity
- CI/CD for the spike (manual deployment only)

## Success Criteria

| Phase | Criteria | Decision |
|-------|----------|----------|
| A pre-condition | WebSocket smoke test passes on HF Spaces | Proceed to Phase A page code |
| A pre-condition fails | WebSocket upgrade rejected by HF proxy | Abandon Taipy, evaluate Panel |
| A passes | All 3 unknowns validated (WS + mplsoccer + Lakebase) | Proceed to B |
| A fails (mplsoccer) | Both rendering approaches produce blur | Evaluate Panel |
| A fails (Lakebase) | OAuth flow breaks in Taipy context | Debug; likely fixable since it's pure Python |
| B passes | Full Shot Map feature parity with Streamlit | Proceed to C |
| C passes | Multi-page routing + cross-page state + URL passthrough works | Plan full migration |
| C fails (URL routing) | HF proxy strips paths | Evaluate hash-based routing fallback |

## Migration Path (if spike succeeds)

0. Extract `db.py` and `config.py` into a shared internal package to eliminate copy divergence
1. Port remaining 10 pages one at a time into `taipy_spike/`
2. Rename `taipy_spike/` → `hf_taipy_app/` (or similar)
3. Add Taipy job management for training pipeline monitoring
4. Switch production HF Space from Streamlit to Taipy
5. Deprecate `hf_streamlit_app/`
