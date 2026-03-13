# Interactive Analytics Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Pitch Control visualization, DEFCON Pressure breakdown, and Pitch Control animation to the HF Space; replace the static xT seed with a data-driven Markov chain grid.

**Architecture:** Four features on one branch (`feat/interactive-analytics`). D1/D2/D4 extend the Gradio demo Space with new tabs using pre-cached Parquet data (no live DB). D3 adds a new analytics module + batch pipeline that computes xT grids from SPADL actions via value iteration, then updates the dbt seed. The pitch control module is copied into `demo_space/` as a self-contained NumPy module (no Spark deps).

**Tech Stack:** Python 3.10, Gradio 5.x, matplotlib/mplsoccer, Plotly, NumPy, pandas, PySpark (D3 pipeline), JAX (optional D3 acceleration), pytest

**Commit policy:** No incremental commits. Single commit after all features are deployed and E2E tested.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `demo_space/pitch_control.py` | Self-contained pitch control module for HF Space (copy of `src/analytics/pitch_control.py`, JAX auto-falls-back to NumPy) |
| `demo_space/data/sample_tracking.parquet` | 2 Metrica matches at 1fps (~240K rows) — match_id, player_id, team, period, frame, x, y, velocity_x, velocity_y, ball_x, ball_y |
| `demo_space/data/defcon_pressure.parquet` | Denormalized DEFCON pressure: player_name, match_label, match_id, competition_id, intercept/concede/disturb/deter pressure + counts |
| `notebooks/export_demo_data.py` | Databricks notebook: exports sample tracking + DEFCON pressure Parquet to UC Volume |
| `src/analytics/expected_threat.py` | Markov chain xT grid computation — pure NumPy with optional JAX acceleration |
| `src/tests/test_expected_threat.py` | Tests for expected_threat module |
| `src/ingestion/expected_threat.py` | Batch pipeline: reads SPADL actions from gold, computes per-competition + global xT grids, writes to Delta |

### Modified files

| File | Changes |
|------|---------|
| `demo_space/app.py` | Add 3 tabs: Pitch Control, DEFCON Pressure, Pitch Control Animation |
| `demo_space/requirements.txt` | Add `plotly>=5.18.0` |
| `demo_space/README.md` | Update tab descriptions and data source attributions |
| `pyproject.toml` | Add `compute_expected_threat` entry point |
| `dbt_project/seeds/expected_threat_grid.csv` | Replace static Karun Singh values with data-driven global grid |
| `TODO.md` | Mark D1-D4 complete |

---

## Chunk 1: D1 + D2 — HF Space Tabs

### Task 1: Copy pitch_control.py to demo_space

**Files:**
- Create: `demo_space/pitch_control.py`
- Source: `src/analytics/pitch_control.py`

The module has zero internal imports (only numpy, pandas, optional jax). It can be copied verbatim. The HF Space doesn't have JAX installed, so `_USE_JAX` will be `False` and all NumPy paths are used automatically.

- [ ] **Step 1: Copy the module**

```bash
cp src/analytics/pitch_control.py demo_space/pitch_control.py
```

- [ ] **Step 2: Verify the module imports standalone**

```bash
cd demo_space && python -c "from pitch_control import compute_pitch_control_frame, PitchControlParams; print('OK')"
```

Expected: `OK` (no import errors, JAX fallback silent)

---

### Task 2: Data export notebook

**Files:**
- Create: `notebooks/export_demo_data.py`

This Databricks notebook exports two Parquet files to UC Volume for manual download into `demo_space/data/`.

- [ ] **Step 1: Write the export notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Export Demo Data for HF Space
# MAGIC
# MAGIC Exports sample tracking data (D1) and DEFCON pressure data (D2) to UC Volume.
# MAGIC Download from Volume to `demo_space/data/` locally after running.

# COMMAND ----------

CATALOG = "soccer_analytics"
VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/libs/hf_export/demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## D1: Sample Tracking Data (2 Metrica matches at 1fps)

# COMMAND ----------

tracking_df = spark.sql(f"""
    SELECT
        match_id,
        period,
        frame,
        timestamp_seconds,
        player_id,
        team,
        x,
        y,
        velocity_x,
        velocity_y,
        ball_x,
        ball_y,
        speed_ms,
        source_provider,
        frame_rate
    FROM {CATALOG}.dev_gold.fct_tracking_frames
    WHERE source_provider = 'metrica'
      AND MOD(frame, frame_rate) = 0  -- 1fps sampling
    ORDER BY match_id, period, frame, player_id
""")

print(f"Tracking rows (1fps): {tracking_df.count():,}")

tracking_df.coalesce(1).write.mode("overwrite").parquet(f"{VOLUME_PATH}/sample_tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D2: DEFCON Pressure (denormalized with player names + match labels)

# COMMAND ----------

pressure_df = spark.sql(f"""
    SELECT
        dp.player_id,
        p.player_name,
        dp.match_id,
        dp.competition_id,
        dp.season_id,
        dp.data_source,
        CONCAT(ms.home_team, ' ', ms.home_score, '-', ms.away_score, ' ', ms.away_team) AS match_label,
        dp.total_pressure,
        dp.total_defensive_actions,
        dp.intercept_pressure,
        dp.concede_pressure,
        dp.disturb_pressure,
        dp.deter_pressure,
        dp.intercept_count,
        dp.concede_count,
        dp.disturb_count,
        dp.deter_count
    FROM {CATALOG}.dev_gold.fct_defcon_pressure dp
    LEFT JOIN {CATALOG}.dev_gold.dim_players p
        ON dp.player_id = p.canonical_player_id
    LEFT JOIN {CATALOG}.dev_gold.fct_match_summary ms
        ON dp.match_id = ms.match_id
    ORDER BY dp.player_id, dp.match_id
""")

print(f"Pressure rows: {pressure_df.count():,}")

pressure_df.coalesce(1).write.mode("overwrite").parquet(f"{VOLUME_PATH}/defcon_pressure")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Instructions
# MAGIC
# MAGIC From local machine:
# MAGIC ```bash
# MAGIC # Tracking data
# MAGIC databricks fs cp "dbfs:/Volumes/soccer_analytics/bronze/libs/hf_export/demo/sample_tracking/" \
# MAGIC   demo_space/data/sample_tracking.parquet --recursive --profile OAUTH
# MAGIC
# MAGIC # DEFCON pressure
# MAGIC databricks fs cp "dbfs:/Volumes/soccer_analytics/bronze/libs/hf_export/demo/defcon_pressure/" \
# MAGIC   demo_space/data/defcon_pressure.parquet --recursive --profile OAUTH
# MAGIC ```
# MAGIC
# MAGIC Note: Spark writes a directory with `part-*.parquet` files. After download,
# MAGIC consolidate into a single file:
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC df = pd.read_parquet("demo_space/data/sample_tracking.parquet")
# MAGIC df.to_parquet("demo_space/data/sample_tracking.parquet", index=False)
# MAGIC ```
```

- [ ] **Step 2: Run the notebook on Databricks**

Upload to workspace and execute. Verify row counts:
- Tracking: ~240K rows (3 Metrica matches × ~80K rows/match at 1fps)
- Pressure: ~7K rows

- [ ] **Step 3: Download Parquet files to local**

```bash
# After Spark export, consolidate and place in demo_space/data/
# (exact commands in notebook Download Instructions section)
```

---

### Task 3: Pitch Control Gradio tab (D1)

**Files:**
- Modify: `demo_space/app.py`

- [ ] **Step 1: Add tracking data loading at module level**

After the existing data loading block (line ~37), add:

```python
tracking_df = _load_parquet("sample_tracking.parquet")

# Coerce tracking numeric columns
_TRACKING_NUMERIC = ["x", "y", "velocity_x", "velocity_y", "ball_x", "ball_y", "speed_ms"]
for _col in _TRACKING_NUMERIC:
    if not tracking_df.empty and _col in tracking_df.columns:
        tracking_df[_col] = pd.to_numeric(tracking_df[_col], errors="coerce")
```

- [ ] **Step 2: Add pitch control import**

```python
from pitch_control import PitchControlParams, compute_pitch_control_frame
```

- [ ] **Step 3: Add helper functions for pitch control tab**

```python
# ---------------------------------------------------------------------------
# Pitch Control (D1)
# ---------------------------------------------------------------------------

_PC_PARAMS = PitchControlParams()


def _get_tracking_matches() -> list[str]:
    """Get unique match IDs from tracking data."""
    if tracking_df.empty:
        return []
    return sorted(tracking_df["match_id"].astype(str).unique().tolist())


def _get_frame_range(match_id: str, period: int) -> tuple[int, int]:
    """Get min/max frame numbers for a match and period."""
    mask = (tracking_df["match_id"].astype(str) == match_id) & (tracking_df["period"] == period)
    subset = tracking_df[mask]
    if subset.empty:
        return 0, 0
    frames = subset["frame"].astype(int)
    return int(frames.min()), int(frames.max())


def _get_frame_rate(match_id: str) -> int:
    """Get frame rate for a match (used as slider step for 1-second increments)."""
    mask = tracking_df["match_id"].astype(str) == match_id
    subset = tracking_df[mask]
    if subset.empty or "frame_rate" not in subset.columns:
        return 25
    return int(subset["frame_rate"].iloc[0])


def create_pitch_control_plot(
    match_id: str | None,
    period: int,
    frame: int,
    show_velocity: bool,
) -> plt.Figure:
    """Render pitch control heatmap for a single frame."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color="#1a1a2e", line_color="#e0e0e0")
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.set_facecolor("#1a1a2e")

    if tracking_df.empty or not match_id:
        ax.text(60, 40, "No tracking data loaded.", ha="center", va="center",
                color="white", fontsize=14)
        return fig

    # Filter to exact frame
    mask = (
        (tracking_df["match_id"].astype(str) == match_id)
        & (tracking_df["period"] == period)
        & (tracking_df["frame"] == frame)
    )
    frame_data = tracking_df[mask].copy()

    if frame_data.empty:
        ax.text(60, 40, "No data for this frame.", ha="center", va="center",
                color="white", fontsize=14)
        return fig

    # Build players DataFrame for pitch control model
    players = frame_data[["player_id", "team", "x", "y", "velocity_x", "velocity_y"]].copy()
    players = players.dropna(subset=["x", "y"])

    # Compute pitch control surface
    grid_x, grid_y, surface = compute_pitch_control_frame(players, _PC_PARAMS)

    # Plot heatmap (RdBu: 1.0=home/blue, 0.0=away/red, 0.5=white/contested)
    cmap = plt.get_cmap("RdBu")
    ax.imshow(
        surface,
        extent=[float(grid_x[0]), float(grid_x[-1]),
                float(grid_y[0]), float(grid_y[-1])],
        origin="lower",
        cmap=cmap,
        vmin=0.0,
        vmax=1.0,
        alpha=0.5,
        aspect="auto",
        zorder=1,
        interpolation="bilinear",
    )

    # Draw players
    home = players[players["team"] == "home"]
    away = players[players["team"] == "away"]
    pitch.scatter(home["x"], home["y"], ax=ax, c="#457b9d", s=80,
                  edgecolors="white", linewidth=0.5, zorder=3, label="Home")
    pitch.scatter(away["x"], away["y"], ax=ax, c="#e63946", s=80,
                  edgecolors="white", linewidth=0.5, zorder=3, label="Away")

    # Ball
    ball_x = frame_data["ball_x"].iloc[0]
    ball_y = frame_data["ball_y"].iloc[0]
    if pd.notna(ball_x) and pd.notna(ball_y):
        ax.plot(ball_x, ball_y, "h", color="#f4d03f", markersize=12,
                markeredgecolor="black", markeredgewidth=1.0, zorder=4)

    # Velocity arrows
    if show_velocity:
        for _, row in players.iterrows():
            vx, vy = row.get("velocity_x", 0), row.get("velocity_y", 0)
            if pd.notna(vx) and pd.notna(vy) and (abs(vx) + abs(vy)) > 0.1:
                color = "#457b9d" if row["team"] == "home" else "#e63946"
                ax.arrow(float(row["x"]), float(row["y"]),
                         float(vx) * 2.0, float(vy) * 2.0,
                         head_width=0.8, head_length=0.5, fc=color, ec=color,
                         alpha=0.7, zorder=3)

    # Stats
    home_ctrl = float(np.mean(surface))  # overall mean: 1.0=full home, 0.0=full away
    away_ctrl = 1.0 - home_ctrl
    ts = frame_data["timestamp_seconds"].iloc[0] if "timestamp_seconds" in frame_data.columns else 0
    minutes = int(ts // 60)
    seconds = int(ts % 60)

    ax.set_title(
        f"Pitch Control \u2014 Period {period}, {minutes}:{seconds:02d} "
        f"(Home {home_ctrl:.0%} / Away {away_ctrl:.0%})",
        color="white", fontsize=13, pad=8,
    )
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e",
              edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig
```

- [ ] **Step 4: Add the Pitch Control tab in the Blocks layout**

After the "Pass Quality" tab block and before the footer Markdown, add:

```python
    with gr.Tab("Pitch Control"):
        gr.Markdown(
            "Physics-based pitch control (Spearman 2017) computed in real-time from tracking data.\n\n"
            "*Blue = home team control, red = away team control, white = contested. "
            "Sample tracking data from Metrica Sports open data (25fps, displayed at 1fps).*"
        )
        _pc_matches = _get_tracking_matches()
        _pc_match_choices = [(f"Match {m}", m) for m in _pc_matches] if _pc_matches else []
        _default_match = _pc_matches[0] if _pc_matches else None

        with gr.Row():
            pc_match = gr.Dropdown(
                choices=_pc_match_choices, value=_default_match,
                label="Match", interactive=True,
            )
            pc_period = gr.Radio([1, 2], value=1, label="Period")
        pc_frame = gr.Slider(
            minimum=0, maximum=1000, step=1, value=0, label="Frame",
        )
        pc_velocity = gr.Checkbox(value=False, label="Show velocity arrows")
        pc_plot = gr.Plot(label="Pitch Control")

        def _update_frame_slider(match_id, period):
            if not match_id:
                return gr.Slider(minimum=0, maximum=1000, step=1, value=0)
            lo, hi = _get_frame_range(match_id, period)
            fps = _get_frame_rate(match_id)
            return gr.Slider(minimum=lo, maximum=hi, step=fps, value=lo)

        pc_match.change(fn=_update_frame_slider, inputs=[pc_match, pc_period], outputs=[pc_frame])
        pc_period.change(fn=_update_frame_slider, inputs=[pc_match, pc_period], outputs=[pc_frame])

        _pc_inputs = [pc_match, pc_period, pc_frame, pc_velocity]
        pc_frame.release(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
        pc_velocity.change(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
        pc_match.change(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
        pc_period.change(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
```

- [ ] **Step 5: Test locally**

```bash
cd demo_space && python app.py
```

Open `http://localhost:7860`, click the Pitch Control tab, select a match, drag the frame slider. Verify:
- Heatmap renders (blue/red/white)
- Players are plotted (blue dots home, red dots away)
- Ball is visible (yellow hexagon)
- Velocity arrows toggle works
- Frame slider updates the visualization

---

### Task 4: DEFCON Pressure Gradio tab (D2)

**Files:**
- Modify: `demo_space/app.py`
- Modify: `demo_space/requirements.txt`

- [ ] **Step 1: Add plotly to requirements.txt**

```
plotly>=5.18.0
```

- [ ] **Step 2: Add plotly import and DEFCON data loading**

At the top of `app.py`:

```python
import plotly.express as px
```

After the tracking data loading block:

```python
pressure_df = _load_parquet("defcon_pressure.parquet")
```

- [ ] **Step 3: Add helper functions for DEFCON Pressure tab**

```python
# ---------------------------------------------------------------------------
# DEFCON Pressure Breakdown (D2)
# ---------------------------------------------------------------------------


def _search_pressure_players(query: str) -> gr.Dropdown:
    """Search for players in the DEFCON pressure data."""
    if pressure_df.empty or not query or len(query) < 2:
        return gr.Dropdown(choices=[], value=None, label="Select player")
    matches = pressure_df[
        pressure_df["player_name"].astype(str).str.contains(query, case=False, na=False)
    ]
    if matches.empty:
        return gr.Dropdown(choices=[], value=None, label="Select player",
                           info="No matches found")
    # Unique players, sorted
    players = sorted(matches["player_name"].dropna().unique().tolist())[:20]
    return gr.Dropdown(
        choices=players, value=players[0] if len(players) == 1 else None,
        label="Select player", info=f"{len(players)} player(s) found",
    )


def create_pressure_chart(player_name: str | None):
    """Create a Plotly grouped bar chart of DEFCON pressure breakdown by match."""
    if pressure_df.empty or not player_name:
        # Return an empty matplotlib figure as fallback
        fig_mpl, ax = plt.subplots(figsize=(10, 5))
        fig_mpl.set_facecolor("#1a1a2e")
        ax.set_facecolor("#1a1a2e")
        ax.text(0.5, 0.5, "Select a player to view pressure breakdown.",
                ha="center", va="center", color="white", fontsize=14,
                transform=ax.transAxes)
        ax.set_xticks([])
        ax.set_yticks([])
        return fig_mpl

    player_data = pressure_df[pressure_df["player_name"] == player_name].copy()
    if player_data.empty:
        fig_mpl, ax = plt.subplots(figsize=(10, 5))
        fig_mpl.set_facecolor("#1a1a2e")
        ax.set_facecolor("#1a1a2e")
        ax.text(0.5, 0.5, f"No pressure data for {player_name}.",
                ha="center", va="center", color="white", fontsize=14,
                transform=ax.transAxes)
        return fig_mpl

    # Melt for grouped bar chart
    id_cols = ["match_label", "match_id"]
    value_cols = ["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"]
    melted = player_data[id_cols + value_cols].melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="credit_type",
        value_name="pressure",
    )

    # Clean labels
    melted["credit_type"] = melted["credit_type"].str.replace("_pressure", "").str.title()

    fig = px.bar(
        melted,
        x="match_label",
        y="pressure",
        color="credit_type",
        barmode="group",
        title=f"DEFCON Pressure Breakdown \u2014 {player_name}",
        labels={"pressure": "Pressure Value", "match_label": "Match", "credit_type": "Credit Type"},
        color_discrete_map={
            "Intercept": "#e63946",
            "Concede": "#f4a261",
            "Disturb": "#457b9d",
            "Deter": "#2a9d8f",
        },
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font_color="#e0e0e0",
        xaxis_tickangle=-45,
        legend_title_text="Credit Type",
        margin=dict(b=120),
    )
    return fig
```

- [ ] **Step 4: Add the DEFCON Pressure tab in the Blocks layout**

After the Pitch Control tab:

```python
    with gr.Tab("DEFCON Pressure"):
        gr.Markdown(
            "Defensive contribution breakdown per match using the DEFCON framework.\n\n"
            "*Each defensive action is classified as Intercept (direct recovery), "
            "Concede (goal-side positioning), Disturb (proximity pressure), or "
            "Deter (cone blocking). Data from StatsBomb 360 freeze frames.*"
        )
        with gr.Row():
            pressure_search = gr.Textbox(
                label="Search player", placeholder="e.g. Van Dijk, Kante..."
            )
        pressure_dropdown = gr.Dropdown(
            choices=[], label="Select player", interactive=True,
        )
        pressure_chart = gr.Plot(label="Pressure Breakdown")

        pressure_search.change(
            fn=_search_pressure_players,
            inputs=[pressure_search],
            outputs=[pressure_dropdown],
        )
        pressure_dropdown.change(
            fn=create_pressure_chart,
            inputs=[pressure_dropdown],
            outputs=[pressure_chart],
        )
```

- [ ] **Step 5: Test locally**

```bash
cd demo_space && pip install plotly && python app.py
```

Open Pitch Control tab: verify heatmap. Open DEFCON Pressure tab: search a player name, verify grouped bar chart renders with 4 colored bars per match.

---

## Chunk 2: D3 — Dynamic xT Grid

### Task 5: Expected threat analytics module

**Files:**
- Create: `src/analytics/expected_threat.py`

This module computes xT grids from SPADL action data using the Markov chain value iteration algorithm (Karun Singh 2018). Supports arbitrary grid sizes. JAX-accelerated when available (useful for dense grids like 104x68), NumPy fallback for standard 12x8.

- [ ] **Step 1: Write the analytics module**

```python
"""Expected Threat (xT) grid computation via Markov chain value iteration.

Replaces the static 12x8 Karun Singh seed with data-driven transition
probabilities computed from SPADL pass/shot events.

Reference: Karun Singh (2018) "Introducing Expected Threat (xT)"
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False

# SPADL action types that represent ball movement (transitions)
_MOVE_TYPES = frozenset({
    "pass",
    "cross",
    "throw_in",
    "freekick_crossed",
    "freekick_short",
    "corner_crossed",
    "corner_short",
    "take_on",
    "dribble",
    "goalkick",
    "clearance",
})

# SPADL action types that represent shots
_SHOT_TYPES = frozenset({"shot", "shot_penalty", "shot_freekick"})


@dataclass(frozen=True)
class ExpectedThreatParams:
    """Parameters for xT grid computation."""

    n_zones_x: int = 12
    n_zones_y: int = 8
    pitch_length: float = 105.0  # SPADL coordinates (meters)
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-6


def _assign_zones(
    x: np.ndarray,
    y: np.ndarray,
    params: ExpectedThreatParams,
) -> np.ndarray:
    """Assign (x, y) coordinates to flat zone indices.

    Returns 1-D array of zone indices in [0, n_zones_x * n_zones_y).
    """
    zone_w = params.pitch_length / params.n_zones_x
    zone_h = params.pitch_width / params.n_zones_y
    zx = np.clip((x / zone_w).astype(int), 0, params.n_zones_x - 1)
    zy = np.clip((y / zone_h).astype(int), 0, params.n_zones_y - 1)
    return zx * params.n_zones_y + zy


def _build_transition_matrix(
    start_zones: np.ndarray,
    end_zones: np.ndarray,
    n_zones: int,
) -> np.ndarray:
    """Build row-normalized transition matrix from successful move events.

    Returns (n_zones, n_zones) matrix where T[i, j] = P(move to j | in i, move).
    """
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)
    row_sums = transition.sum(axis=1, keepdims=True)
    row_sums = np.maximum(row_sums, 1.0)
    return transition / row_sums


def _value_iteration_numpy(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iterations: int,
    tolerance: float,
) -> tuple[np.ndarray, int]:
    """Run value iteration: xT = s*g + m*(T @ xT).

    Returns (xT vector, iterations used).
    """
    xt = np.zeros_like(shot_prob)
    for i in range(max_iterations):
        xt_new = shot_prob * goal_prob + move_prob * (transition @ xt)
        delta = float(np.max(np.abs(xt_new - xt)))
        xt = xt_new
        if delta < tolerance:
            return xt, i + 1
    return xt, max_iterations


if _USE_JAX:

    def _value_iteration_jax(
        shot_prob: np.ndarray,
        goal_prob: np.ndarray,
        move_prob: np.ndarray,
        transition: np.ndarray,
        max_iterations: int,
        tolerance: float,
    ) -> tuple[np.ndarray, int]:
        """JAX-accelerated value iteration for dense grids."""
        s = jnp.asarray(shot_prob)
        g = jnp.asarray(goal_prob)
        m = jnp.asarray(move_prob)
        t_mat = jnp.asarray(transition)

        @jax.jit
        def _step(xt: jax.Array) -> jax.Array:
            return s * g + m * (t_mat @ xt)

        xt = jnp.zeros_like(s)
        for i in range(max_iterations):
            xt_new = _step(xt)
            delta = float(jnp.max(jnp.abs(xt_new - xt)))
            xt = xt_new
            if delta < tolerance:
                return np.asarray(xt), i + 1
        return np.asarray(xt), max_iterations


def compute_expected_threat_grid(
    actions_df: pd.DataFrame,
    params: ExpectedThreatParams | None = None,
) -> np.ndarray:
    """Compute an xT grid from SPADL action data via Markov chain value iteration.

    Args:
        actions_df: SPADL actions with columns: type_name, result_name,
            start_x, start_y, end_x, end_y. Coordinates in SPADL 105x68m.
        params: Grid and convergence parameters. Defaults if None.

    Returns:
        np.ndarray of shape (n_zones_x, n_zones_y) with xT values.
        Grid orientation: [0, 0] = own-goal bottom-left, [11, 7] = opponent
        goal top-right. Matches the dbt seed CSV layout.
    """
    if params is None:
        params = ExpectedThreatParams()

    n_zones = params.n_zones_x * params.n_zones_y

    # Classify events
    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names])
    is_shot = np.array([t in _SHOT_TYPES for t in type_names])
    is_success = result_names == "success"

    # Assign zones
    start_x = np.asarray(actions_df["start_x"], dtype=np.float64)
    start_y = np.asarray(actions_df["start_y"], dtype=np.float64)
    end_x = np.asarray(actions_df["end_x"], dtype=np.float64)
    end_y = np.asarray(actions_df["end_y"], dtype=np.float64)

    start_zones = _assign_zones(start_x, start_y, params)
    end_zones = _assign_zones(end_x, end_y, params)

    # Per-zone counts
    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(
        start_zones[is_shot & is_success], minlength=n_zones
    ).astype(np.float64)

    # Probabilities per zone
    safe_total = np.maximum(total_per_zone, 1.0)
    shot_prob = shots_per_zone / safe_total
    goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
    move_prob = 1.0 - shot_prob

    # Transition matrix (successful moves only)
    successful_moves = is_move & is_success
    transition = _build_transition_matrix(
        start_zones[successful_moves],
        end_zones[successful_moves],
        n_zones,
    )

    # Value iteration
    use_jax = _USE_JAX and n_zones > 200  # JAX overhead not worth it for small grids
    if use_jax:
        xt_flat, _iters = _value_iteration_jax(
            shot_prob, goal_prob, move_prob, transition,
            params.max_iterations, params.tolerance,
        )
    else:
        xt_flat, _iters = _value_iteration_numpy(
            shot_prob, goal_prob, move_prob, transition,
            params.max_iterations, params.tolerance,
        )

    return xt_flat.reshape(params.n_zones_x, params.n_zones_y)


def grid_to_dataframe(
    grid: np.ndarray,
    competition_id: str | None = None,
) -> pd.DataFrame:
    """Convert an xT grid array to a DataFrame matching the dbt seed schema.

    Args:
        grid: (n_zones_x, n_zones_y) array of xT values.
        competition_id: Optional competition identifier.

    Returns:
        DataFrame with columns: zone_x, zone_y, xt_value
        (and optionally competition_id).
    """
    n_x, n_y = grid.shape
    rows: list[dict[str, object]] = []
    for zx in range(n_x):
        for zy in range(n_y):
            row: dict[str, object] = {
                "zone_x": zx,
                "zone_y": zy,
                "xt_value": round(float(grid[zx, zy]), 5),
            }
            if competition_id is not None:
                row["competition_id"] = competition_id
            rows.append(row)
    return pd.DataFrame(rows)
```

- [ ] **Step 2: Verify module imports**

```bash
uv run python -c "from analytics.expected_threat import compute_expected_threat_grid, ExpectedThreatParams; print('OK')"
```

---

### Task 6: Expected threat tests

**Files:**
- Create: `src/tests/test_expected_threat.py`

- [ ] **Step 1: Write tests**

```python
"""Tests for the Expected Threat (xT) Markov chain computation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.expected_threat import (
    ExpectedThreatParams,
    _MOVE_TYPES,
    _SHOT_TYPES,
    _assign_zones,
    _build_transition_matrix,
    _value_iteration_numpy,
    compute_expected_threat_grid,
    grid_to_dataframe,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_actions(
    types: list[str],
    results: list[str],
    start_positions: list[tuple[float, float]],
    end_positions: list[tuple[float, float]],
) -> pd.DataFrame:
    """Build a synthetic SPADL actions DataFrame."""
    return pd.DataFrame({
        "type_name": types,
        "result_name": results,
        "start_x": [p[0] for p in start_positions],
        "start_y": [p[1] for p in start_positions],
        "end_x": [p[0] for p in end_positions],
        "end_y": [p[1] for p in end_positions],
    })


_DEFAULT_PARAMS = ExpectedThreatParams()


# ---------------------------------------------------------------------------
# Zone assignment
# ---------------------------------------------------------------------------

class TestAssignZones:
    """Test zone assignment logic."""

    def test_origin_maps_to_zone_zero(self) -> None:
        zones = _assign_zones(np.array([0.0]), np.array([0.0]), _DEFAULT_PARAMS)
        assert zones[0] == 0

    def test_pitch_center(self) -> None:
        zones = _assign_zones(np.array([52.5]), np.array([34.0]), _DEFAULT_PARAMS)
        # zone_x = int(52.5 / 8.75) = 6, zone_y = int(34.0 / 8.5) = 4
        expected = 6 * 8 + 4
        assert zones[0] == expected

    def test_clamps_to_max_zone(self) -> None:
        zones = _assign_zones(np.array([105.0]), np.array([68.0]), _DEFAULT_PARAMS)
        expected = 11 * 8 + 7  # max zone
        assert zones[0] == expected

    def test_negative_coords_clamp_to_zero(self) -> None:
        zones = _assign_zones(np.array([-1.0]), np.array([-1.0]), _DEFAULT_PARAMS)
        assert zones[0] == 0


# ---------------------------------------------------------------------------
# Transition matrix
# ---------------------------------------------------------------------------

class TestBuildTransitionMatrix:
    """Test transition matrix construction."""

    def test_row_normalized(self) -> None:
        starts = np.array([0, 0, 0, 1])
        ends = np.array([1, 2, 1, 0])
        t_mat = _build_transition_matrix(starts, ends, 3)
        row_sums = t_mat.sum(axis=1)
        np.testing.assert_allclose(row_sums[0], 1.0)
        np.testing.assert_allclose(row_sums[1], 1.0)

    def test_empty_row_stays_zero(self) -> None:
        starts = np.array([0])
        ends = np.array([1])
        t_mat = _build_transition_matrix(starts, ends, 3)
        # Zone 2 has no events, row should be all zeros (divided by 1.0 floor)
        np.testing.assert_allclose(t_mat[2], 0.0)

    def test_transition_counts(self) -> None:
        starts = np.array([0, 0, 0])
        ends = np.array([1, 1, 2])
        t_mat = _build_transition_matrix(starts, ends, 3)
        np.testing.assert_allclose(t_mat[0, 1], 2 / 3)
        np.testing.assert_allclose(t_mat[0, 2], 1 / 3)


# ---------------------------------------------------------------------------
# Value iteration
# ---------------------------------------------------------------------------

class TestValueIteration:
    """Test the value iteration convergence."""

    def test_converges_simple_case(self) -> None:
        """A zone with only shots should have xT = shot_prob * goal_prob."""
        s = np.array([1.0, 0.0])  # zone 0: always shoots
        g = np.array([0.5, 0.0])  # zone 0: 50% conversion
        m = np.array([0.0, 1.0])  # zone 1: always moves
        t_mat = np.array([[0.0, 0.0], [1.0, 0.0]])  # zone 1 → zone 0
        xt, iters = _value_iteration_numpy(s, g, m, t_mat, 100, 1e-6)
        np.testing.assert_allclose(xt[0], 0.5, atol=1e-6)  # shot_prob * goal_prob
        np.testing.assert_allclose(xt[1], 0.5, atol=1e-6)  # transitions to zone 0
        assert iters < 100

    def test_zero_actions_yields_zero_xt(self) -> None:
        s = np.zeros(4)
        g = np.zeros(4)
        m = np.ones(4)
        t_mat = np.eye(4) * 0.0
        xt, _ = _value_iteration_numpy(s, g, m, t_mat, 100, 1e-6)
        np.testing.assert_allclose(xt, 0.0)


# ---------------------------------------------------------------------------
# End-to-end grid computation
# ---------------------------------------------------------------------------

class TestComputeExpectedThreatGrid:
    """Test the full xT computation pipeline."""

    def test_output_shape(self) -> None:
        params = ExpectedThreatParams(n_zones_x=12, n_zones_y=8)
        actions = _make_actions(
            types=["pass", "pass", "shot"],
            results=["success", "success", "success"],
            start_positions=[(10, 10), (50, 34), (95, 34)],
            end_positions=[(50, 34), (95, 34), (100, 34)],
        )
        grid = compute_expected_threat_grid(actions, params)
        assert grid.shape == (12, 8)

    def test_penalty_area_highest(self) -> None:
        """With many goals from the attacking zone, that zone should have highest xT."""
        params = ExpectedThreatParams(n_zones_x=4, n_zones_y=2)
        # 50 shots from zone (3, 0), 40 goals — 80% conversion
        actions = _make_actions(
            types=["shot"] * 50 + ["pass"] * 20,
            results=["success"] * 40 + ["fail"] * 10 + ["success"] * 20,
            start_positions=[(100, 10)] * 50 + [(30, 10)] * 20,
            end_positions=[(105, 34)] * 50 + [(100, 10)] * 20,
        )
        grid = compute_expected_threat_grid(actions, params)
        # Zone (3, 0) should have the highest xT
        assert grid[3, 0] == grid.max()
        assert grid[3, 0] > 0.5

    def test_all_values_non_negative(self) -> None:
        params = ExpectedThreatParams(n_zones_x=4, n_zones_y=2)
        actions = _make_actions(
            types=["pass", "shot", "pass", "dribble"],
            results=["success", "fail", "success", "success"],
            start_positions=[(10, 10), (90, 34), (50, 50), (70, 34)],
            end_positions=[(50, 34), (100, 34), (70, 34), (90, 34)],
        )
        grid = compute_expected_threat_grid(actions, params)
        assert np.all(grid >= 0.0)

    def test_empty_actions(self) -> None:
        actions = pd.DataFrame({
            "type_name": pd.Series(dtype=str),
            "result_name": pd.Series(dtype=str),
            "start_x": pd.Series(dtype=float),
            "start_y": pd.Series(dtype=float),
            "end_x": pd.Series(dtype=float),
            "end_y": pd.Series(dtype=float),
        })
        grid = compute_expected_threat_grid(actions)
        assert grid.shape == (12, 8)
        np.testing.assert_allclose(grid, 0.0)

    def test_ignored_action_types(self) -> None:
        """Fouls, tackles, keeper actions should not contribute to transitions."""
        actions = _make_actions(
            types=["foul", "tackle", "keeper_save"],
            results=["success", "success", "success"],
            start_positions=[(50, 34), (60, 34), (5, 34)],
            end_positions=[(50, 34), (60, 34), (5, 34)],
        )
        grid = compute_expected_threat_grid(actions)
        # No valid moves or shots → zero xT
        np.testing.assert_allclose(grid, 0.0)


# ---------------------------------------------------------------------------
# Grid to DataFrame conversion
# ---------------------------------------------------------------------------

class TestGridToDataframe:
    """Test the grid-to-DataFrame conversion."""

    def test_seed_schema(self) -> None:
        grid = np.ones((12, 8)) * 0.1
        df = grid_to_dataframe(grid)
        assert list(df.columns) == ["zone_x", "zone_y", "xt_value"]
        assert len(df) == 96

    def test_with_competition_id(self) -> None:
        grid = np.ones((4, 2)) * 0.5
        df = grid_to_dataframe(grid, competition_id="7")
        assert "competition_id" in df.columns
        assert df["competition_id"].iloc[0] == "7"

    def test_round_trip_values(self) -> None:
        grid = np.array([[0.12345, 0.67890], [0.11111, 0.99999]])
        df = grid_to_dataframe(grid)
        # Values rounded to 5 decimals
        assert df[df["zone_x"] == 0]["xt_value"].iloc[0] == 0.12345
```

- [ ] **Step 2: Run tests**

```bash
uv run pytest src/tests/test_expected_threat.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Run linting**

```bash
uv run ruff check src/analytics/expected_threat.py src/tests/test_expected_threat.py
uv run pyright src/analytics/expected_threat.py src/tests/test_expected_threat.py
```

Expected: zero violations.

---

### Task 7: Expected threat batch pipeline

**Files:**
- Create: `src/ingestion/expected_threat.py`
- Modify: `pyproject.toml` (add entry point)

- [ ] **Step 1: Write the batch pipeline**

```python
"""Expected Threat batch pipeline — computes xT grids from SPADL action data.

Reads SPADL actions from the gold mart (fct_action_values), computes per-competition
xT grids via Markov chain value iteration, and writes results to Delta.

Also computes a global grid (all competitions) for updating the dbt seed CSV.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from analytics.expected_threat import (
    ExpectedThreatParams,
    compute_expected_threat_grid,
    grid_to_dataframe,
)
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "expected_threat_grids"
_GOLD_TABLE = "fct_action_values"

# SPADL action types relevant to xT
_RELEVANT_TYPES = (
    "pass", "cross", "throw_in", "freekick_crossed", "freekick_short",
    "corner_crossed", "corner_short", "take_on", "dribble", "goalkick",
    "clearance", "shot", "shot_penalty", "shot_freekick",
)

logger = logging.getLogger(__name__)


def _load_actions(spark: SparkSession, catalog: str) -> pd.DataFrame:
    """Load SPADL actions from gold mart, filtered to xT-relevant types."""
    types_sql = ", ".join(f"'{t}'" for t in _RELEVANT_TYPES)
    query = f"""
        SELECT
            competition_id,
            type_name,
            result_name,
            start_x,
            start_y,
            end_x,
            end_y
        FROM {catalog}.dev_gold.{_GOLD_TABLE}
        WHERE type_name IN ({types_sql})
    """  # noqa: S608
    return spark.sql(query).toPandas()  # type: ignore[union-attr]


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    log: logging.Logger,
) -> None:
    """Compute per-competition and global xT grids, write to Delta."""
    params = ExpectedThreatParams()

    log.info("Loading SPADL actions from %s.dev_gold.%s", catalog, _GOLD_TABLE)
    actions_df = _load_actions(spark, catalog)
    log.info("Loaded %d relevant actions", len(actions_df))

    if actions_df.empty:
        log.warning("No actions found — skipping xT computation")
        return

    # Per-competition grids
    all_grids: list[pd.DataFrame] = []
    competitions = sorted(actions_df["competition_id"].dropna().unique())
    log.info("Computing xT grids for %d competitions", len(competitions))

    for comp_id in competitions:
        comp_actions = actions_df[actions_df["competition_id"] == comp_id]
        n_events = len(comp_actions)
        if n_events < 100:
            log.warning("Competition %s has only %d events — skipping", comp_id, n_events)
            continue

        grid = compute_expected_threat_grid(comp_actions, params)
        grid_df = grid_to_dataframe(grid, competition_id=str(comp_id))
        all_grids.append(grid_df)
        log.info(
            "Competition %s: %d events, max xT=%.5f",
            comp_id, n_events, float(grid.max()),
        )

    # Global grid (all competitions combined)
    global_grid = compute_expected_threat_grid(actions_df, params)
    global_df = grid_to_dataframe(global_grid, competition_id="global")
    all_grids.append(global_df)
    log.info(
        "Global grid: %d events, max xT=%.5f",
        len(actions_df), float(global_grid.max()),
    )

    # Combine and write
    combined_df = pd.concat(all_grids, ignore_index=True)
    spark_df = spark.createDataFrame(combined_df)  # type: ignore[union-attr]
    write_delta_table(
        spark_df,
        catalog=catalog,
        schema=schema,
        table_name=_TABLE_NAME,
        mode="overwrite",
        logger=log,
    )

    # Export global grid as CSV for dbt seed update.
    # NOTE: Path(__file__) resolves to the local filesystem, so this only
    # works when run locally (uv run compute_expected_threat). On Databricks
    # serverless the write silently targets a DBFS path — run locally after
    # the Delta write to update the committed seed CSV.
    seed_df = grid_to_dataframe(global_grid)
    seed_path = Path(__file__).resolve().parents[2] / "dbt_project" / "seeds" / "expected_threat_grid.csv"
    seed_df.to_csv(seed_path, index=False)
    log.info("Updated dbt seed at %s", seed_path)

    log.info("Done — wrote %d grid rows (%d competitions + global)", len(combined_df), len(competitions))


def main() -> None:
    """CLI entry point."""
    args = parse_ingestion_args("Compute Expected Threat grids from SPADL actions")
    log = configure_logging("expected_threat")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, log)
```

- [ ] **Step 2: Add entry point to pyproject.toml**

In the `[project.scripts]` section, add:

```toml
compute_expected_threat = "ingestion.expected_threat:main"
```

- [ ] **Step 3: Verify import**

```bash
uv run python -c "from ingestion.expected_threat import run_pipeline; print('OK')"
```

---

### Task 8: dbt seed integration

**Files:**
- Modify: `dbt_project/seeds/expected_threat_grid.csv` (updated by pipeline)

The batch pipeline (Task 7) writes the updated CSV directly. After running on Databricks:

- [ ] **Step 1: Verify the updated CSV has correct schema**

```bash
head -3 dbt_project/seeds/expected_threat_grid.csv
```

Expected:
```
zone_x,zone_y,xt_value
0,0,<data-driven value>
0,1,<data-driven value>
```

Must have 96 rows + 1 header = 97 lines. Columns: `zone_x, zone_y, xt_value`.

- [ ] **Step 2: Run dbt seed locally (dry run)**

```bash
cd dbt_project && MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['seed', '--select', 'expected_threat_grid', '--profiles-dir', '.'])"
```

Expected: seed loads without errors.

- [ ] **Step 3: Verify off_ball_xt pipeline still works**

The `off_ball_xt` pipeline reads from `{catalog}.dev_silver.expected_threat_grid` which is populated by `dbt seed`. The schema is unchanged (zone_x, zone_y, xt_value), so no pipeline changes are needed. The only difference is the values are now data-driven.

No code changes to `src/ingestion/off_ball_xt.py` or `src/analytics/off_ball_xt.py` required.

---

## Chunk 3: D4 + Deployment

### Task 9: Pitch Control animation (D4)

**Files:**
- Modify: `demo_space/app.py` (extend Pitch Control tab)

D4 adds play/pause animation to the Pitch Control tab. Uses Gradio's `gr.Timer` component to auto-advance frames.

- [ ] **Step 1: Add animation controls to the Pitch Control tab**

Modify the Pitch Control tab block (from Task 3) to add animation controls below the frame slider:

```python
        # --- Animation controls (D4) ---
        with gr.Row():
            pc_play_btn = gr.Button("Play", size="sm")
            pc_speed = gr.Slider(
                minimum=0.5, maximum=4.0, step=0.5, value=1.0,
                label="Playback speed (x)",
            )
        pc_timer = gr.Timer(value=0.2, active=False)  # 200ms tick = ~5fps visual

        _pc_playing = gr.State(value=False)

        def _toggle_play(playing: bool) -> tuple[bool, gr.Timer, gr.Button]:
            new_state = not playing
            return (
                new_state,
                gr.Timer(active=new_state),
                gr.Button("Pause" if new_state else "Play"),
            )

        def _advance_frame(
            frame: int, playing: bool, speed: float,
            match_id: str | None, period: int,
        ) -> int:
            if not playing or not match_id:
                return frame
            _, hi = _get_frame_range(match_id, period)
            fps = _get_frame_rate(match_id)
            step = max(1, int(fps * speed * 0.2))  # frames per 200ms tick
            new_frame = frame + step
            if new_frame >= hi:
                return hi
            return new_frame

        pc_play_btn.click(
            fn=_toggle_play,
            inputs=[_pc_playing],
            outputs=[_pc_playing, pc_timer, pc_play_btn],
        )
        pc_timer.tick(
            fn=_advance_frame,
            inputs=[pc_frame, _pc_playing, pc_speed, pc_match, pc_period],
            outputs=[pc_frame],
        )
        # Timer tick updates frame → triggers plot refresh
        pc_frame.change(
            fn=create_pitch_control_plot,
            inputs=_pc_inputs,
            outputs=pc_plot,
        )
```

Note: Replace the `pc_frame.release` event binding from Task 3 with `pc_frame.change` so that both manual slider drags and timer-driven frame updates trigger plot refresh.

- [ ] **Step 2: Test animation locally**

```bash
cd demo_space && python app.py
```

Open Pitch Control tab. Click Play — heatmap should auto-advance through frames. Click Pause — stops. Adjust speed slider — animation speed changes. Verify frame slider moves with the animation.

---

### Task 10: Update demo_space/README.md

**Files:**
- Modify: `demo_space/README.md`

- [ ] **Step 1: Update the README**

Update the HF Space README to document all 6 tabs:

```markdown
---
title: Soccer Analytics Explorer
emoji: ⚽
colorFrom: blue
colorTo: green
sdk: gradio
sdk_version: 5.23.3
app_file: app.py
pinned: false
license: apache-2.0
---

# Soccer Analytics Explorer

Interactive demo for the [Luxury Lakehouse](https://huggingface.co/luxury-lakehouse)
soccer analytics platform.

## Tabs

| Tab | Description |
|-----|-------------|
| **Player Similarity** | Doc2Vec behavioral embedding search — find players with similar styles |
| **Shot Map** | Shot locations on a half-pitch, colored by outcome |
| **Pass Quality** | Pass origins with line-breaking pass highlighting |
| **Pitch Control** | Physics-based pitch control (Spearman 2017) with frame-by-frame animation and velocity arrows |
| **DEFCON Pressure** | Defensive contribution breakdown per match — Intercept/Concede/Disturb/Deter |

## Data

All data is pre-cached as Parquet files (no live database connectivity):

- `career_embeddings.parquet` — Doc2Vec career embeddings (~8,950 players)
- `sample_shots.parquet` — 1,000 shots from StatsBomb Open Data
- `sample_passes.parquet` — 2,000 passes with line-breaking detection
- `sample_tracking.parquet` — Metrica Sports tracking at 1fps (2 matches)
- `defcon_pressure.parquet` — DEFCON pressure aggregates with player names

## Sources

- [StatsBomb Open Data](https://github.com/statsbomb/open-data) (CC-BY 4.0)
- [Wyscout Public Dataset](https://figshare.com/collections/Soccer_match_event_dataset/4415000) (CC-BY 4.0)
- [Metrica Sports Sample Data](https://github.com/metrica-sports/sample-data) (CC-BY 4.0)
```

---

### Task 11: HF Space deployment

- [ ] **Step 1: Install plotly in local demo_space venv and verify**

```bash
cd demo_space && pip install -r requirements.txt && python app.py
```

All 5 tabs should render without errors.

- [ ] **Step 2: Push demo_space to HF Hub**

```bash
cd demo_space && git init && git remote add origin https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo
git add -A && git commit -m "feat: add Pitch Control, DEFCON Pressure tabs + animation (D1-D4)"
git push origin main --force
```

- [ ] **Step 3: Verify on HF Spaces**

Open `https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo`. Verify all 5 tabs load and function correctly.

---

### Task 12: Databricks deployment (D3)

- [ ] **Step 1: Upload export_demo_data.py notebook**

```bash
databricks workspace import notebooks/export_demo_data.py \
  /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/export_demo_data \
  --language PYTHON --profile OAUTH --overwrite
```

- [ ] **Step 2: Run the expected_threat pipeline**

Either via the Databricks workflow or directly:

```bash
databricks jobs run-now --job-name "compute_expected_threat" --profile OAUTH
```

Or add to the existing workflow in Terraform and apply.

- [ ] **Step 3: Verify Delta table**

```sql
SELECT competition_id, COUNT(*) AS zones, MAX(xt_value) AS max_xt
FROM soccer_analytics.bronze.expected_threat_grids
GROUP BY competition_id
ORDER BY competition_id
```

Expected: one row per competition + a "global" row, each with 96 zones.

- [ ] **Step 4: Run dbt seed**

```bash
cd dbt_project && dbt seed --select expected_threat_grid
```

- [ ] **Step 5: Verify off_ball_xt still works**

```sql
SELECT match_id, COUNT(*), AVG(total_off_ball_xt) FROM soccer_analytics.dev_silver.stg_off_ball_xt__results LIMIT 5
```

Values may differ from before (data-driven grid vs static), but schema and pipeline should be intact.

---

### Task 13: E2E verification and documentation

- [ ] **Step 1: Verify all components**

| Component | Check | Expected |
|-----------|-------|----------|
| HF Space — Player Similarity | Search "Messi" | Top-K results render |
| HF Space — Shot Map | Filter by competition | Map updates |
| HF Space — Pass Quality | Toggle line-breaking | Highlights appear |
| HF Space — Pitch Control | Drag frame slider | Heatmap updates |
| HF Space — Pitch Control | Click Play | Animation runs |
| HF Space — DEFCON Pressure | Search player | Grouped bar chart renders |
| Databricks — expected_threat_grids | Query Delta table | Per-competition + global grids |
| dbt — expected_threat_grid seed | dbt seed | Loads updated CSV |
| Local — pytest | `uv run pytest src/tests/ -v` | All tests pass |
| Local — ruff | `uv run ruff check src/` | Zero violations |
| Local — pyright | `uv run pyright src/` | Zero errors |

- [ ] **Step 2: Update TODO.md**

Mark D1-D4 as complete:

```markdown
| ~~D1~~ | ~~HF Space — Pitch Control + Velocity Arrows~~ | ~~Wicked~~ | Complete |
| ~~D2~~ | ~~HF Space — DEFCON Pressure Breakdown~~ | ~~Wicked~~ | Complete |
| ~~D3~~ | ~~Dynamic xT Grid~~ | ~~Wicked~~ | Complete |
| ~~D4~~ | ~~Pitch Control Animation~~ | ~~Dunkin'~~ | Complete |
```

- [ ] **Step 3: Single commit (after all verification passes)**

```bash
git add <all changed files>
git commit -m "feat: interactive analytics — Pitch Control tab, DEFCON Pressure tab, dynamic xT grid, animation (D1-D4)"
```
