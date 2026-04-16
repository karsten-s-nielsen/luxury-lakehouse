# Heat Map Combo A+C Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single combined density heatmap with a 2x2 bubble map grid — split by action type (passes/shots) in row 1, with top-3 focus highlighting in row 2.

**Architecture:** Two new rendering functions replace `_render_heatmap`. Each takes a filtered DataFrame (pass-only or shot-only), bins via mplsoccer `bin_statistic(bins=(12, 8))`, and draws area-encoded circles at bin centers. The focus variant mutes non-top-3 bins and adds gold ring + count annotations. The page config switches from one `ContentRow` to two `ContentRow(columns=2)` — same pattern as Match Summary.

**Tech Stack:** mplsoccer (Pitch, bin_statistic), matplotlib (scatter, colorbar, ScalarMappable, Normalize), ColorBrewer palettes (Blues, OrRd)

**Spec:** `docs/superpowers/specs/2026-04-16-heatmap-combo-redesign.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `hf_taipy_app/src/state/heat_map.py` | Modify | Replace `_render_heatmap` with `_render_bubble_map` + `_render_bubble_focus_map`; replace `hm_pitch_image` with 4 state vars; update `hm_refresh` |
| `hf_taipy_app/src/pages/heat_map.py` | Modify | 2x2 `ContentRow(columns=2)` layout; update `empty_condition` |
| `hf_taipy_app/src/test_render.py` | Modify | Replace `hm_pitch_image` mock with 4 new state var mocks |

---

### Task 1: Update state module — rendering functions and state variables

**Files:**
- Modify: `hf_taipy_app/src/state/heat_map.py`

- [ ] **Step 1: Replace state variables**

Replace the single `hm_pitch_image` with four image state vars. Update `__all__`.

```python
# OLD (lines 33-34):
hm_pitch_image: str = ""

# NEW:
hm_pass_bubbles: str = ""
hm_shot_bubbles: str = ""
hm_pass_focus: str = ""
hm_shot_focus: str = ""
```

Update `__all__` — remove `"hm_pitch_image"`, add `"hm_pass_bubbles"`, `"hm_shot_bubbles"`, `"hm_pass_focus"`, `"hm_shot_focus"`.

- [ ] **Step 2: Add AMBER import**

Add `AMBER` to the render import line:

```python
from render import AMBER, PITCH_BG_COLOR, PITCH_LINE_COLOR, fmt_int, pitch_to_file
```

Add matplotlib.colors import:

```python
from matplotlib.colors import Normalize
```

- [ ] **Step 3: Replace `_render_heatmap` with `_render_bubble_map`**

Delete `_render_heatmap` (lines 85-110). Replace with:

```python
def _render_bubble_map(
    actions: pd.DataFrame,
    title: str,
    cmap_name: str,
    count_label: str,
) -> str:
    """Render area-encoded bubble map at bin centers — ColorBrewer palette."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(10, 7))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if actions.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_map")

    # Expand pre-aggregated rows for bin_statistic
    counts = actions["cnt"].astype(int).values
    expanded_x = np.repeat(actions["x"].values, counts)
    expanded_y = np.repeat(actions["y"].values, counts)

    bin_stats = pitch.bin_statistic(expanded_x, expanded_y, statistic="count", bins=(12, 8))

    # Flatten bin grid to 1-D arrays
    stat_flat = bin_stats["statistic"].flatten()
    cx_flat = bin_stats["cx"].flatten()
    cy_flat = bin_stats["cy"].flatten()

    # Only plot non-zero bins
    mask = stat_flat > 0
    values = stat_flat[mask]
    cx = cx_flat[mask]
    cy = cy_flat[mask]

    if len(values) == 0:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_map")

    # Area encoding — max bubble 500 pt²
    max_size = 500
    sizes = (values / values.max()) * max_size

    # ColorBrewer sequential palette
    cmap = plt.get_cmap(cmap_name)
    norm = Normalize(vmin=0, vmax=float(values.max()))
    colors = cmap(norm(values))

    ax.scatter(cx, cy, s=sizes, c=colors, alpha=0.85, edgecolors="none", zorder=2)

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label(count_label, color=PITCH_LINE_COLOR, fontsize=10)
    cbar.ax.yaxis.set_tick_params(color=PITCH_LINE_COLOR)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color=PITCH_LINE_COLOR)

    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "bubble_map")
```

- [ ] **Step 4: Add `_render_bubble_focus_map`**

Add immediately after `_render_bubble_map`:

```python
def _render_bubble_focus_map(
    actions: pd.DataFrame,
    title: str,
    cmap_name: str,
    count_label: str,
) -> str:
    """Bubble map with top-3 bins highlighted — gold ring + count annotation."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(10, 7))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if actions.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_focus")

    # Expand pre-aggregated rows for bin_statistic
    counts = actions["cnt"].astype(int).values
    expanded_x = np.repeat(actions["x"].values, counts)
    expanded_y = np.repeat(actions["y"].values, counts)

    bin_stats = pitch.bin_statistic(expanded_x, expanded_y, statistic="count", bins=(12, 8))

    stat_flat = bin_stats["statistic"].flatten()
    cx_flat = bin_stats["cx"].flatten()
    cy_flat = bin_stats["cy"].flatten()

    mask = stat_flat > 0
    values = stat_flat[mask]
    cx = cx_flat[mask]
    cy = cy_flat[mask]

    if len(values) == 0:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_focus")

    max_size = 500
    sizes = (values / values.max()) * max_size

    cmap = plt.get_cmap(cmap_name)
    norm = Normalize(vmin=0, vmax=float(values.max()))
    colors = cmap(norm(values))

    # Top-3 bin indices (or fewer if <3 non-zero bins)
    top_k = min(3, len(values))
    top_indices = np.argsort(values)[-top_k:]
    muted_mask = np.ones(len(values), dtype=bool)
    muted_mask[top_indices] = False

    # Muted background bubbles
    if muted_mask.any():
        ax.scatter(
            cx[muted_mask], cy[muted_mask],
            s=sizes[muted_mask], c=colors[muted_mask],
            alpha=0.25, edgecolors="none", zorder=2,
        )

    # Highlighted top-3 bubbles with gold ring
    ax.scatter(
        cx[top_indices], cy[top_indices],
        s=sizes[top_indices], c=colors[top_indices],
        alpha=0.85, edgecolors=AMBER, linewidths=2, zorder=3,
    )

    # Count annotations above each highlighted bubble
    for idx in top_indices:
        ax.annotate(
            str(int(values[idx])),
            (cx[idx], cy[idx]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color="white",
            zorder=4,
        )

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label(count_label, color=PITCH_LINE_COLOR, fontsize=10)
    cbar.ax.yaxis.set_tick_params(color=PITCH_LINE_COLOR)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color=PITCH_LINE_COLOR)

    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "bubble_focus")
```

- [ ] **Step 5: Update `hm_refresh` callback**

Replace all `hm_pitch_image` references with the 4 new state vars. Split actions by action_type before rendering.

In the **reset block** (no comp_id selected, lines 160-168), replace:
```python
state.hm_pitch_image = ""
```
with:
```python
state.hm_pass_bubbles = ""
state.hm_shot_bubbles = ""
state.hm_pass_focus = ""
state.hm_shot_focus = ""
```

In the **empty-data block** (lines 185-193), replace:
```python
state.hm_pitch_image = ""
```
with:
```python
state.hm_pass_bubbles = ""
state.hm_shot_bubbles = ""
state.hm_pass_focus = ""
state.hm_shot_focus = ""
```

In the **fetch error block** (lines 180-183), replace:
```python
state.hm_pitch_image = ""
```
with:
```python
state.hm_pass_bubbles = ""
state.hm_shot_bubbles = ""
state.hm_pass_focus = ""
state.hm_shot_focus = ""
```

In the **rendering section** (line 202), replace:
```python
state.hm_pitch_image = _render_heatmap(actions)
```
with:
```python
# Split by action type — query already returns action_type column
pass_actions = actions.loc[actions["action_type"] == "pass"]
shot_actions = actions.loc[actions["action_type"] == "shot"]

# Row 1 — Combo A: split bubble maps (exploration view)
state.hm_pass_bubbles = _render_bubble_map(pass_actions, "Pass Distribution", "Blues", "Pass Count")
state.hm_shot_bubbles = _render_bubble_map(shot_actions, "Shot Distribution", "OrRd", "Shot Count")

# Row 2 — Combo C: split bubble maps with focus (coaching view)
state.hm_pass_focus = _render_bubble_focus_map(pass_actions, "Pass Hotspots (Top 3)", "Blues", "Pass Count")
state.hm_shot_focus = _render_bubble_focus_map(shot_actions, "Shot Hotspots (Top 3)", "OrRd", "Shot Count")
```

---

### Task 2: Update page config — 2x2 grid layout

**Files:**
- Modify: `hf_taipy_app/src/pages/heat_map.py`

- [ ] **Step 1: Replace content and empty_condition**

Replace the `content` and `empty_condition` fields in `PageConfig`:

```python
content=[
    ContentRow(
        [
            ContentBlock("image", "hm_pass_bubbles"),
            ContentBlock("image", "hm_shot_bubbles"),
        ],
        columns=2,
        condition="len(hm_pass_bubbles) > 0",
    ),
    ContentRow(
        [
            ContentBlock(
                "image",
                "hm_pass_focus",
                caption="Top 3 zones highlighted",
            ),
            ContentBlock(
                "image",
                "hm_shot_focus",
                caption="Top 3 zones highlighted",
            ),
        ],
        columns=2,
        condition="len(hm_pass_bubbles) > 0",
    ),
],
empty_condition="len(hm_pass_bubbles) == 0 and len(competition_lov) > 0",
```

---

### Task 3: Update test_render.py — state variable mocks

**Files:**
- Modify: `hf_taipy_app/src/test_render.py`

- [ ] **Step 1: Replace hm_pitch_image mock**

At line 183, replace:
```python
hm_pitch_image = ""
```
with:
```python
hm_pass_bubbles = ""
hm_shot_bubbles = ""
hm_pass_focus = ""
hm_shot_focus = ""
```

---

### Task 4: Lint and type check

- [ ] **Step 1: Run ruff check**

```bash
cd hf_taipy_app && uv run ruff check src/state/heat_map.py src/pages/heat_map.py src/test_render.py
```

Expected: 0 violations.

- [ ] **Step 2: Run ruff format check**

```bash
cd hf_taipy_app && uv run ruff format --check src/state/heat_map.py src/pages/heat_map.py src/test_render.py
```

Expected: all files formatted.

- [ ] **Step 3: Run pyright**

```bash
cd hf_taipy_app && uv run pyright src/state/heat_map.py src/pages/heat_map.py
```

Expected: 0 errors in basic mode.

---

### Task 5: E2E visual test

- [ ] **Step 1: Start the Taipy app locally**

```bash
cd hf_taipy_app && python src/main.py
```

Navigate to `http://localhost:7860`, select Heat Map page.

- [ ] **Step 2: Verify 4-chart grid**

Select a competition + team. Confirm:
1. Row 1 shows two bubble maps side by side (passes left, shots right)
2. Row 2 shows same bubble maps with top-3 focus highlighting
3. Blues colormap on left column, OrRd on right column
4. Gold rings visible on top-3 bins in row 2
5. Count annotations appear above highlighted bubbles
6. Colorbars labeled "Pass Count" / "Shot Count"
7. Captions "Top 3 zones highlighted" below row 2 charts

- [ ] **Step 3: Edge cases**

1. Select a player with very few shots — confirm shot charts render gracefully (empty or 1-2 bubbles)
2. Switch competition — confirm all 4 charts update
3. Clear team filter — confirm all 4 charts update to competition-wide view
