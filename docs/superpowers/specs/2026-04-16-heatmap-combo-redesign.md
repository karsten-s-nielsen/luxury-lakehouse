# Heat Map Combo A+C Redesign

## Origin

Kirk chart-choice audit (`docs/superpowers/specs/kirk-poc-findings/heat-map.md`, 2026-04-16):

- **Finding #1 (Low)**: Spatial density heatmap is functionally a density surface, not Kirk's categorical-intersection heat map. Bubble map is the closer match for Spatial x Distribution.
- **Finding #2 (Medium)**: Passes and shots collapsed into a single density layer — the `action_type` dimension (already fetched by the query) is discarded in the visualization.

## Design Decision

Two-row 2x2 grid layout (same pattern as Match Summary's 4-chart grid):

| | Left (Passes) | Right (Shots) |
|---|---|---|
| **Row 1 — Combo A (Split Bubbles)** | Pass bubble map, Blues colormap | Shot bubble map, OrRd colormap |
| **Row 2 — Combo C (Split Bubbles + Focus)** | Pass bubbles, top-3 bins highlighted with gold rings + count annotations, rest muted to 25% alpha | Shot bubbles, same focus treatment |

**Row 1** is the exploration view — full distribution, no editorial filtering. Analysts see where actions concentrate.

**Row 2** is the explanation/coaching view — the top-3 hottest bins are visually isolated, making the "where does this team/player concentrate?" question immediately answerable. This follows Kirk Ch5: monitoring (explore) vs briefing (explain).

## Visual Specification

### Bubble Encoding

- **Position**: bin center coordinates from `Pitch.bin_statistic(bins=(12, 8))`
- **Size**: circle area proportional to bin count (area encoding, not radius — perceptually linear per Cleveland & McGill)
- **Color**: sequential ColorBrewer palette mapped to count intensity
  - Passes: `Blues` (ColorBrewer sequential, CVD-safe)
  - Shots: `OrRd` (ColorBrewer sequential, CVD-safe)
- **Zero bins**: omitted (no dot rendered)
- **Colorbar**: retained, labeled "Pass Count" / "Shot Count"

### Focus Treatment (Row 2)

- Top-3 bins by count get:
  - Full alpha (0.85)
  - Gold ring edge (`#f59e0b`, linewidth 2) — reuses existing `AMBER` constant
  - Count annotation label above each bubble (white text, small font)
- Remaining bins: alpha reduced to 0.25, no edge
- If fewer than 3 non-zero bins exist, highlight all non-zero bins

### Layout

- Two `ContentRow(columns=2)` instances, each with two `ContentBlock("image", var)`
- Row condition: `len(hm_pass_bubbles) > 0` (gates both rows on data presence)
- Figure size: `(10, 7)` per chart (half-width in the 3fr content column)
- Captions on Row 2 blocks: "Top 3 zones highlighted" (brief, non-redundant with title)

### Color Compliance

Per ColorBrewer adoption (Option C, cognitive-interface-audit v1.18.0):
- `Blues` and `OrRd` are named ColorBrewer palettes — Tier 1 compliant
- Replaces `cmap="hot"` (non-ColorBrewer, perceptual kinks, flagged by Phase 0)
- Gold ring (`#f59e0b`) is a UI accent, not a data-encoding color — no CVD validation needed

## State Variable Changes

| Old | New | Purpose |
|---|---|---|
| `hm_pitch_image` | `hm_pass_bubbles` | Row 1 left — pass bubble map |
| — | `hm_shot_bubbles` | Row 1 right — shot bubble map |
| — | `hm_pass_focus` | Row 2 left — pass bubbles + focus |
| — | `hm_shot_focus` | Row 2 right — shot bubbles + focus |

Metrics sidebar unchanged (Total Actions, Passes, Shots, Most Active Zone).

## Data Flow

No query changes needed. `fetch_heatmap_actions()` already returns columns `x, y, action_type, cnt`. Split client-side:

```
actions[actions["action_type"] == "pass"]  → pass_actions
actions[actions["action_type"] == "shot"]  → shot_actions
```

Each subset feeds independently into the bubble rendering functions.

## Files Changed

| File | Change |
|---|---|
| `hf_taipy_app/src/state/heat_map.py` | Replace `_render_heatmap` with `_render_bubble_map` + `_render_bubble_focus_map`; 4 new state vars; update `hm_refresh` |
| `hf_taipy_app/src/pages/heat_map.py` | 2x2 `ContentRow(columns=2)` layout; update `empty_condition` |
| `hf_taipy_app/src/template.py` | No changes (PAGE_TERMS already has Heat-Map entry; no new glossary terms needed) |

## What This Does NOT Change

- Query layer (`queries/tracking.py`) — no SQL changes
- Metrics sidebar — same 4 metrics
- Zone classification (`_classify_zone`) — still used for Most Active Zone metric
- Page registration, nav section, citations
