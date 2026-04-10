# PA1 + PA4: Game State Segmentation & Conversion Rate Funnel

**Date:** 2026-04-09
**Branch:** `feat/game-state-conversion-funnel`
**Source:** Performance analysis courses (Donnelly, Thomson, Sfeir, Gronnemark)

---

## Summary

Two cross-cutting enhancements delivered as a single cycle:

- **PA1 (Game State Segmentation):** Add per-event game state (`winning`/`losing`/`drawing`) to dbt mart models. Surface as a shared filter dropdown on event-level pages, hidden on career-aggregate and tracking-only pages.
- **PA4 (Conversion Rate Funnel):** New dashboard page showing Possessions → A3 Entries → Shots → Goals with conversion rates at each stage. Horizontal mirror bar chart comparing home vs away teams.

PA1 is a prerequisite for PA4 — the funnel page consumes the game state filter.

---

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Game state derivation | New dbt intermediate model (`int_running_score`) | Per-event accuracy; dbt owns the logic, app just filters |
| Game state values | `winning` / `losing` / `drawing` | Matches PA literature (Donnelly, Thomson); consistent grammar (present participles); aligns with existing `draw` in `fct_match_summary.match_result` |
| Game state column placement | Added to `fct_action_values`, `fct_shots`, `fct_passes` | App filter is a simple `WHERE game_state = %s` — no query-time joins |
| Filter scope | Shared state variable, conditionally shown per page | `SidebarWidget.condition` controls visibility; pages without per-event data hide the dropdown |
| Possession data for PA4 | Add `possession_id` + `possession_team_id` to `fct_action_values` | Possession is a property of the action, not a separate entity; enables single-table funnel query |
| A3 entry definition | `start_x <= 70 AND end_x > 70 AND action_result = 'success'` | Standard PA definition — ball must cross the threshold, any successful action type (passes, dribbles, carries) |
| Funnel chart type | Horizontal mirror bars (Plotly) | Clean team comparison, easy to read exact values, simple Plotly implementation |
| Team comparison | Mirror bars — home extends right (red `#e63946`), away extends left (steel-blue `#457b9d`) | Canonical home/away colors from Kirk audit (K-2) |
| Page layout | Dashboard (`StatCard` tiles + full-width chart) | KPI cards with help_text tooltips; full-width maximizes mirror chart space |
| Multi-match aggregation | Both single-match and multi-match | Match selector is optional; query drops `WHERE match_id` when "All" |

---

## dbt Data Layer

### New intermediate model: `int_running_score.sql`

**Source:** `fct_shots` (`match_id`, `team_id`, `is_goal`, `period`, `minute`, `second`)

Computes cumulative home/away goals at each goal event via window function. Output columns:

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | bigint | FK to dim_matches |
| `goal_minute` | int | Minute of the goal |
| `goal_second` | int | Second of the goal |
| `goal_period` | int | Period (1–5) |
| `home_score_after` | int | Cumulative home goals after this event |
| `away_score_after` | int | Cumulative away goals after this event |

One row per goal event per match, plus a "kickoff" row (0–0) for each match. Downstream mart models reference this as a CTE to derive `game_state` per action row.

### Extend `fct_action_values.sql`

New columns:

| Column | Type | Source |
|--------|------|--------|
| `possession_id` | int | `stg_statsbomb__events.possession` via join on `original_event_id` |
| `possession_team_id` | int | `stg_statsbomb__events.possession_team_id` via same join |
| `game_state` | string | Derived from `int_running_score`: `winning` / `losing` / `drawing` from the acting team's perspective |

### Extend `fct_shots.sql`

New column:

| Column | Type | Source |
|--------|------|--------|
| `game_state` | string | Derived from `int_running_score` |

### Extend `fct_passes.sql`

New column:

| Column | Type | Source |
|--------|------|--------|
| `game_state` | string | Derived from `int_running_score` |

### Contract updates (`_marts__models.yml`)

All new columns added with `data_type` and appropriate `data_tests` (e.g., `game_state` accepted values test for `winning`/`losing`/`drawing`).

---

## Shared State (PA1)

### Additions to `state/shared.py`

```python
selected_game_state: str | None = "All"
game_state_lov: list[str] = ["All", "Winning", "Losing", "Drawing"]
```

Callback: `on_game_state_change(state)` — calls `_refresh_current_page(state)`.

### Pages showing the game state dropdown

| Page | Fact table | Shows dropdown |
|------|-----------|---------------|
| Shot Map | `fct_shots` | Yes |
| Heat Map | `fct_action_values` | Yes |
| Action Values | `fct_action_values` | Yes |
| Pass Map | `fct_passes` | Yes |
| Pass Timing | `fct_passes` | Yes |
| Match Summary | event-level aggregation | Yes |
| Defensive Impact | `fct_defensive_values` / `fct_defcon_pressure` | Yes |
| Goalkeeper | `fct_shots` | Yes |
| Conversion Funnel | `fct_action_values` (PA4) | Yes |
| Player Radar | career aggregates | No |
| Player Similarity | embeddings | No |
| Rankings | cross-player aggregates | No |
| Pitch Control | tracking data | No |
| Movement Analysis | tracking data | No |
| Team Shape | tracking data | No |
| Tactical Positions | tracking data | No |
| AI/ML Workflows | operational | No |

Each visible page adds `SidebarWidget(kind="dropdown", var="selected_game_state", lov="game_state_lov", on_change="on_game_state_change", help="Filter by game state at the time of each action: winning, losing, or drawing.")` and adds `WHERE game_state = %s` to its query when `state.selected_game_state != "All"`.

---

## PA4 Conversion Rate Funnel Page

### New files

| File | Purpose |
|------|---------|
| `hf_taipy_app/src/state/conversion_funnel.py` | State module (prefix `cf_`) |
| `hf_taipy_app/src/pages/conversion_funnel.py` | Page config + `build_page()` |

### State module (`cf_` prefix)

- `cf_refresh(state)` queries `fct_action_values_synced` for all four funnel stages in a single query
- Computes per-team aggregates: possessions, A3 entries, shots, goals
- Computes step conversion rates and end-to-end conversion rate
- Renders Plotly mirror bar chart (`plotly.graph_objects.Bar`, horizontal orientation)
- Home bars extend right (red `#e63946`), away bars extend left (steel-blue `#457b9d`)
- Conversion rate annotations between bars
- `register_page_refresher("Conversion-Funnel", cf_refresh, is_dashboard=True)`

### Page config (dashboard layout)

**StatCards (4):**

| Label | Var | Detail | Help text |
|-------|-----|--------|-----------|
| Possessions | `cf_possessions` | `cf_possessions_detail` | Total team possessions in the selected scope. Possession = a continuous sequence of actions by one team. |
| A3 Entries | `cf_a3_entries` | `cf_a3_detail` | Successful actions crossing into the attacking third (final 35m of the pitch). Higher = better territorial penetration. |
| Shots | `cf_shots` | `cf_shots_detail` | Total shots attempted. Conversion from A3 entries = chance creation rate. |
| Goals | `cf_goals` | `cf_goals_detail` | Goals scored. Conversion from shots = finishing efficiency. |

Detail vars show step conversion rate (e.g., "25.3% of possessions").

**Content:** Single `ContentRow` with `ContentBlock("chart", "cf_funnel_chart")`.

**Sidebar widgets:** Competition, team, match (optional), game state.

**Citation:** Donnelly's systematic approach ("The What → The Outcome").

### Query pattern

Single SQL query against `fct_action_values_synced` joined to `fct_match_summary_synced` (for home/away team IDs):

- Possessions: `COUNT(DISTINCT possession_id)` per team
- A3 entries: `start_x <= 70 AND end_x > 70 AND action_result = 'success'`
- Shots: `action_type IN ('shot', 'shot_penalty', 'shot_freekick')`
- Goals: shots where `action_result = 'success'`
- Optional `WHERE game_state = %s` when filter is active
- Optional `WHERE match_id = %s` when a specific match is selected

### Registration

- Import in `main.py`, add `PageEntry` to `PAGE_REGISTRY`
- Add glossary terms to `PAGE_TERMS` in `template.py`: A3 Entry, Conversion Rate, Possession, Funnel

---

## Testing Strategy

### dbt tests

- `int_running_score` unit test: known match with 2 goals at known minutes, assert correct cumulative scores
- `fct_action_values` contract: verify `possession_id`, `possession_team_id`, `game_state` columns pass enforcement
- `fct_shots` / `fct_passes`: verify `game_state` values always one of `winning`/`losing`/`drawing`

### Python unit tests (TDD)

- Funnel aggregation: given a DataFrame of actions with possession IDs, coordinates, action types → verify correct counts at each funnel stage
- A3 entry detection edge cases: `start_x = 70` (not an entry), `start_x = 69.9 AND end_x = 70.1` (is an entry), failed actions excluded
- Game state derivation: given goals at specific minutes, assert correct game state for events at various timestamps
- Mirror chart rendering: verify Plotly figure has correct traces, orientation, colors, annotations

### E2E tests (Puppeteer)

- Navigate to Conversion Funnel page → select competition → team → verify stat cards populate
- Select specific match → verify funnel updates
- Select game state filter → verify funnel re-renders
- Verify game state dropdown appears on Shot Map
- Verify game state dropdown does NOT appear on Player Similarity

---

## Kirk/Voss Audit Compliance

| Finding | How addressed |
|---------|---------------|
| K-1 Color consistency | Canonical home/away colors used (`#e63946` / `#457b9d`) |
| K-2 Home/Away convention | Mirror bars: home = red (right), away = steel-blue (left) |
| K-3 Annotations | Conversion rate labels between funnel stages |
| K-5 Redundant encoding | Bar width encodes volume + numeric labels on bars |
| V-1 Error messages | Empty state follows 3-part template from V-1 remediation |

---

## Out of Scope

- Multi-match trend visualization (future enhancement)
- Per-player funnel breakdown (future enhancement)
- Benchmark reference values (requires published conversion rate baselines)
- Set piece filtering (PA2 scope)
