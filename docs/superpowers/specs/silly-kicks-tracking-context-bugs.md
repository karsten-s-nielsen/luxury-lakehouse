# silly-kicks Tracking Context Bugs — Copy/Paste Context for Fix Session

> **Context:** These bugs were identified by running `_enrich_match` on 5 fully-completed
> IDSSE/Metrica matches via the lakehouse `compute_tracking_context` pipeline (run 129934372700080,
> 2026-05-16). The enrichment code itself is correct — these are silly-kicks library issues.
>
> **Local reproduction update (2026-05-17):** Bugs 1 and 4 were ARTIFACTS of Bug 5 (zero xT from
> missing goals). With a proper xT grid, blocking_score produces 17/24 non-zero values and
> gk_pitch_control_share_weighted is 22/24 non-null. **Only Bugs 2, 3, and 5 are real.**

---

## Bug 5: IDSSE/Metrica SPADL converter never maps goals to result_id=1 (CRITICAL)

**Affected:** `result_id` column in SPADL actions for IDSSE and Metrica providers.

**Observed (production, all bronze.spadl_actions):**

| Provider | Total Shots | Goals (result_id=1) | Goal Rate |
|----------|-------------|---------------------|-----------|
| idsse | 164 | 0 | 0% |
| metrica | 68 | 0 | 0% |
| skillcorner | 199 | 26 | 13% |
| statsbomb | 78,211 | 9,788 | 12.5% |
| wyscout | 40,839 | 5,106 | 12.5% |

**Impact:** xT (`ExpectedThreat.fit()`) requires goals as terminal reward in its Markov chain. With zero goals, the xT grid converges to all-zero. In production this is masked because xT is fitted on ALL providers combined (StatsBomb/Wyscout contribute goals). But:
1. Local testing with only IDSSE/Metrica data produces zero xT → blocking_score=0, gk_pc_share=NULL
2. If the lakehouse ever fitted xT per-provider or per-match, IDSSE/Metrica would produce zero xT
3. Any downstream analysis counting goals per match would undercount for IDSSE/Metrica

**Root cause:** The DFL/Sportec SPADL converter and Metrica SPADL converter do not map their respective goal event indicators to `result_id=1`. DFL events have a "Treffer" (hit/goal) boolean or separate "Tor" (goal) event types. Metrica events have a "GOAL" tag. Neither is being captured as `result_id=1` in the SPADL output.

**Where to fix:** The SPADL converter for each provider:
- IDSSE/Sportec: `silly_kicks/spadl/sportec.py` (or equivalent DFL converter) — look for how shot events are mapped and where `result_id` is assigned
- Metrica: `silly_kicks/spadl/metrica.py` — same

**Verification query:**
```sql
-- After fix, these should show result_id=1 rows for idsse/metrica:
SELECT data_source, result_id, COUNT(*) as cnt
FROM soccer_analytics.bronze.spadl_actions
WHERE type_id IN (11, 12, 13)
GROUP BY data_source, result_id
ORDER BY data_source, result_id
```

**Expected post-fix:** IDSSE should have ~12% goal rate (typical Bundesliga), Metrica ~12% (typical).

---

## Bug 1: blocking_score — NOT A BUG (was artifact of Bug 5)

**Previous diagnosis:** `_classify_man_markers` was thought to classify all defenders as man-markers.

**Actual finding (2026-05-17):** With a non-zero xT grid, blocking_score produces 17/24 non-zero values (max=11.73, blocked_threat_fraction max=0.17). The previous production observation of all-zero was caused by xT being near-zero (Bug 5 masked this in local testing with single-match data; in production, the combined xT from StatsBomb+Wyscout IS non-zero, so this shouldn't have been zero in production either).

**Action:** Re-validate in next production run. If still zero with combined xT, revisit man-marking radius. The local reproduction shows the algorithm works correctly with proper xT.

---

## ~~Bug 4: gk_pitch_control_share_weighted — NOT A BUG (was artifact of Bug 5)~~

**Previous diagnosis:** `compute_pitch_control(decompose=True)` producing near-zero `total_weight`.

**Actual finding (2026-05-17):** With a non-zero xT grid, `gk_pitch_control_share_weighted` is 22/24 non-null. The NULL was caused by `total_weight = threat_weight.sum() < 1e-8` which is true when xT is zero everywhere.

**Action:** No silly-kicks fix needed. Confirm in next production run (production uses combined xT which is non-zero).

---

## Bug 2: gk_was_distributing / gk_was_engaged = false (all rows) — CONFIRMED

**Affected columns:** `blocking_score`, `blocked_threat_fraction`, `max_single_defender_blocking_score`

**Observed:** ALL values are 0.0 across ALL 5 completed matches (J03WMX, J03WOH, J03WPY, Sample_Game_1, Sample_Game_2). `n_blocked_receivers` and `n_potential_receivers` DO have non-zero values — only the score columns are zero.

**Root cause:** `_classify_man_markers` classifies ALL outfield defenders as man-markers, leaving `lane_blocker_ids` empty. When `lane_blocker_ids` is empty, `_compute_cover_shadow_dict` returns `blocking_score: 0.0` without computing pitch control (short-circuit at line 845-852 of `_cover_shadows.py`).

**Call chain:**
```
tracking_context._enrich_match (Step 14)
  → features.add_cover_shadows(actions, frames, xt, links=links, home_team_id=home_team_id)
    → _cover_shadows._compute_cover_shadow_dict(frame_data, passer_xy, tid, xt, ...)
      → _cover_shadows._classify_man_markers(defenders_outfield, attackers, goal_x_own, params)
        → Returns ALL defender player_ids as man-markers
      → lane_blocker_ids = [pid for pid in defenders if pid not in man_markers]  ← EMPTY
      → if not lane_blocker_ids: return {blocking_score: 0.0, ...}  ← SHORT-CIRCUIT
```

**Where to fix:** `silly_kicks/tracking/_cover_shadows.py:286-336` (`_classify_man_markers`)

**The algorithm (lines 316-335):**
```python
# Compute behind-points for all attackers
behind_points = att_pos + params.man_mark_behind_offset * toward_own_goal

# Build all (defender_idx, attacker_idx, distance) candidates within radius
candidates = []
for ai, bp in enumerate(behind_points):
    dists = np.linalg.norm(def_pos - bp, axis=1)
    for di in np.where(dists < params.man_mark_radius)[0]:
        candidates.append((di, ai, dists[di]))

# Greedy nearest-first 1:1 assignment
candidates.sort(key=lambda c: c[2])
assigned_defenders: set[int] = set()
assigned_attackers: set[int] = set()
man_markers: set = set()

for di, ai, _dist in candidates:
    if di in assigned_defenders or ai in assigned_attackers:
        continue
    assigned_defenders.add(di)
    assigned_attackers.add(ai)
    man_markers.add(def_ids[di])
```

**Parameters:**
- `man_mark_radius: float = 3.0` (metres)
- `man_mark_behind_offset: float = 1.0` (metres behind attacker toward own goal)

**Hypothesis:** With `man_mark_radius = 3.0` and `man_mark_behind_offset = 1.0`, the "behind point" is only 1m behind each attacker. In typical defensive shapes, defenders ARE within 3m of a point 1m behind some attacker. The threshold is too permissive — it matches almost every defender to some attacker in dense defensive blocks. The greedy 1:1 assignment then assigns ALL defenders (since there are typically 10 outfield defenders and 10 outfield attackers, and enough candidates exist for full matching).

**Expected behavior:** Some defenders (those in deeper positions, covering space rather than marking) should NOT be classified as man-markers. They should appear in `lane_blocker_ids` and produce non-zero blocking scores.

**Potential fix directions:**
1. Tighten `man_mark_radius` (e.g., 1.5m instead of 3.0m)
2. Add an angular constraint (defender must be goalward of attacker)
3. Cap max man-markers (e.g., never classify more than N-2 defenders as man-markers, ensuring at least 2 lane-blockers exist)
4. Only consider defenders who are actually between ball and goal (positional filter)

**Verification:** After fix, `lane_blocker_ids` should be non-empty for typical frames → `compute_blocking_score` is called → blocking_score > 0 for frames where defenders actually obstruct passing lanes.

---

## Bug 2: gk_was_distributing / gk_was_engaged = false (all rows) — CONFIRMED

**Affected columns:** `gk_was_distributing`, `gk_was_engaged`, `gk_actions_in_possession`

**Observed:** ALL values are `false` / `0` across ALL matches. `defending_gk_player_id_native` IS populated (correctly resolved from frames via `defending_gk_from_frames` fallback), so the GK is found — but the behavioral columns never fire.

**Root cause:** The behavioral lookback at `silly_kicks/spadl/utils.py:650-672` searches for `keeper_*` actions by the OPPOSING team within the lookback window. For DFL/Sportec/IDSSE data, the SPADL stream rarely contains explicit `keeper_save`, `keeper_claim`, or `keeper_punch` actions before shots — the DFL event schema doesn't have granular keeper event types that map to SPADL keeper action types. The Metrica sample data is similarly sparse on keeper actions.

**Call chain:**
```
tracking_context._enrich_match (Step 1)
  → spadl.utils.add_pre_shot_gk_context(actions, frames=frames)
    → For each shot row (line 656):
        → Looks back up to 5 actions / 10 seconds
        → Filters: (team_id[win] != shooter_team) & is_keeper[win]
        → is_keeper = np.isin(type_id, list(keeper_type_ids))
        → keeper_type_ids = {actiontype_id["keeper_save"], actiontype_id["keeper_claim"],
                             actiontype_id["keeper_punch"], actiontype_id["keeper_pick_up"]}
        → No defending-team keeper_* actions found in window → `continue` at line 671
```

**Where to fix:** `silly_kicks/spadl/utils.py:650-672`

**Key code (lines 650-672):**
```python
shot_type_ids = {spadlconfig.actiontype_id[name] for name in _GK_SHOT_TYPE_NAMES}
keeper_type_ids = {spadlconfig.actiontype_id[name] for name in _GK_KEEPER_TYPE_NAMES}
is_shot = np.isin(type_id, list(shot_type_ids))
is_keeper = np.isin(type_id, list(keeper_type_ids))

shot_indices = np.where(is_shot)[0]
for shot_idx in shot_indices:
    shooter_team = team_id[shot_idx]
    ...
    defending_in_window = in_window & (team_id[win] != shooter_team)
    defending_keeper_in_window = defending_in_window & is_keeper[win]
    if not defending_keeper_in_window.any():
        continue  # ← ALWAYS HITS for DFL/IDSSE data
```

**`_GK_KEEPER_TYPE_NAMES`** (check exact set — likely):
```python
_GK_KEEPER_TYPE_NAMES = ("keeper_save", "keeper_claim", "keeper_punch", "keeper_pick_up")
```

**Why it fails for DFL/IDSSE:** The DFL XML event schema produces SPADL actions where goalkeeper actions are rare or mapped to different type names. The SPADL converter may not produce `keeper_*` type actions from DFL events. Without these markers, the lookback never finds a defending-keeper action.

**Why it fails for Metrica:** Metrica Sample Games have sparse event data (actions are inferred from tracking, not from rich event streams). Goalkeeper events are largely absent.

**Important:** `defending_gk_player_id` IS correctly populated via the FALLBACK at line 712-716:
```python
if frames is not None:
    from silly_kicks.tracking._gk_resolve import defending_gk_from_frames
    gk_series = defending_gk_from_frames(sorted_actions, frames)
    sorted_actions["defending_gk_player_id"] = sorted_actions["defending_gk_player_id"].fillna(gk_series)
```
This frame-based resolver WORKS (finds `is_goalkeeper=True` opposing player in nearest frame). Only the events-based behavioral columns fail.

**Expected behavior:** `gk_was_engaged` should be True for shots where the GK was active (saves, punches, claims) shortly before. For data sources without keeper action types, these columns are inherently unresolvable from events alone.

**Potential fix directions:**
1. **Tracking-based behavioral detection:** Instead of relying on SPADL keeper actions, detect GK engagement from tracking data (GK moved significantly in last N seconds, GK velocity spike, GK left penalty area). This would work for ALL providers.
2. **Provider-specific GK action mapping:** For IDSSE, check if any DFL event types (e.g., "Torschuss" / "Parade") map to keeper actions that aren't currently being captured as `keeper_*` in the SPADL converter.
3. **Accept as inherent limitation:** Document that `gk_was_distributing`/`gk_was_engaged` require rich event streams (StatsBomb-level) and produce false for tracking-only or sparse-event providers.

**Verification:** For StatsBomb data (which has explicit keeper_save events), these columns should produce True values. For IDSSE/Metrica, verify whether ANY keeper-type actions exist in the SPADL stream: `actions[actions["type_id"].isin(keeper_type_ids)]`.

---

## Bug 3: Ward line_break__ward = 0 for all IDSSE matches (works on Metrica) — CONFIRMED

**Affected columns:** `line_break__ward`, `lines_broken__ward`, `line_breaking_type__ward`

**Observed:** ALL Ward line-breaking values are 0/NULL for ALL IDSSE matches (J03WMX, J03WOH, J03WPY). Metrica Sample_Game_1 and Sample_Game_2 DO produce non-zero values. The threshold-based `line_break` + `n_attackers_behind_line` columns (Step 9, `add_off_ball_context`) work correctly for BOTH providers.

**Root cause:** Coordinate system mismatch in `detect_line_breaking`. The function expects SPADL per-action coordinates where the acting team ALWAYS attacks x=105 (standard SPADL convention). However, the coordinate transform at lines 224-233 uses `home_team_id` to flip non-home actions:

```python
# Convert SPADL action coords to tracking coords for intersection
if action_team == home_team_id:
    track_start_x = start_x
    track_start_y = start_y
    track_end_x = end_x
    track_end_y = end_y
else:
    track_start_x = 105.0 - start_x
    track_start_y = 68.0 - start_y
    track_end_x = 105.0 - end_x
    track_end_y = 68.0 - end_y
```

**The problem:** In LTR-normalized tracking frames, the home team attacks toward x=105 (right). But SPADL actions have per-action attack direction (actor always attacks x=105). The coordinate flip above correctly converts SPADL action coords to tracking coords for the away team. But there may be an issue with how `opp_x` (opponent x-coordinates from frames) relates to the pass trajectory.

For IDSSE specifically: The tracking frames use LTR normalization. The opponent's x-coordinates from frames are in the LTR coordinate system. The pass trajectory (after the flip) is ALSO in LTR coordinates. The Ward clustering uses opponent x-spread to identify defensive lines. If the opponent positions aren't being correctly filtered or the game_id/period_id/frame_id lookup fails (type mismatch between IDSSE string IDs and frame groupby keys), the function returns zeros.

**Call chain:**
```
tracking_context._enrich_match (Step 10)
  → features.add_line_break(actions, frames, links=links, method="ward", home_team_id=home_team_id)
    → _line_breaking.detect_line_breaking(actions, frames, home_team_id=home_team_id, links=links)
      → frame_groups = dict(iter(non_ball_non_gk.groupby(
            ["game_id", "period_id", "frame_id", "team_id"], sort=False)))
      → For each pass/cross action:
          → frame_key = (game_id, period_id, frame_id)
          → teams_at_frame = frame_to_teams.get(frame_key, [])  ← LIKELY EMPTY for IDSSE
```

**Where to fix:** `silly_kicks/tracking/_line_breaking.py:149-206`

**Key diagnostic question:** What is `game_id` in the frames vs actions?
- In the lakehouse, `_enrich_match` uses `silly_kicks`-internal `game_id` (integer, from `actions["game_id"]`)
- For IDSSE, the enrichment code sets `actions["game_id"] = 1` (fixed per-match)
- Frames should have the same `game_id = 1` after conversion via `_bronze_idsse_to_sportec_input`
- **Verify:** Does `frames["game_id"]` match `actions["game_id"]` for IDSSE? If frames lack `game_id` or have a different value, the groupby key `(game_id, period_id, frame_id, team_id)` will never match.

**Why Metrica works:** Metrica frames and actions may be using the same `game_id` scheme (integer 1, 2, 3).

**Potential fix directions:**
1. **Check `game_id` alignment:** After `_bronze_idsse_to_sportec_input` converts tracking to frames, verify that `frames["game_id"]` is set identically to `actions["game_id"]`. If frames have `game_id=None` or a string match_id, the groupby fails silently.
2. **Check `team_id` type alignment:** IDSSE uses string team_ids (`"DFL-CLU-000005"`). If `actions["team_id"]` is a string but `frames["team_id"]` is stored differently, the `t != action_team` check at line 198 may behave unexpectedly.
3. **Add a diagnostic:** In `detect_line_breaking`, log how many actions get matched to frame groups vs how many fall through with `opp_teams = []`.

**Verification:** Run `detect_line_breaking` locally on IDSSE J03WMX data and add logging at line 197:
```python
frame_key = (game_id, period_id, frame_id)
teams_at_frame = frame_to_teams.get(frame_key, [])
if not teams_at_frame:
    print(f"NO TEAMS for frame_key={frame_key}, available keys sample={list(frame_to_teams.keys())[:3]}")
```

---

## Bug 4: gk_pitch_control_share_weighted = 100% NULL

**Affected columns:** `gk_pitch_control_share_weighted`

**Observed:** 100% NULL across ALL matches. However, `gk_reachable_area_m2` and `gk_closing_time_*` columns ARE populated with physically correct values. This means `compute_gk_influence` IS being called and primitives (b) and (c) succeed — only primitive (a) fails.

**Root cause:** `compute_gk_influence` primitive (a) calls `compute_pitch_control(..., decompose=True)` and then `surface.player_surface(gk_player_id)`. The decomposed pitch control computation requires per-player influence attribution. If `player_surface()` raises `ValueError` (player_id not found in decomposition), or if the threat-weighted sum is zero, the result is NaN.

The `_gk_influence_at_actions` error handler (features.py:2126-2132) catches `ValueError`/`KeyError` silently:
```python
try:
    gi = compute_gk_influence(...)
    cache[cache_key] = gi
except (ValueError, KeyError) as exc:
    _warnings.warn(...)
    cache[cache_key] = None
```

But since `gk_reachable_area_m2` IS populated, the function isn't raising. Instead, `compute_gk_influence` returns a `GkInfluence` object where `pitch_control_share_weighted = NaN` (from the `total_weight < 1e-8` branch at `_gk_influence.py:356-358`).

**Call chain:**
```
tracking_context._enrich_match (Step 13)
  → features.add_gk_influence(actions, frames, xt, links=links, home_team_id=home_team_id)
    → features._gk_influence_at_actions(actions, frames, xt, ...)
      → _gk_influence.compute_gk_influence(frame_data, attacking_team_id, gk_player_id, xt, ...)
        → surface = compute_pitch_control(frame, ..., decompose=True)
        → gk_surface = surface.player_surface(gk_player_id)
        → team_surface = sum of all defending-team per_player_influence
        → share_grid = gk_surface / team_surface  (where team_surface > 0)
        → interp = xt.interpolator(kind="linear")
        → threat_grid = interp(surface.grid_x, surface.grid_y)
        → cell_area = surface.cell_area
        → threat_weight = threat_grid * cell_area
        → total_weight = threat_weight.sum()
        → IF total_weight < 1e-8: pitch_control_share_weighted = NaN  ← HITS THIS
```

**Where to fix:** `silly_kicks/tracking/_gk_influence.py:315-359`

**Hypothesis:** The xT model passed to `compute_gk_influence` has its grid values produce near-zero interpolated values at the pitch control surface's grid coordinates. Possible causes:
1. **xT interpolator coordinate mismatch:** `xt.interpolator(kind="linear")` may return a function that expects different coordinate conventions than `surface.grid_x / grid_y`. If xT is on a (16, 12) grid mapping to (0-105, 0-68) but the interpolator returns values on a different scale, the result could be near-zero.
2. **xT flip logic:** Lines 346-349 flip the threat grid for away-team attacks:
   ```python
   if attacking_team_id != home_team_id:
       threat_grid = threat_grid[:, ::-1]
   ```
   But the condition checks `attacking_team_id != home_team_id`. In the lakehouse, `home_team_id` is the DFL home team ID (string). `attacking_team_id` is `tid` from the action row. This comparison should work — but verify the actual values.
3. **Pitch control surface grid is unexpectedly large/small:** If `surface.grid_x` and `surface.grid_y` produce coordinates outside the xT model's domain, the interpolator returns 0.

**Diagnostic approach:**
```python
# After fitting xT and before calling add_gk_influence:
from silly_kicks.xthreat import ExpectedThreat
xt = ExpectedThreat(l=16, w=12)
xt.fit(actions)
interp = xt.interpolator(kind="linear")
# Check that the xT grid has non-zero values:
import numpy as np
x_test = np.linspace(0, 105, 50)
y_test = np.linspace(0, 68, 32)
vals = interp(x_test, y_test)
print(f"xT grid: min={vals.min():.6f}, max={vals.max():.6f}, non_zero={np.count_nonzero(vals)}")
# Also check what surface.grid_x / grid_y look like:
from silly_kicks.tracking.pitch_control import compute_pitch_control
surface = compute_pitch_control(frame_data, tid, method="spearman", decompose=True)
print(f"PC grid: x={surface.grid_x[[0,-1]]}, y={surface.grid_y[[0,-1]]}")
print(f"cell_area={surface.cell_area}")
```

**Verification:** Run `compute_gk_influence` locally on a single IDSSE frame with verbose output. Check `total_weight` value. If it's < 1e-8, trace back to which of `threat_grid`, `cell_area` is zero/near-zero.

---

## Production Evidence Summary (Updated 2026-05-17)

### Real bugs (confirmed locally):

| Bug | Column | Expected | Observed | Root Cause |
|-----|--------|----------|----------|-----------|
| 5 | `result_id` (IDSSE/Metrica shots) | 1 for goals | 0 (always) | DFL/Metrica converter doesn't map goals |
| 2 | `gk_was_engaged` | True for some shots | false (always) | No `keeper_*` SPADL actions in DFL/Metrica stream |
| 3 | `line_break__ward` (IDSSE) | True for some passes | 0/NULL (always) | `game_id`/`team_id` type mismatch in frame groupby |

### Not bugs (artifacts of Bug 5, confirmed working with proper xT):

| Column | Local Result (with xT) | Status |
|--------|----------------------|--------|
| `blocking_score` | 17/24 non-zero, max=11.73 | WORKS |
| `blocked_threat_fraction` | 17/24 non-zero, max=0.17 | WORKS |
| `gk_pitch_control_share_weighted` | 22/24 non-null | WORKS |

### Columns verified as CORRECT (both production and local):
- All spatial coordinates (0-105, 0-68)
- All 3 pitch control variants (0.0-1.0)
- `pressure_on_actor__bekkers_pi` (0.0-1.0)
- DAS (0-1221 m²) — after Fix D (`.sum()` instead of `.iloc[0]`)
- Team shape (14 columns, all physically reasonable)
- Defensive line + compactness + lateral width
- Off-ball runs (threshold method)
- `line_break` + `n_attackers_behind_line` (threshold method — works for BOTH providers)
- Sync score
- `gk_reachable_area_m2`, `gk_closing_time_*` (non-null, reasonable ranges)
- `n_blocked_receivers`, `n_potential_receivers` (non-zero, lane control fires correctly)
- `blocking_score`, `blocked_threat_fraction` (non-zero with proper xT)

---

## silly-kicks Version

```
silly-kicks >= 3.15.1, < 4
```

Installed: check `uv pip show silly-kicks` or `silly_kicks.__version__`.

---

## Test Harness (for local reproduction)

To reproduce these locally without Spark:

```python
import pandas as pd
from silly_kicks.xthreat import ExpectedThreat
from ingestion.tracking_context import _enrich_match

# Load real bronze data (extracted via scripts/extract_tracking_fixtures.py)
actions = pd.read_parquet("src/tests/fixtures/tracking_context/idsse_J03WMX_actions.parquet")
frames = pd.read_parquet("src/tests/fixtures/tracking_context/idsse_J03WMX_p1_frames.parquet")

# Fit xT on the actions
xt = ExpectedThreat(l=16, w=12)
xt.fit(actions)

# Run the exact same enrichment code
result = _enrich_match(
    actions=actions,
    frames=frames,
    xt=xt,
    home_team_id="DFL-CLU-000005",  # from events
    match_id_native="J03WMX",
    data_source="idsse",
)

# Check the broken columns
print(result["blocking_score"].describe())
print(result["gk_was_engaged"].value_counts())
print(result["line_break__ward"].value_counts())
print(result["gk_pitch_control_share_weighted"].describe())
```

---

## Priority Ranking (Updated 2026-05-17)

1. **Bug 5 (goals not mapped for IDSSE/Metrica)** — CRITICAL. Affects xT fitting, xG downstream, any goal-counting analytics. Must fix in SPADL converters.
2. **Bug 3 (Ward IDSSE = 0)** — Likely a `game_id` or `team_id` type mismatch in the frame groupby lookup. Metrica works, IDSSE doesn't → data-shape or type difference.
3. **Bug 2 (gk_was_engaged = false)** — Inherent limitation for event-sparse providers. May need tracking-based behavioral detection as alternative.
4. ~~Bug 1 (blocking_score = 0)~~ — NOT A BUG. Works with proper xT.
5. ~~Bug 4 (gk_pitch_control_share = NULL)~~ — NOT A BUG. Works with proper xT.
