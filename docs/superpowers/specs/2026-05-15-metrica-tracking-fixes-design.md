# Metrica Tracking Fixes + Local Integration Tests

**Date:** 2026-05-15
**Scope:** Single PR — TDD (tests first, then fixes)
**Depends on:** silly-kicks >= 3.15.1 (already pinned)

## Problem

The tracking context enrichment pipeline (`bronze.spadl_tracking_context`) has 7 outstanding bugs. silly-kicks 3.15.1 automatically fixes 3 of them via pin bump. The remaining 4 require lakehouse code changes, and there is no way to catch them locally — every fix currently requires a wheel deploy + Databricks task run (~1 hour round-trip). This spec covers the 4 lakehouse fixes and the local integration test infrastructure to prevent future regressions.

## Bug Inventory

### Auto-fixed by silly-kicks 3.15.1 (no lakehouse code change)

| # | Bug | Fix |
|---|-----|-----|
| 2 | GK features NULL | `defending_gk_from_frames()` fallback wired in `add_pre_shot_gk_context` |
| 3 | blocking_score always 0.0 | Mutual-exclusion 1:1 assignment replaces greedy union |
| 7 | ward line_break always FALSE (all IDSSE) | `_derive_end_coordinates()` gives pass-class actions proper end coords |

### Lakehouse fixes required (this PR)

| # | Bug | Root Cause | File | Fix |
|---|-----|-----------|------|-----|
| A | Metrica G1+G2 ball_x/ball_y all NULL (0/145K, 0/141K) | `_build_player_columns` line 102 checks `jersey == "Ball"` but "Ball" is in column_row not jersey_row | `metrica_tracking.py:102` | `jersey == "Ball" or stripped == "Ball"` |
| B | Metrica G3 actor_speed all NULL (0/1282) | `_bronze_metrica_to_frames` line 1104 hardcodes `f"Player{jersey}"` but G3 SPADL uses `"Player 22"` (with space) | `tracking_context.py:1104` | Data-driven jersey-to-pid lookup from SPADL actions |
| C | J03WN1/J03WOY crash on NULL team_id_native | `_resolve_enrichment_identity` line 526 raises ValueError when batch has only the single NULL-team freekick_short | `tracking_context.py:526` | Check non-null subset; skip NULL-team rows in enrichment |
| D | DAS uses deprecated `get_das()` (asymmetric) | Inline DAS bypass (lines 693-759) calls `get_das()` which returns per-frame scalar DAS, not per-player decomposition | `tracking_context.py:697,714,723` | Switch to `get_individual_das()` + per-team `.sum()` |

### Deferred (not this PR)

| # | Bug | Reason |
|---|-----|--------|
| 9 | Timeout on J03WQQ/J03WR9 (1800s) | Terraform change, separate from code fixes. Will bump to 3600s in a dedicated TF PR. |

## Metrica Raw Data Inconsistencies

Metrica's 3 sample games were hand-curated over several years and are wildly inconsistent. The fixes must work for all 3 as-is. No more games will be released.

| Dimension | Game 1 (CSV) | Game 2 (CSV) | Game 3 (EPTS) |
|-----------|-------------|-------------|---------------|
| Event player IDs | `"Player19"` (no space) | `"Player19"` (no space) | `"Player 10"` (with space) |
| Tracking JSON keys | bare jersey `"11"` | bare jersey `"25"` | bare jersey `"11"` |
| CSV column_row player names | `"Player11"` (no space) | `"Player25"` (no space), BUT `"Player 26"` (with space!) in G2 away | N/A (EPTS format) |
| Ball in CSV header | column_row (NOT jersey_row) | column_row (NOT jersey_row) | N/A (colon-delimited) |
| SPADL `player_id_native` | `"Player22"` (no space) | `"Player22"` (no space) | `"Player 22"` (with space) |
| Ball tracking coverage (bronze) | **0/145K (bug A)** | **0/141K (bug A)** | 97K/143K (68%, normal) |
| Parsing code path | `metrica_tracking._build_player_columns` | same | `metrica_common._parse_epts_tracking` |

## Fixes

### Fix A: Ball Column Parsing (`metrica_tracking.py:102`)

**Current:** `elif jersey == "Ball":` — never fires because "Ball" is in `column_row[i]` (accessed as `stripped`), not `jersey_row[i]`.

**Fix:** `elif jersey == "Ball" or stripped == "Ball":` — handles actual CSV format while remaining safe for any hypothetical format where "Ball" appears in the jersey row.

**Blast radius:** CSV path only (Games 1+2). Game 3 EPTS path is completely separate code in `metrica_common.py`.

**Verified:** End-to-end test with monkey-patched fix produces 88K/145K ball rows for Game 1 (61% coverage — normal for optical tracking dropout).

**After fix:** Re-ingest Games 1+2 to populate ball_x/ball_y. This unlocks `infer_ball_carrier` -> `derive_team_in_possession` -> DAS + bekkers_pi + blocking for Games 1+2.

### Fix B: Player ID Format (`tracking_context.py:1104`)

**Current:** `f"Player{jersey}"` hardcodes no-space format. Works for Games 1+2, breaks Game 3.

**Fix:** Data-driven lookup. At the call site (line 438), `actions` is available. Build a `jersey_to_pid` mapping from SPADL `player_id_native` values:

```python
# Call site (tracking_context.py, _build_tracking_context_udf closure):
import re as _re
_pid_natives = actions["player_id_native"].dropna().unique()
_jersey_re = _re.compile(r"Player\s*(\d+)")
jersey_to_pid: dict[str, str] = {}
for pid in _pid_natives:
    m = _jersey_re.match(str(pid))
    if m:
        jersey_to_pid[m.group(1)] = str(pid)
frames = _bronze_metrica_to_frames(pdf, game_id=game_id, jersey_to_pid=jersey_to_pid)
```

```python
# In _bronze_metrica_to_frames (line 1104):
"player_id": jersey_to_pid.get(jersey, fallback_fmt.format(jersey)),
```

The SPADL actions are the single source of truth for player ID format. The fallback handles any jersey in tracking that has no corresponding event (e.g., subs who never touched the ball). The fallback format is **data-driven** — detected from the SPADL actions to match the game's convention:

```python
# Call site — detect whether this game uses "Player 22" or "Player22":
_has_space = any(" " in str(p) for p in _pid_natives if _jersey_re.match(str(p)))
fallback_fmt = "Player {}" if _has_space else "Player{}"
frames = _bronze_metrica_to_frames(pdf, game_id=game_id, jersey_to_pid=jersey_to_pid, fallback_fmt=fallback_fmt)
```

This ensures the fallback format is consistent with the game's player ID convention. Games 1+2 (no space) get `"Player{jersey}"`, Game 3 (with space) gets `"Player {jersey}"`.

**Signature change:** `_bronze_metrica_to_frames(trk_pdf, *, game_id, jersey_to_pid, fallback_fmt)` — two new required keyword arguments.

### Fix C: NULL Team Tolerance (`tracking_context.py:526`)

**Current:** `_resolve_enrichment_identity` checks `actions["team_id_native"].dropna().empty` on ALL actions. When the batch contains only the single NULL-team freekick_short (J03WN1/J03WOY), this raises ValueError.

**Fix:** Check on the non-null subset. If some (but not all) rows have NULL team_id_native, fill those rows with a sentinel that won't match any frame team_id, so enrichment produces NaN for those actions (graceful degradation).

```python
# Replace line 526-528:
non_null_mask = actions["team_id_native"].notna()
if not non_null_mask.any():
    msg = f"team_id_native is entirely null for provider={provider}"
    raise ValueError(msg)
```

Then in the IDSSE/Metrica branches, operate only on `non_null_mask` rows:

```python
# IDSSE branch:
actions.loc[non_null_mask, "team_id"] = actions.loc[non_null_mask, "team_id_native"]
actions.loc[non_null_mask, "player_id"] = actions.loc[non_null_mask, "player_id_native"]
# NULL-team rows get NaN team_id/player_id -> enrichment produces NaN (graceful)
```

### Fix D: DAS Symmetry (`tracking_context.py:693-759`)

**Current:** Lines 697+714 call `get_das()` which returns per-frame scalar DAS. Line 723 reads `.iloc[0]` per group.

**Fix:** Switch to `get_individual_das()` which returns per-player DAS, then `.sum()` per team per frame:

```python
# Line 697: change import
from silly_kicks.tracking._das import get_individual_das

# Line 714: change call
das_result = get_individual_das(das_frames, use_progress_bar=False, chunk_size=10)

# Lines 718-723: change aggregation
player_rows = das_result[das_result["is_ball"] != True]  # noqa: E712
valid_rows = player_rows.dropna(subset=["DAS"])
das_lookup: dict[tuple, dict] = {}
for (pid, fid, tid), grp in valid_rows.groupby(["period_id", "frame_id", "team_id"]):
    das_lookup.setdefault((pid, fid), {})[tid] = float(grp["DAS"].sum())
```

The only change in the aggregation loop is `.iloc[0]` -> `.sum()`. `get_individual_das` returns per-player DAS values; summing by team gives the team-level DAS that the downstream mapping expects.

**TODO (post-PR):** This inline bypass now duplicates `_precompute_das_lookup` from `silly_kicks.tracking.features`. Long-term, the inline bypass should be replaced by calling `_precompute_das_lookup` directly (with pre-filtered frames to stay within 1 GB UDF memory). Blocked until `_precompute_das_lookup` is promoted to public API or `add_das` gains an optional frame-filter parameter.

**Memory budget (L2 review):** `get_individual_das` returns per-player rows (~22x more output rows than `get_das`). However, the inline bypass pre-filters frames to only those linked to the current batch (~12-15 frames). At this scale, per-player expansion is negligible (~300 rows). The 2.2 GB OOM risk only applies when running on ALL frames (250+ per batch). The pre-filter is the critical safety mechanism — verify during integration testing that peak RSS stays well under 800 MB for a typical 250-action batch.

## Local Integration Test Infrastructure

### Strategy

**Fixtures:** Real bronze data from Databricks, saved as local parquet files. One-time extraction via `scripts/extract_tracking_fixtures.py`. Small files (<5 MB) committed to git; large IDSSE tracking file (>50 MB) gitignored with graceful `pytest.skip()`.

**Alternative fixture source (L1 review):** Before writing the Databricks extraction script, check whether silly-kicks' own test suite already ships IDSSE fixtures (e.g., in `tests/fixtures/` or `tests/data/`). If suitable fixtures exist there, use them directly — they're already version-controlled, require no Databricks access, and are maintained by the silly-kicks team. The Databricks extraction script is only needed for fixtures not available from silly-kicks.

**TDD flow:** Tests written first, running against current (buggy) code to prove they catch the bugs. Then fixes applied, tests go green.

**No Spark dependency.** All tests run in `uv run pytest` — pure pandas, calling the same functions the UDF closure calls.

### Fixture Selection

| Fixture | Purpose | Estimated Size |
|---------|---------|----------------|
| `idsse_J03WMX_p1_tracking.parquet` | IDSSE tracking (period 1 only) — full enrichment chain test | ~30-50 MB (gitignored) |
| `idsse_J03WMX_actions.parquet` | IDSSE SPADL actions — enrichment chain test | <1 MB |
| `idsse_J03WMX_events.parquet` | IDSSE events — home_team_id resolution | <1 MB |
| `metrica_game3_tracking.parquet` | Metrica G3 tracking — player ID format test (bug B) | <2 MB |
| `metrica_game3_actions.parquet` | Metrica G3 SPADL actions — player ID match assertion | <1 MB |
| `metrica_game1_tracking.parquet` | Metrica G1 tracking — ball data test (bug A) | <2 MB |
| `metrica_game1_actions.parquet` | Metrica G1 SPADL actions — ball-dependent feature tests | <1 MB |
| `idsse_J03WN1_actions.parquet` | IDSSE SPADL actions for J03WN1 — null-team test (bug C) | <1 MB |

**IDSSE tracking extraction caveat:** The Databricks SQL Statement Execution API has a 25 MB / 100K row result limit. IDSSE J03WMX period 1 is ~750K rows. The extraction script must use chunked fetching (frame range splits) and concatenate locally.

### Test Files

| File | Tests |
|------|-------|
| `test_tracking_context_converters.py` | Ball column parsing (bug A), player ID format match (bug B), IDSSE converter schema, Metrica converter schema |
| `test_tracking_context_integration.py` | Full `_enrich_match` on real IDSSE data — hard assertions on GK (auto-fix #2), blocking (auto-fix #3), ward (auto-fix #7), DAS non-null (fix D) |
| `test_tracking_context_identity_resolution.py` (modify existing) | Add test for sparse-null-team batch (bug C) |

### Key Test Assertions

**Bug A (ball parsing):**
- Parse Game 1 home+away CSVs locally (download from GitHub, run `_build_player_columns` + `_reshape_tracking_to_narrow`)
- Assert `Ball_x` column exists in parsed DataFrame
- Assert >50% of rows have non-null `ball_x`

Note: This test downloads directly from GitHub (no Databricks fixture needed) and exercises the CSV parser code path end-to-end.

**Bug B (player ID format):**
- Load `metrica_game3_tracking.parquet` fixture, run `_bronze_metrica_to_frames`
- Load `metrica_game3_actions.parquet` fixture
- Assert every `player_id_native` in actions exists in frames `player_id` set

**Bug C (null team tolerance):**
- Load `idsse_J03WN1_actions.parquet` fixture
- Filter to the single NULL-team freekick_short action
- Call `_resolve_enrichment_identity` with this single-row batch
- Assert it does NOT raise ValueError
- Assert enrichment-resolved team_id is NaN (graceful degradation)

**Bug D (DAS symmetry):**
- In the full IDSSE integration test, assert `das_team` has >0 non-null values
- Assert `das_team != das_opponent` for at least some rows (symmetry was the old bug — per-frame scalar was identical for both teams)
- **Non-negativity:** For rows where `das_team` or `das_opponent` are non-null, assert both values are `>= 0`. This catches sign errors in the per-team aggregation.
- **Note:** DAS is NOT a zero-sum partition of pitch space. `get_individual_das()` returns per-player decomposition values that don't reconstruct to `get_das()` team totals (empirically: scalar ~0.41, individual sum ~100.5 — different quantities entirely). There is no conservation law between `das_team + das_opponent` and any meaningful total. Do NOT assert summation equality.

**Auto-fix #2 (GK):**
- In IDSSE integration test, assert `defending_gk_player_id` has >0 non-null values

**Auto-fix #3 (blocking):**
- In IDSSE integration test, assert `blocking_score` has >0 non-zero values (old bug was all 0.0)

**Auto-fix #7 (ward):**
- In IDSSE integration test, assert `line_break__ward` has >0 TRUE values (old bug was all FALSE)
- **Caveat:** `_derive_end_coordinates` fills NaN end coords from SPADL `end_x`/`end_y`, but Metrica set-piece actions (e.g., SG1 goalkick) may have NaN end coords even after derivation, producing zero-length passes → ward returns FALSE. This is expected for Metrica set pieces. The ward assertion applies to **IDSSE** data (where pass end coords are reliably present in DFL XML). A future Metrica-specific ward test should assert only on open-play passes, not set pieces.

### Memory Profiling

The existing TC-1e plan included `tracemalloc` memory tests. This is out of scope for this PR — the DAS OOM is structural (2.2 GB peak inside 1 GB UDF) and is already handled by the inline bypass with frame pre-filtering. Memory profiling adds test complexity without catching new bugs.

## Re-Ingestion

After the code fix lands and the wheel is deployed:

1. DELETE Metrica tracking bronze rows for Games 1+2
2. Re-trigger ingestion for `Sample_Game_1` and `Sample_Game_2`
3. Verify ball_x non-null counts are >50% of total rows
4. Re-trigger tracking context enrichment for all 3 Metrica games
5. Re-trigger tracking context enrichment for all IDSSE matches **except J03WQQ and J03WR9** (timeout-prone — see below)

**Timeout dependency (M2 review):** IDSSE matches J03WQQ and J03WR9 currently time out at the 1800s limit. The timeout bump to 3600s is a separate TF PR (bug #9). Re-triggering enrichment for these two matches before the timeout bump will fail. Either:
- (a) Land the TF timeout PR first, then re-trigger all IDSSE matches including J03WQQ/J03WR9, **or**
- (b) Exclude J03WQQ/J03WR9 from the initial re-enrichment run, re-trigger them after the TF PR lands.

Option (b) is the default — it decouples the code-fix PR from the TF PR and avoids blocking validation on infra changes.

This is an operational step, not a code change. The PR itself only contains code fixes + tests.

## Known Limitations

- **Metrica events-side SPADL quality:** silly-kicks' Metrica DataFrame converter uses exact-match on CHALLENGE subtypes (`sub_raw == "WON"`), which misses SG1's compound subtypes like `"TACKLE-WON"` and `"GROUND-WON"`. Result: **zero tackles and zero fouls** in SG1 SPADL output (206/233 challenges silently dropped as `non_action`). SG1 also lacks CARRY events (SG2 has them via kloppy synthesis). This is a silly-kicks converter issue — silly-kicks will fix independently (compound-subtype parsing + FAULT extraction from CHALLENGE subtypes). Lakehouse should expect sparse action variety on Metrica SG1 until the silly-kicks fix ships. Downstream impact: any tracking enrichment features that depend on tackle or foul actions existing in SPADL (e.g., pressure context around tackles) will be vacuous on Metrica SG1.

## Out of Scope

- **Timeout bump (bug #9):** Terraform change, separate PR
- **Memory profiling tests:** DAS OOM is structural, already mitigated by inline bypass
- **Metrica data quality beyond these 3 games:** User confirmed no more games will be released
- **SkillCorner tracking context:** Separate provider, separate enrichment path
