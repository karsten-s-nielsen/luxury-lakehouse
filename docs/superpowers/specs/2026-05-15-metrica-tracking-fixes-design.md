# Metrica Tracking Fixes + SkillCorner Enablement

**Date:** 2026-05-18
**Scope:** Single PR — TDD (tests first, then fixes)
**Depends on:** PR #289 (all previous TC-1 fixes merged), PR #292 (team_id hash)

## Problem

Three issues from production run 830163656900015:

### Issue 1: Metrica speed/bekkers_pi NULL (residual from PR #289)

**Root cause:** `tracking_context.py:439` reads `actions["player_id"]` (NULL for Metrica per ADR-016) instead of `actions["player_id_native"]`. The jersey-to-pid lookup dict is empty → fallback format `"Player{jersey}"` (no space) mismatches Game 3 SPADL `"Player 10"` (with space). Verified locally via reproduction script.

**Fix:** One-line: `_pid_col = "player_id"` → `_pid_col = "player_id_native"`.

### Issue 2: SkillCorner tracking context FAILED

**Root cause (layered):**
1. `_SKILLCORNER_TRACKING_SELECT_COLS` references `team`, `is_goalkeeper`, `home_team_id` — columns that don't exist in `bronze.skillcorner_tracking` (ingestion parser only extracts match_id, period, frame, timestamp, player_id, x, y, is_visible, ball_x, ball_y, ball_z, ball_is_detected, frame_rate)
2. `_resolve_enrichment_identity` raises `NotImplementedError` for `provider="skillcorner"`
3. Driver reads `home_team_id` from tracking table (line 1741) — column doesn't exist

**Fix:** Join `bronze.skillcorner_matches` at compute time to add `team`, `is_goalkeeper`, `home_team_id`. Don't add to bronze — these are metadata from match.json, not from the tracking JSONL source (follows "Bronze = raw source" principle).

- Change `_SKILLCORNER_TRACKING_SELECT_COLS` to only select bronze-native columns
- Spark JOIN with matches on `player_id` to add `team` (cast team_id to string), `is_goalkeeper` (position_acronym == "GK")
- Read `home_team_id` from `bronze.skillcorner_matches` instead of tracking
- Implement SkillCorner identity resolution: `team_id = team_id_native` (both stringified numeric), `player_id = player_id_native` (stringified numeric)
- Convert `player_id` to string in `_bronze_skillcorner_to_frames` for type consistency with other providers

### Issue 3: Retries not disabled (Terraform)

**Root cause:** Terraform Databricks provider treats `max_retries = 0` as Go zero-value, omits from API payload. Platform default (1 retry) applies.

**Fix:** Use `retry_on_timeout = false` (boolean, no zero-value issue) + document the `max_retries = 0` limitation. If provider doesn't support `retry_on_timeout`, use `max_retries = -1` or post-deploy API call.

## Test Strategy

TDD: Write failing tests → implement fix → verify green. Local-only (no Spark, no Databricks).

- Identity resolution: test SkillCorner branch produces correct team_id/player_id
- Converter: test `_bronze_skillcorner_to_frames` with synthetic data
- Player_id lookup: test `player_id_native` is used (not `player_id`) for jersey lookup
- Column projection: update existing parity tests
- E2E: run `_enrich_match` with SkillCorner-shaped synthetic data
