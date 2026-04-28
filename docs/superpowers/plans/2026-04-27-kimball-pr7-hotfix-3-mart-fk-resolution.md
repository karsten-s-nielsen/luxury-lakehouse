# Kimball PR 7 Hotfix #3 — Mart Kimball-FK Resolution Implementation Plan

> **For agentic workers:** This plan is executed inline (per `feedback_no_approval_asks_in_plan_execution` + `feedback_agent_tool_requires_per_call_approval` + `feedback_no_reviewer_subagents_in_execution`) — no subagent dispatch. Use checkbox (`- [ ]`) syntax for tracking. **Single commit at end of phase 7** per `feedback_single_commit_squash` (do NOT commit per task). Only git gates (commit, push, PR create, merge, branch delete) pause for explicit user approval per `feedback_only_git_gates_need_approval`.

**Goal:** Fix every PR-7 latent Kimball-FK resolution failure (12 catastrophic 100%-NULL + 8 partial-coverage) at the staging boundary, introduce two new shared bridge views to resolve formations and match-summary marts, and add structural test guards (relationships + per-provider parameterized) so the same class of bug fails dbt-CI on first build for any future drift.

**Architecture:** Single principle — *staging canonicalizes native ids to dim-compatible form*. In-place rewrites at `stg_idsse__tracking` (strip `idsse_` prefix) and `stg_metrica__tracking` (synth `player_id`) eliminate per-mart workarounds. Two new intermediate bridges (`int_tracking__player_match_team_bridge` per-(match, player) and `int_tracking__match_side_team_bridge` per-(match, side)) supply the team_id mapping that formations marts need. `stg_idsse__home_away_teams` is deleted and subsumed by the side bridge. `fct_match_summary` replaces its "cannot be pivoted" IDSSE/Metrica branches with the side-bridge JOIN. Tests parameterize per-(mart, key, provider) so single-provider drift fails against the named provider, not behind aggregate counts.

**Tech Stack:** dbt 1.10–1.12 / Databricks Spark SQL, dbt_utils, dbt_expectations, pyright, ruff, pytest, Python 3.10, Databricks SDK.

**Spec source:** `docs/superpowers/specs/2026-04-27-kimball-pr7-hotfix-3-design.md`.

**Branch:** `kimball-pr7-hotfix-3-mart-fk-resolution` (already created from main at squash commit `dba6183`).

---

## File map

**Create:**
- `dbt_project/models/intermediate/int_tracking__player_match_team_bridge.sql` (NEW)
- `dbt_project/models/intermediate/int_tracking__match_side_team_bridge.sql` (NEW)

**Modify (staging — in-place native-id canonicalization):**
- `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` — strip `idsse_` prefix from match_id
- `dbt_project/models/staging/metrica/stg_metrica__tracking.sql` — `player_id` becomes synth form
- `dbt_project/models/staging/idsse/stg_idsse__passes.sql` — update `ball_at_end_frame` CTE to read new bridge

**Modify (intermediate YAML — bridges):**
- `dbt_project/models/intermediate/_intermediate__models.yml` — add entries for both new bridges

**Modify (staging YAML — drop deleted view):**
- `dbt_project/models/staging/idsse/_idsse__models.yml` — remove `stg_idsse__home_away_teams` entry

**Delete:**
- `dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql`

**Modify (marts — bridge JOINs):**
- `dbt_project/models/marts/fct_player_positions.sql` — JOIN player-match bridge → dim_teams → team_key
- `dbt_project/models/marts/fct_position_maps.sql` — same pattern
- `dbt_project/models/marts/fct_formation_labels.sql` — JOIN side bridge → dim_teams → team_key
- `dbt_project/models/marts/fct_match_summary.sql` — replace IDSSE/Metrica branches with side bridge

**Modify (mart YAML — relationships + not_null guards):**
- `dbt_project/models/marts/_marts__models.yml` — per-PR-7 FK column tests

**Modify (Python tests — per-(mart, key, provider) parameterization):**
- `src/tests/test_marts_kimball_contracts.py` — replace `_CASES` placeholder thresholds

**Investigate (fct_shots 3 NULL player_key):**
- Decision branch in Phase 6: either extend `dim_players` generator (`scripts/generate_entity_xref.py` likely) OR add upstream filter in `int_unified_shots`.

---

## Phase 0 — Pre-flight

### Task 0: Verify branch state + dim/staging cardinalities

- [ ] **Step 0.1: Confirm branch exists and is clean**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
git status -s
git rev-parse HEAD
git log --oneline -3 origin/main
```

Expected: branch `kimball-pr7-hotfix-3-mart-fk-resolution` checked out, current HEAD = main's `dba6183`, 5 untracked plan/spec files (keep untracked per session rule).

- [ ] **Step 0.2: Verify dim cardinalities + staging team_id coverage**

```bash
uv run --with databricks-sql-connector python -c "
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    for stg in ['stg_idsse__tracking','stg_metrica__tracking','stg_skillcorner__tracking']:
        cur.execute(f'SELECT COUNT(*), COUNT(team_id), COUNT(player_id) FROM soccer_analytics.dev_silver.{stg}')
        print(f'{stg}:', cur.fetchone())
"
```

Expected: each staging view returns ~equal `total / team_id / player_id` non-null counts. (If team_id is NULL on bronze IDSSE, escalate to user — that's a separate ingest bug.)

---

## Phase 1 — Static test additions (TDD: tests fail today, SQL changes make them pass)

### Task 1: Add YAML schema tests for new bridges

**Files:**
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`

The bridges don't exist yet (Phase 3); these YAML entries describe the future tables and their tests. dbt parse won't fail — it just records the schema for when models exist.

- [ ] **Step 1.1: Add bridge YAML entries**

Append to `dbt_project/models/intermediate/_intermediate__models.yml` (place near other intermediate models — after `int_unified_passes` entry):

```yaml
  - name: int_tracking__player_match_team_bridge
    description: >
      Per-(source_provider, match_id, player_id) → team_id bridge for tracking-derived
      facts. Materialized as table (small ~616-row footprint avoids repeat 38M-row
      DISTINCT scans). PR 7 hotfix #3.
    columns:
      - name: source_provider
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse', 'metrica', 'skillcorner']
      - name: match_id
        data_type: string
        data_tests:
          - not_null
      - name: player_id
        data_type: string
        data_tests:
          - not_null
      - name: team_id
        data_type: string
        data_tests:
          - not_null
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [source_provider, match_id, player_id]

  - name: int_tracking__match_side_team_bridge
    description: >
      Per-(source_provider, match_id, side='home'/'away') → team_id bridge. Generalises
      the deleted stg_idsse__home_away_teams across all 3 tracking providers. Used by
      fct_match_summary to resolve home/away team_keys for IDSSE/Metrica/SkillCorner
      and by fct_formation_labels for per-side team resolution. PR 7 hotfix #3.
    columns:
      - name: source_provider
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['idsse', 'metrica', 'skillcorner']
      - name: match_id
        data_type: string
        data_tests:
          - not_null
      - name: side
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['home', 'away']
      - name: team_id
        data_type: string
        data_tests:
          - not_null
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [source_provider, match_id, side]
```

- [ ] **Step 1.2: Verify YAML is valid**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt parse 2>&1 | tail -5
```

Expected: no parse errors (only existing deprecation warnings).

### Task 2: Add per-PR-7 FK relationships + not_null tests to mart YAML

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

For every mart with PR-7 Kimball-FK columns, add `not_null` (per-provider filtered where some providers have legitimate gaps) and `relationships` tests.

- [ ] **Step 2.1: Locate fct_action_values column entries**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
grep -n "name: fct_action_values" dbt_project/models/marts/_marts__models.yml | head -2
```

Read the lines around the match to find `team_key` and `player_key` column entries. Add tests inline.

- [ ] **Step 2.2: Update fct_action_values team_key + player_key entries**

Find the `fct_action_values` section in `_marts__models.yml`. Update `team_key` entry:

```yaml
      - name: team_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_teams (PR 4b — populated for SB+WS via
          dim_teams JOIN on (provider, native_team_id=cast(team_id as string))).
        data_tests:
          - not_null:
              config:
                where: "data_source IN ('statsbomb', 'wyscout')"
          - relationships:
              arguments:
                to: ref('dim_teams')
                field: team_key
```

Update `player_key` entry similarly:

```yaml
      - name: player_key
        data_type: bigint
        description: >
          Kimball surrogate FK to dim_players (PR 4b — populated for SB+WS via
          dim_players JOIN on (provider, native_player_id=cast(player_id as string))).
        data_tests:
          - not_null:
              config:
                where: "data_source IN ('statsbomb', 'wyscout')"
          - relationships:
              arguments:
                to: ref('dim_players')
                field: player_key
```

- [ ] **Step 2.3: Update fct_tracking_frames team_key + player_key + match_key entries**

Locate the fct_tracking_frames section. Update entries:

```yaml
      - name: match_key
        data_type: bigint
        description: Kimball surrogate FK to dim_matches (PR 7 hotfix #3 — populated for all 3 tracking providers post idsse_ prefix strip at staging).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_matches')
                field: match_key
      - name: team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams (PR 7 hotfix #3 — populated for all 3 tracking providers via canonicalized native_team_id).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_teams')
                field: team_key
      - name: player_key
        data_type: bigint
        description: Kimball surrogate FK to dim_players (PR 7 hotfix #3 — populated for all 3 tracking providers post Metrica Player-prefix strip at staging).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_players')
                field: player_key
```

- [ ] **Step 2.4: Update fct_player_positions, fct_position_maps, fct_formation_labels FK entries**

For each of the three formations marts, locate match_key + team_key + player_key entries (where present). Apply this pattern:

```yaml
      - name: match_key
        data_type: bigint
        description: Kimball surrogate FK to dim_matches (PR 7 hotfix #3 — populated post staging canonicalization).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_matches')
                field: match_key
      - name: team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams (PR 7 hotfix #3 — resolved via int_tracking__player_match_team_bridge or int_tracking__match_side_team_bridge JOIN to dim_teams).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_teams')
                field: team_key
      - name: player_key
        data_type: bigint
        description: Kimball surrogate FK to dim_players (PR 7 hotfix #3 — populated for all 3 tracking providers).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_players')
                field: player_key
```

For `fct_formation_labels` (per-side, no player_key column): only add match_key + team_key tests.

- [ ] **Step 2.5: Update fct_match_summary home_team_key + away_team_key entries**

```yaml
      - name: home_team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams (home team) — PR 7 hotfix #3 extended resolution to all providers via int_tracking__match_side_team_bridge.
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_teams')
                field: team_key
      - name: away_team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams (away team).
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_teams')
                field: team_key
```

- [ ] **Step 2.6: Update fct_physical_stats + fct_off_ball_xt match_key + player_key entries**

These are downstream of fct_tracking_frames — apply the standard pattern from Step 2.3 (not_null + relationships).

- [ ] **Step 2.7: Verify YAML is valid**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt parse 2>&1 | tail -5
```

Expected: clean parse.

### Task 3: Refactor `test_marts_kimball_contracts.py` to per-(mart, key, provider) parameterization

**Files:**
- Modify: `src/tests/test_marts_kimball_contracts.py`

Replace the placeholder-`0.0` PR-7 entries with calibrated per-provider expectations.

- [ ] **Step 3.1: Read the existing _CASES tuple**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
grep -n "PR 7" src/tests/test_marts_kimball_contracts.py | head
```

Note the line range of PR-7 entries (approximately lines 87-130 per prior diagnosis).

- [ ] **Step 3.2: Replace PR-7 _CASES entries**

Replace the PR-7 block in `_CASES` with the parameterized form. Keep the existing PR-5b/PR-6 entries unchanged. The block to replace starts after the `# PR 6 — fct_gk_actions_detail` comment lines and continues to the closing parenthesis of `_CASES`.

Use this replacement (parameterized to a 4-tuple `(mart, key, provider, threshold)` — bump the test signature accordingly):

```python
# PR 7 hotfix #3 — per-(mart, key, provider) calibrated thresholds.
# Three patterns:
#   1. Strict 1.0 — JOIN MUST resolve 100%; failure indicates recipe drift or new data gap.
#   2. 0.0 with comment — structural source-data gap (e.g., Wyscout has no recipient field).
#   3. Calibrated <1.0 — true partial coverage with documented reason; commit measured value.
("fct_passes", "team_key", "statsbomb", 1.0),
("fct_passes", "team_key", "wyscout", 1.0),
("fct_passes", "team_key", "idsse", 1.0),
("fct_passes", "team_key", "metrica", 1.0),
("fct_passes", "passer_player_key", "statsbomb", 1.0),
("fct_passes", "passer_player_key", "wyscout", 1.0),
("fct_passes", "passer_player_key", "idsse", 1.0),
("fct_passes", "passer_player_key", "metrica", 1.0),
("fct_passes", "recipient_player_key", "statsbomb", 1.0),  # SB has recipient on every pass
("fct_passes", "recipient_player_key", "wyscout", 0.0),    # WS open-data has NO recipient field — kloppy strips at parse
("fct_passes", "recipient_player_key", "idsse", 0.5),      # CALIBRATE post-rebuild
("fct_passes", "recipient_player_key", "metrica", 0.5),    # CALIBRATE post-rebuild

("fct_action_values", "team_key", "statsbomb", 1.0),
("fct_action_values", "team_key", "wyscout", 1.0),
("fct_action_values", "player_key", "statsbomb", 1.0),
("fct_action_values", "player_key", "wyscout", 1.0),

("fct_shots", "team_key", "statsbomb", 1.0),
("fct_shots", "team_key", "wyscout", 1.0),
("fct_shots", "player_key", "statsbomb", 1.0),  # Phase 6 may relax to 0.99998 if 3 NULL rows are source-data gaps
("fct_shots", "player_key", "wyscout", 1.0),

("fct_line_breaking_results", "team_key", "statsbomb", 1.0),
("fct_line_breaking_results", "player_key", "statsbomb", 1.0),

("fct_match_summary", "home_team_key", "statsbomb", 1.0),
("fct_match_summary", "home_team_key", "wyscout", 1.0),
("fct_match_summary", "home_team_key", "idsse", 1.0),
("fct_match_summary", "home_team_key", "metrica", 1.0),
("fct_match_summary", "away_team_key", "statsbomb", 1.0),
("fct_match_summary", "away_team_key", "wyscout", 1.0),
("fct_match_summary", "away_team_key", "idsse", 1.0),
("fct_match_summary", "away_team_key", "metrica", 1.0),

("fct_tracking_frames", "match_key", "idsse", 1.0),
("fct_tracking_frames", "match_key", "metrica", 1.0),
("fct_tracking_frames", "match_key", "skillcorner", 1.0),
("fct_tracking_frames", "team_key", "idsse", 1.0),
("fct_tracking_frames", "team_key", "metrica", 1.0),
("fct_tracking_frames", "team_key", "skillcorner", 1.0),
("fct_tracking_frames", "player_key", "idsse", 1.0),
("fct_tracking_frames", "player_key", "metrica", 1.0),
("fct_tracking_frames", "player_key", "skillcorner", 1.0),

("fct_player_positions", "match_key", "idsse", 1.0),
("fct_player_positions", "match_key", "metrica", 1.0),
("fct_player_positions", "match_key", "skillcorner", 1.0),
("fct_player_positions", "team_key", "idsse", 1.0),
("fct_player_positions", "team_key", "metrica", 1.0),
("fct_player_positions", "team_key", "skillcorner", 1.0),
("fct_player_positions", "player_key", "idsse", 1.0),
("fct_player_positions", "player_key", "metrica", 1.0),
("fct_player_positions", "player_key", "skillcorner", 1.0),

("fct_position_maps", "match_key", "idsse", 1.0),
("fct_position_maps", "match_key", "metrica", 1.0),
("fct_position_maps", "match_key", "skillcorner", 1.0),
("fct_position_maps", "team_key", "idsse", 1.0),
("fct_position_maps", "team_key", "metrica", 1.0),
("fct_position_maps", "team_key", "skillcorner", 1.0),
("fct_position_maps", "player_key", "idsse", 1.0),
("fct_position_maps", "player_key", "metrica", 1.0),
("fct_position_maps", "player_key", "skillcorner", 1.0),

("fct_formation_labels", "match_key", "idsse", 1.0),
("fct_formation_labels", "match_key", "metrica", 1.0),
("fct_formation_labels", "match_key", "skillcorner", 1.0),
("fct_formation_labels", "team_key", "idsse", 1.0),
("fct_formation_labels", "team_key", "metrica", 1.0),
("fct_formation_labels", "team_key", "skillcorner", 1.0),

("fct_physical_stats", "match_key", "idsse", 1.0),
("fct_physical_stats", "match_key", "metrica", 1.0),
("fct_physical_stats", "match_key", "skillcorner", 1.0),
("fct_physical_stats", "player_key", "idsse", 1.0),
("fct_physical_stats", "player_key", "metrica", 1.0),
("fct_physical_stats", "player_key", "skillcorner", 1.0),

("fct_off_ball_xt", "match_key", "idsse", 1.0),
("fct_off_ball_xt", "match_key", "metrica", 1.0),
("fct_off_ball_xt", "match_key", "skillcorner", 1.0),
("fct_off_ball_xt", "player_key", "idsse", 1.0),
("fct_off_ball_xt", "player_key", "metrica", 1.0),
("fct_off_ball_xt", "player_key", "skillcorner", 1.0),

("fct_pass_timing", "match_key", "idsse", 1.0),
("fct_pass_timing", "player_key", "idsse", 1.0),
("fct_pausa_rankings", "player_key", "idsse", 1.0),
```

- [ ] **Step 3.3: Update the test signature to accept the provider parameter**

The test function signature changes from `(mart, key, threshold)` to `(mart, key, provider, threshold)`. Update the SQL it runs:

```python
@requires_databricks
@pytest.mark.parametrize(("mart", "key_column", "provider", "threshold"), _CASES_PR7)
def test_kimball_key_populated_per_provider(conn, mart: str, key_column: str, provider: str, threshold: float) -> None:
    """For each (mart, key, provider) combo, non-NULL rate must meet threshold."""
    catalog = "soccer_analytics"
    schema = "dev_gold"
    table = f"{catalog}.{schema}.{mart}"

    # Identify the provider column. Most marts use 'data_source'; tracking marts use 'source_provider'.
    cur = conn.cursor()
    cur.execute(f"DESCRIBE {table}")  # noqa: S608 — table is constant, mart is constrained by tuple
    cols = {row[0] for row in cur.fetchall() if row[0] and not row[0].startswith("#")}
    provider_col = "source_provider" if "source_provider" in cols else "data_source"

    cur.execute(
        f"SELECT count(*) AS total, count({key_column}) AS non_null "  # noqa: S608 — params constrained
        f"FROM {table} WHERE {provider_col} = '{provider}'"
    )
    row = cur.fetchone()
    if row is None:
        pytest.skip(f"empty result on {table} for provider={provider}")
        return
    total, non_null = int(row[0]), int(row[1])

    if total == 0:
        pytest.skip(f"{mart} has zero rows for provider={provider} — feature gate may be off")
        return

    rate = non_null / total
    assert rate >= threshold, (
        f"{mart}.{key_column} provider={provider}: non-NULL rate {rate:.4f} below "
        f"{threshold} threshold (total={total}, non_null={non_null}). "
        f"Investigate dim resolution recipe drift."
    )
```

Rename the existing `_CASES` PR-7 block to `_CASES_PR7` (4-tuple) and split the test function into the legacy 3-tuple test (for PR-5b/PR-6 entries) plus the new 4-tuple test above.

- [ ] **Step 3.4: Run static-pattern test (will skip without DATABRICKS_*)**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run pytest src/tests/test_marts_kimball_contracts.py --collect-only 2>&1 | tail -15
```

Expected: ~80+ test cases collected, including the new per-provider parameterizations.

- [ ] **Step 3.5: Run lint + format on the test file**

```bash
uv run ruff check src/tests/test_marts_kimball_contracts.py
uv run ruff format src/tests/test_marts_kimball_contracts.py
uv run pyright src/tests/test_marts_kimball_contracts.py
```

Expected: ruff All checks passed, format already-formatted, pyright 0/0/0.

---

## Phase 2 — Staging in-place rewrites

### Task 4: stg_idsse__tracking — strip `idsse_` prefix from match_id

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`

- [ ] **Step 4.1: Apply the prefix strip**

In `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`, replace the `match_id` line in the `normalized` CTE (around line 23):

Find:
```sql
        -- Match context
        match_id,
```

Replace with:
```sql
        -- Match context
        -- PR 7 hotfix #3: strip the `idsse_` bronze prefix at staging boundary so
        -- downstream consumers (fct_tracking_frames, dim_matches JOINs) receive the
        -- canonical native form. Pre-fix: 100% of fct_tracking_frames IDSSE rows had
        -- match_key=NULL because match_id='idsse_J03WMX' couldn't match
        -- dim_matches.native_match_id='J03WMX'. Downstream regexp_replace strips in
        -- stg_idsse__home_away_teams (DELETED in this hotfix) and
        -- stg_idsse__passes.ball_at_end_frame become idempotent no-ops on already-
        -- clean strings; the regex matches nothing and returns input unchanged.
        regexp_replace(cast(match_id as string), '^idsse_', '') as match_id,
```

- [ ] **Step 4.2: Compile to verify SQL is valid**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select stg_idsse__tracking 2>&1 | grep -E "Found|Error|exit" | head -3
echo "exit=$?"
```

Expected: compile succeeds, exit=0. (The dbt logger may print verbose error-level lines for long SQL; ignore — only true compile errors matter.)

### Task 5: stg_metrica__tracking — synth `player_id`

**Files:**
- Modify: `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`

The bronze tracking JSON map keys are bare numerics (`'1'`, `'11'`); `dim_players` synthesises `metrica_<match>_<side>_<map_key>`. Make staging emit the synth form so fct_tracking_frames JOINs to dim_players resolve.

- [ ] **Step 5.1: Apply the synth concat**

In `dbt_project/models/staging/metrica/stg_metrica__tracking.sql`, find the `player_id,` line in the `normalized` CTE (around line 104):

Find:
```sql
        -- Player identity
        player_id,
        team,
```

Replace with:
```sql
        -- Player identity
        -- PR 7 hotfix #3: synthesize the dim_players-compatible native_player_id.
        -- Bronze tracking JSON map keys are bare numerics (e.g., '5', '11'); dim_players
        -- (stg_metrica__team_players, PR 5a) synthesizes
        -- `metrica_<match>_<side>_<map_key>`. Pre-fix: 100% of fct_tracking_frames Metrica
        -- rows had player_key=NULL because the bare key 5 couldn't JOIN the synth form.
        -- This canonicalization preserves the bare key internally as the lateral-view
        -- variable `player_key` (still accessible if any future consumer needs it).
        concat('metrica_', match_id, '_', team, '_', player_id) as player_id,
        team,
```

Note: the `player_id` referenced in the `concat` is the `player_key as player_id` from the upstream `home_players_exploded` / `away_players_exploded` CTEs (line 31 / line 59). That's the bare numeric. After concat, the `normalized` CTE's `player_id` column contains the synth form.

- [ ] **Step 5.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select stg_metrica__tracking 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

---

## Phase 3 — Bridges

### Task 6: Create int_tracking__player_match_team_bridge

**Files:**
- Create: `dbt_project/models/intermediate/int_tracking__player_match_team_bridge.sql`

- [ ] **Step 6.1: Create the bridge file**

```sql
{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, player_id, team_id) — the per-match
-- player→team mapping needed by formations marts where the formations
-- algorithm output strips team labels and dim_players doesn't carry team_key.
--
-- Sourced from the three stg_*__tracking views (NOT fct_tracking_frames) to
-- avoid the circular dependency where a formations mart depending on a
-- fct_tracking_frames-derived bridge would force fact_tracking_frames to
-- be built before formations marts. Tracking staging is provider-canonical
-- post-PR-7-hotfix-#3 (idsse_ prefix stripped, metrica player_id synth form),
-- so the union here works directly.
--
-- Materialized as table (not view): bridge cardinality is ~616 rows total.
-- The DISTINCT collapse over 38M underlying tracking rows is the expensive
-- operation; tabling pays it once at build, then 4 downstream consumer JOINs
-- hit a tiny lookup table. View materialization would force a fresh 38M-row
-- distinct on every consumer JOIN.

with idsse_pmt as (
    select distinct
        'idsse'         as source_provider,
        cast(match_id as string)     as match_id,
        cast(player_id as string)    as player_id,
        cast(team_id as string)      as team_id
    from {{ ref('stg_idsse__tracking') }}
    where team_id is not null
      and player_id is not null
),
metrica_pmt as (
    select distinct
        'metrica'       as source_provider,
        cast(match_id as string)     as match_id,
        cast(player_id as string)    as player_id,
        cast(team_id as string)      as team_id
    from {{ ref('stg_metrica__tracking') }}
    where team_id is not null
      and player_id is not null
),
skillcorner_pmt as (
    select distinct
        'skillcorner'   as source_provider,
        cast(match_id as string)     as match_id,
        cast(player_id as string)    as player_id,
        cast(team_id as string)      as team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team_id is not null
      and player_id is not null
)

select * from idsse_pmt
union all select * from metrica_pmt
union all select * from skillcorner_pmt
```

- [ ] **Step 6.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select int_tracking__player_match_team_bridge 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

### Task 7: Create int_tracking__match_side_team_bridge

**Files:**
- Create: `dbt_project/models/intermediate/int_tracking__match_side_team_bridge.sql`

- [ ] **Step 7.1: Create the bridge file**

```sql
{{ config(materialized='table', schema='silver') }}

-- One row per (source_provider, match_id, side='home'/'away', team_id) — the
-- per-match home/away team mapping needed by:
--   * fct_match_summary IDSSE / Metrica / SkillCorner home/away resolution
--     (replacing the "cannot be pivoted" branches with a bridge JOIN).
--   * fct_formation_labels per-(match, side) team_key resolution.
--   * stg_idsse__passes.ball_at_end_frame (formerly read stg_idsse__home_away_teams,
--     now reads this filtered to source_provider='idsse').
--
-- Generalises the deleted stg_idsse__home_away_teams across all 3 tracking providers.
-- Sourced from the three stg_*__tracking views (post-PR-7-hotfix-#3 staging
-- canonicalization).
--
-- Materialized as table for the same reason as int_tracking__player_match_team_bridge:
-- 40 rows total; tabling avoids repeat 38M-row DISTINCT scans across consumers.

with idsse_mst as (
    select distinct
        'idsse'         as source_provider,
        cast(match_id as string)     as match_id,
        cast(team as string)         as side,
        cast(team_id as string)      as team_id
    from {{ ref('stg_idsse__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
metrica_mst as (
    select distinct
        'metrica'       as source_provider,
        cast(match_id as string)     as match_id,
        cast(team as string)         as side,
        cast(team_id as string)      as team_id
    from {{ ref('stg_metrica__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
),
skillcorner_mst as (
    select distinct
        'skillcorner'   as source_provider,
        cast(match_id as string)     as match_id,
        cast(team as string)         as side,
        cast(team_id as string)      as team_id
    from {{ ref('stg_skillcorner__tracking') }}
    where team in ('home', 'away')
      and team_id is not null
)

select * from idsse_mst
union all select * from metrica_mst
union all select * from skillcorner_mst
```

- [ ] **Step 7.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select int_tracking__match_side_team_bridge 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

---

## Phase 4 — Delete stg_idsse__home_away_teams + update consumer

### Task 8: Update stg_idsse__passes to read the new bridge

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__passes.sql`

- [ ] **Step 8.1: Replace the bridge JOIN target**

In `dbt_project/models/staging/idsse/stg_idsse__passes.sql`, locate the `hydrated` CTE (around line 71). The CTE's `left join {{ ref('stg_idsse__home_away_teams') }} bridge` must be replaced.

Find:
```sql
hydrated as (

    select
        e.*,
        coalesce(
            case
                when e.play_team like 'DFL-CLU-%' then cast(e.play_team as string)
                else cast(null as string)
            end,
            bridge.team_id
        )                                                       as bridge_team_id
    from events_with_native_match_id e
    left join {{ ref('stg_idsse__home_away_teams') }} bridge
        on bridge.match_id = e.native_match_id
       and bridge.side = lower(e.play_team)

),
```

Replace with:
```sql
hydrated as (

    -- PR 7 hotfix #3: stg_idsse__home_away_teams was deleted. Its replacement is
    -- int_tracking__match_side_team_bridge filtered to source_provider='idsse'.
    -- Functionally identical to the old bridge but generalises across all 3
    -- tracking providers (single source of truth for per-(match, side)→team_id).
    select
        e.*,
        coalesce(
            case
                when e.play_team like 'DFL-CLU-%' then cast(e.play_team as string)
                else cast(null as string)
            end,
            bridge.team_id
        )                                                       as bridge_team_id
    from events_with_native_match_id e
    left join {{ ref('int_tracking__match_side_team_bridge') }} bridge
        on bridge.source_provider = 'idsse'
       and bridge.match_id = e.native_match_id
       and bridge.side = lower(e.play_team)

),
```

- [ ] **Step 8.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select stg_idsse__passes 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

### Task 9: Delete stg_idsse__home_away_teams.sql + remove YAML entry

**Files:**
- Delete: `dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql`
- Modify: `dbt_project/models/staging/idsse/_idsse__models.yml` (remove the model entry)

- [ ] **Step 9.1: Locate the YAML entry**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
grep -n "stg_idsse__home_away_teams" dbt_project/models/staging/idsse/_idsse__models.yml
```

Note the line range of the `- name: stg_idsse__home_away_teams` block.

- [ ] **Step 9.2: Remove the YAML entry**

In `dbt_project/models/staging/idsse/_idsse__models.yml`, find the entire `- name: stg_idsse__home_away_teams` model block (description + columns + tests) and delete it. Surrounding entries (other models in the file) stay intact.

- [ ] **Step 9.3: Delete the SQL file**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
git rm dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql
```

- [ ] **Step 9.4: Verify dbt parse picks up the deletion**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt parse 2>&1 | tail -10
```

Expected: clean parse. No "model not found" errors (no remaining consumers of the deleted model).

---

## Phase 5 — Mart updates

### Task 10: fct_player_positions — add bridge JOIN for team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_player_positions.sql`

The mart currently emits `cast(null as bigint) as team_key` (or has no team_key column at all). Add the bridge JOIN.

- [ ] **Step 10.1: Read the existing JOIN block**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
grep -n "left join\|team_key" dbt_project/models/marts/fct_player_positions.sql | head -10
```

Note where the existing `dim_matches` and `dim_players` JOINs are (around lines 95-100).

- [ ] **Step 10.2: Add bridge + dim_teams JOIN to the final SELECT chain**

In `dbt_project/models/marts/fct_player_positions.sql`, find the final JOIN block (around line 92):

Find:
```sql
        on  pp.match_id = tm.match_id
       and pp.player_id = tm.player_id
    left join {{ ref('dim_matches') }} dm
       and dm.native_match_id = pp.match_id
    left join {{ ref('dim_players') }} dp
       and dp.native_player_id = pp.player_id
```

(The exact prefix/structure may differ — adapt to the existing code; the spirit is to add two JOINs after the existing dim_players JOIN.)

Replace the JOIN sequence to add the bridge resolution. Append after the existing dim_players JOIN:

```sql
    -- PR 7 hotfix #3: resolve team_key via per-(match, player)→team_id bridge.
    -- Formations algorithm output strips team labels; dim_players doesn't carry
    -- team_key (per-player career grain). The bridge gives us the per-match
    -- team_id; JOIN dim_teams resolves to the BIGINT surrogate.
    left join {{ ref('int_tracking__player_match_team_bridge') }} pmtb
        on  pmtb.source_provider = pp.source_provider
       and pmtb.match_id = pp.match_id
       and pmtb.player_id = pp.player_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = pp.source_provider
       and dt.native_team_id = pmtb.team_id
```

Then add `dt.team_key as team_key` to the final SELECT list (replacing the existing `cast(null as bigint) as team_key` if present, or adding alongside other key columns).

- [ ] **Step 10.3: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select fct_player_positions 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

### Task 11: fct_position_maps — same pattern as Task 10

**Files:**
- Modify: `dbt_project/models/marts/fct_position_maps.sql`

- [ ] **Step 11.1: Apply the same bridge JOIN pattern**

In `dbt_project/models/marts/fct_position_maps.sql`, locate the existing dim JOIN block and append the bridge + dim_teams JOIN identical to Task 10.2 (substituting the mart's CTE alias `fc` if used instead of `pp`):

```sql
    -- PR 7 hotfix #3: resolve team_key via int_tracking__player_match_team_bridge.
    left join {{ ref('int_tracking__player_match_team_bridge') }} pmtb
        on  pmtb.source_provider = fc.source_provider
       and pmtb.match_id = fc.match_id
       and pmtb.player_id = fc.player_id
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = fc.source_provider
       and dt.native_team_id = pmtb.team_id
```

Update the final SELECT to emit `dt.team_key as team_key`.

- [ ] **Step 11.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select fct_position_maps 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

### Task 12: fct_formation_labels — use side bridge for team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_formation_labels.sql`

Different shape: per-(match, side='home'/'away') not per-player. Use the side bridge.

- [ ] **Step 12.1: Add side bridge JOIN**

In `dbt_project/models/marts/fct_formation_labels.sql`, locate the existing JOIN block (after `formation_labels` selection and before final SELECT). Append:

```sql
    -- PR 7 hotfix #3: resolve team_key via per-(match, side)→team_id bridge.
    -- formation_labels.team is 'home'/'away' and source_provider identifies the
    -- tracking source. Bridge resolves the per-match team_id; dim_teams gives
    -- the BIGINT surrogate.
    left join {{ ref('int_tracking__match_side_team_bridge') }} mstb
        on  mstb.source_provider = formation_labels.source_provider
       and mstb.match_id = formation_labels.match_id
       and mstb.side = formation_labels.team
    left join {{ ref('dim_teams') }} dt
        on  dt.provider = formation_labels.source_provider
       and dt.native_team_id = mstb.team_id
```

Add `dt.team_key as team_key` to the final SELECT.

- [ ] **Step 12.2: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select fct_formation_labels 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

### Task 13: fct_match_summary — REPLACE IDSSE/Metrica branches with side bridge

**Files:**
- Modify: `dbt_project/models/marts/fct_match_summary.sql`

This is the most surgical change in this hotfix. The existing CTE `match_team_ids` (around lines 62-72) extracts SB/WS team_id pivots from events. The mart has dead-code branches for IDSSE/Metrica that conclude "cannot be pivoted home/away" — those must be REPLACED, not augmented.

- [ ] **Step 13.1: Read the existing IDSSE/Metrica branch**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
sed -n '60,180p' dbt_project/models/marts/fct_match_summary.sql
```

Identify the CTEs / SELECT branches that currently say "IDSSE/Metrica have NULL team_id and so cannot be pivoted home/away" (around lines 91, 165-188 per the earlier diagnosis).

- [ ] **Step 13.2: Replace IDSSE/Metrica branches**

Add a new CTE (place after `match_team_ids`):

```sql
-- PR 7 hotfix #3: per-(match, side)→team_id resolution for IDSSE/Metrica/SkillCorner
-- via int_tracking__match_side_team_bridge. Replaces the prior "cannot be pivoted"
-- code path. The bridge provides the canonical home/away team_id mapping for
-- every tracking-aware provider; dim_teams resolves to team_key.
match_team_ids_tracking as (
    select
        dm.match_key,
        max(case when mstb.side = 'home' then dt.team_key end) as home_team_key,
        max(case when mstb.side = 'home' then dt.team_id end)  as home_team_id,
        max(case when mstb.side = 'home' then dt.team_name end) as home_team_name,
        max(case when mstb.side = 'away' then dt.team_key end) as away_team_key,
        max(case when mstb.side = 'away' then dt.team_id end)  as away_team_id,
        max(case when mstb.side = 'away' then dt.team_name end) as away_team_name
    from {{ ref('int_tracking__match_side_team_bridge') }} mstb
    inner join {{ ref('dim_matches') }} dm
        on  dm.provider = mstb.source_provider
       and dm.native_match_id = mstb.match_id
    inner join {{ ref('dim_teams') }} dt
        on  dt.provider = mstb.source_provider
       and dt.native_team_id = mstb.team_id
    group by dm.match_key
),
```

Then in the final SELECT chain (where home_team_key / away_team_key are emitted), replace the existing IDSSE/Metrica `cast(null as bigint)` placeholders with a `coalesce()` between the SB/WS event-pivot result and the new tracking-bridge result. The exact SQL depends on existing structure — pattern:

```sql
        coalesce(<SB/WS pivot>.home_team_key, mtit.home_team_key) as home_team_key,
        coalesce(<SB/WS pivot>.away_team_key, mtit.away_team_key) as away_team_key,
        coalesce(<SB/WS pivot>.home_team_id,  mtit.home_team_id)  as home_team_id,
        coalesce(<SB/WS pivot>.away_team_id,  mtit.away_team_id)  as away_team_id,
```

with `mtit` aliasing the new `match_team_ids_tracking` CTE. Add `LEFT JOIN match_team_ids_tracking mtit USING (match_key)` to the final-SELECT FROM clause.

**Implementation requirement: REMOVE the dead "cannot be pivoted" code path entirely.** Do NOT leave commented-out blocks. Single resolution path per provider — SB/WS via events, IDSSE/Metrica/SkillCorner via the new bridge.

- [ ] **Step 13.3: Compile to verify**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt compile --select fct_match_summary 2>&1 | grep -E "Found|Error" | head -3
```

Expected: clean compile.

---

## Phase 6 — fct_shots 3-NULL investigation + fix

### Task 14: Diagnose fct_shots.player_key 3 NULL rows

**Files:**
- Decision branch — outcome determines next file modifications.

- [ ] **Step 14.1: Identify the 3 NULL rows**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run --with databricks-sql-connector python -c "
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    cur.execute('''
        SELECT data_source, player_id, team_id, COUNT(*) cnt
        FROM soccer_analytics.dev_gold.fct_shots
        WHERE player_key IS NULL
        GROUP BY 1, 2, 3
    ''')
    print('NULL player_key shots:')
    for r in cur.fetchall(): print(' ', r)
    # Check if these player_ids exist in dim_players
    cur.execute('''
        SELECT s.player_id, s.data_source,
               EXISTS(SELECT 1 FROM soccer_analytics.dev_gold.dim_players dp
                      WHERE dp.provider = s.data_source AND dp.native_player_id = cast(s.player_id as string)) as in_dim
        FROM soccer_analytics.dev_gold.fct_shots s
        WHERE s.player_key IS NULL
        GROUP BY 1, 2
    ''')
    print('Existence check:')
    for r in cur.fetchall(): print(' ', r)
"
```

- [ ] **Step 14.2: Decide based on diagnosis**

If `in_dim` is **False** for all 3 rows: it's a **dim_players coverage gap**. Path A: extend the dim_players generator (likely `scripts/generate_entity_xref.py` or similar). Find the file and add the missing players. Re-run the generator on dev to regenerate dim_players. Threshold stays 1.0.

If `in_dim` is **True** for all 3 rows: it's a **JOIN recipe drift** (cast difference, encoding, etc.). Path B: investigate `fct_shots` / `int_unified_shots` JOIN to `dim_players` for whichever `data_source` is affected.

If `player_id` is **NULL** in the source for all 3 rows: it's a **source-data gap** (rare SB/WS shots without player attribution). Path C: relax `fct_shots.player_key.statsbomb` threshold in `_CASES_PR7` from 1.0 to 0.99998 with explicit comment.

- [ ] **Step 14.3: Apply chosen fix**

Apply Path A, B, or C based on Step 14.2 diagnosis. Document the chosen path inline in `test_marts_kimball_contracts.py` near the fct_shots threshold entry.

- [ ] **Step 14.4: Compile + lint affected files (if any code changed)**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
# If Python changed:
uv run ruff check src/ scripts/ 2>&1 | tail -3
uv run ruff format --check src/ scripts/ 2>&1 | tail -3
# If dbt SQL changed:
cd dbt_project && uv run --extra dbt dbt parse 2>&1 | tail -3
```

Expected: all clean.

---

## Phase 7 — Local validation

### Task 15: Run all local checks

- [ ] **Step 15.1: ruff check**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run ruff check src/ scripts/ 2>&1 | tail -5
```

Expected: All checks passed.

- [ ] **Step 15.2: ruff format check**

```bash
uv run ruff format --check src/ scripts/ 2>&1 | tail -3
```

Expected: All files already formatted.

- [ ] **Step 15.3: pyright**

```bash
uv run pyright src/ 2>&1 | tail -5
```

Expected: 0 errors, 0 warnings, 0 informations.

- [ ] **Step 15.4: dbt parse**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32/dbt_project"
uv run --extra dbt dbt parse 2>&1 | tail -5
```

Expected: clean parse.

- [ ] **Step 15.5: dbt compile on changed-and-downstream models**

```bash
uv run --extra dbt dbt compile --select \
    +int_tracking__player_match_team_bridge+ \
    +int_tracking__match_side_team_bridge+ \
    fct_action_values fct_tracking_frames fct_match_summary \
    fct_player_positions fct_position_maps fct_formation_labels \
    2>&1 | grep -E "Found|Error" | head -5
```

Expected: clean compile (deprecation warnings are fine; ignore the verbose dbt-logger long-line errors — only true compile errors matter).

- [ ] **Step 15.6: pytest static portion of contract tests**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run pytest src/tests/test_marts_kimball_contracts.py --collect-only 2>&1 | tail -10
```

Expected: ~80+ tests collected including the new per-provider parameterizations. If running with DATABRICKS_* env vars, expect ~12-30 tests to FAIL — that's correct, they assert the post-rebuild state of dev_gold which hasn't been rebuilt yet.

---

## Phase 8 — Commit + push (USER GATE)

### Task 16: Stage, commit, push (USER APPROVAL REQUIRED — pause for explicit approval)

- [ ] **Step 16.1: Stage all changed files**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
git status -s
git add \
  dbt_project/models/intermediate/int_tracking__player_match_team_bridge.sql \
  dbt_project/models/intermediate/int_tracking__match_side_team_bridge.sql \
  dbt_project/models/intermediate/_intermediate__models.yml \
  dbt_project/models/staging/idsse/stg_idsse__tracking.sql \
  dbt_project/models/staging/idsse/stg_idsse__passes.sql \
  dbt_project/models/staging/idsse/_idsse__models.yml \
  dbt_project/models/staging/metrica/stg_metrica__tracking.sql \
  dbt_project/models/marts/fct_player_positions.sql \
  dbt_project/models/marts/fct_position_maps.sql \
  dbt_project/models/marts/fct_formation_labels.sql \
  dbt_project/models/marts/fct_match_summary.sql \
  dbt_project/models/marts/_marts__models.yml \
  src/tests/test_marts_kimball_contracts.py
git rm dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql
git status -s
```

Expected: 13 modified, 1 deleted, 5 untracked plan/spec files (kept untracked per session rule).

- [ ] **Step 16.2: PAUSE — surface diff summary to user, await commit approval**

Surface to user:
- Diff stat: `git diff --stat HEAD`
- Proposed commit message (drafted below).
- Ask: "OK to commit + push + open PR?"

- [ ] **Step 16.3: Commit (after user approval)**

```bash
git commit -m "$(cat <<'EOF'
fix(kimball-pr7-hotfix-3): mart Kimball-FK resolution + canonical native-id staging

Closes 12 catastrophic 100%-NULL Kimball-FK columns + 8 partial-coverage cases
surfaced by Phase 2 step 15 contract-threshold calibration.

Single principle: staging canonicalizes native ids to dim-compatible form.

Staging in-place rewrites:
- stg_idsse__tracking: strip 'idsse_' prefix from match_id (downstream consumers
  see canonical form; redundant downstream regex strips become no-ops on already-
  clean strings).
- stg_metrica__tracking: synthesize player_id as concat('metrica_', match_id, '_',
  team, '_', map_key) to match dim_players' synth recipe.

New shared bridges (Kimball factless-fact pattern):
- int_tracking__player_match_team_bridge: per-(provider, match, player) → team_id
  for formations marts. Materialized as table — 616 rows; tabling avoids repeat
  38M-row DISTINCT scans across 4 consumers.
- int_tracking__match_side_team_bridge: per-(provider, match, side='home'/'away')
  → team_id for fct_match_summary + fct_formation_labels. Generalises the
  deleted stg_idsse__home_away_teams across all 3 tracking providers.

Mart updates:
- fct_player_positions / fct_position_maps: JOIN player-match bridge → dim_teams
  → team_key.
- fct_formation_labels: JOIN side bridge → dim_teams → team_key.
- fct_match_summary: REPLACE the prior "cannot be pivoted" IDSSE/Metrica branches
  with side-bridge JOIN. Single resolution path per provider (SB/WS via events,
  IDSSE/Metrica/SkillCorner via tracking bridge).

Deleted:
- stg_idsse__home_away_teams.sql (subsumed by int_tracking__match_side_team_bridge).
- Single consumer (stg_idsse__passes.ball_at_end_frame) updated to read the new
  bridge filtered to source_provider='idsse'.

Structural test guards (catches recurrence on first build):
- _intermediate__models.yml: not_null + unique_combination_of_columns + accepted_values
  on both bridges.
- _marts__models.yml: not_null (per-provider filtered where structural gaps exist) +
  relationships on every PR-7 Kimball-FK column across affected marts.
- test_marts_kimball_contracts.py: per-(mart, key, provider) parameterized
  thresholds replacing 0.0 placeholders. Single-provider drift surfaces against
  the named provider, not behind aggregates.

Stale-incremental marts (auto-fix on full-refresh, no SQL change):
fct_action_values, fct_tracking_frames, fct_physical_stats, fct_off_ball_xt all
have on_schema_change='append_new_columns'; PR 7 added new key columns, existing
rows kept NULL until --full-refresh. Post-merge deploy uses wide-selector
--full-refresh.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 16.4: Push to origin**

```bash
git push -u origin kimball-pr7-hotfix-3-mart-fk-resolution 2>&1 | tail -5
```

Expected: branch published, PR URL printed.

- [ ] **Step 16.5: Open PR**

```bash
gh pr create --base main --head kimball-pr7-hotfix-3-mart-fk-resolution \
  --title "fix(kimball-pr7-hotfix-3): mart Kimball-FK resolution + canonical native-id staging" \
  --body "$(cat <<'EOF'
## Summary

Closes the third class of latent PR-7 bugs surfaced during Phase 2 step 15 contract-threshold calibration — 12 (mart, key) pairs at 100% NULL plus 8 partial-coverage cases. Same shape as PRs #215 + #216: PR 7 introduced Kimball-FK columns onto fact marts but the staging-side native-id resolution wasn't uniformly correct across all providers.

## Bug class

The staging layer didn't uniformly canonicalize native ids to the form `dim_*` carries, so fact-side LEFT JOINs to dim tables either fail entirely or fail per-provider:

- **IDSSE**: `stg_idsse__tracking.match_id` carried `idsse_` prefix; downstream consumers each stripped it locally rather than at staging boundary.
- **Metrica**: bronze tracking JSON keys are bare numerics (`'5'`); event `player` column is `'Player5'`; `dim_players` synth is `metrica_<match>_<side>_5` — three different forms; staging didn't canonicalize.
- **Formations marts**: source data lacks team info; `dim_players` lacks team_key; no bridge existed.
- **Stale-incremental marts**: `on_schema_change='append_new_columns'` populates new columns only on new rows; existing rows stay NULL until `--full-refresh`.

## Single principle

> **The staging layer is where native ids are canonicalized to the form `dim_*` carries.**

This hotfix enforces that. Two new shared bridge views supply the team-id mapping that formations marts and `fct_match_summary` IDSSE/Metrica need; the deleted `stg_idsse__home_away_teams` is subsumed by the side bridge.

## Spec source

`docs/superpowers/specs/2026-04-27-kimball-pr7-hotfix-3-design.md` (committed alongside this PR's plan but kept untracked per project session rule).

## Test plan

- [ ] dbt-live-CI on this PR rebuilds affected marts with corrected staging — 8 new parameterized live tests + dbt schema relationships tests turn green.
- [ ] Post-merge dev deploy: wide-selector `--full-refresh` (~90-180 min) produces 100% non-NULL on every PR-7 Kimball FK across all expected providers.
- [ ] `pytest src/tests/test_marts_kimball_contracts.py -v` — 0 failures on parameterized per-provider tests.
- [ ] Refresh synced tables; verify all online.
- [ ] Resume Phase 2 step 16 onward (live-invariant test sweep + HF dataset republish).

## Provider-add scaling test gap

Tracked separately as `G4` in `TODO.md` — the L0 structural test infrastructure that would have caught the entire PR-7-era bug class on first commit. Out of scope for this hotfix (test infrastructure design is its own work).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Phase 9 — CI verification

### Task 17: Watch CI on the PR until green

- [ ] **Step 17.1: Start CI watch in background**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
PR=$(gh pr view --json number --jq '.number')
gh pr checks $PR --watch 2>&1 | tee /tmp/hotfix3_ci.log &
```

Expected workflows: `lint-and-test`, `live-build` (dbt-live-CI), `validate` (Terraform), `semgrep`. live-build is the slowest (~6-10 min for the wider mart rebuild).

- [ ] **Step 17.2: Wait for completion**

ScheduleWakeup with 540s delay; on wake check `gh pr checks $PR` and assert all pass.

- [ ] **Step 17.3: If lint-and-test fails**

Read the failed-step output:
```bash
gh run view <run-id> --log-failed 2>&1 | tail -50
```

Most likely class: pyright errors on the `databricks.sql` import pattern (recurrent issue from hotfixes #1 + #2). Fix → format/lint clean → push CI-fix commit (per PR #215 + #216 precedent — second commit on same branch is fine; squash-merge collapses).

- [ ] **Step 17.4: If live-build fails**

The live test parameterizations fail until the dbt-live-CI rebuilds the affected marts. dbt-live-CI runs `dbt build --select state:modified+` so it WILL rebuild the changed marts on this PR. If a per-provider test still fails post-rebuild, that's a real bug — investigate before merging.

---

## Phase 10 — Squash-merge (USER GATE)

### Task 18: Merge after CI green (USER APPROVAL REQUIRED)

- [ ] **Step 18.1: PAUSE — confirm all CI green, surface for merge approval**

```bash
gh pr checks $PR | tail -10
```

All four checks must show `pass`. Surface to user; await explicit "approved" before merging.

- [ ] **Step 18.2: Squash-merge after approval**

```bash
gh pr merge $PR --squash 2>&1 | tail -3
gh pr view $PR --json state,mergeCommit
```

Expected: state=MERGED, mergeCommit OID printed. Note this OID — needed for Phase 12 deploy step naming and Phase 14 memory updates.

- [ ] **Step 18.3: Sync local main**

```bash
git checkout main
git pull
git log --oneline -3
```

Expected: top commit is the new squash commit.

---

## Phase 11 — Wheel deploy + dbt full-refresh

### Task 19: Wait for main-branch Python CI to deploy wheel

- [ ] **Step 19.1: Watch main-branch Python CI**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
RUN_ID=$(gh run list --branch main --workflow "Python CI" --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch $RUN_ID 2>&1 | tail -5 &
```

Expected runtime: 5-10 min. Sleeps ~6 min via ScheduleWakeup.

- [ ] **Step 19.2: Verify wheel deploy step succeeded**

```bash
gh run view $RUN_ID --json conclusion,status
gh run view $RUN_ID --json jobs --jq '.jobs[0].steps[] | select(.name | test("wheel|Run tests")) | {name, conclusion}'
```

Expected: conclusion=success, all wheel-related steps success.

### Task 20: Trigger dbt build --full-refresh via Databricks runs/submit

**Files:**
- Stage: `/tmp/submit_dbt_hotfix3.py`

- [ ] **Step 20.1: Stage the submit script**

```bash
cat > /tmp/submit_dbt_hotfix3.py <<'PYEOF'
"""Submit dbt build --full-refresh for hotfix #3 wide-selector deploy."""
from __future__ import annotations
import sys, time, json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute as compute_service

w = WorkspaceClient()

env_spec = jobs.JobEnvironment(
    environment_key="dbt",
    spec=compute_service.Environment(
        client="1",
        dependencies=[
            "/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.3.19-py3-none-any.whl",
            "dbt-core>=1.10.0",
            "dbt-databricks>=1.10.0",
        ],
    ),
)

# Wide selector — covers ~25-30 marts via descendant closure. Single dbt run
# resolves the DAG and builds in dependency order.
task = jobs.SubmitTask(
    task_key="dbt_hotfix3_wide_full_refresh",
    environment_key="dbt",
    timeout_seconds=14400,  # 4-hour ceiling (180-min realistic estimate)
    python_wheel_task=jobs.PythonWheelTask(
        package_name="luxury_lakehouse",
        entry_point="dbt_build",
        parameters=[
            "--select",
            "+int_tracking__player_match_team_bridge+",
            "+int_tracking__match_side_team_bridge+",
            "fct_action_values+",
            "fct_tracking_frames+",
            "fct_match_summary+",
            "--full-refresh",
        ],
    ),
)

resp = w.jobs.submit(
    run_name="dbt full-refresh hotfix #3 (wide selector)",
    timeout_seconds=14400,
    environments=[env_spec],
    tasks=[task],
)
run_id = resp.run_id
print(json.dumps({"run_id": run_id}), flush=True)

IN_FLIGHT = {"PENDING", "RUNNING", "TERMINATING", "QUEUED", "BLOCKED", "WAITING_FOR_RETRY"}
attempt = 0
max_attempts = 960  # 4-hour ceiling at 15s polls
while True:
    run = w.jobs.get_run(run_id=run_id)
    state = run.state
    life = state.life_cycle_state.value if state and state.life_cycle_state else ""
    result = state.result_state.value if state and state.result_state else None
    msg = state.state_message if state else None
    page = run.run_page_url
    print(f"[poll attempt={attempt}] life={life} result={result} msg={msg}", flush=True)
    if life not in IN_FLIGHT:
        print(json.dumps({
            "run_id": run_id,
            "life_cycle_state": life,
            "result_state": result,
            "run_page_url": page,
        }), flush=True)
        sys.exit(0 if result == "SUCCESS" else 1)
    attempt += 1
    if attempt >= max_attempts:
        print("TIMEOUT", flush=True)
        sys.exit(2)
    time.sleep(15)
PYEOF
echo "Wrote /tmp/submit_dbt_hotfix3.py"
```

- [ ] **Step 20.2: Submit + poll**

```bash
uv run python /tmp/submit_dbt_hotfix3.py 2>&1 | tee /tmp/submit_dbt_hotfix3.log &
```

Run in background; first ScheduleWakeup at 600s, then every 600s until done.

- [ ] **Step 20.3: Verify SUCCESS**

```bash
tail -5 /tmp/submit_dbt_hotfix3.log
```

Expected: terminal JSON `{"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", ...}`.

- [ ] **Step 20.4: If FAILED — diagnose + branch**

If `result_state` is `FAILED`, check the run page URL for the dbt error message. Most likely classes:

1. **Schema-test failure on a relationships test** — a recipe drift we missed. Investigate, file as a follow-up commit on the SAME branch; user approval needed for re-deploy.
2. **Compile error** — should have been caught locally; investigate cause.
3. **Timeout (180+ min)** — abort; split selector per spec §3.2.7 Phase 1 + Phase 2.

---

## Phase 12 — Post-deploy verification

### Task 21: Verify per-(mart, key, provider) coverage

- [ ] **Step 21.1: Run the parameterized live test**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run pytest src/tests/test_marts_kimball_contracts.py -v 2>&1 | tail -50
```

Expected: ALL parameterized per-provider tests pass.

- [ ] **Step 21.2: Manual full-coverage cross-check**

```bash
uv run --with databricks-sql-connector python -c "
import os, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from databricks import sql
host = os.environ['DATABRICKS_HOST'].replace('https://','').rstrip('/')
PAIRS = [
    ('fct_passes', 'team_key', 'data_source'),
    ('fct_passes', 'passer_player_key', 'data_source'),
    ('fct_action_values', 'team_key', 'data_source'),
    ('fct_action_values', 'player_key', 'data_source'),
    ('fct_tracking_frames', 'match_key', 'source_provider'),
    ('fct_tracking_frames', 'team_key', 'source_provider'),
    ('fct_tracking_frames', 'player_key', 'source_provider'),
    ('fct_player_positions', 'match_key', 'source_provider'),
    ('fct_player_positions', 'team_key', 'source_provider'),
    ('fct_player_positions', 'player_key', 'source_provider'),
    ('fct_position_maps', 'match_key', 'source_provider'),
    ('fct_position_maps', 'team_key', 'source_provider'),
    ('fct_formation_labels', 'match_key', 'source_provider'),
    ('fct_formation_labels', 'team_key', 'source_provider'),
    ('fct_physical_stats', 'match_key', 'source_provider'),
    ('fct_physical_stats', 'player_key', 'source_provider'),
    ('fct_off_ball_xt', 'match_key', 'source_provider'),
    ('fct_off_ball_xt', 'player_key', 'source_provider'),
    ('fct_match_summary', 'home_team_key', None),
    ('fct_match_summary', 'away_team_key', None),
]
with sql.connect(server_hostname=host, http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN']) as c:
    cur = c.cursor()
    for mart, key, provider_col in PAIRS:
        if provider_col:
            cur.execute(f'SELECT {provider_col}, COUNT(*) total, COUNT({key}) non_null FROM soccer_analytics.dev_gold.{mart} GROUP BY 1 ORDER BY 1')
            rows = cur.fetchall()
            print(f'{mart}.{key}:')
            for r in rows:
                rate = r[2]/r[1] if r[1] else 0.0
                marker = '[OK]' if rate >= 0.99 else '[FAIL]'
                print(f'  {marker} {r[0]:12s} total={r[1]:>10,d} non_null={r[2]:>10,d} rate={rate:.4f}')
        else:
            cur.execute(f'SELECT COUNT(*) total, COUNT({key}) non_null FROM soccer_analytics.dev_gold.{mart}')
            r = cur.fetchone()
            rate = r[1]/r[0] if r[0] else 0.0
            marker = '[OK]' if rate >= 0.99 else '[FAIL]'
            print(f'{mart}.{key}: {marker} total={r[0]:>10,d} non_null={r[1]:>10,d} rate={rate:.4f}')
"
```

Expected: every row prints `[OK]` (rate >= 0.99). Wyscout `recipient_player_key` and any explicitly-acknowledged structural gaps will show 0.0 — acceptable per the per-provider thresholds in `_CASES_PR7`.

- [ ] **Step 21.3: If any row prints `[FAIL]`**

Treat as a regression. Investigate the specific (mart, key, provider) combo. Fix on the same branch (new commit) → re-deploy. NO follow-up TODO per the user's "no follow-up TODOs" directive.

---

## Phase 13 — Refresh synced tables

### Task 22: Refresh affected Lakebase synced tables

- [ ] **Step 22.1: Trigger refresh**

```bash
cd "D:/Development/karstenskyt__luxury-lakehouse-d32"
uv run python -m ingestion.refresh_synced_tables --wait 2>&1 | tee /tmp/refresh_hotfix3.log &
```

Run in background; first wake at 600s.

- [ ] **Step 22.2: Verify all online**

After refresh script exits:

```bash
tail -10 /tmp/refresh_hotfix3.log | grep -E "COMPLETE|FAILED|ERROR|TIMEOUT"
```

Expected: ~40 of 41 tables COMPLETE (fct_tracking_frames_synced may take longer for initial snapshot if recreated; check separately).

- [ ] **Step 22.3: Check fct_tracking_frames_synced specifically**

```bash
uv run python -c "
import requests
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()
host = ws.config.host
headers = ws.config.authenticate()
r = requests.get(f'{host}/api/2.0/database/synced_tables/soccer_analytics.dev_gold.fct_tracking_frames_synced', headers=headers, verify=True, timeout=(10,30))
print(r.json().get('data_synchronization_status', {}).get('detailed_state'))
"
```

Expected: `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`. If still in initial snapshot, sleep 600s + recheck.

---

## Phase 14 — Memory + ADR finalization (post-deploy)

### Task 23: Update project memory + ADRs

- [ ] **Step 23.1: Update `project_kimball_pr7_phase2_resume.md`**

Mark Phase 2 step 14-16 as ✅ done. Update Phase 2 status from "MID-FLIGHT" to reflect hotfix #3 having shipped.

- [ ] **Step 23.2: Append hotfix #3 row to `project_kimball_migration_cycle.md`**

Add row mirroring the existing PR #215 + #216 entries:

```
| PR 7 hotfix #3 | Mart Kimball-FK resolution + canonical native-id staging — 12 catastrophic 100%-NULL + 8 partial cases. Two staging in-place rewrites + two new shared bridge views + 4 mart updates + structural test guards. | **MERGED 2026-04-XX** PR #<num> (squash commit `<oid>`). |
```

- [ ] **Step 23.3: Update MEMORY.md index**

Add or update the index entry for the hotfix #3 cycle.

- [ ] **Step 23.4: ADR-011 staged-rollout closure**

Confirm ADR-011's PR 7 row reads `Shipped (2026-04-XX, <hotfix-3-oid>) — closed via PR 7 main + #215 + #216 + #<hotfix-3-num>`.

---

## Phase 15 — Resume Phase 2 step 17+ onward

### Task 24: Hand back to the parent Phase 2 plan

Phase 2 step 17 (HF dataset payload republishes) onward proceeds against the now-correctly-resolved dev_gold marts. No HF dataset would have shipped with 100%-NULL columns visible to consumers.

- [ ] **Step 24.1: Confirm parent plan resumption**

The hotfix #3 cycle closes. Resume per `project_kimball_pr7_phase2_resume.md` Phase 2 step 17 onward.

---

## Self-review checklist (run before handing back to user)

- [ ] **Spec coverage**: every section in `2026-04-27-kimball-pr7-hotfix-3-design.md` has a corresponding task.
- [ ] **Placeholder scan**: zero "TBD", "TODO", "implement later", "fill in details" in step bodies.
- [ ] **Type consistency**: bridge column names match across YAML / SQL / mart JOIN clauses (`source_provider`, `match_id`, `player_id`, `team_id`, `side`).
- [ ] **Test signature consistency**: `_CASES_PR7` is 4-tuple `(mart, key, provider, threshold)`; new test function accepts the same; `_CASES` legacy 3-tuple unchanged for PR-5b/PR-6 entries.
- [ ] **Selector consistency**: deploy selector in Phase 11 matches spec §3.2.7.
- [ ] **All git gates marked USER APPROVAL REQUIRED**: Step 16.2, Step 18.1.
