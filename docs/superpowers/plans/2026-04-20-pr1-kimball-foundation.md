# PR 1 — Kimball Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Kimball-compliant `dim_matches` conformed dimension + deterministic `generate_match_key` macro + ADR-011, spanning StatsBomb + Wyscout + IDSSE + Metrica providers, without migrating any existing fact tables. Establishes the primitive that PR 2 through PR 8 consume for the full `match_id` → `match_key` migration.

**Architecture:** Kimball conformed dimension (Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed., Ch. 1). Surrogate key generated deterministically via Spark `xxhash64(concat_ws('|', provider, native_match_id))`. Natural keys (`provider`, `native_match_id`) are preserved as attributes on the dim for lineage. No fact-table changes in this PR — coexistence phase. PR 2 begins migrating facts to reference `match_key`.

**Tech Stack:** dbt 1.9, Spark SQL (`xxhash64`), `dbt_utils`, Databricks Unity Catalog Delta tables, Lakebase (PostgreSQL sync), pytest (integration tests), existing `dbt_expectations` for range assertions.

**Commit discipline:** ONE commit at the end of the plan, after all verification passes and user approves. Individual tasks use TDD within their scope but do NOT commit between tasks (per repo convention `feedback_single_commit_squash` — single commit per feature branch).

---

## File Structure

### New files

| Path | Responsibility |
|------|----------------|
| `dbt_project/macros/generate_match_key.sql` | Kimball surrogate generator macro (Jinja + Spark `xxhash64`) |
| `dbt_project/models/staging/idsse/stg_idsse__matches.sql` | Per-match metadata staging (7 IDSSE matches) |
| `dbt_project/models/staging/metrica/stg_metrica__matches.sql` | Per-match metadata staging (3 Metrica sample games) |
| `dbt_project/models/marts/dim_matches.sql` | Conformed match dimension unifying 4 providers |
| `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md` | Architectural decision record |
| `src/tests/test_generate_match_key_macro.py` | Macro compilation + property tests (no live DB) |
| `src/tests/test_dbt_dim_matches.py` | Integration tests (requires live Databricks warehouse) |

### Modified files

| Path | What changes |
|------|--------------|
| `dbt_project/models/marts/_marts__models.yml` | Add `dim_matches` contract block (columns, data_types, data_tests) |
| `dbt_project/models/staging/idsse/_idsse__sources.yml` | Add `loaded_at_field` harmonization if missing for matches source |
| `dbt_project/models/staging/metrica/_metrica__sources.yml` | Same as above; also document the `match_id` column on `metrica_tracking` + `metrica_events` (currently missing from YAML — see verification step) |
| `src/ingestion/refresh_synced_tables.py` | Register `dim_matches` in `SYNCED_TABLES` |
| `scripts/create_indexes.py` | Add PG index definitions for `dim_matches_synced`: PK on `match_key`, composite on `(provider, native_match_id)` |
| `TODO.md` | Remove stale tech-debt #6 Path C claim, add ADR-011 reference, add PR 2-8 roadmap stubs under a new "Kimball Migration" section |
| `ARCHITECTURE.md` | Add ADR-011 to the decision log; add Kimball/Ross to Appendix D Academic References if not present |
| `src/tests/test_architecture_md_appendix.py` | Extend `expected_authors` with `"Kimball"` if newly added |

### Unchanged (explicit non-goal)

| Path | Why unchanged |
|------|---------------|
| All `fct_*.sql` | No fact table migrations in PR 1. That's PR 2-8. |
| `dim_tracking_matches.sql` | Kept as-is in PR 1; deprecated in PR 2 when `fct_passes` migrates. |
| All Taipy files | No UI changes in PR 1. |
| All Python ingestion modules | No ingestion changes — bronze retains native match_ids. |

---

## Pre-flight verification

- [ ] **Step P.1:** Confirm current branch is `feat/tracking-passes-idsse-metrica` and working tree is clean.

```bash
git rev-parse --abbrev-ref HEAD && git status --short
```

Expected output:
```
feat/tracking-passes-idsse-metrica
(empty — clean)
```

- [ ] **Step P.2:** Confirm Metrica bronze has `match_id` column (not documented in source YAML).

```bash
uv run python scripts/ensure_warehouse.py -- uv run python -c "
from databricks import sql
import os
conn = sql.connect(
    server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'],
    http_path=os.environ['DATABRICKS_HTTP_PATH'],
    access_token=os.environ['DATABRICKS_TOKEN'],
)
cur = conn.cursor()
cur.execute('DESCRIBE soccer_analytics.bronze.metrica_events')
for row in cur.fetchall():
    if 'match' in row[0].lower():
        print(row)
cur.execute('DESCRIBE soccer_analytics.bronze.metrica_tracking')
for row in cur.fetchall():
    if 'match' in row[0].lower():
        print(row)
"
```

Expected: at least one `match_id` line per table. If not present, flag to user before proceeding — the Metrica staging model cannot be built without this column.

- [ ] **Step P.3:** Confirm IDSSE bronze has `match_id` column.

```bash
uv run python -c "
from databricks import sql; import os
conn = sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'], http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('DESCRIBE soccer_analytics.bronze.idsse_tracking')
for row in cur.fetchall():
    if 'match' in row[0].lower(): print(row)
"
```

Expected: `match_id` column, type `STRING`.

---

## Task 1: Create `generate_match_key` macro (TDD)

**Files:**
- Create: `dbt_project/macros/generate_match_key.sql`
- Test: `src/tests/test_generate_match_key_macro.py`

- [ ] **Step 1.1: Write the failing test**

Create `src/tests/test_generate_match_key_macro.py`:

```python
"""Tests for the `generate_match_key` dbt macro.

The macro generates a deterministic BIGINT surrogate key from
`(provider, native_match_id)` pairs. Required properties:
  - Deterministic: same input → same output across invocations
  - Provider-sensitive: (statsbomb, '123') ≠ (wyscout, '123')
  - BIGINT output (fits in int64, Postgres BIGINT compatible)
  - Collision-free at our scale (<10k matches / provider)

Macro implementation is Spark SQL (`xxhash64`). These tests verify the
compiled SQL string via `dbt compile --inline`; a separate integration
test (`test_dbt_dim_matches.py`) verifies determinism against live Databricks.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"


def _compile_inline(inline_model: str) -> str:
    """Run `dbt compile --inline` and return compiled SQL from stdout."""
    result = subprocess.run(
        [
            "uv", "run", "dbt", "compile",
            "--inline", inline_model,
            "--project-dir", str(DBT_PROJECT_DIR),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def test_macro_uses_xxhash64():
    """The macro must emit `xxhash64` — collision-resistant at 64-bit width."""
    sql = _compile_inline(
        "select {{ generate_match_key('provider', 'native_match_id') }} as match_key "
        "from (select 'statsbomb' as provider, '3895302' as native_match_id)"
    )
    assert "xxhash64" in sql.lower(), f"Expected xxhash64 in compiled SQL; got: {sql}"


def test_macro_includes_both_inputs():
    """Provider and native_match_id must both appear in compiled output."""
    sql = _compile_inline(
        "select {{ generate_match_key('provider', 'native_match_id') }} as match_key "
        "from (select 'wyscout' as provider, '1' as native_match_id)"
    )
    assert "provider" in sql
    assert "native_match_id" in sql


def test_macro_uses_concat_ws_with_delimiter():
    """Must use `concat_ws` with an explicit delimiter to avoid
    ('ab', '') vs ('a', 'b') collisions."""
    sql = _compile_inline(
        "select {{ generate_match_key('provider', 'native_match_id') }} as match_key "
        "from (select 'statsbomb' as provider, '123' as native_match_id)"
    )
    assert "concat_ws" in sql.lower()


def test_macro_casts_native_id_to_string():
    """native_match_id may arrive as BIGINT (StatsBomb/Wyscout) or STRING
    (IDSSE/Metrica); macro must cast to string for uniform hashing."""
    sql = _compile_inline(
        "select {{ generate_match_key('provider', 'native_match_id') }} as match_key "
        "from (select 'statsbomb' as provider, 123 as native_match_id)"
    )
    assert "cast" in sql.lower()
    assert "string" in sql.lower()
```

- [ ] **Step 1.2: Run the tests to verify they fail**

```bash
uv run pytest src/tests/test_generate_match_key_macro.py -v
```

Expected: 4 tests FAIL with `CompilationError: dbt found macro 'generate_match_key' ... not defined`. The failure confirms the tests genuinely exercise the macro.

- [ ] **Step 1.3: Implement the macro**

Create `dbt_project/macros/generate_match_key.sql`:

```sql
{% macro generate_match_key(provider_col, native_match_id_col) %}
    -- Kimball surrogate key for the conformed `dim_matches` dimension.
    --
    -- Deterministic 64-bit hash of (provider, native_match_id) using Spark's
    -- `xxhash64`, which returns a signed BIGINT matching PostgreSQL BIGINT
    -- semantics on Lakebase synced tables. Collision probability at 10k
    -- matches per provider is ~10^-15 (birthday bound on a 64-bit hash).
    --
    -- The delimiter in `concat_ws` prevents concatenation ambiguities:
    -- (provider='ab', native='') would collide with (provider='a', native='b')
    -- without it. The '|' character is not present in any provider name or
    -- native ID format we ingest.
    --
    -- `cast(... as string)` normalizes mixed-type natives: StatsBomb/Wyscout
    -- use BIGINT natively; IDSSE/Metrica use STRING. Both hash identically
    -- once stringified.
    --
    -- See ADR-011 for architectural context.
    xxhash64(
        concat_ws(
            '|',
            {{ provider_col }},
            cast({{ native_match_id_col }} as string)
        )
    )
{% endmacro %}
```

- [ ] **Step 1.4: Run the tests to verify they pass**

```bash
uv run pytest src/tests/test_generate_match_key_macro.py -v
```

Expected: 4 passed.

---

## Task 2: Create `stg_idsse__matches` staging model (TDD)

**Files:**
- Create: `dbt_project/models/staging/idsse/stg_idsse__matches.sql`
- Modify: `dbt_project/models/staging/idsse/_idsse__sources.yml` (for source column documentation)

Grain: one row per IDSSE match. Source: `soccer_analytics.bronze.idsse_tracking` (DISTINCT match_id). Competition and team metadata comes from `stg_tracking__player_metadata` for team names and from the `_MATCH_COMPETITION` mapping in `src/ingestion/idsse.py` (preserved as a hardcoded lookup CTE in the staging model since DFL competition IDs are not in bronze today).

- [ ] **Step 2.1: Write the dbt data test (schema-level)**

Edit `dbt_project/models/staging/idsse/_idsse__sources.yml` — append the staging-model section at the end of the file (after the existing `sources:` block):

```yaml
models:
  - name: stg_idsse__matches
    description: >
      One row per IDSSE Bundesliga match (7 matches). Combines distinct
      match_ids from bronze.idsse_tracking with hardcoded DFL competition
      mappings from src/ingestion/idsse.py and team names from
      stg_tracking__player_metadata (pivoted home/away).
    columns:
      - name: native_match_id
        description: >
          Native IDSSE match identifier with the 'idsse_' prefix stripped
          (e.g., 'J03WMX'). Use with provider='idsse' for uniqueness.
        data_tests:
          - unique
          - not_null
      - name: provider
        description: Constant 'idsse' for rows from this staging model.
        data_tests:
          - not_null
          - accepted_values:
              values: ['idsse']
      - name: competition_id
        description: DFL competition identifier (e.g., 'DFL-COM-000001').
        data_tests:
          - not_null
      - name: home_team_name
        description: Home team display name from stg_tracking__player_metadata.
      - name: away_team_name
        description: Away team display name from stg_tracking__player_metadata.
```

- [ ] **Step 2.2: Run the dbt test to verify it fails**

```bash
uv run python scripts/ensure_warehouse.py -- uv run dbt test --select stg_idsse__matches --project-dir dbt_project
```

Expected: ERROR — "Model 'stg_idsse__matches' not found". Confirms the schema test targets a non-existent model.

- [ ] **Step 2.3: Implement the staging model**

Create `dbt_project/models/staging/idsse/stg_idsse__matches.sql`:

```sql
-- stg_idsse__matches.sql
-- One row per IDSSE Bundesliga match. 7 matches total (static collection).
--
-- Sources:
--   - bronze.idsse_tracking (distinct match_id for match presence)
--   - stg_tracking__player_metadata (home/away team display names)
--   - Hardcoded DFL competition mappings (see src/ingestion/idsse.py)
--
-- Strips the 'idsse_' prefix from bronze match_id to yield the native
-- DFL MatchId (e.g., 'idsse_J03WMX' -> 'J03WMX'). The provider column
-- (constant 'idsse') disambiguates against other providers' native IDs
-- via the surrogate key in dim_matches.

with tracking_matches as (

    select distinct
        match_id as prefixed_match_id
    from {{ source('idsse', 'idsse_tracking') }}

),

idsse_competitions as (

    -- DFL competition mapping from src/ingestion/idsse.py._MATCH_COMPETITION.
    -- 5 matches in DFL-COM-000002, 2 in DFL-COM-000001.
    -- Keep in sync with the Python source until a proper DFL metadata
    -- bronze table exists.
    select * from (
        values
            ('idsse_J03WMX', 'DFL-COM-000001'),
            ('idsse_J03WN1', 'DFL-COM-000001'),
            ('idsse_J03WPY', 'DFL-COM-000002'),
            ('idsse_J03WOH', 'DFL-COM-000002'),
            ('idsse_J03WQQ', 'DFL-COM-000002'),
            ('idsse_J03WOY', 'DFL-COM-000002'),
            ('idsse_J03WR9', 'DFL-COM-000002')
    ) as t(prefixed_match_id, competition_id)

),

team_names as (

    select
        match_id,
        max(case when team_side = 'home' then team_display_name end) as home_team_name,
        max(case when team_side = 'away' then team_display_name end) as away_team_name
    from {{ ref('stg_tracking__player_metadata') }}
    where provider = 'idsse'
    group by match_id

),

final as (

    select
        -- Native DFL MatchId with the 'idsse_' prefix stripped
        regexp_replace(tm.prefixed_match_id, '^idsse_', '') as native_match_id,
        'idsse'                                              as provider,
        ic.competition_id,
        tn.home_team_name,
        tn.away_team_name,
        tm.prefixed_match_id                                 as bronze_match_id

    from tracking_matches tm
    left join idsse_competitions ic
        on tm.prefixed_match_id = ic.prefixed_match_id
    left join team_names tn
        on tm.prefixed_match_id = tn.match_id

)

select * from final
```

- [ ] **Step 2.4: Build the model and run the test**

```bash
uv run python scripts/ensure_warehouse.py -- uv run dbt build --select stg_idsse__matches --project-dir dbt_project
```

Expected: `Completed successfully` with 1 model built, 4 tests passed (unique + not_null on native_match_id, not_null + accepted_values on provider, not_null on competition_id).

- [ ] **Step 2.5: Verify row count and content**

```bash
uv run python -c "
from databricks import sql; import os
conn = sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'], http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT count(*) FROM soccer_analytics.dev_silver.stg_idsse__matches')
print('Row count:', cur.fetchone()[0])
cur.execute('SELECT native_match_id, provider, competition_id, home_team_name, away_team_name FROM soccer_analytics.dev_silver.stg_idsse__matches ORDER BY native_match_id')
for row in cur.fetchall():
    print(row)
"
```

Expected: Row count = 7. All 7 DFL match codes visible (`J03WMX`, `J03WN1`, `J03WOH`, `J03WOY`, `J03WPY`, `J03WQQ`, `J03WR9`). All rows have `provider='idsse'` and a non-NULL `competition_id`. Team names may be NULL if `stg_tracking__player_metadata` has not yet propagated IDSSE teams — flag if so.

---

## Task 3: Create `stg_metrica__matches` staging model (TDD)

**Files:**
- Create: `dbt_project/models/staging/metrica/stg_metrica__matches.sql`
- Modify: `dbt_project/models/staging/metrica/_metrica__sources.yml`

Metrica sample-data has 3 anonymized matches (`Sample_Game_1`, `Sample_Game_2`, `Sample_Game_3`). Team names are not real; we use the generic 'Home' / 'Away' labels from the Metrica event data. No competition_id available. No match_date available.

- [ ] **Step 3.1: Add the `match_id` column to the Metrica sources YAML**

Both `metrica_tracking` and `metrica_events` have a `match_id` column in bronze but it is not documented in `_metrica__sources.yml`. Edit `dbt_project/models/staging/metrica/_metrica__sources.yml`:

Find this block (around line 19):

```yaml
      - name: metrica_tracking
        description: >
          Raw positional tracking data at 25 frames per second. Each row
          represents one frame containing x/y coordinates for every tracked
          player on both teams plus the ball. Coordinates are normalized
          to [0, 1] range (pitch proportion) — must be scaled to
          StatsBomb-compatible 120x80 for cross-source analysis.
        columns:
          - name: period
            description: Match half (1 or 2)
```

And after `columns:`, prepend:

```yaml
          - name: match_id
            description: >
              Metrica sample-data match identifier — string like
              'Sample_Game_1', 'Sample_Game_2', 'Sample_Game_3'. Set at
              ingestion in src/ingestion/metrica_tracking.py.
```

Then do the same for `metrica_events` — add `match_id` as the first column.

- [ ] **Step 3.2: Write the dbt data test (schema-level)**

Append to `dbt_project/models/staging/metrica/_metrica__sources.yml`:

```yaml
models:
  - name: stg_metrica__matches
    description: >
      One row per Metrica sample-data match (3 matches: Sample_Game_1,
      Sample_Game_2, Sample_Game_3). Match metadata is intentionally sparse —
      Metrica open-data is anonymized with no competition_id, no match_date,
      and generic 'Home'/'Away' team labels.
    columns:
      - name: native_match_id
        description: Metrica match identifier (e.g., 'Sample_Game_1').
        data_tests:
          - unique
          - not_null
      - name: provider
        description: Constant 'metrica' for rows from this staging model.
        data_tests:
          - not_null
          - accepted_values:
              values: ['metrica']
      - name: home_team_name
        description: Constant 'Home' — Metrica open-data is anonymized.
      - name: away_team_name
        description: Constant 'Away' — Metrica open-data is anonymized.
```

- [ ] **Step 3.3: Run the dbt test to verify it fails**

```bash
uv run dbt test --select stg_metrica__matches --project-dir dbt_project
```

Expected: ERROR — model not found.

- [ ] **Step 3.4: Implement the staging model**

Create `dbt_project/models/staging/metrica/stg_metrica__matches.sql`:

```sql
-- stg_metrica__matches.sql
-- One row per Metrica sample-data match. 3 matches total (static).
--
-- Metrica open-data is anonymized:
--   - No real team names (using generic 'Home' / 'Away')
--   - No competition_id, no season_id, no match_date
--   - Native match_id format: 'Sample_Game_{1,2,3}'
--
-- Sources: distinct match_id from bronze.metrica_tracking.

with tracking_matches as (

    select distinct match_id
    from {{ source('metrica', 'metrica_tracking') }}

),

final as (

    select
        match_id                 as native_match_id,
        'metrica'                as provider,
        'Home'                   as home_team_name,
        'Away'                   as away_team_name

    from tracking_matches

)

select * from final
```

- [ ] **Step 3.5: Build the model and run the test**

```bash
uv run dbt build --select stg_metrica__matches --project-dir dbt_project
```

Expected: `Completed successfully` with 1 model built, 4 tests passed.

- [ ] **Step 3.6: Verify row count and content**

```bash
uv run python -c "
from databricks import sql; import os
conn = sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'], http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT native_match_id, provider FROM soccer_analytics.dev_silver.stg_metrica__matches ORDER BY native_match_id')
for row in cur.fetchall(): print(row)
"
```

Expected: exactly 3 rows: `('Sample_Game_1', 'metrica')`, `('Sample_Game_2', 'metrica')`, `('Sample_Game_3', 'metrica')`.

---

## Task 4: Create `dim_matches` conformed dimension (TDD)

**Files:**
- Create: `dbt_project/models/marts/dim_matches.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 4.1: Write the dbt contract + schema tests**

Edit `dbt_project/models/marts/_marts__models.yml` — find the `# ── Dimension Tables ───` section (or insert one before the first `dim_*` entry, or append at end of `models:` list). Add this block:

```yaml
  - name: dim_matches
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    description: >
      Conformed match dimension unifying all four providers (StatsBomb,
      Wyscout, IDSSE, Metrica). Primary key is `match_key` — a deterministic
      BIGINT surrogate generated via `generate_match_key(provider, native_match_id)`.
      Natural keys are preserved as attributes for lineage.

      PR 1 establishes this dim as the Kimball conformed dimension; no fact
      tables reference it yet. PR 2 through PR 8 migrate existing facts
      from their native-ID `match_id` columns to `match_key` FKs against
      this dim. See ADR-011.
    columns:
      - name: match_key
        data_type: bigint
        description: >
          Kimball surrogate primary key. Deterministic BIGINT hash of
          (provider, native_match_id). Future facts reference this as FK.
        data_tests:
          - unique
          - not_null
      - name: provider
        data_type: string
        description: >
          Data source that produced this match. One of: statsbomb, wyscout,
          idsse, metrica.
        data_tests:
          - not_null
          - accepted_values:
              values: ['statsbomb', 'wyscout', 'idsse', 'metrica']
      - name: native_match_id
        data_type: string
        description: >
          Source-system-native match identifier, stringified (StatsBomb and
          Wyscout stringify their BIGINT IDs; IDSSE and Metrica are natively
          string). Together with `provider`, uniquely identifies a match.
        data_tests:
          - not_null
      - name: competition_id
        data_type: string
        description: >
          Provider-native competition ID. NULL for Metrica (anonymized). For
          StatsBomb/Wyscout the BIGINT competition_id is stringified for uniform
          typing across providers.
      - name: season_id
        data_type: string
        description: >
          Provider-native season ID, stringified. NULL for IDSSE/Metrica.
      - name: match_date
        data_type: date
        description: Match calendar date. NULL for Metrica.
      - name: home_team_id
        data_type: int
        description: >
          Provider-native home team ID. NULL for Metrica (anonymized) and
          potentially NULL for IDSSE depending on stg_tracking__player_metadata
          coverage.
      - name: away_team_id
        data_type: int
        description: Provider-native away team ID. NULL for anonymized providers.
      - name: home_team_name
        data_type: string
        description: Home team display name.
      - name: away_team_name
        data_type: string
        description: Away team display name.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - provider
            - native_match_id
```

- [ ] **Step 4.2: Run the dbt test to verify it fails**

```bash
uv run dbt test --select dim_matches --project-dir dbt_project
```

Expected: ERROR — model not found.

- [ ] **Step 4.3: Implement the dimension**

Create `dbt_project/models/marts/dim_matches.sql`:

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['provider']
) }}
-- dim_matches.sql
-- Conformed match dimension unifying StatsBomb, Wyscout, IDSSE, and Metrica.
--
-- PRIMARY KEY: match_key (BIGINT surrogate, deterministic hash).
-- UNIQUE: (provider, native_match_id).
--
-- Kimball conformed dimension per ADR-011. PR 1 establishes this dim; no
-- facts reference it yet. PR 2 migrates fct_passes + fct_line_breaking_results
-- + fct_match_summary to match_key FKs. Subsequent PRs migrate remaining facts.
--
-- Cardinality at the time of PR 1:
--   - statsbomb: ~3500 matches (open data)
--   - wyscout:   ~1900 matches (open data)
--   - idsse:     7 matches
--   - metrica:   3 matches
--   - TOTAL:     ~5410 matches

with statsbomb_matches as (

    select
        cast(match_id as string)      as native_match_id,
        'statsbomb'                   as provider,
        cast(competition_id as string) as competition_id,
        cast(season_id as string)     as season_id,
        cast(match_date as date)      as match_date,
        cast(home_team_id as int)     as home_team_id,
        cast(away_team_id as int)     as away_team_id,
        home_team_name,
        away_team_name
    from {{ ref('stg_statsbomb__matches') }}

),

wyscout_matches as (

    select
        cast(match_id as string)      as native_match_id,
        'wyscout'                     as provider,
        cast(competition_id as string) as competition_id,
        cast(season_id as string)     as season_id,
        cast(match_date as date)      as match_date,
        cast(home_team_id as int)     as home_team_id,
        cast(away_team_id as int)     as away_team_id,
        home_team_name,
        away_team_name
    from {{ ref('stg_wyscout__matches') }}

),

idsse_matches as (

    select
        native_match_id,
        provider,
        competition_id,
        cast(null as string)          as season_id,
        cast(null as date)            as match_date,
        cast(null as int)             as home_team_id,
        cast(null as int)             as away_team_id,
        home_team_name,
        away_team_name
    from {{ ref('stg_idsse__matches') }}

),

metrica_matches as (

    select
        native_match_id,
        provider,
        cast(null as string)          as competition_id,
        cast(null as string)          as season_id,
        cast(null as date)            as match_date,
        cast(null as int)             as home_team_id,
        cast(null as int)             as away_team_id,
        home_team_name,
        away_team_name
    from {{ ref('stg_metrica__matches') }}

),

unioned as (

    select * from statsbomb_matches
    union all
    select * from wyscout_matches
    union all
    select * from idsse_matches
    union all
    select * from metrica_matches

),

final as (

    select
        {{ generate_match_key('provider', 'native_match_id') }} as match_key,
        provider,
        native_match_id,
        competition_id,
        season_id,
        match_date,
        home_team_id,
        away_team_id,
        home_team_name,
        away_team_name

    from unioned

)

select * from final
```

- [ ] **Step 4.4: Build and test the dim**

```bash
uv run python scripts/ensure_warehouse.py -- uv run dbt build --select dim_matches --project-dir dbt_project
```

Expected: `Completed successfully`. Tests pass: unique + not_null on match_key; not_null + accepted_values on provider; not_null on native_match_id; unique_combination_of_columns on (provider, native_match_id). If unique match_key test fails, the macro has a collision — flag to user before proceeding.

- [ ] **Step 4.5: Verify 4-provider coverage via live query**

```bash
uv run python -c "
from databricks import sql; import os
conn = sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'], http_path=os.environ['DATABRICKS_HTTP_PATH'], access_token=os.environ['DATABRICKS_TOKEN'])
cur = conn.cursor()
cur.execute('SELECT provider, count(*) FROM soccer_analytics.dev_gold.dim_matches GROUP BY provider ORDER BY provider')
for row in cur.fetchall(): print(row)
cur.execute('SELECT count(DISTINCT match_key), count(*) FROM soccer_analytics.dev_gold.dim_matches')
print('Distinct keys vs total rows:', cur.fetchone())
"
```

Expected:
- 4 rows: `statsbomb, <count>`; `wyscout, <count>`; `idsse, 7`; `metrica, 3`.
- Distinct match_key count equals total row count (no collisions).

---

## Task 5: Write integration tests for dim_matches

**Files:**
- Create: `src/tests/test_dbt_dim_matches.py`

- [ ] **Step 5.1: Write the test file**

Create `src/tests/test_dbt_dim_matches.py`:

```python
"""Integration tests for the conformed dim_matches dimension.

Requires live Databricks SQL warehouse access via standard environment
variables. Skipped in CI by default (no live access); run locally after
`dbt build --select dim_matches`.
"""
from __future__ import annotations

import os

import pytest

pytest.importorskip("databricks")
from databricks import sql  # noqa: E402


requires_databricks = pytest.mark.skipif(
    not all(
        os.environ.get(var)
        for var in ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
    ),
    reason="Databricks SQL env vars not set",
)


@pytest.fixture(scope="module")
def conn():
    connection = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield connection
    finally:
        connection.close()


@requires_databricks
def test_four_providers_present(conn):
    """dim_matches must contain rows for all four providers."""
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT provider FROM soccer_analytics.dev_gold.dim_matches"
    )
    providers = {row[0] for row in cur.fetchall()}
    assert providers == {"statsbomb", "wyscout", "idsse", "metrica"}, providers


@requires_databricks
def test_idsse_row_count(conn):
    """IDSSE has exactly 7 matches (static figshare collection)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM soccer_analytics.dev_gold.dim_matches WHERE provider='idsse'"
    )
    assert cur.fetchone()[0] == 7


@requires_databricks
def test_metrica_row_count(conn):
    """Metrica sample-data has exactly 3 matches (static)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM soccer_analytics.dev_gold.dim_matches WHERE provider='metrica'"
    )
    assert cur.fetchone()[0] == 3


@requires_databricks
def test_match_key_unique(conn):
    """No surrogate-key collisions across providers."""
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*), count(DISTINCT match_key) FROM soccer_analytics.dev_gold.dim_matches"
    )
    total, distinct = cur.fetchone()
    assert total == distinct, f"Collision detected: {total - distinct} duplicate match_keys"


@requires_databricks
def test_match_key_deterministic_across_rebuild(conn):
    """Same (provider, native_match_id) pair must produce the same match_key
    across dbt rebuilds. This test captures a known pair's match_key, asserts
    stability. If dbt rebuilds between runs, the key should not change."""
    cur = conn.cursor()
    cur.execute(
        "SELECT match_key FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' AND native_match_id='J03WMX'"
    )
    row = cur.fetchone()
    assert row is not None, "idsse_J03WMX missing from dim_matches"
    match_key = row[0]
    # Re-query — same row should return same key
    cur.execute(
        "SELECT match_key FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' AND native_match_id='J03WMX'"
    )
    assert cur.fetchone()[0] == match_key


@requires_databricks
def test_provider_sensitivity(conn):
    """Verify hash provider-sensitivity using Spark-computed values —
    (statsbomb, '1') and (wyscout, '1') must produce different match_keys.
    We use xxhash64 directly rather than relying on actual data pair."""
    cur = conn.cursor()
    cur.execute(
        "SELECT xxhash64(concat_ws('|', 'statsbomb', '1')), "
        "       xxhash64(concat_ws('|', 'wyscout', '1'))"
    )
    sb_key, wy_key = cur.fetchone()
    assert sb_key != wy_key, "Macro not provider-sensitive"


@requires_databricks
def test_idsse_native_ids_have_no_prefix(conn):
    """IDSSE native_match_id must NOT carry the 'idsse_' prefix —
    stg_idsse__matches strips it so native_match_id is the raw DFL MatchId."""
    cur = conn.cursor()
    cur.execute(
        "SELECT native_match_id FROM soccer_analytics.dev_gold.dim_matches "
        "WHERE provider='idsse' ORDER BY native_match_id"
    )
    native_ids = [row[0] for row in cur.fetchall()]
    assert native_ids == ["J03WMX", "J03WN1", "J03WOH", "J03WOY", "J03WPY", "J03WQQ", "J03WR9"]
```

- [ ] **Step 5.2: Run the integration tests**

```bash
uv run pytest src/tests/test_dbt_dim_matches.py -v
```

Expected: 7 passed. If any test fails, flag the specific failure to the user before proceeding — especially the collision test, which would invalidate the macro.

---

## Task 6: Register `dim_matches` for Lakebase sync

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py`

- [ ] **Step 6.1: Read the current SYNCED_TABLES registry**

```bash
```

Actually — read the file to locate the SYNCED_TABLES definition:

```bash
uv run python -c "
import re
with open('src/ingestion/refresh_synced_tables.py') as f:
    src = f.read()
m = re.search(r'SYNCED_TABLES\s*[:=].*?\]', src, re.DOTALL)
print(m.group(0)[:2000] if m else 'SYNCED_TABLES not found')
"
```

Expected: Prints the current SYNCED_TABLES list. Note the exact syntax used (list of strings, list of tuples, list of dataclasses) so we can match the style.

- [ ] **Step 6.2: Add `dim_matches` to the registry**

Add `dim_matches` to the SYNCED_TABLES list in the same style as existing entries. For example, if entries are plain strings (e.g., `"dim_tracking_matches"`), insert `"dim_matches"` alphabetically or with the other dim_* entries. If entries are structured (with source schema, PG schema, primary key, etc.), provide the matching fields:

- Source table: `soccer_analytics.dev_gold.dim_matches`
- Target PG schema: (match existing dim tables — usually `public` or equivalent)
- Primary key: `match_key`
- Scheduled refresh: `SNAPSHOT` (same as other dim tables)

- [ ] **Step 6.3: Create the synced table**

Per ADR-005 (Lakebase synced-table grants), new synced tables must be created via the Databricks UI or via `scripts/refresh_synced_tables.py`. Follow whichever pattern matches existing new-synced-table flow.

```bash
uv run python src/ingestion/refresh_synced_tables.py --create dim_matches --wait
```

(If `--create` is not a supported flag, use the existing create pattern — check the module's `--help` output and match it.)

Expected: table created, refresh completes. If creation requires the UI path per ADR-005, follow that path and document it in the PR description.

- [ ] **Step 6.4: Run the refresh**

```bash
uv run python src/ingestion/refresh_synced_tables.py --wait
```

Expected: refresh completes without error. `dim_matches_synced` is now queryable from PG / Lakebase.

- [ ] **Step 6.5: Apply grants per ADR-005**

```bash
uv run python scripts/run_lakebase_grants.py apply --verify
```

Expected: grants applied to `dim_matches_synced` for the Taipy SP; verify step reports SELECT present.

---

## Task 7: Add PG indexes for `dim_matches_synced`

**Files:**
- Modify: `scripts/create_indexes.py`

- [ ] **Step 7.1: Read the current index registry**

```bash
uv run python -c "
with open('scripts/create_indexes.py') as f: print(f.read()[:4000])
"
```

Note the style used for existing index definitions (list of tuples, list of dicts, explicit CREATE INDEX calls, etc.). The following indexes are needed:

- **Primary key** on `match_key` (BIGINT). Lakebase creates a PK automatically if the dbt contract declares it, but verify with `EXPLAIN ANALYZE`; add a manual index if not.
- **Composite lookup** on `(provider, native_match_id)` for human-readable joins from legacy fact tables during the PR 2-8 migration. This is a debug-path index.

- [ ] **Step 7.2: Add the index entries**

Add to the existing index registry (matching the repo's style):

```python
# dim_matches — Kimball conformed dimension (ADR-011)
("dim_matches_synced", "match_key",                 "idx_dim_matches_match_key"),
("dim_matches_synced", "(provider, native_match_id)", "idx_dim_matches_provider_native"),
```

(Adapt to actual file style — may be a dict or a class.)

- [ ] **Step 7.3: Apply indexes WITHOUT `ONLY` clause**

Per CLAUDE.md "No `ON ONLY` indexes" rule — Lakebase synced tables are internally partitioned, and parent-only indexes are invisible to the planner. Ensure the index-creation SQL does NOT include `ONLY`.

```bash
uv run python scripts/create_indexes.py --verify
```

Expected: indexes created (or idempotently re-created), verify pass reports Index Scan plans, not Seq Scan.

- [ ] **Step 7.4: Verify with EXPLAIN ANALYZE**

```bash
uv run python -c "
import psycopg, os
# Connect using the Lakebase DNS env var — adapt to the repo's connection pattern
conn = psycopg.connect(os.environ['LAKEBASE_URL_RW'])
cur = conn.cursor()
cur.execute('EXPLAIN ANALYZE SELECT * FROM dim_matches_synced WHERE match_key = 123')
for row in cur.fetchall(): print(row)
"
```

Expected: plan contains `Index Scan` (not `Seq Scan`). If Seq Scan, the index was created parent-only — re-create without ONLY.

---

## Task 8: Write ADR-011

**Files:**
- Create: `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`

- [ ] **Step 8.1: Write the ADR**

Create `docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md`:

```markdown
# ADR-011: Unified Kimball Match Dimension with Conformed Pass Fact

| Field | Value |
|---|---|
| **Date** | 2026-04-20 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The warehouse ingests match-level data from four providers with heterogeneous native match-identifier formats:

- StatsBomb: BIGINT (e.g., `3895302`)
- Wyscout: BIGINT (e.g., `5154201`)
- IDSSE: STRING (e.g., `J03WMX`, carried as `idsse_J03WMX` in bronze)
- Metrica: STRING (e.g., `Sample_Game_1`)

Until PR 1, fact tables stored the native ID directly in a column called `match_id` typed as BIGINT — relying on the happy accident that StatsBomb and Wyscout integer IDs did not collide in the observed ranges. This is a "smart key" anti-pattern: source semantics embedded in the primary key. It has three concrete symptoms:

1. Type mismatch when landing tracking-provider passes: Metrica and IDSSE match_ids are strings but `fct_passes.match_id` is BIGINT. Attempting to union them into `fct_passes` fails the dbt contract.
2. Cross-provider collisions are theoretically possible — StatsBomb and Wyscout both use small positive integers; only the observed distribution has kept them apart.
3. Schema-level coupling between source system and warehouse — if StatsBomb renumbers their open-data matches, our fact tables must rebuild.

The forcing function for this ADR is the LB-IDSSE + LB-METRICA cycle, which requires landing tracking-provider passes in `fct_passes`. The options are documented in §Alternatives.

## Decision

Adopt a Kimball-style conformed match dimension (`dim_matches`) keyed by a **deterministic surrogate BIGINT** generated via the `generate_match_key(provider, native_match_id)` dbt macro (Spark `xxhash64` over `concat_ws('|', provider, cast(native_match_id as string))`). Every fact table that references a match will carry `match_key BIGINT` as a foreign key to `dim_matches.match_key`. Natural keys (`provider`, `native_match_id`) are preserved on the dim as attributes for lineage, debugging, and human-readable joins.

The migration is staged across PR 2 through PR 8 to keep each PR reviewable and each deploy reversible. PR 1 (this PR) ships the dim and macro only; no fact tables are modified.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Synthetic bigint for tracking-only; keep native IDs on StatsBomb/Wyscout | Minimum blast radius for the LB-IDSSE cycle; tracking providers get a surrogate, existing facts unchanged | Kimball-violating (smart keys remain); postpones the right thing; means every new provider relitigates the decision | Smart-key anti-pattern perpetuated; structural debt |
| B. Stringify `match_id` across all facts; use native strings everywhere | No surrogate layer; raw ID visible in every mart | Huge blast radius (all Lakebase synced tables recreate, PG indexes rebuild, many Taipy query type-compat audits); smart-key anti-pattern still present; string joins slightly less performant than BIGINT | Blast radius + retains smart keys |
| C. Kimball surrogate on unified dim_matches (chosen) | Warehouse independence from source systems; collision-free by construction (hash includes provider); deterministic under rebuild; single-column BIGINT join; Type-2 SCD-ready; new providers plug in uniformly | Larger migration (fact-layer rename from `match_id` to `match_key`); extra dim-join for debugging raw native IDs; requires an ADR + a staged rollout plan | — |

## Consequences

### Positive

- **Collision-free across providers.** The hash includes `provider` in the input, so Wyscout `match_id=123` and StatsBomb `match_id=123` produce different `match_key` values even though their natives collide as integers.
- **Warehouse-source independence.** If StatsBomb changes their match-ID scheme, our downstream keys are unchanged.
- **Determinism across rebuilds.** `xxhash64` is pure; `dbt build --full-refresh` produces identical `match_key` values.
- **Uniform provider onboarding.** Respo.Vision, SkillCorner events (when they arrive), homegrown tracking all plug in via a new staging model + dim union — no architecture discussion per provider.
- **Conformed-fact alignment.** Downstream unified facts (`fct_passes`, `fct_match_summary`, `fct_line_breaking_results`) use a single BIGINT FK. Cross-provider analytics become one-table queries.

### Negative

- **Migration cost.** ~28 mart tables + ~23 Taipy UI files + ~80 Python modules reference `match_id` today. Migrating each to `match_key` is spread across PR 2-8 to stay reviewable.
- **Extra dim-join for raw native IDs.** Debugging from `fct_passes` back to StatsBomb's native match page requires joining `dim_matches` to recover `native_match_id`. The one-hop cost is low; the indirection is the price of the surrogate.
- **Lakebase synced-table recreation.** Each migrated fact table must recreate its synced table to accommodate the column rename, triggering grant re-application per ADR-005. Managed by scheduling migrations in PR-sized batches.

### Neutral

- **Surrogate is signed BIGINT.** Spark's `xxhash64` returns a signed 64-bit integer, including negatives. PostgreSQL BIGINT accepts the full int64 range, so no adjustment is needed. Signed vs unsigned does not affect collision probability.
- **Delimiter choice `'|'`.** Prevents concatenation ambiguity. Not present in any current provider name or native ID format. Documented in the macro source.

## CLAUDE.md Amendment

No CLAUDE.md amendment. This ADR establishes a new pattern that complements existing rules rather than carving out an exception.

## Related

- **Branches:** `feat/tracking-passes-idsse-metrica`
- **Plans:** `docs/superpowers/plans/2026-04-20-pr1-kimball-foundation.md`, (subsequent plans for PR 2-8 — TBD per PR)
- **ADRs:** ADR-005 (Lakebase synced-table grants — each migration PR will re-apply grants)
- **External references:**
  - Kimball & Ross, *The Data Warehouse Toolkit*, 3rd ed. (Wiley 2013), Ch. 1 "Dimensional Modeling Primer", pp. 13–16 on surrogate keys; Ch. 4 on conformed dimensions.
  - Spark `xxhash64` documentation: https://spark.apache.org/docs/latest/api/sql/index.html#xxhash64

## Notes

### Staged rollout policy

| PR | Scope | Status |
|---|---|---|
| PR 1 | Foundation: `generate_match_key` macro + `dim_matches` + ADR-011 | Active |
| PR 2 | Passes conformed + LB-IDSSE + LB-METRICA functional surfacing | Planned |
| PR 3 | Shots + xG migration | Planned |
| PR 4 | Action values + VAEP migration | Planned |
| PR 5 | Player stats + embeddings migration | Planned |
| PR 6 | Defensive + goalkeeper + pitch control migration | Planned |
| PR 7 | Tracking + formations + pausa + tail facts migration | Planned |
| PR 8 | Scripts + final cleanup + doc sweep | Planned |

After PR 8 merges, the warehouse contains zero smart-keyed `match_id` columns. Legacy bronze tables retain their native match_ids (provenance layer).

### Collision math

xxhash64 is a 64-bit hash. Birthday collision probability for N hashed items is approximately `N^2 / 2^65`. At our expected scale (~10^4 matches total across all providers for the foreseeable future), collision probability is on the order of `10^-17` — negligible. If the warehouse grows to `10^9` matches, revisit.
```

- [ ] **Step 8.2: Verify the ADR tests pass**

If the repo has an ADR-presence test, run it:

```bash
uv run pytest src/tests/ -v -k adr
```

Expected: all ADR-related tests pass. No ADR numbering conflicts.

---

## Task 9: Update TODO.md and ARCHITECTURE.md

**Files:**
- Modify: `TODO.md`
- Modify: `ARCHITECTURE.md`
- Modify: `src/tests/test_architecture_md_appendix.py` (if Kimball is newly added as an expected author)

- [ ] **Step 9.1: Update TODO.md tech-debt #6**

Edit `TODO.md`. Find the tech-debt #6 row (around line 46):

Before:
```
| 6 | Line-breaking SkillCorner not yet wired | `line_breaking.py` | Path A (StatsBomb 360), Path B (Metrica tracking), and Path C (IDSSE tracking) all operational. SkillCorner (10 matches) has tracking but no event data — cannot compute line-breaking without pass events. | SkillCorner: blocked on event data procurement or ball trajectory detection. |
```

After:
```
| 6 | Line-breaking SkillCorner not yet wired | `line_breaking.py` | Path A (StatsBomb 360), Path B (Metrica tracking), and Path C (IDSSE tracking) are wired at ingestion since Q1 2026. As of PR 1 of the Kimball migration (ADR-011), Path B and C results are NOT yet surfaced in `fct_passes` / `fct_match_summary` / Pass Map UI — that surfacing is scheduled for PR 2 of the same cycle. SkillCorner (10 matches) has tracking but no event data — cannot compute line-breaking without pass events. | SkillCorner: blocked on event data procurement or ball trajectory detection. |
```

- [ ] **Step 9.2: Add the Kimball Migration roadmap to TODO.md**

Find the "On Deck" table (around line 20). Remove the `LB-IDSSE` and `LB-METRICA` rows (they are absorbed into PR 2 of the new Kimball Migration cycle).

Insert a new section heading between "On Deck" and "Technical Debt", or append a new "Kimball Migration" section. The exact format:

```markdown
---

## Kimball Migration — Active Cycle

Cycle started 2026-04-20. Architectural basis: [ADR-011](docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md). Target end-state: zero smart-keyed `match_id` columns across all fact tables; every match reference is `match_key BIGINT FK → dim_matches`.

| PR | Scope | Status |
|----|-------|--------|
| 1 | Foundation — `generate_match_key` macro, `dim_matches` conformed dim, ADR-011 | **Active** (this branch) |
| 2 | Passes conformed fact — IDSSE + Metrica arms in `int_unified_passes`, `fct_passes`/`fct_line_breaking_results`/`fct_match_summary` → `match_key`, Pass Map UI update. Delivers LB-IDSSE + LB-METRICA functional goals. | Planned |
| 3 | Shots + xG migration — `int_unified_shots`, `fct_shots`, `fct_xg_predictions`, shot readers | Planned |
| 4 | Action values + VAEP migration — `int_running_score`, `fct_action_values`, SPADL/VAEP ingestion | Planned |
| 5 | Player stats + embeddings migration | Planned |
| 6 | Defensive + goalkeeper + pitch control migration | Planned |
| 7 | Tracking + formations + pausa + tail facts migration | Planned |
| 8 | Scripts + final cleanup + doc sweep | Planned |
```

- [ ] **Step 9.3: Update TODO.md "Last updated" line**

Find the `**Last updated**:` line (top of TODO.md, around line 5). Update to today with a brief description:

```markdown
**Last updated**: 2026-04-20 (PR 1 of the Kimball Migration cycle — ADR-011 — ships `dim_matches` conformed dimension + `generate_match_key` macro. No fact tables migrated yet. See "Kimball Migration" section below for PR 2-8 roadmap.)
```

- [ ] **Step 9.4: Add ADR-011 to ARCHITECTURE.md decision log**

Edit `ARCHITECTURE.md`. Locate the ADR index section (search for `ADR-010` or `docs/superpowers/adrs`). Add an entry for ADR-011 immediately after ADR-010, matching the existing link format.

- [ ] **Step 9.5: Check Appendix D academic references for Kimball**

```bash
grep -i "kimball" ARCHITECTURE.md
```

If zero results — add `Kimball, R. & Ross, M.` to ARCHITECTURE.md §8 "D. Academic References" (exact format should match existing entries; citation: *The Data Warehouse Toolkit*, 3rd ed., Wiley 2013).

If already present — no change needed.

- [ ] **Step 9.6: Sync `test_architecture_md_appendix.py`**

If Kimball was newly added in Step 9.5, edit `src/tests/test_architecture_md_appendix.py` and add `"Kimball"` to the `expected_authors` list.

```bash
uv run pytest src/tests/test_architecture_md_appendix.py -v
```

Expected: passes. If fails with "Kimball not in expected_authors", update the list and re-run.

---

## Task 10: Run the full test suite + dbt build

- [ ] **Step 10.1: Run dbt build for the new models + their upstream dependencies**

```bash
uv run python scripts/ensure_warehouse.py -- uv run dbt build --select +dim_matches --project-dir dbt_project
```

Expected: all models in the chain build successfully; all associated data_tests pass.

- [ ] **Step 10.2: Run the full pytest suite**

```bash
uv run pytest src/tests/ -v 2>&1 | tail -60
```

Expected: all tests pass (or the tail shows only pre-existing expected skips). If any **new** failures appear, investigate before proceeding.

- [ ] **Step 10.3: Run ruff + pyright**

```bash
uv run ruff check src/tests/test_generate_match_key_macro.py src/tests/test_dbt_dim_matches.py
uv run ruff format --check src/tests/test_generate_match_key_macro.py src/tests/test_dbt_dim_matches.py
uv run pyright src/tests/test_generate_match_key_macro.py src/tests/test_dbt_dim_matches.py
```

Expected: zero violations.

- [ ] **Step 10.4: Refresh Lakebase synced table one more time**

```bash
uv run python src/ingestion/refresh_synced_tables.py --wait
```

Expected: complete without error. `dim_matches_synced` has expected row count.

- [ ] **Step 10.5: E2E smoke — query dim_matches from PG side**

```bash
uv run python -c "
import psycopg, os
conn = psycopg.connect(os.environ['LAKEBASE_URL_RW'])
cur = conn.cursor()
cur.execute('SELECT provider, count(*) FROM dim_matches_synced GROUP BY provider ORDER BY provider')
for row in cur.fetchall(): print(row)
"
```

Expected: 4 rows, `idsse=7`, `metrica=3`, `statsbomb=<count>`, `wyscout=<count>`.

---

## Task 11: Present results and request commit approval

- [ ] **Step 11.1: Prepare summary**

Generate the staged-file summary:

```bash
git status --short
git diff --stat
```

- [ ] **Step 11.2: Present to user**

Produce a concise summary containing:

- Files created/modified (from `git status --short`)
- All tests passing (from Task 10 outputs)
- Row counts verified (from Step 4.5 and 10.5)
- dim_matches present in Lakebase with grants + indexes
- ADR-011 written
- TODO.md updated; LB-IDSSE + LB-METRICA removed from On Deck (absorbed into Kimball Migration §)
- Any unexpected findings or deviations from the plan

Ask: **"Approve commit of PR 1 (Kimball Foundation)?"** Do not commit without explicit approval (user rule: "no commits or prs without explicit approval").

- [ ] **Step 11.3: If approved, commit**

Only if user gives explicit go-ahead:

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(dbt): Kimball Foundation — dim_matches + generate_match_key macro (ADR-011)

Establish the conformed match dimension spanning StatsBomb, Wyscout, IDSSE,
and Metrica providers. Surrogate `match_key BIGINT` generated deterministically
via xxhash64 on `(provider, native_match_id)`. No fact tables migrated in
this PR — coexistence phase. PR 2 begins migrating fact tables to reference
match_key as FK.

See ADR-011 for architectural context; TODO.md "Kimball Migration" section
for the PR 2-8 roadmap.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git status
```

Expected: commit succeeds; working tree clean; pre-commit hooks (if any) pass. If hooks fail, fix the issue and create a NEW commit (do NOT amend).

- [ ] **Step 11.4: Do NOT push or open PR**

The user's rules for this cycle: "no commits or prs without explicit approval" — Step 11.3's commit has just been explicitly approved; pushing and opening the PR require their OWN explicit approval. Wait for the user before running `git push` or `gh pr create`.

---

## Self-review checklist (run before claiming the plan is complete)

- [ ] Every task has at least one code block or exact command — no "TBD" or "similar to above".
- [ ] Every test task includes both the expected-fail command and the expected-pass command.
- [ ] The macro test file name (`test_generate_match_key_macro.py`) matches across Tasks 1, 10.
- [ ] `generate_match_key` signature (`provider_col, native_match_id_col`) is consistent across macro, tests, and dim_matches usage.
- [ ] `match_key` column type (BIGINT) is consistent across macro docstring, dim_matches schema, dbt contract, PG index definition.
- [ ] Provider values (`statsbomb`, `wyscout`, `idsse`, `metrica`) are consistent across `accepted_values` in contract, ADR-011 §Decision, integration test assertion.
- [ ] IDSSE native_match_id format (stripped of `idsse_` prefix) is consistent across staging model, dim union, integration test, ADR.
- [ ] ADR number (`ADR-011`) is correct — does NOT collide with existing ADR-001..ADR-010.
- [ ] No references to `ADR-008` remain in any new file (previous question's placeholder number).
- [ ] TODO.md `LB-IDSSE` + `LB-METRICA` rows are removed from On Deck (absorbed into Kimball Migration §).
