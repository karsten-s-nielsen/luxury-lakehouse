# Kimball PR 5a — Foundation + minimum-viable fact migrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Branch commit strategy:** Per user's single-commit-per-branch preference (`feedback_single_commit_squash`), DO NOT commit after each task. Each task's flow is: write test → run fail → implement → run pass → proceed. One terminal commit at Phase 7 after all E2E is green.
>
> **Execution conduct:** Advance task-by-task autonomously per `feedback_no_approval_asks_in_plan_execution`. Only pause for explicit user approval at: (a) Phase 7 terminal commit, (b) Phase 7 PR creation, (c) genuine external-action blockers (Figshare URL resolution, Databricks Job triggers touching production data, synced-table refreshes). Zero per-phase status narration per `feedback_no_per_phase_status_during_execution`.
>
> **Default to inline Write/Edit/Bash over Agent dispatches** per `feedback_agent_tool_requires_per_call_approval` — Agent tool use triggers a per-call approval prompt that breaks the autonomous flow.

**Goal:** Kimball-conform `dim_teams` and `dim_players` with `(provider, native_id)` BIGINT surrogates via new `generate_team_key` / `generate_player_key` macros across all four providers (StatsBomb, Wyscout, IDSSE, Metrica). Activate cross-provider entity resolution with a new `generate_entity_xref.py` script emitting SB↔WS↔IDSSE pairs for both players and teams. Close the pre-existing Wyscout teams.json ingestion gap. Add a Metrica pseudo-competition row. Migrate `fct_player_stats` + `fct_funnel_stages_agg` to carry `player_key`/`team_key`, closing both `severity: warn` suppressions from PR 4b. Populate `fct_match_summary` Wyscout home/away team_ids via a new `stg_wyscout__home_away_teams` bridge parsing the already-landed `teams_data_parsed` MAP.

**Architecture:** Four-provider dim unions keyed by `xxhash64(provider|native_id)`. Silver layer gains three new bridge models (`stg_idsse__home_away_teams`, `stg_wyscout__home_away_teams`, `stg_metrica__team_players`) plus `stg_wyscout__teams` from the new Figshare teams.json ingestion. Entity resolution becomes live via `entity_resolution_enabled=true` default; `int_player_xref` + new `int_team_xref` (both `view` materialisation) consume provider-labelled `bronze.player_xref_raw` + new `bronze.team_xref_raw`. Synthesised identity branches (Metrica anonymised, Wyscout unresolved-teamsdata fallback) carry `is_synthesized=true` + `synthesis_reason` attributes. Forward-compat: `is_anonymized BOOLEAN` on Metrica bronze distinguishes sample-CSV from future subscription data. Dual-column additive pattern: `team_id`/`player_id`/`canonical_player_id` legacies preserved through the coordinated 2026-07-22 sunset (PR 8 cleanup).

**Tech Stack:** dbt (Databricks adapter, `dbt-core>=1.10.0,<1.12.0`); Spark/Delta; PySpark; Databricks SQL Connector; rapidfuzz (fuzzy name matching); huggingface_hub; pytest (+ dbt-expectations); Taipy GUI; Lakebase PG synced tables; PEP 723 uv scripts; HuggingFace Hub (no new datasets in PR 5a — only org-level card awareness).

**Source spec:** `docs/superpowers/specs/2026-04-24-kimball-pr5-design.md`.

**Branch:** `kimball-pr5a-foundation` (tracking `origin/main` at 728245f, post-PR-4c merge). Already created at spec phase.

**Depends on:** PR 4c (HF card inventory parity) must be on main — confirmed at branch creation. PR 4a's live dbt CI provides the latent-bug surfacing pathway per `reference_live_ci_surfaces_latent_bugs`.

---

## Decisions required — resolve during execution

| # | Decision | Default (this plan assumes) | Resolved by |
|---|---|---|---|
| **D1** | Figshare `ndownloader.figshare.com/files/<id>` for teams.json | **Resolved via WebFetch at Task 2.1 Step 1** (collection page: `https://figshare.com/collections/Soccer_match_event_dataset/4415000`). Fallback via Figshare REST API if WebFetch returns stale/mangled HTML. | Task 2.1 |
| **D2** | Wyscout `teams_data_parsed` parse-failure rate — is the synthesised fallback cosmetic (~0 rows) or structurally meaningful | **Measured empirically at Task 2.4 Step 1** via `SELECT count(*) FILTER (WHERE teams_data_parsed IS NULL OR size(map_keys(teams_data_parsed)) = 0) FROM dev_silver.stg_wyscout__matches`. If count > 50 (>2.5% of ~1,900 WS matches), investigate source-data quality before shipping the fallback. If ≤ 50, fallback is cosmetic — ship as designed. | Task 2.4 |
| **D3** | IDSSE↔StatsBomb player xref coverage at confidence ≥ 70 | **Measured empirically at Task 3.4 Step 3** by running `generate_entity_xref.py` against dev warehouse + counting output rows. If 0 pairs: IDSSE silos by data (not by design) — documented in dim_players header. If > 0 pairs: xref populates as designed. Either outcome is acceptable; the infrastructure ships regardless. | Task 3.4 |
| **D4** | Lakebase synced-table schema evolution vs recreation for the 6 touched tables | **Default: auto-evolution via `refresh_synced_tables.py`** per `reference_lakebase_synced_table_auto_evolution` (empirically confirmed for additive columns on `fct_action_values` during PR 4b). All PR 5a changes are additive. Recreation via `maintain_synced_tables.py` is the fallback if a refresh errors out. | Task 6.1 |
| **D5** | `ingest_wyscout` Job trigger timing vs dbt build | **Sequence: ingest teams.json FIRST (populates `bronze.wyscout_teams`)** → then dbt build of staging + dim_teams. Staging-coverage tests for `wyscout_teams` fail if run before the ingestion populates bronze. | Task 2.3 + Task 4.2 |

---

## File structure map

### Created

| Path | Responsibility |
|---|---|
| `dbt_project/macros/generate_team_key.sql` | xxhash64 surrogate macro for dim_teams (mirrors `generate_competition_key`). |
| `dbt_project/macros/generate_player_key.sql` | xxhash64 surrogate macro for dim_players. |
| `scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql` | One-shot bronze schema migration: ALTER TABLE + UPDATE for existing sample rows. |
| `scripts/migrations/2026-04-24-add-provider-cols-to-player-xref-raw.sql` | One-shot bronze schema migration: ALTER TABLE ADD + UPDATE backfill for existing 2,780 SB↔WS rows. |
| `scripts/migrations/2026-04-24-create-team-xref-raw.sql` | One-shot bronze table creation: `CREATE TABLE IF NOT EXISTS ...`. |
| `dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql` | Bridge: `(match_id, side)` → real DFL TeamId from tracking. |
| `dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql` | Bridge: explodes `teams_data_parsed` MAP; fallback synth branch when parse NULL. |
| `dbt_project/models/staging/wyscout/stg_wyscout__teams.sql` | Clean Wyscout team roster from new `bronze.wyscout_teams`. |
| `dbt_project/models/staging/metrica/stg_metrica__team_players.sql` | Per-match synthesised team + player identity. |
| `dbt_project/models/intermediate/int_team_xref.sql` | Cross-provider team xref (mirrors int_player_xref). |
| `dbt_project/seeds/team_xref_overrides.csv` | Header-only seed; manual overrides as surfaced. |
| `scripts/generate_entity_xref.py` | PEP 723 fuzzy-match generator (rapidfuzz) — writes to `bronze.player_xref_raw` + `bronze.team_xref_raw`. |
| `src/tests/test_generate_entity_xref.py` | Unit: fuzzy-match correctness + MERGE idempotency. |
| `src/tests/test_int_player_xref_invariants.py` | Live-warehouse: confidence range, no self-loops, provider rules, injectivity per pair. |
| `src/tests/test_int_team_xref_invariants.py` | Same shape for teams. |
| `src/tests/test_metrica_ingestion_flag.py` | Unit: `is_anonymized=true` sample path; stub `false` subscription path. |
| `src/tests/test_wyscout_teams_contract.py` | Parser-level contract test for the new `stg_wyscout__teams` + `bronze.wyscout_teams` live DESCRIBE. |

### Modified

| Path | Reason |
|---|---|
| `dbt_project/dbt_project.yml` | Flip `vars.entity_resolution_enabled` default `false → true`. |
| `dbt_project/models/marts/dim_competitions.sql` | Add Metrica pseudo-competition CTE. |
| `dbt_project/models/marts/dim_teams.sql` | Complete rewrite to ADR-011 pattern — 4-provider union with Kimball surrogate, synthesis flags, canonical_team_key. |
| `dbt_project/models/marts/dim_players.sql` | Complete rewrite to ADR-011 pattern — 4-provider union, Kimball surrogate + preserved canonical_player_id, canonical_player_key, synthesis flags. |
| `dbt_project/models/marts/fct_match_summary.sql` | Wyscout CTE LEFT JOIN to `stg_wyscout__home_away_teams` to populate home/away team_id (previously NULL for ~36% of rows). |
| `dbt_project/models/marts/fct_player_stats.sql` | Add `player_key` + `team_key` via INNER JOIN dim_players + dim_teams; drops 1 NULL player_id outlier. |
| `dbt_project/models/marts/fct_funnel_stages_agg.sql` | Add `match_key`, `team_key`, `opponent_team_key`; restore `relationships` test to fct_match_summary. |
| `dbt_project/models/marts/_marts__models.yml` | Contract updates for dim_teams / dim_players / dim_competitions / fct_player_stats / fct_funnel_stages_agg / fct_match_summary. Flip 2 warn-suppressions to error. |
| `dbt_project/models/intermediate/int_player_xref.sql` | Remove gated-off branch; honour `source_a`/`source_b` cols; materialisation `ephemeral → view`. |
| `dbt_project/models/intermediate/_intermediate__models.yml` | Add contract entries for int_player_xref + int_team_xref (unique on xref grain, not_null confidence). |
| `dbt_project/seeds/player_xref_overrides.csv` | Schema extension: add `source_a`, `source_b` cols; backfill existing header to literal `'statsbomb', 'wyscout'`. |
| `dbt_project/models/staging/metrica/stg_metrica__matches.sql` | Hardcode `competition_id = 'metrica-sample'`. |
| `dbt_project/models/staging/metrica/_metrica__sources.yml` | Add `is_anonymized BOOLEAN` to `metrica_tracking`. |
| `dbt_project/models/staging/tracking/_tracking__sources.yml` | Add `is_anonymized BOOLEAN` to `tracking_player_metadata`. |
| `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml` | Add `source_a`, `source_b` to `player_xref_raw`; add new `team_xref_raw` table. |
| `dbt_project/models/staging/idsse/_idsse__sources.yml` | Add 14 already-present-but-undocumented columns to `idsse_tracking` (team_id + 13 PR-1.8-era cols). |
| `dbt_project/models/staging/idsse/stg_idsse__tracking.sql` | Surface `team_id` passthrough. |
| `dbt_project/models/staging/idsse/stg_idsse__passes.sql` | LEFT JOIN new `stg_idsse__home_away_teams` bridge → `team_id` populated. |
| `dbt_project/models/staging/wyscout/_wyscout__sources.yml` | Add new `wyscout_teams` table definition. |
| `dbt_project/models/staging/wyscout/_wyscout__models.yml` | Add `stg_wyscout__teams` entry. |
| `src/ingestion/wyscout.py` | Add `_TEAMS_URL` constant + `ingest_teams()` function + main() dispatch entry. Resolved Figshare ID gets embedded. Docstring updated. |
| `src/ingestion/metrica.py` | Set `is_anonymized=true` in sample-path bronze write; docstring + header comment document sample vs subscription contract. |
| `src/tests/fixtures/wyscout_bronze_schema_snapshot.json` | Add `wyscout_teams` entry with expected columns. |
| `src/tests/test_wyscout_bronze_coverage.py` | Add `wyscout_teams` entry. |
| `src/tests/test_metrica_bronze_coverage.py` | Assert `is_anonymized` surfaced. |
| `src/tests/test_idsse_bronze_coverage.py` | Assert `team_id` + 13 other cols surfaced. |
| `src/tests/test_bronze_live_schema.py` | Add live-DESCRIBE entries for `bronze.wyscout_teams` + `bronze.team_xref_raw` + updated `player_xref_raw` + updated `metrica_tracking` + updated `tracking_player_metadata`. |
| `src/tests/test_staging_coverage.py` | Pick up new staging models. |
| `workflow-cards/wf-wyscout.yaml` | Add `wyscout_teams` to ingestion-task outputs. |
| `dbt_project/models/staging/wyscout/stg_wyscout__matches.sql` | No SQL change required (teams_data_parsed already parsed at PR 1.5); documentation comment mentioning the new `stg_wyscout__home_away_teams` consumer. |
| `hf_taipy_app/src/state/shared.py` | **Conditional** (not confirmed needed) — if `get_team_key`/`get_player_key` helpers don't exist, add alongside existing `get_match_key`/`get_competition_key`. Verified at Task 5.3. |

### Explicitly NOT modified (Chesterton's Fence)

- **Taipy page queries that filter on `team_id`/`player_id`** — PR 5a adds `team_key`/`player_key` columns additively. Consumer migration happens in PR 5b (embeddings consumers) and PR 6+ for other marts. Keeping consumer code on the legacy columns during the 90-day window is by-design.
- **`src/ingestion/` writers for fct_player_stats / fct_funnel_stages_agg** — these marts are dbt-produced (SPADL/VAEP via silly-kicks, aggregation via dbt). The mart rebuilds on next dbt job run; ingestion code doesn't reference the new dim keys.
- **ADR directory** — no new ADR. PR 5a extends ADR-011 under the existing staged rollout.
- **Scripts: `train_football2vec_*`, `export_scoutgpt_training_data`, `export_embeddings_training_data`, `player_embeddings_v2`, `player_embeddings_common`** — scripted per spec §2 out-of-scope. Still read `canonical_player_id` during the dual-column window.
- **HF dataset card READMEs (`docs/huggingface/dataset-cards/`)** — PR 5b updates embedding card documentation; PR 5a ships no HF dataset changes.
- **`terraform/modules/synced_tables/main.tf`** — additive columns don't need TF edits; auto-evolution handles them.
- **`hf_taipy_app/src/queries/*.py`** — PR 5b consumer migration; no touching in 5a.
- **`src/tests/test_hf_publish_parity.py`** — no new HF artifacts; parity test unchanged.

---

## Phase 0: Pre-flight verification (read-only)

All downstream phases depend on these. Do not skip.

### Task 0.1: Confirm PR 4c is on main + live-CI green

**Files:** None.

- [ ] **Step 1:** Verify PR 4c is on `origin/main`:

```bash
git fetch origin main && git log origin/main --oneline -1
```

Expected output (or newer):

```
728245f feat(hf): PR 4c — shared hf_publish helper + full card inventory parity (#183)
```

If older than 728245f: investigate; PR 5a's branch base assumption breaks.

- [ ] **Step 2:** Confirm recent `dbt-live-ci.yml` runs are green:

```bash
gh run list --workflow=dbt-live-ci.yml --limit=5 --json conclusion,headBranch,displayTitle,url
```

Expected: recent runs show `"conclusion": "success"`. Failures must be triaged before PR 5a per `reference_live_ci_surfaces_latent_bugs` playbook.

### Task 0.2: Live DESCRIBE on `bronze.idsse_tracking`

**Files:** None.

- [ ] **Step 1:** Query Databricks to confirm all 14 columns are present:

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql

host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
http_path = os.environ["DATABRICKS_HTTP_PATH"]
token = os.environ["DATABRICKS_TOKEN"]

conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
cur = conn.cursor()
cur.execute("DESCRIBE TABLE soccer_analytics.bronze.idsse_tracking")
cols = [r.col_name for r in cur.fetchall() if r.col_name and not r.col_name.startswith('#')]
print(sorted(cols))
expected = {"team_id", "t", "s", "a", "d", "m", "ball_z", "ball_s", "ball_a", "ball_d", "ball_m", "ball_t", "ball_possession", "ball_status"}
missing = expected - set(cols)
print(f"Missing: {missing}")
assert not missing, f"Expected cols absent from bronze: {missing}"
conn.close()
PYEOF
```

Expected: no missing columns. If any missing: bronze re-ingest required (ADD TO Phase 1 scope).

### Task 0.3: Live count of `bronze.player_xref_raw`

**Files:** None.

- [ ] **Step 1:** Verify row count matches memory (2,780 per `project_kimball_migration_cycle.md`):

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql

host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
http_path = os.environ["DATABRICKS_HTTP_PATH"]
token = os.environ["DATABRICKS_TOKEN"]

conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
cur = conn.cursor()
cur.execute("SELECT count(*) AS n FROM soccer_analytics.bronze.player_xref_raw")
print(cur.fetchall())
cur.execute("DESCRIBE TABLE soccer_analytics.bronze.player_xref_raw")
for r in cur.fetchall():
    print(r)
conn.close()
PYEOF
```

Expected: `n` close to 2,780 (slight drift acceptable); no `source_a` or `source_b` column yet.

### Task 0.4: Confirm fct_player_stats NULL player_id row still present

**Files:** None.

- [ ] **Step 1:** Query Databricks to confirm the 1 NULL row:

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql

host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
http_path = os.environ["DATABRICKS_HTTP_PATH"]
token = os.environ["DATABRICKS_TOKEN"]

conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
cur = conn.cursor()
cur.execute("SELECT count(*) AS null_rows FROM soccer_analytics.dev_gold.fct_player_stats WHERE player_id IS NULL")
print(cur.fetchall())
cur.execute("SELECT count(*) AS null_opp FROM soccer_analytics.dev_gold.fct_funnel_stages_agg WHERE opponent_team_id IS NULL")
print(cur.fetchall())
conn.close()
PYEOF
```

Expected: `null_rows=1`, `null_opp=7587`. If different: update contract flip expected-row-drop calculations in Tasks 5.2 + 5.3.

### Task 0.5: Confirm `canonical_player_id` downstream consumer count

**Files:** None.

- [ ] **Step 1:** Grep the codebase to confirm the 57-file cascade assertion:

Use Grep tool: pattern `canonical_player_id`, output_mode=`files_with_matches`.

Expected: ~57 files match. If the count has changed meaningfully (±10), re-scope Task 4.3 (dim_players) to preserve compatibility with any new consumers. Critical invariant: the legacy column value must remain stable.

### Task 0.6: Confirm `bronze.wyscout_teams` does NOT exist (gap confirmation)

**Files:** None.

- [ ] **Step 1:** Rule out pre-existing ingestion artefact:

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql

host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
http_path = os.environ["DATABRICKS_HTTP_PATH"]
token = os.environ["DATABRICKS_TOKEN"]

conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
cur = conn.cursor()
cur.execute("SHOW TABLES FROM soccer_analytics.bronze LIKE 'wyscout*'")
print([r for r in cur.fetchall()])
conn.close()
PYEOF
```

Expected: only `wyscout_events`, `wyscout_matches`, `wyscout_players`. If `wyscout_teams` exists: STOP and investigate; someone already did this work and the plan's ingestion scope needs to change.

---

## Phase 1: Macros + bronze schema

### Task 1.1: `generate_team_key` macro

**Files:**
- Create: `dbt_project/macros/generate_team_key.sql`
- Test: `src/tests/test_generate_team_key_macro.py`

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_generate_team_key_macro.py`:

```python
"""Unit test for generate_team_key macro.

Asserts the rendered SQL matches the expected xxhash64(concat_ws('|', ...)) shape
and the null-passthrough behaviour mirrors generate_competition_key.
"""
from __future__ import annotations

import re
from pathlib import Path


MACRO_PATH = Path("dbt_project/macros/generate_team_key.sql")


def test_macro_file_exists() -> None:
    assert MACRO_PATH.exists(), f"{MACRO_PATH} missing"


def test_macro_signature_two_args() -> None:
    src = MACRO_PATH.read_text()
    assert "macro generate_team_key(provider_col, native_team_id_col)" in src


def test_macro_uses_xxhash64_with_delimiter() -> None:
    src = MACRO_PATH.read_text()
    assert "xxhash64" in src
    assert "concat_ws(" in src
    assert "'|'" in src, "delimiter must be the pipe character"


def test_macro_null_safe_branch_for_native_id() -> None:
    src = MACRO_PATH.read_text()
    assert re.search(r"when\s+\{\{\s*native_team_id_col\s*\}\}\s+is\s+null\s+then\s+null", src, re.IGNORECASE)


def test_macro_references_adr_011() -> None:
    src = MACRO_PATH.read_text()
    assert "ADR-011" in src
```

- [ ] **Step 2: Run the test to confirm FAIL**

```bash
uv run pytest src/tests/test_generate_team_key_macro.py -v
```

Expected: all 5 tests fail with "MACRO_PATH missing" or "AssertionError" (file absent).

- [ ] **Step 3: Implement the macro**

Create `dbt_project/macros/generate_team_key.sql`:

```sql
{% macro generate_team_key(provider_col, native_team_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_teams` dimension.

    Deterministic 64-bit hash of (provider, native_team_id) via Spark's
    `xxhash64`, returning a signed BIGINT compatible with PostgreSQL BIGINT
    semantics on Lakebase synced tables.

    Mirrors `generate_match_key` + `generate_competition_key` (same xxhash64 +
    concat_ws('|') pattern) so behaviour, collision bounds, and rationale are
    identical. Unifies StatsBomb + Wyscout (INT native team_ids stringified)
    with IDSSE DFL TeamIds ('DFL-CLU-XXXXXX') and Metrica synthesised IDs
    ('metrica_Sample_Game_1_home', etc.).

    The delimiter in `concat_ws` prevents concatenation ambiguities:
    (provider='ab', native='') would collide with (provider='a', native='b')
    without it. The '|' character is not present in any provider name or
    native team ID format we ingest.

    `cast(... as string)` normalizes mixed-type natives: StatsBomb/Wyscout
    use BIGINT natively; IDSSE/Metrica use STRING. Both hash identically
    once stringified.

    Args:
      provider_col: Column name or expression for the provider identifier
                    (e.g. 'statsbomb', 'wyscout', 'idsse', 'metrica')
      native_team_id_col: Column name or expression for the provider's
                          native team ID (BIGINT or STRING)

    Returns:
      BIGINT surrogate key. NULL when native_team_id is NULL.

    Reference: ADR-011 — Kimball surrogate keys for conformed dimensions
               across StatsBomb / Wyscout / IDSSE / Metrica; extended
               from dim_matches to dim_teams in PR 5a.
#}
    case
        when {{ native_team_id_col }} is null then null
        else xxhash64(
            concat_ws(
                '|',
                {{ provider_col }},
                cast({{ native_team_id_col }} as string)
            )
        )
    end
{% endmacro %}
```

- [ ] **Step 4: Run the test to confirm PASS**

```bash
uv run pytest src/tests/test_generate_team_key_macro.py -v
```

Expected: all 5 tests pass.

### Task 1.2: `generate_player_key` macro

**Files:**
- Create: `dbt_project/macros/generate_player_key.sql`
- Test: `src/tests/test_generate_player_key_macro.py`

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_generate_player_key_macro.py`:

```python
"""Unit test for generate_player_key macro. Mirrors generate_team_key."""
from __future__ import annotations

import re
from pathlib import Path


MACRO_PATH = Path("dbt_project/macros/generate_player_key.sql")


def test_macro_file_exists() -> None:
    assert MACRO_PATH.exists()


def test_macro_signature_two_args() -> None:
    src = MACRO_PATH.read_text()
    assert "macro generate_player_key(provider_col, native_player_id_col)" in src


def test_macro_uses_xxhash64_with_delimiter() -> None:
    src = MACRO_PATH.read_text()
    assert "xxhash64" in src
    assert "concat_ws(" in src
    assert "'|'" in src


def test_macro_null_safe_branch_for_native_id() -> None:
    src = MACRO_PATH.read_text()
    assert re.search(r"when\s+\{\{\s*native_player_id_col\s*\}\}\s+is\s+null\s+then\s+null", src, re.IGNORECASE)


def test_macro_references_adr_011() -> None:
    src = MACRO_PATH.read_text()
    assert "ADR-011" in src
```

- [ ] **Step 2: Run the test to confirm FAIL**

```bash
uv run pytest src/tests/test_generate_player_key_macro.py -v
```

Expected: all 5 tests fail.

- [ ] **Step 3: Implement the macro**

Create `dbt_project/macros/generate_player_key.sql`:

```sql
{% macro generate_player_key(provider_col, native_player_id_col) %}
{#
    Kimball surrogate key for the conformed `dim_players` dimension.

    Deterministic 64-bit hash of (provider, native_player_id) via Spark's
    `xxhash64`. Mirrors generate_team_key / generate_match_key /
    generate_competition_key.

    Unifies StatsBomb (INT player_id) + Wyscout (INT wyId) + IDSSE
    (DFL PersonId STRING) + Metrica (synthesised STRING 'metrica_<match>_<side>_<key>').

    Reference: ADR-011 — Kimball surrogate keys; PR 5a extends pattern to players.
#}
    case
        when {{ native_player_id_col }} is null then null
        else xxhash64(
            concat_ws(
                '|',
                {{ provider_col }},
                cast({{ native_player_id_col }} as string)
            )
        )
    end
{% endmacro %}
```

- [ ] **Step 4: Run the test to confirm PASS**

```bash
uv run pytest src/tests/test_generate_player_key_macro.py -v
```

Expected: all 5 tests pass.

### Task 1.3: Bronze migration — `is_anonymized` on `metrica_tracking` + `tracking_player_metadata`

**Files:**
- Create: `scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql`
- Modify: `dbt_project/models/staging/metrica/_metrica__sources.yml`
- Modify: `dbt_project/models/staging/tracking/_tracking__sources.yml`
- Modify: `src/ingestion/metrica.py`
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1: Write the migration SQL**

Create `scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql`:

```sql
-- Kimball PR 5a — add is_anonymized flag to Metrica bronze tables.
-- Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §4
-- Current data is 100% sample CSV — backfill to true.
-- Future subscription-path ingestion sets false at write time.

ALTER TABLE soccer_analytics.bronze.metrica_tracking
  ADD COLUMN IF NOT EXISTS is_anonymized BOOLEAN;

UPDATE soccer_analytics.bronze.metrica_tracking
   SET is_anonymized = true
 WHERE is_anonymized IS NULL;

ALTER TABLE soccer_analytics.bronze.tracking_player_metadata
  ADD COLUMN IF NOT EXISTS is_anonymized BOOLEAN;

-- tracking_player_metadata covers IDSSE + SkillCorner today, both real. Default false.
UPDATE soccer_analytics.bronze.tracking_player_metadata
   SET is_anonymized = false
 WHERE is_anonymized IS NULL;
```

- [ ] **Step 2: Write a live-schema failing test**

Update `src/tests/test_bronze_live_schema.py` to add an assertion:

```python
def test_metrica_tracking_has_is_anonymized() -> None:
    conn = _connect()
    try:
        cols = {r.col_name for r in _describe(conn, "soccer_analytics.bronze.metrica_tracking")}
        assert "is_anonymized" in cols, f"is_anonymized absent: {sorted(cols)}"
    finally:
        conn.close()


def test_tracking_player_metadata_has_is_anonymized() -> None:
    conn = _connect()
    try:
        cols = {r.col_name for r in _describe(conn, "soccer_analytics.bronze.tracking_player_metadata")}
        assert "is_anonymized" in cols
    finally:
        conn.close()
```

(The `_connect` and `_describe` helpers already exist in the file per PR 1.8 drop-safety sweep — use the same shape as existing tests.)

- [ ] **Step 3: Run tests to confirm FAIL**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_metrica_tracking_has_is_anonymized src/tests/test_bronze_live_schema.py::test_tracking_player_metadata_has_is_anonymized -v
```

Expected: both fail with "is_anonymized absent".

- [ ] **Step 4: Execute the migration against dev Databricks**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os, pathlib
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
for stmt in pathlib.Path("scripts/migrations/2026-04-24-add-metrica-is-anonymized.sql").read_text().split(";"):
    s = stmt.strip()
    if s and not s.startswith("--"):
        print(f"Executing: {s[:80]}...")
        cur.execute(s)
conn.close()
print("Migration complete")
PYEOF
```

Expected output: 4 statements executed (2 ALTER + 2 UPDATE). Duration: <30s for sample data scale.

- [ ] **Step 5: Run live-schema tests to confirm PASS**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_metrica_tracking_has_is_anonymized src/tests/test_bronze_live_schema.py::test_tracking_player_metadata_has_is_anonymized -v
```

Expected: both pass.

- [ ] **Step 6: Update source YAML — Metrica**

Edit `dbt_project/models/staging/metrica/_metrica__sources.yml` — under the `metrica_tracking` table's `columns:` block, add:

```yaml
          - name: is_anonymized
            description: >
              True for sample-CSV ingestion from the Metrica open-data GitHub
              repo (3 anonymised matches). False for future subscription-API
              ingestion with real player/team identity. Set at bronze-write
              time by ingestion.metrica; propagates to stg_metrica__team_players
              and drives synthesis branch selection in dim_teams + dim_players.
              See docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §4.
```

- [ ] **Step 7: Update source YAML — tracking_player_metadata**

Edit `dbt_project/models/staging/tracking/_tracking__sources.yml` — under `tracking_player_metadata.columns`, add:

```yaml
          - name: is_anonymized
            description: >
              False for IDSSE / SkillCorner (real player identity). Forward-compat
              column; not currently surfaced in staging. PR 5a adds the bronze
              column to unify the flag shape with metrica_tracking.
```

- [ ] **Step 8: Update `src/ingestion/metrica.py` to set the flag**

Use Grep on `src/ingestion/metrica.py` for `write_delta_table` or similar write call. Add `is_anonymized=True` (or append column to the DataFrame pre-write):

```python
# Before the write_delta_table call for metrica_tracking bronze:
tracking_df["is_anonymized"] = True  # Sample-CSV path; subscription sets False.
```

And update the module docstring:

```python
"""Metrica tracking + match ingestion.

...

``is_anonymized`` flag: set True here because the sample-CSV path writes
anonymised data (player keys 'Player11'–'Player25' per side, no real team
names). A future subscription-API path would set False and pass real
identities through to dim_teams / dim_players. See
docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §4.
"""
```

- [ ] **Step 9: Add a unit test for the ingestion flag**

Create `src/tests/test_metrica_ingestion_flag.py`:

```python
"""Unit test for Metrica ingestion's is_anonymized flag contract."""
from __future__ import annotations

import inspect

from ingestion import metrica


def test_ingestion_module_docstring_documents_is_anonymized() -> None:
    doc = inspect.getdoc(metrica)
    assert doc is not None
    assert "is_anonymized" in doc, "module docstring must document the is_anonymized contract"
    assert "sample" in doc.lower() and "subscription" in doc.lower(), \
        "docstring must distinguish sample vs subscription paths"


def test_ingestion_source_sets_is_anonymized_true_literal() -> None:
    src = inspect.getsource(metrica)
    assert 'is_anonymized' in src
    assert 'True' in src, "sample path must write is_anonymized=True"
```

Run: `uv run pytest src/tests/test_metrica_ingestion_flag.py -v` — expected PASS after the ingestion file edit in Step 8.

### Task 1.4: Bronze migration — provider cols on `player_xref_raw` + backfill

**Files:**
- Create: `scripts/migrations/2026-04-24-add-provider-cols-to-player-xref-raw.sql`
- Modify: `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml`
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1: Write the migration SQL**

Create `scripts/migrations/2026-04-24-add-provider-cols-to-player-xref-raw.sql`:

```sql
-- Kimball PR 5a — extend bronze.player_xref_raw to support cross-provider pairs.
-- Existing 2,780 rows are all SB↔WS (only provider pair the legacy matcher handled).
-- New rows (from scripts/generate_entity_xref.py) will cover SB↔IDSSE + WS↔IDSSE.
-- Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2

ALTER TABLE soccer_analytics.bronze.player_xref_raw
  ADD COLUMN IF NOT EXISTS source_a STRING;

ALTER TABLE soccer_analytics.bronze.player_xref_raw
  ADD COLUMN IF NOT EXISTS source_b STRING;

-- One-time backfill: every existing row is SB↔WS.
UPDATE soccer_analytics.bronze.player_xref_raw
   SET source_a = 'statsbomb',
       source_b = 'wyscout'
 WHERE source_a IS NULL OR source_b IS NULL;

-- Enable name-mapping mode (required before any RENAME/DROP later; harmless if already on).
ALTER TABLE soccer_analytics.bronze.player_xref_raw
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
                     'delta.minReaderVersion' = '2',
                     'delta.minWriterVersion' = '5');
```

- [ ] **Step 2: Write a failing live-schema test**

Add to `src/tests/test_bronze_live_schema.py`:

```python
def test_player_xref_raw_has_provider_cols() -> None:
    conn = _connect()
    try:
        cols = {r.col_name for r in _describe(conn, "soccer_analytics.bronze.player_xref_raw")}
        assert "source_a" in cols
        assert "source_b" in cols

        # Backfill coverage — no NULL provider on existing rows
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*) AS null_provider_rows
              FROM soccer_analytics.bronze.player_xref_raw
             WHERE source_a IS NULL OR source_b IS NULL
        """)
        assert cur.fetchall()[0].null_provider_rows == 0
    finally:
        conn.close()
```

- [ ] **Step 3: Run test to confirm FAIL**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_player_xref_raw_has_provider_cols -v
```

Expected: FAIL.

- [ ] **Step 4: Execute the migration against dev Databricks**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os, pathlib
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
for stmt in pathlib.Path("scripts/migrations/2026-04-24-add-provider-cols-to-player-xref-raw.sql").read_text().split(";"):
    s = stmt.strip()
    if s and not s.startswith("--"):
        print(f"Executing: {s[:80]}...")
        cur.execute(s)
conn.close()
PYEOF
```

Expected: 4 statements executed.

- [ ] **Step 5: Run test to confirm PASS**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_player_xref_raw_has_provider_cols -v
```

Expected: PASS.

- [ ] **Step 6: Update the source YAML**

Edit `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml` — extend `player_xref_raw.columns`:

```yaml
          - name: source_a
            description: >
              Provider label for side A of the xref pair (e.g., 'statsbomb',
              'wyscout', 'idsse'). PR 5a addition. Existing 2,780 rows
              backfilled to 'statsbomb'; new rows from generate_entity_xref.py
              cover all provider pairs.
          - name: source_b
            description: >
              Provider label for side B of the xref pair. Convention:
              source_a < source_b lexicographically, so each unordered
              pair appears exactly once.
```

### Task 1.5: Create `bronze.team_xref_raw`

**Files:**
- Create: `scripts/migrations/2026-04-24-create-team-xref-raw.sql`
- Modify: `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml`
- Modify: `src/tests/test_bronze_live_schema.py`

- [ ] **Step 1: Write the migration SQL**

Create `scripts/migrations/2026-04-24-create-team-xref-raw.sql`:

```sql
-- Kimball PR 5a — new bronze table for cross-provider team identity pairs.
-- Mirror of player_xref_raw shape. Populated by scripts/generate_entity_xref.py.
-- Ref: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2 + §3.4

CREATE TABLE IF NOT EXISTS soccer_analytics.bronze.team_xref_raw (
    source_a       STRING,
    team_id_a        STRING,
    source_b       STRING,
    team_id_b        STRING,
    confidence       DOUBLE,
    match_layer      INT,
    resolution_type  STRING,
    _ingested_at     TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

- [ ] **Step 2: Write a failing test**

Add to `src/tests/test_bronze_live_schema.py`:

```python
def test_team_xref_raw_exists_with_expected_schema() -> None:
    conn = _connect()
    try:
        cols = {r.col_name for r in _describe(conn, "soccer_analytics.bronze.team_xref_raw")}
        expected = {"source_a", "team_id_a", "source_b", "team_id_b",
                    "confidence", "match_layer", "resolution_type", "_ingested_at"}
        missing = expected - cols
        assert not missing, f"Missing cols: {missing}"
    finally:
        conn.close()
```

- [ ] **Step 3: Run test to confirm FAIL**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_team_xref_raw_exists_with_expected_schema -v
```

Expected: FAIL (table doesn't exist).

- [ ] **Step 4: Execute the migration**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os, pathlib
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
cur.execute(pathlib.Path("scripts/migrations/2026-04-24-create-team-xref-raw.sql").read_text())
conn.close()
PYEOF
```

- [ ] **Step 5: Run test to confirm PASS**

```bash
uv run pytest src/tests/test_bronze_live_schema.py::test_team_xref_raw_exists_with_expected_schema -v
```

- [ ] **Step 6: Add source entry to dbt**

Edit `dbt_project/models/staging/entity_resolution/_entity_resolution__sources.yml` — add a new table entry:

```yaml
      - name: team_xref_raw
        description: >
          Cross-provider team identity mapping produced by
          scripts/generate_entity_xref.py. Mirror of player_xref_raw.
          Grain: one row per (source_a, team_id_a, source_b, team_id_b).
          Convention: source_a < source_b lexicographically.
        columns:
          - name: source_a
            description: Provider label for side A
          - name: team_id_a
            description: Native team ID on side A (stringified)
          - name: source_b
            description: Provider label for side B
          - name: team_id_b
            description: Native team ID on side B
          - name: confidence
            description: Fuzzy-match confidence 70-100 (values below 70 discarded at generator)
          - name: match_layer
            description: 0 = manual override, >=1 = automated tier
          - name: resolution_type
            description: 'automated' or 'manual_override'
          - name: _ingested_at
            description: UTC timestamp of MERGE write
```

### Task 1.6: IDSSE bronze source YAML — add 14 columns

**Files:**
- Modify: `dbt_project/models/staging/idsse/_idsse__sources.yml`
- Modify: `src/tests/test_idsse_bronze_coverage.py`

- [ ] **Step 1: Add the 14 columns to the source YAML**

Edit `dbt_project/models/staging/idsse/_idsse__sources.yml` — under `idsse_tracking.columns`, append (the 14 columns confirmed present via Task 0.2):

```yaml
          - name: team_id
            description: >
              Real DFL TeamId (e.g., 'DFL-CLU-XXXXXX') from the enclosing
              FrameSet TeamId attribute during position parsing (src/ingestion/idsse.py:580).
              Surfaced in stg_idsse__tracking from PR 5a onward; previously present in
              bronze but not documented or consumed. Feeds stg_idsse__home_away_teams
              bridge → dim_teams.
          - name: t
            description: Tracking-frame t-value (PR 1.8-era column).
          - name: s
            description: Player speed (m/s).
          - name: a
            description: Player acceleration (m/s²).
          - name: d
            description: Player direction (radians or degrees per bronze contract).
          - name: m
            description: Movement flag boolean.
          - name: ball_z
            description: Ball z-coordinate (height; meters).
          - name: ball_s
            description: Ball speed (m/s).
          - name: ball_a
            description: Ball acceleration (m/s²).
          - name: ball_d
            description: Ball direction.
          - name: ball_m
            description: Ball movement flag.
          - name: ball_t
            description: Ball tracking-frame t-value.
          - name: ball_possession
            description: Ball possession state (home/away/neutral label).
          - name: ball_status
            description: Ball status (in_play / out / etc.).
```

- [ ] **Step 2: Extend the coverage test to assert these cols are declared**

Update `src/tests/test_idsse_bronze_coverage.py` — add a `team_id` + 13 other cols to the `_IDSSE_TRACKING_EXPECTED_SOURCE_COLS` constant (inspect the existing constant shape; if it's a set/frozenset, just extend it). Run:

```bash
uv run pytest src/tests/test_idsse_bronze_coverage.py -v
```

Expected: PASS (source-level assertions) but staging-level assertion for `team_id` surfaced in staging may FAIL until Task 2.2. That's expected — Task 2.2 surfaces it.

---

## Phase 2: Staging layer

### Task 2.1: Resolve Figshare teams.json download URL

**Files:** None (research only).

- [ ] **Step 1: Fetch the Figshare collection page**

Use WebFetch tool with URL `https://figshare.com/collections/Soccer_match_event_dataset/4415000` and prompt:

```
Find the download URL for the teams.json file in this Figshare collection.
Specifically extract the "ndownloader.figshare.com/files/<NUMERIC_ID>" URL
pointing at teams.json. Also report file size if available. Ignore the URLs
for events, matches, players — those are already known.
```

Expected: response contains `https://ndownloader.figshare.com/files/<NNNNN>` for teams.json.

- [ ] **Step 2: Verify the URL resolves**

```bash
curl -sI --max-time 30 "https://ndownloader.figshare.com/files/<NNNNN>" | head -5
```

Expected: HTTP 200 or 302 (figshare may redirect). If 404: try Figshare API fallback:

```bash
curl -s --max-time 30 "https://api.figshare.com/v2/collections/4415000" | \
    python -c "import sys, json; d = json.load(sys.stdin); print(json.dumps(d, indent=2)[:4000])"
```

Parse the response for the teams.json file's `download_url` field.

- [ ] **Step 3: Record the resolved URL**

Note the URL value — it goes into the constant in Task 2.2 Step 3. Sanity-check: the 3 existing Wyscout URLs in `src/ingestion/wyscout.py` are numeric IDs in the 14,464,xxx – 15,073,xxx range; teams.json is likely in the same range.

### Task 2.2: Extend `src/ingestion/wyscout.py` — add `ingest_teams`

**Files:**
- Modify: `src/ingestion/wyscout.py`
- Modify: `src/tests/fixtures/wyscout_bronze_schema_snapshot.json`

- [ ] **Step 1: Write failing tests for the new function**

Create `src/tests/test_wyscout_teams_ingestion.py`:

```python
"""Contract tests for the new ingest_teams path in wyscout.py."""
from __future__ import annotations

import inspect

from ingestion import wyscout


def test_teams_url_constant_exists() -> None:
    assert hasattr(wyscout, "_TEAMS_URL"), "_TEAMS_URL constant must exist"
    assert wyscout._TEAMS_URL.startswith("https://ndownloader.figshare.com/files/"), \
        f"URL format mismatch: {wyscout._TEAMS_URL}"


def test_ingest_teams_function_exists() -> None:
    assert hasattr(wyscout, "ingest_teams"), "ingest_teams function must exist"
    sig = inspect.signature(wyscout.ingest_teams)
    params = set(sig.parameters.keys())
    assert {"spark", "catalog", "schema"}.issubset(params), \
        f"ingest_teams signature mismatch: {params}"


def test_guard_check_includes_teams_table() -> None:
    """Guard must check teams table presence alongside events/matches/players."""
    src = inspect.getsource(wyscout._WyscoutGuard)
    assert "wyscout_teams" in src, "guard must consider wyscout_teams in skip check"
```

- [ ] **Step 2: Run test to confirm FAIL**

```bash
uv run pytest src/tests/test_wyscout_teams_ingestion.py -v
```

Expected: all 3 tests fail.

- [ ] **Step 3: Add `_TEAMS_URL` constant and `_WYSCOUT_TEAMS_EXPECTED_COLS`**

Edit `src/ingestion/wyscout.py`. Find the `_PLAYERS_URL` constant around line 107 and add immediately after:

```python
# Figshare URL for Wyscout teams.json — resolved Task 2.1.
# Field schema: wyId (int), officialName, name, city, area (nested struct),
# type, country, etc. See Pappalardo et al. 2019 for the full schema.
_TEAMS_URL = "https://ndownloader.figshare.com/files/<NNNNN>"  # REPLACE with resolved URL from Task 2.1
```

Add in the expected-cols block near existing `_WYSCOUT_PLAYERS_EXPECTED_COLS` (around line 72):

```python
_WYSCOUT_TEAMS_EXPECTED_COLS: tuple[str, ...] = expected_cols_from_snapshot(
    _WYSCOUT_SNAPSHOT_TABLES, "wyscout_teams"
)
_WYSCOUT_TEAMS_DTYPE_OVERRIDES: dict[str, str] = dtype_overrides_from_snapshot(
    _WYSCOUT_SNAPSHOT_TABLES, "wyscout_teams"
)
```

- [ ] **Step 4: Add `ingest_teams` function**

Insert after the existing `ingest_players` function in `src/ingestion/wyscout.py` (find via Grep for `def ingest_players`):

```python
def ingest_teams(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: pathlib.Path | None = None,
) -> None:
    """Ingest Wyscout teams.json from Figshare into bronze.wyscout_teams.

    7 competitions × ~40 teams each ≈ ~280 teams (open-data scope, 2017/18).

    Schema preserved verbatim from Pappalardo et al. 2019 Figshare payload;
    nested JSON columns (`area`) are serialized to JSON strings so dbt can
    parse them with SQL JSON functions.

    Follows the same download/parse/write pattern as ``ingest_players``.
    """
    logger = logging.getLogger(__name__)
    logger.info("wyscout.teams.start url=%s", _TEAMS_URL)

    if data_dir is not None and (data_dir / "teams.json").exists():
        with open(data_dir / "teams.json", encoding="utf-8") as fp:
            teams = json.load(fp)
    else:
        raw = fetch_url(_TEAMS_URL)
        teams = json.loads(raw)

    logger.info("wyscout.teams.parsed count=%d", len(teams))

    # teams.json is a flat JSON array
    df = pd.DataFrame(teams)

    # Decode literal \uXXXX escapes like ingest_players does
    df = _decode_unicode_escapes(df)

    # Serialize nested JSON-object columns to strings for Delta compat
    df = serialize_json_columns(df, ["area"])

    # Column name normalization (keep camelCase for bronze-completeness)
    df = _normalize_mixed_types(df, logger=logger)

    df = finalize_bronze_df(
        df,
        expected_cols=_WYSCOUT_TEAMS_EXPECTED_COLS,
        dtype_overrides=_WYSCOUT_TEAMS_DTYPE_OVERRIDES,
    )

    validated_count = validate_dataframe(df, min_rows=100)
    logger.info("wyscout.teams.validated rows=%d", validated_count)

    write_delta_table(
        spark=spark,
        df=df,
        table_name=f"{catalog}.{schema}.wyscout_teams",
        mode="overwrite",  # static reference data; overwrite safely
        row_count=validated_count,
    )
    logger.info("wyscout.teams.written rows=%d", validated_count)
    gc.collect()
```

- [ ] **Step 5: Update `_WyscoutGuard.check` to include wyscout_teams**

Find the `_WyscoutGuard.check` method (around line 83) and update:

```python
def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
    """Skip if all Wyscout competitions are already ingested."""
    import logging as _logging

    from ingestion.utils import tolerate_missing_table

    expected = len(_COMPETITIONS)
    _guard_logger = _logging.getLogger(__name__)
    with tolerate_missing_table(_guard_logger, "Wyscout tables missing — needs ingestion"):
        e_count = spark.table(f"{catalog}.{schema}.wyscout_events").select("competition_name").distinct().count()
        m_count = spark.table(f"{catalog}.{schema}.wyscout_matches").select("competition_name").distinct().count()
        p_exists = spark.table(f"{catalog}.{schema}.wyscout_players").limit(1).count() > 0
        t_exists = spark.table(f"{catalog}.{schema}.wyscout_teams").limit(1).count() > 0
        if e_count >= expected and m_count >= expected and p_exists and t_exists:
            return FilterResult(workflow_id=self.workflow_id, count=0)
    return FilterResult(workflow_id=self.workflow_id, count=1)
```

- [ ] **Step 6: Wire into main() dispatch**

Find the `main()` entry point (use Grep for `def main` or `@workflow` decorator). Add an `ingest_teams(spark, catalog, schema)` call alongside the existing 3 ingestions. Match the existing error-handling pattern exactly.

- [ ] **Step 7: Add snapshot fixture**

Edit `src/tests/fixtures/wyscout_bronze_schema_snapshot.json`. Add a `"wyscout_teams"` entry parallel to the existing three. Use this shape (match existing per-table format exactly):

```json
"wyscout_teams": {
  "expected_cols": ["wyId", "officialName", "name", "city", "area", "type", "_ingested_at"],
  "dtype_overrides": {
    "wyId": "int64",
    "officialName": "object",
    "name": "object",
    "city": "object",
    "area": "object",
    "type": "object"
  }
}
```

(Confirm format against existing `wyscout_players` entry first; mirror that shape.)

- [ ] **Step 8: Run the new unit tests**

```bash
uv run pytest src/tests/test_wyscout_teams_ingestion.py -v
```

Expected: all 3 PASS.

### Task 2.3: Trigger one-shot `ingest_wyscout` Databricks Job

**Files:** None (execution step).

⚠️ **USER APPROVAL REQUIRED** — this writes to production-shared `soccer_analytics.bronze` (even though it's additive + idempotent).

- [ ] **Step 1: Locate the Wyscout ingestion Job ID**

```bash
gh variable list --json name,value --jq '.[] | select(.name | contains("WYSCOUT"))' 2>/dev/null || \
    echo "Check terraform/modules/jobs/main.tf for job name 'ingest_wyscout' and its resolved ID"
```

Or query directly:

```bash
uv run --with databricks-sdk python <<'PYEOF'
import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
for job in w.jobs.list():
    if "wyscout" in (job.settings.name or "").lower():
        print(f"{job.job_id} — {job.settings.name}")
PYEOF
```

- [ ] **Step 2: Ask user for approval before triggering**

Present to user: "Request approval to trigger Databricks Job `ingest_wyscout` (job_id=<N>) as a one-shot run to populate `bronze.wyscout_teams` (~280 rows, idempotent, additive). This is a production-adjacent action."

- [ ] **Step 3: Trigger the job and poll (background)**

Once approved:

```bash
uv run --with databricks-sdk python scripts/trigger_job_oneshot.py --job-id <N> --wait 2>&1 | tee /tmp/wyscout_teams_ingest.log &
```

(Adjust if `trigger_job_oneshot.py` doesn't exist; use `scripts/trigger_dbt_job.py` as a pattern.)

Run in background per the long-running-bash rule. Poll `/tmp/wyscout_teams_ingest.log` every 30s.

- [ ] **Step 4: Verify `bronze.wyscout_teams` has expected cardinality**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
cur.execute("SELECT count(*) AS n FROM soccer_analytics.bronze.wyscout_teams")
print(cur.fetchall())
cur.execute("DESCRIBE TABLE soccer_analytics.bronze.wyscout_teams")
for r in cur.fetchall():
    print(r)
conn.close()
PYEOF
```

Expected: `n` between 200 and 300 (roughly ~280 teams). If < 100 or > 500: investigate before proceeding.

- [ ] **Step 5: Add live-DESCRIBE test**

Add to `src/tests/test_bronze_live_schema.py`:

```python
def test_wyscout_teams_exists() -> None:
    conn = _connect()
    try:
        cols = {r.col_name for r in _describe(conn, "soccer_analytics.bronze.wyscout_teams")}
        expected = {"wyId", "officialName", "name", "city", "area", "type", "_ingested_at"}
        assert expected.issubset(cols), f"Missing: {expected - cols}"
    finally:
        conn.close()
```

Run: `uv run pytest src/tests/test_bronze_live_schema.py::test_wyscout_teams_exists -v` → expected PASS.

### Task 2.4: Create `stg_wyscout__teams` + staging coverage

**Files:**
- Create: `dbt_project/models/staging/wyscout/stg_wyscout__teams.sql`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__sources.yml`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml`
- Modify: `src/tests/test_wyscout_bronze_coverage.py`

- [ ] **Step 1: Add `wyscout_teams` source entry**

Edit `dbt_project/models/staging/wyscout/_wyscout__sources.yml` — add under `tables:`:

```yaml
      - name: wyscout_teams
        description: >
          Wyscout team roster from Figshare teams.json (Pappalardo et al. 2019).
          ~280 teams across 7 competitions, 2017/18 season. Ingested by
          ingestion.wyscout.ingest_teams; closes pre-existing team-name gap
          for dim_teams Wyscout rows. PR 5a addition.
        columns:
          - name: wyId
            description: Wyscout team ID (integer surrogate key)
          - name: officialName
            description: Official team name
          - name: name
            description: Display/short team name
          - name: city
            description: City name
          - name: area
            description: Nested JSON struct (name, alpha3code) — serialized as JSON string
          - name: type
            description: Team type (club / national)
          - name: _ingested_at
            description: UTC timestamp when row was written
```

- [ ] **Step 2: Write failing contract test**

Create `src/tests/test_wyscout_teams_contract.py`:

```python
"""Contract test: stg_wyscout__teams exposes every bronze col + expected types."""
from __future__ import annotations

from pathlib import Path


def test_staging_model_exists() -> None:
    assert Path("dbt_project/models/staging/wyscout/stg_wyscout__teams.sql").exists()


def test_staging_model_selects_team_id() -> None:
    src = Path("dbt_project/models/staging/wyscout/stg_wyscout__teams.sql").read_text()
    assert "team_id" in src or "as team_id" in src


def test_staging_model_selects_team_name() -> None:
    src = Path("dbt_project/models/staging/wyscout/stg_wyscout__teams.sql").read_text()
    assert "team_name" in src


def test_staging_model_preserves_bronze_passthrough_cols() -> None:
    src = Path("dbt_project/models/staging/wyscout/stg_wyscout__teams.sql").read_text()
    # Bronze-completeness per PR 1.5 pattern
    for col in ("wyId", "officialName", "_ingested_at"):
        assert col in src, f"Bronze passthrough missing: {col}"
```

- [ ] **Step 3: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_wyscout_teams_contract.py -v
```

Expected: 4 tests fail (file doesn't exist).

- [ ] **Step 4: Create `stg_wyscout__teams.sql`**

Create `dbt_project/models/staging/wyscout/stg_wyscout__teams.sql`:

```sql
-- stg_wyscout__teams.sql
-- Wyscout team roster (Figshare teams.json, Pappalardo et al. 2019).
--
-- ~280 teams across 7 competitions (2017/18 season). PR 5a addition —
-- closes the pre-existing team-name coverage gap for dim_teams Wyscout rows.
--
-- Area is a nested JSON struct (name, alpha3code) — parsed here.

with source as (

    select * from {{ source('wyscout', 'wyscout_teams') }}

),

final as (

    select
        cast(wyId as int)                                as team_id,
        officialName                                     as official_name,
        name                                             as team_name,
        city,
        get_json_object(area, '$.name')                  as area_name,
        get_json_object(area, '$.alpha3code')            as area_alpha3,
        type                                             as team_type,

        -- Bronze passthroughs surfaced for Kimball completeness
        cast(wyId as bigint)                             as wyId,
        officialName                                     as officialName,
        city                                             as city_raw,
        area                                             as area,
        type                                             as type,
        _ingested_at                                     as _ingested_at,

        'wyscout'                                        as data_source

    from source
    where wyId is not null

)

select * from final
```

- [ ] **Step 5: Add staging model entry to models YAML**

Edit `dbt_project/models/staging/wyscout/_wyscout__models.yml` — add:

```yaml
  - name: stg_wyscout__teams
    description: >
      Cleaned Wyscout team roster. Feeds dim_teams Wyscout CTE to populate
      team_name (previously NULL because teams.json wasn't ingested).
    columns:
      - name: team_id
        data_type: int
        description: Wyscout team identifier (from wyId)
        data_tests:
          - unique
          - not_null
      - name: official_name
        data_type: string
      - name: team_name
        data_type: string
      - name: city
        data_type: string
      - name: area_name
        data_type: string
      - name: area_alpha3
        data_type: string
      - name: team_type
        data_type: string
```

- [ ] **Step 6: Extend coverage test**

Edit `src/tests/test_wyscout_bronze_coverage.py` — add a new test method covering `wyscout_teams`:

```python
def test_wyscout_teams_bronze_coverage():
    """Every bronze col on wyscout_teams surfaces in stg_wyscout__teams.

    Same shape as the existing per-table bronze-coverage tests.
    """
    _assert_bronze_coverage(
        bronze_table="wyscout_teams",
        staging_model="stg_wyscout__teams",
    )
```

(Adjust to match the existing helper's signature in the file — read the file first to confirm the helper name.)

- [ ] **Step 7: Run tests to confirm PASS**

```bash
uv run pytest src/tests/test_wyscout_teams_contract.py src/tests/test_wyscout_bronze_coverage.py -v
```

Expected: all PASS.

### Task 2.5: IDSSE `team_id` passthrough in staging

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__tracking.sql`

- [ ] **Step 1: Write failing test**

Create `src/tests/test_stg_idsse_tracking_surfaces_team_id.py`:

```python
"""Ensures stg_idsse__tracking now surfaces the team_id col from bronze."""
from pathlib import Path


def test_staging_includes_team_id_passthrough() -> None:
    src = Path("dbt_project/models/staging/idsse/stg_idsse__tracking.sql").read_text()
    assert "team_id" in src, "team_id column must be surfaced in stg_idsse__tracking"
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_stg_idsse_tracking_surfaces_team_id.py -v
```

Expected: FAIL.

- [ ] **Step 3: Edit `stg_idsse__tracking.sql`**

Find the `normalized` CTE's SELECT and add a `team_id` passthrough alongside `team`:

```sql
        -- Real DFL TeamId (e.g., 'DFL-CLU-XXXXXX') — present in bronze since
        -- PR 1.8 but surfaced in staging from PR 5a onward. Feeds dim_teams
        -- IDSSE CTE and stg_idsse__home_away_teams bridge.
        team_id                                         as team_id,
```

- [ ] **Step 4: Run to confirm PASS**

```bash
uv run pytest src/tests/test_stg_idsse_tracking_surfaces_team_id.py -v
```

Expected: PASS.

### Task 2.6: New model `stg_idsse__home_away_teams`

**Files:**
- Create: `dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql`
- Modify: `dbt_project/models/staging/idsse/_idsse__models.yml`

- [ ] **Step 1: Write a failing shape test**

Create `src/tests/test_stg_idsse_home_away_teams.py`:

```python
from pathlib import Path


MODEL = Path("dbt_project/models/staging/idsse/stg_idsse__home_away_teams.sql")


def test_model_file_exists() -> None:
    assert MODEL.exists()


def test_model_selects_match_id_side_team_id() -> None:
    src = MODEL.read_text()
    # Minimum invariant columns for the bridge
    assert "match_id" in src
    assert "side" in src or "as side" in src
    assert "team_id" in src
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_stg_idsse_home_away_teams.py -v
```

- [ ] **Step 3: Create `stg_idsse__home_away_teams.sql`**

Create the file:

```sql
-- stg_idsse__home_away_teams.sql
-- Bridge model: one row per (match_id, side) → real DFL TeamId.
--
-- IDSSE tracking carries team_id + team (home/away) per row. Collapsing
-- to grain (match_id, side, team_id) gives a bridge joinable from
-- event-layer staging (stg_idsse__passes, etc.) where `*_team` columns
-- are "home"/"away" strings only (no raw DFL TeamId on events).
--
-- Grain: one row per (match_id, side). Uniqueness enforced by schema test
-- in _idsse__models.yml.

with tracking as (

    select distinct
        match_id,
        team          as side,
        team_id
    from {{ ref('stg_idsse__tracking') }}
    where team in ('home', 'away')
      and team_id is not null

),

final as (

    select
        regexp_replace(match_id, '^idsse_', '') as match_id,
        side,
        team_id                                  as team_id
    from tracking

)

select * from final
```

- [ ] **Step 4: Add model entry to YAML**

Edit `dbt_project/models/staging/idsse/_idsse__models.yml` — add:

```yaml
  - name: stg_idsse__home_away_teams
    description: >
      Bridge from (match_id, side) to real DFL TeamId for IDSSE matches.
      Consumed by stg_idsse__passes to hydrate team_id on event rows
      (which only carry "home"/"away" strings natively).
    columns:
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
            combination_of_columns: [match_id, side]
```

- [ ] **Step 5: Run to confirm PASS**

```bash
uv run pytest src/tests/test_stg_idsse_home_away_teams.py -v
```

### Task 2.7: Update `stg_idsse__passes` to use bridge

**Files:**
- Modify: `dbt_project/models/staging/idsse/stg_idsse__passes.sql`

- [ ] **Step 1: Read the existing model to understand its shape**

Use Read tool on `dbt_project/models/staging/idsse/stg_idsse__passes.sql`. Find where `team` (home/away) is selected; identify an appropriate insertion point for the LEFT JOIN + `team_id` hydration.

- [ ] **Step 2: Add failing test**

Create `src/tests/test_stg_idsse_passes_has_team_id.py`:

```python
from pathlib import Path


def test_stg_idsse_passes_joins_home_away_bridge() -> None:
    src = Path("dbt_project/models/staging/idsse/stg_idsse__passes.sql").read_text()
    assert "stg_idsse__home_away_teams" in src, \
        "stg_idsse__passes must LEFT JOIN the new bridge to hydrate team_id"
    assert "team_id" in src
```

- [ ] **Step 3: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_stg_idsse_passes_has_team_id.py -v
```

- [ ] **Step 4: Add the LEFT JOIN**

Edit `dbt_project/models/staging/idsse/stg_idsse__passes.sql` — add a JOIN in the appropriate CTE. Example shape (adjust to the actual model structure):

```sql
-- (inside the CTE that has match_id + team side available)
left join {{ ref('stg_idsse__home_away_teams') }} bridge
    on regexp_replace(events.match_id, '^idsse_', '') = bridge.match_id
   and lower(events.play_team) = bridge.side
```

And in the SELECT list, add:

```sql
    bridge.team_id                            as team_id,
```

(Exact column name for the side/home/away depends on the model — `play_team` for Play-type events, `shot_team` for ShotAtGoal, etc. Use the appropriate column for the pass event_type.)

- [ ] **Step 5: Run to confirm PASS**

```bash
uv run pytest src/tests/test_stg_idsse_passes_has_team_id.py -v
```

### Task 2.8: New model `stg_wyscout__home_away_teams` (with synth fallback)

**Files:**
- Create: `dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql`
- Modify: `dbt_project/models/staging/wyscout/_wyscout__models.yml`

- [ ] **Step 1: Measure teams_data_parsed parse-failure rate (D2 resolution)**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
# Count total WS matches + matches with unusable teams_data_parsed
cur.execute("""
    SELECT
        count(*) AS total,
        count(*) FILTER (WHERE teams_data_parsed IS NULL OR size(map_keys(teams_data_parsed)) = 0) AS unusable
    FROM soccer_analytics.dev_silver.stg_wyscout__matches
""")
print(cur.fetchall())
conn.close()
PYEOF
```

Expected: `unusable / total` < 2.5% → fallback is cosmetic. If ≥2.5%: investigate source data before shipping fallback.

- [ ] **Step 2: Write failing tests**

Create `src/tests/test_stg_wyscout_home_away_teams.py`:

```python
from pathlib import Path


MODEL = Path("dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql")


def test_model_file_exists() -> None:
    assert MODEL.exists()


def test_model_has_explode_primary_path() -> None:
    src = MODEL.read_text()
    assert "lateral view" in src.lower() or "explode(" in src.lower()


def test_model_has_synth_fallback_branch() -> None:
    src = MODEL.read_text()
    assert "wyscout_unresolved" in src
    assert "is_synthesized" in src
    assert "synthesis_reason" in src
```

- [ ] **Step 3: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_stg_wyscout_home_away_teams.py -v
```

- [ ] **Step 4: Create the model**

Create `dbt_project/models/staging/wyscout/stg_wyscout__home_away_teams.sql`:

```sql
-- stg_wyscout__home_away_teams.sql
-- Bridge: one row per (match_id, side, team_id) for Wyscout matches.
--
-- PR 1.5 landed `teams_data_parsed` as a MAP<STRING, STRUCT<...>> on
-- stg_wyscout__matches but nothing consumed it until PR 5a. The MAP's keys
-- are stringified team_ids; values carry the {side, teamId, ...} struct.
--
-- Convention: the FIRST map key is the home team per Wyscout spec and the
-- existing Python helper (spadl_adapter.resolve_wyscout_home_team_ids).
--
-- Synth fallback: when teams_data_parsed is NULL or empty (parse failure),
-- emit two synthesised rows `wyscout_unresolved_{match_id}_{side}` with
-- is_synthesized=true + synthesis_reason='wyscout_unresolved_teamsdata' so
-- downstream dim_teams marks them distinctly. This closes the NULL
-- opponent_team_id gap in fct_funnel_stages_agg (the 7,587-row warn).

with matches as (

    select
        match_id,
        teams_data_parsed
    from {{ ref('stg_wyscout__matches') }}

),

-- Primary path: explode teams_data_parsed to (match_id, side, team_id)
exploded as (

    select
        match_id,
        case when v.side = 'home' then 'home'
             when v.side = 'away' then 'away'
             else null
        end                                            as side,
        cast(k as int)                                 as team_id,
        false                                          as is_synthesized,
        cast(null as string)                           as synthesis_reason
    from matches
    lateral view explode(teams_data_parsed) AS k, v
    where matches.teams_data_parsed is not null
      and size(map_keys(matches.teams_data_parsed)) > 0

),

-- Fallback path: synth rows for matches where parse yielded NULL/empty map
synth as (

    select
        match_id,
        'home'                                          as side,
        cast(null as int)                               as team_id,
        true                                            as is_synthesized,
        'wyscout_unresolved_teamsdata'                  as synthesis_reason
    from matches
    where teams_data_parsed is null
       or size(map_keys(teams_data_parsed)) = 0

    union all

    select
        match_id,
        'away'                                          as side,
        cast(null as int)                               as team_id,
        true                                            as is_synthesized,
        'wyscout_unresolved_teamsdata'                  as synthesis_reason
    from matches
    where teams_data_parsed is null
       or size(map_keys(teams_data_parsed)) = 0

),

final as (

    select
        match_id,
        side,
        -- For synth rows, produce a deterministic native_team_id that dim_teams
        -- can use. For real rows, native_team_id = cast(team_id as string).
        case
            when is_synthesized
                then concat('wyscout_unresolved_', cast(match_id as string), '_', side)
            else cast(team_id as string)
        end                                             as native_team_id,
        team_id,
        is_synthesized,
        synthesis_reason
    from (
        select * from exploded
        union all
        select * from synth
    )
    where side is not null  -- Drop any rows where side couldn't be classified

)

select * from final
```

- [ ] **Step 5: Add model entry**

Edit `dbt_project/models/staging/wyscout/_wyscout__models.yml` — add:

```yaml
  - name: stg_wyscout__home_away_teams
    description: >
      Bridge: one row per (match_id, side) mapping to Wyscout team_id.
      Primary path: explodes teams_data_parsed. Fallback synth branch
      when parse fails — native_team_id = 'wyscout_unresolved_<match>_<side>'
      with is_synthesized=true.
    columns:
      - name: match_id
        data_type: bigint
        data_tests:
          - not_null
      - name: side
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['home', 'away']
      - name: native_team_id
        data_type: string
        data_tests:
          - not_null
      - name: team_id
        data_type: int
        description: NULL for synth fallback rows; populated for real parsed rows
      - name: is_synthesized
        data_type: boolean
        data_tests:
          - not_null
      - name: synthesis_reason
        data_type: string
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [match_id, side]
```

- [ ] **Step 6: Run to confirm PASS**

```bash
uv run pytest src/tests/test_stg_wyscout_home_away_teams.py -v
```

### Task 2.9: New model `stg_metrica__team_players` + `stg_metrica__matches` competition

**Files:**
- Create: `dbt_project/models/staging/metrica/stg_metrica__team_players.sql`
- Modify: `dbt_project/models/staging/metrica/stg_metrica__matches.sql`
- Modify: `dbt_project/models/staging/metrica/_metrica__models.yml`

- [ ] **Step 1: Write failing test for stg_metrica__team_players**

Create `src/tests/test_stg_metrica_team_players.py`:

```python
from pathlib import Path


MODEL = Path("dbt_project/models/staging/metrica/stg_metrica__team_players.sql")


def test_exists() -> None:
    assert MODEL.exists()


def test_emits_native_team_id_and_native_player_id() -> None:
    src = MODEL.read_text()
    assert "native_team_id" in src
    assert "native_player_id" in src


def test_carries_is_anonymized_from_bronze() -> None:
    src = MODEL.read_text()
    assert "is_anonymized" in src


def test_synth_pattern_uses_match_and_side() -> None:
    src = MODEL.read_text()
    assert "metrica_" in src
    assert "match_id" in src
    assert "home" in src.lower() and "away" in src.lower()
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_stg_metrica_team_players.py -v
```

- [ ] **Step 3: Create `stg_metrica__team_players.sql`**

```sql
-- stg_metrica__team_players.sql
-- Per-match team + player identity for Metrica.
--
-- Metrica sample data is anonymised: home_players + away_players are
-- MAP<STRING, STRUCT<...>> JSON columns where keys are "Player11"-"Player25"
-- style strings with no real identity. Per ADR-011 + PR 5a design:
-- synthesise per-match team + player IDs rather than fabricating
-- cross-match identity we can't verify.
--
-- Forward-compat: bronze `is_anonymized` flag drives synthesis branch.
-- Future subscription data (is_anonymized=false) flows through a parallel
-- real-identity branch.
--
-- Grain:
--   Teams: one row per (match_id, side).
--   Players: one row per (match_id, side, player_key_in_map).

with tracking as (

    select
        match_id,
        home_players,
        away_players,
        is_anonymized
    from {{ source('metrica', 'metrica_tracking') }}

),

home_exploded as (

    select distinct
        match_id,
        'home'                                          as side,
        is_anonymized,
        k                                               as player_key_in_map
    from tracking
    lateral view explode(
        from_json(home_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) AS k, v

),

away_exploded as (

    select distinct
        match_id,
        'away'                                          as side,
        is_anonymized,
        k                                               as player_key_in_map
    from tracking
    lateral view explode(
        from_json(away_players, 'MAP<STRING, STRUCT<x:DOUBLE, y:DOUBLE>>')
    ) AS k, v

),

all_team_players as (

    select * from home_exploded
    union all
    select * from away_exploded

),

final as (

    select
        match_id,
        side,
        is_anonymized,
        player_key_in_map,

        -- Synthesised identities (anonymised path)
        case
            when is_anonymized then concat('metrica_', match_id, '_', side)
            -- Real subscription path (future): use real team_id from bronze.
            -- Since current bronze doesn't carry that column, emit NULL for
            -- non-anonymised rows until subscription ingestion wires it.
            else cast(null as string)
        end                                             as native_team_id,

        case
            when is_anonymized then concat('metrica_', match_id, '_', side, '_', player_key_in_map)
            else cast(null as string)
        end                                             as native_player_id,

        case when is_anonymized then true else false end as is_synthesized,

        case
            when is_anonymized then 'metrica_anonymized'
            else cast(null as string)
        end                                             as synthesis_reason

    from all_team_players

)

select * from final
```

- [ ] **Step 4: Hardcode Metrica competition_id**

Edit `dbt_project/models/staging/metrica/stg_metrica__matches.sql` — change the `final` CTE to include `competition_id`:

```sql
final as (

    select
        match_id                 as native_match_id,
        'metrica'                as provider,
        -- PR 5a: pseudo-competition sentinel so dim_matches auto-resolves
        -- competition_key and Metrica passes surface in Pass Map's filter
        -- cascade. Ref TODO #32 + docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2.
        'metrica-sample'         as competition_id,
        'Home'                   as home_team_name,
        'Away'                   as away_team_name

    from tracking_matches

)
```

- [ ] **Step 5: Add model entries**

Edit `dbt_project/models/staging/metrica/_metrica__models.yml` — add `stg_metrica__team_players`:

```yaml
  - name: stg_metrica__team_players
    description: >
      Per-match team + player identity for Metrica. Synthesised IDs for
      anonymised sample data; real-ID path ready for subscription data
      (flag driven by bronze is_anonymized).
    columns:
      - name: match_id
        data_type: string
        data_tests:
          - not_null
      - name: side
        data_type: string
        data_tests:
          - accepted_values:
              arguments:
                values: ['home', 'away']
      - name: is_anonymized
        data_type: boolean
      - name: player_key_in_map
        data_type: string
        data_tests:
          - not_null
      - name: native_team_id
        data_type: string
      - name: native_player_id
        data_type: string
      - name: is_synthesized
        data_type: boolean
      - name: synthesis_reason
        data_type: string
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [match_id, side, player_key_in_map]
```

- [ ] **Step 6: Run to confirm PASS**

```bash
uv run pytest src/tests/test_stg_metrica_team_players.py -v
```

### Task 2.10: Extend Metrica bronze coverage test

**Files:**
- Modify: `src/tests/test_metrica_bronze_coverage.py`

- [ ] **Step 1: Add `is_anonymized` to expected-columns list**

Read the existing file and extend the `_METRICA_TRACKING_EXPECTED_SOURCE_COLS` (or equivalent) constant to include `is_anonymized`. Same shape as Task 1.6 did for IDSSE's 14 new cols.

- [ ] **Step 2: Run the coverage test**

```bash
uv run pytest src/tests/test_metrica_bronze_coverage.py -v
```

Expected: PASS.

### Task 2.11: Staging-coverage meta test

**Files:**
- Modify: `src/tests/test_staging_coverage.py`

- [ ] **Step 1: Ensure new staging models are surfaced**

If `test_staging_coverage.py` enumerates known staging models explicitly, add the four new ones (`stg_wyscout__teams`, `stg_wyscout__home_away_teams`, `stg_idsse__home_away_teams`, `stg_metrica__team_players`). Otherwise, if it auto-discovers, just run.

```bash
uv run pytest src/tests/test_staging_coverage.py -v
```

Expected: PASS.

---

## Phase 3: Entity resolution infrastructure

### Task 3.1: Flip `entity_resolution_enabled` default

**Files:**
- Modify: `dbt_project/dbt_project.yml`

- [ ] **Step 1: Locate the var**

Grep `dbt_project/dbt_project.yml` for `entity_resolution_enabled`. Confirm current default is `false`.

- [ ] **Step 2: Flip to true**

Edit the vars block:

```yaml
vars:
  # ...
  entity_resolution_enabled: true  # PR 5a (ADR-011): activate cross-provider SB↔WS↔IDSSE xref.
```

- [ ] **Step 3: Run `dbt parse` to confirm no YAML breakage**

```bash
cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks --with dbt-utils --with dbt-expectations dbt parse --profiles-dir ../.dbt --target dev
```

Expected: "Found X models, Y tests, ..."; no errors.

### Task 3.2: Extend `int_player_xref` — provider cols + view materialisation

**Files:**
- Modify: `dbt_project/models/intermediate/int_player_xref.sql`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Modify: `dbt_project/seeds/player_xref_overrides.csv`

- [ ] **Step 1: Backfill provider cols in the seed**

Edit `dbt_project/seeds/player_xref_overrides.csv` — add `source_a,source_b` columns to the header + every data row. If the file currently has headers `statsbomb_player_id,wyscout_player_id,action`, transform to `source_a,player_id_a,source_b,player_id_b,action` (and split any existing rows accordingly — every existing row becomes `statsbomb,<sb_id>,wyscout,<ws_id>,<action>`).

Also verify `dbt_project/dbt_project.yml` seed config or `_seeds__models.yml` declares column types for the new columns (string).

- [ ] **Step 2: Write failing test for the new int_player_xref shape**

Create `src/tests/test_int_player_xref_invariants.py`:

```python
"""Live-warehouse invariants for int_player_xref post-PR-5a extension.

Tests assume `entity_resolution_enabled=true` and that
`scripts/generate_entity_xref.py` has been run at least once to populate
bronze.player_xref_raw with cross-provider pairs.
"""
from __future__ import annotations

import os

import pytest
from databricks import sql


@pytest.fixture(scope="module")
def conn():
    c = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    yield c
    c.close()


def _fetch(conn, sql_text: str):
    cur = conn.cursor()
    cur.execute(sql_text)
    return cur.fetchall()


def test_confidence_range_70_to_100(conn) -> None:
    rows = _fetch(conn, """
        SELECT count(*) AS oob
          FROM soccer_analytics.dev_silver.int_player_xref
         WHERE confidence < 70 OR confidence > 100
    """)
    assert rows[0].oob == 0


def test_no_self_loops(conn) -> None:
    rows = _fetch(conn, """
        SELECT count(*) AS loops
          FROM soccer_analytics.dev_silver.int_player_xref
         WHERE source_a = source_b AND player_id_a = player_id_b
    """)
    assert rows[0].loops == 0


def test_provider_ordering_invariant(conn) -> None:
    """Convention: source_a < source_b lexicographically."""
    rows = _fetch(conn, """
        SELECT count(*) AS bad
          FROM soccer_analytics.dev_silver.int_player_xref
         WHERE source_a >= source_b
    """)
    assert rows[0].bad == 0


def test_providers_in_known_set(conn) -> None:
    rows = _fetch(conn, """
        SELECT DISTINCT source_a AS provider FROM soccer_analytics.dev_silver.int_player_xref
        UNION
        SELECT DISTINCT source_b FROM soccer_analytics.dev_silver.int_player_xref
    """)
    seen = {r.provider for r in rows}
    assert seen.issubset({"statsbomb", "wyscout", "idsse", "metrica"}), f"Unknown: {seen}"


def test_injectivity_per_provider_pair(conn) -> None:
    """Within one (source_a, source_b) pair, each player on side A maps to at most one on side B."""
    rows = _fetch(conn, """
        WITH dups AS (
            SELECT source_a, source_b, player_id_a, count(DISTINCT player_id_b) AS n
              FROM soccer_analytics.dev_silver.int_player_xref
             GROUP BY source_a, source_b, player_id_a
            HAVING count(DISTINCT player_id_b) > 1
        )
        SELECT count(*) AS dup_count FROM dups
    """)
    assert rows[0].dup_count == 0
```

- [ ] **Step 2: Run to confirm FAIL (entity_resolution not yet live + generator not run yet)**

```bash
uv run pytest src/tests/test_int_player_xref_invariants.py -v
```

Expected: likely FAIL with "view/table not found" (if `int_player_xref` is still ephemeral) or "source_a col doesn't exist". That's expected — Step 3 rewrites the model.

- [ ] **Step 3: Rewrite `int_player_xref.sql`**

Replace the file contents with:

```sql
-- int_player_xref.sql
-- Cross-provider player identity mapping.
--
-- PR 5a (ADR-011): extended from SB↔WS-only to cross-provider —
-- (source_a, player_id_a) ↔ (source_b, player_id_b) at confidence ≥ 70.
-- Populated by scripts/generate_entity_xref.py + manual overrides via
-- seeds/player_xref_overrides.csv.
--
-- Materialisation: view (previously ephemeral). Flipped so
-- test_int_player_xref_invariants.py can query it directly.

{{ config(
    materialized='view',
    enabled=var('entity_resolution_enabled', false)
) }}

with automated_matches as (

    select
        cast(source_a as string)          as source_a,
        cast(player_id_a as string)         as player_id_a,
        cast(source_b as string)          as source_b,
        cast(player_id_b as string)         as player_id_b,
        confidence,
        match_layer
    from {{ source('entity_resolution', 'player_xref_raw') }}
    where confidence >= 70.0
      and source_a is not null
      and source_b is not null
      and source_a < source_b  -- Enforce ordering invariant

),

overrides as (

    select
        cast(source_a as string)          as source_a,
        cast(player_id_a as string)         as player_id_a,
        cast(source_b as string)          as source_b,
        cast(player_id_b as string)         as player_id_b,
        action
    from {{ ref('player_xref_overrides') }}

),

-- Remove automated matches vetoed by a manual override (force_reject or force_match).
-- force_match pairs are re-added below with 100% confidence + layer=0.
filtered as (

    select
        a.source_a,
        a.player_id_a,
        a.source_b,
        a.player_id_b,
        a.confidence,
        a.match_layer,
        'automated' as resolution_type
    from automated_matches a
    left join overrides o
        on  a.source_a = o.source_a
       and a.player_id_a = o.player_id_a
       and a.source_b = o.source_b
       and a.player_id_b = o.player_id_b
    where o.source_a is null

),

forced as (

    select
        o.source_a,
        o.player_id_a,
        o.source_b,
        o.player_id_b,
        100.0 as confidence,
        0 as match_layer,
        'manual_override' as resolution_type
    from overrides o
    where o.action = 'force_match'

),

combined as (

    select * from filtered
    union all
    select * from forced

)

select * from combined
```

- [ ] **Step 4: Add contract entry**

Edit `dbt_project/models/intermediate/_intermediate__models.yml` — add (or update the existing int_player_xref entry):

```yaml
  - name: int_player_xref
    description: >
      Cross-provider player identity mapping (automated xref + manual overrides).
      Grain: (source_a, player_id_a, source_b, player_id_b).
      Convention: source_a < source_b lexicographically.
    columns:
      - name: source_a
        data_type: string
        data_tests:
          - not_null
      - name: player_id_a
        data_type: string
        data_tests:
          - not_null
      - name: source_b
        data_type: string
        data_tests:
          - not_null
      - name: player_id_b
        data_type: string
        data_tests:
          - not_null
      - name: confidence
        data_type: double
        data_tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              arguments:
                min_value: 70
                max_value: 100
      - name: match_layer
        data_type: int
      - name: resolution_type
        data_type: string
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [source_a, player_id_a, source_b, player_id_b]
```

### Task 3.3: Create `int_team_xref` + `team_xref_overrides`

**Files:**
- Create: `dbt_project/models/intermediate/int_team_xref.sql`
- Create: `dbt_project/seeds/team_xref_overrides.csv`
- Modify: `dbt_project/models/intermediate/_intermediate__models.yml`
- Create: `src/tests/test_int_team_xref_invariants.py`

- [ ] **Step 1: Create the seed file**

Create `dbt_project/seeds/team_xref_overrides.csv`:

```csv
source_a,team_id_a,source_b,team_id_b,action
```

(Header-only; manual overrides added as surfaced.)

- [ ] **Step 2: Write failing invariants test**

Create `src/tests/test_int_team_xref_invariants.py` — identical shape to `test_int_player_xref_invariants.py` but targeting `int_team_xref` + `team_id_a`/`team_id_b` columns. (Copy/paste-adapt.)

- [ ] **Step 3: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_int_team_xref_invariants.py -v
```

Expected: FAIL (model doesn't exist yet).

- [ ] **Step 4: Create `int_team_xref.sql`**

```sql
-- int_team_xref.sql
-- Cross-provider team identity mapping — mirror of int_player_xref.
--
-- Populated by scripts/generate_entity_xref.py + seeds/team_xref_overrides.csv.

{{ config(
    materialized='view',
    enabled=var('entity_resolution_enabled', false)
) }}

with automated_matches as (

    select
        cast(source_a as string)          as source_a,
        cast(team_id_a as string)           as team_id_a,
        cast(source_b as string)          as source_b,
        cast(team_id_b as string)           as team_id_b,
        confidence,
        match_layer
    from {{ source('entity_resolution', 'team_xref_raw') }}
    where confidence >= 70.0
      and source_a is not null
      and source_b is not null
      and source_a < source_b

),

overrides as (

    select
        cast(source_a as string)          as source_a,
        cast(team_id_a as string)           as team_id_a,
        cast(source_b as string)          as source_b,
        cast(team_id_b as string)           as team_id_b,
        action
    from {{ ref('team_xref_overrides') }}

),

filtered as (

    select
        a.*,
        'automated' as resolution_type
    from automated_matches a
    left join overrides o
        on  a.source_a = o.source_a
       and a.team_id_a = o.team_id_a
       and a.source_b = o.source_b
       and a.team_id_b = o.team_id_b
    where o.source_a is null

),

forced as (

    select
        source_a, team_id_a, source_b, team_id_b,
        100.0 as confidence, 0 as match_layer, 'manual_override' as resolution_type
    from overrides
    where action = 'force_match'

),

combined as (

    select * from filtered
    union all
    select * from forced

)

select * from combined
```

- [ ] **Step 5: Add contract entry**

Edit `dbt_project/models/intermediate/_intermediate__models.yml` — add entry for `int_team_xref` mirroring `int_player_xref`'s shape (substitute `team_id_a`/`team_id_b`).

### Task 3.4: Create `scripts/generate_entity_xref.py`

**Files:**
- Create: `scripts/generate_entity_xref.py`
- Create: `src/tests/test_generate_entity_xref.py`

- [ ] **Step 1: Write failing unit tests**

Create `src/tests/test_generate_entity_xref.py`:

```python
"""Unit tests for fuzzy-match and MERGE-key logic in generate_entity_xref.py.

The script is a PEP 723 standalone; import via runpy for function-level tests.
"""
from __future__ import annotations

import importlib.util
import pathlib


SCRIPT = pathlib.Path("scripts/generate_entity_xref.py")


def _load_module():
    spec = importlib.util.spec_from_file_location("generate_entity_xref", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_fuzzy_match_identical_names_scores_100() -> None:
    mod = _load_module()
    score = mod.fuzzy_match_score("Cristiano Ronaldo", "Cristiano Ronaldo")
    assert score == 100


def test_fuzzy_match_word_order_variant_scores_above_90() -> None:
    mod = _load_module()
    # token_sort_ratio handles reordering
    score = mod.fuzzy_match_score("Ronaldo, Cristiano", "Cristiano Ronaldo")
    assert score >= 90


def test_fuzzy_match_threshold_70_filters_weak_pairs() -> None:
    mod = _load_module()
    score = mod.fuzzy_match_score("Lionel Messi", "Cristiano Ronaldo")
    assert score < 70  # Different player


def test_provider_ordering_convention() -> None:
    """Emitted xref rows must have source_a < source_b."""
    mod = _load_module()
    rows = mod.emit_pair_ordered("wyscout", "100", "statsbomb", "200", 85, 1)
    assert rows["source_a"] == "statsbomb"
    assert rows["source_b"] == "wyscout"
    assert rows["player_id_a"] == "200"
    assert rows["player_id_b"] == "100"
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_generate_entity_xref.py -v
```

Expected: FAIL ("SCRIPT doesn't exist").

- [ ] **Step 3: Create `scripts/generate_entity_xref.py`**

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "databricks-sql-connector>=3.0",
#     "rapidfuzz>=3.0",
#     "pandas>=2.0",
# ]
# ///
"""Generate cross-provider entity xref rows for bronze.{player,team}_xref_raw.

Fuzzy-match player + team names across StatsBomb, Wyscout, IDSSE (Metrica
is anonymised → unreachable). Emits rows at confidence ≥ 70 with the
source_a < source_b ordering convention.

Idempotent: uses Delta MERGE INTO on the unique xref grain
(source_a, player_id_a, source_b, player_id_b). Re-runs update
confidences in place without duplication.

Usage:
    uv run scripts/generate_entity_xref.py --dry-run
    uv run scripts/generate_entity_xref.py  # executes MERGE
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass

import pandas as pd
from databricks import sql
from rapidfuzz import fuzz


CONFIDENCE_THRESHOLD = 70
DBX_CATALOG = "soccer_analytics"


def fuzzy_match_score(a: str, b: str) -> int:
    """Token-sort ratio — handles word-order variants (surname-first vs given-name-first)."""
    return int(fuzz.token_sort_ratio(a, b))


def emit_pair_ordered(
    source_a: str, id_a: str, source_b: str, id_b: str,
    confidence: int, match_layer: int,
) -> dict[str, object]:
    """Emit an xref row enforcing source_a < source_b lexicographically."""
    if source_a > source_b:
        source_a, source_b = source_b, source_a
        id_a, id_b = id_b, id_a
    return {
        "source_a": source_a, "player_id_a": id_a,
        "source_b": source_b, "player_id_b": id_b,
        "confidence": confidence, "match_layer": match_layer,
        "resolution_type": "automated",
    }


def _connect():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def fetch_rosters(conn) -> dict[str, pd.DataFrame]:
    """Pull player + team rosters per provider from dev_gold staging."""
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT player_id, player_name
          FROM soccer_analytics.dev_gold.stg_statsbomb__lineups
         WHERE player_id IS NOT NULL AND player_name IS NOT NULL
    """)
    sb_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_name"])

    cur.execute("""
        SELECT DISTINCT player_id, player_name, nationality
          FROM soccer_analytics.dev_gold.stg_wyscout__players
         WHERE player_id IS NOT NULL AND player_name IS NOT NULL
    """)
    ws_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_name", "nationality"])

    cur.execute("""
        SELECT DISTINCT player_id, player_display_name
          FROM soccer_analytics.dev_gold.stg_tracking__player_metadata
         WHERE provider = 'idsse'
           AND player_id IS NOT NULL
           AND player_display_name IS NOT NULL
    """)
    idsse_players = pd.DataFrame(cur.fetchall(), columns=["player_id", "player_display_name"])

    cur.execute("""
        SELECT DISTINCT team_id, team_name
          FROM soccer_analytics.dev_gold.stg_statsbomb__events
         WHERE team_id IS NOT NULL AND team_name IS NOT NULL
    """)
    sb_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    cur.execute("""
        SELECT DISTINCT team_id, team_name
          FROM soccer_analytics.dev_gold.stg_wyscout__teams
         WHERE team_id IS NOT NULL AND team_name IS NOT NULL
    """)
    ws_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    cur.execute("""
        SELECT DISTINCT team_id, max(team_display_name) AS team_name
          FROM soccer_analytics.dev_gold.stg_tracking__player_metadata
         WHERE provider = 'idsse' AND team_id IS NOT NULL
         GROUP BY team_id
    """)
    idsse_teams = pd.DataFrame(cur.fetchall(), columns=["team_id", "team_name"])

    return {
        "sb_players": sb_players, "ws_players": ws_players, "idsse_players": idsse_players,
        "sb_teams": sb_teams, "ws_teams": ws_teams, "idsse_teams": idsse_teams,
    }


def match_players(rosters: dict[str, pd.DataFrame]) -> list[dict]:
    """SB↔WS + SB↔IDSSE + WS↔IDSSE player pairs at confidence ≥ 70."""
    rows: list[dict] = []

    # SB ↔ WS
    for _, sb in rosters["sb_players"].iterrows():
        best_score = 0
        best_ws = None
        for _, ws in rosters["ws_players"].iterrows():
            s = fuzzy_match_score(sb["player_name"], ws["player_name"])
            if s > best_score:
                best_score = s
                best_ws = ws
        if best_ws is not None and best_score >= CONFIDENCE_THRESHOLD:
            rows.append(emit_pair_ordered(
                "statsbomb", str(sb["player_id"]),
                "wyscout", str(best_ws["player_id"]),
                best_score, 1,
            ))

    # SB ↔ IDSSE
    for _, sb in rosters["sb_players"].iterrows():
        best_score = 0
        best_idsse = None
        for _, ids in rosters["idsse_players"].iterrows():
            s = fuzzy_match_score(sb["player_name"], ids["player_display_name"])
            if s > best_score:
                best_score = s
                best_idsse = ids
        if best_idsse is not None and best_score >= CONFIDENCE_THRESHOLD:
            rows.append(emit_pair_ordered(
                "statsbomb", str(sb["player_id"]),
                "idsse", str(best_idsse["player_id"]),
                best_score, 1,
            ))

    # WS ↔ IDSSE
    for _, ws in rosters["ws_players"].iterrows():
        best_score = 0
        best_idsse = None
        for _, ids in rosters["idsse_players"].iterrows():
            s = fuzzy_match_score(ws["player_name"], ids["player_display_name"])
            if s > best_score:
                best_score = s
                best_idsse = ids
        if best_idsse is not None and best_score >= CONFIDENCE_THRESHOLD:
            rows.append(emit_pair_ordered(
                "idsse", str(best_idsse["player_id"]),
                "wyscout", str(ws["player_id"]),
                best_score, 1,
            ))

    return rows


def match_teams(rosters: dict[str, pd.DataFrame]) -> list[dict]:
    """Same pattern for teams across all three provider pairs."""
    rows: list[dict] = []

    def _pairwise(a_df, a_prov, a_id_col, a_name_col, b_df, b_prov, b_id_col, b_name_col):
        for _, a in a_df.iterrows():
            best_score = 0
            best_b = None
            for _, b in b_df.iterrows():
                s = fuzzy_match_score(a[a_name_col], b[b_name_col])
                if s > best_score:
                    best_score = s
                    best_b = b
            if best_b is not None and best_score >= CONFIDENCE_THRESHOLD:
                rows.append(emit_pair_ordered(
                    a_prov, str(a[a_id_col]),
                    b_prov, str(best_b[b_id_col]),
                    best_score, 1,
                ))

    _pairwise(rosters["sb_teams"], "statsbomb", "team_id", "team_name",
              rosters["ws_teams"], "wyscout", "team_id", "team_name")
    _pairwise(rosters["sb_teams"], "statsbomb", "team_id", "team_name",
              rosters["idsse_teams"], "idsse", "team_id", "team_name")
    _pairwise(rosters["ws_teams"], "wyscout", "team_id", "team_name",
              rosters["idsse_teams"], "idsse", "team_id", "team_name")

    return rows


def merge_xref(conn, table: str, rows: list[dict], key_cols: list[str]) -> None:
    """Delta MERGE on (source_a, id_a, source_b, id_b)."""
    if not rows:
        logging.info("No rows to merge into %s", table)
        return

    df = pd.DataFrame(rows)

    # Use INSERT ... ON CONFLICT is not supported in Delta; use MERGE via temp view.
    cur = conn.cursor()
    temp_view = f"_pr5a_xref_staging_{table.split('.')[-1]}"

    # Write a temp table/view. Using a temporary Delta table for simplicity.
    # (For production hardening consider a MERGE on a Parquet-via-spark.read path.)
    insert_sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW {temp_view} AS
        SELECT * FROM VALUES
        {', '.join(str(tuple(r[c] for c in df.columns)) for _, r in df.iterrows())}
        AS t({', '.join(df.columns)})
    """
    # SQL injection consideration: names/IDs could contain single quotes. Use parameter
    # binding via pandas_to_sql or parameterized MERGE. For brevity above, a simple
    # implementation; productionize if this script sees external input.
    cur.execute(insert_sql)

    merge_sql = f"""
        MERGE INTO {table} target
        USING {temp_view} source
           ON {' AND '.join(f'target.{k} = source.{k}' for k in key_cols)}
         WHEN MATCHED THEN UPDATE SET
              confidence = source.confidence,
              match_layer = source.match_layer,
              resolution_type = source.resolution_type,
              _ingested_at = current_timestamp()
         WHEN NOT MATCHED THEN INSERT ({', '.join(df.columns)}, _ingested_at)
              VALUES ({', '.join(f'source.{c}' for c in df.columns)}, current_timestamp())
    """
    cur.execute(merge_sql)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    conn = _connect()
    try:
        rosters = fetch_rosters(conn)
        logging.info("Rosters: %s", {k: len(v) for k, v in rosters.items()})

        player_rows = match_players(rosters)
        team_rows = match_teams(rosters)

        logging.info("player xref pairs at conf≥70: %d", len(player_rows))
        logging.info("team xref pairs at conf≥70: %d", len(team_rows))

        if args.dry_run:
            for r in (player_rows[:5] + team_rows[:5]):
                print(r)
            return 0

        merge_xref(
            conn,
            f"{DBX_CATALOG}.bronze.player_xref_raw",
            player_rows,
            ["source_a", "player_id_a", "source_b", "player_id_b"],
        )
        merge_xref(
            conn,
            f"{DBX_CATALOG}.bronze.team_xref_raw",
            team_rows,
            ["source_a", "team_id_a", "source_b", "team_id_b"],
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run unit tests**

```bash
uv run pytest src/tests/test_generate_entity_xref.py -v
```

Expected: all PASS.

- [ ] **Step 5: Dry-run against dev warehouse**

```bash
uv run --with databricks-sql-connector --with rapidfuzz --with pandas python scripts/generate_entity_xref.py --dry-run 2>&1 | tee /tmp/xref_dryrun.log &
```

Run in background; poll `/tmp/xref_dryrun.log` every 30s. Review sample pairs for plausibility.

### Task 3.5: Execute `generate_entity_xref.py` against dev (populate xref)

**Files:** None (execution step).

⚠️ **USER APPROVAL REQUIRED** — writes to bronze.

- [ ] **Step 1: Request user approval**

Present: "Request approval to run `generate_entity_xref.py` against dev warehouse. Writes to `bronze.player_xref_raw` (MERGE — may add/update rows) and `bronze.team_xref_raw` (new table, INSERT). Dry-run sample reviewed in Task 3.4 Step 5."

- [ ] **Step 2: Execute**

```bash
uv run --with databricks-sql-connector --with rapidfuzz --with pandas python scripts/generate_entity_xref.py 2>&1 | tee /tmp/xref_generate.log &
```

Background; poll.

- [ ] **Step 3: Sanity-check populated counts**

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
cur.execute("""
    SELECT source_a, source_b, count(*) AS n
      FROM soccer_analytics.bronze.player_xref_raw
     GROUP BY source_a, source_b
     ORDER BY source_a, source_b
""")
print("Player xref per provider-pair:")
for r in cur.fetchall():
    print(r)
cur.execute("""
    SELECT source_a, source_b, count(*) AS n
      FROM soccer_analytics.bronze.team_xref_raw
     GROUP BY source_a, source_b
""")
print("Team xref per provider-pair:")
for r in cur.fetchall():
    print(r)
conn.close()
PYEOF
```

Expected: `(statsbomb, wyscout)` ≈ 2,780 (legacy preserved); `(statsbomb, idsse)` + `(wyscout, idsse)` > 0 if overlap exists (D3 resolution).

- [ ] **Step 4: Run invariants tests**

```bash
uv run pytest src/tests/test_int_player_xref_invariants.py src/tests/test_int_team_xref_invariants.py -v
```

Expected: all PASS.

- [ ] **Step 5: Manual spot-check**

Sample 20 random xref pairs and review:

```bash
uv run --with databricks-sql-connector python <<'PYEOF'
import os
from databricks import sql
conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://","").rstrip("/"),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()
cur.execute("""
    WITH xref AS (
        SELECT * FROM soccer_analytics.dev_silver.int_player_xref
    ),
    sb AS (SELECT player_id::string AS id, player_name FROM soccer_analytics.dev_gold.stg_statsbomb__lineups GROUP BY 1,2),
    ws AS (SELECT player_id::string AS id, player_name FROM soccer_analytics.dev_gold.stg_wyscout__players GROUP BY 1,2)
    SELECT x.source_a, x.player_id_a, a.player_name AS name_a,
           x.source_b, x.player_id_b, b.player_name AS name_b,
           x.confidence
      FROM xref x
      LEFT JOIN sb a ON x.source_a = 'statsbomb' AND x.player_id_a = a.id
      LEFT JOIN ws b ON x.source_b = 'wyscout' AND x.player_id_b = b.id
      ORDER BY rand()
     LIMIT 20
""")
for r in cur.fetchall():
    print(r)
conn.close()
PYEOF
```

Present sample to user for false-positive eyeball review. Any obvious mismatches → add `force_reject` overrides to `player_xref_overrides.csv` + re-run generator.

---

## Phase 4: Dim layer

### Task 4.1: Metrica pseudo-competition in `dim_competitions`

**Files:**
- Modify: `dbt_project/models/marts/dim_competitions.sql`

- [ ] **Step 1: Write failing test**

Create `src/tests/test_dim_competitions_has_metrica.py`:

```python
from pathlib import Path


def test_dim_competitions_adds_metrica_cte() -> None:
    src = Path("dbt_project/models/marts/dim_competitions.sql").read_text()
    assert "metrica_competitions" in src
    assert "'metrica-sample'" in src
    assert "Metrica Sample Dataset" in src
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_dim_competitions_has_metrica.py -v
```

- [ ] **Step 3: Add Metrica CTE**

Edit `dbt_project/models/marts/dim_competitions.sql`. Add a new CTE before `all_competitions`:

```sql
metrica_competitions as (

    -- PR 5a pseudo-competition: Metrica sample data has no competition
    -- metadata. Synthesise 'metrica-sample' so fct_passes' competition_key
    -- resolves non-NULL for Metrica rows and Pass Map's competition filter
    -- cascade shows "Metrica Sample Dataset".
    -- Ref: TODO #32, docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §2.
    select distinct
        'metrica'                              as provider,
        'metrica-sample'                       as native_competition_id,
        cast(null as int)                      as competition_id_legacy,
        'Metrica Sample Dataset'               as competition_name

    from {{ ref('stg_metrica__matches') }}
    where native_match_id is not null

),
```

Add `union all / select * from metrica_competitions` in the `all_competitions` CTE.

Also extend `final`'s `country` + `gender` COALESCE to fall through to NULL for Metrica (same as IDSSE handling for `country='Germany'` — but Metrica has no corresponding real country; leaves NULL).

- [ ] **Step 4: Run to confirm PASS**

```bash
uv run pytest src/tests/test_dim_competitions_has_metrica.py -v
```

### Task 4.2: Rewrite `dim_teams.sql` — 4-provider union

**Files:**
- Modify: `dbt_project/models/marts/dim_teams.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Write failing shape tests**

Create `src/tests/test_dim_teams_kimball_shape.py`:

```python
from pathlib import Path


MODEL = Path("dbt_project/models/marts/dim_teams.sql")


def test_dim_teams_uses_generate_team_key_macro() -> None:
    assert "generate_team_key" in MODEL.read_text()


def test_dim_teams_has_provider_column() -> None:
    src = MODEL.read_text()
    assert "as provider" in src or " provider," in src


def test_dim_teams_has_native_team_id() -> None:
    assert "native_team_id" in MODEL.read_text()


def test_dim_teams_has_is_synthesized_and_synthesis_reason() -> None:
    src = MODEL.read_text()
    assert "is_synthesized" in src
    assert "synthesis_reason" in src


def test_dim_teams_has_all_four_provider_ctes() -> None:
    src = MODEL.read_text()
    # Each provider CTE should appear either as a with-clause name or in a literal string.
    for provider in ("statsbomb", "wyscout", "idsse", "metrica"):
        assert f"'{provider}'" in src, f"Provider literal missing: {provider}"


def test_dim_teams_preserves_legacy_team_id_col() -> None:
    """Legacy team_id INT column kept for the dual-column window."""
    src = MODEL.read_text()
    assert "team_id" in src  # Must still appear in final SELECT


def test_dim_teams_has_canonical_team_key() -> None:
    assert "canonical_team_key" in MODEL.read_text()


def test_dim_teams_joins_int_team_xref() -> None:
    assert "int_team_xref" in MODEL.read_text()


def test_dim_teams_joins_stg_wyscout_teams_for_team_name() -> None:
    assert "stg_wyscout__teams" in MODEL.read_text()
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_dim_teams_kimball_shape.py -v
```

Expected: many fail.

- [ ] **Step 3: Rewrite `dim_teams.sql`**

Replace entire file contents:

```sql
-- dim_teams.sql
-- Conformed team dimension unifying StatsBomb, Wyscout, IDSSE, and Metrica.
--
-- PRIMARY KEY: team_key (BIGINT surrogate, xxhash64 of provider|native_team_id).
-- UNIQUE: (provider, native_team_id).
--
-- Kimball conformed dimension per ADR-011 extension in PR 5a. See
-- docs/superpowers/specs/2026-04-24-kimball-pr5-design.md §3.1.
--
-- Grain: one row per (provider, native_team_id).
--
-- Synthesis rules:
--   - StatsBomb + Wyscout real teams: is_synthesized=false, team_id legacy populated.
--   - IDSSE: real DFL TeamId from stg_idsse__home_away_teams.
--   - Wyscout fallback rows (parse-failure on teams_data_parsed):
--       is_synthesized=true, synthesis_reason='wyscout_unresolved_teamsdata'.
--   - Metrica anonymised: is_synthesized=true, synthesis_reason='metrica_anonymized'.
--   - Metrica subscription (future is_anonymized=false): real-identity path
--     ready but inactive until subscription ingestion lands.
--
-- canonical_team_key: xref-resolved (SB > WS > IDSSE preference) or self.
--
-- Note: dim_teams now sources team_name for Wyscout real rows via
-- stg_wyscout__teams (PR 5a closes the pre-existing gap where teams.json
-- wasn't ingested from Figshare).

with statsbomb_teams as (

    select
        'statsbomb'                                     as provider,
        cast(team_id as string)                         as native_team_id,
        team_id                                         as team_id_legacy,
        team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_statsbomb__events') }}
    where team_id is not null
    group by team_id, team_name

),

wyscout_real_teams as (

    select distinct
        'wyscout'                                       as provider,
        cast(e.team_id as string)                       as native_team_id,
        e.team_id                                       as team_id_legacy,
        wt.team_name                                    as team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_wyscout__events') }} e
    left join {{ ref('stg_wyscout__teams') }} wt
        on e.team_id = wt.team_id
    where e.team_id is not null

),

wyscout_synth_teams as (

    -- Rows produced by stg_wyscout__home_away_teams synthesis-fallback branch
    -- (teams_data_parsed NULL/empty on the source match).
    select distinct
        'wyscout'                                       as provider,
        hat.native_team_id                              as native_team_id,
        cast(null as int)                               as team_id_legacy,
        cast(null as string)                            as team_name,
        true                                            as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        hat.synthesis_reason                            as synthesis_reason
    from {{ ref('stg_wyscout__home_away_teams') }} hat
    where hat.is_synthesized = true

),

idsse_teams as (

    select
        'idsse'                                         as provider,
        hat.team_id                                     as native_team_id,
        cast(null as int)                               as team_id_legacy,
        max(pm.team_display_name)                       as team_name,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_idsse__home_away_teams') }} hat
    left join {{ ref('stg_tracking__player_metadata') }} pm
        on  pm.provider = 'idsse'
       and pm.match_id = concat('idsse_', hat.match_id)  -- bronze match_id still carries the idsse_ prefix
       and pm.team_side = hat.side
    where hat.team_id is not null
    group by hat.team_id

),

metrica_anon_teams as (

    select distinct
        'metrica'                                       as provider,
        native_team_id,
        cast(null as int)                               as team_id_legacy,
        concat('Metrica ', match_id, ' ', initcap(side)) as team_name,
        true                                            as is_synthesized,
        true                                            as is_anonymized,
        'metrica_anonymized'                            as synthesis_reason
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = true

),

metrica_real_teams as (

    -- Future subscription path — zero rows until real ingestion lands.
    select distinct
        'metrica'                                       as provider,
        native_team_id,
        cast(null as int)                               as team_id_legacy,
        cast(null as string)                            as team_name,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = false

),

unioned as (

    select * from statsbomb_teams
    union all
    select * from wyscout_real_teams
    union all
    select * from wyscout_synth_teams
    union all
    select * from idsse_teams
    union all
    select * from metrica_anon_teams
    union all
    select * from metrica_real_teams

),

with_keys as (

    select
        {{ generate_team_key('provider', 'native_team_id') }} as team_key,
        *
    from unioned

),

xref as (

    -- canonical_team_key resolution: pick the canonical provider on a pair.
    -- Preference order: StatsBomb > Wyscout > IDSSE > Metrica.
    select
        t.team_key,
        t.provider,
        t.native_team_id,
        coalesce(
            -- If this team is side A and the canonical provider is side B
            (select {{ generate_team_key('x.source_b', 'x.team_id_b') }}
                from {{ ref('int_team_xref') }} x
                where x.source_a = t.provider
                  and x.team_id_a = t.native_team_id
                order by
                    case x.source_b when 'statsbomb' then 1 when 'wyscout' then 2 when 'idsse' then 3 when 'metrica' then 4 end,
                    x.confidence desc
                limit 1),
            -- If this team is side B and the canonical provider is side A
            (select {{ generate_team_key('x.source_a', 'x.team_id_a') }}
                from {{ ref('int_team_xref') }} x
                where x.source_b = t.provider
                  and x.team_id_b = t.native_team_id
                order by
                    case x.source_a when 'statsbomb' then 1 when 'wyscout' then 2 when 'idsse' then 3 when 'metrica' then 4 end,
                    x.confidence desc
                limit 1),
            -- Fallback: self-pointer
            t.team_key
        ) as canonical_team_key
    from with_keys t

),

final as (

    select
        wk.team_key,
        wk.provider,
        wk.native_team_id,
        wk.team_id_legacy                              as team_id,
        wk.team_name,
        xr.canonical_team_key,
        wk.is_synthesized,
        wk.is_anonymized,
        wk.synthesis_reason,
        wk.provider                                    as team_data_source
    from with_keys wk
    left join xref xr using (team_key, provider, native_team_id)

)

select * from final
```

- [ ] **Step 4: Run to confirm PASS**

```bash
uv run pytest src/tests/test_dim_teams_kimball_shape.py -v
```

- [ ] **Step 5: Update `_marts__models.yml` contract for dim_teams**

Replace the existing `dim_teams` entry in `dbt_project/models/marts/_marts__models.yml` with:

```yaml
  - name: dim_teams
    description: >
      Conformed team dimension (ADR-011 PR 5a). Kimball surrogate team_key
      across StatsBomb / Wyscout / IDSSE / Metrica. Synthesis flags mark
      Metrica anonymised + Wyscout unresolved-teamsdata fallback rows.
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    columns:
      - name: team_key
        data_type: bigint
        description: Kimball surrogate (xxhash64 of provider|native_team_id)
        data_tests:
          - unique
          - not_null
      - name: provider
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica']
      - name: native_team_id
        data_type: string
        data_tests:
          - not_null
      - name: team_id
        data_type: int
        description: >
          LEGACY native integer team_id (StatsBomb + Wyscout real rows only).
          Retained for the coordinated 2026-07-22 dual-column sunset per ADR-011.
          NULL for IDSSE/Metrica/Wyscout-synth rows. Use team_key for new joins.
      - name: team_name
        data_type: string
      - name: canonical_team_key
        data_type: bigint
        description: xref-resolved canonical pointer (self when no match)
        data_tests:
          - not_null
      - name: is_synthesized
        data_type: boolean
        data_tests:
          - not_null
      - name: is_anonymized
        data_type: boolean
      - name: synthesis_reason
        data_type: string
      - name: team_data_source
        data_type: string
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [provider, native_team_id]
```

### Task 4.3: Rewrite `dim_players.sql` — 4-provider union + entity resolution live

**Files:**
- Modify: `dbt_project/models/marts/dim_players.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Write failing shape tests**

Create `src/tests/test_dim_players_kimball_shape.py`:

```python
from pathlib import Path


MODEL = Path("dbt_project/models/marts/dim_players.sql")


def test_uses_generate_player_key_macro() -> None:
    assert "generate_player_key" in MODEL.read_text()


def test_preserves_canonical_player_id_legacy_hash() -> None:
    src = MODEL.read_text()
    # dbt_utils.generate_surrogate_key still used for canonical_player_id — Hyrum's Law compat.
    assert "canonical_player_id" in src
    assert "generate_surrogate_key" in src


def test_has_canonical_player_key_bigint() -> None:
    assert "canonical_player_key" in MODEL.read_text()


def test_has_all_four_provider_ctes() -> None:
    src = MODEL.read_text()
    for provider in ("statsbomb", "wyscout", "idsse", "metrica"):
        assert f"'{provider}'" in src


def test_has_is_synthesized_is_anonymized_synthesis_reason() -> None:
    src = MODEL.read_text()
    for col in ("is_synthesized", "is_anonymized", "synthesis_reason"):
        assert col in src


def test_joins_int_player_xref() -> None:
    assert "int_player_xref" in MODEL.read_text()


def test_metrica_documented_as_siloed_by_design() -> None:
    src = MODEL.read_text()
    assert "Metrica" in src
    assert "anonymised" in src.lower() or "anonymized" in src.lower()
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_dim_players_kimball_shape.py -v
```

- [ ] **Step 3: Rewrite `dim_players.sql`**

Replace file with the 4-provider-union shape per spec §3.2:

```sql
-- dim_players.sql
-- Conformed player dimension (ADR-011 extension, PR 5a).
--
-- Grain: one row per (provider, native_player_id).
--
-- Surrogates:
--   - player_key BIGINT  — new Kimball surrogate via generate_player_key macro.
--   - canonical_player_id STRING — legacy hash preserved for Hyrum's Law compat
--     (57 downstream files reference it; HF embedding datasets carry values).
--   - canonical_player_key BIGINT — xref-resolved canonical pointer (SB > WS > IDSSE);
--     self-pointer when no xref match.
--
-- Metrica data constraint: sample data is anonymised (player keys "Player11"-"Player25",
-- no names), so cross-provider entity resolution is UNREACHABLE. Metrica rows stay
-- siloed permanently — documented design choice, not a deferral. Future subscription
-- data (is_anonymized=false) becomes xref-eligible with real player names.

{{ config(
    materialized='table',
    meta={'contains_pii': False}
) }}

with statsbomb_players as (

    select distinct
        cast(player_id as string)                       as native_player_id,
        player_id                                       as player_id_legacy,
        player_name,
        coalesce(player_nickname, player_name)          as player_display_name,
        position_name                                   as primary_position,
        'statsbomb'                                     as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_statsbomb__lineups') }}
    where player_id is not null

),

wyscout_players_raw as (

    select distinct
        cast(player_id as string)                       as native_player_id,
        player_id                                       as player_id_legacy,
        player_name,
        coalesce(short_name, player_name)               as player_display_name,
        position_name                                   as primary_position,
        'wyscout'                                       as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        birth_date,
        nationality
    from {{ ref('stg_wyscout__players') }}

),

idsse_players as (

    select distinct
        cast(player_id as string)                       as native_player_id,
        cast(null as int)                               as player_id_legacy,
        player_display_name                             as player_name,
        player_display_name                             as player_display_name,
        cast(null as string)                            as primary_position,
        'idsse'                                         as provider,
        false                                           as is_synthesized,
        cast(null as boolean)                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_tracking__player_metadata') }}
    where provider = 'idsse'
      and player_id is not null

),

metrica_anon_players as (

    select distinct
        native_player_id,
        cast(null as int)                               as player_id_legacy,
        concat('Metrica ', match_id, ' ', initcap(side), ' ', player_key_in_map) as player_name,
        player_key_in_map                               as player_display_name,
        cast(null as string)                            as primary_position,
        'metrica'                                       as provider,
        true                                            as is_synthesized,
        true                                            as is_anonymized,
        'metrica_anonymized'                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = true
      and native_player_id is not null

),

metrica_real_players as (

    -- Forward-compat zero-row branch for subscription data.
    select distinct
        native_player_id,
        cast(null as int)                               as player_id_legacy,
        cast(null as string)                            as player_name,
        player_key_in_map                               as player_display_name,
        cast(null as string)                            as primary_position,
        'metrica'                                       as provider,
        false                                           as is_synthesized,
        false                                           as is_anonymized,
        cast(null as string)                            as synthesis_reason,
        cast(null as string)                            as birth_date,
        cast(null as string)                            as nationality
    from {{ ref('stg_metrica__team_players') }}
    where is_anonymized = false
      and native_player_id is not null

),

unioned as (

    select * from statsbomb_players
    union all
    select * from wyscout_players_raw
    union all
    select * from idsse_players
    union all
    select * from metrica_anon_players
    union all
    select * from metrica_real_players

),

with_keys as (

    select
        -- New Kimball surrogate
        {{ generate_player_key('provider', 'native_player_id') }} as player_key,

        -- Legacy hash preserved (Hyrum's Law compat for 57-file downstream cascade
        -- + HF dataset values in football2vec-* datasets).
        {{ dbt_utils.generate_surrogate_key(['player_id_legacy', "'statsbomb'"]) }} as canonical_player_id_sb_only,
        {{ dbt_utils.generate_surrogate_key(['native_player_id', 'provider']) }}     as canonical_player_id_all,

        *
    from unioned

),

-- Preserve historical canonical_player_id values: SB rows get the same hash
-- they had pre-PR-5a (dbt_utils.generate_surrogate_key(['player_id', "'statsbomb'"]));
-- WS rows get the same historical hash from when entity_resolution_enabled was
-- the gated path; IDSSE + Metrica are new so use a provider-prefixed hash.
canonical as (

    select
        wk.*,
        case
            when wk.provider = 'statsbomb' then wk.canonical_player_id_sb_only
            when wk.provider = 'wyscout'   then {{ dbt_utils.generate_surrogate_key(['wk.player_id_legacy', "'wyscout'"]) }}
            else wk.canonical_player_id_all
        end as canonical_player_id
    from with_keys wk

),

xref as (

    -- canonical_player_key resolution: preference SB > WS > IDSSE.
    -- Metrica is anonymised — never resolves via xref — so always self-points.
    select
        t.player_key,
        t.provider,
        t.native_player_id,
        coalesce(
            (select {{ generate_player_key('x.source_b', 'x.player_id_b') }}
                from {{ ref('int_player_xref') }} x
                where x.source_a = t.provider
                  and x.player_id_a = t.native_player_id
                order by
                    case x.source_b when 'statsbomb' then 1 when 'wyscout' then 2 when 'idsse' then 3 when 'metrica' then 4 end,
                    x.confidence desc
                limit 1),
            (select {{ generate_player_key('x.source_a', 'x.player_id_a') }}
                from {{ ref('int_player_xref') }} x
                where x.source_b = t.provider
                  and x.player_id_b = t.native_player_id
                order by
                    case x.source_a when 'statsbomb' then 1 when 'wyscout' then 2 when 'idsse' then 3 when 'metrica' then 4 end,
                    x.confidence desc
                limit 1),
            t.player_key
        ) as canonical_player_key,
        -- Cross-provider IDs for debugging / display
        (select x.player_id_b
            from {{ ref('int_player_xref') }} x
            where x.source_a = t.provider and x.player_id_a = t.native_player_id
              and x.source_b = 'statsbomb'
            limit 1) as xref_statsbomb_player_id_side_b,
        (select x.player_id_a
            from {{ ref('int_player_xref') }} x
            where x.source_b = t.provider and x.player_id_b = t.native_player_id
              and x.source_a = 'statsbomb'
            limit 1) as xref_statsbomb_player_id_side_a,
        (select x.confidence
            from {{ ref('int_player_xref') }} x
            where (x.source_a = t.provider and x.player_id_a = t.native_player_id)
               or (x.source_b = t.provider and x.player_id_b = t.native_player_id)
            order by x.confidence desc
            limit 1) as match_confidence
    from canonical t

),

final as (

    select
        c.player_key,
        c.canonical_player_id,
        xr.canonical_player_key,
        c.provider,
        c.native_player_id,
        c.player_id_legacy                             as player_id,
        c.player_name,
        c.player_display_name,
        c.primary_position,
        pm.position_group,
        case when c.provider = 'statsbomb' then c.player_id_legacy end as statsbomb_player_id,
        case when c.provider = 'wyscout' then c.player_id_legacy end   as wyscout_player_id,
        case when c.provider = 'idsse' then c.native_player_id end     as idsse_player_id,
        xr.match_confidence,
        cast(null as int)                              as match_layer,
        c.birth_date,
        c.nationality,
        c.is_synthesized,
        c.is_anonymized,
        c.synthesis_reason,
        c.provider                                     as data_sources
    from canonical c
    left join xref xr using (player_key, provider, native_player_id)
    left join {{ ref('position_mapping') }} pm
        on c.primary_position = pm.position_name

)

select * from final
```

- [ ] **Step 4: Run to confirm PASS**

```bash
uv run pytest src/tests/test_dim_players_kimball_shape.py -v
```

### Task 4.4: Update `_marts__models.yml` contracts for dim_players + dim_competitions

**Files:**
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Update dim_players contract**

Replace the existing `dim_players` entry in `_marts__models.yml` with the full column list per spec §3.2:

```yaml
  - name: dim_players
    description: >
      Conformed player dimension (ADR-011 PR 5a). 4-provider union with
      Kimball surrogate player_key + preserved canonical_player_id legacy hash
      for Hyrum's Law compat. canonical_player_key collapses xref-matched
      pairs. Metrica siloed by design (anonymised; no cross-provider xref possible).
    config:
      contract:
        enforced: true
      meta:
        data_sensitivity: public
        contains_pii: false
    columns:
      - name: player_key
        data_type: bigint
        data_tests:
          - unique
          - not_null
      - name: canonical_player_id
        data_type: string
        description: Legacy hash preserved for downstream compat through 2026-07-22 sunset
        data_tests:
          - not_null
      - name: canonical_player_key
        data_type: bigint
        data_tests:
          - not_null
      - name: provider
        data_type: string
        data_tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['statsbomb', 'wyscout', 'idsse', 'metrica']
      - name: native_player_id
        data_type: string
        data_tests:
          - not_null
      - name: player_id
        data_type: int
        description: Legacy native ID (StatsBomb + Wyscout only). Dual-column window 2026-07-22.
      - name: player_name
        data_type: string
      - name: player_display_name
        data_type: string
      - name: primary_position
        data_type: string
      - name: position_group
        data_type: string
      - name: statsbomb_player_id
        data_type: int
      - name: wyscout_player_id
        data_type: int
      - name: idsse_player_id
        data_type: string
      - name: match_confidence
        data_type: double
      - name: match_layer
        data_type: int
      - name: birth_date
        data_type: string
      - name: nationality
        data_type: string
      - name: is_synthesized
        data_type: boolean
        data_tests:
          - not_null
      - name: is_anonymized
        data_type: boolean
      - name: synthesis_reason
        data_type: string
      - name: data_sources
        data_type: string
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [provider, native_player_id]
```

- [ ] **Step 2: Update dim_competitions contract**

Find the existing `dim_competitions` entry. Existing contract already has the Kimball shape from PR 2. Add a line to the description noting PR 5a's Metrica pseudo-comp row:

```yaml
    description: >
      (existing description) ... PR 5a adds a Metrica pseudo-competition row
      (provider='metrica', native_competition_id='metrica-sample') so
      Metrica passes surface in Pass Map's competition filter cascade.
```

No column changes required.

- [ ] **Step 3: Local dbt parse**

```bash
cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks --with dbt-utils --with dbt-expectations dbt parse --profiles-dir ../.dbt --target dev
```

Expected: parses cleanly.

---

## Phase 5: Fact migrations

### Task 5.1: `fct_match_summary.sql` — populate Wyscout home/away team_ids

**Files:**
- Modify: `dbt_project/models/marts/fct_match_summary.sql`

- [ ] **Step 1: Write failing test**

Create `src/tests/test_fct_match_summary_wyscout_home_away.py`:

```python
from pathlib import Path


def test_joins_wyscout_home_away_bridge() -> None:
    src = Path("dbt_project/models/marts/fct_match_summary.sql").read_text()
    assert "stg_wyscout__home_away_teams" in src, \
        "fct_match_summary must consume the new Wyscout home/away bridge"
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_fct_match_summary_wyscout_home_away.py -v
```

- [ ] **Step 3: Read existing model to locate Wyscout branch**

Read `dbt_project/models/marts/fct_match_summary.sql`. Find the Wyscout CTE (or whatever CTE emits Wyscout rows with NULL home_team_id / away_team_id). Existing code has `cast(null as int) as home_team_id, cast(null as int) as away_team_id` for Wyscout rows per §Section 1 of the spec (line 232-233 reference).

- [ ] **Step 4: Add LEFT JOINs for Wyscout rows**

In the Wyscout CTE, replace the `cast(null as int) as home_team_id` / `away_team_id` pattern with LEFT JOINs to `stg_wyscout__home_away_teams`:

```sql
-- Before the final SELECT in the Wyscout branch:
left join {{ ref('stg_wyscout__home_away_teams') }} hbridge
    on hbridge.match_id = ws_match.match_id AND hbridge.side = 'home'
left join {{ ref('stg_wyscout__home_away_teams') }} abridge
    on abridge.match_id = ws_match.match_id AND abridge.side = 'away'
```

And update the SELECT:

```sql
    hbridge.team_id                                 as home_team_id,
    abridge.team_id                                 as away_team_id,
```

Note: for synth-fallback rows (is_synthesized=true), `team_id` is NULL, but `native_team_id` = `'wyscout_unresolved_<match>_<side>'`. The existing `home_team_id` column is `int` type, so synth rows here still emit NULL team_id — but that's OK because fct_funnel_stages_agg now reads opponent resolution via the new `team_key` path (Task 5.3) which uses `native_team_id` via dim_teams.

For the dual-column window, `fct_match_summary.home_team_id` stays INT-typed; Wyscout real matches populate it from the bridge; Wyscout synth-fallback matches stay NULL (existing behaviour). Funnel will use team_key (resolves through dim_teams) rather than team_id for opponent derivation.

- [ ] **Step 5: Run to confirm PASS**

```bash
uv run pytest src/tests/test_fct_match_summary_wyscout_home_away.py -v
```

### Task 5.2: `fct_player_stats.sql` — add player_key + team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_player_stats.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Write failing tests**

Create `src/tests/test_fct_player_stats_kimball_cols.py`:

```python
from pathlib import Path


def test_fct_player_stats_has_player_key_select() -> None:
    src = Path("dbt_project/models/marts/fct_player_stats.sql").read_text()
    assert "player_key" in src


def test_fct_player_stats_has_team_key_select() -> None:
    src = Path("dbt_project/models/marts/fct_player_stats.sql").read_text()
    assert "team_key" in src


def test_fct_player_stats_joins_dim_players() -> None:
    src = Path("dbt_project/models/marts/fct_player_stats.sql").read_text()
    assert "dim_players" in src
    assert "INNER JOIN" in src.upper() or "inner join" in src, \
        "Must INNER JOIN to drop the 1 NULL player_id outlier"
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_fct_player_stats_kimball_cols.py -v
```

- [ ] **Step 3: Read existing model**

Use Read tool on `dbt_project/models/marts/fct_player_stats.sql`. Identify the `final` SELECT (or equivalent). The key aggregations (`player_shots_agg`, `player_passes_agg`, `action_values`) each group by `player_id + competition_id + season_id`; `data_source` is implicit (comes from the source CTEs).

- [ ] **Step 4: Add dim joins + new columns**

Modify the final SELECT section to INNER JOIN `dim_players` + LEFT JOIN `dim_teams`:

```sql
-- Inside the final CTE (or at the end):
final as (
    select
        -- Existing surrogate + columns
        ...,

        -- PR 5a additions: Kimball keys via dim joins
        dp.player_key,
        dt.team_key,

        -- Existing metric columns unchanged
        ...
    from (existing aggregates) agg
    -- INNER JOIN drops the 1 NULL player_id outlier (match 3825894, confirmed live).
    inner join {{ ref('dim_players') }} dp
        on dp.provider = 'statsbomb'  -- fct_player_stats is StatsBomb-only pre-PR-5a
       and dp.native_player_id = cast(agg.player_id as string)
    -- LEFT JOIN team_key: may be NULL where the aggregate spans multiple teams
    -- (career aggregates etc.) — nullable column.
    left join {{ ref('dim_teams') }} dt
        on dt.provider = 'statsbomb'
       and dt.native_team_id = cast(agg.team_id as string)
)
```

Note: verify whether `team_id` is available on the aggregate source CTEs (may need to adjust the GROUP BY or drop `team_key` to NULL if not). If the current aggregate is purely per-player-per-competition-per-season without team_id, then `team_key` is NULL for all rows — still valid per the LEFT JOIN. Document inline.

- [ ] **Step 5: Update `_marts__models.yml` — add player_key + team_key + flip warn**

Find the `fct_player_stats` entry in `_marts__models.yml`. Add columns:

```yaml
      - name: player_key
        data_type: bigint
        description: Kimball surrogate FK to dim_players.player_key (ADR-011, PR 5a)
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_players')
              field: player_key
      - name: team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams.team_key. Nullable for cross-team aggregates.
        data_tests:
          - relationships:
              to: ref('dim_teams')
              field: team_key
              config:
                where: "team_key IS NOT NULL"
```

Flip the existing `player_id` `not_null` from `severity: warn` to default (error). Update description:

```yaml
      - name: player_id
        data_type: int
        description: >
          LEGACY native player identifier. Retained for the coordinated 2026-07-22
          dual-column window per ADR-011. Use player_key for new consumer code.
          Previously 1 NULL outlier (match 3825894); closed in PR 5a via INNER JOIN
          to dim_players.
        data_tests:
          - not_null  # Flipped from severity: warn in PR 5a
```

- [ ] **Step 6: Run to confirm PASS**

```bash
uv run pytest src/tests/test_fct_player_stats_kimball_cols.py -v
```

### Task 5.3: `fct_funnel_stages_agg.sql` — add match_key + team_key + opponent_team_key

**Files:**
- Modify: `dbt_project/models/marts/fct_funnel_stages_agg.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 1: Write failing tests**

Create `src/tests/test_fct_funnel_stages_agg_kimball_cols.py`:

```python
from pathlib import Path


def test_has_match_team_opponent_keys() -> None:
    src = Path("dbt_project/models/marts/fct_funnel_stages_agg.sql").read_text()
    assert "match_key" in src
    assert "team_key" in src
    assert "opponent_team_key" in src


def test_joins_dim_teams_twice() -> None:
    src = Path("dbt_project/models/marts/fct_funnel_stages_agg.sql").read_text()
    # One for team_key, one for opponent_team_key
    assert src.count("dim_teams") >= 2
```

- [ ] **Step 2: Run to confirm FAIL**

```bash
uv run pytest src/tests/test_fct_funnel_stages_agg_kimball_cols.py -v
```

- [ ] **Step 3: Read existing model**

Use Read tool on `dbt_project/models/marts/fct_funnel_stages_agg.sql`. The final CTE already emits `match_id`, `team_id`, `opponent_team_id`. The `match_key` is available from the base CTE (uses `using (match_key)` join to fct_match_summary per PR 2). Need to add team_key + opponent_team_key JOINs in the final CTE.

- [ ] **Step 4: Add dim_teams JOINs + propagate match_key**

Edit the `final` CTE. Current structure roughly:

```sql
final as (
    select
        cast(g.match_id as bigint)             as match_id,
        cast(g.competition_id as int)          as competition_id,
        cast(g.team_id as int)                 as team_id,
        cast(g.opponent_team_id as int)        as opponent_team_id,
        ...
    from per_gs g
    inner join per_match m using (match_id, team_id)
)
```

Change to:

```sql
final as (
    select
        g.match_key,                                    -- PR 5a: surrogate from base.match_key
        cast(g.match_id as bigint)             as match_id,
        cast(g.competition_id as int)          as competition_id,
        cast(g.team_id as int)                 as team_id,
        cast(g.opponent_team_id as int)        as opponent_team_id,
        dt_own.team_key                        as team_key,
        dt_opp.team_key                        as opponent_team_key,
        cast(g.game_state as string)           as game_state,
        ...
        current_timestamp()                    as _loaded_at
    from per_gs g
    inner join per_match m using (match_id, team_id)
    -- Kimball surrogate JOINs: StatsBomb + Wyscout providers only (funnel data is SB+WS).
    left join {{ ref('dim_teams') }} dt_own
        on dt_own.provider in ('statsbomb', 'wyscout')
       and dt_own.native_team_id = cast(g.team_id as string)
    left join {{ ref('dim_teams') }} dt_opp
        on dt_opp.provider in ('statsbomb', 'wyscout')
       and dt_opp.native_team_id = cast(g.opponent_team_id as string)
)
```

**Crucial:** to surface `match_key` in `final`, it must be selected through every CTE from `base`. Update `base`, `own_possession`, `per_gs` to carry `match_key` forward if they currently drop it. If the `base` CTE's `using (match_key)` JOIN doesn't actually surface match_key in the output, the workaround is to add `ms.match_key` (but `ms` is already joined via `using`, so `match_key` is accessible). Verify by reading the file.

- [ ] **Step 5: Update `_marts__models.yml` — add keys + flip warn + restore relationship**

Find the `fct_funnel_stages_agg` entry. Add:

```yaml
      - name: match_key
        data_type: bigint
        description: Kimball surrogate FK to dim_matches.match_key (ADR-011)
        data_tests:
          - not_null
          - relationships:
              to: ref('fct_match_summary')
              field: match_key
      - name: team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams.team_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_teams')
              field: team_key
      - name: opponent_team_key
        data_type: bigint
        description: Kimball surrogate FK to dim_teams.team_key
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_teams')
              field: team_key
```

Flip the existing `opponent_team_id` `not_null` from `severity: warn` to default (error). Update the description to reflect PR 5a closure:

```yaml
      - name: opponent_team_id
        data_type: int
        description: >
          LEGACY native team_id of the opposing team. Previously NULL for
          Wyscout rows (~7,587 of ~21,000 rows) because fct_match_summary
          didn't parse teams_data_parsed. PR 5a closes that gap via
          stg_wyscout__home_away_teams bridge. Dual-column window 2026-07-22.
        data_tests:
          - not_null  # Flipped from severity: warn in PR 5a
```

- [ ] **Step 6: Run to confirm PASS**

```bash
uv run pytest src/tests/test_fct_funnel_stages_agg_kimball_cols.py -v
```

### Task 5.4: Full dbt build locally

**Files:** None (execution step).

- [ ] **Step 1: Build the touched models in order**

```bash
cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks --with dbt-utils --with dbt-expectations dbt build --profiles-dir ../.dbt --target dev --select state:modified+ 2>&1 | tee /tmp/pr5a_dbt_build.log &
```

Run in background. Poll /tmp/pr5a_dbt_build.log.

- [ ] **Step 2: Triage failures per `reference_live_ci_surfaces_latent_bugs` playbook**

Expected scope of latent surfacing: dim_teams changes may cascade to marts that JOIN dim_teams and relied on the old grain (no `provider` column). Fix compile errors in-PR; data-test failures outside PR 5a scope → warn-severity with YAML pointer to the closing PR (typically PR 6 or 7).

- [ ] **Step 3: Full dbt test on touched models**

```bash
cd dbt_project && uvx --from "dbt-core>=1.10.0,<1.12.0" --with dbt-databricks --with dbt-utils --with dbt-expectations dbt test --profiles-dir ../.dbt --target dev --select state:modified+ 2>&1 | tee /tmp/pr5a_dbt_test.log &
```

Expected: both warn-flipped tests (`not_null_fct_player_stats_player_id`, `not_null_fct_funnel_stages_agg_opponent_team_id`) PASS at error severity.

---

## Phase 6: Deploy + E2E

### Task 6.1: Refresh synced tables

**Files:** None (execution step).

⚠️ **USER APPROVAL REQUIRED** — touches Lakebase production.

- [ ] **Step 1: Ask for approval**

Present: "Request approval to run `refresh_synced_tables.py` on 6 tables: dim_teams_synced, dim_players_synced, dim_competitions_synced, fct_player_stats_synced, fct_funnel_stages_agg_synced, fct_match_summary_synced. Additive-column auto-evolution; no manual recreation needed per reference_lakebase_synced_table_auto_evolution."

- [ ] **Step 2: Execute**

```bash
uv run python -m ingestion.refresh_synced_tables \
  --tables dim_teams_synced dim_players_synced dim_competitions_synced \
           fct_player_stats_synced fct_funnel_stages_agg_synced fct_match_summary_synced \
  --wait 2>&1 | tee /tmp/pr5a_refresh.log &
```

Run in background.

- [ ] **Step 3: Verify new columns visible via psql**

```bash
uv run --with 'psycopg2-binary' python <<'PYEOF'
import os, psycopg2
conn = psycopg2.connect(
    host=os.environ["LAKEBASE_HOST"],
    port=5432,
    user=os.environ["LAKEBASE_USER"],
    password=os.environ["LAKEBASE_PASSWORD"],
    dbname=os.environ.get("LAKEBASE_DB", "postgres"),
    sslmode="require",
)
cur = conn.cursor()
for table in ["dim_teams_synced", "dim_players_synced", "dim_competitions_synced",
              "fct_player_stats_synced", "fct_funnel_stages_agg_synced"]:
    cur.execute(f"""
        SELECT column_name FROM information_schema.columns
         WHERE table_name = %s ORDER BY ordinal_position
    """, (table,))
    cols = [r[0] for r in cur.fetchall()]
    print(f"{table}: {cols}")
conn.close()
PYEOF
```

Expected: `team_key`, `player_key`, `provider`, `native_team_id`/`native_player_id`, `is_synthesized`, etc. all visible.

### Task 6.2: Re-apply Lakebase grants

**Files:** None.

- [ ] **Step 1: Run maintain_synced_tables.py Step 0.5**

```bash
uv run python scripts/maintain_synced_tables.py --skip-refresh 2>&1 | tee /tmp/pr5a_grants.log &
```

Expected: grants applied cleanly. Failures here are typically TF-lock races or permission regressions — triage per ADR-005.

### Task 6.3: Taipy E2E verification

**Files:** None.

- [ ] **Step 1: Start local Taipy**

```bash
cd hf_taipy_app && python src/main.py &
```

Wait ~10s for startup.

- [ ] **Step 2: Puppeteer-verify Conversion Funnel for a Wyscout match**

Using the Puppeteer pattern from `reference_puppeteer_taipy_dropdowns`:

- Navigate to `http://localhost:7860/conversion_funnel` (or whatever the page path is).
- Select a Wyscout competition (e.g. "Serie A").
- Select a team.
- Verify the Opponent column in the rendered table is NON-NULL for Wyscout rows (previously NULL for 36% of rows).

- [ ] **Step 3: Puppeteer-verify Pass Map Metrica competition**

- Navigate to `/pass_map`.
- Expand the competition dropdown.
- Assert "Metrica Sample Dataset" appears as a selectable option.
- Select it. Confirm passes render.

- [ ] **Step 4: Puppeteer-verify Player Similarity xref-collapsed entity**

- Navigate to the Player Similarity page.
- Search for a well-known cross-provider player (e.g., a Premier League player present in both SB + WS data).
- Confirm the player appears once (not twice — xref collapse is working).

### Task 6.4: Commit ingestion script contract note + dim header docs

**Files:**
- Modify: `src/ingestion/metrica.py` (docstring — done in Task 1.3)
- Modify: `dbt_project/models/marts/dim_teams.sql` (header — done in Task 4.2)
- Modify: `dbt_project/models/marts/dim_players.sql` (header — done in Task 4.3)

- [ ] **Step 1: Spot-check the docstrings / headers are in place**

```bash
grep -n "is_anonymized" src/ingestion/metrica.py
head -50 dbt_project/models/marts/dim_teams.sql
head -50 dbt_project/models/marts/dim_players.sql
```

Expected: forward-compat contract documented in all three files.

---

## Phase 7: Single commit + PR readiness

### Task 7.1: Final local quality gates

**Files:** None.

- [ ] **Step 1: Ruff check**

```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

- [ ] **Step 2: Ruff format check**

```bash
uv run ruff format --check src/ scripts/
```

- [ ] **Step 3: Pyright**

```bash
uv run pyright src/ 2>&1 | tail -30 &
```

(Background — pyright can be slow. Poll until complete.)

Expected: zero errors.

- [ ] **Step 4: Full pytest**

```bash
uv run pytest src/tests/ -v 2>&1 | tee /tmp/pr5a_pytest.log &
```

Expected: all green.

### Task 7.2: Stage changes + inspect diff

**Files:** None.

- [ ] **Step 1: Review all changed files**

```bash
git status --short
```

Expected: the complete set from the File structure map. Any file NOT in the map → investigate.

- [ ] **Step 2: Diff summary**

```bash
git diff --stat
```

Expected: file count + line count roughly matches the scope of PR 5a.

- [ ] **Step 3: Request commit approval**

Present to user:

> "PR 5a implementation complete. All gates green (ruff / pyright / pytest / dbt build / live-CI / Taipy E2E). Ready to stage + commit as a single squash-friendly commit. Request approval to proceed with the commit."

⚠️ **USER APPROVAL REQUIRED** (per CLAUDE.md commit rule).

### Task 7.3: Single commit

**Files:** None.

- [ ] **Step 1: Stage all changes**

Only after explicit approval:

```bash
git add dbt_project/ src/ scripts/ workflow-cards/ docs/
```

Avoid `git add -A` per CLAUDE.md — stage explicit directories only.

- [ ] **Step 2: Create the commit**

```bash
git commit -m "$(cat <<'EOF'
feat(kimball): PR 5a foundation — dim_teams + dim_players Kimball conform, entity resolution activation, Metrica pseudo-comp, Wyscout teams.json ingestion

Extends ADR-011 Kimball migration to teams + players across all four
providers (StatsBomb, Wyscout, IDSSE, Metrica). Activates cross-provider
entity resolution with new generate_entity_xref.py. Closes two warn-
suppressions from PR 4b (fct_player_stats.player_id, fct_funnel_stages_agg.
opponent_team_id). Adds Metrica pseudo-competition to dim_competitions
(TODO #32). Closes pre-existing Wyscout teams.json ingestion gap —
dim_teams.team_name now populated for Wyscout real rows.

Spec: docs/superpowers/specs/2026-04-24-kimball-pr5-design.md
Plan: docs/superpowers/plans/2026-04-24-kimball-pr5a-foundation.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Verify commit**

```bash
git log --oneline -1 && git status
```

Expected: one commit; clean working tree.

### Task 7.4: Push + open PR

**Files:** None.

⚠️ **USER APPROVAL REQUIRED** — push + PR creation each separate approval.

- [ ] **Step 1: Request push approval**

Present: "Request approval to `git push -u origin kimball-pr5a-foundation`."

- [ ] **Step 2: Push**

```bash
git push -u origin kimball-pr5a-foundation
```

- [ ] **Step 3: Request PR-create approval**

Present: "Request approval to `gh pr create` with title + body summarising PR 5a scope."

- [ ] **Step 4: Create PR**

```bash
gh pr create --title "feat(kimball): PR 5a foundation — dim_teams + dim_players + entity resolution + Wyscout teams.json" --body "$(cat <<'EOF'
## Summary

PR 5a of the ADR-011 Kimball migration. Foundation for scope-C team+player surrogate keys across all four providers.

- Kimball-conformed `dim_teams` + `dim_players` with `(provider, native_id)` BIGINT surrogates via new `generate_team_key` + `generate_player_key` macros.
- Activated cross-provider entity resolution (SB↔WS↔IDSSE). `scripts/generate_entity_xref.py` emits pairs via rapidfuzz name matching; `int_player_xref` + new `int_team_xref` (both view materialisation) consume provider-labelled bronze.
- Closed pre-existing Wyscout teams.json ingestion gap. `dim_teams.team_name` now populated for Wyscout real rows (was NULL since day one).
- Metrica pseudo-competition row in `dim_competitions` (TODO #32) → Metrica passes surface in Pass Map cascade.
- Forward-compat `is_anonymized` flag on Metrica bronze distinguishes sample data (synthesised identity) from future subscription data (real identity, xref-eligible).
- Synthesis attributes (`is_synthesized`, `synthesis_reason`) on dim rows so Taipy can surface "synthetic" tags per UX no-silent-substitute rule.
- Migrated `fct_player_stats` + `fct_funnel_stages_agg` + `fct_match_summary` (Wyscout home/away population via new `stg_wyscout__home_away_teams` bridge).
- Flipped both `severity: warn` suppressions from PR 4b back to error severity.

## Test plan

- [x] Ruff check + format clean
- [x] Pyright basic clean
- [x] Local pytest green (macros, xref invariants, bronze live schema, staging coverage, mart contracts)
- [x] dbt build via live-CI green (`state:modified+` selector)
- [x] Lakebase synced tables refreshed with new columns; grants re-applied
- [x] Taipy E2E: Conversion Funnel renders Wyscout opponents, Pass Map shows Metrica competition, Player Similarity surfaces xref-collapsed entities
- [x] Manual spot-check of 20 random xref pairs — no obvious false positives

## Spec + plan

- Design: `docs/superpowers/specs/2026-04-24-kimball-pr5-design.md`
- Implementation plan: `docs/superpowers/plans/2026-04-24-kimball-pr5a-foundation.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Capture PR URL**

Return the PR URL to the user for visibility.

### Task 7.5: Post-merge memory entry

**Files:**
- Create: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\project_kimball_pr5a_shipped.md`
- Modify: `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse-d32\memory\MEMORY.md`

- [ ] **Step 1: Write the session-2-kickoff memory entry**

⚠️ Do NOT write this file until PR 5a is MERGED (not just opened). Record the post-merge live state.

Content template (fill in live values at merge time):

```markdown
---
name: Kimball PR 5a shipped — dim_teams + dim_players + xref + Wyscout teams.json
description: PR 5a (foundation) merged YYYY-MM-DD. Session 2 kickoff for PR 5b (embedding mart migrations) should re-verify memory against live state before touching any mart.
type: project
originSessionId: <current-session-id>
---
Status at merge: dim_teams ~<N> rows (4 providers); dim_players ~<N> rows; int_player_xref populated with SB↔WS (<N>), SB↔IDSSE (<N>), WS↔IDSSE (<N>); int_team_xref similarly. fct_player_stats NULL player_id dropped to 0 via INNER JOIN. fct_funnel_stages_agg NULL opponent_team_id dropped to 0 via Wyscout teams_data_parsed bridge + synth fallback.

PR 5b scope (session 2): migrate fct_player_embeddings (×5) + fct_player_percentiles; Taipy consumer dual-reads (canonical_player_id legacy + player_key new); HF dataset card documentation updates for dual-column window.

Key files that changed in 5a for 5b awareness:
- `dbt_project/models/marts/dim_players.sql` — now has player_key BIGINT alongside preserved canonical_player_id
- `dbt_project/macros/generate_player_key.sql` — use in 5b's mart migrations
- `dbt_project/models/intermediate/int_player_xref.sql` — view; queryable for debugging
```

- [ ] **Step 2: Add MEMORY.md index entry**

Edit `MEMORY.md`:

```markdown
- [Kimball PR 5a shipped](project_kimball_pr5a_shipped.md) — dim_teams/players Kimball, xref live, Wyscout teams.json ingested; session 2 begins PR 5b embedding mart migrations
```

- [ ] **Step 3: Update `project_kimball_migration_cycle.md`**

Mark PR 5a as MERGED with the date + commit SHA. Update PR 5b (formerly "PR 5 second half") as next.

---

## Self-review checklist (executor runs before starting)

Before starting Task 1.1, verify:

1. **Spec coverage:** Every in-scope bullet of `docs/superpowers/specs/2026-04-24-kimball-pr5-design.md` §2 is covered by a Task 1.x–7.x step? Spot-check 5 random spec bullets against the task list.
2. **Placeholder scan:** Zero "TBD" / "TODO" / "fill in" in this plan (except the intentional `<N>` in the Figshare URL resolved at Task 2.1).
3. **Type consistency:** `team_key`, `player_key`, `canonical_player_key`, `canonical_player_id`, `native_team_id`, `native_player_id`, `is_synthesized`, `synthesis_reason`, `is_anonymized` — all spelled identically across every task that references them.
4. **Commit strategy:** No per-task commits. One Phase 7 commit total.
5. **Dependencies:** Task 2.3 (Wyscout teams ingestion trigger) runs BEFORE any Phase 4 dbt build depending on stg_wyscout__teams.

---

**End of plan.**
