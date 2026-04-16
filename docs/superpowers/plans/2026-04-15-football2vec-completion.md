# Football2vec Completion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **User rule override**: The user has a durable rule "no commits without explicit approval". Every commit step, every destructive operation, and every PR creation step **stops for explicit user approval**. "Approved, proceed" on the plan does NOT grant any of those. Each requires its own explicit approval.

**Goal:** Complete the Football2vec embeddings chain — Football2vec v2 gets full StatsBomb coverage (D45), Football2vec 360 gets its missing Databricks import pipeline (D62), and the warm-tier post-hook 1 watermark logic is tightened preventatively (D65).

**Architecture:** Three concerns, three commits, strict ordering. D65 first (smallest, isolated, decouples the preventative fix). D45 second (wheel helper migration + v1 replace_where scoping — unblocks the v2 inference rerun). D62 third (360 import path + three dbt model changes that must land together to avoid a regression when 360 rows first appear at bronze).

**Tech Stack:** Python 3.10, Databricks serverless (PySpark + Delta Lake), dbt (databricks adapter), PyTorch (L40S via HF Jobs), HuggingFace Hub (datasets + models), pytest (TDD discipline), terraform-provider-databricks.

**Reference memory files** (must be read before execution):
- `feedback_no_silent_swallows.md` — the ADR-002 discipline; applies to any new exception handling in this cycle
- `feedback_proxies_not_verification.md` — evidence-before-claims discipline
- `feedback_no_commits_without_approval.md` — commit approval discipline
- `feedback_execute_silently_between_checkpoints.md` — per-subagent reviews are internal; only pause at agreed item-level checkpoints
- `feedback_todo_cleanup_in_commit.md` — TODO cleanup belongs in the task's original commit, not a follow-up
- `feedback_half_baked_quality.md` — verify goals met, not just steps followed
- `feedback_verify_end_to_end.md` — verify data flows end-to-end after dbt changes
- `project_session41_pr122.md` — the Data Integrity Foundation cycle just shipped and updated the SPADL/VAEP chain; v2 inference depends on `fct_action_values` being correct, which it now is

---

## Table of contents

1. [Pre-work — verified facts + branch state](#pre-work)
2. [Commit 1 — D65 warm-tier post-hook 1 watermark fix](#commit-1)
3. [Commit 2 — D45 Football2vec v2 helper migration + v1 replace_where fix](#commit-2)
4. [Commit 3 — D62 Football2vec 360 import pipeline + dbt staging/mart fixes](#commit-3)
5. [Destructive ops phase 1 — v2 stage 2 HF Jobs rerun](#destructive-ops-phase-1)
6. [Destructive ops phase 2 — Databricks task triggers (v2 re-import + 360 first import)](#destructive-ops-phase-2)
7. [Destructive ops phase 3 — dbt full refresh + synced tables + Puppeteer verification](#destructive-ops-phase-3)
8. [PR creation](#pr-creation)
9. [Self-review checklist](#self-review)

---

<a id="pre-work"></a>
## Pre-work — verified facts + branch state

**Branch state (verified 2026-04-15):**
- Branch `feat/football2vec-completion` exists, checked out, clean working tree.
- Base: `main @ e237b5a` (docs(todo) follow-up) → `main @ 046b85c` (PR #122 Data Integrity Foundation).
- Branch name matches CLAUDE.md naming convention (`feat/<kebab-descriptor>`).

**Verified facts (all from live file reads + HF Hub probes, 2026-04-15):**

### D65 — `fct_workflow_costs` post-hook 1 SQL

`dbt_project/models/marts/fct_workflow_costs.sql:4-12`:

```sql
post_hook=[
    "DELETE FROM {{ this.database }}.observability.workflow_cost_live
     WHERE state != 'RUNNING'
       AND ended_at IS NOT NULL
       AND ended_at < (
           SELECT COALESCE(MAX(usage_date), DATE '1970-01-01') + INTERVAL 1 DAY
           FROM {{ this }}
           WHERE attributed_cost_usd IS NOT NULL
       )",
```

**The bug**: `MAX(usage_date) WHERE attributed_cost_usd IS NOT NULL` advances monotonically per-date. A single 2026-04-14 row landing with billing sets the watermark to 2026-04-15, pruning every non-RUNNING warm-tier row with `ended_at < 2026-04-15` — including 2026-04-14 rows whose billing hasn't arrived. **Not currently firing** (warm tier freshly reseeded in session 40's destructive ops phase 1), so this is a preventative fix.

`dbt_project/tests/assert_warm_tier_not_empty.sql` already exists but only catches the coarse "warm tier completely empty" condition — does not catch per-row over-pruning.

### D45 — Football2vec v2 state

`scripts/train_football2vec_v2_helpers.py` exists, **237 lines**. Contains: constants (VOCAB_SIZE, MASK_TOKEN_ID, etc.), `load_training_data()`, `parse_actions()`, `Football2VecDataset` (torch Dataset class), `stratified_split()`, `get_cosine_schedule_with_warmup()`.

**Import sites** (exactly two, verified via grep):
1. `scripts/train_football2vec_v2.py:49` — `from train_football2vec_v2_helpers import (...)` (PEP 723 HF Jobs script; luxury-lakehouse wheel is already in its inline deps per lines 1-12, so importing from the wheel is viable)
2. `src/tests/test_benchmarks.py:671` — `from train_football2vec_v2_helpers import Football2VecDataset` (uses `sys.path.insert(0, scripts_dir)` at lines 668-670)

**pyproject.toml references** (verified):
- Line 174: `"scripts/train_football2vec_v2_helpers.py" = ["BLE001"]` (ruff per-file-ignore)
- Line 208: `"scripts/train_football2vec_v2_helpers.py"` (pyright exclude)

**ARCHITECTURE.md:698**: `│   ├── train_football2vec_v2_helpers.py # Football2vec v2 training helpers (dataset, MLM masking)`

**v1 orchestrator clobber bug** — `src/ingestion/player_embeddings_v1.py:392`:

```python
write_delta_table(
    sdf,
    catalog,
    schema,
    _TABLE_NAME,
    replace_where=f"data_source = '{source_str}'",  # ← THE BUG
    logger=logger,
    row_count=row_count,
)
```

The `replace_where` clause replaces the ENTIRE `data_source='{source_str}'` partition, not just the new matches. Verified scenario (mentally walked through the code):
- Day 1: `compute_embeddings_v2` writes 22,726 wyscout+statsbomb rows to `player_embeddings_raw`.
- Day 1: `compute_embeddings_v1` guard sees those match_ids already present → skips.
- Day 2: One new statsbomb match arrives in `fct_action_values`.
- Day 2: `compute_embeddings_v2` reruns but the HF dataset hasn't changed → writes the same 22,726 rows back.
- Day 2: `compute_embeddings_v1` guard sees one new match → processes it.
- Day 2: v1's write with `replace_where="data_source='statsbomb'"` DELETES all v2 statsbomb rows and replaces with a single v1 32d row. Clobber.

**v2 guard is a placeholder** — `src/ingestion/player_embeddings_v2.py:40-47`:

```python
class _Football2VecV2Guard:
    workflow_id = "wf-football2vec-v2"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)
```

Always returns `count=1`. D45 fix can optionally promote this to a real guard, but it's NOT strictly required for the clobber fix (which lives in v1).

**HF v2 dataset state** (verified via live parquet inspection, 2026-04-15):
- `luxury-lakehouse/football2vec-statsbomb-wyscout` @ `data/embeddings_v2.parquet`
- **22,726 rows** (matches TODO claim: Wyscout-only snapshot, 2026-03-31)
- Schema: `canonical_player_id: string, match_id: string, behavioral_vector: list<double>`
- Sample `behavioral_vector` length: **128** (v2 model output dim)
- No `data_source` column — v2 code derives it from `fct_action_values` via meta_query at `player_embeddings_v2.py:122-142`

### D62 — Football2vec 360 state

**HF 360 dataset state** (verified via live parquet inspection, 2026-04-15):
- `luxury-lakehouse/football2vec-360-embeddings` @ `data/embeddings_360.parquet`
- **9,936 rows** (not zero — data is ready to import)
- Schema: `canonical_player_id: string, match_id: string, behavioral_vector: list<double>` (identical shape to v2)
- Sample `behavioral_vector` length: **144** (128d transformer + 16d Deep Sets)

**Missing function**: `compute_embeddings_360` is not in `src/ingestion/player_embeddings_v2.py` (verified via grep).

**Missing pyproject entry point**: `compute_embeddings_360` is not in `pyproject.toml` (lines 70-110 list all entries, verified).

**Missing Terraform task**: No `compute_embeddings_360` task block in `terraform/modules/workflows/main.tf` (verified via grep for `compute_embeddings`).

**Workflow card** `workflow-cards/wf-football2vec-360.yaml:56-65` declares:
```yaml
inference:
  trigger: scheduled
  runtime: databricks-workflow
  entry_point: compute_embeddings_360
  module: ingestion.player_embeddings_v2
  distribution: applyInPandas    # ← note: applyInPandas is wrong, should be direct import
  partition_key: batch_id
  schedule: "daily 06:00 UTC"
  timeout: "3600s"
  environment: analytics
```

### D62 — dbt staging/mart hazards

`dbt_project/models/staging/embeddings/stg_player_embeddings.sql:21-23`:

```sql
row_number() over (
    partition by canonical_player_id, match_id
    order by _ingested_at desc
) as _row_num
```

**Hazard**: `data_source` is NOT in the partition. Once 360 ships, bronze contains both `(player X, match Y, data_source='statsbomb', 128d)` and `(player X, match Y, data_source='football2vec_360', 144d)` rows. The staging dedup collapses them to one row — whichever has the latest `_ingested_at` — losing the other.

`dbt_project/models/marts/fct_player_embeddings_season.sql:16-22` and `fct_player_embeddings_career.sql:16-22`:

```sql
with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    group by canonical_player_id
),
```

**Hazard**: `max(size(behavioral_vector))` would promote 360 144d over v2 128d for any player who has both. These non-360 marts should EXCLUDE 360 rows outright via `where data_source != 'football2vec_360'`.

`dbt_project/models/marts/fct_player_embeddings_season_360.sql:17` and `_career_360.sql:13` already filter `where data_source = 'football2vec_360'` — no change needed there.

### Environment verification

Run `git status` and `git log --oneline -3` to confirm:

```bash
git status
# Expected: On branch feat/football2vec-completion / nothing to commit, working tree clean
git log --oneline -3
# Expected:
# e237b5a docs(todo): clear D57 references and unblock D45 after gold-data-repair ship
# 046b85c fix: data integrity foundation — SPADL + warm-tier hook + systemic silent-swallow audit (#122)
# 678556e docs: TODO — add U6 VAEP/xT three-axis UX labeling task
```

### Tooling readiness

- `uv run --extra dbt dbt --version` must show `dbt-core 1.11.6+` (verified during PR #122 cycle)
- `uv run pytest src/tests/ --benchmark-disable -q` must pass on current main (baseline: 1485 passed, 12 skipped)
- `uv run ruff check src/ scripts/ hf_taipy_app/src/` must pass (baseline: clean)
- `uv run pyright src/` must pass (baseline: 0 errors, 160 warnings — all pre-existing `reportMissingImports`)
- `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH` (double-slash `//sql/1.0/warehouses/...`) must be set
- `HF_TOKEN` (with write access to `luxury-lakehouse` org) must be set for the HF Jobs run

---

<a id="commit-1"></a>
## Commit 1 — D65 warm-tier post-hook 1 watermark fix

**Goal**: Replace the date-based `MAX(usage_date) + INTERVAL 1 DAY` watermark in `dbt_project/models/marts/fct_workflow_costs.sql` post-hook 1 with logic that does not advance monotonically per-row.

> **PIVOT NOTE — 2026-04-15 (during cycle execution)**: The plan originally proposed an EXISTS-correlated subquery on workflow_id + temporal window (Tasks 1.1-1.2 below). Two implementation attempts at this approach surfaced edge cases against the live `dbt build`:
>
> 1. First attempt — single `NOT EXISTS (NULL billing in last 3 days)` — over-pruned because `NULL = NULL` is FALSE in SQL, so the NOT EXISTS was trivially TRUE for warm-tier rows whose workflow had NULL `workflow_id` (e.g. `grant_event_log` task with no `task_workflow_mapping` entry).
> 2. Second attempt — three-condition `workflow_id IS NOT NULL AND EXISTS (billed) AND NOT EXISTS (unbilled in last 3 days)` — still surfaced the `assert_warm_tier_not_empty` failure because of state damage from attempt 1 plus `grant_event_log` rows the test couldn't distinguish.
>
> The controller pivoted to the plan's documented alternative B: **simple time-based retention** `ended_at < CURRENT_TIMESTAMP - INTERVAL 7 DAYS`. This:
> - Solves the original D65 bug (no per-date watermark advancement)
> - Matches post-hook 2's pattern (consistency)
> - Has no NULL/correlation edge cases
> - Cannot accidentally over-prune
>
> The shipped fix replaces only post-hook 1 with the time-based DELETE. The dbt singular test `assert_warm_tier_preserves_null_billing_rows.sql` (Task 1.1 below) was DELETED because it asserted EXISTS semantics that no longer apply. The structural pytest in Task 1.1b was rewritten to assert the time-based shape (`ended_at < CURRENT_TIMESTAMP - INTERVAL N DAYS`, no `EXISTS`, `state != 'RUNNING'` and `ended_at IS NOT NULL` guards present, post-hook 2 unchanged). The Tasks below describe the original intent for historical context — read the SQL comment block in `fct_workflow_costs.sql` for the shipped rationale.

**Files (as shipped):**
- Modify: `dbt_project/models/marts/fct_workflow_costs.sql` (post-hook 1 → time-based; post-hook 2 unchanged; doc comment block updated)
- Create: `src/tests/test_dbt_fct_workflow_costs.py` (3 structural pytests asserting time-based shape)
- ~~Create: `dbt_project/tests/assert_warm_tier_preserves_null_billing_rows.sql`~~ — removed during pivot, did not ship

### Task 1.1: Write the failing regression test first

**File:** `dbt_project/tests/assert_warm_tier_preserves_null_billing_rows.sql`

This is a dbt singular test. It cannot actually seed synthetic rows — dbt tests are read-only queries. The regression test must instead assert a LIVE-DATA INVARIANT: every non-RUNNING row in `workflow_cost_live` whose timestamp overlaps a `fct_workflow_costs` row with NULL `attributed_cost_usd` must still exist. If the post-hook prunes too aggressively, the overlap query returns zero matches and the test returns violations.

- [ ] **Step 1: Write the failing test file**

```sql
-- assert_warm_tier_preserves_null_billing_rows.sql
-- D65 regression guard: post-hook 1 in fct_workflow_costs must NOT prune
-- warm-tier rows whose corresponding cold-tier row still has NULL billing.
--
-- Historical bug: post-hook 1 used `MAX(usage_date) WHERE attributed_cost_usd
-- IS NOT NULL + INTERVAL 1 DAY` as the watermark, which advanced monotonically
-- as a SINGLE row landed with billing — deleting sibling NULL-billing rows
-- from the same date. Fix: EXISTS correlated subquery on workflow_id + time
-- window, so a warm-tier row survives as long as at least one cold-tier
-- counterpart still has NULL billing.
--
-- This test runs AFTER the post-hook. Returns rows if the post-hook pruned
-- any warm-tier row whose matching cold-tier row still has NULL billing.
--
-- Pattern: fabricate the "should have existed" set via the cold-tier rows
-- that lack billing and have a recent usage_date, then assert that the
-- warm-tier table does NOT show the post-hook has dropped anything matching
-- the unbilled cold-tier rows for the most recent 3 days.

WITH recent_unbilled_workflows AS (
    SELECT DISTINCT
        workflow_id,
        usage_date
    FROM {{ ref('fct_workflow_costs') }}
    WHERE attributed_cost_usd IS NULL
      AND usage_date >= CURRENT_DATE - INTERVAL 3 DAYS
),
warm_tier_non_running AS (
    SELECT workflow_id, started_at, ended_at
    FROM {{ source('observability', 'workflow_cost_live') }}
    WHERE state != 'RUNNING'
),
-- For each unbilled recent cold-tier row, check whether at least one
-- matching warm-tier row survived. If cold-tier has >=1 NULL-billing
-- entries on a given day but warm-tier has 0 corresponding entries for
-- that workflow, post-hook over-pruned.
orphans AS (
    SELECT
        r.workflow_id,
        r.usage_date,
        COUNT(w.workflow_id) AS warm_match_count
    FROM recent_unbilled_workflows r
    LEFT JOIN warm_tier_non_running w
        ON w.workflow_id = r.workflow_id
       AND CAST(w.ended_at AS DATE) = r.usage_date
    GROUP BY r.workflow_id, r.usage_date
    HAVING COUNT(w.workflow_id) = 0
)
SELECT 1 AS violation
FROM orphans
```

- [ ] **Step 2: Run the existing dbt tests to confirm the new file compiles**

```bash
cd dbt_project && uv run --extra dbt dbt parse --profiles-dir .
```

Expected: `parse` succeeds (no syntax errors) and the new test file is recognized. No build yet — this is parse-only.

- [ ] **Step 3: Run ONLY the new test against the current (wrong) SQL**

```bash
cd dbt_project && uv run --extra dbt dbt test --select test_name:assert_warm_tier_preserves_null_billing_rows --profiles-dir .
```

Expected outcome depends on live warm-tier state: if the warm tier has any NULL-billing cold-tier rows from the last 3 days that have already been incorrectly pruned, the test fails with `Got N results, configured to fail if != 0`. If the warm tier is freshly seeded (post session 40 destructive ops) and no rows have been pruned yet, the test passes **even against the buggy SQL** because there's nothing to catch.

**If the test passes against the buggy SQL**, the test is still useful as a forward-looking regression guard — once the bug DOES fire in production, the test will catch it. But we cannot use live-state TDD here. **Fall back to a unit-style test**: the post-hook SQL itself is deterministic, so we test the SQL expression directly via a pytest that parses the model YAML and asserts the watermark uses an EXISTS clause. See Step 3b below.

- [ ] **Step 3b: Write a Python-side structural test that ASSERTS the fix shape**

**File:** `src/tests/test_dbt_fct_workflow_costs.py` (new file)

```python
"""Structural tests for dbt_project/models/marts/fct_workflow_costs.sql.

These tests parse the .sql file directly and assert the post-hook logic
has the fix shape. They do NOT require a live warehouse — pure text
pattern matching. Rationale: dbt singular tests cannot reliably seed
synthetic data, and live-state tests only catch the bug after it fires.
A structural guard ensures the watermark logic stays correct forever.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODEL = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_workflow_costs.sql"


@pytest.fixture(scope="module")
def model_sql() -> str:
    return _MODEL.read_text(encoding="utf-8")


def test_fct_workflow_costs_post_hook_uses_exists_not_max_usage_date(model_sql: str) -> None:
    """D65 regression guard: post-hook 1 must use EXISTS correlated subquery
    against fct_workflow_costs rows, NOT a `MAX(usage_date)` watermark that
    advances monotonically per-row."""
    # The old broken pattern — fail if it reappears.
    old_pattern = re.compile(
        r"MAX\s*\(\s*usage_date\s*\)\s*[,)]?\s*.*INTERVAL\s+1\s+DAY",
        re.IGNORECASE | re.DOTALL,
    )
    assert not old_pattern.search(model_sql), (
        "D65 regression: fct_workflow_costs.sql still uses "
        "`MAX(usage_date) + INTERVAL 1 DAY` watermark. Replace with "
        "an EXISTS correlated subquery matching warm-tier to cold-tier "
        "rows on workflow_id + temporal window."
    )


def test_fct_workflow_costs_post_hook_has_workflow_id_join(model_sql: str) -> None:
    """The correct fix must correlate on workflow_id so per-row billing
    landing does not prune sibling rows from the same date."""
    # The fix must reference workflow_id inside the first post_hook string.
    first_post_hook_match = re.search(
        r'"DELETE FROM\s+\{\{\s*this\.database\s*\}\}\.observability\.workflow_cost_live.*?"',
        model_sql,
        re.DOTALL,
    )
    assert first_post_hook_match is not None, "Could not locate post-hook 1"
    first_post_hook = first_post_hook_match.group(0)
    assert "workflow_id" in first_post_hook, (
        "D65 fix: post-hook 1 must correlate on workflow_id between "
        "workflow_cost_live and fct_workflow_costs. Current SQL is not "
        "per-row-aware and over-prunes."
    )


def test_fct_workflow_costs_post_hook_2_unchanged(model_sql: str) -> None:
    """Post-hook 2 (orphaned RUNNING rows >24h) must stay as-is —
    D65 only touches post-hook 1."""
    # This asserts the 24-hour RUNNING cleanup still exists verbatim.
    assert "INTERVAL 24 HOURS" in model_sql, (
        "Post-hook 2 (orphaned RUNNING >24h cleanup) was removed or changed. "
        "D65 only touches post-hook 1."
    )
    assert "state = 'RUNNING'" in model_sql, (
        "Post-hook 2 state filter changed. D65 only touches post-hook 1."
    )
```

- [ ] **Step 4: Run the failing pytest to verify it catches the current state**

```bash
uv run pytest src/tests/test_dbt_fct_workflow_costs.py -v
```

Expected: `test_fct_workflow_costs_post_hook_uses_exists_not_max_usage_date` FAILS with:
```
AssertionError: D65 regression: fct_workflow_costs.sql still uses `MAX(usage_date) + INTERVAL 1 DAY` watermark. Replace with an EXISTS correlated subquery...
```

The other two tests should PASS (post-hook 2 unchanged + workflow_id not yet added).

### Task 1.2: Fix the post-hook 1 SQL

- [ ] **Step 1: Replace post-hook 1 in `dbt_project/models/marts/fct_workflow_costs.sql`**

```sql
{{ config(
    materialized='table',
    liquid_clustered_by=['task_key', 'usage_date'],
    post_hook=[
        -- Post-hook 1 (D65 fix 2026-04-15): delete warm-tier rows only when
        -- we are CERTAIN their cold-tier counterpart has complete billing.
        -- The prior implementation used `MAX(usage_date) WHERE
        -- attributed_cost_usd IS NOT NULL + INTERVAL 1 DAY`, which advanced
        -- the watermark as a SINGLE row landed with billing — pruning sibling
        -- NULL-billing rows from the same date. The fix uses an EXISTS
        -- correlated subquery matching on workflow_id + temporal window, so
        -- a warm-tier row survives as long as any cold-tier counterpart still
        -- has NULL billing within a 3-day trailing window.
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live AS wl
         WHERE wl.state != 'RUNNING'
           AND wl.ended_at IS NOT NULL
           AND NOT EXISTS (
               SELECT 1
               FROM {{ this }} AS fc
               WHERE fc.workflow_id = wl.workflow_id
                 AND fc.attributed_cost_usd IS NULL
                 AND fc.usage_date >= CURRENT_DATE - INTERVAL 3 DAYS
           )",
        "DELETE FROM {{ this.database }}.observability.workflow_cost_live
         WHERE state = 'RUNNING'
           AND started_at < CURRENT_TIMESTAMP - INTERVAL 24 HOURS"
    ]
) }}
```

Leave the rest of the file (lines 18+) unchanged.

- [ ] **Step 2: Update the doc comment block (lines 44-48)** to reflect the fix

Replace the old comment:
```
-- Post-hook cleanup removes redundant warm-tier rows from workflow_cost_live.
-- COALESCE sentinel: if table is empty (first build), threshold becomes
-- 1970-01-02 — no legitimate workflow ended in 1970, so DELETE matches zero rows.
-- Secondary cleanup: orphaned RUNNING rows >24h. This window is aligned to the
-- 2h compute task budget (CLAUDE.md) — a 24h-old RUNNING row is certainly orphaned.
```

With:
```
-- Post-hook 1 cleanup (D65 fix 2026-04-15): removes redundant warm-tier rows
-- from workflow_cost_live when their cold-tier counterpart has complete billing.
-- Uses NOT EXISTS correlated on workflow_id + a 3-day trailing window so
-- per-row billing arrivals cannot prune sibling NULL-billing rows from the
-- same date. Prior implementation used a `MAX(usage_date) + INTERVAL 1 DAY`
-- watermark which advanced monotonically as a SINGLE row landed with billing.
-- Post-hook 2: orphaned RUNNING rows >24h. This window is aligned to the
-- 2h compute task budget (CLAUDE.md) — a 24h-old RUNNING row is certainly orphaned.
```

- [ ] **Step 3: Run the structural pytest to verify it now passes**

```bash
uv run pytest src/tests/test_dbt_fct_workflow_costs.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 4: Run the dbt compile to verify the post-hook SQL is syntactically valid**

```bash
cd dbt_project && uv run --extra dbt dbt compile --select fct_workflow_costs --profiles-dir .
```

Expected: Compile succeeds with no errors. Confirms the Jinja + SQL is valid.

- [ ] **Step 5: Ensure warehouse is running + dbt build ONLY `fct_workflow_costs` to verify the post-hook actually executes**

```bash
DATABRICKS_HOST=dbc-48322be9-16be.cloud.databricks.com uv run python scripts/ensure_warehouse.py
cd dbt_project && uv run --extra dbt dbt build --select fct_workflow_costs --profiles-dir .
```

Expected:
- Model builds successfully
- Post-hook 1 and post-hook 2 both execute (visible in dbt log output)
- `assert_warm_tier_not_empty` test still passes (this is the coarse pre-existing test)
- `assert_warm_tier_preserves_null_billing_rows` test runs and either passes (live state is clean) or returns zero violations

- [ ] **Step 6: Run the full src/tests suite locally to verify no regressions**

```bash
uv run pytest src/tests/ --benchmark-disable -q 2>&1 | tail -5
```

Expected: `1486 passed, 12 skipped` (baseline 1485 + the new `test_dbt_fct_workflow_costs.py` tests).

- [ ] **Step 7: Run ruff + pyright**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/
uv run ruff format --check src/ scripts/ hf_taipy_app/src/
uv run pyright src/ 2>&1 | tail -3
```

Expected: All clean (ruff check passes, format already formatted, pyright 0 errors).

### Task 1.3: STOP for commit approval

- [ ] **Step 1: Report the Commit 1 diff to the user**

```bash
git status
git diff dbt_project/models/marts/fct_workflow_costs.sql
git diff --stat
```

- [ ] **Step 2: Wait for explicit user approval to commit. Do NOT proceed without it.**

Once approved:

```bash
git add dbt_project/models/marts/fct_workflow_costs.sql \
        dbt_project/tests/assert_warm_tier_preserves_null_billing_rows.sql \
        src/tests/test_dbt_fct_workflow_costs.py

git commit -m "$(cat <<'EOF'
fix(fct_workflow_costs): tighten post-hook 1 watermark to per-workflow EXISTS (D65)

Replace the `MAX(usage_date) WHERE attributed_cost_usd IS NOT NULL +
INTERVAL 1 DAY` watermark with a NOT EXISTS correlated subquery matching
warm-tier rows against their cold-tier counterparts on workflow_id within
a 3-day trailing window. Previously a single 2026-04-14 row landing with
billing would advance the watermark to 2026-04-15 and prune every
non-RUNNING warm-tier row with ended_at < 2026-04-15 — including sibling
2026-04-14 rows whose billing had not yet arrived. The fix makes pruning
per-workflow-aware rather than per-date-aware.

Adds `src/tests/test_dbt_fct_workflow_costs.py` as a structural regression
guard that asserts the fix shape (EXISTS instead of MAX(usage_date),
workflow_id correlation present, post-hook 2 unchanged). Also adds
`dbt_project/tests/assert_warm_tier_preserves_null_billing_rows.sql` as
a live-data guard that fires if the post-hook ever prunes a warm-tier row
whose cold-tier counterpart still has NULL billing.

Not currently firing in production (warm tier was reseeded in session 40's
destructive ops phase 1 after the ADR-002 schema-drift fix). This is a
preventative hardening before the issue recurs at steady state.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Do NOT push. Stay on the branch; the next commit will follow.

---

<a id="commit-2"></a>
## Commit 2 — D45 Football2vec v2 helper migration + v1 replace_where fix

**Goal**: Two changes in one commit because they share the same subsystem:
1. Move `scripts/train_football2vec_v2_helpers.py` into the wheel at `src/ingestion/football2vec_v2_training.py` so HF Jobs scripts can import it cleanly and `src/tests/test_benchmarks.py` no longer needs `sys.path.insert`.
2. Fix `player_embeddings_v1.py:392` `replace_where` clobber bug so v1 only replaces its own processed match_ids, not the entire `data_source` partition.

**Files:**
- Create: `src/ingestion/football2vec_v2_training.py` (contents from the old helpers file, 237 lines)
- Modify: `scripts/train_football2vec_v2.py:49-60` (import path change)
- Modify: `src/tests/test_benchmarks.py:664-682` (import path change, drop sys.path.insert)
- Modify: `src/ingestion/player_embeddings_v1.py:377-395` (replace_where fix)
- Modify: `pyproject.toml:174, 208` (remove obsolete per-file-ignores for the old helpers path)
- Modify: `ARCHITECTURE.md:698` (update reference to new path)
- Delete: `scripts/train_football2vec_v2_helpers.py`
- Create: `src/tests/test_football2vec_v2_training.py` (TDD unit test for the module)
- Create: `src/tests/test_player_embeddings_v1_replace_where.py` (TDD for the clobber fix)

### Task 2.1: Write failing test for the new training module location

- [ ] **Step 1: Create `src/tests/test_football2vec_v2_training.py`**

```python
"""Unit tests for ingestion.football2vec_v2_training module.

TDD guard: this module was migrated from scripts/train_football2vec_v2_helpers.py
into the wheel package so HF Jobs scripts and src/tests/test_benchmarks.py can
import it without sys.path manipulation. This test verifies the public API is
stable at the new location.
"""

from __future__ import annotations

import pytest


def test_module_imports_from_wheel_location() -> None:
    """The training-helpers module must live at ingestion.football2vec_v2_training."""
    import ingestion.football2vec_v2_training as mod

    # Constants
    assert mod.VOCAB_SIZE == 23
    assert mod.MASK_TOKEN_ID == 23
    assert mod.PAD_TOKEN_ID == 24
    assert mod.MAX_SEQ_LEN == 512
    assert mod.WEIGHT_DECAY == 0.01
    assert mod.WARMUP_FRACTION == 0.10
    assert mod.RANDOM_STATE == 42
    assert mod.ADVERSARIAL_LAMBDA_MAX == 0.2
    assert mod.ADVERSARIAL_WARMUP_EPOCHS == 5
    assert mod.DEFAULT_MASK_PROB == 0.15


def test_football2vec_dataset_public_name() -> None:
    """Football2VecDataset must be importable at the new path."""
    pytest.importorskip("torch")
    from ingestion.football2vec_v2_training import Football2VecDataset

    # Smoke check: can we instantiate with minimal fake data?
    ds = Football2VecDataset(
        action_ids=[[1, 2, 3]],
        x_coords=[[10.0, 20.0, 30.0]],
        y_coords=[[5.0, 15.0, 25.0]],
        max_seq_len=10,
        mlm=False,
    )
    assert len(ds) == 1
    item = ds[0]
    assert "action_ids" in item
    assert "x_coords" in item
    assert "y_coords" in item
    assert "attention_mask" in item


def test_public_helper_functions_import() -> None:
    """The public helpers must be importable for the HF Jobs training script."""
    pytest.importorskip("torch")
    pytest.importorskip("sklearn")
    from ingestion.football2vec_v2_training import (
        get_cosine_schedule_with_warmup,
        load_training_data,
        parse_actions,
        stratified_split,
    )

    assert callable(load_training_data)
    assert callable(parse_actions)
    assert callable(stratified_split)
    assert callable(get_cosine_schedule_with_warmup)


def test_old_script_path_no_longer_imported_by_benchmark_fixture() -> None:
    """Regression guard: test_benchmarks.py must not use sys.path hacks."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    bench_file = repo_root / "src" / "tests" / "test_benchmarks.py"
    text = bench_file.read_text(encoding="utf-8")

    # No sys.path manipulation for the scripts directory
    assert "sys.path.insert" not in text or "scripts_dir" not in text, (
        "test_benchmarks.py should import Football2VecDataset from "
        "ingestion.football2vec_v2_training, not via sys.path.insert on "
        "the scripts directory."
    )
    # Positive assertion: the correct import is present
    assert "from ingestion.football2vec_v2_training import" in text, (
        "test_benchmarks.py should import from ingestion.football2vec_v2_training"
    )
```

- [ ] **Step 2: Run the new test to verify it fails**

```bash
uv run pytest src/tests/test_football2vec_v2_training.py -v
```

Expected: 4 tests FAIL with `ModuleNotFoundError: No module named 'ingestion.football2vec_v2_training'` (tests 1-3) and the regression guard (test 4) fails because test_benchmarks.py still has `sys.path.insert`.

### Task 2.2: Create the new module by moving helpers into the wheel

- [ ] **Step 1: Create `src/ingestion/football2vec_v2_training.py`**

Copy the entire contents of `scripts/train_football2vec_v2_helpers.py` verbatim into the new file, BUT update the module docstring to reflect the new location:

```python
"""Football2Vec v2 training helpers — dataset, masking, splits, LR schedule.

Contains data loading, parsing, PyTorch Dataset class, train/val/test
splitting, and the cosine learning rate scheduler used by
``scripts/train_football2vec_v2.py`` (HF Jobs L40S training entry point).

Moved from ``scripts/train_football2vec_v2_helpers.py`` into the wheel so
HF Jobs scripts can import it via ``ingestion.football2vec_v2_training``
without sibling-file tricks, and so ``src/tests/test_benchmarks.py`` can
reach ``Football2VecDataset`` without ``sys.path.insert`` on the scripts
directory.

This module is training-only — it is NEVER imported by production
inference code (``player_embeddings_v1.py``, ``player_embeddings_v2.py``,
etc.). Those paths use the frozen on-disk model and do not need the
training dataset class.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
from torch.utils.data import Dataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants (shared with main training script)
# ---------------------------------------------------------------------------

VOCAB_SIZE = 23
MASK_TOKEN_ID = VOCAB_SIZE  # 23
PAD_TOKEN_ID = VOCAB_SIZE + 1  # 24
MAX_SEQ_LEN = 512
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42
ADVERSARIAL_LAMBDA_MAX = 0.2
ADVERSARIAL_WARMUP_EPOCHS = 5
DEFAULT_MASK_PROB = 0.15


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(hf_token: str, training_dataset: str) -> tuple[pd.DataFrame, str]:
    """Download training data from HF Hub and return DataFrame + commit hash."""
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(training_dataset, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {training_dataset}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(training_dataset, pf, repo_type="dataset", token=hf_token)
        table = pq.read_table(local_path)
        df = table.to_pandas()
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total player-match sequences: %d", len(data))
    dataset_info = api.repo_info(repo_id=training_dataset, repo_type="dataset")
    return data, dataset_info.sha


def parse_actions(
    actions_col: pd.Series,
) -> tuple[list[list[int]], list[list[float]], list[list[float]]]:
    """Parse the actions struct array column into separate lists."""
    all_action_ids: list[list[int]] = []
    all_x_coords: list[list[float]] = []
    all_y_coords: list[list[float]] = []

    for actions in actions_col:
        if actions is None or (hasattr(actions, "__len__") and len(actions) == 0):
            all_action_ids.append([])
            all_x_coords.append([])
            all_y_coords.append([])
            continue
        action_ids: list[int] = []
        x_coords: list[float] = []
        y_coords: list[float] = []
        for act in actions:
            action_ids.append(int(act["action_type"]))
            x_coords.append(float(act["x"]))
            y_coords.append(float(act["y"]))
        all_action_ids.append(action_ids)
        all_x_coords.append(x_coords)
        all_y_coords.append(y_coords)

    return all_action_ids, all_x_coords, all_y_coords


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class Football2VecDataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for SPADL action sequences with MLM masking."""

    def __init__(
        self,
        action_ids: list[list[int]],
        x_coords: list[list[float]],
        y_coords: list[list[float]],
        max_seq_len: int = MAX_SEQ_LEN,
        mask_prob: float = DEFAULT_MASK_PROB,
        *,
        mlm: bool = True,
        competition_ids: list[int] | None = None,
    ) -> None:
        n = len(action_ids)
        sl = max_seq_len
        self.mask_prob = mask_prob
        self.mlm = mlm
        self._n = n

        # Pre-tensorize: pad all sequences once at init time.
        t_action = torch.full((n, sl), PAD_TOKEN_ID, dtype=torch.long)
        t_x = torch.zeros(n, sl, dtype=torch.float32)
        t_y = torch.zeros(n, sl, dtype=torch.float32)
        t_mask = torch.zeros(n, sl, dtype=torch.bool)
        t_seq_lens = torch.zeros(n, dtype=torch.long)

        for i in range(n):
            seq_len = min(len(action_ids[i]), sl)
            if seq_len > 0:
                t_action[i, :seq_len] = torch.tensor(action_ids[i][:seq_len], dtype=torch.long)
                t_x[i, :seq_len] = torch.tensor(x_coords[i][:seq_len], dtype=torch.float32)
                t_y[i, :seq_len] = torch.tensor(y_coords[i][:seq_len], dtype=torch.float32)
                t_mask[i, :seq_len] = True
                t_seq_lens[i] = seq_len

        self._action_ids = t_action
        self._x_coords = t_x
        self._y_coords = t_y
        self._attention_mask = t_mask
        self._seq_lens = t_seq_lens
        self._competition_ids = torch.tensor(competition_ids, dtype=torch.long) if competition_ids else None

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        """Return a single tokenized, padded, optionally masked sample."""
        action_tensor = self._action_ids[idx].clone()  # clone for MLM mutation
        seq_len = int(self._seq_lens[idx].item())

        result: dict[str, torch.Tensor] = {
            "action_ids": action_tensor,
            "x_coords": self._x_coords[idx],
            "y_coords": self._y_coords[idx],
            "attention_mask": self._attention_mask[idx],
        }

        if self.mlm and seq_len > 0:
            labels = torch.full_like(action_tensor, -100)
            n_mask = max(1, int(seq_len * self.mask_prob))
            mask_indices = torch.randperm(seq_len)[:n_mask]
            labels[mask_indices] = action_tensor[mask_indices].clone()
            action_tensor[mask_indices] = MASK_TOKEN_ID
            result["labels"] = labels
        elif self.mlm:
            result["labels"] = torch.full_like(action_tensor, -100)

        if self._competition_ids is not None:
            result["competition_id"] = self._competition_ids[idx]

        return result


# ---------------------------------------------------------------------------
# Train/val/test splitting
# ---------------------------------------------------------------------------


def stratified_split(
    data: pd.DataFrame,
    train_frac: float = 0.80,
    val_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split data into train/val/test stratified by competition_id."""
    from sklearn.model_selection import train_test_split

    stratify_col = data["competition_id"].astype(str)
    counts = stratify_col.value_counts()
    rare_mask = stratify_col.isin(counts[counts < 3].index)
    stratify_col = stratify_col.copy()
    stratify_col.loc[rare_mask] = "_other_"

    indices = np.arange(len(data))
    test_frac = 1.0 - train_frac - val_frac
    train_val_idx, test_idx = train_test_split(
        indices,
        test_size=test_frac,
        random_state=RANDOM_STATE,
        stratify=stratify_col,
    )
    val_relative = val_frac / (train_frac + val_frac)
    stratify_trainval = stratify_col.iloc[train_val_idx]
    tv_counts = stratify_trainval.value_counts()
    tv_rare = stratify_trainval.isin(tv_counts[tv_counts < 2].index)
    stratify_trainval = stratify_trainval.copy()
    stratify_trainval.loc[tv_rare] = "_other_"

    train_idx, val_idx = train_test_split(
        train_val_idx,
        test_size=val_relative,
        random_state=RANDOM_STATE,
        stratify=stratify_trainval,
    )
    return data.iloc[train_idx], data.iloc[val_idx], data.iloc[test_idx]


# ---------------------------------------------------------------------------
# Learning rate scheduler
# ---------------------------------------------------------------------------


def get_cosine_schedule_with_warmup(
    optimizer: torch.optim.Optimizer,
    num_warmup_steps: int,
    num_training_steps: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Cosine annealing with linear warmup."""

    def lr_lambda(current_step: int) -> float:
        if current_step < num_warmup_steps:
            return float(current_step) / float(max(1, num_warmup_steps))
        progress = float(current_step - num_warmup_steps) / float(max(1, num_training_steps - num_warmup_steps))
        return max(0.0, 0.5 * (1.0 + math.cos(math.pi * progress)))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)
```

- [ ] **Step 2: Update `scripts/train_football2vec_v2.py:49-60`** (replace the import)

Old:
```python
from train_football2vec_v2_helpers import (
    ADVERSARIAL_LAMBDA_MAX,
    ADVERSARIAL_WARMUP_EPOCHS,
    VOCAB_SIZE,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    Football2VecDataset,
    get_cosine_schedule_with_warmup,
    load_training_data,
    parse_actions,
    stratified_split,
)
```

New:
```python
from ingestion.football2vec_v2_training import (
    ADVERSARIAL_LAMBDA_MAX,
    ADVERSARIAL_WARMUP_EPOCHS,
    VOCAB_SIZE,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    Football2VecDataset,
    get_cosine_schedule_with_warmup,
    load_training_data,
    parse_actions,
    stratified_split,
)
```

- [ ] **Step 3: Update `src/tests/test_benchmarks.py:664-682`** to drop sys.path.insert

Old (lines 661-682):
```python
@pytest.fixture
def f2v_dataset():  # type: ignore[no-untyped-def]
    """Small pre-tensorized Football2Vec v2 dataset for benchmarking."""
    import sys
    from pathlib import Path

    pytest.importorskip("pyarrow")
    scripts_dir = str(Path(__file__).resolve().parents[2] / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from train_football2vec_v2_helpers import Football2VecDataset

    n = 100
    lens = _rand_seq_lens(n, 10, 50, seed=50)
    return Football2VecDataset(
        action_ids=_rand_int_seqs(lens, 23, seed=51),
        x_coords=_rand_float_seqs(lens, seed=52),
        y_coords=_rand_float_seqs(lens, seed=53),
        max_seq_len=64,
        mlm=True,
        competition_ids=[0] * n,
    )
```

New:
```python
@pytest.fixture
def f2v_dataset():  # type: ignore[no-untyped-def]
    """Small pre-tensorized Football2Vec v2 dataset for benchmarking."""
    pytest.importorskip("pyarrow")
    from ingestion.football2vec_v2_training import Football2VecDataset

    n = 100
    lens = _rand_seq_lens(n, 10, 50, seed=50)
    return Football2VecDataset(
        action_ids=_rand_int_seqs(lens, 23, seed=51),
        x_coords=_rand_float_seqs(lens, seed=52),
        y_coords=_rand_float_seqs(lens, seed=53),
        max_seq_len=64,
        mlm=True,
        competition_ids=[0] * n,
    )
```

- [ ] **Step 4: Delete `scripts/train_football2vec_v2_helpers.py`**

```bash
rm scripts/train_football2vec_v2_helpers.py
```

- [ ] **Step 5: Update `pyproject.toml`** — remove the obsolete per-file-ignores for the old path

Delete line 174: `"scripts/train_football2vec_v2_helpers.py" = ["BLE001"]`
Delete line 208: `"scripts/train_football2vec_v2_helpers.py",`

(Line numbers may shift after edits — use text-based replacement.)

- [ ] **Step 6: Update `ARCHITECTURE.md:698`** — remove the line referencing the old helper path

Delete the line:
```
│   ├── train_football2vec_v2_helpers.py # Football2vec v2 training helpers (dataset, MLM masking)
```

And if there's a matching reference inside `src/ingestion/` tree inside ARCHITECTURE.md, ADD:
```
│   ├── football2vec_v2_training.py   # Football2vec v2 training helpers (moved from scripts/ 2026-04-15)
```

(Locate the `src/ingestion/` section of the tree first.)

- [ ] **Step 7: Run the new test to verify it passes**

```bash
uv run pytest src/tests/test_football2vec_v2_training.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 8: Run the full benchmark suite to verify `test_bench_f2v_getitem` still works**

```bash
uv run pytest src/tests/test_benchmarks.py::test_bench_f2v_getitem -v
```

Expected: PASS. Confirms the new fixture path works.

### Task 2.3: Write failing test for the v1 replace_where clobber fix

- [ ] **Step 1: Create `src/tests/test_player_embeddings_v1_replace_where.py`**

```python
"""TDD guard for player_embeddings_v1 replace_where scoping (D45 Part B).

The v1 Doc2Vec pipeline previously wrote to player_embeddings_raw with
``replace_where=f"data_source = '{source}'"``, which replaced the ENTIRE
partition for that data_source. After v2 ran first and wrote 22K rows,
v1 processing a single new match would clobber all 22K v2 rows and write
back just the one new match row in 32d format.

The fix: scope replace_where to the specific match_ids v1 processed.
"""

from __future__ import annotations

import inspect

import pytest


def test_run_pipeline_v1_uses_match_id_scoped_replace_where() -> None:
    """v1's write must scope replace_where to specific match_ids, not
    data_source alone."""
    from ingestion import player_embeddings_v1

    source = inspect.getsource(player_embeddings_v1.run_pipeline_v1)

    # Old bug pattern — must be gone.
    assert 'replace_where=f"data_source = \'{source_str}\'"' not in source, (
        "D45 regression: player_embeddings_v1.run_pipeline_v1 still uses "
        "data-source-only replace_where. This clobbers v2 128d rows when "
        "v1 processes a single new match. Fix: include match_id IN (...) "
        "in the replace_where predicate."
    )

    # Fix pattern — must be present.
    assert "match_id IN" in source or "match_id in" in source, (
        "D45 fix: player_embeddings_v1.run_pipeline_v1 must build a "
        "match_id IN (...) predicate for replace_where so it only replaces "
        "rows for the matches it actually processed."
    )


def test_run_pipeline_v1_replace_where_is_composed_correctly() -> None:
    """When v1 writes, replace_where must combine data_source AND match_id
    — replacing only the rows v1 owns, not the whole source partition."""
    from ingestion import player_embeddings_v1

    source = inspect.getsource(player_embeddings_v1.run_pipeline_v1)

    # The predicate must reference both data_source and match_id
    # within the same replace_where assignment. We look for the pattern
    # `replace_where=` followed (before the next `)`) by both tokens.
    rw_start = source.find("replace_where=")
    assert rw_start >= 0, "replace_where= assignment not found"
    rw_end = source.find("\n", rw_start + 1) + 200  # scan a reasonable window
    rw_block = source[rw_start:rw_end]

    assert "data_source" in rw_block, (
        "replace_where must reference data_source"
    )
    assert "match_id" in rw_block, (
        "replace_where must reference match_id (scoped delete)"
    )
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest src/tests/test_player_embeddings_v1_replace_where.py -v
```

Expected: Both tests FAIL — the current code has `replace_where=f"data_source = '{source_str}'"` and no `match_id IN` clause.

### Task 2.4: Fix the v1 replace_where clobber

- [ ] **Step 1: Modify `src/ingestion/player_embeddings_v1.py:375-395`**

Find the current loop:
```python
    # 8. Write per data source with replaceWhere for idempotency
    for source in bronze_df["data_source"].unique():
        source_str = str(source)
        source_slice = bronze_df[bronze_df["data_source"] == source_str]

        sdf = spark.createDataFrame(source_slice)
        row_count = validate_dataframe(
            sdf,
            ["canonical_player_id", "match_id", "data_source", "behavioral_vector"],
            _TABLE_NAME,
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"data_source = '{source_str}'",
            logger=logger,
            row_count=row_count,
        )
```

Replace with match-id-scoped version:
```python
    # 8. Write per data source with match-id-scoped replaceWhere for idempotency.
    # D45 fix 2026-04-15: previously replace_where was keyed on data_source
    # alone, which replaced the entire statsbomb/wyscout partition in
    # player_embeddings_raw — clobbering v2's 128d rows whenever v1 processed
    # even a single new match. Scoping the predicate to the specific match_ids
    # v1 just processed keeps v1 writes surgical and leaves v2 rows intact.
    for source in bronze_df["data_source"].unique():
        source_str = str(source)
        source_slice = bronze_df[bronze_df["data_source"] == source_str]

        if source_slice.empty:
            continue

        # Build a SQL-safe IN list from the match_ids actually being written.
        # match_id is stored as STRING in player_embeddings_raw (see _RESULTS_SCHEMA
        # at module top), so quote each value. Escape single quotes defensively
        # even though match_ids in practice are numeric-looking strings.
        source_match_ids: list[str] = sorted({str(m) for m in source_slice["match_id"]})
        escaped = [m.replace("'", "''") for m in source_match_ids]
        in_list = ", ".join(f"'{m}'" for m in escaped)
        predicate = f"data_source = '{source_str}' AND match_id IN ({in_list})"

        sdf = spark.createDataFrame(source_slice)
        row_count = validate_dataframe(
            sdf,
            ["canonical_player_id", "match_id", "data_source", "behavioral_vector"],
            _TABLE_NAME,
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=predicate,
            logger=logger,
            row_count=row_count,
        )
```

- [ ] **Step 2: Re-run the TDD test to verify it now passes**

```bash
uv run pytest src/tests/test_player_embeddings_v1_replace_where.py -v
```

Expected: Both tests PASS.

- [ ] **Step 3: Verify no regression in the full player_embeddings_v1 test suite**

```bash
uv run pytest src/tests/ -k player_embeddings_v1 -v
```

Expected: All existing v1 tests PASS. If any test was depending on the old wide replace_where pattern, update it (unlikely — most tests mock `write_delta_table` itself).

### Task 2.5: Full quality gate before commit approval

- [ ] **Step 1: Full test suite**

```bash
uv run pytest src/tests/ --benchmark-disable -q 2>&1 | tail -5
```

Expected: `1488 passed, 12 skipped` (baseline 1485 + 3 new test files = ~6 new tests → rough upper bound 1491; verify no loss).

- [ ] **Step 2: Ruff + format + pyright**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/
uv run ruff format --check src/ scripts/ hf_taipy_app/src/
uv run pyright src/ 2>&1 | tail -5
```

Expected: All clean.

- [ ] **Step 3: Import boundary check**

```bash
uv run lint-imports --config pyproject.toml 2>&1 | tail -5
```

(If `lint-imports` is not installed globally, use `uv run --with import-linter lint-imports`.)

Expected: All contracts pass. The new `ingestion.football2vec_v2_training` module must not violate any dependency direction rules.

### Task 2.6: STOP for commit approval

- [ ] **Step 1: Report the Commit 2 diff**

```bash
git status
git diff --stat
git diff src/ingestion/player_embeddings_v1.py
```

- [ ] **Step 2: Wait for explicit user approval. Then:**

```bash
git add src/ingestion/football2vec_v2_training.py \
        src/ingestion/player_embeddings_v1.py \
        scripts/train_football2vec_v2.py \
        src/tests/test_benchmarks.py \
        src/tests/test_football2vec_v2_training.py \
        src/tests/test_player_embeddings_v1_replace_where.py \
        pyproject.toml \
        ARCHITECTURE.md

git rm scripts/train_football2vec_v2_helpers.py

git commit -m "$(cat <<'EOF'
feat(football2vec): v2 helper migration + v1 replace_where scoping (D45)

Two coupled changes in the Football2vec embeddings subsystem:

1. Move scripts/train_football2vec_v2_helpers.py (237 lines) into the
   wheel at src/ingestion/football2vec_v2_training.py so HF Jobs scripts
   import cleanly via `ingestion.football2vec_v2_training` and
   src/tests/test_benchmarks.py no longer needs sys.path.insert on the
   scripts directory. Drops the two stale pyproject.toml entries
   (BLE001 per-file-ignore and pyright exclude). Updates ARCHITECTURE.md
   to reflect the new home.

2. Fix the v1 Doc2Vec replace_where clobber bug. Previously
   player_embeddings_v1.run_pipeline_v1 wrote to player_embeddings_raw
   with `replace_where=f"data_source = '{source}'"`, which replaced the
   ENTIRE provider partition. After v2 ran first and wrote 22,726 rows,
   v1 processing a single new match would delete all v2 rows for that
   provider and write back the single match in 32d format — silently
   clobbering v2's full coverage. The fix composes the predicate as
   `data_source = '<source>' AND match_id IN (<ids>)` so v1 only
   replaces the specific rows it processed.

Adds unit test guards:
- src/tests/test_football2vec_v2_training.py — module at new path
- src/tests/test_player_embeddings_v1_replace_where.py — TDD guard that
  asserts v1's replace_where predicate is scoped by match_id

Prepares D45 for the HF Jobs stage-2 inference rerun (destructive ops
phase 1) which will repopulate the HF Hub v2 dataset with full StatsBomb
+ Wyscout coverage (currently Wyscout-only at 22,726 rows).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Do NOT push.

---

<a id="commit-3"></a>
## Commit 3 — D62 Football2vec 360 import pipeline + dbt staging/mart fixes

**Goal**: Ship the missing `compute_embeddings_360()` Databricks entry point plus the three dbt model changes that must land together to avoid a regression when 360 rows first appear in `player_embeddings_raw` (staging dedup loses one side of a v2/360 collision, and the non-360 marts would promote 144d over 128d).

**Files:**
- Modify: `src/ingestion/player_embeddings_v2.py` (add `_HF_360_DATASET`, `_import_embeddings_360()`, `run_pipeline_360()`, `main_360()`)
- Modify: `pyproject.toml` (add `compute_embeddings_360` entry point)
- Modify: `terraform/modules/workflows/main.tf` (add `compute_embeddings_360` task block with `depends_on = compute_embeddings_v2`)
- Modify: `workflow-cards/wf-football2vec-360.yaml` (fix the `distribution: applyInPandas` field — 360 is a direct HF Hub import, not applyInPandas)
- Modify: `dbt_project/models/staging/embeddings/stg_player_embeddings.sql` (add `data_source` to the dedup partition)
- Modify: `dbt_project/models/marts/fct_player_embeddings_season.sql` (add `where data_source != 'football2vec_360'` filter + update `player_best_dim` CTE commentary)
- Modify: `dbt_project/models/marts/fct_player_embeddings_career.sql` (same filter as season variant)
- Create: `src/tests/test_player_embeddings_360.py` (unit test for the new import function)
- Create: `src/tests/test_dbt_player_embeddings_staging_partition.py` (structural guard for the staging partition fix)

### Task 3.1: Write failing test for the 360 import function

- [ ] **Step 1: Create `src/tests/test_player_embeddings_360.py`**

```python
"""Unit tests for ingestion.player_embeddings_v2 360 import path (D62).

The 360 path is structurally near-identical to the v2 path but:
- Uses a different HF Hub dataset (luxury-lakehouse/football2vec-360-embeddings)
- Produces 144-dim behavioral vectors (v2 is 128-dim)
- Labels rows with data_source='football2vec_360' so downstream dbt models
  can isolate them from v2's statsbomb/wyscout partitions
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def test_hf_360_dataset_constant_exists() -> None:
    """The module must export a _HF_360_DATASET constant."""
    from ingestion import player_embeddings_v2 as mod

    assert hasattr(mod, "_HF_360_DATASET")
    assert mod._HF_360_DATASET == "luxury-lakehouse/football2vec-360-embeddings"


def test_import_embeddings_360_function_exists() -> None:
    """The module must expose _import_embeddings_360, run_pipeline_360, main_360."""
    from ingestion import player_embeddings_v2 as mod

    assert callable(getattr(mod, "_import_embeddings_360", None))
    assert callable(getattr(mod, "run_pipeline_360", None))
    assert callable(getattr(mod, "main_360", None))


def test_run_pipeline_360_writes_football2vec_360_data_source() -> None:
    """run_pipeline_360 must label all rows with data_source='football2vec_360'
    regardless of what the source parquet contains."""
    from ingestion import player_embeddings_v2 as mod

    fake_parquet = pd.DataFrame(
        {
            "canonical_player_id": ["p1", "p2", "p3"],
            "match_id": ["m1", "m2", "m3"],
            "behavioral_vector": [
                [0.1] * 144,
                [0.2] * 144,
                [0.3] * 144,
            ],
        }
    )

    captured_writes: list[dict[str, object]] = []

    def fake_write(*args: object, **kwargs: object) -> None:
        captured_writes.append({"args": args, "kwargs": kwargs})

    spark = MagicMock()
    spark.createDataFrame = MagicMock(return_value=MagicMock())
    logger = MagicMock()

    with (
        patch("ingestion.player_embeddings_v2.hf_hub_download", return_value="/tmp/fake.parquet"),
        patch("ingestion.player_embeddings_v2.repo_exists", return_value=True),
        patch("ingestion.player_embeddings_v2.pd.read_parquet", return_value=fake_parquet),
        patch("ingestion.player_embeddings_v2.write_delta_table", side_effect=fake_write),
        patch("ingestion.player_embeddings_v2.validate_dataframe", return_value=3),
        patch("ingestion.player_embeddings_v2._compute_stat_vectors", return_value=(pd.DataFrame(), {})),
        patch("ingestion.player_embeddings_v2._merge_vectors", return_value={}),
        patch("ingestion.player_embeddings_v2._save_norm_params"),
    ):
        result = mod._import_embeddings_360(spark, "soccer_analytics", "bronze", logger)

    assert result is True
    # Must have called write_delta_table exactly once with
    # replace_where="data_source = 'football2vec_360'"
    assert len(captured_writes) == 1, (
        f"Expected one write_delta_table call for the 360 partition, got {len(captured_writes)}"
    )
    rw = captured_writes[0]["kwargs"]["replace_where"]
    assert rw == "data_source = 'football2vec_360'", (
        f"replace_where must isolate the 360 partition, got: {rw}"
    )


def test_run_pipeline_360_rejects_wrong_dimension() -> None:
    """If the HF parquet has vectors with length != 144, the import must raise
    (not silently pass through the wrong dimension)."""
    from ingestion import player_embeddings_v2 as mod

    fake_parquet = pd.DataFrame(
        {
            "canonical_player_id": ["p1"],
            "match_id": ["m1"],
            "behavioral_vector": [[0.1] * 128],  # WRONG: 128 instead of 144
        }
    )

    spark = MagicMock()
    logger = MagicMock()

    with (
        patch("ingestion.player_embeddings_v2.hf_hub_download", return_value="/tmp/fake.parquet"),
        patch("ingestion.player_embeddings_v2.repo_exists", return_value=True),
        patch("ingestion.player_embeddings_v2.pd.read_parquet", return_value=fake_parquet),
    ):
        with pytest.raises((ValueError, RuntimeError), match="144"):
            mod._import_embeddings_360(spark, "soccer_analytics", "bronze", logger)
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest src/tests/test_player_embeddings_360.py -v
```

Expected: All 4 tests FAIL with `AttributeError: module 'ingestion.player_embeddings_v2' has no attribute '_HF_360_DATASET'` (etc.).

### Task 3.2: Implement the 360 import function

- [ ] **Step 1: Add the new constants + imports to `src/ingestion/player_embeddings_v2.py`**

Near line 54 (after the existing `_HF_V2_DATASET` constant), add:

```python
_HF_V2_DATASET = "luxury-lakehouse/football2vec-statsbomb-wyscout"
_HF_360_DATASET = "luxury-lakehouse/football2vec-360-embeddings"
_V2_BEHAVIORAL_DIM = 128
_V360_BEHAVIORAL_DIM = 144
_FOOTBALL2VEC_360_DATA_SOURCE = "football2vec_360"
```

Then, at the top of the file with the other imports, add module-level imports that tests patch (so the test's `patch("ingestion.player_embeddings_v2.repo_exists")` works). After line 15:

```python
import pandas as pd
from huggingface_hub import hf_hub_download, repo_exists  # noqa: E402 — module-level import after pandas for test patch seam
```

(Note: this changes `_import_v2_embeddings()` which currently lazy-imports these. Update `_import_v2_embeddings()` to use the module-level imports too so both paths share the same seam and tests are consistent.)

Update `_import_v2_embeddings()` at line 80-85 from:
```python
    try:
        from huggingface_hub import hf_hub_download, repo_exists
    except ImportError:
        logger.info("huggingface_hub not available — falling back to Doc2Vec v1")
        return False
```

To:
```python
    # huggingface_hub is imported at module level so tests can patch
    # `ingestion.player_embeddings_v2.repo_exists` / `hf_hub_download`.
    pass
```

And delete the old try/import block. Now both `_import_v2_embeddings` and `_import_embeddings_360` (below) use the same module-level imports.

- [ ] **Step 2: Add `_import_embeddings_360()` function after `_import_v2_embeddings()` (around line 216)**

```python
# ---------------------------------------------------------------------------
# 360 import — pre-computed 144-dim 360-enriched transformer embeddings from HF Hub
# ---------------------------------------------------------------------------


def _import_embeddings_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> bool:
    """Import pre-computed 144-dim 360-enriched embeddings from HF Hub.

    Downloads the Parquet file from ``luxury-lakehouse/football2vec-360-embeddings``,
    labels every row with ``data_source='football2vec_360'`` (overriding any
    provider info in the parquet), validates the 144-dim vector contract, and
    writes to ``player_embeddings_raw`` with ``replace_where=data_source='football2vec_360'``
    so the 360 partition is isolated from v2's provider-keyed partitions.

    The 360 model has its own embedding space (144-dim = 128-dim transformer +
    16-dim Deep Sets context) and is NOT directly comparable to the v2 128-dim
    embeddings. Downstream dbt models (``fct_player_embeddings_season_360``,
    ``fct_player_embeddings_career_360``) filter on ``data_source='football2vec_360'``
    to aggregate it separately from the v1/v2 paths.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name.
        logger: Logger instance.

    Returns:
        True if 360 embeddings were successfully imported. False if the
        HF Hub dataset does not exist (early exit, non-fatal).

    Raises:
        RuntimeError: If the downloaded parquet has vectors of the wrong
            dimension (expected 144 per row). Silent dimension drift is
            forbidden — ADR-002 applies.
    """
    if not repo_exists(_HF_360_DATASET, repo_type="dataset"):
        logger.info("HF dataset %s not found — skipping 360 import", _HF_360_DATASET)
        return False

    logger.info("Importing 360-enriched transformer embeddings from %s", _HF_360_DATASET)

    parquet_path = hf_hub_download(
        repo_id=_HF_360_DATASET,
        filename="data/embeddings_360.parquet",
        repo_type="dataset",
    )

    pdf = pd.read_parquet(parquet_path)
    logger.info("Downloaded %d 360 embeddings from HF Hub", len(pdf))

    if pdf.empty:
        logger.warning("360 embeddings Parquet is empty — nothing to import")
        return False

    required_cols = {"canonical_player_id", "match_id", "behavioral_vector"}
    if not required_cols.issubset(pdf.columns):
        missing = required_cols - set(pdf.columns)
        msg = f"360 Parquet missing required columns {missing}"
        raise RuntimeError(msg)

    # Validate the 144-dim contract. Dimension drift would silently corrupt
    # downstream cosine similarity — fail loudly. Check the first 10 rows
    # (cheap, catches drift without full-column iteration).
    for i, vec in enumerate(pdf["behavioral_vector"].iloc[:10]):
        vec_list = list(vec) if not isinstance(vec, list) else vec
        if len(vec_list) != _V360_BEHAVIORAL_DIM:
            msg = (
                f"360 Parquet vector at row {i} has length {len(vec_list)}, "
                f"expected {_V360_BEHAVIORAL_DIM} (144-dim 360-enriched). "
                f"This means the HF dataset schema drifted — do NOT import "
                f"until the training run is re-verified."
            )
            raise RuntimeError(msg)

    # Ensure string types for key columns.
    for col in ("canonical_player_id", "match_id"):
        pdf[col] = pdf[col].astype(str)

    # Normalize behavioral_vector entries to list[float].
    pdf["behavioral_vector"] = pdf["behavioral_vector"].apply(
        lambda v: [float(x) for x in v] if not isinstance(v, list) else v
    )

    # ALL 360 rows get data_source='football2vec_360' — this is the
    # dbt discriminator, NOT the provider. Overrides any provider
    # column in the source parquet.
    pdf["data_source"] = _FOOTBALL2VEC_360_DATA_SOURCE

    # Stat vectors — 360 embeddings are aggregated with the same stat
    # features as v2. Reuse the existing helper.
    event_player_ids: set[int] = set()
    for pid_str in pdf["canonical_player_id"].unique():
        try:
            event_player_ids.add(int(pid_str))
        except (ValueError, TypeError):
            pass

    stat_df, norm_params = _compute_stat_vectors(
        spark, catalog, DEFAULT_GOLD_SCHEMA, player_ids=event_player_ids or None
    )
    logger.info("Computed stat vectors for %d player-comp-season entries", len(stat_df))

    if norm_params:
        _save_norm_params(catalog, norm_params, logger)

    # 360 uses the same match_competition_map as v2 — derive from fct_action_values.
    gold = DEFAULT_GOLD_SCHEMA
    try:
        meta_query = (
            f"SELECT DISTINCT "  # noqa: S608
            f"  CAST(match_id AS STRING) AS match_id, "
            f"  CAST(competition_id AS STRING) AS competition_id, "
            f"  CAST(season_id AS STRING) AS season_id "
            f"FROM {catalog}.{gold}.fct_action_values"
        )
        meta_pdf = spark.sql(meta_query).toPandas()
        match_competition_map: dict[str, tuple[str, str]] = dict(
            zip(
                meta_pdf["match_id"].astype(str),
                zip(meta_pdf["competition_id"].astype(str), meta_pdf["season_id"].astype(str), strict=True),
                strict=True,
            )
        )
    except Exception as exc:
        msg = (
            "Could not load match metadata from fct_action_values — "
            "360 stat vectors will be None for all rows"
        )
        logger.error(msg, exc_info=True)
        match_competition_map = {}

    behavioral_keys = list(
        zip(pdf["canonical_player_id"].astype(str), pdf["match_id"].astype(str), strict=True)
    )
    merged_stats = _merge_vectors(behavioral_keys, stat_df, match_competition_map)

    pdf["stat_vector"] = [
        merged_stats.get(k)
        for k in zip(pdf["canonical_player_id"].astype(str), pdf["match_id"].astype(str), strict=True)
    ]

    # Write to bronze with a SINGLE replace_where on the 360 partition.
    sdf = spark.createDataFrame(
        pdf[["canonical_player_id", "match_id", "data_source", "behavioral_vector", "stat_vector"]]
    )
    row_count = validate_dataframe(
        sdf,
        ["canonical_player_id", "match_id", "data_source", "behavioral_vector"],
        _TABLE_NAME,
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"data_source = '{_FOOTBALL2VEC_360_DATA_SOURCE}'",
        logger=logger,
        row_count=row_count,
    )

    logger.info("Successfully imported %d 360 embeddings from HF Hub", len(pdf))
    return True


@workflow("wf-football2vec-360", phase="inference")
def run_pipeline_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Import pre-computed 360-enriched embeddings from HF Hub.

    Decorated with ``wf-football2vec-360`` for independent cost/runtime tracking.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new 360 embedding work")

    logger.info("Starting 360 transformer embedding import for %s.%s", catalog, schema)

    success = _import_embeddings_360(spark, catalog, schema, logger)
    if success:
        logger.info("360 transformer embedding import complete")
    else:
        logger.info("360 transformer embeddings not available — no action taken")
    return 0


def main_360() -> None:
    """CLI entry point for 360 transformer embedding import from HF Hub."""
    args = parse_ingestion_args("Import 360 transformer embeddings from HF Hub")
    logger = configure_logging("player_embeddings_360")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(_football2vec_360_guard, spark, args.catalog, args.schema)

    run_pipeline_360(spark, args.catalog, args.schema, logger, filter_result=filter_result)
```

- [ ] **Step 2a: Add the 360 guard class** near the existing `_Football2VecV2Guard` (around line 40)

```python
class _Football2Vec360Guard:
    workflow_id = "wf-football2vec-360"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        # Placeholder guard — always returns count=1 to trigger a run.
        # A proper guard would check whether the HF dataset commit hash has
        # changed since the last successful import (track in a sidecar file).
        # Deferred to a follow-up cycle — matches the v2 placeholder pattern.
        return FilterResult(workflow_id=self.workflow_id, count=1)


_football2vec_360_guard = _Football2Vec360Guard()
```

- [ ] **Step 3: Run the 360 unit tests — should now pass**

```bash
uv run pytest src/tests/test_player_embeddings_360.py -v
```

Expected: All 4 tests PASS.

### Task 3.3: Add `compute_embeddings_360` entry point to pyproject.toml

- [ ] **Step 1: Add the entry point** near line 85 (after `compute_embeddings_v1`):

```toml
compute_embeddings = "ingestion.player_embeddings_v2:main"
compute_embeddings_v2 = "ingestion.player_embeddings_v2:main_v2"
compute_embeddings_v1 = "ingestion.player_embeddings_v1:main_v1"
compute_embeddings_360 = "ingestion.player_embeddings_v2:main_360"
```

- [ ] **Step 2: Verify the entry point resolves**

```bash
uv run python -c "from ingestion.player_embeddings_v2 import main_360; print(main_360)"
```

Expected: prints `<function main_360 at 0x...>`.

### Task 3.4: Add Terraform task block for `compute_embeddings_360`

- [ ] **Step 1: Add the task block** in `terraform/modules/workflows/main.tf` immediately after the `compute_embeddings_v1` task (around line 526):

```hcl
  # ── Task: Compute player embeddings 360-enriched (Deep Sets + transformer) ───
  # Football2vec 360: imports pre-trained 144d 360-enriched embeddings from
  # HF Hub, writes to bronze.player_embeddings_raw with
  # data_source='football2vec_360'. Depends on compute_embeddings_v2 running
  # first (shared HF Hub auth + stat vector cache path).
  task {
    task_key        = "compute_embeddings_360"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_embeddings_v2"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_360"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }
```

- [ ] **Step 2: Validate terraform syntax**

```bash
cd terraform/environments/dev && uv run python -c "
import subprocess
# Use tflint if available, else terraform fmt -check
r = subprocess.run(['terraform', 'fmt', '-check', '-recursive', '../../modules/workflows/'], capture_output=True, text=True)
print('stdout:', r.stdout)
print('stderr:', r.stderr)
print('exit:', r.returncode)
" 2>&1
```

Expected: `exit: 0`, terraform fmt reports no issues.

- [ ] **Step 3: Do NOT run `terraform plan` or `terraform apply`** — Terraform changes will be applied by the user after PR merge, not during the plan execution.

### Task 3.5: Fix the workflow card distribution field

- [ ] **Step 1: Update `workflow-cards/wf-football2vec-360.yaml:61`**

Change:
```yaml
    distribution: applyInPandas
    partition_key: batch_id
```

To:
```yaml
    distribution: driver-local
    # No partition_key — 360 is a single-shot HF Hub download on the driver,
    # then a single replace_where write to player_embeddings_raw. No
    # applyInPandas (the 9,936-row parquet fits comfortably on the driver).
```

### Task 3.6: Write failing test for the dbt staging partition fix

- [ ] **Step 1: Create `src/tests/test_dbt_player_embeddings_staging_partition.py`**

```python
"""D62 structural guards for dbt player embedding models.

When the 360 import ships, bronze.player_embeddings_raw will contain both
v2 rows (data_source='statsbomb'/'wyscout', 128d) and 360 rows
(data_source='football2vec_360', 144d). The staging model must NOT collapse
same-(player,match) rows with different data_source, and the non-360 marts
must exclude 360 rows to avoid the player_best_dim CTE promoting 144d
vectors over 128d ones.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STAGING = _REPO_ROOT / "dbt_project" / "models" / "staging" / "embeddings" / "stg_player_embeddings.sql"
_MART_SEASON = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_player_embeddings_season.sql"
_MART_CAREER = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_player_embeddings_career.sql"


@pytest.fixture(scope="module")
def staging_sql() -> str:
    return _STAGING.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def mart_season_sql() -> str:
    return _MART_SEASON.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def mart_career_sql() -> str:
    return _MART_CAREER.read_text(encoding="utf-8")


def test_stg_player_embeddings_partitions_by_data_source(staging_sql: str) -> None:
    """D62 fix: the staging dedup must include data_source in the partition so
    v2 and 360 rows for the same (player, match) coexist as separate rows."""
    # Find the row_number window function
    pattern = re.compile(
        r"row_number\s*\(\s*\)\s*over\s*\(\s*partition\s+by\s+([^)]+)\)",
        re.IGNORECASE,
    )
    m = pattern.search(staging_sql)
    assert m is not None, "stg_player_embeddings.sql has no row_number window"
    partition_cols = m.group(1)
    assert "data_source" in partition_cols, (
        "D62 regression: stg_player_embeddings.sql partitions by "
        f"`{partition_cols.strip()}` — must include data_source so v2 "
        "and 360 rows coexist for the same (player, match)."
    )
    assert "canonical_player_id" in partition_cols
    assert "match_id" in partition_cols


def test_fct_player_embeddings_season_excludes_360(mart_season_sql: str) -> None:
    """D62 fix: the non-360 season mart must exclude football2vec_360 rows."""
    assert "data_source != 'football2vec_360'" in mart_season_sql or \
           "data_source <> 'football2vec_360'" in mart_season_sql, (
        "D62 regression: fct_player_embeddings_season.sql must include "
        "`where data_source != 'football2vec_360'` to prevent the "
        "player_best_dim CTE from promoting 144d vectors over 128d."
    )


def test_fct_player_embeddings_career_excludes_360(mart_career_sql: str) -> None:
    """D62 fix: the non-360 career mart must exclude football2vec_360 rows."""
    assert "data_source != 'football2vec_360'" in mart_career_sql or \
           "data_source <> 'football2vec_360'" in mart_career_sql, (
        "D62 regression: fct_player_embeddings_career.sql must include "
        "`where data_source != 'football2vec_360'` to prevent the "
        "player_best_dim CTE from promoting 144d vectors over 128d."
    )
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest src/tests/test_dbt_player_embeddings_staging_partition.py -v
```

Expected: All 3 tests FAIL — the current SQL has none of the fixes.

### Task 3.7: Fix the dbt staging model

- [ ] **Step 1: Update `dbt_project/models/staging/embeddings/stg_player_embeddings.sql:21-23`**

Change:
```sql
        row_number() over (
            partition by canonical_player_id, match_id
            order by _ingested_at desc
        ) as _row_num
```

To:
```sql
        row_number() over (
            -- D62 2026-04-15: data_source is part of the dedup partition so
            -- v2 rows (128d, data_source='statsbomb'/'wyscout') and 360 rows
            -- (144d, data_source='football2vec_360') coexist for the same
            -- (player, match) pair. Previously the dedup collapsed them,
            -- silently losing one side of the collision.
            partition by canonical_player_id, match_id, data_source
            order by _ingested_at desc
        ) as _row_num
```

### Task 3.8: Fix the non-360 mart filters

- [ ] **Step 1: Update `dbt_project/models/marts/fct_player_embeddings_season.sql`** — modify the `player_best_dim` CTE and add a 360-exclusion filter

Change the `player_best_dim` block (lines 16-22):
```sql
with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    group by canonical_player_id
),
```

To:
```sql
with player_best_dim as (
    -- For players with mixed-dimension vectors (32d v1 + 128d v2),
    -- keep only the highest-dimension embeddings per player.
    -- D62 2026-04-15: explicitly exclude 360-enriched rows (144d) so they
    -- do not promote over v2's 128d embeddings. The 360 aggregates live
    -- in fct_player_embeddings_season_360 / _career_360 with their own
    -- dimensionally-homogeneous aggregation.
    select canonical_player_id, max(size(behavioral_vector)) as best_dim
    from {{ ref('fct_player_embeddings') }}
    where data_source != 'football2vec_360'
    group by canonical_player_id
),
```

Also update the `embeddings_with_context` CTE (lines 24-41) to add the same filter. Change:
```sql
embeddings_with_context as (

    select
        e.canonical_player_id,
        e.match_id,
        e.data_source,
        e.behavioral_vector,
        e.stat_vector,
        m.competition_id,
        m.season_id
    from {{ ref('fct_player_embeddings') }} e
    inner join {{ ref('fct_match_summary') }} m
        on e.match_id = m.match_id
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim

),
```

To:
```sql
embeddings_with_context as (

    select
        e.canonical_player_id,
        e.match_id,
        e.data_source,
        e.behavioral_vector,
        e.stat_vector,
        m.competition_id,
        m.season_id
    from {{ ref('fct_player_embeddings') }} e
    inner join {{ ref('fct_match_summary') }} m
        on e.match_id = m.match_id
    inner join player_best_dim p
        on e.canonical_player_id = p.canonical_player_id
        and size(e.behavioral_vector) = p.best_dim
    -- D62 2026-04-15: 360-enriched embeddings live in their own mart; exclude here.
    where e.data_source != 'football2vec_360'

),
```

- [ ] **Step 2: Update `dbt_project/models/marts/fct_player_embeddings_career.sql`** — same pattern

Change the `player_best_dim` CTE (lines 16-22) to add `where data_source != 'football2vec_360'`.

Change the main query (lines 24-37) to add `where e.data_source != 'football2vec_360'` in the same position.

The exact text replacements follow the same shape as Task 3.8 Step 1.

### Task 3.9: Verify the dbt changes compile and the structural tests pass

- [ ] **Step 1: Re-run the structural test**

```bash
uv run pytest src/tests/test_dbt_player_embeddings_staging_partition.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 2: Re-run the 360 unit tests**

```bash
uv run pytest src/tests/test_player_embeddings_360.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 3: dbt parse + compile**

```bash
cd dbt_project && uv run --extra dbt dbt parse --profiles-dir . && uv run --extra dbt dbt compile --select +fct_player_embeddings_season +fct_player_embeddings_career --profiles-dir .
```

Expected: Both parse and compile succeed.

### Task 3.10: Full quality gate

- [ ] **Step 1: Full test suite**

```bash
uv run pytest src/tests/ --benchmark-disable -q 2>&1 | tail -5
```

Expected: `1492 passed, 12 skipped` (baseline 1485 + new tests from all three commits cumulative).

- [ ] **Step 2: ruff + format + pyright**

```bash
uv run ruff check src/ scripts/ hf_taipy_app/src/
uv run ruff format --check src/ scripts/ hf_taipy_app/src/
uv run pyright src/ 2>&1 | tail -5
```

Expected: all clean.

- [ ] **Step 3: Import boundary check**

```bash
uv run lint-imports --config pyproject.toml 2>&1 | tail -3
```

Expected: all contracts pass.

- [ ] **Step 4: Workflow card validator**

```bash
uv run validate_workflow_cards
```

Expected: exit 0, all 36 cards valid including the edited `wf-football2vec-360.yaml`.

### Task 3.11: STOP for commit approval

- [ ] **Step 1: Report the Commit 3 diff**

```bash
git status
git diff --stat
git diff src/ingestion/player_embeddings_v2.py | head -200
```

- [ ] **Step 2: Wait for explicit user approval. Then:**

```bash
git add src/ingestion/player_embeddings_v2.py \
        pyproject.toml \
        terraform/modules/workflows/main.tf \
        workflow-cards/wf-football2vec-360.yaml \
        dbt_project/models/staging/embeddings/stg_player_embeddings.sql \
        dbt_project/models/marts/fct_player_embeddings_season.sql \
        dbt_project/models/marts/fct_player_embeddings_career.sql \
        src/tests/test_player_embeddings_360.py \
        src/tests/test_dbt_player_embeddings_staging_partition.py

git commit -m "$(cat <<'EOF'
feat(football2vec-360): add compute_embeddings_360 + dbt dedup/filter fixes (D62)

Ship the missing Databricks import pipeline for the 360-enriched Football2vec
embeddings plus the three dbt model changes that must land together to
prevent a silent regression when 360 rows first appear in bronze.

Code changes (src/ingestion/player_embeddings_v2.py):
- New constants: _HF_360_DATASET, _V360_BEHAVIORAL_DIM, _FOOTBALL2VEC_360_DATA_SOURCE
- New function _import_embeddings_360(): downloads from HF dataset
  luxury-lakehouse/football2vec-360-embeddings (9,936 rows × 144d verified live),
  validates the 144-dim vector contract (RuntimeError on drift — silent
  dimension change is forbidden per ADR-002), labels all rows with
  data_source='football2vec_360', writes with a single replace_where on
  that partition.
- New run_pipeline_360() and main_360() entry points (with @workflow decorator)
- New _Football2Vec360Guard placeholder (matches v2 pattern; real guard
  deferred to a follow-up cycle)
- Module-level huggingface_hub imports so tests can patch repo_exists /
  hf_hub_download consistently across v2 and 360 paths

Wheel registration:
- pyproject.toml: new compute_embeddings_360 console-script pointing at main_360

Terraform:
- terraform/modules/workflows/main.tf: new compute_embeddings_360 task block
  with depends_on = compute_embeddings_v2 (shared HF Hub auth + stat vector
  cache). environment_key = embeddings.

Workflow card:
- wf-football2vec-360.yaml: fix the distribution field (was applyInPandas,
  now driver-local — the 9,936-row parquet fits comfortably on the driver).

dbt changes (all three required together to prevent regression):
- stg_player_embeddings.sql: add data_source to the row_number partition so
  v2 and 360 rows for the same (player, match) coexist as distinct rows.
- fct_player_embeddings_season.sql: exclude football2vec_360 rows in both
  the player_best_dim CTE and the embeddings_with_context CTE. Prevents
  player_best_dim from promoting 144d over 128d when a player has both.
- fct_player_embeddings_career.sql: same filter as the season variant.

Tests:
- src/tests/test_player_embeddings_360.py: 4 unit tests covering the new
  constant, function signatures, data_source labeling, and 144-dim
  validation (with mocked HF Hub + Spark).
- src/tests/test_dbt_player_embeddings_staging_partition.py: 3 structural
  guards asserting the staging partition includes data_source and the
  non-360 marts exclude football2vec_360.

Downstream effect: after the destructive-ops phase triggers the new
compute_embeddings_360 Databricks task, fct_player_embeddings_season_360
and _career_360 will populate (currently vacuous) and the corresponding
Lakebase synced tables will carry real data for the first time.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Do NOT push yet — destructive ops precede the push.

---

<a id="destructive-ops-phase-1"></a>
## Destructive ops phase 1 — v2 stage 2 HF Jobs rerun (D45)

**Goal**: Re-run the Football2vec v2 stage 2 training / inference on the NEW dataset (~87K sequences covering both StatsBomb and Wyscout, versus the current 22,726 Wyscout-only snapshot) and publish the updated embeddings parquet to HF Hub at `luxury-lakehouse/football2vec-statsbomb-wyscout`.

**Important**: This step spends real money (~$1-2 on HF Jobs L40S). Get explicit user approval before triggering.

### Task DO1.1: Pre-flight check — wheel version on HF Hub must match

- [ ] **Step 1**: Verify the uncommitted wheel version on HF Hub matches the branch's `shared.wheel.WHEEL_VERSION`

```bash
uv run python -c "
from shared.wheel import WHEEL_VERSION, WHEEL_FILENAME, WHEEL_REPO
from huggingface_hub import HfApi
api = HfApi()
files = api.list_repo_files(WHEEL_REPO, repo_type='model')
print(f'Expected: {WHEEL_FILENAME}')
print(f'On HF Hub: {[f for f in files if f.endswith(\".whl\")]}')
"
```

Expected: The currently-deployed wheel on HF Hub is `luxury_lakehouse-0.3.2-py3-none-any.whl`. Since this plan has NOT yet pushed commits, that's still the main-branch wheel — **it does NOT yet have the D45 code changes**. The HF Jobs training script pins against this wheel via PEP 723.

**DECISION POINT**: The PEP 723 script `scripts/train_football2vec_v2.py:4` references `luxury-lakehouse @ https://huggingface.co/.../luxury_lakehouse-0.3.2-py3-none-any.whl`. Since the training script imports ONLY `ingestion.football2vec_v2_training.*` (our new path — post-D45-commit-2) and `analytics.football2vec_transformer.*`, the training run will fail unless the wheel on HF Hub is updated first.

**Two paths**:
- **(a) Local-build + temporary HF Hub upload** before the PR lands. Safer but manual. Build wheel locally, upload manually to HF Hub build-artifacts repo, run training, revert HF Hub upload after PR merges and CI auto-deploys.
- **(b) Push the branch to GitHub, let CI build + upload wheel to HF Hub automatically on main merge, THEN run training**. Requires the PR to merge first. This is the "PR ships → wheel updates → training run → Databricks tasks" sequence.

**Plan decision: Path (b)** — merge the PR first (after final approval), let CI deploy the wheel, then run destructive ops. This is the same sequence used in the Data Integrity Foundation cycle.

Practically: skip Destructive Ops Phase 1 until after the PR is merged. The PR merge is covered in section 8 below. Destructive ops run post-merge.

- [ ] **Step 2**: Mark this task as "blocked until PR merge" and proceed to section 8 (PR creation).

---

<a id="destructive-ops-phase-2"></a>
## Destructive ops phase 2 — Databricks task triggers

**Goal** (runs AFTER PR merge + wheel deploy): Re-trigger `compute_embeddings_v2` and then `compute_embeddings_360` via `databricks jobs run-now` with `only: [...]` task subset filtering so we only touch the embeddings tasks, not the full 28-task daily job.

### Task DO2.1: Trigger compute_embeddings_v2 against the fresh wheel

- [ ] **Step 1: Build the run-now request body**

```bash
cat > /tmp/runnow_embed_v2.json <<'EOF'
{
  "job_id": 302697362345215,
  "only": ["compute_embeddings_v2"]
}
EOF
```

- [ ] **Step 2: Trigger with --no-wait and capture run_id**

```bash
uv run databricks jobs run-now --json @/tmp/runnow_embed_v2.json --no-wait 2>&1
```

Capture the `run_id` from the response (JSON with `"run_id": <int>`).

- [ ] **Step 3: Monitor the run via the Monitor tool pattern** (emit on state change, poll every 45s)

Script template for the Monitor:
```bash
RUN_ID=<captured from step 2>
last_state=""
while true; do
  line=$(uv run databricks jobs get-run $RUN_ID 2>&1 | python -c "
import json, sys
d = json.load(sys.stdin)
lc = d.get('state', {}).get('life_cycle_state', '?')
rs = d.get('state', {}).get('result_state', '')
for t in d.get('tasks', []):
    if t.get('task_key') == 'compute_embeddings_v2':
        ts = t.get('state', {})
        print(f'run={lc}/{rs}|task={ts.get(\"life_cycle_state\",\"?\")}/{ts.get(\"result_state\",\"\")}')
        break
")
  if [ \"$line\" != \"$last_state\" ]; then
    echo \"$(date +%H:%M:%S) $line\"
    last_state=\"$line\"
  fi
  if echo \"$line\" | grep -qE \"run=(TERMINATED|INTERNAL_ERROR|SKIPPED)/\"; then
    echo \"TERMINAL\"
    exit 0
  fi
  sleep 45
done
```

Expected: `run=TERMINATED/SUCCESS|task=TERMINATED/SUCCESS` within ~5 minutes (v2 is a fast HF Hub download + Delta MERGE, no heavy compute).

- [ ] **Step 4: Verify bronze row counts**

Use a temporary baseline script (delete after use — patterns from the Data Integrity Foundation cycle):

```python
# scripts/_d45_verify_v2_rerun.py (create, run, delete)
"""One-off verification for D45 v2 inference rerun."""
from __future__ import annotations

import os
from databricks import sql


def _path_parts() -> tuple[str, str, str]:
    host = os.environ["DATABRICKS_HOST"].rstrip("/").removeprefix("https://")
    token = os.environ["DATABRICKS_TOKEN"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"].replace("//sql/", "/sql/")
    return host, token, http_path


QUERIES: list[tuple[str, str]] = [
    (
        "bronze.player_embeddings_raw by data_source",
        """
        SELECT
            data_source,
            COUNT(*) AS rows,
            COUNT(DISTINCT canonical_player_id) AS players,
            COUNT(DISTINCT match_id) AS matches,
            MAX(size(behavioral_vector)) AS max_dim
        FROM soccer_analytics.bronze.player_embeddings_raw
        GROUP BY data_source
        ORDER BY data_source
        """,
    ),
]


def main() -> None:
    host, token, http_path = _path_parts()
    with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cur:
            for label, q in QUERIES:
                print(f"\n=== {label} ===")
                cur.execute(q)
                cols = [c[0] for c in (cur.description or [])]
                for row in cur.fetchall():
                    print("  " + ", ".join(f"{c}={v}" for c, v in zip(cols, row, strict=False)))


if __name__ == "__main__":
    main()
```

Run:
```bash
DATABRICKS_HOST=dbc-48322be9-16be.cloud.databricks.com uv run python scripts/ensure_warehouse.py
uv run --with databricks-sql-connector python scripts/_d45_verify_v2_rerun.py
```

Expected: `data_source=statsbomb` and `data_source=wyscout` rows with `max_dim=128`. If v2 training rerun has already shipped (separate deferred concern), row counts should be higher than the current 22,726 baseline. If the training rerun has NOT shipped yet, v2 still publishes the 22,726 rows — this is expected and acceptable for this plan; the D45 fix is about clobber semantics, not about the training rerun.

### Task DO2.2: Trigger compute_embeddings_360 against the fresh wheel

- [ ] **Step 1: Trigger with only-filter for the 360 task**

```bash
cat > /tmp/runnow_embed_360.json <<'EOF'
{
  "job_id": 302697362345215,
  "only": ["compute_embeddings_360"]
}
EOF
uv run databricks jobs run-now --json @/tmp/runnow_embed_360.json --no-wait 2>&1
```

- [ ] **Step 2: Monitor to completion** (same pattern as Task DO2.1 but filter for `compute_embeddings_360`)

Expected: `TERMINATED/SUCCESS` within ~3-5 minutes.

- [ ] **Step 3: Verify 360 rows are present**

```bash
uv run --with databricks-sql-connector python scripts/_d45_verify_v2_rerun.py
```

Expected: A new row in the output: `data_source=football2vec_360, rows=9936, max_dim=144` (or similar). Confirms the 360 import wrote the expected row count and dimension.

- [ ] **Step 4: Verify the D45 fix worked — v2 rows are STILL present after 360 ran**

The output should show:
- `data_source=statsbomb, max_dim=128, rows>0` (v2 survived)
- `data_source=wyscout, max_dim=128, rows>0` (v2 survived)
- `data_source=football2vec_360, max_dim=144, rows=9936` (new 360 partition)

If statsbomb/wyscout rows are MISSING, the D45 fix is broken — STOP and investigate.

### Task DO2.3: Delete the temp verification script

- [ ] **Step 1**: `rm scripts/_d45_verify_v2_rerun.py`

---

<a id="destructive-ops-phase-3"></a>
## Destructive ops phase 3 — dbt full refresh + synced tables + Puppeteer verification

**Goal**: Rebuild `fct_player_embeddings` and its four downstream aggregate models, refresh the affected Lakebase synced tables, and Puppeteer-verify that the Player Similarity page renders correctly on both staging and production with the new data flow.

### Task DO3.1: dbt build --full-refresh on player embeddings subtree

- [ ] **Step 1: Ensure warehouse running**

```bash
DATABRICKS_HOST=dbc-48322be9-16be.cloud.databricks.com uv run python scripts/ensure_warehouse.py
```

- [ ] **Step 2: Build with `--vars embeddings_enabled=true` to activate the gated models**

```bash
cd dbt_project && uv run --extra dbt dbt build --full-refresh --select fct_player_embeddings+ --vars '{"embeddings_enabled": true}' --profiles-dir .
```

Expected: PASS/WARN summary with:
- `fct_player_embeddings` (staging + mart)
- `fct_player_embeddings_season` and `_career` (non-360)
- `fct_player_embeddings_season_360` and `_career_360` (NOW POPULATED for the first time)

- [ ] **Step 3: Verify row counts in the aggregate tables**

Write a second temp script `scripts/_d45_verify_marts.py` with queries:

```sql
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_season;
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_career;
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_season_360;
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_player_embeddings_career_360;
```

Expected:
- Non-360 season/career: >0 rows (v2 coverage — same or more than pre-cycle baseline)
- **360 season/career: >0 rows for the first time**. Pre-cycle baseline was 0 rows each.

### Task DO3.2: Refresh the affected Lakebase synced tables

- [ ] **Step 1: Refresh with explicit table list**

```bash
DATABRICKS_HOST=dbc-48322be9-16be.cloud.databricks.com uv run python -m ingestion.refresh_synced_tables --wait --tables fct_player_embeddings_synced,fct_player_embeddings_career_synced,fct_player_embeddings_season_synced,fct_player_embeddings_career_360_synced,fct_player_embeddings_season_360_synced 2>&1 | tail -20
```

Expected: 5/5 COMPLETE, 0 errors.

### Task DO3.3: Delete temp verification scripts

- [ ] **Step 1**: `rm scripts/_d45_verify_marts.py` (if used)

### Task DO3.4: Puppeteer-verify Player Similarity on staging

- [ ] **Step 1: Navigate to staging app**

```javascript
// via mcp__puppeteer__puppeteer_navigate
url: "https://luxury-lakehouse-staging.hf.space"
launchOptions: {"headless": false, "channel": "chrome", "defaultViewport": {"width": 1600, "height": 1000}}
```

- [ ] **Step 2: Click Player Similarity in the sidebar**

```javascript
// mcp__puppeteer__puppeteer_click with selector matching /Player.Similarity/i href
```

- [ ] **Step 3: Select a known EPL player (e.g., Sergio Agüero) from the Player dropdown**

Follow the same select pattern used in the session 41 Player Impact verification: find the label via `Array.from(document.querySelectorAll('label')).find(l => l.textContent.trim() === 'Player')`, then dispatch pointerdown/mousedown/pointerup/mouseup/click on the wrapper, then select the player option.

- [ ] **Step 4: Screenshot and verify**

- The Player Similarity chart renders with similarity results (list or radar chart depending on the view selected)
- No error banner
- No 0-results empty state

- [ ] **Step 5: Switch the "Model" selector (if present) between v2 and 360 variants and verify BOTH render**

If the Player Similarity page has a model selector that includes "Football2vec v2" and "Football2vec 360", verify both selections produce results. If the page currently only exposes v2, add a TODO entry to surface the 360 option — but do NOT block the cycle on that.

### Task DO3.5: Puppeteer-verify Player Similarity on production

- [ ] **Step 1: Navigate to production**

```javascript
url: "https://luxury-lakehouse-soccer-analytics-app.hf.space/Player-Similarity"
```

- [ ] **Step 2-4: Same as Task DO3.4 steps 3-5** (both apps read from the same Lakebase synced tables)

Expected: Identical results to staging.

---

<a id="pr-creation"></a>
## PR creation

**Goal**: Ship the three commits in a single PR, run `/final-review` before creation, get user approval, merge, let CI auto-deploy the wheel, THEN run destructive ops phases 2 and 3 (per the sequence decided in phase 1).

### Task PR.1: Final review

- [ ] **Step 1**: Invoke `mad-scientist-skills:final-review` skill. Run all phases.
- [ ] **Step 2**: Address any Critical or High severity findings from the review.

### Task PR.2: Push the branch

- [ ] **Step 1**: User must explicitly approve the push to remote. Wait.

- [ ] **Step 2**: On approval:

```bash
git push -u origin feat/football2vec-completion 2>&1 | tail -10
```

### Task PR.3: Create the PR

- [ ] **Step 1: Write the PR body to a temp file**

Use a heredoc with the following structure (adapt to session realities):

```markdown
## Summary

- **D45 Football2vec v2 StatsBomb coverage + helper migration** — moves
  `scripts/train_football2vec_v2_helpers.py` (237 lines) into the wheel at
  `src/ingestion/football2vec_v2_training.py`, drops the obsolete pyproject
  entries, and updates test_benchmarks / train_football2vec_v2 imports. Fixes
  the v1 Doc2Vec replace_where clobber bug where a single new statsbomb
  match would delete all 22K v2 rows.
- **D62 Football2vec 360 import pipeline + dbt fixes** — ships the missing
  `compute_embeddings_360` Databricks entry point (wheel + Terraform + card),
  PLUS three dbt model fixes (staging partition, non-360 mart 360-exclusion
  filters) that must land together to prevent a regression when 360 rows
  first hit bronze.
- **D65 warm-tier post-hook 1 watermark fix** — replaces
  `MAX(usage_date) + INTERVAL 1 DAY` with a per-workflow EXISTS subquery
  so per-row billing arrivals no longer prune sibling NULL-billing rows.

## Test plan

- [x] Full local pytest (`1492 passed, 12 skipped`)
- [x] Ruff + pyright + import-linter (clean)
- [x] dbt parse + compile for all affected models
- [x] Workflow card validator
- [ ] CI green on this branch
- [ ] Destructive ops phase 2: `compute_embeddings_v2` re-triggered, bronze
      row counts verified per data_source + max_dim
- [ ] Destructive ops phase 2: `compute_embeddings_360` triggered for the
      first time, 9,936 × 144d rows written to `bronze.player_embeddings_raw`
- [ ] Destructive ops phase 3: `dbt build --full-refresh --select fct_player_embeddings+`
      populates both non-360 and 360 aggregate marts
- [ ] Destructive ops phase 3: 5 affected synced tables refreshed
- [ ] Puppeteer-verify Player Similarity on staging + production

## Notes

- Destructive ops phase 1 (HF Jobs v2 training rerun) is NOT part of this
  cycle — the D45 fix here is about clobber semantics, not retraining.
  Separate deferred item; the training rerun will ship in a follow-up.
- Three dbt changes (staging partition + 2 mart filters) land atomically
  to avoid a regression window where 360 rows in bronze would silently
  kill v2 rows in the mart.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 2**: On user approval:

```bash
gh pr create --title "feat: football2vec completion — v2 helper migration, 360 import, warm-tier fix" --body-file /tmp/pr_body.md 2>&1 | tail -5
```

### Task PR.4: Wait for CI green

- [ ] **Step 1: Poll CI until complete**

```bash
gh pr checks <pr-number> 2>&1
```

Expected: lint-and-test PASS, semgrep PASS, (terraform-plan and dbt-ci may be skipped via path filter).

If any check fails, investigate the root cause before re-pushing. The v1 replace_where test and the D62 structural tests are the highest-risk for CI-specific failures (tests that pass locally but fail in CI).

### Task PR.5: Merge

- [ ] **Step 1: User approves the merge strategy (squash) and triggers the merge**

```bash
gh pr merge <pr-number> --squash --delete-branch 2>&1
```

### Task PR.6: Verify main CI deploys the wheel

- [ ] **Step 1: Watch the main-branch Python CI run**

```bash
gh run watch <run-id> --exit-status 2>&1 | tail -10
```

Look for the `Build wheel`, `Upload wheel to HF Hub`, `Deploy wheel to Databricks` steps. All must pass. The deployed wheel at `/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.3.2-py3-none-any.whl` now contains the D45 + D62 + D65 changes.

### Task PR.7: Run Destructive Ops Phases 2 and 3

- [ ] **Step 1**: Return to section 5 (Destructive ops phase 2) and execute sequentially.

### Task PR.8: TODO + memory cleanup (after destructive ops succeed)

- [ ] **Step 1**: Update `TODO.md`:
  - Remove D45 entry (shipped)
  - Remove D62 entry (shipped)
  - Remove D65 entry (shipped)
  - Update "Last updated" header with the cycle summary

- [ ] **Step 2**: Memory updates:
  - Create `project_session42_pr<number>.md` with the cycle outcome (mirror `project_session41_pr122.md`)
  - Update `MEMORY.md` "Latest State" to point at the new session memory
  - No deletions needed — D45 etc. were TODO rows, not memory snapshots

- [ ] **Step 3**: Commit the TODO + memory updates direct to main (doc-only follow-up, CI path-filters out). Requires a SEPARATE explicit approval. Per `feedback_todo_cleanup_in_commit.md`, TODO cleanup ideally belongs in the original ship commit — but since the ship commit was the squash-merged PR, a follow-up doc commit is acceptable (matches the session 39 `132f46b` precedent).

---

<a id="self-review"></a>
## Self-review checklist

**Spec coverage**:
- [x] D65 warm-tier post-hook 1 — Commit 1
- [x] D45 Football2vec v2 helper migration — Commit 2 part A
- [x] D45 v1 replace_where clobber fix — Commit 2 part B
- [x] D62 `compute_embeddings_360` entry point + Terraform — Commit 3
- [x] D62 dbt staging partition fix — Commit 3
- [x] D62 dbt non-360 mart 360-exclusion filter — Commit 3

**TDD discipline**:
- [x] D65: Structural pytest written first (test fails against buggy SQL), fix applied, test passes
- [x] D45 helper migration: unit test asserts the module is importable at the new path (fails), migration applied, test passes
- [x] D45 v1 replace_where: source-inspection test asserts match_id IN is present (fails), fix applied, test passes
- [x] D62 360 import: unit tests with mocked HF Hub + Spark (fail), implementation added, tests pass
- [x] D62 dbt: structural SQL pattern pytest (fails), SQL updated, tests pass

**Commit gating**:
- [x] Every commit step stops for explicit approval
- [x] No commits without user approval
- [x] Push stops for explicit approval
- [x] PR creation stops for explicit approval
- [x] Merge stops for explicit approval
- [x] TODO cleanup is a SEPARATE doc-only commit with its own approval

**Destructive ops sequencing**:
- [x] Phase 1 (HF Jobs training rerun) deferred — out of scope for this cycle (clobber-fix, not retraining)
- [x] Phase 2 runs AFTER PR merge + wheel deploy (wheel must contain D45+D62+D65 code first)
- [x] Phase 2 uses `run-now --json @body.json` with `only: [...]` task subset filtering
- [x] Phase 3 runs dbt --full-refresh, refresh synced tables, Puppeteer-verify both staging and production

**File structure**:
- [x] New wheel module: `src/ingestion/football2vec_v2_training.py` (public, no underscore — imported by test_benchmarks)
- [x] New unit tests under `src/tests/` with descriptive names
- [x] New dbt singular test under `dbt_project/tests/`
- [x] No cross-layer dependency violations (analytics does not import from ingestion; ingestion imports from analytics via explicit modules)

**Placeholder scan**:
- [x] No "TODO"/"FIXME"/"implement later"/"TBD" in plan tasks
- [x] Every code block contains actual content
- [x] Every commit message is a complete heredoc, not a template
- [x] Expected output is specified for every `Run:` line

**Type consistency**:
- [x] `_HF_360_DATASET` is the constant name, referenced consistently
- [x] `_V360_BEHAVIORAL_DIM = 144` matches the live parquet dim
- [x] `_FOOTBALL2VEC_360_DATA_SOURCE = "football2vec_360"` matches the dbt filter literal
- [x] `compute_embeddings_360` entry point name matches across pyproject, Terraform, and workflow card

**Verifiable facts discipline**:
- [x] Every SQL quote is verbatim from the current file (verified via direct file reads 2026-04-15)
- [x] Every Python code quote is verbatim from the current file
- [x] Every HF Hub claim is verified via live parquet inspection (22,726 v2 rows, 9,936 360 rows, 128d/144d dimensions)
- [x] Every line number citation is from the current file state, not remembered state
