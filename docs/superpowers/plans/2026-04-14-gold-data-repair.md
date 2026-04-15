# Data Integrity Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **User rule override**: The user has a durable rule "no commits without explicit approval". Every commit step, every destructive operation, and every PR creation step **stops for explicit user approval**. "Approved, proceed" on the plan does NOT grant any of those. Each requires its own explicit approval.

**Goal:** Fix data integrity before touching any downstream work. Three connected repair items in one cycle:

1. **D57 SPADL goal encoding** — narrow `replace_where` predicates + Stage 2 staleness fix + UDF silent-swallow removal (was already the goal of the `gold-data-repair` branch; scope expanded to cover the root causes found in session 40).
2. **Warm-tier hook schema drift** — `CostEstimateHook` has been silently failing for 62+ hours because `DeltaTable.merge().whenMatchedUpdateAll()` can't resolve an orphaned `task_key` column in the live table. Cost tracking + the `assert_warm_tier_not_empty` dbt test are both broken as a result.
3. **Systemic silent-swallow audit** — the recurring anti-pattern that caused both the VAEP scoring bug (fixed earlier in session 40) and the cost-hook bug needs to be eliminated repo-wide before it causes a third blocker.

**Deferred to a follow-up cycle**: D45 (Football2vec v2 StatsBomb coverage + v1 full deletion). User directive: downstream work waits until data integrity is secured.

**Architecture:** One PR, 4 code commits, 2 destructive-ops phases, 1 plan/spec/memory update commit. Each commit maps to a standalone concern that can be reviewed independently. Destructive operations are strictly post-commit.

**Tech Stack:** Python 3.10, Databricks serverless (pyspark + Delta Lake), silly-kicks 1.0.1 (SPADL/VAEP library), dbt, pytest.

**Reference spec:** `docs/superpowers/specs/2026-04-14-gold-data-repair-design.md` — the original spec is **out of date** (blamed `_flatten_extra()`); rewrite §4.1 as part of this plan.

**Reference memory files** (must be read before execution):
- `project_spadl_vaep_chain.md` — guard staleness + exception-swallow analysis
- `project_warm_tier_blocker.md` — warm-tier blocker background
- `feedback_proxies_not_verification.md` — evidence-before-claims discipline
- `feedback_no_commits_without_approval.md` — commit approval discipline

---

## Table of contents

1. [Pre-work — re-sync with branch state](#pre-work)
2. [Commit 1 — SPADL integrity (D57 + UDF swallows)](#commit-1)
3. [Commit 2 — Warm-tier hook schema drift fix](#commit-2)
4. [Destructive ops phase 1 — ALTER + seed warm tier](#destructive-ops-phase-1)
5. [Commit 3 — Systemic `src/` silent-swallow remediation](#commit-3)
6. [Commit 4 — `scripts/` + `hf_taipy_app/` swallow remediation](#commit-4)
7. [Commit 5 — Memory entry + spec rewrite + TODO updates](#commit-5)
8. [Destructive ops phase 2 — Stage 2 re-run + dbt full-refresh](#destructive-ops-phase-2)
9. [mad-scientist-skills audit anti-pattern updates (sibling repo)](#mad-scientist-skills)
10. [PR creation](#pr-creation)
11. [Self-review checklist](#self-review)

---

<a id="pre-work"></a>
## Pre-work — re-sync with branch state

### Task 0: Verify branch state and review in-flight work

Branch `feat/gold-data-repair` already has **uncommitted changes** from earlier in session 40. Verify they're intact before adding to them.

- [ ] **Step 1: Confirm branch + working tree**

```bash
git status --short
git branch --show-current
```

Expected: branch `feat/gold-data-repair`. Modified files: `TODO.md`, `src/ingestion/spadl_conversion.py`, `src/ingestion/spadl_vaep.py`, `src/tests/test_spadl_vaep.py`. Untracked files: `docs/superpowers/plans/2026-04-14-gold-data-repair.md` (this file, being rewritten), `docs/superpowers/specs/2026-04-14-gold-data-repair-design.md`, `src/tests/test_spadl_conversion.py`, plus several `tmp_investigate_*.py` + `tmp_investigate_*_output.txt` files from the session 40 investigation.

- [ ] **Step 2: Confirm the in-flight Commit 1 code is still good**

```bash
uv run ruff check src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
uv run ruff format --check src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
uv run pyright src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py
uv run pytest src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py -v
```

Expected: ruff clean, format clean, pyright 0 errors, **20/20 tests pass**.

If anything fails, investigate before adding new code on top.

- [ ] **Step 3: Review the 3 code changes already in the working tree**

```bash
git diff HEAD src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py
```

Confirm the three changes are:
1. `spadl_conversion.py` — `_make_statsbomb_replace_where` + `_make_wyscout_replace_where` helper functions + narrow replace_where wiring in both converters.
2. `spadl_vaep.py:_VaepGuard.check()` — `unscored_vaep_match_ids` computed as `sorted(set(new_spadl) | set(unscored))` (guard metadata union, fixing the staleness bug).
3. `spadl_vaep.py:_make_scoring_udf` — `except Exception: pass` replaced with `raise RuntimeError(f"VAEP scoring failed for game_id={game_id}") from exc`.

And `src/tests/test_spadl_conversion.py` + new test classes in `src/tests/test_spadl_vaep.py` (`TestVaepGuardMetadata`, `TestScoringUdfErrorPropagation`).

**If any of these are missing, the session crash lost work. Stop and reconstruct from session 40 memory notes.**

- [ ] **Step 4: Delete session 40 investigation scratch files**

```bash
ls tmp_investigate_*.py tmp_investigate_*_output.txt tmp_task_log*.txt tmp_task_log*.py 2>&1
```

Confirm which tmp files exist. Do **not** commit any of them. They will be deleted at the end of this cycle in a cleanup step (Task 22).

- [ ] **Step 5: Verify live Databricks credentials work**

```bash
uv run python -c "import os; from pathlib import Path; [os.environ.setdefault(*l.strip().split('=',1)) for l in Path('hf_taipy_app/.env').read_text().splitlines() if '=' in l and not l.strip().startswith('#')]; from databricks.sdk import WorkspaceClient; w = WorkspaceClient(); print(w.config.host)"
```

Expected: prints the Databricks workspace host. Used later for destructive ops.

---

<a id="commit-1"></a>
## Commit 1 — SPADL integrity (D57 + UDF swallows)

**Scope:** Extend the in-flight SPADL work with **three additional UDF silent-swallow removals** found during the session 40 audit. Each of these is structurally identical to the VAEP scoring UDF bug — `except Exception: return empty_df` inside a Spark `applyInPandas` closure, silently losing per-group data.

### Task 1: Fix StatsBomb SPADL converter UDF silent swallow

**File:** `src/ingestion/spadl_conversion.py:141-145`

**Current code** (the swallow):
```python
try:
    adapted = _adapt(pdf, home_team_id)
    actions, _report = _spadl_sb.convert_to_actions(adapted, home_team_id)
except Exception:
    return _pd.DataFrame(columns=_spadl_cols)
```

**Fix:**
```python
try:
    adapted = _adapt(pdf, home_team_id)
    actions, _report = _spadl_sb.convert_to_actions(adapted, home_team_id)
except Exception as exc:
    msg = f"StatsBomb SPADL conversion failed for match_id={match_id}"
    raise RuntimeError(msg) from exc
```

**Rationale:** This UDF runs inside `applyInPandas` grouped by `match_id`. A silent empty return silently drops all of a match's actions from `bronze.spadl_actions`. We suspect this has been hiding real silly-kicks conversion failures for weeks — possibly the actual root cause of the D57 symptom the cycle started with. Hard-fail-first per user decision on plan direction.

- [ ] **Step 1: Apply the edit** via the `Edit` tool. The `match_id` variable is already bound above line 141 — no new binding needed.

- [ ] **Step 2: Write a regression test** in `src/tests/test_spadl_conversion.py`:

```python
class TestStatsBombConverterPropagation:
    """Regression guard for the 2026-04-14 silent-swallow removal.

    If the StatsBomb UDF's silly-kicks call fails for any reason, the error
    must propagate with match_id context so Spark surfaces it on the driver.
    """

    def test_statsbomb_udf_raises_runtime_error_with_match_id(self) -> None:
        from unittest.mock import patch
        import pandas as pd
        from ingestion.spadl_conversion import _make_statsbomb_udf

        pdf = pd.DataFrame({
            "home_team_id": [999],
            "match_id": [12345],
            "competition_id": [2],
            "season_id": [44],
            # Minimal event rows for the silly-kicks adapter input shape.
            "event_id": ["evt1", "evt2"],
            "type": [{"name": "Pass"}, {"name": "Shot"}],
            "timestamp": ["00:00:00.000", "00:00:01.000"],
            "period": [1, 1],
            "team": [{"id": 999, "name": "Test"}, {"id": 999, "name": "Test"}],
            "player": [{"id": 1, "name": "A"}, {"id": 1, "name": "A"}],
            "location": [[10.0, 20.0], [50.0, 30.0]],
        })

        udf = _make_statsbomb_udf(...)  # Build with test dependencies
        with patch("ingestion.spadl_conversion._spadl_sb.convert_to_actions",
                   side_effect=KeyError("simulated silly_kicks failure")):
            with pytest.raises(RuntimeError, match=r"StatsBomb SPADL conversion failed for match_id=12345"):
                udf(pdf)
```

**NOTE:** `_make_statsbomb_udf` may not be the exact factory name. Verify by reading the file around line 120 to find the closure factory. The test must exercise the factory path, not the UDF directly, so the closure captures the mock patches.

- [ ] **Step 3: Run the test, verify it fails against the pre-fix code, then passes against the post-fix code.**

```bash
uv run pytest src/tests/test_spadl_conversion.py::TestStatsBombConverterPropagation -v
```

### Task 2: Fix Wyscout SPADL converter UDF silent swallow

**File:** `src/ingestion/spadl_conversion.py:340-344`

**Current code:**
```python
try:
    adapted = _adapt(pdf)
    actions, _report = _spadl_ws.convert_to_actions(adapted, home_team_id, goalkeeper_ids=_gk_ids)
except Exception:
    return _pd.DataFrame(columns=_spadl_cols)
```

**Fix:**
```python
try:
    adapted = _adapt(pdf)
    actions, _report = _spadl_ws.convert_to_actions(adapted, home_team_id, goalkeeper_ids=_gk_ids)
except Exception as exc:
    msg = f"Wyscout SPADL conversion failed for match_id={match_id}"
    raise RuntimeError(msg) from exc
```

- [ ] **Step 1: Apply the edit.**
- [ ] **Step 2: Add `TestWyscoutConverterPropagation` test class** mirroring the StatsBomb test above.
- [ ] **Step 3: Run tests.**

### Task 3: Fix VAEP feature extraction silent swallow

**File:** `src/ingestion/vaep_training.py:69-78`

**Current code:**
```python
for game_id in game_ids:
    game_actions = _game_groups.get(game_id, pd.DataFrame()).reset_index(drop=True)
    if len(game_actions) < 2:
        continue
    try:
        gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)
        x_game = pd.concat([fn(gamestates) for fn in _get_feature_fns()], axis=1)
        y_scores = labels.scores(game_actions, nr_actions=10)
        y_concedes = labels.concedes(game_actions, nr_actions=10)
        all_x.append(x_game)
        all_y_scores.append(y_scores)
        all_y_concedes.append(y_concedes)
    except Exception:
        _log.exception("Failed feature extraction for game %s", game_id)
```

**Fix:**
```python
for game_id in game_ids:
    game_actions = _game_groups.get(game_id, pd.DataFrame()).reset_index(drop=True)
    if len(game_actions) < 2:
        continue
    try:
        gamestates = fs.gamestates(game_actions, nb_prev_actions=_NB_PREV_ACTIONS)
        x_game = pd.concat([fn(gamestates) for fn in _get_feature_fns()], axis=1)
        y_scores = labels.scores(game_actions, nr_actions=10)
        y_concedes = labels.concedes(game_actions, nr_actions=10)
        all_x.append(x_game)
        all_y_scores.append(y_scores)
        all_y_concedes.append(y_concedes)
    except Exception as exc:
        msg = f"VAEP feature extraction failed for game_id={game_id}"
        raise RuntimeError(msg) from exc
```

- [ ] **Step 1: Apply the edit.**
- [ ] **Step 2: Add a regression test** in `src/tests/test_spadl_vaep.py` — extend the existing `TestScoringUdfErrorPropagation` class or add a new `TestVaepTrainingFeatureExtraction` class.
- [ ] **Step 3: Run tests.**

### Task 4: Full quality gate on Commit 1

- [ ] **Step 1: Ruff + format + pyright + pytest**

```bash
uv run ruff check src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/vaep_training.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
uv run ruff format --check src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/vaep_training.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
uv run pyright src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/vaep_training.py
uv run pytest src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py -v
```

Expected: all green, ≥25 tests pass.

### Task 5: STOP — present Commit 1 diff for approval

- [ ] **Step 1: Show the user the full diff:**

```bash
git diff HEAD src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/vaep_training.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
```

- [ ] **Step 2: Report quality gate results + test count + intended commit message.**
- [ ] **Step 3: WAIT for explicit user approval before running `git commit`.**

### Task 6: Create Commit 1

Only after approval:

- [ ] **Step 1: Stage and commit**

```bash
git add src/ingestion/spadl_conversion.py src/ingestion/spadl_vaep.py src/ingestion/vaep_training.py src/tests/test_spadl_conversion.py src/tests/test_spadl_vaep.py
git commit -m "$(cat <<'EOF'
fix(spadl): D57 goal encoding + UDF silent-swallow removals

D57 root cause (revised): `_VaepGuard.check()` computed
`unscored_vaep_match_ids` from `find_new_ids(spadl, vaep)` BEFORE Stage 1
runs, so when Stage 1 repopulated statsbomb in the same run, Stage 2
skipped it with stale metadata. Fix: union `new_spadl ∪ unscored` in the
guard metadata (disjoint sets by construction).

Narrow `replace_where` predicates in `spadl_conversion.py` so incremental
runs only overwrite the specific match_ids the UDF just produced, not the
whole provider partition.

Remove 3 `except Exception: return empty_df` UDF silent swallows that
were hiding per-group data loss in Spark applyInPandas closures:
- `_make_scoring_udf` (spadl_vaep.py) — VAEP scoring per game
- StatsBomb converter UDF (spadl_conversion.py) — per-match SPADL
- Wyscout converter UDF (spadl_conversion.py) — per-match SPADL
Plus the VAEP feature extraction loop in `vaep_training.py`.

All four now raise RuntimeError with match_id/game_id context so Spark
propagates errors to the driver instead of silently dropping rows.

Regression tests for every change. Part of the session 40
silent-swallow-anti-pattern cleanup (memory entry to follow in
separate commit).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 2: `git status` after commit to verify.**

---

<a id="commit-2"></a>
## Commit 2 — Warm-tier hook schema drift fix

**Scope:** Resolve the `DELTA_MERGE_UNRESOLVED_EXPRESSION` that has been silently failing every `CostEstimateHook._merge()` call since 2026-04-12T02:43Z. Bump log levels so future hook failures are visible. Align `sync_hf_costs.py` to the canonical 16-col schema. Add two regression tests (schema drift guard + integration). Delete the dead `scripts/sync_hf_costs.py` copy.

**NOTE:** The actual `ALTER TABLE DROP COLUMN task_key` is a destructive op and is **NOT** part of the commit. See [Destructive ops phase 1](#destructive-ops-phase-1) for that step. Commit 2 makes the CODE match what the live table should look like; the destructive op makes the LIVE table match the code.

### Task 7: Bump cost_hook.py log levels warning → error

**File:** `src/ingestion/cost_hook.py`

Four sites, all structurally identical:

```python
# on_start (line ~90)
except Exception:
    logger.warning("CostEstimateHook.on_start failed for run_id=%s", ctx.run_id, exc_info=True)

# on_complete (line ~110), on_skip (line ~133), on_error (line ~153)
# ... same pattern
```

Change each `logger.warning` to `logger.error`. Still fire-and-forget; still doesn't crash pipelines. But ERROR-level logs are visible in standard error-log queries, warnings are not.

- [ ] **Step 1: 4× `Edit` calls** on `cost_hook.py:90`, `:110`, `:133`, `:153`.

### Task 8: Align `src/ingestion/sync_hf_costs.py` to canonical 16-col schema

**File:** `src/ingestion/sync_hf_costs.py:152-171`

**Current `map_to_delta_schema`:**
```python
def map_to_delta_schema(cost_data: dict[str, Any], task_key: str) -> dict[str, Any]:
    hf_job_id = cost_data.get("hf_job_id")
    return {
        "workflow_id": cost_data.get("workflow_id"),
        "phase": cost_data.get("phase"),
        "run_id": f"hf-{hf_job_id}" if hf_job_id else None,
        "runtime": "hf_jobs",
        "task_key": task_key,                # REMOVE — orphan column
        "hf_job_id": hf_job_id,
        "state": cost_data.get("state"),
        "started_at": cost_data.get("started_at"),
        "ended_at": cost_data.get("ended_at"),
        "duration_seconds": cost_data.get("duration_seconds"),
        "row_count": cost_data.get("row_count"),
        "rate_usd_per_hour": cost_data.get("rate_usd_per_hour"),
        "estimated_cost_usd": cost_data.get("estimated_cost_usd"),
        "cost_source": "hf_hub_sync",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        # MISSING: entity_count, guard_duration_seconds
    }
```

**Fix:**
```python
def map_to_delta_schema(cost_data: dict[str, Any], task_key: str) -> dict[str, Any]:
    """Map HF Jobs cost JSON to workflow_cost_live Delta schema (16 cols).

    task_key is accepted as a parameter for backward compatibility (callers
    still pass it from workflow cards) but NOT written — the workflow_cost_live
    table does not have a task_key column.
    """
    _ = task_key  # Intentionally unused; see docstring.
    hf_job_id = cost_data.get("hf_job_id")
    return {
        "workflow_id": cost_data.get("workflow_id"),
        "phase": cost_data.get("phase"),
        "run_id": f"hf-{hf_job_id}" if hf_job_id else None,
        "runtime": "hf_jobs",
        "hf_job_id": hf_job_id,
        "state": cost_data.get("state"),
        "started_at": cost_data.get("started_at"),
        "ended_at": cost_data.get("ended_at"),
        "duration_seconds": cost_data.get("duration_seconds"),
        "row_count": cost_data.get("row_count"),
        "entity_count": None,                   # NEW — HF Jobs have no entity_count
        "guard_duration_seconds": None,         # NEW — HF Jobs have no Databricks guard
        "rate_usd_per_hour": cost_data.get("rate_usd_per_hour"),
        "estimated_cost_usd": cost_data.get("estimated_cost_usd"),
        "cost_source": "hf_hub_sync",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
```

- [ ] **Step 1: Apply the edit.** Docstring explains why task_key param is retained but unused.

### Task 9: Delete dead `scripts/sync_hf_costs.py`

The stale copy is not invoked anywhere (verified — `pyproject.toml:97` entry point is `ingestion.sync_hf_costs:main`, which resolves to the `src/` module). It has a pre-PR-#115 schema and adds confusion.

- [ ] **Step 1: Remove the file:**

```bash
rm scripts/sync_hf_costs.py
```

- [ ] **Step 2: Confirm no references** anywhere:

```bash
grep -rn "scripts/sync_hf_costs\|scripts.sync_hf_costs" . --exclude-dir=.git --exclude-dir=node_modules --exclude-dir=dbt_packages
```

Expected: no hits outside the plan/spec docs (which reference the file by name for historical context).

### Task 10: Bump workflows/runner.py hook dispatcher log level warning → error

**File:** `src/workflows/runner.py:30-32`

**Current:**
```python
except Exception:
    _logger.warning(
        "Hook %s.%s failed — continuing pipeline execution",
        ...
    )
```

Change to `_logger.error`. This is the runner-side counterpart to the hook-side swallow — both layers must be loud to surface hook failures in error-log queries.

- [ ] **Step 1: Apply the edit.**

### Task 11: Add schema-drift guard test

**File:** `src/tests/test_cost_hook.py` — extend.

The test parses `scripts/create_cost_table.sql`, extracts the column list, and asserts that `CostEstimateHook._merge`'s `StructType` matches it exactly.

```python
class TestCostHookSchemaDriftGuard:
    """Parse create_cost_table.sql and assert the hook's StructType matches.

    Regression guard for the 2026-04-12 warm-tier blocker: PR #115 removed
    task_key + job_run_id from create_cost_table.sql but the live table was
    only partially migrated. The code path and the canonical SQL diverged.
    This test prevents the two sources of truth from drifting again.
    """

    def test_hook_struct_type_matches_canonical_sql(self) -> None:
        import re
        from pathlib import Path
        from ingestion.cost_hook import CostEstimateHook

        sql = Path("scripts/create_cost_table.sql").read_text()
        # Extract the column list from CREATE TABLE ... ( ... )
        match = re.search(
            r"CREATE TABLE[^(]*\(\s*(.*?)\s*\)\s*USING",
            sql, re.DOTALL | re.IGNORECASE,
        )
        assert match, "Could not find CREATE TABLE block in create_cost_table.sql"
        columns_block = match.group(1)
        # Extract column names: first token on each non-empty line
        canonical_cols: list[str] = []
        for line in columns_block.splitlines():
            line = line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            name = line.split()[0].strip()
            canonical_cols.append(name)

        # Build a fake hook and introspect its _merge method's schema by
        # calling createDataFrame through a mock. Simpler: expose the
        # schema as a module-level constant in cost_hook.py (new constant),
        # reference it here.
        from ingestion.cost_hook import _COST_LIVE_SCHEMA
        hook_cols = [f.name for f in _COST_LIVE_SCHEMA.fields]

        assert sorted(hook_cols) == sorted(canonical_cols), (
            f"Schema drift between create_cost_table.sql and cost_hook._COST_LIVE_SCHEMA.\n"
            f"SQL cols: {sorted(canonical_cols)}\n"
            f"Hook cols: {sorted(hook_cols)}\n"
            f"In SQL but not hook: {set(canonical_cols) - set(hook_cols)}\n"
            f"In hook but not SQL: {set(hook_cols) - set(canonical_cols)}"
        )
```

**Requires code change:** `src/ingestion/cost_hook.py` — extract the `StructType([...])` literal in `_merge` into a module-level constant `_COST_LIVE_SCHEMA`, then reference it inside `_merge`. This is a small refactor needed so the test can introspect the schema without constructing a hook.

- [ ] **Step 1: Refactor `cost_hook.py`:**
  - Add `_COST_LIVE_SCHEMA: StructType = StructType([...])` at module level (above the class).
  - Change `_merge` to use `schema=_COST_LIVE_SCHEMA` instead of constructing a new literal.
- [ ] **Step 2: Add the test class above to `test_cost_hook.py`.**
- [ ] **Step 3: Run the test.** It should PASS (the schema already matches; this is a guard against future drift).

### Task 12: Add integration test for CostEstimateHook against a temp Delta table

This test is a Databricks-environment-dependent test. It runs only if `SPARK_HOME` or local Spark is available. Skip otherwise (but CI running on Databricks should exercise it).

**File:** `src/tests/test_cost_hook.py` — extend.

```python
class TestCostHookIntegration:
    """Integration test against a real Delta table.

    This test requires Spark + Delta Lake available locally. Skipped otherwise.
    Catches schema drift at the MERGE level, not just at the StructType level —
    the real DELTA_MERGE_UNRESOLVED_EXPRESSION failure that caused the 2026-04-12
    warm-tier blocker would be caught here.
    """

    @pytest.fixture
    def spark(self):
        try:
            from pyspark.sql import SparkSession
            spark = (SparkSession.builder
                     .appName("test_cost_hook_integration")
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .master("local[1]")
                     .getOrCreate())
            yield spark
            spark.stop()
        except Exception:
            pytest.skip("Local Spark/Delta not available")

    def test_on_start_then_on_complete_writes_to_temp_table(self, spark, tmp_path) -> None:
        import re
        from pathlib import Path
        from ingestion.cost_hook import CostEstimateHook, _COST_LIVE_SCHEMA
        from workflows.context import WorkflowContext

        # Create temp Delta table with canonical schema
        table_name = "test_cost_live"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS test_obs")
        spark.sql(f"DROP TABLE IF EXISTS test_obs.{table_name}")
        # Build CREATE TABLE from the same SQL file the schema-drift guard uses
        sql = Path("scripts/create_cost_table.sql").read_text()
        create_ddl = sql.replace("{catalog}.observability", "test_obs").replace(
            "workflow_cost_live", table_name
        ).replace("CREATE TABLE IF NOT EXISTS", "CREATE TABLE")
        spark.sql(create_ddl)

        hook = CostEstimateHook(spark, "test_obs", "unused_schema")
        hook._table = f"test_obs.{table_name}"  # Override for test

        ctx = WorkflowContext(
            workflow_id="wf-test", phase="ingest", run_id="test-run-1",
            started_at=datetime.now(timezone.utc),
            entity_count=10, guard_duration_seconds=5,
        )

        # Fire on_start — should INSERT one row
        hook.on_start(ctx)
        count_after_start = spark.sql(f"SELECT COUNT(*) FROM test_obs.{table_name}").collect()[0][0]
        assert count_after_start == 1

        # Fire on_complete — should UPDATE the same row (same run_id)
        hook.on_complete(ctx, row_count=100)
        rows = spark.sql(f"SELECT state, row_count FROM test_obs.{table_name}").collect()
        assert len(rows) == 1
        assert rows[0]["state"] == "COMPLETED"
        assert rows[0]["row_count"] == 100
```

- [ ] **Step 1: Add the test class.**
- [ ] **Step 2: Run it locally if Spark is available. Will skip in CI until Spark is wired up.**
- [ ] **Step 3: Document the skip in the commit message** so the user knows the integration test exists but may be skipped in some environments.

### Task 13: Full quality gate on Commit 2

- [ ] **Step 1:**

```bash
uv run ruff check src/ingestion/cost_hook.py src/ingestion/sync_hf_costs.py src/workflows/runner.py src/tests/test_cost_hook.py
uv run ruff format --check src/ingestion/cost_hook.py src/ingestion/sync_hf_costs.py src/workflows/runner.py src/tests/test_cost_hook.py
uv run pyright src/ingestion/cost_hook.py src/ingestion/sync_hf_costs.py src/workflows/runner.py
uv run pytest src/tests/test_cost_hook.py -v
```

Expected: all green; schema-drift guard test passes; integration test either passes (local Spark available) or skips cleanly.

### Task 14: STOP — present Commit 2 diff for approval

- [ ] **Step 1: Show full diff.**
- [ ] **Step 2: WAIT for explicit user approval before `git commit`.**

### Task 15: Create Commit 2

```bash
git add src/ingestion/cost_hook.py src/ingestion/sync_hf_costs.py src/workflows/runner.py src/tests/test_cost_hook.py
git rm scripts/sync_hf_costs.py
git commit -m "$(cat <<'EOF'
fix(cost-hook): warm-tier hook schema drift + observability

CostEstimateHook has been silently failing every MERGE since
2026-04-12T02:43Z due to DELTA_MERGE_UNRESOLVED_EXPRESSION on the
orphaned `task_key` column. PR #115 removed task_key from the canonical
create_cost_table.sql + cost_hook.py, but the live observability Delta
table still had the column. whenMatchedUpdateAll() requires every target
column to have a same-named source column; the resolver fails at parse
time for every call, even the first on_start insert.

Caught every hook's try/except logger.warning catch block at
WARNING level, invisible in error-log queries. Blocker surfaced only
when the D59 cycle wired dbt_build into the daily job — the
assert_warm_tier_not_empty test finally started firing in production.

Fixes:
- Bump cost_hook.py + workflows/runner.py hook dispatcher log levels
  from warning → error so the next drift is visible immediately
- Align src/ingestion/sync_hf_costs.py map_to_delta_schema to the
  16-col canonical schema (remove task_key, add entity_count + None
  and guard_duration_seconds + None for HF Jobs runs)
- Extract cost_hook._COST_LIVE_SCHEMA as a module-level constant so
  the schema-drift guard test can introspect it
- Delete scripts/sync_hf_costs.py (dead stale copy, not referenced
  anywhere — pyproject entry point is ingestion.sync_hf_costs:main)

Tests:
- TestCostHookSchemaDriftGuard — parses create_cost_table.sql, asserts
  the hook's StructType column names match exactly. Prevents future
  drift.
- TestCostHookIntegration — real Delta table, on_start + on_complete
  round trip. Skipped when local Spark unavailable; runs in Databricks
  CI.

DOES NOT drop the task_key column from the live table. That's a
destructive operation requiring separate approval (see plan
Task 16-18). DOES NOT seed the warm tier; next daily job will after
this commit ships.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

<a id="destructive-ops-phase-1"></a>
## Destructive ops phase 1 — ALTER + seed warm tier

**Prerequisites:** Commit 2 must be committed. Wheel must be built and deployed (steps below).

**Caution:** Two destructive operations here. Each requires its own explicit user approval.

### Task 16: STOP — present destructive-ops phase 1 plan for approval

- [ ] **Step 1: Describe to the user what's about to happen:**

  1. `ALTER TABLE soccer_analytics.observability.workflow_cost_live DROP COLUMN task_key` — metadata-only column drop (column mapping mode is already enabled at Delta v2736).
  2. Build the luxury-lakehouse wheel with the Commit 2 code changes (`sync_hf_costs.py` schema fix + cost_hook log levels).
  3. Deploy the new wheel to the UC Volume.
  4. Trigger ONE run of the `hf_sync` task on the daily Databricks workflow (manual task run via Jobs API). This is the lightest-weight pipeline that runs under the cost hook, so its successful MERGE write seeds the warm tier with one non-RUNNING row.
  5. Verify the seed: query `workflow_cost_live` for row count + latest state; query Delta history for a fresh MERGE op; query the task's stderr to confirm no hook warnings.

- [ ] **Step 2: Confirm**: the destructive ops are reversible. Column re-add is possible via `ALTER TABLE ADD COLUMN task_key STRING`. Test pipeline run is a no-op for data — only cost-tracking metadata is written.

- [ ] **Step 3: WAIT for explicit user approval.**

### Task 17: ALTER TABLE DROP COLUMN task_key

- [ ] **Step 1: Run the ALTER via a tmp script (create, run, delete):**

Create `tmp_drop_task_key.py`:
```python
import os
from pathlib import Path

hf_env = Path("hf_taipy_app/.env")
for line in hf_env.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.strip().split("=", 1)
        os.environ.setdefault(k, v.strip().strip('"').strip("'"))

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
wh_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").rsplit("/", 1)[-1]

resp = w.statement_execution.execute_statement(
    statement="ALTER TABLE soccer_analytics.observability.workflow_cost_live DROP COLUMN task_key",
    warehouse_id=wh_id,
    wait_timeout="50s",
)
print(f"State: {resp.status.state.value}")
if resp.status.error:
    print(f"Error: {resp.status.error}")
```

```bash
uv run python tmp_drop_task_key.py
```

Expected: `State: SUCCEEDED`.

- [ ] **Step 2: Verify the column is gone:**

```python
# Re-use the same pattern to run:
# SELECT column_name FROM soccer_analytics.information_schema.columns WHERE table_schema = 'observability' AND table_name = 'workflow_cost_live' ORDER BY ordinal_position
# Expect: no task_key in the list
```

- [ ] **Step 3: Delete `tmp_drop_task_key.py`.**

### Task 18: Build + deploy wheel with Commit 2

- [ ] **Step 1: Build the wheel:**

```bash
uv build --wheel 2>&1 | tail -20
```

Expected: wheel file created under `dist/`.

- [ ] **Step 2: Deploy the wheel to UC Volume** via whatever mechanism the project uses. Check `scripts/deploy_wheel.py` or the project's standard wheel deployment flow:

```bash
grep -l "deploy_wheel\|publish_wheel\|upload_wheel" scripts/ pyproject.toml 2>&1
```

Reference the appropriate script. If the project's convention is "wheel is bumped via `scripts/bump_wheel.py` and CI publishes it on main merge", then this manual deploy step may require a custom path — discuss with user before proceeding.

- [ ] **Step 3: WAIT for user approval** if the deployment path is non-obvious.

### Task 19: Trigger one `hf_sync` task run to seed warm tier

- [ ] **Step 1: Trigger via Databricks Jobs API:**

```python
# Create tmp_trigger_hf_sync.py:
import os
from pathlib import Path
for line in Path("hf_taipy_app/.env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.strip().split("=", 1)
        os.environ.setdefault(k, v.strip().strip('"').strip("'"))

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Look up job ID and task_key from Terraform state or the live workspace
# The daily job's name is something like "luxury-lakehouse-daily-dev"
jobs = list(w.jobs.list())
daily_job = next(j for j in jobs if "daily" in (j.settings.name or "").lower())
print(f"Job: {daily_job.settings.name} (id={daily_job.job_id})")

# Trigger a run_now with the hf_sync task only
# (API supports task filtering via run_now's task_key param — verify against SDK version)
resp = w.jobs.run_now(job_id=daily_job.job_id)
print(f"Triggered run: {resp.run_id}")
```

**Caveat:** `run_now` may trigger the ENTIRE job, not just the `hf_sync` task. If that's the case, either:
- Accept the full daily-job run (will complete normally; we want that anyway)
- Or manually invoke just the task via the UI

Discuss with user before triggering. **WAIT for approval if it's not obvious how to trigger a single task.**

- [ ] **Step 2: Wait for the run to complete** (should take 2-10 minutes for a full daily job, <1 minute for just hf_sync).

- [ ] **Step 3: Delete `tmp_trigger_hf_sync.py`.**

### Task 20: Verify warm-tier seed

Run the verification queries — if any fails, STOP and investigate:

- [ ] **Step 1: Workflow_cost_live row count > 0:**

```sql
SELECT COUNT(*) FROM soccer_analytics.observability.workflow_cost_live
```
Expected: ≥1.

- [ ] **Step 2: At least one non-RUNNING row:**

```sql
SELECT COUNT(*) FROM soccer_analytics.observability.workflow_cost_live WHERE state != 'RUNNING'
```
Expected: ≥1.

- [ ] **Step 3: Delta history has a fresh MERGE (not just DELETE):**

```sql
DESCRIBE HISTORY soccer_analytics.observability.workflow_cost_live
```
Look at the top entry — should be a MERGE by ingestion SP within the last 10 minutes.

- [ ] **Step 4: Task stderr clean** — pull the task log via the same `jobs.get_run_output` pattern used in the session 40 investigation (see `tmp_investigate_task_log.py`). Grep for `CostEstimateHook` and confirm there are NO warnings/errors from the hook.

- [ ] **Step 5: Report results to user.** If all four pass, Commit 2's fix is empirically verified end-to-end.

---

<a id="commit-3"></a>
## Commit 3 — Systemic `src/` silent-swallow remediation

**Scope:** Apply the "no silent swallow" rule across `src/`. Based on the session 40 audit of 55 hits in `src/`:

- **~20 Category B hits** (bootstrap "table doesn't exist") → narrow to `_tolerate_missing_table` helper.
- **~6 Category E hits** in `model_validation.py` → narrow to specific exception + bump log to ERROR.
- **~5 Category F hits** → raise, typed fallback with metadata flag, or specific exception type.
- **Enable BLE001** in ruff with documented per-file-ignores.

### Task 21: Add `_tolerate_missing_table` helper

**File:** `src/ingestion/utils.py`

```python
from contextlib import contextmanager
import logging
from typing import Iterator

@contextmanager
def tolerate_missing_table(logger: logging.Logger, msg: str) -> Iterator[None]:
    """Context manager that silences only the Spark 'TABLE_OR_VIEW_NOT_FOUND' error.

    Use this in guard / bootstrap code that queries a results table which may
    not exist on first run. Any OTHER exception propagates — including the
    hook-breaking DELTA_MERGE_UNRESOLVED_EXPRESSION that silent `except Exception:`
    patterns used to hide.

    Example:
        with tolerate_missing_table(logger, "No existing X table — starting fresh"):
            existing = spark.read.table(table).collect()
    """
    try:
        yield
    except Exception as exc:  # noqa: BLE001 — we filter by message
        # Import here — pyspark is optional at install time but always present at runtime.
        msg_str = str(exc)
        if any(marker in msg_str for marker in (
            "TABLE_OR_VIEW_NOT_FOUND",
            "Path does not exist",
            "[TABLE_OR_VIEW_NOT_FOUND]",
        )):
            logger.info(msg)
            return
        raise
```

- [ ] **Step 1: Add the function to `utils.py`.**
- [ ] **Step 2: Add a unit test** in `src/tests/test_utils.py` covering: (a) context manager suppresses TABLE_OR_VIEW_NOT_FOUND and calls logger.info, (b) context manager re-raises any other exception.

### Task 22: Rewrite Category B "table doesn't exist" sites

Mechanical rewrites across ~20 files. Each follows the pattern:

```python
# BEFORE
try:
    existing = spark.read.table(full_table).collect()
except Exception:
    logger.info("No existing X table — starting fresh")
    return set()

# AFTER
from ingestion.utils import tolerate_missing_table
existing: set[int] = set()
with tolerate_missing_table(logger, "No existing X table — starting fresh"):
    existing = {row[0] for row in spark.read.table(full_table).collect()}
return existing
```

Files to rewrite (confirm list before starting; may differ from the session 40 audit if working tree has moved):

- [ ] `src/ingestion/statsbomb.py:172`
- [ ] `src/ingestion/statsbomb.py:247`
- [ ] `src/ingestion/statsbomb_backfill_360.py:43`
- [ ] `src/ingestion/statsbomb_backfill_360.py:59`
- [ ] `src/ingestion/statsbomb_backfill_extra.py:46`
- [ ] `src/ingestion/spadl_conversion.py:82`
- [ ] `src/ingestion/idsse.py:329`
- [ ] `src/ingestion/metrica_tracking.py:303`
- [ ] `src/ingestion/metrica_events.py:112`
- [ ] `src/ingestion/skillcorner.py:202`
- [ ] `src/ingestion/wyscout.py:338`
- [ ] `src/ingestion/wyscout.py:408`
- [ ] `src/ingestion/wyscout.py:503`
- [ ] `src/ingestion/defcon_lite_common.py:77`
- [ ] `src/ingestion/xg_model.py:179`
- [ ] `src/ingestion/xg_model_v2.py:111`
- [ ] `src/ingestion/xg_model_v2.py:155`
- [ ] `src/ingestion/export_embeddings_training_data.py:52`
- [ ] `src/ingestion/export_embeddings_training_data.py:58`
- [ ] `src/ingestion/prepare_360_training_data.py:120`
- [ ] `src/ingestion/prepare_360_training_data.py:126`
- [ ] `src/ingestion/export_scoutgpt_training_data.py:169`
- [ ] `src/ingestion/export_scoutgpt_training_data.py:203`
- [ ] `src/ingestion/export_scoutgpt_training_data.py:205`
- [ ] `src/ingestion/player_embeddings_v1.py:68`
- [ ] `src/ingestion/utils.py:283`

**Note on `spadl_vaep.py:215`, `metrica.py:58`, `idsse_events.py:35`, `idsse.py:63`, `wyscout.py:65`, `skillcorner.py:54`:** These are guards with `except Exception: pass` patterns. They need the same narrowing but may be structurally different (inside `check()` methods, no logger available, return FilterResult). Handle each case individually — may need to get the logger from the module or use `print` to stderr if there's no logger.

**Note on `guards.py:218`:** This catches an import failure, which is a legitimately broad catch. Keep as-is but change log level to `error` (the import failure is NOT expected and indicates a missing dependency).

- [ ] **Step 1: Apply edits.** Expect ~25-30 individual `Edit` calls.
- [ ] **Step 2: Run `ruff check` + `pytest` after each batch of ~5 files to catch regressions early.**

### Task 23: Rewrite Category E `model_validation.py` silent-skip sites

**File:** `src/ingestion/model_validation.py` — 6 sites (lines 80, 119, 176, 233, 277, 310).

Current pattern:
```python
try:
    ...
except Exception:
    logger.warning("Cannot read %s — skipping X validation", table)
    return results
```

**Fix pattern:** Narrow to `TABLE_OR_VIEW_NOT_FOUND` (treated as "first run, nothing to validate"). Any other exception should propagate with ERROR level logging.

```python
from ingestion.utils import tolerate_missing_table

validation_done = False
with tolerate_missing_table(logger, f"Cannot find {table} — skipping X validation (first run)"):
    ...
    validation_done = True
if not validation_done:
    return results
```

Or simpler: wrap the whole body in try/except but ONLY catch the specific exception:

```python
try:
    ...
except pyspark.errors.exceptions.connect.AnalysisException as exc:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(exc):
        logger.info("Cannot find %s — skipping X validation (first run)", table)
        return results
    logger.error("Cannot validate %s — unexpected error", table, exc_info=True)
    raise
```

Second pattern is cleaner — pick one and use it consistently.

- [ ] **Step 1: Apply 6 edits** using the chosen pattern.
- [ ] **Step 2: Run tests.**

### Task 24: Fix Category F pipeline-critical fallbacks

Each of these requires individual judgment:

- [ ] **`src/ingestion/hf_sync.py:126`** — sub-workflow failure → continue. Bump to ERROR level + record the failure in the task's return value so the caller can see which sub-workflows failed.
- [ ] **`src/ingestion/player_embeddings_v2.py:143`** — match metadata load failure → "stat vectors will be None". Add `metadata_fallback=True` to the FilterResult.metadata so downstream consumers see the fallback flag.
- [ ] **`src/ingestion/player_embeddings_v2.py:283`** — v2 import failure → fall back to v1. Bump to ERROR level. Record fallback in metrics/metadata.
- [ ] **`src/ingestion/xg_model_v2.py:332`** — "No xG v2 weights found — cannot run" → returns 0. **Change to raise RuntimeError** — missing weights is an error state, not "nothing to do".
- [ ] **`src/analytics/defcon_lite.py:161`** — hardcoded `pc_at_action = 0.5` fallback. Investigate what exception was being caught; narrow it, or raise if the fallback is masking a real bug.
- [ ] **`src/ingestion/pausa.py:166`** — "Cannot read table — run OBSO batch first" returns 0. This should raise WorkflowSkippedError instead (the workflow is not ready to run, not zero rows).

### Task 25: Enable BLE001 in ruff + per-file-ignores

**File:** `pyproject.toml` — `[tool.ruff.lint]` section.

```toml
[tool.ruff.lint]
select = [
    "E", "W", "F", "I", "N", "UP", "B", "S", "RUF",
    "BLE",  # NEW — flake8-blind-except. Catches `except:` and `except Exception:`.
]

[tool.ruff.lint.per-file-ignores]
"src/tests/**" = ["S101"]
"src/ingestion/cost_hook.py" = ["BLE001"]  # Fire-and-forget telemetry by design (logs at ERROR level, does not propagate)
"src/workflows/runner.py" = ["BLE001"]  # Hook dispatcher catches to keep pipeline running (logs at ERROR level)
"src/ingestion/utils.py" = ["BLE001"]  # Contains tolerate_missing_table context manager
"src/ingestion/guards.py" = ["BLE001"]  # Import failure catch
"src/ingestion/hf_jobs_cost.py" = ["BLE001"]  # Retry loop + cost history pruning (best-effort)
"src/ingestion/refresh_synced_tables.py" = ["BLE001"]  # CLI error accumulator
"src/ingestion/sync_hf_costs.py" = ["BLE001"]  # Per-item HF parse loop (best-effort)
"src/evolve/**" = ["BLE001"]  # Evolve backend error-return types (typed failure paths)
```

Note: the per-file-ignores list may need to be pared down or expanded based on what `ruff check` reports after enabling BLE001. Start with the list above, run `ruff check src/`, and add/remove entries to minimize the ignore list while keeping the lint clean.

- [ ] **Step 1: Apply the pyproject.toml edit.**
- [ ] **Step 2: Run `ruff check src/`** — examine the BLE001 violations.
- [ ] **Step 3: Iterate**: for each violation, either (a) narrow the catch to a specific exception, (b) add to per-file-ignores with a comment explaining why, or (c) fix the code to not catch at all.
- [ ] **Step 4: Run full pytest suite** to catch any regressions from the narrowings.

### Task 26: Full quality gate on Commit 3

- [ ] **Step 1:**

```bash
uv run ruff check src/
uv run ruff format --check src/
uv run pyright src/
uv run pytest src/tests/ -v 2>&1 | tail -40
```

Expected: all green. Pytest count should be larger than pre-cycle (new tests added in Commits 1+2).

### Task 27: STOP — present Commit 3 diff for approval

Commit 3 will be the LARGEST of the cycle (25+ files modified, BLE001 ruff rule added). Present carefully.

- [ ] **Step 1: Show `git diff HEAD --stat` first** so user sees file count / line count.
- [ ] **Step 2: Show individual file diffs** for the non-mechanical changes (Task 24 fixes in particular — those are judgment calls).
- [ ] **Step 3: WAIT for explicit user approval before `git commit`.**

### Task 28: Create Commit 3

```bash
git add src/ingestion/utils.py src/tests/test_utils.py \
        src/ingestion/statsbomb.py src/ingestion/statsbomb_backfill_360.py \
        src/ingestion/statsbomb_backfill_extra.py src/ingestion/spadl_conversion.py \
        src/ingestion/idsse.py src/ingestion/idsse_events.py \
        src/ingestion/metrica.py src/ingestion/metrica_tracking.py src/ingestion/metrica_events.py \
        src/ingestion/skillcorner.py src/ingestion/wyscout.py \
        src/ingestion/defcon_lite_common.py \
        src/ingestion/xg_model.py src/ingestion/xg_model_v2.py \
        src/ingestion/export_embeddings_training_data.py src/ingestion/prepare_360_training_data.py \
        src/ingestion/export_scoutgpt_training_data.py src/ingestion/player_embeddings_v1.py \
        src/ingestion/player_embeddings_v2.py src/ingestion/spadl_vaep.py \
        src/ingestion/model_validation.py \
        src/ingestion/hf_sync.py src/ingestion/guards.py src/ingestion/pausa.py \
        src/analytics/defcon_lite.py \
        pyproject.toml

git commit -m "$(cat <<'EOF'
refactor: narrow silent-exception swallows repo-wide + enable BLE001

Systemic remediation of the anti-pattern that caused both the VAEP scoring
bug and the warm-tier cost-hook bug fixed earlier in this cycle.

New helper: `ingestion.utils.tolerate_missing_table` — context manager that
suppresses only the Spark `TABLE_OR_VIEW_NOT_FOUND` error. Any other
exception (permission denied, schema corruption, MERGE resolve failure)
propagates instead of being hidden by a bare `except Exception:`.

Rewrites:
- 20 `Category B` bootstrap "table doesn't exist yet" catches in
  ingestion/ to use the new helper. The previous code caught any
  exception as "first run"; now only legitimate table-missing errors
  are suppressed.
- 6 `Category E` validation-skip catches in model_validation.py
  narrowed to specific Spark AnalysisException.
- 5 `Category F` pipeline-critical fallbacks: hf_sync.py sub-workflow
  error visibility, player_embeddings_v2.py fallback metadata flags,
  xg_model_v2.py missing-weights now raises (was silently returning 0),
  defcon_lite.py hardcoded fallback narrowed, pausa.py missing-input
  now raises WorkflowSkippedError.

Ruff: enable BLE001 (flake8-blind-except). Per-file-ignores document
the small set of intentional fire-and-forget code paths (cost_hook
fire-and-forget telemetry, runner.py hook dispatcher, evolve backend
error-return types, CLI error accumulators).

No behavior change on the happy path — every previously-working
code path still works. On the error path, the failure now surfaces
with a typed exception and an ERROR-level log instead of a silent
success or a WARNING-level log hidden from error-log queries.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

<a id="commit-4"></a>
## Commit 4 — `scripts/` + `hf_taipy_app/` swallow remediation

**Scope:** Narrow the small set of dangerous patterns found in the scripts/ + hf_taipy_app/ audit. Per the session 40 agent audit, this is MUCH smaller than the src/ remediation — zero Category C (UDF swallow) or Category F (pipeline fallback) hits. ~8 files of mechanical narrowings.

### Task 29: Narrow hf_taipy_app Taipy query catches to specific DB exceptions

Eight query/state modules in `hf_taipy_app/src/` catch `except Exception: logger.warning + return empty` when they should catch specific `psycopg2` exceptions:

- [ ] **`hf_taipy_app/src/queries/shots.py:66`** — xG predictions query; narrow to `psycopg2.ProgrammingError` (relation does not exist) + `psycopg2.OperationalError` (connection issues).
- [ ] **`hf_taipy_app/src/queries/workflows.py:62,94,119`** — cold/warm cost queries and run metrics; narrow similarly.
- [ ] **`hf_taipy_app/src/queries/defensive.py:58,409`** — percentile joins and lookups; narrow.
- [ ] **`hf_taipy_app/src/queries/players.py:114`** — percentile data; narrow.
- [ ] **`hf_taipy_app/src/state/workflows.py:232`** — Jobs API query; narrow to the specific Databricks SDK exception.

For each: replace `except Exception:` with `except (psycopg2.ProgrammingError, psycopg2.OperationalError) as exc:` (or the Databricks SDK equivalent) so a schema mismatch or auth failure doesn't get silently caught as "optional data unavailable".

- [ ] **Step 1: Apply ~8 edits.**
- [ ] **Step 2: Run the Taipy app's tests:**

```bash
uv run pytest hf_taipy_app/src/test_*.py -v 2>&1 | tail -30
```

### Task 30: Narrow scripts/ broad catches

- [ ] **`scripts/compute_space_creation_hf.py:130` + `scripts/compute_obso_hf.py:184`** — column projection fallbacks. Add an explicit column validation assertion: `missing = set(needed_cols) - set(df.columns); if missing: raise ValueError(f"Missing columns: {missing}")` inside the fallback branch.
- [ ] **`scripts/create_indexes.py:323,339,342,361,376`** — 5 broad `except Exception` after specific catches. Remove the redundant catches (the specific ones above them already handle what they need to handle), OR rename to `except Exception as exc: logger.exception("Unexpected error in X"); raise` to preserve defensive visibility without suppressing.

- [ ] **Step 1: Apply edits.**
- [ ] **Step 2: Run ruff check on scripts/** — confirm BLE001 violations are either fixed or have per-file-ignore entries.

### Task 31: Full quality gate on Commit 4

```bash
uv run ruff check scripts/ hf_taipy_app/
uv run ruff format --check scripts/ hf_taipy_app/
uv run pyright scripts/ hf_taipy_app/ 2>&1 | tail -20
uv run pytest hf_taipy_app/src/test_*.py -v 2>&1 | tail -20
```

Expected: all green.

### Task 32: STOP — present Commit 4 diff for approval

- [ ] **Step 1: Show diff.**
- [ ] **Step 2: WAIT for approval.**

### Task 33: Create Commit 4

```bash
git add hf_taipy_app/src/queries/ hf_taipy_app/src/state/workflows.py \
        scripts/compute_space_creation_hf.py scripts/compute_obso_hf.py scripts/create_indexes.py

git commit -m "$(cat <<'EOF'
refactor: narrow exception catches in scripts/ + hf_taipy_app/

Completes the session 40 silent-swallow remediation by narrowing broad
`except Exception` catches in the remaining surfaces outside src/.

hf_taipy_app Taipy query modules: narrow to `psycopg2.ProgrammingError`
and `psycopg2.OperationalError` so schema mismatches and auth failures
surface as errors instead of being silently caught as "optional data
unavailable" (the previous fall-through to empty DataFrame / dict).

scripts/compute_space_creation_hf.py + compute_obso_hf.py: add explicit
column validation assertions inside the projection-fallback branch.
Silent silent "column projection failed" paths previously masked
production schema drift as "just use all columns".

scripts/create_indexes.py: remove redundant broad catches after specific
psycopg2 error handling. Preserves defensive exception visibility while
eliminating the blind-catch noise.

Zero behavior change on the happy path. On the error path, failures
surface with typed exceptions.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

<a id="commit-5"></a>
## Commit 5 — Memory entry + spec rewrite + TODO updates + cleanup

**Scope:** Non-code documentation: the durable feedback memory entry, the spec rewrite, TODO.md cleanup, and tmp file deletions.

### Task 34: Write `feedback_no_silent_swallows.md`

**File:** `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\feedback_no_silent_swallows.md`

See the [memory entry draft](#memory-entry-draft) at the end of this plan.

- [ ] **Step 1: Write the file.**
- [ ] **Step 2: Add a pointer in `memory/MEMORY.md`** under User Feedback section:

```markdown
- [feedback_no_silent_swallows.md](feedback_no_silent_swallows.md) — CRITICAL: default exception handling is raise-or-observable, never silent-warn. When you find one swallow, audit the pattern repo-wide before moving on.
```

### Task 35: Rewrite spec §4.1 with corrected D57 diagnosis

**File:** `docs/superpowers/specs/2026-04-14-gold-data-repair-design.md`

The original spec (untracked) blamed `_flatten_extra()`. The corrected diagnosis is the `_VaepGuard.check()` staleness + the 3 UDF silent-swallows.

- [ ] **Step 1: Rewrite §4.1** to reference:
  - The trace evidence from `project_spadl_vaep_chain.md` (StatsBomb match 3754348 and Wyscout match 2500097).
  - The corrected root cause (staleness + UDF swallows).
  - The revised success criteria (no more silent data loss in Stage 1 or Stage 2 scoring).
- [ ] **Step 2: Add new §4.3** covering the warm-tier blocker diagnosis and fix (DELTA_MERGE_UNRESOLVED_EXPRESSION + schema drift).
- [ ] **Step 3: Add new §4.4** covering the systemic silent-swallow audit and remediation scope.
- [ ] **Step 4: Remove or move §4.2** (D45 Football2vec) — deferred to a separate cycle.

### Task 36: Update TODO.md

**File:** `TODO.md` — remove completed items, add follow-ups discovered during the cycle.

- [ ] **Step 1: Remove** D57 (now completed as part of this cycle).
- [ ] **Step 2: Keep D45** but update the note to say "blocked on data-integrity cycle completion 2026-04-15".
- [ ] **Step 3: Add D65**: "assert_warm_tier_not_empty dbt test — verify post-hook 1 watermark logic is correct" as a follow-up to the Commit 2 warm-tier fix. The current post-hook 1 uses `MAX(usage_date)` as a watermark which is subtly wrong (see session 40 analysis). It's not currently firing because warm tier is being freshly populated, but the logic bug should still be fixed.
- [ ] **Step 4: Add D66**: "silent-swallow audit — scripts/ + hf_taipy_app/ round 2" as a follow-up to catch any hits the agent audit missed.

### Task 37: Delete session 40 investigation scratch files

```bash
rm -f tmp_investigate_*.py tmp_investigate_*_output.txt tmp_task_log*.txt tmp_task_log*.py tmp_drop_task_key.py tmp_trigger_hf_sync.py
git status --short
```

Expected: no `tmp_*` files remain.

### Task 38: STOP — present Commit 5 diff for approval

- [ ] **Step 1: Show diff + list of deleted tmp files.**
- [ ] **Step 2: WAIT for approval.**

### Task 39: Create Commit 5

```bash
git add docs/superpowers/specs/2026-04-14-gold-data-repair-design.md \
        docs/superpowers/plans/2026-04-14-gold-data-repair.md \
        TODO.md

git commit -m "$(cat <<'EOF'
docs: data-integrity cycle spec + plan + TODO

- Rewrite spec §4.1 with the corrected D57 diagnosis (guard staleness +
  UDF silent swallows, not `_flatten_extra()`)
- Add spec §4.3 covering the warm-tier hook schema-drift diagnosis
- Add spec §4.4 covering the silent-swallow systemic audit
- Remove D57 from TODO; mark D45 deferred; add D65 (warm-tier post-hook
  watermark logic) and D66 (scripts/ swallow audit round 2) as
  follow-ups discovered during this cycle

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

<a id="destructive-ops-phase-2"></a>
## Destructive ops phase 2 — Stage 2 re-run + dbt full-refresh

**Prerequisites:** Commits 1-5 all committed. Destructive ops phase 1 complete and verified.

### Task 40: STOP — present destructive ops phase 2 plan for approval

- [ ] **Step 1: Describe what's about to happen:**
  1. Trigger `compute_spadl_vaep` via Databricks Jobs API.
  2. Guard will compute `unscored_vaep_match_ids = 3,462 statsbomb match_ids` (thanks to the Commit 1 fix).
  3. Stage 1 will be a no-op for statsbomb (already populated in bronze.spadl_actions v639).
  4. Stage 2 will score all 3,462 statsbomb matches and write ~9M rows to `bronze.vaep_action_values`.
  5. **HARD FAIL RISK:** If any single match's silly-kicks scoring fails (now raising because of the UDF fix), the whole task fails. This is the correct behavior per user decision #4 — investigate the specific match, fix, rerun. Have a rollback strategy ready.
  6. After Stage 2 succeeds: run `dbt build --full-refresh --select fct_action_values+` via the standard `scripts/dbt_build_and_refresh.py` wrapper (which runs `scripts/refresh_synced_tables.py --wait` on success).
  7. Warm-tier test should now pass (warm tier was seeded in phase 1).
  8. Puppeteer-verify an EPL Shot Map page to confirm goal counts appear.

- [ ] **Step 2: WAIT for explicit user approval.**

### Task 41: Trigger compute_spadl_vaep

- [ ] **Step 1: Trigger the task** (same mechanism as hf_sync trigger in Task 19).
- [ ] **Step 2: Monitor the task log in real time** (poll every 30s, don't go silent).
- [ ] **Step 3: Handle the hard-fail case**: if the task fails with a `RuntimeError: VAEP scoring failed for game_id=X` or `StatsBomb SPADL conversion failed for match_id=Y`, STOP. Report the failing match/game to the user + investigate the specific match before retrying.

### Task 42: Verify Stage 2 completion

- [ ] **Step 1: Query `bronze.vaep_action_values` statsbomb partition:**

```sql
SELECT data_source, COUNT(*) FROM soccer_analytics.bronze.vaep_action_values GROUP BY data_source
```
Expected: statsbomb ≈ 9M, wyscout ≈ 2.46M.

- [ ] **Step 2: Query Delta history for a fresh WRITE:**

```sql
DESCRIBE HISTORY soccer_analytics.bronze.vaep_action_values
```
Expected: top entry is a MERGE/WRITE with `numOutputRows ≈ 9M` by ingestion SP, within the last 30 minutes.

- [ ] **Step 3: Spot check match 3754348** (the trace match from memory): `SELECT type_id, result_id, COUNT(*) FROM bronze.vaep_action_values WHERE match_id = 3754348 GROUP BY type_id, result_id`. Expect shot-like rows (type_id=11) with both success and fail rows present.

### Task 43: dbt build --full-refresh

- [ ] **Step 1:**

```bash
uv run python scripts/dbt_build_and_refresh.py -- build --full-refresh --select fct_action_values+
```

This wrapper:
1. Ensures the SQL warehouse is running.
2. Runs `dbt build --full-refresh` on the specified models.
3. On success, runs `refresh_synced_tables.py --wait`.

Expected: PASS. The `assert_warm_tier_not_empty` test should pass because warm tier was seeded in phase 1.

- [ ] **Step 2: If the warm-tier test fails** despite the seed, investigate immediately — either the seed didn't persist (post-hook 1 over-pruned) or the test's fire condition is triggering for a different reason.

### Task 44: Verify fct_action_values is fresh

- [ ] **Step 1: Row counts:**

```sql
SELECT data_source, COUNT(*) FROM soccer_analytics.dev_gold.fct_action_values GROUP BY data_source
```
Expected: statsbomb ≈ 7.15M (post stg-dedup), wyscout ≈ 2.46M.

- [ ] **Step 2: EPL shot success rows present:**

```sql
SELECT COUNT(*) FROM soccer_analytics.dev_gold.fct_action_values
WHERE action_type = 'shot' AND action_result = 'success' AND competition_id = 2
```
Expected: > 0. Pre-fix this was 0 (the D57 symptom).

### Task 45: Puppeteer verification

- [ ] **Step 1: Start the local Taipy dev server** or navigate to the staging HF Space.
- [ ] **Step 2: Navigate to Shot Map page.** Select an EPL competition. Pick a team.
- [ ] **Step 3: Verify goal count > 0** in the page summary.
- [ ] **Step 4: Screenshot + report** the verification results.

### Task 46: Report data-ops completion

- [ ] **Step 1: Summary to user:**
  - Stage 2 completed: X matches scored, Y rows written.
  - dbt build: PASS / FAIL + test count.
  - Synced table refresh: done.
  - Puppeteer verification: EPL Shot Map shows N goals for team T.
  - Warm-tier test: PASS.

---

<a id="mad-scientist-skills"></a>
## mad-scientist-skills audit anti-pattern updates (sibling repo)

**Location:** `D:/Development/karstenskyt__mad-scientist-skills/plugins/mad-scientist-skills/skills/`

This is a **separate git repository** from `luxury-lakehouse`. Work happens on a branch there and gets committed separately. The luxury-lakehouse PR does NOT contain these changes.

### Task 47: Update architecture-audit/SKILL.md

**File:** `D:/Development/karstenskyt__mad-scientist-skills/plugins/mad-scientist-skills/skills/architecture-audit/SKILL.md`

**Phase 0 anti-pattern table — add 3 new rows:**

| Pattern | Language | Risk |
|---------|----------|------|
| Silent telemetry swallow | Python (`except Exception: logger.warning(...)` in hook, callback, fire-and-forget, or `@observer`-style code path) | Observability failures hide indefinitely at WARNING level; invisible in standard error-log queries. Compounds into data-integrity bugs when no other signal surfaces the failure. Must either re-raise, return a typed error value, or log at ERROR level minimum. |
| UDF empty-return on exception | Python (`except Exception: return pd.DataFrame(columns=...)` or `return []` inside `applyInPandas`/`mapInPandas`/a closure passed to a distributed execution framework) | Silent per-group data loss — executor-level failures vanish, Spark concatenates UDF outputs so missing rows are invisible in aggregate output. Must propagate with group-key context. |
| Writer-to-target schema drift | Python `StructType(...)` or `pydantic.BaseModel` hardcoded alongside `CREATE TABLE` DDL with no programmatic reconciliation test | `DeltaTable.merge().whenMatchedUpdateAll()` fails at parse time when source schema ⊂ target schema; code may ship broken for weeks before detection. Must have a CI test that parses both schemas and asserts equality. |

**Phase 4 Cross-deployment contract validation — add 1 new row:**

| Writer/target schema reconciliation | Is there a test that parses the target schema (CREATE TABLE DDL, OpenAPI spec, protobuf/avro schema, dbt contract) and asserts equality with the writer's in-code schema (Spark StructType, Pydantic model, dataclass)? A fire-and-forget writer that swallows schema-mismatch exceptions will ship broken for weeks without the reconciliation test | High |

**Phase 5 Data platforms table — extend existing row `Schema as implicit contract`:**

Add to the "What to look for" column: "Also check for **hardcoded DataFrame schemas (Spark StructType literals) inside pipeline code with no automated reconciliation against the target table's DDL**. A fire-and-forget writer with a drifted schema will silently fail every merge."

- [ ] **Step 1: Edit the file** with 4 additions.
- [ ] **Step 2: Version bump**: update CHANGELOG.md in that repo with a new minor version entry describing the additions.

### Task 48: Update observability-audit/SKILL.md

**File:** `D:/Development/karstenskyt__mad-scientist-skills/plugins/mad-scientist-skills/skills/observability-audit/SKILL.md`

**Phase 0 Standard tier anti-pattern table — add 3 new rows:**

| Pattern | Language | Risk |
|---------|----------|------|
| `except Exception: logger.warning` in hook/callback/observer | Python | Fire-and-forget pattern that "logs" the failure at WARNING level — invisible in error-log queries that filter by level. Must log at ERROR or surface via a typed error-return path. |
| `except Exception: return empty_df` inside `applyInPandas` / `mapInPandas` | Python | Silent per-group data loss inside distributed UDF. No single log line reports the drop; downstream aggregates silently include the missing data. Must propagate with group-key context. |
| `except Exception: use_fallback_value()` without observable signal | Any | Silent model/value downgrade — pipeline produces degraded output without user-visible indicator. Must emit a metric, log at ERROR level, or mark the output with a fallback flag visible to consumers. |

**Phase 0 Standard tier table — extend existing row `except Exception: / except: without logging`:**

Current: "Swallowed exception — silent failure"

Add: "**Note**: catches WITH logging are NOT safe by default — a `logger.warning(...)` in a fire-and-forget telemetry path hides failures just as thoroughly as no logging at all, because warnings are filtered out of error dashboards and not alerted on. Audit requires checking not just *whether* the catch logs, but *at what level* and *through which observability channel*."

**New section: "Phase 3.5 — Telemetry Completeness Assertions" (between Phase 3 Structured Logging and Phase 4 Metrics):**

Add a new phase that checks:
- For each lifecycle hook / observer / callback, is there an integration test that asserts the hook's output actually lands in the target sink (not just "the method was called")?
- For each fire-and-forget write, is the write path wrapped in a test that creates a temp table, exercises the write, and asserts the row count is ≥ expected?
- For each observability table (cost tracking, run metrics, hook failures), is there a dbt test or CI query that alerts when the table is empty for longer than N hours?

**Writing the phase** is a longer change — draft it in full, ~50-80 lines of SKILL.md content. Structure it like the existing phases with a Standard tier + Enterprise tier.

- [ ] **Step 1: Edit the file** with 2 additions + 1 new phase.
- [ ] **Step 2: Version bump** CHANGELOG.md.

### Task 49: Commit to mad-scientist-skills repo

- [ ] **Step 1:** Change directory to `D:/Development/karstenskyt__mad-scientist-skills/`.
- [ ] **Step 2:** Create a feature branch, e.g., `feat/silent-swallow-audit-checks`.
- [ ] **Step 3:** `git add` both SKILL.md files + CHANGELOG.md.
- [ ] **Step 4:** Present the diff for user approval.
- [ ] **Step 5:** Commit on approval.
- [ ] **Step 6:** PR in that repo follows its own workflow (separate PR from luxury-lakehouse).

---

<a id="pr-creation"></a>
## PR creation

### Task 50: Push branch + create PR

**Prerequisites:** All commits in the luxury-lakehouse repo are created, all destructive ops completed and verified, user has approved PR creation.

- [ ] **Step 1: Push the branch:**

```bash
git push -u origin feat/gold-data-repair
```

- [ ] **Step 2: Create PR:**

```bash
gh pr create --title "fix: data integrity foundation — SPADL + warm-tier hook + systemic silent-swallow audit" --body "$(cat <<'EOF'
## Summary

One cycle, three connected fixes for data integrity issues that were hiding behind silent exception swallows.

**Commits:**
1. `fix(spadl): D57 goal encoding + UDF silent-swallow removals`
2. `fix(cost-hook): warm-tier hook schema drift + observability`
3. `refactor: narrow silent-exception swallows repo-wide + enable BLE001`
4. `refactor: narrow exception catches in scripts/ + hf_taipy_app/`
5. `docs: data-integrity cycle spec + plan + TODO`

**Root causes resolved:**
- D57: `_VaepGuard.check()` metadata staleness + 3 UDF `except Exception: return empty_df` patterns hiding per-match data loss. See `project_spadl_vaep_chain.md`.
- Warm-tier blocker: `DELTA_MERGE_UNRESOLVED_EXPRESSION` on orphaned `task_key` column; hook swallow + runner.py hook dispatcher swallow both at WARNING level made it invisible for 62+ hours. See `project_warm_tier_blocker.md`.
- Systemic: the same anti-pattern was present in 25+ other `src/` locations. Narrowed to `_tolerate_missing_table` helper for table-missing bootstrap; narrowed model_validation.py to specific exceptions; raised Category F fallbacks that were silently degrading output. Enabled BLE001 ruff rule.

**Destructive ops performed against production:**
1. `ALTER TABLE soccer_analytics.observability.workflow_cost_live DROP COLUMN task_key` — metadata-only.
2. One `hf_sync` task trigger to seed the warm tier post-fix.
3. `compute_spadl_vaep` re-run — scored N statsbomb matches, wrote ~9M rows to `bronze.vaep_action_values`.
4. `dbt build --full-refresh --select fct_action_values+` — rebuilt downstream marts with the fresh VAEP scores.

**Verification artifacts (citable):**
- Warm tier populated: `SELECT COUNT(*) FROM workflow_cost_live WHERE state != 'RUNNING'` = N.
- EPL Shot Map goal count: Puppeteer screenshot attached.
- `fct_workflow_costs` warm-tier enrichment now non-null: `pipeline_state`, `duration_seconds`, `entity_count` populated for tasks from the post-fix daily job.

## Test plan

- [ ] `uv run pytest src/tests/ -v` — all pass, including new regression tests for each UDF swallow fix + schema drift guard + integration test
- [ ] `uv run ruff check src/ scripts/ hf_taipy_app/` — clean, BLE001 enabled
- [ ] `uv run pyright src/` — 0 errors
- [ ] Warm-tier test passes: `uv run python scripts/dbt_build_and_refresh.py -- test --select assert_warm_tier_not_empty`
- [ ] Puppeteer: EPL Shot Map shows non-zero goal count for Man United
- [ ] Taipy AI/ML Workflows page renders cost + cold-start columns populated (no NULLs for post-fix runs)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report PR URL to user.**

---

<a id="self-review"></a>
## Self-review checklist

Before marking this plan as "ready to execute":

- **Scope coverage**: Every scope item listed in the goal is addressed:
  - D57 SPADL goal encoding → Commit 1
  - UDF silent swallows → Commit 1
  - Warm-tier hook schema drift → Commit 2 + destructive ops phase 1
  - Systemic src/ silent-swallow audit → Commit 3
  - scripts/ + hf_taipy_app/ audit → Commit 4
  - Memory entry → Commit 5
  - Spec rewrite → Commit 5
  - mad-scientist-skills update → Tasks 47-49 (sibling repo)
  - Data ops → destructive ops phase 2
  - D45 → explicitly deferred to follow-up cycle

- **Commit boundaries**: Each commit is self-contained and reviewable. Commit 1 is SPADL-only. Commit 2 is warm-tier-only. Commit 3 is systemic src/ refactor. Commit 4 is outside-src/ refactor. Commit 5 is docs. A reviewer can follow the narrative via `git log`.

- **Approval checkpoints**: 10+ explicit STOP-for-approval steps. Every commit, every destructive op, every sibling-repo commit, and PR creation each require their own approval.

- **Rollback strategy**: Commits 1 & 2 are pure code (can revert). Commit 3 is pure refactor (revert-safe). Commit 4 is pure refactor (revert-safe). Destructive ops phase 1 is reversible (re-ADD task_key column if needed). Destructive ops phase 2 is reversible via dbt `--full-refresh` on the prior-state bronze tables.

- **Hard-fail-first for SPADL UDFs**: Per user decision, the Stage 2 re-run WILL fail hard on any previously-hidden match failure. This is correct behavior. Task 41 has an explicit "handle the hard-fail case" step.

- **No TBDs**: every task has concrete code or commands. Steps that depend on environment state (wheel deployment path, task_key verification) have explicit discussion points with the user rather than assumptions.

- **Test coverage**: each code change has a corresponding regression test. Schema drift has a guard test. Integration test for cost hook (skipped if Spark unavailable). No "add tests" placeholders.

- **Citable verification artifacts**: every "verify" step names a specific SQL query or UI artifact. No "looks good" claims.

---

<a id="memory-entry-draft"></a>
## Appendix: memory entry draft

**File:** `C:\Users\Karsten\.claude\projects\D--Development-karstenskyt--luxury-lakehouse\memory\feedback_no_silent_swallows.md`

```markdown
---
name: no silent exception swallows; audit pattern repo-wide when one is found
description: Default exception handling is raise-or-observable, never silent-warn. When you find one swallow, audit the pattern repo-wide before moving on. "Out of scope" is not a valid disposition for a blocker that has fired twice.
type: feedback
---

**Rule:** Telemetry code, callback/hook code, and defensive fallback code must use one of three exception handling patterns. Anything else is a silent swallow and must be fixed immediately.

1. **Propagate** — re-raise the exception with context. Preferred for unknown or unexpected errors.
2. **Typed error return** — return a structured value (dataclass, enum, sentinel object) that callers MUST destructure to access results. The caller cannot ignore the error case because the type system forces the check.
3. **Observable log + typed error return** — log at ERROR level (NOT warning, NOT debug) AND return a typed error value. Only legitimate for fire-and-forget telemetry that must not crash the calling pipeline.

**Forbidden patterns:**

- `except Exception: pass` — completely silent.
- `except Exception: logger.warning(...)` — hidden in warning-level noise, invisible in error-log queries.
- `except Exception: return pd.DataFrame(...)` or `return {}` inside a UDF or distributed executor closure — silent per-group data loss.
- `except Exception: use_fallback_value()` without a metric/log/flag making the fallback visible — silent output degradation.

**Why:** Logged in session 40 (2026-04-14) after three instances of the same anti-pattern caused multiple hours of debugging:

1. `spadl_vaep.py:_make_scoring_udf` — `except Exception: pass  # noqa: S110` silently dropped failed VAEP scoring games. Task was reporting SUCCEEDED with zero rows written.
2. `cost_hook.py` 4 methods — `except Exception: logger.warning(...)` silently failed every hook MERGE for 62+ hours after a schema migration left an orphaned `task_key` column. The entire warm-tier observability pipeline was broken, and only surfaced when `assert_warm_tier_not_empty` dbt test was finally wired into the daily job.
3. `spadl_conversion.py` StatsBomb + Wyscout UDFs — `except Exception: return _pd.DataFrame(columns=...)` inside Spark `applyInPandas` closures. Probably hiding per-match silly-kicks conversion failures that were the REAL cause of the D57 goal encoding symptom.

Each individual swallow looked defensive and reasonable in isolation. Together they created a compounding data-integrity failure where the ground truth about what the pipelines were doing was invisible.

**How to apply:**

- When writing new code: default to `raise` or typed error return. Only use `except Exception:` with explicit justification in a comment, and never at WARNING level.
- When finding a silent swallow: audit the codebase for the same pattern. The session 40 swallow was one of 55 in `src/` alone. Pattern recurrence is near-certain.
- When a blocker has been flagged "out of scope" twice: investigate, do not defer. The memory file `project_warm_tier_blocker.md` explicitly warned this — the session 40 investigation was triggered only after the user refused the "out of scope" escape a third time.
- When reviewing exception handling: check not just IF the catch logs, but AT WHAT LEVEL and THROUGH WHICH OBSERVABILITY CHANNEL. A warning-level log in a fire-and-forget path is structurally equivalent to no logging at all.

**Enforcement:**

- `ruff BLE001` (flake8-blind-except) is enabled in `pyproject.toml`. Violations require explicit `# noqa: BLE001` comment with justification, OR a per-file-ignore in `pyproject.toml` documenting the architectural reason.
- `TestCostHookSchemaDriftGuard` enforces the `cost_hook.py` StructType matches `create_cost_table.sql` — prevents future recurrence of the warm-tier blocker.
- `mad-scientist-skills:architecture-audit` Phase 0 scan includes silent-telemetry-swallow and UDF-empty-return anti-patterns — future audits catch this class.
- `mad-scientist-skills:observability-audit` Phase 0 scan includes the same patterns plus telemetry-completeness assertions in Phase 3.5.

**Related memories:**
- `feedback_proxies_not_verification.md` — tests green / ruff clean are proxies, not proof
- `feedback_verify_means_verify.md` — don't cherry-pick passing signals
- `feedback_no_hack_fixes.md` — root cause before any fix
- `project_spadl_vaep_chain.md` — the SPADL silent swallow investigation
- `project_warm_tier_blocker.md` — the warm-tier blocker investigation
```

---

## End of plan
