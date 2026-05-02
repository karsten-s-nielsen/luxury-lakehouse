# OPT-1 — Config + Docs Hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Single squash-commit "Dunkin'" cycle that (a) adds `max_retries=1` to 5 retryless TF tasks, (b) runs a measured timeout right-sizing audit against the live observability view, (c) refreshes the 32-day-stale `docs/performance-baselines.md`, and (d) audits `.toPandas()` call sites and updates the CLAUDE.md budget rule.

**Architecture:** Pure config (Terraform HCL) + markdown. No Python code, no dbt models, no Spark. Smoke-test cycle preceding the riskier OPT-2/3/4 work. Live-backend fact-checking via Databricks SDK statement_execution against `observability.system_lakeflow_job_task_run_timeline` (definer's-rights view, ADR-007 path) and a local benchmark re-run on Win11 96GB.

**Tech Stack:** Terraform (HCL), Markdown, Databricks SDK (`databricks.sdk.WorkspaceClient.statement_execution`), pytest-benchmark.

**Locked design (do not relitigate):**
1. Single squash commit at end. All four parts (a)–(d) bundle together.
2. The 5 retryless tasks ARE idempotent — user has confirmed; no debate.
3. Part (b) measures via the definer's-rights view, NOT raw `system.lakeflow.*`.
4. Part (d) is **CLAUDE.md update + audit only**. Actual `.toPandas()` migrations belong to OPT-3.
5. No scope decisions: surface findings as data + options, never as recommendations.

**Out of scope:** OPT-2/3/4, LL3-CO, ExT2-P2, K8, PR-γ batch 2+, wheel version bump (cycle convention — doc-only cycles don't bump).

---

## Files at Risk — DO NOT TOUCH

- `dbt_project/models/marts/*.sql` (OPT-2)
- `src/ingestion/{defcon_lite,spadl_vaep,xg_model}.py` (OPT-3)
- `src/analytics/{rotary_attention,scoutgpt_training,football2vec_*}.py` (OPT-4)
- `src/ingestion/idsse.py:_parse_positions_xml` (OPT-4 / Cycle B)
- The 3 PR-γ pilot synced tables — leave to collect daily-job data

## Files to Modify

- **Modify:** `terraform/modules/workflows/main.tf` — 5 task blocks (lines ~590, ~624, ~656, ~720, ~964)
- **Modify:** `docs/performance-baselines.md` — full table refresh + header date update
- **Modify:** `CLAUDE.md` — `## Database Performance` → `### Databricks (PySpark / Delta Lake)` `.toPandas()` rule

## Files to Create

- **Create (transient):** `scripts/_opt1_timeout_audit.py` — one-shot SDK query script, **DELETE before commit** (audit output is captured into PR body, not committed code)

---

## Task 1: Branch + verify clean baseline

**Files:** none modified — git operations only

- [ ] **Step 1: Verify origin/main alignment**

```bash
git fetch origin
git status
git log --oneline -1
```

Expected: `On branch main`, `Your branch is up to date with 'origin/main'`, HEAD=`2198d3b`.

- [ ] **Step 2: Create feature branch from origin/main**

```bash
git checkout -b chore/opt-1-config-docs-hygiene origin/main
```

Expected: switched to new branch; `git rev-parse HEAD` returns `2198d3b`.

- [ ] **Step 3: Verify clean tree**

```bash
git status
```

Expected: `nothing to commit, working tree clean`.

---

## Task 2: Part (a) — Add `max_retries=1` to 5 retryless TF tasks

**Files:**
- Modify: `terraform/modules/workflows/main.tf` (5 spot edits at task blocks)

The project standard is `max_retries=1` declared immediately after `timeout_seconds`. 5 of 33 daily-job task blocks lack this declaration today. All 5 are idempotent operations — adding the retry is safe. Verified retryless via `grep -n max_retries` against the file (28 matches today; expect 33 after).

- [ ] **Step 1: Add `max_retries=1` to `dbt_build_input_marts`**

File: `terraform/modules/workflows/main.tf:589-590`

Replace:
```hcl
  task {
    task_key        = "dbt_build_input_marts"
    timeout_seconds = 3600

    python_wheel_task {
```

With:
```hcl
  task {
    task_key        = "dbt_build_input_marts"
    timeout_seconds = 3600
    max_retries     = 1

    python_wheel_task {
```

- [ ] **Step 2: Add `max_retries=1` to `dbt_build_intermediate_marts`**

File: `terraform/modules/workflows/main.tf:622-624`

Replace:
```hcl
  task {
    task_key        = "dbt_build_intermediate_marts"
    timeout_seconds = 3600

    python_wheel_task {
```

With:
```hcl
  task {
    task_key        = "dbt_build_intermediate_marts"
    timeout_seconds = 3600
    max_retries     = 1

    python_wheel_task {
```

- [ ] **Step 3: Add `max_retries=1` to `dbt_build_output_marts`**

File: `terraform/modules/workflows/main.tf:654-656`

Replace:
```hcl
  task {
    task_key        = "dbt_build_output_marts"
    timeout_seconds = 3600

    python_wheel_task {
```

With:
```hcl
  task {
    task_key        = "dbt_build_output_marts"
    timeout_seconds = 3600
    max_retries     = 1

    python_wheel_task {
```

- [ ] **Step 4: Add `max_retries=1` to `hf_sync`**

File: `terraform/modules/workflows/main.tf:718-720`

Replace:
```hcl
  task {
    task_key        = "hf_sync"
    timeout_seconds = 1800

    python_wheel_task {
```

With:
```hcl
  task {
    task_key        = "hf_sync"
    timeout_seconds = 1800
    max_retries     = 1

    python_wheel_task {
```

- [ ] **Step 5: Add `max_retries=1` to `refresh_synced_tables`**

File: `terraform/modules/workflows/main.tf:962-964`

Replace:
```hcl
  task {
    task_key        = "refresh_synced_tables"
    timeout_seconds = 2400 # 30 min refresh window + overhead

    python_wheel_task {
```

With:
```hcl
  task {
    task_key        = "refresh_synced_tables"
    timeout_seconds = 2400 # 30 min refresh window + overhead
    max_retries     = 1

    python_wheel_task {
```

- [ ] **Step 6: Run terraform fmt**

```bash
terraform -chdir=terraform/environments/dev fmt -recursive ../../modules/workflows
```

Expected: `terraform/modules/workflows/main.tf` may be reformatted. Pre-emptively running `fmt` ourselves prevents the pre-commit hook silent-rewrite gotcha (the hook has produced "Passed" output but no `[main xxxx]` line in past sessions when fmt rewrote files mid-commit).

- [ ] **Step 7: Verify edit count via grep**

```bash
grep -cn "max_retries" terraform/modules/workflows/main.tf
```

Expected: count went from 28 → 33 (5 new occurrences).

- [ ] **Step 8: Run TF parity test if it exists for retries**

```bash
uv run pytest src/tests/test_terraform_workflow_dbt_task.py -v
```

Expected: PASS (the PR-β test class added 7 tests for the three-stage TF — should still pass after our 3-line addition).

---

## Task 3: Part (b) — Timeout right-sizing audit (live measurement)

**Files:**
- Create (transient): `scripts/_opt1_timeout_audit.py`

The user instructed "fact check any claim against actual backend when possible, you have aws, databricks and hf access." Part (b) reads the live `observability.system_lakeflow_job_task_run_timeline` view. Output is a table for the PR body, plus a row-by-row recommendation surface (current vs measured p95 vs proposed). **Do not commit the script** — it is one-shot. Use it to capture data, copy result into PR body and into the cycle PR description.

- [ ] **Step 1: Create the one-shot audit script**

File: `scripts/_opt1_timeout_audit.py`

```python
"""ONE-SHOT — DELETE BEFORE COMMIT.

Queries observability.system_lakeflow_job_task_run_timeline for last 30
days p50/p95/max wall-clock per task_key. Joins against current Terraform
timeout_seconds declarations in terraform/modules/workflows/main.tf to
emit a per-task right-sizing table.

Run: uv run python scripts/_opt1_timeout_audit.py
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_TF_PATH = Path("terraform/modules/workflows/main.tf")
_TASK_RE = re.compile(
    r'task_key\s*=\s*"(?P<key>[^"]+)"\s*\n\s*timeout_seconds\s*=\s*(?P<timeout>\d+)',
    re.MULTILINE,
)
_QUERY = """
SELECT
    task_key,
    PERCENTILE(execution_duration_seconds, 0.50) AS p50,
    PERCENTILE(execution_duration_seconds, 0.95) AS p95,
    MAX(execution_duration_seconds) AS max_s,
    COUNT(*) AS run_count
FROM {catalog}.observability.system_lakeflow_job_task_run_timeline
WHERE period_start_time >= current_date() - INTERVAL 30 DAYS
  AND result_state = 'SUCCEEDED'
GROUP BY task_key
ORDER BY p95 DESC
"""


def _round_up_5min(s: float) -> int:
    """Round up to next 5-minute boundary, minimum 300s."""
    return max(300, int((s // 300) + 1) * 300)


def _proposed(p95: float | None, current: int) -> tuple[int, str]:
    """Conservative right-size: p95 * 2 rounded up to nearest 5min."""
    if p95 is None:
        return current, "no-data"
    proposed = _round_up_5min(p95 * 2)
    if proposed >= current:
        return current, "keep"
    return proposed, "shrink"


def main() -> None:
    catalog = os.environ.get("TF_VAR_catalog_name", "soccer_analytics")
    w = WorkspaceClient()
    warehouse_id = os.environ["DATABRICKS_HTTP_PATH"].rstrip("/").split("/")[-1]

    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=_QUERY.format(catalog=catalog),
        wait_timeout="50s",
    )
    sid = resp.statement_id
    while resp.status and resp.status.state not in (
        StatementState.SUCCEEDED,
        StatementState.FAILED,
        StatementState.CANCELED,
    ):
        resp = w.statement_execution.get_statement(sid)
    if resp.status.state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else "unknown"
        raise RuntimeError(f"query failed: {msg}")

    rows = resp.result.data_array or []
    measured = {r[0]: (float(r[1]), float(r[2]), float(r[3]), int(r[4])) for r in rows}

    tf_text = _TF_PATH.read_text(encoding="utf-8")
    tf_pairs = {m.group("key"): int(m.group("timeout")) for m in _TASK_RE.finditer(tf_text)}

    print("| task_key | current | p50 | p95 | max | runs | proposed | action |")
    print("|---|---:|---:|---:|---:|---:|---:|---|")
    for key in sorted(tf_pairs):
        cur = tf_pairs[key]
        if key in measured:
            p50, p95, max_s, n = measured[key]
            prop, action = _proposed(p95, cur)
            print(
                f"| {key} | {cur} | {p50:.0f} | {p95:.0f} | {max_s:.0f} | {n} | {prop} | {action} |"
            )
        else:
            print(f"| {key} | {cur} | — | — | — | 0 | {cur} | no-data |")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the audit (live Databricks query)**

```bash
uv run python scripts/_opt1_timeout_audit.py
```

Expected: a markdown table on stdout, ~33 rows, columns `task_key | current | p50 | p95 | max | runs | proposed | action`. The `action` column is one of `shrink` / `keep` / `no-data`.

If the query fails with auth error: confirm `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH` env vars set. The SDK auto-resolves PAT auth from env. The warehouse will auto-start on first call.

- [ ] **Step 3: Capture output to a temp file for the PR body**

```bash
uv run python scripts/_opt1_timeout_audit.py > /tmp/opt1_audit.md
```

(Windows: redirect to `%TEMP%\opt1_audit.md` or just paste into a scratch file.)

Save this content for the PR description. Do NOT commit it.

- [ ] **Step 4: Surface findings as data, not recommendations**

Read the table. **DO NOT** silently apply the `proposed` column. Surface the table to the user with two questions:

> 1. "The audit recommends shrinking N task timeouts (rows with `action=shrink`). Apply all? Apply selected? Keep current and just document the audit?"
> 2. "Tasks with `no-data` (didn't run in last 30 days): \[list\] — keep current values?"

Wait for explicit user direction on which (if any) `timeout_seconds` to change in `main.tf`. The audit script's recommendations are findings, not decisions.

- [ ] **Step 5: Apply user-directed timeout changes (if any)**

If the user picks specific tasks to shrink: edit `terraform/modules/workflows/main.tf` for each one, replacing `timeout_seconds = OLD` with `timeout_seconds = NEW` and re-running `terraform fmt`. If the user says "keep all and just document," skip this step entirely — the audit table still goes into the PR body for posterity.

- [ ] **Step 6: Delete the transient script**

```bash
rm scripts/_opt1_timeout_audit.py
```

(Windows PowerShell: `Remove-Item scripts/_opt1_timeout_audit.py`.)

Confirm with `git status` that the only TF changes are intentional, and `_opt1_timeout_audit.py` is not present.

---

## Task 4: Part (c) — Refresh `docs/performance-baselines.md`

**Files:**
- Modify: `docs/performance-baselines.md`

The file was last updated 2026-03-31 — 32 days stale. Missing rows for major work that landed since: ExT v2 Phase 0/1, Football2Vec v2 cross-attention promotion, XG2 production unblock, Kimball PR 7 impact on `fct_action_values`. Re-run benchmarks locally on Win11 96GB.

- [ ] **Step 1: Run the benchmark suite**

```bash
uv run pytest src/tests/ -m benchmark --benchmark-only --benchmark-columns=median,iqr_outliers,outliers,rounds,iterations
```

Expected wall-clock: 5–15 min depending on Numba/JIT warm cache. If the run hangs >25 min, kill and investigate (`pytest -m benchmark --collect-only` to see which test stalls).

If benchmarks aren't tagged with `@pytest.mark.benchmark`, fall back to `-k benchmark` then verify which tests actually used `benchmark` fixture. (Most pytest-benchmark-fixtured tests register the marker automatically.)

Capture stdout — copy the `Name | Min | Max | Mean | StdDev | Median | IQR | Outliers | OPS | Rounds | Iterations` table to a temp file.

- [ ] **Step 2: Update Function Benchmarks table**

File: `docs/performance-baselines.md` lines 6–22.

For each row in the existing table, replace `Median` and `p95` with the new measurement (median in µs from pytest-benchmark; the file's "p95" column maps to pytest-benchmark's `IQR` upper or a calculated `mean + 2σ` — match whatever convention the existing rows used).

If a function listed in the table has no benchmark, leave the row but mark `Median` as `—` and add a `(no benchmark)` note.

If a benchmark exists for a function NOT in the table (e.g. ScoutGPT/Football2Vec/360 forward pass that CLAUDE.md performance-budget references), add a row.

- [ ] **Step 3: Update Pipeline Timing table**

File: `docs/performance-baselines.md` lines 24–53.

The "from job run 311181772997773 (2026-03-25)" reference is stale. Get the most recent successful job run via:

```bash
uv run python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
import os
job_id = 980461192099048  # daily-job per memory
runs = w.jobs.list_runs(job_id=job_id, completed_only=True, limit=5)
for r in runs:
    print(r.run_id, r.state.result_state, r.start_time, r.run_duration_ms)
"
```

Pick the most recent SUCCESS run. Then for each task in the existing table, query its execution_duration_seconds via the observability view (the Part (b) script gives us this data already — reuse those numbers if Part (b) ran).

Update each row's `Wall Clock` cell. Update the parenthetical reference at line 26 to the new job run id + date.

Add new rows for tasks that exist today but weren't in the table:
- `compute_xg_model_v2` (XG2 unblock)
- `dbt_build_input_marts` / `dbt_build_intermediate_marts` / `dbt_build_output_marts` (PR-Cycle-C PR-β three-stage)
- `import_obso_results` (PR-Cycle-B split)
- Any new tasks the audit Part (b) finds.

- [ ] **Step 4: Update header**

File: `docs/performance-baselines.md` line 4.

Replace:
```
Updated: 2026-03-31 (branch: feature/cycle2-training)
```

With:
```
Updated: 2026-05-02 (branch: chore/opt-1-config-docs-hygiene)
```

- [ ] **Step 5: Append a "Major changes since 2026-03-31" section**

After the Pipeline Timing table, add:

```markdown

## Major changes since 2026-03-31

- **2026-04-21/22 ScoutGPT cross_attention promoted** (PR #176, default flipped from `additive`); Fourier kept as enum alternative. See `docs/evolve/cross-attention-promote/SUMMARY.md`.
- **2026-04-22 XG2 production unblock** (PR #177, ADR-012). Daily `compute_xg_model_v2` back to SUCCESS after 7-day failure; new `ingestion.artifact_deploy` module codifies training→production delivery contract.
- **2026-04-23/25 Football2Vec L2 adversarial harvest** (PR #201). 6-seed sweep — no promotions; orchestrator hardening (7 rules, ADR-002 §5 lineage) is the durable deliverable.
- **2026-04-26 ExT v2 Phase 0** (PR #206). Singh-2018 baseline NLL **3.78924** held-out, 8.8M actions, 5,404 matches.
- **2026-04-27 ExT v2 Phase 1** (PR #213). KDE-smoothed Singh NLL **3.74823** (+1.082% over Phase 0; bandwidth saturated upper edge of `[0.01, 2.0]` — Phase 2 widens prior).
- **2026-04-27/28 Kimball PR 7 + 6 hotfixes** (#214–#220). `fct_action_values` rebuild times shifted; observed in this baseline refresh.
- **2026-05-01/02 PR-Cycle-C** (#243 PR-α + #247 PR-β). dbt_build split into 3 sequential tasks (`dbt_build_input_marts` / `_intermediate_marts` / `_output_marts`) per ADR-019; `compute_pausa` race fix; CAN_RUN auto-heal step.
```

---

## Task 5: Part (d) — Audit `.toPandas()` call sites + update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (single bullet line 141)

Today's CLAUDE.md says: `Budget: <5M rows for .toPandas()`. Today's fact-table sizes (per `project_pr_cycle_c_alpha_complete.md` empirical scan): `fct_tracking_frames` 38.1M, `fct_action_values` 9.5M, `fct_passes` 5.06M — all above the budget. Either tighten the rule with explicit exemption guidance for legitimate full-table call sites, or document each existing exemption in-place. Per `feedback_no_scope_decisions.md`: surface options, let user pick.

- [ ] **Step 1: Capture current call sites**

```bash
grep -rn "\.toPandas()" src/ --include="*.py" > /tmp/topandas_audit.txt
```

(Windows PowerShell: `Select-String -Path src/**/*.py -Pattern '\.toPandas\(\)' > $env:TEMP\topandas_audit.txt`.)

The output already captured in this session's research:

| File:line | Pattern | Classification |
|---|---|---|
| `elastic_sync.py:141, 240` | per-match tracking pull, with comment "~170 MB" | **bounded** (per-match filter applied) |
| `entity_resolution.py:103, 125` | TF-IDF input, ~12K players | **bounded by source size** |
| `expected_threat.py:167, 229` | SPADL actions for transition matrix; line 229 has explicit comment | **needs verification** (is there a row-cap or competition filter?) |
| `model_validation.py:82, 134, 191, 250, 294, 326` | model artefact comparisons; all have `.limit(500_000)` | **bounded** (explicit `.limit`) |
| `player_embeddings_common.py:272, 320, 369` | embedding training inputs; line 320/369 have `.limit(50_000)` | **272 needs verification**; **320/369 bounded** |
| `player_embeddings_v1.py:317` | behavioral_pdf comment says "~90K rows" | **bounded by output size** |
| `player_embeddings_v2.py:144, 387` | meta_query result | **needs verification** (what does the query select?) |
| `spadl_conversion.py:327, 332, 657` | matches metadata + team lookup | **bounded** (small dim-table-shaped pulls) |
| `statsbomb.py:169, 544` | line 169 reads competitions table (small); line 544 reads distinct competition/season pairs | **bounded** (dimension-shaped pulls) |

- [ ] **Step 2: Verify the "needs verification" rows**

For each "needs verification" row, read the surrounding code (5-line window) to determine whether a `.filter()`, `.where()`, `.limit()`, or `.select()` projects to a bounded subset BEFORE the `.toPandas()`. Document the finding inline.

- `expected_threat.py:167` — read line 145–175
- `expected_threat.py:229` — read line 215–240
- `player_embeddings_common.py:272` — read line 255–280 (this is `_load_events_sdf(...).toPandas()` — the function may bound itself)
- `player_embeddings_v2.py:144, 387` — read 130–155 and 380–400 (same pattern, twice)

The classification table above gets finalized in the PR body once each row is verified.

- [ ] **Step 3: Decide CLAUDE.md update — surface as options to user**

Two options to surface (per `feedback_no_scope_decisions.md` — do not pick, let user choose):

**Option (i)** — Tighten the budget number to match today's reality and explicitly call out exemption protocol:

```markdown
- **Avoid `.toPandas()` on unbounded tables**: Never pull an entire fact table to driver memory. Use Spark-native operations or filter to bounded subsets first. **Default budget: <5M rows**. Call sites that legitimately exceed the budget MUST add an inline comment explaining why driver memory is sufficient (e.g. per-match filter, `.limit()`, dim-table size); see `docs/engineering/databricks-serverless.md` § "Bounded `.toPandas()` exemptions" for the catalogued list.
```

**Option (ii)** — Catalogue the existing exemptions inline in CLAUDE.md instead of farming out to a separate doc:

```markdown
- **Avoid `.toPandas()` on unbounded tables**: Never pull an entire fact table to driver memory. Use Spark-native operations or filter to bounded subsets first. Default budget: <5M rows. Audit findings as of 2026-05-02 — N bounded call sites (per-match filter / `.limit(500_000)` / dim-shaped); M unbounded full-table pulls flagged for OPT-3 migration to `applyInPandas`.
```

Surface both options + the audit table to the user. Wait for explicit choice.

- [ ] **Step 4: Apply user-chosen update to CLAUDE.md line 141**

Edit `CLAUDE.md:141` per the chosen option's exact text.

- [ ] **Step 5: Verify `markdownlint` doesn't trip**

```bash
uv run pre-commit run markdownlint --files CLAUDE.md docs/performance-baselines.md
```

Expected: PASS. If markdownlint is not in `.pre-commit-config.yaml`, skip this step.

---

## Task 6: Pre-commit verification

**Files:** none modified — verification only

- [ ] **Step 1: Run all relevant pre-commit hooks against staged files**

```bash
git add terraform/modules/workflows/main.tf docs/performance-baselines.md CLAUDE.md
git diff --cached --stat
uv run pre-commit run --files terraform/modules/workflows/main.tf docs/performance-baselines.md CLAUDE.md
```

Expected: all hooks pass. The dangerous one is `terraform fmt` — if it rewrites the file mid-hook, the commit will SILENTLY FAIL (no `[main xxxx]` line in output). Recovery: re-stage the new content, request a fresh sentinel touch, retry.

- [ ] **Step 2: Run TF parity tests + ruff/pyright**

```bash
uv run pytest src/tests/test_terraform_workflow_dbt_task.py src/tests/test_terraform_env_dep_parity.py -v
uv run ruff check src/ scripts/
uv run pyright src/
```

Expected: PASS / no violations. (We didn't modify Python in OPT-1, so ruff/pyright should be a no-op net.)

- [ ] **Step 3: Show the user the staged diff**

```bash
git diff --cached
git status
```

Confirm:
1. Only `terraform/modules/workflows/main.tf` + `docs/performance-baselines.md` + `CLAUDE.md` staged.
2. `scripts/_opt1_timeout_audit.py` NOT present (deleted in Task 3 Step 6).
3. The diff matches what the user reviewed.

- [ ] **Step 4: Wait for explicit commit approval**

Per `feedback_no_commits_without_explicit_approval.md`: commit only after explicit user approval. Per `reference_git_commit_sentinel.md`: user runs `!touch ~/.claude-git-approval` immediately before the commit retry.

Show the proposed commit message:

```
chore(tf+docs): OPT-1 — config + docs hygiene cycle

- terraform/modules/workflows/main.tf: max_retries=1 added to 5
  retryless task blocks (dbt_build_input_marts, _intermediate_marts,
  _output_marts, hf_sync, refresh_synced_tables). All 5 idempotent.
- terraform/modules/workflows/main.tf: timeout_seconds right-sizing
  from observability.system_lakeflow_job_task_run_timeline 30-day p95
  measurement (full audit table in PR body). [APPLIED ROWS: ...]
- docs/performance-baselines.md: re-ran benchmark suite; refreshed
  function table + pipeline timing table; added "Major changes since
  2026-03-31" section.
- CLAUDE.md: updated .toPandas() budget rule per audit (Option [i|ii]);
  exemption catalogue [inline | linked to databricks-serverless.md].
```

Wait for approval.

- [ ] **Step 5: Sentinel-gated commit**

After user provides chat approval AND runs `!touch ~/.claude-git-approval`:

```bash
git commit -m "chore(tf+docs): OPT-1 — config + docs hygiene cycle

[full message]"
```

Verify success: `git log -1 --oneline` shows the new commit on `chore/opt-1-config-docs-hygiene`.

- [ ] **Step 6: Push branch + open PR**

Routine ops, NOT sentinel-gated. Chat approval required.

```bash
git push -u origin chore/opt-1-config-docs-hygiene
gh pr create --title "chore: OPT-1 — config + docs hygiene cycle" --body-file <body-file>
```

PR body includes:
- Audit table from Part (b) (current vs measured p95 vs proposed vs action).
- Performance baselines delta summary from Part (c).
- `.toPandas()` audit findings from Part (d).
- Cycle reference: "smoke-test cycle, single squash commit, OPT-1 of OPT-1..4 series."

- [ ] **Step 7: Report PR URL to user**

Print the PR URL. Wait for CI green before declaring success. Do NOT auto-merge — merge requires separate explicit approval.

---

## Self-Review Checklist

Run through this before announcing the plan complete:

- [ ] **Spec coverage**: All four parts (a)–(d) have tasks. ✓ Task 2 (a), Task 3 (b), Task 4 (c), Task 5 (d).
- [ ] **No placeholders**: Every step has exact commands, exact file paths, exact line numbers. ✓ verified.
- [ ] **Locked design respected**: Single squash commit; no scope decisions; live-backend fact-checking enabled. ✓.
- [ ] **Sentinel discipline**: `git commit` is sentinel-gated; routine push/PR ops are not. ✓ Task 6 split correctly.
- [ ] **Surface findings as data**: Tasks 3 + 5 explicitly call out "surface as options, do not decide" and wait for user input. ✓.
- [ ] **Files at risk respected**: dbt_project, src/ingestion, src/analytics, idsse.py untouched. ✓.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-02-opt-1-config-docs-hygiene.md`.

**Inline Execution** is the right mode for this cycle — single squash commit, four tightly-coupled parts, two explicit user-decision checkpoints (Task 3 Step 4 timeout choices; Task 5 Step 3 CLAUDE.md option choice). Subagent-driven would over-fragment a Dunkin' cycle.

Will execute via superpowers:executing-plans, stopping at the two user-decision checkpoints + the final commit/PR approval.
