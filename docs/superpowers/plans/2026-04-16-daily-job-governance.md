# Daily-Job Governance — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `terraform plan` trustworthy and workflow cards match reality for the daily Databricks job, fix the grant-script 404, and add additive least-privilege ACLs for the CI SP (admins-group removal deferred).

**Architecture:** Four focused fixes to the same governance surface (`module.workflows.databricks_job.data_ingestion` + its grants):
1. Sort `environment` and `task` blocks alphabetically in `terraform/modules/workflows/main.tf` to match the Databricks provider's internal state storage order, eliminating phantom drift.
2. Add a bidirectional workflow-card ↔ Terraform parity test using a new `trigger: orchestrated` enum value plus `sub_operations:` / `orchestrated_by:` fields on the `hf_sync` super-task and its 7 sub-operation cards.
3. Migrate `scripts/grant_synced_table_permissions.py` from raw `requests` to `databricks.sdk.WorkspaceClient` (typed responses, no silent swallows, correct project-name identifier).
4. Add explicit `databricks_permissions` IS_OWNER blocks on both jobs for the CI SP. Do **NOT** remove the `admins` group membership in this cycle.

**Tech Stack:** Terraform 1.10+, Databricks provider, `databricks-sdk` (Python), pytest, Pydantic v2 (workflow card model), PyYAML.

**Non-goals (deferred):** Removing `databricks_group_member.terraform_ci_admin`; replacing `account_admin` role; publishing freeze-frame/SPADL-VAEP/XG HF scripts workflow cards; removing dead pyproject entry_points.

---

## Cycle rules (user-stated)

- **No git commits, pushes, or PRs without explicit user approval.** Each task ends with a STOP checkpoint — present evidence, wait for approval.
- **TDD**: every behavior change has a failing test first.
- **E2E verification**: each task ends by running against live AWS/Databricks/HF state (user has granted full access).
- **Minimal commits**: target a single commit for the whole cycle if tests pass cleanly; split only if required.
- **Evidence-based decisions**: every claim must cite file:line, command output, or URL so the user can double-check.

---

## File Structure

Files created or modified, by task:

| Task | Action | Path | Responsibility |
|------|--------|------|----------------|
| 1 | Modify | `terraform/modules/workflows/main.tf` | Reorder `environment` + `task` blocks alphabetically |
| 1 | Create | `src/tests/test_workflows_tf_ordering.py` | Pytest: assert main.tf blocks are alphabetical |
| 2 | Modify | `src/workflows/card.py` | Add `"orchestrated"` to `TriggerLiteral`; add `orchestrated_by`, `sub_operations`, `parent_workflow` fields to `InferenceExecution` and a new `OrchestrationExecution` variant on `Execution` |
| 2 | Create | `workflow-cards/wf-hf-sync.yaml` | New super-task card listing 7 sub_operations |
| 2 | Modify | 7 existing cards | Change `trigger: manual` → `trigger: orchestrated`, add `orchestrated_by: wf-hf-sync` (wf-import-space-creation, wf-import-obso, wf-import-psxg, wf-football2vec-v2-export, wf-export-shots, wf-prepare-360-data, wf-sync-hf-costs) |
| 2 | Modify | 7 existing cards | Change `trigger: manual` → `trigger: scheduled` for RED-card-stale direct tasks (wf-statsbomb, wf-metrica, wf-wyscout, wf-idsse, wf-skillcorner, wf-entity-resolution, wf-elastic-sync) |
| 2 | Create | `src/tests/test_card_parity_with_terraform.py` | Bidirectional parity test — card trigger/entry_point ↔ TF task reality |
| 2 | Modify | `src/tests/test_card.py` | Add tests for new `orchestrated` trigger enum + cross-reference fields |
| 3 | Modify | `scripts/grant_synced_table_permissions.py` | Full SDK rewrite: `ws.postgres.list_projects`, `ws.permissions.get/set`, `ws.pipelines.get`. No raw `requests`. No silent-swallow. |
| 3 | Create | `src/tests/test_grant_synced_table_permissions.py` | Unit tests with SDK fakes for project-name resolution + permission-level enforcement |
| 4 | Modify | `terraform/environments/dev/main.tf` | Add two `databricks_permissions` blocks: `ci_sp_data_ingestion_owner`, `ci_sp_sync_hf_costs_owner` |

Task 1 must precede Task 4 — the phantom-drift fix makes Task 4's plan output clean enough to review.

Tasks 2 and 3 are independent of the Terraform changes and of each other.

---

## Task 1: Phantom-drift fix — alphabetical `environment` / `task` blocks

**Goal:** Eliminate the 600+ line phantom drift on `module.workflows.databricks_job.data_ingestion` by sorting main.tf blocks to match Terraform state's alphabetical storage order.

**Evidence (verified 2026-04-16)**:
- `terraform state show module.workflows.databricks_job.data_ingestion` lists `environment_key` values as `[analytics, dbt, default, embeddings, hf, statsbomb, tracking]` and `task_key` values alphabetically starting with `backfill_statsbomb_360, backfill_statsbomb_extra, compute_defcon_lite, compute_elastic_sync, ...`.
- `terraform/modules/workflows/main.tf` declares them topologically: environments `[default, statsbomb, analytics, tracking, embeddings, hf, dbt]`, tasks `[ingest_statsbomb, ingest_metrica, ingest_wyscout, ingest_idsse, ingest_skillcorner, backfill_statsbomb_extra, compute_spadl_vaep, ...]`.
- `terraform plan` reports 7 environment renames + 4 task renames as phantom drift — provider does positional block matching.
- `depends_on` references are by task_key name (semantic), so DAG is unaffected by declaration order.

### Files
- Modify: `terraform/modules/workflows/main.tf`
- Create: `src/tests/test_workflows_tf_ordering.py`

- [ ] **Step 1.1: Write failing pytest for alphabetical ordering**

Create `src/tests/test_workflows_tf_ordering.py`:

```python
"""Guardrail: main.tf task + environment blocks stay alphabetical.

The Databricks Terraform provider matches nested blocks positionally against
state. State stores blocks sorted alphabetically by key; declaring them in
any other order produces phantom drift in every `terraform plan`. This test
keeps main.tf aligned so CI plan reviews stay signal, not noise.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_MAIN_TF = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "workflows" / "main.tf"


def _extract_top_level_block_keys(text: str, block_type: str, key_field: str) -> list[str]:
    """Return the ordered list of `{key_field} = "..."` values for every
    top-level `{block_type} {{ ... }}` block inside the first `resource
    "databricks_job" "data_ingestion"` body.

    Uses brace-depth tracking so nested blocks (e.g. `depends_on` inside a
    `task`) are skipped — only depth-2 blocks count (depth 1 = resource body,
    depth 2 = top-level child block).
    """
    lines = text.splitlines()
    keys: list[str] = []
    depth = 0
    in_resource = False
    current_block: str | None = None
    block_start_depth: int | None = None
    key_pattern = re.compile(rf'^\s*{re.escape(key_field)}\s*=\s*"([^"]+)"')
    resource_start = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    block_start = re.compile(rf'^\s*{re.escape(block_type)}\s*\{{')

    for line in lines:
        if not in_resource:
            if resource_start.search(line):
                in_resource = True
                depth = 1
            continue
        open_braces = line.count("{")
        close_braces = line.count("}")
        # Detect block start at depth 1 (i.e. about to become depth 2)
        if current_block is None and depth == 1 and block_start.match(line):
            current_block = block_type
            block_start_depth = depth + open_braces
        if current_block is not None and key_pattern.match(line) and depth == block_start_depth:
            m = key_pattern.match(line)
            assert m is not None
            keys.append(m.group(1))
            current_block = None
            block_start_depth = None
        depth += open_braces - close_braces
        if depth <= 0:
            break
    return keys


def test_environment_blocks_alphabetical() -> None:
    text = _MAIN_TF.read_text(encoding="utf-8")
    env_keys = _extract_top_level_block_keys(text, "environment", "environment_key")
    assert env_keys == sorted(env_keys), (
        f"environment blocks must be sorted alphabetically to match Databricks "
        f"provider state-storage order. Got {env_keys}, expected {sorted(env_keys)}."
    )


def test_task_blocks_alphabetical() -> None:
    text = _MAIN_TF.read_text(encoding="utf-8")
    task_keys = _extract_top_level_block_keys(text, "task", "task_key")
    assert task_keys == sorted(task_keys), (
        f"task blocks must be sorted alphabetically by task_key. "
        f"Got {task_keys}, expected {sorted(task_keys)}."
    )
```

- [ ] **Step 1.2: Run test — verify it fails**

Run: `uv run pytest src/tests/test_workflows_tf_ordering.py -v`

Expected: both tests FAIL. Output should show the first out-of-order pair, e.g.:
```
AssertionError: environment blocks must be sorted alphabetically ... Got ['default', 'statsbomb', 'analytics', ...], expected ['analytics', 'dbt', 'default', ...]
```

- [ ] **Step 1.3: Reorder `environment` blocks alphabetically in main.tf**

Edit `terraform/modules/workflows/main.tf` — move the 7 top-level `environment { ... }` blocks so their `environment_key` values are in order: `analytics, dbt, default, embeddings, hf, statsbomb, tracking`.

The blocks currently start at approximately lines 800, 813, 828, 851, 865, 885, 902 (verify with `grep -n "^  environment {" terraform/modules/workflows/main.tf`). Each block is ~13-25 lines long.

Do **not** change any block content — only reorder. Verify with `git diff terraform/modules/workflows/main.tf | grep -E "^[-+]" | head -40` that only whitespace-equivalent moves happened.

- [ ] **Step 1.4: Reorder `task` blocks alphabetically in main.tf**

Move the 29 top-level `task { ... }` blocks so their `task_key` values are in alphabetical order:

```
backfill_statsbomb_360, backfill_statsbomb_extra, compute_defcon_lite, compute_elastic_sync,
compute_embeddings_360, compute_embeddings_v1, compute_embeddings_v2, compute_expected_threat,
compute_formations_efpi, compute_formations_shape_graph, compute_line_breaking, compute_off_ball_xt,
compute_pausa, compute_pitch_control, compute_spadl_vaep, compute_xg_model, compute_xg_model_v2,
dbt_build, extract_tracking_metadata, hf_sync, ingest_idsse, ingest_idsse_events, ingest_metrica,
ingest_skillcorner, ingest_statsbomb, ingest_wyscout, refresh_synced_tables, resolve_players,
run_model_validation
```

Again, do not change any block content — only reorder. `depends_on` references are semantic (by task_key), so DAG is preserved.

- [ ] **Step 1.5: Run `terraform fmt` and the pytest**

```bash
cd terraform/modules/workflows && terraform fmt main.tf
cd ../../.. && uv run pytest src/tests/test_workflows_tf_ordering.py -v
```

Expected: `terraform fmt` touches whitespace only; pytest: 2 passed.

- [ ] **Step 1.6: E2E — live `terraform plan` must be clean**

```bash
AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev plan \
  -target=module.workflows.databricks_job.data_ingestion -no-color 2>&1 | tail -20
```

Expected: `No changes. Your infrastructure matches the configuration.` OR at most `0 to add, 0 to change, 0 to destroy.`

If drift is still reported, inspect the plan diff — residual drift is a separate provider bug and must be investigated before proceeding (per "three-strikes rule" in CLAUDE.md).

- [ ] **Step 1.7: STOP — present evidence, await user approval**

Present to user:
- pytest output (2 passed)
- `terraform plan` tail showing "No changes"
- `git diff --stat terraform/modules/workflows/main.tf` (line count; should be high churn on a single file)

**Do not commit.**

---

## Task 2: D63 — workflow card ↔ Terraform parity with explicit orchestration

**Goal:** Make every card's declared `trigger` match how it is actually invoked. Introduce `trigger: orchestrated` + bidirectional `sub_operations:` / `orchestrated_by:` fields so the `hf_sync` super-task pattern is explicit and auditable. Add a parity pytest that catches future drift.

**Evidence (verified 2026-04-16)**:
- Live daily job has 29 tasks (confirmed via `ws.jobs.get(job_id=302697362345215)`).
- `src/ingestion/hf_sync.py:104-112` defines `_SUB_OPERATIONS` calling 7 modules sequentially: `import_space_creation, import_obso_results, import_psxg_predictions, export_embeddings_training_data, export_shots_on_target, prepare_360_training_data, sync_hf_costs`.
- Delta history of `soccer_analytics.bronze.pausa_raw_scores` shows 27 WRITEs by the ingestion SP (`008b207b-96a8-4d54-b185-a77479a55abe`) on ephemeral job clusters at daily cadence — the only path is via `hf_sync` → `import_obso_results.run_pipeline()` (`import_obso_results.py:141-155` writes to that table with `replace_where="match_id IN (...)"`, matching v27 parameters exactly).
- 8 direct RED-card-stale cards: `wf-statsbomb, wf-metrica, wf-wyscout, wf-idsse, wf-skillcorner, wf-entity-resolution, wf-elastic-sync, wf-import-obso` (card says manual, TF schedules it directly or via hf_sync).

### Files
- Modify: `src/workflows/card.py`
- Modify: `src/tests/test_card.py`
- Create: `workflow-cards/wf-hf-sync.yaml`
- Modify: `workflow-cards/wf-import-obso.yaml`, `wf-import-space-creation.yaml`, `wf-import-psxg.yaml`, `wf-football2vec-v2-export.yaml`, `wf-export-shots.yaml`, `wf-prepare-360-data.yaml`, `wf-sync-hf-costs.yaml`
- Modify: `workflow-cards/wf-statsbomb.yaml`, `wf-metrica.yaml`, `wf-wyscout.yaml`, `wf-idsse.yaml`, `wf-skillcorner.yaml`, `wf-entity-resolution.yaml`, `wf-elastic-sync.yaml`
- Create: `src/tests/test_card_parity_with_terraform.py`

### 2A — Update the card Pydantic model

- [ ] **Step 2A.1: Write failing tests for the new schema**

Append to `src/tests/test_card.py`:

```python
# ---------------------------------------------------------------------------
# New tests: orchestrated trigger + sub_operations cross-reference
# ---------------------------------------------------------------------------

def test_trigger_literal_allows_orchestrated() -> None:
    """Sub-operation cards use trigger=orchestrated + orchestrated_by."""
    yaml_body = textwrap.dedent("""\
        ---
        name: Import OBSO
        id: wf-import-obso
        version: "1.0.0"
        status: production
        type: data-movement
        domain: soccer-analytics
        owners: [karsten]
        execution:
          import:
            trigger: orchestrated
            orchestrated_by: wf-hf-sync
            runtime: databricks-workflow
            entry_point: import_obso_results
            module: ingestion.import_obso_results
            distribution: driver-bound
            timeout: "900s"
        ---
    """)
    card = WorkflowCard.from_yaml_text(yaml_body, filename="wf-import-obso.yaml")
    assert card.execution.root["import"].trigger == "orchestrated"
    assert card.execution.root["import"].orchestrated_by == "wf-hf-sync"


def test_orchestrated_requires_orchestrated_by() -> None:
    """trigger=orchestrated without orchestrated_by must fail validation."""
    yaml_body = textwrap.dedent("""\
        ---
        name: Bad
        id: wf-bad
        version: "1.0.0"
        status: production
        type: data-movement
        domain: soccer-analytics
        owners: [karsten]
        execution:
          import:
            trigger: orchestrated
            runtime: databricks-workflow
            entry_point: x
            module: y.z
            distribution: driver-bound
            timeout: "900s"
        ---
    """)
    with pytest.raises(ValidationError, match="orchestrated_by"):
        WorkflowCard.from_yaml_text(yaml_body, filename="wf-bad.yaml")


def test_orchestration_execution_sub_operations() -> None:
    """Super-task cards can declare orchestration.sub_operations."""
    yaml_body = textwrap.dedent("""\
        ---
        name: HF Sync Super-task
        id: wf-hf-sync
        version: "1.0.0"
        status: production
        type: data-movement
        domain: soccer-analytics
        owners: [karsten]
        execution:
          orchestration:
            trigger: scheduled
            runtime: databricks-workflow
            entry_point: hf_sync
            module: ingestion.hf_sync
            distribution: driver-bound
            timeout: "1800s"
            sub_operations:
              - wf-import-obso
              - wf-sync-hf-costs
        ---
    """)
    card = WorkflowCard.from_yaml_text(yaml_body, filename="wf-hf-sync.yaml")
    assert card.execution.root["orchestration"].sub_operations == ["wf-import-obso", "wf-sync-hf-costs"]


def test_sub_operations_forbidden_on_non_orchestration_phase() -> None:
    """sub_operations on a non-orchestration phase must fail."""
    yaml_body = textwrap.dedent("""\
        ---
        name: Bad
        id: wf-bad
        version: "1.0.0"
        status: production
        type: training-and-inference
        domain: x
        owners: [karsten]
        execution:
          inference:
            trigger: scheduled
            runtime: databricks-workflow
            entry_point: x
            module: y.z
            distribution: driver-bound
            timeout: "900s"
            sub_operations: [wf-other]
        ---
    """)
    with pytest.raises(ValidationError):
        WorkflowCard.from_yaml_text(yaml_body, filename="wf-bad.yaml")
```

- [ ] **Step 2A.2: Run tests — verify they fail**

Run: `uv run pytest src/tests/test_card.py -v -k "orchestrated or sub_operations"`

Expected: 4 FAIL with messages like "orchestrated is not a valid value" or "'orchestrated_by' is not a field".

- [ ] **Step 2A.3: Extend the card model**

Edit `src/workflows/card.py`:

1. Change `TriggerLiteral` (line ~38):
   ```python
   TriggerLiteral = Literal["manual", "scheduled", "event-driven", "orchestrated"]
   ```

2. In `class InferenceExecution` (line ~120), add optional field AFTER the existing fields:
   ```python
   class InferenceExecution(BaseModel):
       trigger: TriggerLiteral
       runtime: RuntimeLiteral
       entry_point: str
       module: str
       distribution: DistributionLiteral
       partition_key: str | None = None
       schedule: str | None = None
       timeout: str
       environment: str | None = None
       orchestrated_by: str | None = None  # NEW — workflow id of parent super-task

       @model_validator(mode="after")
       def _orchestrated_requires_parent(self) -> "InferenceExecution":
           if self.trigger == "orchestrated" and not self.orchestrated_by:
               msg = "trigger='orchestrated' requires orchestrated_by field"
               raise ValueError(msg)
           if self.trigger != "orchestrated" and self.orchestrated_by:
               msg = "orchestrated_by is only valid when trigger='orchestrated'"
               raise ValueError(msg)
           return self
   ```
   Add `from pydantic import model_validator` to the imports at top.

3. Add a new `OrchestrationExecution` class right above `class Execution`:
   ```python
   class OrchestrationExecution(BaseModel):
       """Super-task execution that fans out into declared sub-operation workflow cards."""

       trigger: TriggerLiteral
       runtime: RuntimeLiteral
       entry_point: str
       module: str
       distribution: DistributionLiteral
       timeout: str
       environment: str | None = None
       schedule: str | None = None
       sub_operations: list[str] = Field(default_factory=list)

       @model_validator(mode="after")
       def _sub_operations_non_empty(self) -> "OrchestrationExecution":
           if not self.sub_operations:
               msg = "orchestration phase requires a non-empty sub_operations list"
               raise ValueError(msg)
           return self
   ```

4. Update `class Execution` to expose the new phase:
   ```python
   class Execution(BaseModel):
       training: TrainingExecution | None = None
       inference: InferenceExecution | None = None
       export: InferenceExecution | None = None
       ingestion: InferenceExecution | None = None
       sync: InferenceExecution | None = None
       orchestration: OrchestrationExecution | None = None  # NEW

       model_config = ConfigDict(extra="allow")
   ```
   Note: the existing `extra="allow"` already permits `import:` as an extra field (Python-reserved keyword).

- [ ] **Step 2A.4: Run the tests — verify pass**

Run: `uv run pytest src/tests/test_card.py -v`

Expected: all tests pass (existing tests unchanged, 4 new tests green).

- [ ] **Step 2A.5: Run pyright + ruff**

```bash
uv run pyright src/workflows/card.py src/tests/test_card.py
uv run ruff check src/workflows/card.py src/tests/test_card.py
uv run ruff format --check src/workflows/card.py src/tests/test_card.py
```

Expected: all green.

### 2B — Create the super-task card + update sub-operation cards

- [ ] **Step 2B.1: Create `workflow-cards/wf-hf-sync.yaml`**

Write the new file:

```yaml
---
name: HF Sync Super-task
id: wf-hf-sync
version: "1.0.0"
status: production
type: data-movement
domain: soccer-analytics
owners:
  - karsten
tags:
  - orchestration
  - huggingface
  - daily-job

# No academic methodology — operational orchestration layer that fans out
# into 7 independent sub-operation workflows. See src/ingestion/hf_sync.py
# for the _SUB_OPERATIONS list.
references: []

inputs:
  datasets: []

outputs:
  tables: []

execution:
  orchestration:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: hf_sync
    module: ingestion.hf_sync
    distribution: driver-bound
    timeout: "1800s"
    environment: hf
    sub_operations:
      - wf-import-space-creation
      - wf-import-obso
      - wf-import-psxg
      - wf-football2vec-v2-export
      - wf-export-shots
      - wf-prepare-360-data
      - wf-sync-hf-costs

depends_on:
  - wf-vaep
  - wf-entity-resolution
  - wf-xg-v1
  - wf-elastic-sync

idempotency:
  strategy: none
  key: sub-operation-level
  description: "Each sub-operation has its own idempotency strategy (skip-guard or upsert). This super-task does not itself track state."

performance:
  inference_timeout: "1800s"
  memory_ceiling: "16 GB driver"

cost:
  inference:
    runtime: databricks
    sku: "jobs_serverless_compute_run_dbus"
    typical_dbu: 8
    typical_cost_usd: 0.5

monitoring:
  freshness_sla_hours: 48

links:
  source_code:
    - "src/ingestion/hf_sync.py"
---

## Overview

The `hf_sync` task is the daily job's final orchestration stage. It iterates
through 7 sub-operations in order, each wrapped in a best-effort error
boundary (failures logged at ERROR but do not abort the parent task):

1. Import space creation data from HF Hub
2. Import OBSO/PAUSA results from HF Hub
3. Import PSXG predictions from HF Hub
4. Export football2vec v2 training data to HF Hub
5. Export shots-on-target to HF Hub
6. Prepare 360 training data for HF Jobs
7. Sync HF Jobs cost telemetry into Delta

The parent task runs once daily via the `soccer-analytics-ingestion-dev`
Databricks job (`terraform/modules/workflows/main.tf:~719`); each
sub-operation's own workflow card declares `trigger: orchestrated` and
`orchestrated_by: wf-hf-sync`.
```

- [ ] **Step 2B.2: Update each of the 7 sub-operation cards**

For each card in the list below, change the existing phase block so `trigger: manual` becomes `trigger: orchestrated` and add `orchestrated_by: wf-hf-sync` immediately after the `trigger:` line.

Cards to update (with the phase key each uses):

| Card file | Phase key | Line number hint |
|-----------|-----------|------------------|
| `workflow-cards/wf-import-space-creation.yaml` | `import` | :~33 |
| `workflow-cards/wf-import-obso.yaml` | `import` | :37 |
| `workflow-cards/wf-import-psxg.yaml` | `import` | :~33 |
| `workflow-cards/wf-football2vec-v2-export.yaml` | `export` | :~37 |
| `workflow-cards/wf-export-shots.yaml` | `export` | :~35 |
| `workflow-cards/wf-prepare-360-data.yaml` | `export` | :~40 |
| `workflow-cards/wf-sync-hf-costs.yaml` | `sync` | :37 |

Example diff for `wf-import-obso.yaml`:
```yaml
execution:
  import:
-   trigger: manual
+   trigger: orchestrated
+   orchestrated_by: wf-hf-sync
    runtime: databricks-workflow
    entry_point: import_obso_results
```

For `wf-sync-hf-costs.yaml` specifically, also remove the inaccurate `schedule: "every 15 minutes"` line and keep `runtime: databricks-workflow` — the actual cadence is daily via the parent. If the standalone `sync_hf_costs_daily` job is later re-enabled, that can be documented separately.

### 2C — Update the 7 RED-card-stale direct-task cards

- [ ] **Step 2C.1: Change `trigger: manual` → `trigger: scheduled` on 7 cards**

These cards describe tasks that ARE direct entries in the daily job (not orchestrated under `hf_sync`). Per live `ws.jobs.get` output, the following entry_points are direct tasks:

| Card file | Entry point | TF task (main.tf line) |
|-----------|-------------|------------------------|
| `workflow-cards/wf-statsbomb.yaml` | `ingest_statsbomb` | :~69 |
| `workflow-cards/wf-metrica.yaml` | `ingest_metrica` | :~88 |
| `workflow-cards/wf-wyscout.yaml` | `ingest_wyscout` | :~108 |
| `workflow-cards/wf-idsse.yaml` | `ingest_idsse` | :~129 |
| `workflow-cards/wf-skillcorner.yaml` | `ingest_skillcorner` | :~149 |
| `workflow-cards/wf-entity-resolution.yaml` | `resolve_players` | :~465 |
| `workflow-cards/wf-elastic-sync.yaml` | `compute_elastic_sync` | :~595 |

For each, change exactly one line:
```yaml
-   trigger: manual
+   trigger: scheduled
```

Do not touch any other field.

### 2D — Parity test

- [ ] **Step 2D.1: Write failing parity test**

Create `src/tests/test_card_parity_with_terraform.py`:

```python
"""Workflow card ↔ Terraform daily-job parity.

Rules:
  1. Every direct task in the daily job has a card with trigger=scheduled and
     a matching entry_point.
  2. Every card with trigger=scheduled or trigger=orchestrated has its
     entry_point present in pyproject.toml [project.scripts].
  3. Every module in src/ingestion/hf_sync.py:_SUB_OPERATIONS maps to exactly
     one card with trigger=orchestrated + orchestrated_by=wf-hf-sync.
  4. Every card declaring orchestrated_by=<id> has its id listed in card <id>'s
     execution.orchestration.sub_operations.
  5. Every sub_operations entry in the super-task card corresponds to a real
     sub-operation card whose orchestrated_by points back.
  6. trigger=orchestrated requires orchestrated_by (Pydantic already enforces);
     this test enforces the inverse — no card declares orchestrated_by without
     trigger=orchestrated. (Redundant belt-and-braces — Pydantic is the belt.)
"""

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_CARDS_DIR = _REPO / "workflow-cards"
_MAIN_TF = _REPO / "terraform" / "modules" / "workflows" / "main.tf"
_HF_SYNC = _REPO / "src" / "ingestion" / "hf_sync.py"
_PYPROJECT = _REPO / "pyproject.toml"

_FRONTMATTER = re.compile(r"^---\s*\n(.*?\n)---\s*\n", re.DOTALL)


def _load_card(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    m = _FRONTMATTER.match(text)
    if not m:
        pytest.fail(f"Card {path.name} has no YAML frontmatter")
    return yaml.safe_load(m.group(1))


def _card_phases(card: dict) -> dict[str, dict]:
    return {k: v for k, v in (card.get("execution") or {}).items() if isinstance(v, dict)}


def _parse_tf_task_entry_points() -> dict[str, str]:
    """Return {task_key: entry_point} for every top-level task in the daily job.

    Uses a line-based scanner with brace-depth tracking — good enough for
    HCL's restricted task { ... } shape. No HCL parser dependency added."""
    text = _MAIN_TF.read_text(encoding="utf-8")
    lines = text.splitlines()
    depth = 0
    in_resource = False
    result: dict[str, str] = {}
    current_task: dict[str, str | None] = {"task_key": None, "entry_point": None}
    in_task_depth: int | None = None
    resource_re = re.compile(r'^resource\s+"databricks_job"\s+"data_ingestion"\s*\{')
    task_re = re.compile(r"^\s*task\s*\{")
    task_key_re = re.compile(r'^\s*task_key\s*=\s*"([^"]+)"')
    entry_point_re = re.compile(r'^\s*entry_point\s*=\s*"([^"]+)"')

    for line in lines:
        if not in_resource:
            if resource_re.search(line):
                in_resource = True
                depth = 1
            continue
        opens = line.count("{")
        closes = line.count("}")
        if in_task_depth is None and depth == 1 and task_re.match(line):
            in_task_depth = depth + opens
            current_task = {"task_key": None, "entry_point": None}
        if in_task_depth is not None:
            if current_task["task_key"] is None:
                m = task_key_re.match(line)
                if m:
                    current_task["task_key"] = m.group(1)
            if current_task["entry_point"] is None:
                m = entry_point_re.match(line)
                if m:
                    current_task["entry_point"] = m.group(1)
        depth += opens - closes
        if in_task_depth is not None and depth < in_task_depth:
            if current_task["task_key"] and current_task["entry_point"]:
                result[current_task["task_key"]] = current_task["entry_point"]
            in_task_depth = None
        if depth <= 0:
            break
    return result


def _parse_hf_sync_sub_operations() -> list[str]:
    """Return the module paths in hf_sync.py:_SUB_OPERATIONS via AST."""
    tree = ast.parse(_HF_SYNC.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "_SUB_OPERATIONS":
            assert isinstance(node.value, ast.List), "_SUB_OPERATIONS must be a list literal"
            modules: list[str] = []
            for item in node.value.elts:
                assert isinstance(item, ast.Tuple) and len(item.elts) == 2
                assert isinstance(item.elts[0], ast.Constant)
                modules.append(item.elts[0].value)
            return modules
    pytest.fail("_SUB_OPERATIONS not found in hf_sync.py")


def _parse_pyproject_entry_points() -> dict[str, str]:
    data = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    return dict((data.get("project") or {}).get("scripts") or {})


# ---------------------------------------------------------------------------
# Rule 1: direct tasks in TF → scheduled cards (the inverse of RED-card-stale)
# ---------------------------------------------------------------------------

_DIRECT_TASK_ENTRY_POINT_TO_CARD = {
    "ingest_statsbomb": "wf-statsbomb",
    "ingest_metrica": "wf-metrica",
    "ingest_wyscout": "wf-wyscout",
    "ingest_idsse": "wf-idsse",
    "ingest_skillcorner": "wf-skillcorner",
    "ingest_idsse_events": None,  # no card — governance gap accepted, tracked in audit
    "backfill_statsbomb_extra": None,  # no card
    "backfill_statsbomb_360": None,  # no card
    "compute_spadl_vaep": "wf-vaep",
    "compute_expected_threat": "wf-xt-grids",
    "compute_xg_model": "wf-xg-v1",
    "compute_xg_model_v2": "wf-xg-v2",
    "compute_off_ball_xt": "wf-off-ball-xt",
    "compute_pitch_control": "wf-pitch-control",
    "compute_formations_efpi": "wf-formations",
    "compute_formations_shape_graph": "wf-shape-graphs",
    "compute_line_breaking": "wf-line-breaking",
    "compute_defcon_lite": "wf-defcon",
    "resolve_players": "wf-entity-resolution",
    "compute_embeddings_v2": "wf-football2vec-v2",
    "compute_embeddings_v1": "wf-football2vec",
    "compute_embeddings_360": "wf-football2vec-360",
    "compute_elastic_sync": "wf-elastic-sync",
    "compute_pausa": "wf-obso-pausa",
    "run_model_validation": "wf-model-validation",
    "extract_tracking_metadata": None,  # no card
    "hf_sync": "wf-hf-sync",
    "dbt_build": "wf-dbt-build",
    "refresh_synced_tables": None,  # no card (governance gap accepted)
}


def test_every_tf_task_either_maps_to_scheduled_card_or_is_documented_gap() -> None:
    tf_tasks = _parse_tf_task_entry_points()
    unexpected = set(tf_tasks) - set(_DIRECT_TASK_ENTRY_POINT_TO_CARD)
    assert not unexpected, (
        f"TF has task(s) not classified in this test's mapping: {unexpected}. "
        "Add them to _DIRECT_TASK_ENTRY_POINT_TO_CARD (with None for intentional gaps)."
    )
    missing = set(_DIRECT_TASK_ENTRY_POINT_TO_CARD) - set(tf_tasks)
    assert not missing, (
        f"Mapping references TF tasks that no longer exist: {missing}. "
        "Either remove the entry or restore the TF task."
    )

    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for entry_point, card_id in _DIRECT_TASK_ENTRY_POINT_TO_CARD.items():
        if card_id is None:
            continue
        if card_id not in cards:
            errors.append(f"Mapping points at missing card {card_id!r}")
            continue
        phases = _card_phases(cards[card_id])
        # Find the phase that declares this entry_point OR (for hf-sync) the orchestration block
        matched = False
        for phase_name, phase in phases.items():
            if phase.get("entry_point") == entry_point:
                expected = "orchestrated" if phase_name == "orchestration" else "scheduled"
                actual = phase.get("trigger")
                # hf_sync is special: super-task phase is `orchestration` with trigger=scheduled
                if card_id == "wf-hf-sync" and phase_name == "orchestration":
                    expected = "scheduled"
                if actual != expected:
                    errors.append(
                        f"{card_id} phase {phase_name!r} declares entry_point={entry_point!r} "
                        f"but trigger={actual!r} (expected {expected!r} — it is a direct TF task)"
                    )
                matched = True
                break
        if not matched:
            errors.append(f"{card_id} does not declare any phase with entry_point={entry_point!r}")
    assert not errors, "\n".join(errors)


# ---------------------------------------------------------------------------
# Rule 3/4/5: hf_sync super-task bidirectional parity
# ---------------------------------------------------------------------------


def test_hf_sync_sub_operations_bidirectional() -> None:
    modules = _parse_hf_sync_sub_operations()
    # Map hf_sync.py module path -> expected card id (derived from module name)
    module_to_card = {
        "ingestion.import_space_creation": "wf-import-space-creation",
        "ingestion.import_obso_results": "wf-import-obso",
        "ingestion.import_psxg_predictions": "wf-import-psxg",
        "ingestion.export_embeddings_training_data": "wf-football2vec-v2-export",
        "ingestion.export_shots_on_target": "wf-export-shots",
        "ingestion.prepare_360_training_data": "wf-prepare-360-data",
        "ingestion.sync_hf_costs": "wf-sync-hf-costs",
    }
    expected_sub_ops = [module_to_card[m] for m in modules]

    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    assert "wf-hf-sync" in cards, "wf-hf-sync.yaml must exist as the super-task card"

    hf_sync_phases = _card_phases(cards["wf-hf-sync"])
    orch = hf_sync_phases.get("orchestration")
    assert orch, "wf-hf-sync must declare an execution.orchestration phase"
    declared = list(orch.get("sub_operations") or [])
    assert declared == expected_sub_ops, (
        f"wf-hf-sync.execution.orchestration.sub_operations must match "
        f"hf_sync.py:_SUB_OPERATIONS order.\n  declared: {declared}\n  expected: {expected_sub_ops}"
    )

    # Each sub_op card declares orchestrated_by: wf-hf-sync with trigger=orchestrated
    for sub_op_id in expected_sub_ops:
        assert sub_op_id in cards, f"Missing sub-operation card {sub_op_id}"
        phases = _card_phases(cards[sub_op_id])
        match = None
        for phase_name, phase in phases.items():
            if phase.get("trigger") == "orchestrated":
                match = (phase_name, phase)
                break
        assert match, f"{sub_op_id} must have a phase with trigger=orchestrated"
        phase_name, phase = match
        assert phase.get("orchestrated_by") == "wf-hf-sync", (
            f"{sub_op_id} phase {phase_name!r} must declare orchestrated_by: wf-hf-sync "
            f"(got {phase.get('orchestrated_by')!r})"
        )


# ---------------------------------------------------------------------------
# Rule 2: every scheduled or orchestrated card has its entry_point in pyproject.toml
# ---------------------------------------------------------------------------


def test_scheduled_and_orchestrated_cards_have_entry_points_in_pyproject() -> None:
    scripts = _parse_pyproject_entry_points()
    cards = {p.stem: _load_card(p) for p in _CARDS_DIR.glob("wf-*.yaml")}
    errors: list[str] = []
    for card_id, card in cards.items():
        for phase_name, phase in _card_phases(card).items():
            trig = phase.get("trigger")
            if trig not in ("scheduled", "orchestrated"):
                continue
            entry = phase.get("entry_point")
            if entry and entry not in scripts:
                errors.append(
                    f"{card_id} phase {phase_name!r} declares entry_point={entry!r} "
                    f"(trigger={trig!r}) but it is absent from pyproject.toml [project.scripts]"
                )
    assert not errors, "\n".join(errors)
```

- [ ] **Step 2D.2: Run parity test — expect PASS (all card updates done)**

Run: `uv run pytest src/tests/test_card_parity_with_terraform.py -v`

Expected: 3 PASS. If any fails, the earlier card edits missed something — re-read the failure and fix the specific card.

- [ ] **Step 2D.3: Run existing card-validator CLI**

Run: `uv run validate_workflow_cards`

Expected: "36/36 valid" — or 37 if the validator counts `wf-hf-sync.yaml`. No parse errors.

- [ ] **Step 2D.4: E2E — sanity-check card consumers still work**

Taipy's workflow page reads cards. Run (no need to start the full app):
```bash
uv run python -c "
from workflows.card import WorkflowCard
from pathlib import Path
for p in sorted(Path('workflow-cards').glob('wf-*.yaml')):
    c = WorkflowCard.from_yaml_file(p)
    print(p.name, 'OK')
" 2>&1 | tail -5
```

Expected: every card prints OK.

- [ ] **Step 2D.5: STOP — present evidence, await user approval**

Present to user:
- pytest output for `test_card.py` and `test_card_parity_with_terraform.py`
- `validate_workflow_cards` output
- `git diff --stat workflow-cards/ src/workflows/card.py src/tests/` (expect ~17 files changed)

**Do not commit.**

---

## Task 3: Grant script 404 fix — migrate to `databricks-sdk`

**Goal:** Replace raw `requests` calls in `scripts/grant_synced_table_permissions.py` with typed `databricks-sdk` calls. Resolve the project by short name (not UID) so the permissions endpoint stops 404ing. Eliminate silent-swallow error-handling.

**Evidence (verified 2026-04-16 against live workspace)**:
- `GET /api/2.0/permissions/database-projects/342068ec-...-bed5-0aa4cbf326ba` → 404
- `GET /api/2.0/permissions/database-projects/soccer-analytics-dev` → 200 with typed ACL
- `ws.postgres.list_projects()` returns `[Project(name='projects/soccer-analytics-dev', uid='342068ec-...')]`
- `ws.permissions.get(request_object_type='database-projects', request_object_id='soccer-analytics-dev')` returns correct ACL
- Current script's `_show_status` at line 141-153 has a silent `if r.ok:` branch — it skips entirely on failure with no log. That is why `--status` emitted zero `project_acl_entry` events.

### Files
- Modify: `scripts/grant_synced_table_permissions.py`
- Create: `src/tests/test_grant_synced_table_permissions.py`

- [ ] **Step 3.1: Write failing unit tests**

Create `src/tests/test_grant_synced_table_permissions.py`:

```python
"""Unit tests for grant_synced_table_permissions module-level helpers.

Covers:
  - Project short-name resolution (strips 'projects/' prefix from Project.name)
  - Permissions GET/SET path uses SDK, not raw HTTP
  - Non-OK responses are RAISED, not silently logged-and-skipped
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

# Add scripts/ to path so we can import the script as a module.
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

import grant_synced_table_permissions as mod  # noqa: E402


# ---------------------------------------------------------------------------
# _resolve_database_project_name
# ---------------------------------------------------------------------------


def test_resolve_database_project_name_strips_prefix() -> None:
    ws = MagicMock()
    ws.postgres.list_projects.return_value = [
        SimpleNamespace(name="projects/soccer-analytics-dev", uid="342068ec-..."),
        SimpleNamespace(name="projects/other-project", uid="zzz"),
    ]
    name = mod._resolve_database_project_name(ws, synced_table_full_name="soccer_analytics.dev_gold.fct_shots_synced")
    # The function resolves via the synced table's effective_database_project_id,
    # then maps that UID back to the short project name.
    ws.database.get_synced_database_table.return_value = SimpleNamespace(
        effective_database_project_id="342068ec-..."
    )
    name = mod._resolve_database_project_name(ws, synced_table_full_name="soccer_analytics.dev_gold.fct_shots_synced")
    assert name == "soccer-analytics-dev"


def test_resolve_project_name_raises_when_not_found() -> None:
    ws = MagicMock()
    ws.database.get_synced_database_table.return_value = SimpleNamespace(effective_database_project_id="ghost-uid")
    ws.postgres.list_projects.return_value = [
        SimpleNamespace(name="projects/other", uid="another-uid"),
    ]
    with pytest.raises(RuntimeError, match="ghost-uid"):
        mod._resolve_database_project_name(ws, synced_table_full_name="x.y.z")


# ---------------------------------------------------------------------------
# Permission GET/SET use SDK; errors surface loudly
# ---------------------------------------------------------------------------


def test_show_status_logs_project_acl_entries(capsys) -> None:
    """On success, _show_status must emit at least one project_acl_entry event."""
    ws = MagicMock()
    ws.permissions.get.return_value = SimpleNamespace(
        access_control_list=[
            SimpleNamespace(
                user_name=None,
                group_name=None,
                service_principal_name="sp-app-id",
                all_permissions=[SimpleNamespace(permission_level="CAN_USE")],
            )
        ]
    )
    mod._show_project_acl(ws, project_name="soccer-analytics-dev", sp_app_ids={("hf", "sp-app-id")})
    out = capsys.readouterr().out
    assert '"event": "project_acl_entry"' in out
    assert '"principal": "sp-app-id"' in out


def test_show_status_raises_on_sdk_error() -> None:
    """On SDK failure, _show_project_acl must raise — no silent swallow."""
    from databricks.sdk.errors import DatabricksError

    ws = MagicMock()
    ws.permissions.get.side_effect = DatabricksError("boom")
    with pytest.raises(DatabricksError):
        mod._show_project_acl(ws, project_name="soccer-analytics-dev", sp_app_ids=set())
```

- [ ] **Step 3.2: Run tests — expect FAIL (functions don't exist yet)**

Run: `uv run pytest src/tests/test_grant_synced_table_permissions.py -v`

Expected: FAIL with `AttributeError: module 'grant_synced_table_permissions' has no attribute '_resolve_database_project_name'` (or similar).

- [ ] **Step 3.3: Rewrite `scripts/grant_synced_table_permissions.py`**

Replace the entire file with the SDK-based version. Key changes:

1. Remove all `requests` imports and raw HTTP helpers (`_host`, `_patch_acl`, `_enumerate_pipeline_ids` via HTTP).
2. Remove `from ingestion.refresh_synced_tables import ... _get_auth_headers`.
3. Add typed SDK-based helpers. The new file structure:

```python
#!/usr/bin/env python3
"""Grant Lakebase synced table refresh permissions to service principals.

Uses the Databricks SDK throughout — no raw HTTP. See the module docstring
at the top of the previous revision for the rationale and modes.

Usage:
    uv run python scripts/grant_synced_table_permissions.py
    uv run python scripts/grant_synced_table_permissions.py --status
    uv run python scripts/grant_synced_table_permissions.py --dry-run
    uv run python scripts/grant_synced_table_permissions.py --revoke
    uv run python scripts/grant_synced_table_permissions.py --environment prod
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

from ingestion.refresh_synced_tables import DEFAULT_CATALOG, DEFAULT_SCHEMA, SYNCED_TABLES

_LOG_SOURCE = "grant_synced_table_permissions"

DATABASE_PROJECT_PERMISSION = PermissionLevel.CAN_USE
PIPELINE_PERMISSION = PermissionLevel.CAN_RUN

HF_APP_SP_NAME_PATTERN = "luxury-lakehouse-hf-app-v2-{env}"
INGESTION_SP_NAME_PATTERN = "luxury-lakehouse-ingestion-{env}"

_PROJECT_NAME_PREFIX = "projects/"


def _log(event: str, **kwargs: object) -> None:
    record = {"source": _LOG_SOURCE, "event": event, **kwargs}
    print(json.dumps(record, default=str), flush=True)


def _resolve_sp_app_id(ws: WorkspaceClient, display_name: str) -> str:
    for sp in ws.service_principals.list():
        if sp.display_name == display_name:
            if not sp.application_id:
                raise RuntimeError(f"Service principal {display_name!r} has no application_id")
            return sp.application_id
    raise RuntimeError(f"Service principal {display_name!r} not found in workspace")


def _resolve_database_project_name(ws: WorkspaceClient, synced_table_full_name: str) -> str:
    """Resolve the short project name (e.g. 'soccer-analytics-dev') for a synced table.

    The Databricks Permissions API for 'database-projects' identifies projects
    by their short name (the part after 'projects/'), NOT by the UID returned
    in the synced-table metadata's effective_database_project_id. This helper
    bridges the two.
    """
    meta = ws.database.get_synced_database_table(synced_table_full_name)
    uid = meta.effective_database_project_id
    if not uid:
        raise RuntimeError(f"Synced table {synced_table_full_name} has no effective_database_project_id")
    for project in ws.postgres.list_projects():
        if project.uid == uid:
            if not project.name or not project.name.startswith(_PROJECT_NAME_PREFIX):
                raise RuntimeError(f"Project {project.uid!r} has unexpected name {project.name!r}")
            return project.name[len(_PROJECT_NAME_PREFIX):]
    raise RuntimeError(f"No Lakebase project has uid={uid!r}; cannot resolve permissions-API name")


def _enumerate_pipelines(ws: WorkspaceClient) -> list[tuple[str, str, str]]:
    """Resolve all synced tables' backing pipeline_ids via the SDK.

    Returns list of (table_name, schema, pipeline_id). Raises on resolve
    failure — no silent drops (previous behavior buried failures in --status).
    """
    resolved: list[tuple[str, str, str]] = []
    for table_name, schema_override in SYNCED_TABLES:
        schema = schema_override or DEFAULT_SCHEMA
        full = f"{DEFAULT_CATALOG}.{schema}.{table_name}"
        meta = ws.database.get_synced_database_table(full)
        pid = meta.data_synchronization_status.pipeline_id if meta.data_synchronization_status else None
        if not pid:
            raise RuntimeError(f"Synced table {full} has no pipeline_id in data_synchronization_status")
        resolved.append((table_name, schema, pid))
    return resolved


def _principal_name(entry) -> str:
    return entry.user_name or entry.group_name or entry.service_principal_name or "?"


def _permission_levels(entry) -> list[str]:
    return [p.permission_level.value if hasattr(p.permission_level, "value") else str(p.permission_level)
            for p in (entry.all_permissions or [])]


def _show_project_acl(ws: WorkspaceClient, project_name: str, sp_app_ids: set[tuple[str, str]]) -> None:
    acl = ws.permissions.get(request_object_type="database-projects", request_object_id=project_name)
    target_set = {app_id for _, app_id in sp_app_ids}
    for entry in (acl.access_control_list or []):
        principal = _principal_name(entry)
        _log(
            "project_acl_entry",
            principal=principal,
            permissions=_permission_levels(entry),
            is_target_sp=principal in target_set,
        )


def _show_pipeline_acl(ws: WorkspaceClient, table: str, pipeline_id: str) -> None:
    acl = ws.permissions.get(request_object_type="pipelines", request_object_id=pipeline_id)
    for entry in (acl.access_control_list or []):
        _log(
            "pipeline_acl_entry",
            table=table,
            pipeline_id=pipeline_id,
            principal=_principal_name(entry),
            permissions=_permission_levels(entry),
        )


def _set_acl(
    ws: WorkspaceClient,
    object_type: str,
    object_id: str,
    sp_app_id: str,
    level: PermissionLevel | None,
) -> None:
    """Use update (additive, preserves existing ACL) for grants.

    `set` replaces the full ACL. `update` patches in the specified entries.
    We want additive semantics (don't wipe other principals' grants)."""
    ws.permissions.update(
        request_object_type=object_type,
        request_object_id=object_id,
        access_control_list=[
            AccessControlRequest(
                service_principal_name=sp_app_id,
                permission_level=level,  # None triggers revoke per SDK semantics
            )
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--grant", action="store_true")
    mode_group.add_argument("--revoke", action="store_true")
    mode_group.add_argument("--status", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--environment", default="dev")
    args = parser.parse_args()

    if not (args.grant or args.revoke or args.status):
        args.grant = True

    ws = WorkspaceClient()
    mode = "status" if args.status else ("revoke" if args.revoke else "grant")
    _log("start", environment=args.environment, mode=mode, dry_run=args.dry_run)

    hf_app_name = HF_APP_SP_NAME_PATTERN.format(env=args.environment)
    ingestion_name = INGESTION_SP_NAME_PATTERN.format(env=args.environment)
    hf_app_sp = _resolve_sp_app_id(ws, hf_app_name)
    ingestion_sp = _resolve_sp_app_id(ws, ingestion_name)
    sp_targets: set[tuple[str, str]] = {(hf_app_name, hf_app_sp), (ingestion_name, ingestion_sp)}
    _log("sps_resolved", sps={label: app_id for label, app_id in sp_targets})

    sample = f"{DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{SYNCED_TABLES[0][0]}"
    project_name = _resolve_database_project_name(ws, sample)
    _log("project_resolved", project_name=project_name)

    if args.status:
        _show_project_acl(ws, project_name, sp_targets)
        for table, _schema, pid in _enumerate_pipelines(ws):
            _show_pipeline_acl(ws, table, pid)
            break  # sample one pipeline — no need to dump all 34
        return 0

    pipelines = _enumerate_pipelines(ws)
    _log("pipelines_resolved", count=len(pipelines))

    t0 = time.monotonic()
    total = 0
    level: PermissionLevel | None = None if args.revoke else DATABASE_PROJECT_PERMISSION
    for sp_label, sp_app_id in sp_targets:
        if args.dry_run:
            _log("would_apply", target="database-project", sp_label=sp_label, sp_app_id=sp_app_id, permission=str(level))
        else:
            _set_acl(ws, "database-projects", project_name, sp_app_id, level)
            _log("project_grant", sp_label=sp_label, permission=str(level))
        total += 1

    level = None if args.revoke else PIPELINE_PERMISSION
    for table, _schema, pid in pipelines:
        for sp_label, sp_app_id in sp_targets:
            if args.dry_run:
                _log("would_apply", target="pipeline", table=table, sp_label=sp_label, sp_app_id=sp_app_id, permission=str(level))
            else:
                _set_acl(ws, "pipelines", pid, sp_app_id, level)
            total += 1
    elapsed_s = round(time.monotonic() - t0, 2)
    _log("complete", total_grants=total, elapsed_s=elapsed_s, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

Key semantic changes:
- Short project name is used for the permissions API (fixes the 404).
- SDK raises `DatabricksError` on non-2xx — no silent swallow.
- `ws.permissions.update()` is additive (does not wipe other principals).
- `_enumerate_pipelines` raises on any resolve failure (was: silently skipped with `_log("resolve_failed")`).
- The `--status` sample-one-pipeline behavior is retained for readability.

- [ ] **Step 3.4: Run unit tests — expect PASS**

Run: `uv run pytest src/tests/test_grant_synced_table_permissions.py -v`

Expected: 4 PASS.

- [ ] **Step 3.5: E2E `--status` check — must emit project_acl_entry**

```bash
AWS_PROFILE=devops-agent uv run python scripts/grant_synced_table_permissions.py --status --environment dev 2>&1
```

Expected output includes lines with `"event": "project_acl_entry"` and `"event": "pipeline_acl_entry"`. In the prior (broken) version, `project_acl_entry` events were missing.

- [ ] **Step 3.6: E2E `--dry-run --grant` check — no errors**

```bash
AWS_PROFILE=devops-agent uv run python scripts/grant_synced_table_permissions.py --dry-run --environment dev 2>&1 | tail -10
```

Expected: `would_apply` events for project + 34 pipelines × 2 SPs = 70 total, then `complete` with `total_grants=70`. No exceptions.

- [ ] **Step 3.7: E2E `--grant` live (idempotent) — verify grants stay correct**

Before: run `--status`, capture the current project ACL entries.

```bash
AWS_PROFILE=devops-agent uv run python scripts/grant_synced_table_permissions.py --grant --environment dev 2>&1 | tail -10
```

Expected: `complete` with `total_grants=70` and zero errors. The two SPs already have CAN_USE, so the grant is a no-op / refresh.

After: run `--status` again — both SPs should still show `CAN_USE`. No new principals introduced.

- [ ] **Step 3.8: Run ruff + pyright**

```bash
uv run ruff check scripts/grant_synced_table_permissions.py
uv run ruff format --check scripts/grant_synced_table_permissions.py
uv run pyright scripts/grant_synced_table_permissions.py
```

Expected: all clean. (If pyright flags the SDK enum types, add `type: ignore[...]` with a targeted comment — but prefer to find the correct SDK type first.)

- [ ] **Step 3.9: STOP — present evidence, await user approval**

Present to user:
- pytest output (4 PASS)
- `--status` output showing `project_acl_entry` events (was missing before)
- `--dry-run` output (70 would_apply events)
- `--grant` live output (complete with no errors)
- Second `--status` output proving grants unchanged (still `CAN_USE`)

**Do not commit.**

---

## Task 4: SEC4 least-privilege ACLs — extend & rename existing resources

**Goal:** Consolidate all per-job principals into one authoritative `databricks_permissions` resource per job, renamed to reflect that it IS the ACL. Add CI SP `IS_OWNER`. Keep existing `hf_app_v2 CAN_VIEW`. Use Terraform `moved` blocks for zero-churn rename. Sort `access_control` blocks alphabetically by `service_principal_name` to preempt provider positional-block-matching drift. **Do NOT** remove the admins-group membership in this cycle.

**Evidence (from SEC4 agent report + live plan run 2026-04-16)**:
- CI SP currently holds `admins` group membership at `terraform/modules/service_principals/main.tf:84-87` (confers workspace admin).
- `databricks_permissions.hf_app_view_ingestion_job` (environments/dev/main.tf:262) declares only `hf_app_v2 CAN_VIEW` but **live state has CI SP IS_OWNER** added out-of-band.
- `databricks_permissions.hf_app_view_sync_hf_costs_job` (main.tf:271) same pattern.
- `databricks_permissions` is **exclusive per target object** — provider docs: "This resource is authoritative. Any existing permissions not managed by this resource will be revoked." A second resource on the same `job_id` is forbidden.
- Terraform `moved` blocks safely rename without destroy/create churn — https://developer.hashicorp.com/terraform/language/modules/develop/refactoring.

### Files
- Modify: `terraform/environments/dev/main.tf`

- [ ] **Step 4.1: Write failing tests for Terraform validation**

Create `src/tests/test_sec4_ci_sp_job_owner.py`:

```python
"""SEC4: the two daily-job ACL resources must be authoritative, correctly
named, and carry both hf_app_v2 CAN_VIEW + CI SP IS_OWNER access_control
blocks sorted alphabetically by principal.

This test parses Terraform statically. It does not simulate a plan — it
just checks the declarations are present and shaped correctly. Live plan
verification happens in Step 4.6.
"""

from __future__ import annotations

import re
from pathlib import Path

_DEV = Path(__file__).resolve().parents[2] / "terraform" / "environments" / "dev" / "main.tf"

_CI_SP_REF = "module.service_principals.terraform_ci_sp_application_id"
_APP_SP_REF = "module.service_principals.hf_app_sp_application_id"


def _extract_resource_body(text: str, resource_type: str, resource_name: str) -> str | None:
    """Return the full body of a named resource block, or None if not found."""
    pattern = re.compile(rf'^resource\s+"{re.escape(resource_type)}"\s+"{re.escape(resource_name)}"\s*\{{', re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return None
    depth = 0
    i = m.start()
    # Advance through the block tracking brace depth line-by-line.
    lines = text.splitlines(keepends=True)
    start_line = text[: m.start()].count("\n")
    out: list[str] = []
    for line in lines[start_line:]:
        out.append(line)
        depth += line.count("{") - line.count("}")
        if depth == 0 and out:
            return "".join(out)
    return None


def _access_control_principals_in_order(body: str) -> list[tuple[str, str]]:
    """Return [(principal_ref, permission_level), ...] in declared order."""
    # Each access_control { ... } sub-block has service_principal_name = <ref> and permission_level = "X".
    pattern = re.compile(
        r"access_control\s*\{[^}]*?service_principal_name\s*=\s*(\S+)[^}]*?permission_level\s*=\s*\"([^\"]+)\"",
        re.DOTALL,
    )
    return [(m.group(1).rstrip(","), m.group(2)) for m in pattern.finditer(body)]


def _assert_acl_resource_correctly_shaped(body: str, resource_name: str) -> None:
    principals = _access_control_principals_in_order(body)
    principal_refs = [p for p, _ in principals]
    perms = dict(principals)
    assert _CI_SP_REF in perms, f"{resource_name}: CI SP access_control block missing"
    assert perms[_CI_SP_REF] == "IS_OWNER", f"{resource_name}: CI SP must be IS_OWNER, got {perms[_CI_SP_REF]!r}"
    assert _APP_SP_REF in perms, f"{resource_name}: hf_app_v2 CAN_VIEW block missing"
    assert perms[_APP_SP_REF] == "CAN_VIEW", f"{resource_name}: hf_app_v2 must be CAN_VIEW, got {perms[_APP_SP_REF]!r}"
    assert principal_refs == sorted(principal_refs), (
        f"{resource_name}: access_control blocks must be sorted alphabetically by "
        f"service_principal_name; got {principal_refs}"
    )


def test_ingestion_job_acl_exists_and_is_correctly_shaped() -> None:
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "ingestion_job_acl")
    assert body, "resource databricks_permissions.ingestion_job_acl not found"
    _assert_acl_resource_correctly_shaped(body, "ingestion_job_acl")


def test_sync_hf_costs_job_acl_exists_and_is_correctly_shaped() -> None:
    text = _DEV.read_text(encoding="utf-8")
    body = _extract_resource_body(text, "databricks_permissions", "sync_hf_costs_job_acl")
    assert body, "resource databricks_permissions.sync_hf_costs_job_acl not found"
    _assert_acl_resource_correctly_shaped(body, "sync_hf_costs_job_acl")


def test_old_resource_names_have_moved_blocks() -> None:
    """Rename must be declared via Terraform `moved` blocks so apply is a
    rename rather than destroy/create — zero ACL gap."""
    text = _DEV.read_text(encoding="utf-8")
    for old, new in [
        ("databricks_permissions.hf_app_view_ingestion_job", "databricks_permissions.ingestion_job_acl"),
        ("databricks_permissions.hf_app_view_sync_hf_costs_job", "databricks_permissions.sync_hf_costs_job_acl"),
    ]:
        # Look for a moved { from = <old>  to = <new> } block.
        pattern = re.compile(
            rf"moved\s*\{{\s*from\s*=\s*{re.escape(old)}\s*to\s*=\s*{re.escape(new)}\s*\}}",
            re.DOTALL,
        )
        assert pattern.search(text), f"missing moved block: from {old} to {new}"


def test_no_orphaned_old_resource_names() -> None:
    """After rename, the old resource names must not remain as resources."""
    text = _DEV.read_text(encoding="utf-8")
    for old in ("hf_app_view_ingestion_job", "hf_app_view_sync_hf_costs_job"):
        pattern = re.compile(rf'^resource\s+"databricks_permissions"\s+"{old}"\s*\{{', re.MULTILINE)
        assert not pattern.search(text), f"old resource name {old!r} still declared — rename incomplete"
```

- [ ] **Step 4.2: Run tests — expect FAIL**

Run: `uv run pytest src/tests/test_sec4_ci_sp_job_owner.py -v`

Expected: 4 FAIL (resources not found / moved blocks missing).

- [ ] **Step 4.3: Replace the two existing resources with renamed + extended versions**

Find the existing `hf_app_view_ingestion_job` block in `terraform/environments/dev/main.tf` (approximately line 262) and replace it with:

```hcl
# Authoritative ACL for the daily ingestion job. Every non-default grant
# on this job is declared here; databricks_permissions is exclusive per
# target object. Blocks are sorted alphabetically by service_principal_name
# to preempt the Databricks provider's positional-block-matching drift.
resource "databricks_permissions" "ingestion_job_acl" {
  job_id = module.workflows.ingestion_job_id

  access_control {
    # hf_app_v2 SP — Taipy app reads job run history for the AI/ML Workflows page.
    service_principal_name = module.service_principals.hf_app_sp_application_id
    permission_level       = "CAN_VIEW"
  }

  access_control {
    # CI SP — terraform apply modifies the job definition; IS_OWNER is the
    # least-privilege grant that permits this without workspace admin.
    # Precondition for SEC4 admins-group removal (deferred follow-up).
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "IS_OWNER"
  }
}
```

Sort verification: `hf_app_sp_application_id` < `terraform_ci_sp_application_id` alphabetically. ✓

Do the equivalent replacement for `hf_app_view_sync_hf_costs_job` (approximately line 271) →

```hcl
resource "databricks_permissions" "sync_hf_costs_job_acl" {
  job_id = databricks_job.sync_hf_costs_daily.id

  access_control {
    # hf_app_v2 SP — Taipy app surfaces HF cost sync status.
    service_principal_name = module.service_principals.hf_app_sp_application_id
    permission_level       = "CAN_VIEW"
  }

  access_control {
    # CI SP — see ingestion_job_acl rationale.
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "IS_OWNER"
  }
}
```

**Add Terraform `moved` blocks** immediately after both resources so the rename is treated as a state migration, not destroy/create:

```hcl
moved {
  from = databricks_permissions.hf_app_view_ingestion_job
  to   = databricks_permissions.ingestion_job_acl
}

moved {
  from = databricks_permissions.hf_app_view_sync_hf_costs_job
  to   = databricks_permissions.sync_hf_costs_job_acl
}
```

Verify `module.workflows.ingestion_job_id` and `module.service_principals.hf_app_sp_application_id` both exist as outputs. Grep `terraform/modules/workflows/outputs.tf` and `terraform/modules/service_principals/outputs.tf`. If either is missing, add the corresponding output.

- [ ] **Step 4.4: Run tests — expect PASS**

Run: `uv run pytest src/tests/test_sec4_ci_sp_job_owner.py -v`

Expected: 4 PASS.

- [ ] **Step 4.5: `terraform fmt` + `terraform validate`**

```bash
cd terraform/environments/dev && AWS_PROFILE=devops-agent terraform fmt -recursive && AWS_PROFILE=devops-agent terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4.6: E2E `terraform plan` — expect 2 renames + ACL additions, no destruction**

```bash
AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev plan -no-color 2>&1 | tail -60
```

Expected:
- Terraform reports the 2 `moved` blocks as state-address changes (`Terraform will perform the following actions: moved from ... to ...`).
- Each resource shows an **in-place update**: one existing `access_control` (hf_app_v2 CAN_VIEW) stays, one NEW `access_control` (CI SP IS_OWNER) is added. No blocks are removed.
- Summary: `0 to add, 2 to change, 0 to destroy` — same count as before, but the *change* is now "+CI SP IS_OWNER" instead of "-CI SP IS_OWNER".

If any other resource shows drift, or the destroy count is non-zero, STOP and investigate — do not apply.

- [ ] **Step 4.7: STOP — present evidence, await user approval**

Present to user:
- pytest output (2 PASS)
- `terraform plan` summary (should be `2 to add, 0 to change, 0 to destroy`)
- The exact resource additions (reproduced from the plan output)

**Do not apply. Do not commit.** User decides whether to proceed to apply + commit.

---

## Task 5: Full-repo gate + commit preparation

- [ ] **Step 5.1: Full test suite + lints**

Run in parallel (independent):
```bash
uv run pytest src/ -v 2>&1 | tail -5
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
```

Expected: all green. If any pre-existing failure is present (unrelated to this cycle), capture the list — do not try to fix unrelated failures in this branch.

- [ ] **Step 5.2: Verify no unintended file changes**

```bash
git status --short
git diff --stat main..HEAD
```

Expected files changed:
- `terraform/modules/workflows/main.tf` (big diff, block reordering + nothing else)
- `terraform/environments/dev/main.tf` (~20 lines added)
- `src/workflows/card.py` (~30 lines added)
- `workflow-cards/wf-hf-sync.yaml` (new)
- 14 existing `workflow-cards/wf-*.yaml` (2-line diffs each)
- `scripts/grant_synced_table_permissions.py` (large rewrite)
- `src/tests/test_card.py` (4 new tests)
- `src/tests/test_card_parity_with_terraform.py` (new)
- `src/tests/test_workflows_tf_ordering.py` (new)
- `src/tests/test_grant_synced_table_permissions.py` (new)
- `src/tests/test_sec4_ci_sp_job_owner.py` (new)
- `docs/superpowers/plans/2026-04-16-daily-job-governance.md` (this file)
- `TODO.md` — remove D63 from On Deck; move SEC4 admins-removal note to Technical Debt / Blocked

**Remove D63 from TODO.md On Deck** (per `feedback_todo_cleanup_in_commit.md`). Leave the SEC4 row but narrow its scope to "admins-group removal remaining (deferred to follow-up)".

- [ ] **Step 5.3: Run `mad-scientist-skills:final-review`**

Present the reviewer with:
- Branch: `feat/daily-job-governance`
- Changed files list
- Note: this is pre-commit review, not post-commit.

Address any High severity findings. Document any Low findings in the PR description if approved to commit.

- [ ] **Step 5.4: STOP — final approval for commit**

Present to user:
- Consolidated test output
- Consolidated terraform plan (Task 1 + Task 4 combined: still `2 to add, 0 to change, 0 to destroy`)
- `git diff --stat`
- Final-review reviewer output

Ask: "Approve single commit + PR? If yes, I will create commit message from the evidence, run `git commit`, push, and open PR. If preferred, I can split the commit — but cleaner as one given the small blast radius."

- [ ] **Step 5.5 (ONLY IF APPROVED): Commit, push, create PR**

Use a `HEREDOC` commit message capturing the 4 items and their evidence citations. Push the branch and open a PR with summary + test plan (per `.claude` PR template).

**DO NOT PROCEED TO THIS STEP WITHOUT EXPLICIT USER APPROVAL.**

---

## Rollback plan

If any step fails mid-cycle:

| Task | Rollback |
|------|----------|
| 1 | `git checkout -- terraform/modules/workflows/main.tf` — block reorder is pure text |
| 2 | `git checkout -- workflow-cards/ src/workflows/card.py` — no runtime side effects |
| 3 | `git checkout -- scripts/grant_synced_table_permissions.py` — no remote state touched by test runs |
| 4 | `git checkout -- terraform/environments/dev/main.tf` — no `terraform apply` happens until Task 5.5 explicit approval |

Live state is not mutated at any point before Task 5.5 except by Task 3.7 (`--grant` live, which is idempotent and additive — re-granting CAN_USE is a no-op).

---

## Open decisions user may revisit later

- Whether to ALSO update the `wf-sync-hf-costs.yaml` card to re-enable the standalone `sync-hf-costs-daily` job (currently PAUSED). The card now claims `orchestrated` via `hf_sync`; if the PAUSED job is intentional, leave it. If unpausing is desired, that's a separate change outside this cycle.
- The 6 no-card TF tasks (`backfill_statsbomb_extra`, `backfill_statsbomb_360`, `ingest_idsse_events`, `extract_tracking_metadata`, `refresh_synced_tables`, `hf_sync` — wait, `hf_sync` now has a card) are documented governance gaps; creating cards for each is a follow-up, not blocking.
- Dead `compute_embeddings` / `compute_formations` / `validate_workflow_cards` pyproject entry_points — removal is a follow-up; deleting in this cycle expands the diff without user-facing benefit.

---

## Cycle self-review checklist

- [x] Every task ends at a STOP with evidence + no commit
- [x] Every code change is preceded by a failing test
- [x] Every behavior change is E2E-verified against live state
- [x] No hidden dependencies between tasks (Task 2 and Task 3 are independent; Task 4 depends on Task 1 for clean plan)
- [x] Rollback is trivial at every step
- [x] All claims cite file:line or command output
- [x] User's rule "no commits/PRs without approval" is honored at 5 separate STOP checkpoints
