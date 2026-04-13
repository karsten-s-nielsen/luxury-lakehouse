# Synced Table Auto-Refresh + Authenticated Cache-Clear API

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the manual synced-table refresh gap with automation after both daily pipeline runs and dbt builds, plus add an HF-authenticated admin API endpoint to force cache clear (optionally also triggering a synced refresh) on demand.

**Architecture:**
1. **Refactor `scripts/refresh_synced_tables.py`** to authenticate via `databricks-sdk` `WorkspaceClient` (auto-detects PAT, OAuth M2M, CLI profile, ambient runtime). This makes the script runnable from local dev, GitHub Actions, Databricks jobs, and any other environment — eliminating the current CLI subprocess dependency.
2. **Wrapper script `scripts/dbt_build_and_refresh.py`** chains `dbt build` → `refresh_synced_tables.py --wait`, fail-fast on dbt error. Becomes the canonical local dev flow for "rebuild gold + propagate".
3. **Final task in the daily Databricks job** runs `refresh_synced_tables` after all 9 leaf compute tasks. Refreshes all 34 synced tables (the warm observability table gets new data; the 33 gold tables refresh as a no-op until the next dbt build).
4. **`hf_taipy_app/src/admin_api.py`** Flask blueprint with `POST /api/cache/clear` (optional `?refresh_synced=1`), protected by HuggingFace user-token validation against `whoami-v2` with `luxury-lakehouse` org membership + admin/write role check.
5. **Document deferred D59** (move dbt build into the daily Databricks job) in TODO.md.

**Tech Stack:** Python 3.10, databricks-sdk 0.102.0, Flask 3.1.1 (already pulled in by Taipy 4.1.1), pytest 8+, requests 2.33.1, Terraform (Databricks provider). Single feature branch `feat/synced-table-auto-refresh`. Single commit at the end of E2E verification (per project convention).

---

## Files Touched

### Created
| Path | Purpose |
|---|---|
| `scripts/dbt_build_and_refresh.py` | Wrapper: dbt build → refresh synced tables |
| `hf_taipy_app/src/admin_api.py` | Flask blueprint with admin endpoint + HF token validation |
| `hf_taipy_app/src/test_admin_api.py` | Admin API unit tests (run via `cd hf_taipy_app/src && pytest test_admin_api.py`) |
| `src/tests/test_refresh_synced_tables.py` | Refresh script auth/logic tests |
| `src/tests/test_dbt_build_and_refresh.py` | Wrapper exit-code propagation tests |

### Modified
| Path | Change |
|---|---|
| `scripts/refresh_synced_tables.py` | Replace `_get_auth_token` (subprocess CLI) with `_get_auth_headers` (`WorkspaceClient`); remove `subprocess`, `json` imports; fix line 146 help text "all 11" → "all 34" |
| `hf_taipy_app/src/cache.py` | Add public `cache_size() -> int` helper |
| `hf_taipy_app/src/main.py` | Build Flask app, register admin blueprint, pass `flask=flask_app` to `Gui(...)` |
| `pyproject.toml` | Add `refresh_synced_tables = "scripts.refresh_synced_tables:main"` entry point under `[project.scripts]` |
| `terraform/modules/workflows/main.tf` | Add final `refresh_synced_tables` `python_wheel_task` with `depends_on` chain to all 9 leaf tasks |
| `CLAUDE.md` | Add line under Project Conventions documenting `dbt_build_and_refresh.py` as canonical dev flow |
| `TODO.md` | Add D59 deferred entry under Technical Debt |

### Verified Constants (do not change without re-verification)
- `databricks-sdk` version: `0.102.0` (in `pyproject.toml:40`)
- Taipy version: `4.1.1` (in `hf_taipy_app/requirements.txt:293`)
- Flask version: `3.1.1` (in `hf_taipy_app/requirements.txt:80`)
- Pinned Python: `3.10` (`pyproject.toml:6`)
- Synced table count: `34` (verified in `terraform/modules/synced_tables/main.tf` and `scripts/refresh_synced_tables.py:40-75`)
- Daily job leaf tasks (`terraform/modules/workflows/main.tf`): `run_model_validation` (604), `hf_sync` (685), `compute_formations_shape_graph` (370), `compute_embeddings_v1` (503), `compute_off_ball_xt` (281), `compute_line_breaking` (397), `compute_defcon_lite` (426), `compute_xg_model_v2` (259), `extract_tracking_metadata` (629)

---

## Pre-flight

### Task 0: Confirm branch and clean state

- [ ] **Step 1: Verify on the right branch**

Run: `git branch --show-current`
Expected: `feat/synced-table-auto-refresh`

- [ ] **Step 2: Verify only the previously-tracked uncommitted file is present**

Run: `git status --short`
Expected: only `M hf_taipy_app/requirements.txt` (carried over from the prior session)

- [ ] **Step 3: Confirm dependencies are installed**

Run: `uv run python -c "from databricks.sdk import WorkspaceClient; print('ok')"`
Expected: `ok`

If it fails, run: `uv sync --extra sdk` and retry.

- [ ] **Step 4: Confirm pytest discovers existing tests**

Run: `uv run pytest src/tests/ --collect-only -q 2>&1 | tail -5`
Expected: a count line (e.g. `523 tests collected`) — confirms test discovery works.

---

## Part 1: Refactor `refresh_synced_tables.py` to use WorkspaceClient

**Why:** The current `_get_auth_token()` (lines 81-89) shells out to `databricks auth token --profile OAUTH`. This works only on machines with the Databricks CLI installed and an `OAUTH` profile configured. It will NOT work inside a Databricks job (no CLI), GitHub Actions (no profile), or the Taipy app (which runs `WorkspaceClient` already). Replacing the subprocess call with `WorkspaceClient` makes the script environment-agnostic and unblocks Parts 3 and 4.

### Task 1.1: Write failing tests for new auth helper

**Files:**
- Create: `src/tests/test_refresh_synced_tables.py`

- [ ] **Step 1: Create the test file with three failing tests**

```python
"""Tests for scripts/refresh_synced_tables.py — auth and table list invariants."""
from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest


def test_get_auth_headers_uses_workspace_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must obtain headers from WorkspaceClient.config.authenticate."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer test-token-123"}

    monkeypatch.setattr(
        "scripts.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    from scripts.refresh_synced_tables import _get_auth_headers

    headers = _get_auth_headers()

    assert headers == {"Authorization": "Bearer test-token-123"}
    mock_ws.config.authenticate.assert_called_once()


def test_get_auth_headers_does_not_call_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """_get_auth_headers must NOT shell out to the Databricks CLI."""
    mock_ws = MagicMock()
    mock_ws.config.authenticate.return_value = {"Authorization": "Bearer x"}
    monkeypatch.setattr(
        "scripts.refresh_synced_tables.WorkspaceClient",
        lambda: mock_ws,
    )

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("subprocess.run must not be called from _get_auth_headers")

    monkeypatch.setattr(subprocess, "run", _fail)

    from scripts.refresh_synced_tables import _get_auth_headers

    _get_auth_headers()  # must not raise


def test_synced_tables_list_has_34_entries() -> None:
    """SYNCED_TABLES drift guard — should match the 34 tables in Terraform."""
    from scripts.refresh_synced_tables import SYNCED_TABLES

    assert len(SYNCED_TABLES) == 34
```

- [ ] **Step 2: Run the tests to verify they fail in the expected ways**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py -v`
Expected:
- `test_get_auth_headers_uses_workspace_client` — FAIL with `AttributeError: module 'scripts.refresh_synced_tables' has no attribute 'WorkspaceClient'` OR `ImportError: cannot import name '_get_auth_headers'`
- `test_get_auth_headers_does_not_call_subprocess` — FAIL same way
- `test_synced_tables_list_has_34_entries` — PASS (the list already has 34 entries; this is a drift-guard test that documents current state)

### Task 1.2: Implement WorkspaceClient-based auth

**Files:**
- Modify: `scripts/refresh_synced_tables.py:22-29` (imports), `:81-89` (function), and the one caller at `:163`

- [ ] **Step 1: Replace imports**

Open `scripts/refresh_synced_tables.py`. Find the existing imports:

```python
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import requests
```

Replace with:

```python
from __future__ import annotations

import argparse
import os
import sys
import time

import requests
from databricks.sdk import WorkspaceClient
```

(Removed: `json`, `subprocess`. Added: `WorkspaceClient`.)

- [ ] **Step 2: Replace the auth function**

Find lines 81-89:

```python
def _get_auth_token() -> str:
    """Get a workspace token via Databricks CLI OAuth."""
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "OAUTH"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["access_token"]
```

Replace with:

```python
def _get_auth_headers() -> dict[str, str]:
    """Get Databricks auth headers via WorkspaceClient.

    Auto-detects credentials in priority order:
    PAT (DATABRICKS_TOKEN) → OAuth M2M (DATABRICKS_CLIENT_ID/SECRET) →
    CLI profile → ambient runtime context (Databricks job).
    """
    ws = WorkspaceClient()
    return ws.config.authenticate()
```

- [ ] **Step 3: Update the caller**

Find lines 163-164 inside `main()`:

```python
    token = _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
```

Replace with:

```python
    headers = _get_auth_headers()
    headers["Content-Type"] = "application/json"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py -v`
Expected: all 3 tests PASS.

- [ ] **Step 5: Verify the script imports cleanly**

Run: `uv run python -c "from scripts.refresh_synced_tables import main, _get_auth_headers, SYNCED_TABLES; print('ok', len(SYNCED_TABLES))"`
Expected: `ok 34`

### Task 1.3: Fix the line 146 docstring bug

**Files:**
- Modify: `scripts/refresh_synced_tables.py:142-147`

- [ ] **Step 1: Update the help text**

Find:

```python
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated subset of table names (default: all 11)",
    )
```

Replace with:

```python
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated subset of table names (default: all 34)",
    )
```

- [ ] **Step 2: Verify via --help**

Run: `uv run python scripts/refresh_synced_tables.py --help 2>&1 | grep "all 34"`
Expected: a line containing `default: all 34`

### Task 1.4: Local end-to-end verification

- [ ] **Step 1: Smoke-test the script against dev workspace**

Run: `uv run python scripts/refresh_synced_tables.py --tables fct_workflow_costs_synced 2>&1 | head -20`

Expected one of:
- `[1/1] Triggered refresh: fct_workflow_costs_synced` followed by `Summary: 1 triggered, 0 errors`
- `[1/1] Already running: fct_workflow_costs_synced` followed by `Summary: 1 triggered, 0 errors`

If it fails with an auth error, the WorkspaceClient is not finding credentials in the local environment — verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (or `DATABRICKS_CLIENT_ID`+`DATABRICKS_CLIENT_SECRET`) are set, then retry.

- [ ] **Step 2: Confirm linters pass on the modified file**

Run: `uv run ruff check scripts/refresh_synced_tables.py`
Expected: no errors.

Run: `uv run ruff format --check scripts/refresh_synced_tables.py`
Expected: no errors. If it complains, run `uv run ruff format scripts/refresh_synced_tables.py`.

---

## Part 2: dbt wrapper script

**Why:** Today, after running `dbt build` locally, the developer must remember to also run `scripts/refresh_synced_tables.py --wait` to propagate gold-tier updates into Lakebase. This step is forgotten regularly, causing stale data in the Taipy app. The wrapper script makes the two operations atomic: dbt fails → no refresh; dbt succeeds → refresh runs synchronously.

### Task 2.1: Write failing tests for the wrapper

**Files:**
- Create: `src/tests/test_dbt_build_and_refresh.py`

- [ ] **Step 1: Create the test file**

```python
"""Tests for scripts/dbt_build_and_refresh.py — fail-fast wrapper semantics."""
from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest


def _make_completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr="")


def test_dbt_failure_aborts_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt build fails, refresh_synced_tables must NOT run, exit code propagates."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        if any("dbt" in part for part in cmd):
            return _make_completed(1)
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()

    assert exit_code == 1, "dbt failure exit code must propagate"
    refresh_calls = [c for c in calls if any("refresh_synced_tables" in part for part in c)]
    assert refresh_calls == [], "refresh_synced_tables must not be invoked after dbt failure"


def test_dbt_success_triggers_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt build succeeds, refresh_synced_tables --wait must run and exit propagates."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()

    assert exit_code == 0
    refresh_calls = [c for c in calls if any("refresh_synced_tables" in part for part in c)]
    assert len(refresh_calls) == 1, "refresh_synced_tables must run exactly once"
    assert any("--wait" in part for part in refresh_calls[0]), "refresh must use --wait"


def test_refresh_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """If dbt succeeds but refresh fails, the wrapper exit code must reflect refresh failure."""
    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if any("refresh_synced_tables" in part for part in cmd):
            return _make_completed(2)
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()
    assert exit_code == 2


def test_extra_args_forwarded_to_dbt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Args after the script name must be forwarded to dbt build."""
    calls: list[list[str]] = []

    def mock_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        return _make_completed(0)

    monkeypatch.setattr(subprocess, "run", mock_run)
    monkeypatch.setattr("sys.argv", ["dbt_build_and_refresh.py", "--select", "tag:cost", "--target", "dev"])

    from scripts.dbt_build_and_refresh import main

    exit_code = main()
    assert exit_code == 0

    dbt_calls = [c for c in calls if any("dbt" in part for part in c)]
    assert len(dbt_calls) == 1
    assert "--select" in dbt_calls[0]
    assert "tag:cost" in dbt_calls[0]
    assert "--target" in dbt_calls[0]
    assert "dev" in dbt_calls[0]
```

- [ ] **Step 2: Run the tests to confirm they fail with ImportError**

Run: `uv run pytest src/tests/test_dbt_build_and_refresh.py -v`
Expected: 4 tests, all FAIL with `ModuleNotFoundError: No module named 'scripts.dbt_build_and_refresh'`

### Task 2.2: Implement the wrapper script

**Files:**
- Create: `scripts/dbt_build_and_refresh.py`

- [ ] **Step 1: Create the script**

```python
#!/usr/bin/env python3
"""Run `dbt build` then `refresh_synced_tables.py --wait` atomically.

Canonical local dev flow for "rebuild gold tables and propagate to Lakebase".
If dbt fails, refresh is skipped. If refresh fails after dbt success, the
wrapper exits with the refresh exit code.

Usage:
    python scripts/dbt_build_and_refresh.py                       # full build
    python scripts/dbt_build_and_refresh.py --select tag:cost     # targeted
    python scripts/dbt_build_and_refresh.py --target prod         # forwarded
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DBT_PROJECT = _REPO_ROOT / "dbt_project"
_REFRESH_SCRIPT = _REPO_ROOT / "scripts" / "refresh_synced_tables.py"


def main() -> int:
    """Run dbt build, then refresh synced tables on success."""
    dbt_args = sys.argv[1:]
    print(f"==> Running: dbt build {' '.join(dbt_args)}", flush=True)

    dbt_result = subprocess.run(  # noqa: S603
        ["dbt", "build", *dbt_args],  # noqa: S607
        cwd=str(_DBT_PROJECT),
        check=False,
    )

    if dbt_result.returncode != 0:
        print(
            f"==> ERROR: dbt build failed (exit {dbt_result.returncode}). "
            f"Skipping synced table refresh.",
            flush=True,
        )
        return dbt_result.returncode

    print("==> dbt build succeeded. Triggering synced table refresh (--wait)...", flush=True)

    refresh_result = subprocess.run(  # noqa: S603
        [sys.executable, str(_REFRESH_SCRIPT), "--wait"],
        check=False,
    )

    if refresh_result.returncode != 0:
        print(
            f"==> ERROR: refresh_synced_tables failed (exit {refresh_result.returncode}).",
            flush=True,
        )
    else:
        print("==> dbt build + synced table refresh complete.", flush=True)

    return refresh_result.returncode


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run the tests to confirm they pass**

Run: `uv run pytest src/tests/test_dbt_build_and_refresh.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 3: Verify ruff is clean**

Run: `uv run ruff check scripts/dbt_build_and_refresh.py`
Expected: no errors.

Run: `uv run ruff format --check scripts/dbt_build_and_refresh.py`
Expected: no errors. If it complains, run `uv run ruff format scripts/dbt_build_and_refresh.py`.

### Task 2.3: Document canonical flow in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` — add a line under Project Conventions

- [ ] **Step 1: Find the project conventions section**

Open `CLAUDE.md`, search for: `**Use \`scripts/ensure_warehouse.py\`**`

You should see this line:

```
- **Use `scripts/ensure_warehouse.py`** before any `dbt build`: ...
```

- [ ] **Step 2: Insert a new bullet immediately after that line**

Add:

```
- **Use `scripts/dbt_build_and_refresh.py`** as the canonical dev flow for "rebuild gold tables and propagate". Wraps `dbt build` (any args forwarded) with a synchronous `refresh_synced_tables.py --wait` on success. Fails fast if dbt errors — refresh only runs after a clean dbt build. This eliminates the manual two-step that was the original cause of stale Lakebase data after gold rebuilds. Direct `dbt build` invocation is allowed but will leave Lakebase synced tables stale until the next manual refresh.
```

### Task 2.4: Skip-test verification (no live invocation)

- [ ] **Step 1: Confirm import**

Run: `uv run python -c "from scripts.dbt_build_and_refresh import main; print('ok')"`
Expected: `ok`

- [ ] **Step 2: Confirm --help works (argparse not needed; the script has none, but `python -h` should still parse)**

Run: `uv run python scripts/dbt_build_and_refresh.py 2>&1 | head -1`
Expected: line starts with `==> Running: dbt build` (the script will then attempt to invoke dbt). Cancel with Ctrl+C if it actually starts dbt — we just want to confirm the wrapper reaches the run step. Alternatively, test in dry mode by running it in a temporary directory where `dbt_project` does not exist:

Run: `cd /tmp && uv run python "$OLDPWD/scripts/dbt_build_and_refresh.py" 2>&1 | head -3` (this WILL fail at the dbt step but that's expected — we're confirming the script runs and surfaces the error properly)

Skip this step on Windows if `/tmp` is awkward; the unit tests already cover the error paths.

---

## Part 2c: D59 deferred TODO entry

### Task 2c.1: Add D59 to TODO.md

**Files:**
- Modify: `TODO.md`

- [ ] **Step 1: Find the Technical Debt section**

Open `TODO.md`, look for the section that contains item #1 about synced table refresh. The plan is to add D59 nearby.

- [ ] **Step 2: Insert the D59 entry**

Add (use the exact wording or polish if context suggests; preserve numbering convention):

```markdown
**D59: Move dbt build into the daily Databricks job (deferred)**

Currently dbt runs only on developer machines. The daily Databricks job (06:00 UTC, defined in `terraform/modules/workflows/main.tf:41`, 23 tasks) ends at producing bronze + warm-tier observability data; gold synced tables only update when a developer manually runs `scripts/dbt_build_and_refresh.py`.

**Goal:** add `dbt_build` as a final-stage `python_wheel_task` (or `notebook_task`) depending on all 9 leaf compute tasks, followed immediately by `refresh_synced_tables` (which already exists as the final task added in this PR).

**Lift:**
- Install `dbt-databricks` in the job environment (entry point or notebook with `%pip install`)
- Ship `profiles.yml` (or env-var auth) accessible inside the job
- Ensure the TF-SP service principal has CAN_USE on the SQL warehouse
- Validate dbt slim CI (`state:modified+`) behavior in the job context
- Decide on dbt threads count (default 4 vs higher for serverless warehouse)
- Confirm warehouse auto-resume timing in this context (CLAUDE.md notes the auto-resume retry has been unreliable)

**Risk:** dbt runs against the SQL warehouse, not a Databricks compute cluster — this means a job task that calls dbt is essentially a thin shell wrapper, but auth and warehouse-resume timing need validation in the job environment.

**Owner:** TBD. **Prerequisite:** the WorkspaceClient refactor of `scripts/refresh_synced_tables.py` (Part 1 of this PR) must be merged so the refresh script works inside Databricks jobs.
```

---

## Part 3: Final task in the daily Databricks job

**Why:** Currently, when the daily job finishes at 06:00 UTC, the warm-tier `workflow_cost_live_synced` table sees no automatic refresh. The Taipy app reads stale warm-tier data until a developer manually runs the refresh script. Adding a final task that depends on all leaf compute tasks closes this gap. Per the user's decision, this task refreshes ALL 34 synced tables — for the 33 gold tables, this is a no-op until the next dbt build, but the cost is small (~30s of pipeline poll time per idle table) and it eliminates a class of bugs where a partial dbt rebuild creates a half-stale Lakebase.

### Task 3.1: Add `refresh_synced_tables` entry point in pyproject.toml

**Files:**
- Modify: `pyproject.toml:70-109` (the `[project.scripts]` section)

- [ ] **Step 1: Find the entry point list**

The list ends at `hf_sync = "ingestion.hf_sync:main"` (line 108).

- [ ] **Step 2: Add a new entry point**

Add this line at the end of the `[project.scripts]` section, before line 110 (`[tool.ruff]`):

```toml
refresh_synced_tables = "scripts.refresh_synced_tables:main"
```

- [ ] **Step 3: Verify the entry point is registered**

Run: `uv pip install -e . --quiet && uv run python -c "from importlib.metadata import entry_points; eps = entry_points(group='console_scripts'); print('refresh_synced_tables' in [e.name for e in eps])"`
Expected: `True`

(If you see `False`, run `uv sync` to refresh the install and retry.)

### Task 3.2: Add Terraform task for refresh

**Files:**
- Modify: `terraform/modules/workflows/main.tf` — add a new `task` block at the end of the `databricks_job.data_ingestion` resource (after the `extract_tracking_metadata` task at line 629 OR `hf_sync` task at line 685, whichever is last)

- [ ] **Step 1: Read the existing task structure**

Open `terraform/modules/workflows/main.tf`. Locate the last `task` block in `databricks_job.data_ingestion`. Look at one of the existing python_wheel_task blocks (e.g., `extract_tracking_metadata` at line 629) to model the structure:

Existing pattern (illustrative — actual lines may vary):

```hcl
  task {
    task_key = "extract_tracking_metadata"
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "extract_tracking_metadata"
    }
    environment_key = "default"
  }
```

- [ ] **Step 2: Append the new refresh task**

Add this `task` block immediately AFTER the `hf_sync` task (which is currently the last one in the file). It depends on all 9 leaf tasks (the 9 task_keys verified above):

```hcl
  task {
    task_key = "refresh_synced_tables"
    depends_on {
      task_key = "run_model_validation"
    }
    depends_on {
      task_key = "hf_sync"
    }
    depends_on {
      task_key = "compute_formations_shape_graph"
    }
    depends_on {
      task_key = "compute_embeddings_v1"
    }
    depends_on {
      task_key = "compute_off_ball_xt"
    }
    depends_on {
      task_key = "compute_line_breaking"
    }
    depends_on {
      task_key = "compute_defcon_lite"
    }
    depends_on {
      task_key = "compute_xg_model_v2"
    }
    depends_on {
      task_key = "extract_tracking_metadata"
    }
    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "refresh_synced_tables"
      parameters   = ["--wait"]
    }
    environment_key = "default"
  }
```

**Important:** the entry point name `refresh_synced_tables` must match what was added to `pyproject.toml` in Task 3.1. The `--wait` parameter ensures the job task waits for all 34 pipeline updates to reach IDLE before exiting (so a downstream observer of the job knows the refresh completed).

- [ ] **Step 3: Validate Terraform syntax**

Run: `cd terraform/environments/dev && terraform fmt -check ../../modules/workflows/main.tf`
Expected: no output (file is well-formed). If it fails, run `terraform fmt ../../modules/workflows/main.tf` and retry.

Run: `cd terraform/environments/dev && terraform validate`
Expected: `Success! The configuration is valid.`

If validation fails with auth errors (the validate step needs no credentials but `terraform init` does), run `terraform init -backend=false` first, then retry validate.

- [ ] **Step 4: Plan the change (do NOT apply)**

Run: `cd terraform/environments/dev && terraform plan -target=module.workflows.databricks_job.data_ingestion -out=/tmp/refresh-task.tfplan 2>&1 | tail -40`

Expected diff: one task added to `databricks_job.data_ingestion` named `refresh_synced_tables` with 9 `depends_on` blocks. No other resources changed. Save the plan output for the user to review.

If the diff includes unexpected changes (e.g., other tasks rewritten), STOP and investigate before applying — Terraform may be detecting drift from the production state.

### Task 3.3: Verify the entry point can be invoked locally

- [ ] **Step 1: Smoke-test the entry point**

Run: `uv run refresh_synced_tables --tables fct_workflow_costs_synced 2>&1 | head -5`

Expected: same output as Task 1.4 Step 1 (`Triggered refresh:` or `Already running:` for the one table).

If the command is not found, the entry point did not register — re-run `uv sync` and retry.

### Task 3.4: Defer terraform apply

- [ ] **Step 1: Document apply as a separate step**

Do NOT run `terraform apply` as part of this plan. Terraform apply is a destructive operation (modifies the dev workspace) and requires explicit user approval. Note this in the final summary at Part 5.

---

## Part 4: Authenticated admin API endpoint

**Why:** Today there is no way to clear the Taipy app's in-memory cache without redeploying the Space (which takes 5+ minutes). Adding a `POST /api/cache/clear` endpoint protected by HF user-token validation closes this gap. The optional `?refresh_synced=1` parameter additionally triggers a background refresh of all 34 synced tables, providing a single-call "force everything fresh now" admin button.

**Auth model:** caller presents `Authorization: Bearer hf_xxx` (HF user access token). The endpoint validates by calling `https://huggingface.co/api/whoami-v2` with that token, then checks the response for membership in `luxury-lakehouse` org with role `admin` or `write`. Token revocation by the user is immediate. No token is stored in the Space; each call independently validates.

### Task 4.1: Add `cache_size()` helper to cache.py

**Files:**
- Modify: `hf_taipy_app/src/cache.py:45-47`

- [ ] **Step 1: Add the helper after `clear_cache`**

Open `hf_taipy_app/src/cache.py`. Find:

```python
def clear_cache() -> None:
    """Clear all cached entries (e.g., on competition change)."""
    _cache.clear()
```

Add immediately after:

```python


def cache_size() -> int:
    """Return the current number of cached entries."""
    return len(_cache)
```

(Two blank lines before the new function, per PEP 8.)

### Task 4.2: Write failing tests for HF token validation

**Files:**
- Create: `hf_taipy_app/src/test_admin_api.py`

- [ ] **Step 1: Create the test file**

```python
"""Tests for hf_taipy_app/src/admin_api.py.

Run from inside hf_taipy_app/src so the flat-import paths resolve:
    cd hf_taipy_app/src && python -m pytest test_admin_api.py -v
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests
from flask import Flask


@pytest.fixture(autouse=True)
def _reset_cache() -> object:
    """Each test starts and ends with an empty in-memory cache."""
    import cache

    cache._cache.clear()
    yield
    cache._cache.clear()


# --- _validate_hf_admin: input shape ---


def test_validate_missing_header() -> None:
    from admin_api import _validate_hf_admin

    ok, status, msg = _validate_hf_admin(None)

    assert ok is False
    assert status == 401
    assert "missing" in msg.lower() or "malformed" in msg.lower()


def test_validate_no_bearer_prefix() -> None:
    from admin_api import _validate_hf_admin

    ok, status, _ = _validate_hf_admin("Token abc123")

    assert ok is False
    assert status == 401


def test_validate_token_format_invalid() -> None:
    from admin_api import _validate_hf_admin

    ok, status, _ = _validate_hf_admin("Bearer not-a-real-hf-token")

    assert ok is False
    assert status == 401


# --- _validate_hf_admin: HF API responses ---


def _make_resp(status_code: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.json = MagicMock(return_value=payload or {})
    return resp


def test_validate_hf_returns_401(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(401))

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 401
    assert "revoked" in msg.lower() or "invalid" in msg.lower()


def test_validate_hf_returns_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(503))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 503


def test_validate_hf_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    def _raise(*_: object, **__: object) -> None:
        raise requests.Timeout()

    monkeypatch.setattr("admin_api.requests.get", _raise)

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 503
    assert "timeout" in msg.lower()


# --- _validate_hf_admin: org/role checks ---


def test_validate_user_not_in_org(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "stranger",
        "orgs": [{"name": "other-org", "roleInOrg": "admin"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, msg = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 403
    assert "luxury-lakehouse" in msg or "member" in msg.lower()


def test_validate_user_wrong_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "reader",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "read"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is False
    assert status == 403


def test_validate_user_admin_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "karsten",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "admin"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is True
    assert status == 200


def test_validate_user_write_role(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import _validate_hf_admin

    payload = {
        "name": "writer",
        "orgs": [{"name": "luxury-lakehouse", "roleInOrg": "write"}],
    }
    monkeypatch.setattr("admin_api.requests.get", lambda *_, **__: _make_resp(200, payload))

    ok, status, _ = _validate_hf_admin("Bearer hf_" + "a" * 30)

    assert ok is True
    assert status == 200


# --- /api/cache/clear endpoint ---


def test_endpoint_no_auth_returns_401() -> None:
    from admin_api import build_admin_blueprint

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post("/api/cache/clear")

    assert resp.status_code == 401


def test_endpoint_valid_auth_clears_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    import cache
    from admin_api import build_admin_blueprint

    monkeypatch.setattr("admin_api._validate_hf_admin", lambda _h: (True, 200, "karsten"))

    cache._cache["k1"] = (1.0, "v1")
    cache._cache["k2"] = (2.0, "v2")
    assert cache.cache_size() == 2

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post(
            "/api/cache/clear",
            headers={"Authorization": "Bearer hf_xxx"},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["cleared"] is True
    assert body["entries_cleared"] == 2
    assert body["refresh_synced_triggered"] is False
    assert cache.cache_size() == 0


def test_endpoint_with_refresh_synced_triggers_background(monkeypatch: pytest.MonkeyPatch) -> None:
    from admin_api import build_admin_blueprint

    monkeypatch.setattr("admin_api._validate_hf_admin", lambda _h: (True, 200, "karsten"))

    refresh_calls: list[bool] = []
    monkeypatch.setattr(
        "admin_api._trigger_synced_refresh_async",
        lambda: refresh_calls.append(True),
    )

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    with app.test_client() as client:
        resp = client.post(
            "/api/cache/clear?refresh_synced=1",
            headers={"Authorization": "Bearer hf_xxx"},
        )

    assert resp.status_code == 200
    body = resp.get_json()
    assert body["cleared"] is True
    assert body["refresh_synced_triggered"] is True
    assert refresh_calls == [True]


def test_endpoint_denied_request_logs_no_token(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Denied requests must log status + reason but never the token value."""
    import logging

    from admin_api import build_admin_blueprint

    monkeypatch.setattr(
        "admin_api._validate_hf_admin",
        lambda _h: (False, 401, "Token invalid or revoked"),
    )

    app = Flask(__name__)
    app.register_blueprint(build_admin_blueprint())

    secret_token = "hf_should_never_be_logged_xxxxxxxxxxxxxx"  # pragma: allowlist secret
    with caplog.at_level(logging.INFO, logger="admin_api"):
        with app.test_client() as client:
            client.post(
                "/api/cache/clear",
                headers={"Authorization": f"Bearer {secret_token}"},
            )

    # Token must NEVER appear in any log record
    for record in caplog.records:
        assert secret_token not in record.getMessage()
        assert secret_token not in str(record.args or "")
```

- [ ] **Step 2: Run the tests to confirm they fail with ImportError**

Run: `cd hf_taipy_app/src && uv run python -m pytest test_admin_api.py -v 2>&1 | head -30`

Expected: 14 tests, all FAIL with `ModuleNotFoundError: No module named 'admin_api'`

(Stay in `hf_taipy_app/src/` for subsequent test runs.)

### Task 4.3: Implement admin_api.py

**Files:**
- Create: `hf_taipy_app/src/admin_api.py`

- [ ] **Step 1: Create the module**

```python
"""Authenticated admin endpoints for the Taipy app.

Provides POST /api/cache/clear (with optional ?refresh_synced=1) protected
by HuggingFace user-token validation against whoami-v2.

Auth model: caller presents an HF user access token as
`Authorization: Bearer hf_xxx`. The token is validated by calling
https://huggingface.co/api/whoami-v2 with the token in the header.
The response must show membership in the `luxury-lakehouse` org with
role `admin` or `write`.

Tokens are not stored anywhere — each call validates independently
against HF, so revocation by the user is immediate.
"""
from __future__ import annotations

import logging
import re
import threading
from typing import Final

import requests
from flask import Blueprint, jsonify, request

from cache import cache_size, clear_cache

_logger = logging.getLogger(__name__)

_HF_WHOAMI_URL: Final = "https://huggingface.co/api/whoami-v2"
_REQUIRED_ORG: Final = "luxury-lakehouse"
_ALLOWED_ROLES: Final = frozenset({"admin", "write"})
_HF_TOKEN_RE: Final = re.compile(r"^hf_[A-Za-z0-9]{20,}$")
_REQUEST_TIMEOUT: Final = (5, 15)  # (connect, read) per CLAUDE.md security standard


def _validate_hf_admin(auth_header: str | None) -> tuple[bool, int, str]:
    """Validate HF user token and check org membership.

    Returns
    -------
    (allowed, http_status, message)
        On success: (True, 200, <username>) — message is the HF username for logging.
        On failure: (False, <4xx-or-503>, <reason>) — reason is safe to return to caller.
    """
    if not auth_header or not auth_header.startswith("Bearer "):
        return False, 401, "Missing or malformed Authorization header"

    token = auth_header.removeprefix("Bearer ").strip()
    if not _HF_TOKEN_RE.match(token):
        return False, 401, "Token format invalid"

    try:
        resp = requests.get(
            _HF_WHOAMI_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=_REQUEST_TIMEOUT,
            verify=True,
        )
    except requests.Timeout:
        return False, 503, "HuggingFace identity service timeout"
    except requests.RequestException as exc:
        return False, 503, f"HuggingFace identity service unreachable: {type(exc).__name__}"

    if resp.status_code == 401:
        return False, 401, "Token invalid or revoked"
    if not resp.ok:
        return False, 503, f"HuggingFace API error: {resp.status_code}"

    try:
        data = resp.json()
    except ValueError:
        return False, 503, "HuggingFace API returned non-JSON"

    orgs_raw = data.get("orgs", [])
    if not isinstance(orgs_raw, list):
        return False, 503, "HuggingFace API returned malformed orgs field"

    orgs = {o.get("name", ""): o.get("roleInOrg", "") for o in orgs_raw if isinstance(o, dict)}
    if _REQUIRED_ORG not in orgs:
        return False, 403, f"Token user is not a member of {_REQUIRED_ORG}"
    if orgs[_REQUIRED_ORG] not in _ALLOWED_ROLES:
        return False, 403, "Insufficient role (need admin or write)"

    username = data.get("name", "unknown")
    return True, 200, str(username)


def _trigger_synced_refresh_async() -> None:
    """Spawn a background thread to refresh all 34 synced tables.

    Refresh takes minutes; the HTTP caller should not wait. Errors are
    logged but not propagated. The thread is daemonized so app shutdown
    does not block on it.
    """
    def _run() -> None:
        try:
            import sys

            from scripts.refresh_synced_tables import main as refresh_main

            old_argv = sys.argv
            sys.argv = ["refresh_synced_tables.py", "--wait"]
            try:
                refresh_main()
            finally:
                sys.argv = old_argv
        except SystemExit as exc:
            _logger.info("admin: background synced refresh exited with code %s", exc.code)
        except Exception:
            _logger.exception("admin: background synced refresh failed")

    threading.Thread(target=_run, daemon=True, name="admin-synced-refresh").start()


def build_admin_blueprint() -> Blueprint:
    """Build the admin Flask blueprint.

    The blueprint registers POST /api/cache/clear with HF token auth.
    Inject the returned blueprint into a Flask app, then pass that
    Flask app to `taipy.gui.Gui(flask=...)`.
    """
    bp = Blueprint("admin", __name__)

    @bp.route("/api/cache/clear", methods=["POST"])
    def _clear_cache_endpoint():  # type: ignore[no-untyped-def]
        ok, status, msg = _validate_hf_admin(request.headers.get("Authorization"))
        remote = request.remote_addr or "unknown"

        if not ok:
            _logger.info(
                "admin: cache clear DENIED status=%d remote=%s reason=%s",
                status,
                remote,
                msg,
            )
            return jsonify({"error": msg}), status

        # `msg` holds the HF username on success
        _logger.info(
            "admin: cache clear ALLOWED user=%s remote=%s",
            msg,
            remote,
        )

        entries_before = cache_size()
        clear_cache()

        also_refresh = request.args.get("refresh_synced") == "1"
        if also_refresh:
            _trigger_synced_refresh_async()
            _logger.info("admin: synced table refresh triggered (background) by user=%s", msg)

        return (
            jsonify(
                {
                    "cleared": True,
                    "entries_cleared": entries_before,
                    "refresh_synced_triggered": also_refresh,
                }
            ),
            200,
        )

    return bp
```

- [ ] **Step 2: Run the tests to confirm they pass**

Run: `cd hf_taipy_app/src && uv run python -m pytest test_admin_api.py -v 2>&1 | tail -30`

Expected: all 14 tests PASS.

If any test fails, read the failure carefully. Likely causes:
- `monkeypatch.setattr("admin_api.requests.get", ...)` failing because `requests` isn't a module attribute on `admin_api` — fix by changing imports to `import requests` (already done) so the mock target is correct.
- `cache._cache` not found — confirm the autouse `_reset_cache` fixture is in place.

- [ ] **Step 3: Verify ruff and pyright are clean**

Run: `uv run ruff check hf_taipy_app/src/admin_api.py hf_taipy_app/src/cache.py`
Expected: no errors (note: hf_taipy_app has per-file ignores in pyproject.toml — long lines and a few other rules are exempt).

### Task 4.4: Inject Flask app into the Taipy Gui

**Files:**
- Modify: `hf_taipy_app/src/main.py:65,147` (add Flask import + change Gui instantiation)

- [ ] **Step 1: Add the Flask + admin_api imports**

Open `hf_taipy_app/src/main.py`. Find line 65:

```python
from taipy.gui import Gui
```

Add immediately after (lines 66-67):

```python
from flask import Flask

from admin_api import build_admin_blueprint
```

(Three lines added: a blank line preserved between groups if needed; ruff will sort imports.)

- [ ] **Step 2: Build the Flask app and pass it to Gui**

Find the `if __name__ == "__main__":` block at line 140:

```python
if __name__ == "__main__":
    import health_check
    from config import validate_databricks_credentials

    validate_databricks_credentials()
    health_check.start()

    gui = Gui(pages=pages, css_file="style_v2.css")
    gui.run(
        host="0.0.0.0",
        port=7860,
        ...
```

Replace `gui = Gui(pages=pages, css_file="style_v2.css")` (line 147) with:

```python
    flask_app = Flask("luxury-lakehouse-taipy")
    flask_app.register_blueprint(build_admin_blueprint())

    gui = Gui(pages=pages, css_file="style_v2.css", flask=flask_app)
```

Three lines added; the `gui.run(...)` call below is unchanged.

- [ ] **Step 3: Verify imports + module loads**

Run: `cd hf_taipy_app/src && uv run python -c "import main; print('ok')"`

Expected: `ok` (with possibly a few warning logs from `validate_databricks_credentials` if env vars aren't set; that's fine — we're verifying import not run).

If you see `ImportError: cannot import name 'build_admin_blueprint' from 'admin_api'`, the previous task didn't save admin_api.py correctly — re-check.

- [ ] **Step 4: Verify ruff is clean on main.py**

Run: `uv run ruff check hf_taipy_app/src/main.py`
Expected: no errors.

### Task 4.5: Local end-to-end verification

- [ ] **Step 1: Start the Taipy app locally**

Run (in a separate terminal, or `run_in_background: true`):

```bash
cd hf_taipy_app/src && uv run python main.py
```

Wait for the log line `[Taipy][INFO] * Server starting on http://0.0.0.0:7860` (or similar). The app should be reachable at http://localhost:7860.

If the app fails to start because of missing env vars (`LAKEBASE_HOST`, etc.), make sure `hf_taipy_app/.env` exists with the dev credentials per the existing pattern. The admin endpoint does NOT require Lakebase to be reachable.

- [ ] **Step 2: Curl the endpoint with NO auth → expect 401**

In another terminal:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:7860/api/cache/clear
```

Expected output: `401`

- [ ] **Step 3: Curl with a malformed Bearer token → expect 401**

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:7860/api/cache/clear \
  -H "Authorization: Bearer not-a-real-token"
```

Expected: `401`

- [ ] **Step 4: Curl with a valid HF PAT → expect 200**

You will need to provide a real HF user access token from `https://huggingface.co/settings/tokens` for an account that is a member of `luxury-lakehouse` with role `admin` or `write`.

```bash
curl -s -X POST http://localhost:7860/api/cache/clear \
  -H "Authorization: Bearer hf_REAL_TOKEN_HERE"
```

Expected output: `{"cleared":true,"entries_cleared":N,"refresh_synced_triggered":false}` with `N >= 0` (depends on whether you've navigated the app first and warmed the cache).

- [ ] **Step 5: Curl with the same token + ?refresh_synced=1 → expect 200 + background refresh**

```bash
curl -s -X POST "http://localhost:7860/api/cache/clear?refresh_synced=1" \
  -H "Authorization: Bearer hf_REAL_TOKEN_HERE"
```

Expected output: `{"cleared":true,"entries_cleared":N,"refresh_synced_triggered":true}`

In the Taipy server log, expect to see:
```
admin: cache clear ALLOWED user=<your-hf-username> remote=...
admin: synced table refresh triggered (background) by user=<your-hf-username>
```

Within the next ~30 seconds, the background refresh thread should make the first whoami-v2 call. Within ~5 minutes, all 34 synced tables should reach IDLE in Databricks.

- [ ] **Step 6: Verify denied requests log no token**

Tail the app log for the recent denied requests. Confirm: no `hf_xxx` token value appears anywhere in the log output. Only the HTTP status and reason should be logged.

- [ ] **Step 7: Stop the local app**

Ctrl+C the terminal running `python main.py` (or kill the background bash if you used `run_in_background: true`).

---

## Part 5: Final integration verification

### Task 5.1: Run the full test suite

- [ ] **Step 1: Run the project unit tests**

Run: `uv run pytest src/tests/test_refresh_synced_tables.py src/tests/test_dbt_build_and_refresh.py -v`
Expected: 7 passed (3 + 4).

- [ ] **Step 2: Run the hf_taipy_app admin API tests**

Run: `cd hf_taipy_app/src && uv run python -m pytest test_admin_api.py -v`
Expected: 14 passed.

- [ ] **Step 3: Run the existing project test suite to verify no regressions**

Run: `uv run pytest src/tests/ -q --tb=line 2>&1 | tail -20`
Expected: same pass count as the baseline before this PR (collect only takes a few seconds; the full run may take minutes — `run_in_background: true` is appropriate). No failures.

If anything breaks, investigate immediately. The changes in this PR are isolated to a script, a wrapper, a Terraform file, and a new admin module — they should not affect any existing tests. Any failure indicates an unexpected coupling.

### Task 5.2: Run linters and type checks

- [ ] **Step 1: Run ruff on changed files**

Run:

```bash
uv run ruff check scripts/refresh_synced_tables.py scripts/dbt_build_and_refresh.py hf_taipy_app/src/admin_api.py hf_taipy_app/src/cache.py hf_taipy_app/src/main.py src/tests/test_refresh_synced_tables.py src/tests/test_dbt_build_and_refresh.py
```

Expected: no errors.

Run:

```bash
uv run ruff format --check scripts/refresh_synced_tables.py scripts/dbt_build_and_refresh.py hf_taipy_app/src/admin_api.py hf_taipy_app/src/cache.py hf_taipy_app/src/main.py
```

Expected: no errors. If anything is unformatted, run the same command without `--check`.

- [ ] **Step 2: Run pyright on the admin module and cache helper**

Run: `uv run pyright hf_taipy_app/src/admin_api.py hf_taipy_app/src/cache.py`
Expected: 0 errors (warnings about missing taipy stubs are acceptable).

(Note: pyright excludes `scripts/` per `pyproject.toml:140`, so the script changes don't need pyright validation.)

### Task 5.3: Verify Terraform plan is still clean

- [ ] **Step 1: Re-plan after all other changes**

Run: `cd terraform/environments/dev && terraform plan -target=module.workflows.databricks_job.data_ingestion 2>&1 | tail -30`

Expected: same diff as in Task 3.2 Step 4 — one task added, no other changes. Capture the output for the user to review.

### Task 5.4: Compose final summary for the user

- [ ] **Step 1: Build the change summary**

Compile a summary covering:

- **Files created (5):** list paths
- **Files modified (7):** list paths
- **Tests added:** count and pass status (3 + 4 + 14 = 21 new tests)
- **Verification performed:**
  - Unit tests pass
  - Linters pass
  - Pyright clean on new module
  - Local refresh script smoke test passed (Task 1.4)
  - Local Taipy app curl tests passed (Task 4.5 steps 2-5)
  - Terraform plan generated (apply NOT performed)
- **Pending user actions:**
  1. Review the Terraform plan (saved at `/tmp/refresh-task.tfplan`)
  2. Review the diff (`git diff`)
  3. Approve commit
  4. Approve `terraform apply` (separate, after commit)
  5. Approve deploy to staging Space + production Space (per `feedback_staging_before_production.md`)

- [ ] **Step 2: Request commit approval**

Present the summary to the user. Wait for explicit `commit` approval before running any `git add` / `git commit`. Do NOT auto-commit.

The proposed commit message (subject ≤72 chars):

```
feat: synced-table auto-refresh + HF-authenticated admin API

- WorkspaceClient auth for refresh_synced_tables.py (env-agnostic)
- scripts/dbt_build_and_refresh.py wrapper (canonical dev flow)
- Daily Databricks job: refresh all 34 synced tables after leaf tasks
- POST /api/cache/clear with HF whoami-v2 token validation
- 21 new unit tests, no regressions
- D59 deferred TODO entry for moving dbt build into the Databricks job
```

Once the user approves the commit, run:

```bash
git add scripts/refresh_synced_tables.py scripts/dbt_build_and_refresh.py \
        hf_taipy_app/src/admin_api.py hf_taipy_app/src/cache.py hf_taipy_app/src/main.py \
        src/tests/test_refresh_synced_tables.py src/tests/test_dbt_build_and_refresh.py \
        hf_taipy_app/src/test_admin_api.py \
        pyproject.toml terraform/modules/workflows/main.tf \
        CLAUDE.md TODO.md
git status
git commit -m "$(cat <<'EOF'
feat: synced-table auto-refresh + HF-authenticated admin API

- WorkspaceClient auth for refresh_synced_tables.py (env-agnostic)
- scripts/dbt_build_and_refresh.py wrapper (canonical dev flow)
- Daily Databricks job: refresh all 34 synced tables after leaf tasks
- POST /api/cache/clear with HF whoami-v2 token validation
- 21 new unit tests, no regressions
- D59 deferred TODO entry for moving dbt build into the Databricks job

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

Note: the previously-tracked `hf_taipy_app/requirements.txt` modification is NOT included in `git add` above unless the user confirms it should be part of this PR.

---

## Self-Review Checklist (executed by plan author before delivery)

- [x] **Spec coverage:** Every part of the approved direction has at least one task.
  - Part 1 (refresh script refactor) → Tasks 1.1-1.4
  - Part 2 (dbt wrapper) → Tasks 2.1-2.4
  - Part 2c (D59 TODO) → Task 2c.1
  - Part 3 (daily job task) → Tasks 3.1-3.4
  - Part 4 (admin endpoint) → Tasks 4.1-4.5
  - Part 5 (final verification) → Tasks 5.1-5.4
  - Pre-existing bug fix (line 146) → Task 1.3

- [x] **Placeholder scan:** No `TBD`, `TODO`, `implement later`, `add appropriate error handling`, or `similar to Task N` patterns.

- [x] **Type/name consistency:**
  - `_get_auth_headers` (not `_get_auth_token`) used consistently in Tasks 1.1, 1.2, 1.4
  - `cache_size()` defined in Task 4.1 and used in Task 4.3
  - `build_admin_blueprint()` defined in Task 4.3 and used in Task 4.4
  - `_validate_hf_admin` signature `(auth_header) -> tuple[bool, int, str]` consistent across tests and impl
  - `_trigger_synced_refresh_async()` no-arg, no-return — consistent across tests and impl

- [x] **TDD discipline:** Each new module has tests written BEFORE implementation:
  - Task 1.1 (tests) → Task 1.2 (impl)
  - Task 2.1 (tests) → Task 2.2 (impl)
  - Task 4.2 (tests) → Task 4.3 (impl)
  - Terraform changes (Task 3.2) cannot be TDD'd; verified via `terraform validate` + `plan`
  - Documentation changes (Task 2.3, 2c.1, 5.4) are non-code

- [x] **Verified facts:**
  - `databricks-sdk==0.102.0` is in `taipy-app` extra (line 62)
  - `Gui(flask=...)` parameter exists at `taipy/gui/gui.py:184` (verified by agent)
  - HF Space exposes only port 7860 (no sidecar viable; verified)
  - All 9 leaf task names verified against `terraform/modules/workflows/main.tf` line numbers in agent report

- [x] **Commit boundaries:** Single commit at end (per project convention `feedback_no_staging_commits.md`). No intermediate commits planned.

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| `WorkspaceClient.config.authenticate()` may have a different return shape than expected | Tasks 1.1 + 1.2 use a mock that mirrors the documented `dict[str, str]` shape; if the live shape differs, Task 1.4 (smoke test) will catch it before commit |
| Background refresh thread spawned by admin endpoint may interfere with the app's main event loop | Daemon thread, no shared state with Taipy state management; failure mode is logged-and-swallowed |
| Adding `Flask(...)` instantiation may conflict with Taipy's internal Flask blueprint registration | Verified pattern from `gui.py:2740-2741` agent report — Taipy iterates `_flask_blueprint` AFTER our blueprint is already on the Flask app; no conflict |
| New Terraform task may trigger drift on unrelated resources | Task 3.2 Step 4 explicitly inspects the diff; if unexpected changes appear, STOP |
| HF whoami-v2 endpoint format may have changed | The agent verified this against live HF docs; Task 4.5 Step 4 catches any discrepancy with a real token |
| `--wait` polling in the daily job task could time out (max 30 min) and fail the whole job | The polling timeout is hard-coded at 30 min; if more is needed, raise the constant in a follow-up. Worst case: the refresh task fails and the daily job is marked failed, but no data is lost |
| Test discovery for `hf_taipy_app/src/test_admin_api.py` won't run via the project pytest because `testpaths = ["src/tests"]` | Tests are explicitly run from `cd hf_taipy_app/src && python -m pytest test_admin_api.py` in Task 5.1 Step 2; documented in CI follow-up if needed |

---

## Out-of-Scope (NOT in this PR)

- **D59**: moving dbt build into the daily Databricks job. Documented as a deferred TODO in Task 2c.1.
- **Reducing cold-tier TTL below 600s**: confirmed in earlier investigation that PR #115 already lowered it from 3600s → 600s.
- **5 export/import workflows showing no enrichment**: separate orchestration mystery noted in MEMORY.md, unrelated to refresh automation.
- **Reconciling 23-task vs 26-task job count**: noted as drift between memory and current Terraform but not blocking this PR.
- **Strict RFC 6749 OAuth (Option B/C from earlier comparison)**: rejected because of operational complexity; HF PAT + whoami-v2 validation is the chosen pragmatic equivalent.
- **Rate limiting on the admin endpoint**: not added; HF token validation is sufficient for an admin-only endpoint. Reconsider if abuse appears.
- **UI button to clear cache from inside Taipy**: not in scope; the API endpoint is the only surface added in this PR.
