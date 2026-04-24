# Kimball PR 4a — Live dbt CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `pull_request` GitHub Actions workflow that runs live `dbt build --select state:modified+` against Databricks, posts a PR comment on failure, and blocks merge on failure. Closes the gap where PR 3's `try_cast` bugs merged to main undetected because the existing `dbt-ci.yml` workflow runs `dbt parse` only (Thrift from public runners is blocked).

**Architecture:** GH Actions (OIDC-authenticated to Databricks, same pattern as `terraform-apply.yml`) packages dbt_project/ + the main-branch dbt manifest into a tarball, uploads to Unity Catalog Volume, submits a Databricks Jobs one-shot run via `/api/2.0/jobs/runs/submit` with a `spark_python_task` that invokes a pre-uploaded shim script reading the tarball and running dbt inside Databricks (where Thrift works). GH Actions polls `/api/2.0/jobs/runs/get` to terminal state; on failure, downloads run output from UC Volume, parses `target/run_results.json`, posts a PR comment via `$GITHUB_TOKEN`, and exits non-zero. The existing `dbt-ci.yml` (parse-only) stays unchanged — this is additive coverage.

**Tech Stack:** GitHub Actions, Databricks Jobs API (OIDC auth), Unity Catalog Volumes, dbt-core + dbt-databricks, pytest.

**Source spec:** `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md` (uncommitted; commits with this plan's first commit on the branch per user guidance 2026-04-23).

---

## Decisions required — resolve before/during execution

| # | Decision | Default (this plan assumes) | Alternative |
|---|---|---|---|
| **D1** | Databricks Job invocation mechanism | **spark_python_task with a shim uploaded to UC Volume.** One-shot `runs_submit` each PR; no permanent Job resource in Terraform. Tarball of `dbt_project/` + manifest uploaded per-run to `/Volumes/soccer_analytics/dev_gold/ci_dbt/`. Shim script itself pre-uploaded once (Phase 1 Task 1.3). | **(a) Permanent `databricks_job` resource via Terraform + `runs_submit` with `job_id`** — more auditable, versioned via TF; adds a terraform round-trip for any shim behavior change. **(b) `notebook_task` with `git_source` pointing at this repo** — cleanest "no upload" path but requires workspace-level GitHub credential setup + a notebook file path inside the repo. |
| **D2** | Cluster type for the Databricks Job | **Serverless compute** (`new_cluster` not specified; `serverless_compute_name` omitted — runs_submit defaults to the workspace serverless job compute pool). No cluster config in our Terraform; relies on workspace serverless. | Classic `new_cluster` with a single-node `Standard_DS3_v2` equivalent — extra boot time (~3 min) and explicit cost but avoids serverless quirks. |
| **D3** | UC Volume path layout | **`/Volumes/soccer_analytics/dev_gold/ci_dbt/<pr_number>-<commit_sha>/`** per run. Includes `dbt_project.tar.gz` + `manifest_main.json` + (after run) `run_results.json`. Retention: delete entries older than 14 days via a daily cron (Phase 6 open item — not included in PR 4a; logged as follow-up). | Flat `/Volumes/.../ci_dbt/<commit_sha>.tar.gz` — less organized; harder to correlate runs with PRs. |
| **D4** | `--select` argument when `dbt_project.yml` / `packages.yml` / `profiles.yml` diff against main | **`state:modified+ --state=/tmp/manifest_main/` is the default; fallback to `+all` (full build) when any config file diffs.** Three-file diff check runs in GH Actions before trigger. | Always full build — simpler but 3–4× slower on trivial PRs. Always `state:modified+` — misses config-level changes that affect unchanged models. |

---

## File structure map

### Created

| Path | Responsibility |
|---|---|
| `.github/workflows/dbt-live-ci.yml` | Pull-request workflow. Computes `--select` arg, uploads tarball to UC Volume, triggers Databricks Job, polls, posts comment on failure. |
| `scripts/ci/run_dbt_in_databricks.py` | Shim executed inside the Databricks Job cluster. Downloads tarball from UC Volume, extracts to `/tmp/`, installs dbt (pinned), runs `dbt deps` + `dbt build --select <arg> --state <manifest_main_path>`, uploads `run_results.json` back to UC Volume, exits with dbt's exit code. |
| `scripts/trigger_dbt_job.py` | GH-side helper. Takes `--select-arg`, `--pr-number`, `--commit-sha`, `--tarball-path`. POSTs to `/api/2.0/jobs/runs/submit`, polls `/api/2.0/jobs/runs/get` until terminal, returns `(state, run_id, run_page_url, result_state)` via JSON on stdout. Uses OIDC-acquired Databricks token from env. |
| `scripts/post_dbt_failure_comment.py` | GH-side helper. Takes `--pr-number`, `--run-id`, `--run-page-url`, `--result-state`, `--run-output-volume-path`. Downloads `run_results.json` from UC Volume, parses for failing models/tests, POSTs a GH PR comment via `$GITHUB_TOKEN`. |
| `scripts/upload_ci_shim.py` | One-time setup helper. Uploads `scripts/ci/run_dbt_in_databricks.py` to `/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py`. Re-run when the shim changes. |
| `src/tests/test_trigger_dbt_job.py` | Unit tests for trigger_dbt_job.py (requests mocked). |
| `src/tests/test_post_dbt_failure_comment.py` | Unit tests for post_dbt_failure_comment.py (requests + GH API mocked). |
| `src/tests/test_run_dbt_in_databricks.py` | Unit tests for the shim (subprocess + file IO mocked). |
| `src/tests/fixtures/run_results_mixed.json` | dbt run_results.json fixture — 2 passing models + 1 failing model + 1 failing test. Used by post_dbt_failure_comment.py tests. |
| `src/tests/fixtures/run_results_all_pass.json` | dbt run_results.json fixture — all passing (regression guard on "don't post comment on success"). |

### Modified

| Path | Reason |
|---|---|
| `pyproject.toml` | Optional — add `[project.scripts]` entries `trigger-dbt-job`, `post-dbt-failure-comment` for CLI convenience (used by the workflow yaml). Only if the Phase 2 Task 2.2 approach uses `uv run` console-script invocation rather than direct `python scripts/...` calls. Plan defaults to the latter; this row is removed during Phase 2 if not needed. |

### Explicitly NOT modified (Chesterton's Fence)

- `.github/workflows/dbt-ci.yml` (parse-only; unchanged; continues to run on every PR as an additional required check).
- `.github/workflows/terraform-apply.yml` (OIDC pattern reference; untouched).
- `terraform/environments/dev/*.tf` (no new Terraform resources; D1 default is one-shot `runs_submit` with no permanent Job resource).
- `scripts/ensure_warehouse.py`, `scripts/dbt_build_and_refresh.py` (local dev flow; unchanged).
- Wheel + pyproject.toml `[project.scripts]` for `ci_dbt`-style entry points (we avoid the wheel bump path; shim lives in UC Volume).

---

## Phase 0: Pre-flight verification (read-only)

All downstream phases depend on these. Do not skip.

### Task 0.1: Verify DATABRICKS_CLIENT_ID OIDC trust and permissions

**Files:** None — live backend checks.

- [ ] **Step 1:** Confirm the OIDC trust policy from this repo to the Databricks service principal is in place.

Run, from repo root:

```bash
gh api repos/karsten-s-nielsen/luxury-lakehouse-d32/actions/variables/DATABRICKS_CLIENT_ID 2>/dev/null | jq -r .value
echo "---"
gh api repos/karsten-s-nielsen/luxury-lakehouse-d32/actions/variables/DATABRICKS_HOST 2>/dev/null | jq -r .value
```

Expected: a UUID-shaped client_id and a `https://<workspace>.cloud.databricks.com` URL. Both are already set (used by `terraform-apply.yml`).

- [ ] **Step 2:** Verify the SP has `CAN_SUBMIT_RUN` scope. Run:

```bash
uv run python - <<'PY'
import os, requests
host = os.environ["DATABRICKS_HOST"].rstrip("/")
tok = os.environ["DATABRICKS_TOKEN"]
# Use an existing PAT for this read; OIDC is not available outside GH Actions context.
r = requests.get(
    f"{host}/api/2.0/jobs/list?limit=1",
    headers={"Authorization": f"Bearer {tok}"},
    timeout=(10, 30),
    verify=True,
)
print(r.status_code, r.text[:200])
PY
```

Expected: HTTP 200 with a `{"jobs": ...}` JSON body. If 401/403, the personal token used here lacks the scope; that's OK for plan-time verification — the real OIDC run in PR 4a uses the SP, which terraform-apply.yml confirms has broad workspace access.

- [ ] **Step 3:** Verify UC Volume write permissions on the target path.

```bash
uv run python - <<'PY'
import os, requests
host = os.environ["DATABRICKS_HOST"].rstrip("/")
tok = os.environ["DATABRICKS_TOKEN"]
r = requests.get(
    f"{host}/api/2.1/unity-catalog/volumes/soccer_analytics.dev_gold.ci_dbt",
    headers={"Authorization": f"Bearer {tok}"},
    timeout=(10, 30),
    verify=True,
)
print(r.status_code, r.json())
PY
```

Expected: **(a)** HTTP 200 with volume metadata → Volume exists, continue. **(b)** HTTP 404 → Volume doesn't exist yet; create it via Terraform in Task 0.1.bis below.

- [ ] **Step 3.bis (conditional):** If Volume is missing, add its creation to `terraform/modules/catalog/main.tf` or equivalent (inspect during execution — path differs by existing TF module layout):

```hcl
resource "databricks_volume" "ci_dbt" {
  name             = "ci_dbt"
  catalog_name     = "soccer_analytics"
  schema_name      = "dev_gold"
  volume_type      = "MANAGED"
  comment          = "CI dbt run payloads (PR 4a, 2026-04-23). dbt_project tarballs in + run_results.json out."
}
```

Apply via `terraform plan` + `terraform apply` (requires user approval for the apply).

Alternative (used during PR 4a execution for faster feedback): create the volume directly via the Databricks SDK as a one-off, then add the Terraform resource in the same PR so the creation is tracked in IaC going forward. The SDK-first path avoids a terraform plan/apply cycle for a single trivial volume.

### Task 0.2: Verify `$GITHUB_TOKEN` scope for PR comments

**Files:** None.

- [ ] **Step 1:** Confirm that workflows on this repo have `pull-requests: write` available. In `.github/workflows/dbt-live-ci.yml` we'll set `permissions: pull-requests: write`, but the underlying GH token default behavior for workflows on same-repo PRs is write-enabled by default per repo settings.

```bash
gh api repos/karsten-s-nielsen/luxury-lakehouse-d32 | jq .permissions
```

Expected: `{"admin": true, "push": true, "pull": true}` or equivalent showing write access. Fork-PR case (PR from a fork) has token scope limited; our comment poster detects and skips (Phase 3 Task 3.1).

### Task 0.3: Verify dbt-databricks version + Python version needed

**Files:** None.

- [ ] **Step 1:** The shim will install dbt-databricks inside the Databricks runtime. Python version in Databricks serverless is 3.11 (per Databricks docs) — but our repo pins to Python 3.10. Check if dbt-databricks supports 3.11.

```bash
uv run python - <<'PY'
import urllib.request, json
r = urllib.request.urlopen("https://pypi.org/pypi/dbt-databricks/json", timeout=10)
info = json.loads(r.read())
print("Latest version:", info["info"]["version"])
print("Requires Python:", info["info"]["requires_python"])
PY
```

Expected: latest version ≥ 1.10 (same version family the repo uses locally); `requires_python` allows 3.10 AND 3.11. If not, the shim pins to a compatible older version.

- [ ] **Step 2:** Record the dbt-databricks version the shim will pin to. Default: match what `pyproject.toml` specifies in the `[project.optional-dependencies]` dbt extra. Run:

```bash
grep -A 5 "^dbt = \[" pyproject.toml
```

Expected: a version specifier like `"dbt-core>=1.10.0,<1.12.0"`, `"dbt-databricks>=1.10.0,<1.12.0"`. Record.

### Task 0.4: Confirm existing main-branch `dbt parse` baseline is green

**Files:** None.

- [ ] **Step 1:** Ensure we have a green baseline before starting.

```bash
gh run list --workflow=dbt-ci.yml --limit=5 --json conclusion,headBranch,url
```

Expected: recent main-branch runs show `"conclusion": "success"`. If recent failures: stop and investigate before PR 4a work.

### Task 0.5: Commit decision record for D1–D4

**Files:** None — this is a "Confirm with user" checkpoint, not a code step.

- [ ] **Step 1:** Before starting Phase 1, state the 4 decisions explicitly in the session and receive explicit "approved" from the user. If the user overrides D1 to (a) permanent TF-managed Job or (b) git_source notebook, rewrite Phase 1 to match before proceeding.

---

## Phase 1: Databricks shim + UC Volume upload

### Task 1.1: Write the shim tests first (red)

**Files:**
- Create: `src/tests/test_run_dbt_in_databricks.py`
- Create: `src/tests/fixtures/dbt_project_stub.tar.gz` — 0-byte placeholder for path checks (actual tarball content mocked in tests).

- [ ] **Step 1:** Create `src/tests/test_run_dbt_in_databricks.py`:

```python
"""Unit tests for scripts.ci.run_dbt_in_databricks (PR 4a shim).

The shim runs inside a Databricks Job cluster. It reads a tarball of
dbt_project/ + manifest-main.json from UC Volume, extracts, installs
dbt, runs `dbt build --select <arg>`, and uploads run_results.json
back to UC Volume.

All subprocess and file-system interactions are mocked here; integration
testing happens in Phase 5 E2E.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts.ci import run_dbt_in_databricks as shim


class TestParseArgs:
    def test_happy_path_required_args(self) -> None:
        args = shim.parse_args(
            [
                "--tarball-path", "/Volumes/x/y/z/proj.tar.gz",
                "--manifest-path", "/Volumes/x/y/z/manifest_main.json",
                "--select-arg", "state:modified+",
                "--output-path", "/Volumes/x/y/z/run_results.json",
            ]
        )
        assert args.tarball_path == "/Volumes/x/y/z/proj.tar.gz"
        assert args.select_arg == "state:modified+"

    def test_missing_required_raises(self) -> None:
        with pytest.raises(SystemExit):
            shim.parse_args(["--tarball-path", "/a"])


class TestDownloadFromVolume:
    @patch("scripts.ci.run_dbt_in_databricks._workspace_client")
    def test_download_writes_bytes_to_tmp(self, mock_ws: MagicMock, tmp_path: Path) -> None:
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files
        mock_files.download.return_value = MagicMock(contents=b"fake-tarball-bytes")
        out = tmp_path / "proj.tar.gz"

        shim.download_from_volume("/Volumes/x/y/z/proj.tar.gz", out)

        assert out.read_bytes() == b"fake-tarball-bytes"
        mock_files.download.assert_called_once_with("/Volumes/x/y/z/proj.tar.gz")


class TestExtractTarball:
    def test_extract_creates_dbt_project_directory(self, tmp_path: Path) -> None:
        # Build a real tarball fixture: a tar with a dbt_project/ dir containing profiles.yml.
        import tarfile

        src_dir = tmp_path / "src"
        (src_dir / "dbt_project").mkdir(parents=True)
        (src_dir / "dbt_project" / "profiles.yml").write_text("hello")

        tarball = tmp_path / "proj.tar.gz"
        with tarfile.open(tarball, "w:gz") as tf:
            tf.add(src_dir / "dbt_project", arcname="dbt_project")

        extract_dir = tmp_path / "extract"
        shim.extract_tarball(tarball, extract_dir)

        assert (extract_dir / "dbt_project" / "profiles.yml").read_text() == "hello"


class TestRunDbt:
    @patch("scripts.ci.run_dbt_in_databricks.subprocess.run")
    def test_dbt_deps_called_first(self, mock_run: MagicMock, tmp_path: Path) -> None:
        proj_dir = tmp_path / "dbt_project"
        proj_dir.mkdir()
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        shim.run_dbt(
            project_dir=proj_dir,
            select_arg="state:modified+",
            manifest_main_dir=tmp_path / "manifest",
        )

        calls = mock_run.call_args_list
        assert len(calls) == 2
        # First call: dbt deps
        assert calls[0][0][0][:2] == ["dbt", "deps"]
        # Second call: dbt build --select ...
        assert "--select" in calls[1][0][0]
        assert "state:modified+" in calls[1][0][0]

    @patch("scripts.ci.run_dbt_in_databricks.subprocess.run")
    def test_dbt_build_failure_propagates(
        self, mock_run: MagicMock, tmp_path: Path
    ) -> None:
        proj_dir = tmp_path / "dbt_project"
        proj_dir.mkdir()
        mock_run.side_effect = [
            subprocess.CompletedProcess(args=[], returncode=0),   # dbt deps OK
            subprocess.CompletedProcess(args=[], returncode=2),   # dbt build failed
        ]

        rc = shim.run_dbt(
            project_dir=proj_dir,
            select_arg="state:modified+",
            manifest_main_dir=tmp_path / "manifest",
        )
        assert rc == 2


class TestUploadOutput:
    @patch("scripts.ci.run_dbt_in_databricks._workspace_client")
    def test_uploads_run_results_json(self, mock_ws: MagicMock, tmp_path: Path) -> None:
        results = tmp_path / "run_results.json"
        results.write_text(json.dumps({"results": []}))
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files

        shim.upload_output(results, "/Volumes/x/y/z/run_results.json")

        mock_files.upload.assert_called_once()
        args, kwargs = mock_files.upload.call_args
        assert args[0] == "/Volumes/x/y/z/run_results.json"
        assert kwargs.get("overwrite", False) is True


class TestMainEndToEnd:
    @patch("scripts.ci.run_dbt_in_databricks.upload_output")
    @patch("scripts.ci.run_dbt_in_databricks.run_dbt")
    @patch("scripts.ci.run_dbt_in_databricks.extract_tarball")
    @patch("scripts.ci.run_dbt_in_databricks.download_from_volume")
    def test_main_returns_dbt_exit_code(
        self,
        mock_dl: MagicMock,
        mock_ex: MagicMock,
        mock_run: MagicMock,
        mock_up: MagicMock,
    ) -> None:
        mock_run.return_value = 2  # dbt failed
        rc = shim.main(
            [
                "--tarball-path", "/Volumes/a/b/c/proj.tar.gz",
                "--manifest-path", "/Volumes/a/b/c/manifest_main.json",
                "--select-arg", "state:modified+",
                "--output-path", "/Volumes/a/b/c/run_results.json",
            ]
        )
        assert rc == 2
        mock_up.assert_called_once()  # Output uploaded even on dbt failure
```

- [ ] **Step 2:** Run tests — expect failure (shim doesn't exist yet):

```bash
uv run pytest src/tests/test_run_dbt_in_databricks.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` for `scripts.ci.run_dbt_in_databricks`. Red.

### Task 1.2: Write the shim (green)

**Files:**
- Create: `scripts/ci/__init__.py` (empty).
- Create: `scripts/ci/run_dbt_in_databricks.py`.

- [ ] **Step 1:** Create `scripts/ci/__init__.py`:

```python
"""CI shims executed inside Databricks Job clusters (PR 4a)."""
```

- [ ] **Step 2:** Create `scripts/ci/run_dbt_in_databricks.py`:

```python
"""Shim executed inside a Databricks Job cluster for live dbt CI (PR 4a).

Invoked by spark_python_task with args:
    --tarball-path     UC Volume path to the dbt_project tarball
    --manifest-path    UC Volume path to the main-branch dbt manifest JSON
    --select-arg       dbt --select argument (e.g. 'state:modified+' or '+all')
    --output-path      UC Volume path where run_results.json will be uploaded

Flow:
    1. Download tarball + manifest from UC Volume to /tmp/.
    2. Extract tarball; manifest is copied into target-main/ alongside dbt_project/.
    3. Pip-install dbt-core + dbt-databricks (version pinned per shim upload).
    4. cd dbt_project; dbt deps; dbt build --select <arg> --state target-main/ --profiles-dir .
    5. Upload target/run_results.json back to UC Volume.
    6. Exit with dbt's exit code.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","message":"%(message)s"}',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# dbt version — keep in sync with pyproject.toml [project.optional-dependencies].dbt.
_DBT_PIN = "dbt-core>=1.10.0,<1.12.0"
_DBT_DATABRICKS_PIN = "dbt-databricks>=1.10.0,<1.12.0"


def _workspace_client() -> WorkspaceClient:
    """Construct a Databricks WorkspaceClient using ambient runtime auth.

    Inside a Databricks job, the runtime provides implicit auth. No
    explicit credentials are needed.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dbt inside a Databricks Job for live CI.")
    parser.add_argument("--tarball-path", required=True, help="UC Volume path to dbt_project tarball")
    parser.add_argument("--manifest-path", required=True, help="UC Volume path to main-branch manifest JSON")
    parser.add_argument("--select-arg", required=True, help="dbt --select argument")
    parser.add_argument("--output-path", required=True, help="UC Volume path to upload run_results.json to")
    return parser.parse_args(argv)


def download_from_volume(volume_path: str, out_path: Path) -> None:
    """Download a file from UC Volume to the local cluster filesystem."""
    ws = _workspace_client()
    logger.info("Downloading %s to %s", volume_path, out_path)
    resp = ws.files.download(volume_path)
    # resp.contents is a stream; read it into bytes.
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as f:
        f.write(resp.contents.read() if hasattr(resp.contents, "read") else resp.contents)


def extract_tarball(tarball: Path, extract_dir: Path) -> None:
    """Extract a .tar.gz into extract_dir."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s to %s", tarball, extract_dir)
    with tarfile.open(tarball, "r:gz") as tf:
        tf.extractall(extract_dir, filter="data")  # noqa: S202 — 'data' filter is the secure 3.12+ default.


def install_dbt() -> None:
    """Install dbt-core and dbt-databricks into the cluster's Python env."""
    logger.info("Installing dbt: %s + %s", _DBT_PIN, _DBT_DATABRICKS_PIN)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", _DBT_PIN, _DBT_DATABRICKS_PIN],
        check=True,
    )


def run_dbt(project_dir: Path, select_arg: str, manifest_main_dir: Path) -> int:
    """Run `dbt deps` + `dbt build --select <arg> --state <manifest_main_dir>`.

    Returns dbt build's exit code (0 = success, 1/2 = warnings/errors).
    """
    logger.info("Running dbt deps in %s", project_dir)
    deps = subprocess.run(
        ["dbt", "deps", "--profiles-dir", "."],
        cwd=project_dir,
        check=False,
    )
    if deps.returncode != 0:
        logger.error("dbt deps failed with exit code %d", deps.returncode)
        return deps.returncode

    logger.info("Running dbt build --select %s --state %s", select_arg, manifest_main_dir)
    build = subprocess.run(
        [
            "dbt", "build",
            "--select", select_arg,
            "--state", str(manifest_main_dir),
            "--profiles-dir", ".",
        ],
        cwd=project_dir,
        check=False,
    )
    return build.returncode


def upload_output(local_path: Path, volume_path: str) -> None:
    """Upload the local run_results.json to UC Volume with overwrite=True."""
    ws = _workspace_client()
    logger.info("Uploading %s to %s", local_path, volume_path)
    with local_path.open("rb") as f:
        ws.files.upload(volume_path, f, overwrite=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    workdir = Path("/tmp/dbt_live_ci")
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)

    tarball = workdir / "dbt_project.tar.gz"
    manifest_main = workdir / "manifest_main.json"
    download_from_volume(args.tarball_path, tarball)
    download_from_volume(args.manifest_path, manifest_main)

    extract_dir = workdir / "extracted"
    extract_tarball(tarball, extract_dir)

    project_dir = extract_dir / "dbt_project"
    if not project_dir.exists():
        raise RuntimeError(f"dbt_project/ not found in tarball (looked at {project_dir})")

    # target-main/ is where --state looks; copy manifest_main in.
    target_main = project_dir / "target-main"
    target_main.mkdir(parents=True, exist_ok=True)
    shutil.copy(manifest_main, target_main / "manifest.json")

    install_dbt()
    exit_code = run_dbt(project_dir, args.select_arg, target_main)

    # Upload run_results.json even on failure — post_dbt_failure_comment.py needs it.
    run_results = project_dir / "target" / "run_results.json"
    if run_results.exists():
        upload_output(run_results, args.output_path)
    else:
        logger.warning("run_results.json not found at %s — uploading empty placeholder", run_results)
        empty = workdir / "empty_run_results.json"
        empty.write_text('{"results": [], "note": "dbt did not produce run_results.json"}')
        upload_output(empty, args.output_path)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3:** Run tests:

```bash
uv run pytest src/tests/test_run_dbt_in_databricks.py -v
```

Expected: all tests pass. Green.

- [ ] **Step 4:** Lint + type check:

```bash
uv run ruff check scripts/ci/ src/tests/test_run_dbt_in_databricks.py
uv run ruff format --check scripts/ci/ src/tests/test_run_dbt_in_databricks.py
uv run pyright scripts/ci/ src/tests/test_run_dbt_in_databricks.py
```

Expected: zero violations on each.

- [ ] **Step 5:** Commit (requires user approval per CLAUDE.md git rules):

```bash
git add scripts/ci/__init__.py scripts/ci/run_dbt_in_databricks.py \
        src/tests/test_run_dbt_in_databricks.py
git commit -m "feat(ci): dbt shim for Databricks-side live CI (PR 4a Phase 1)"
```

### Task 1.3: Upload shim to UC Volume (one-time)

**Files:**
- Create: `scripts/upload_ci_shim.py`.

- [ ] **Step 1:** Create `scripts/upload_ci_shim.py`:

```python
#!/usr/bin/env python3
"""Upload the CI dbt shim to UC Volume. Re-run when the shim changes.

Destination: /Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py

Uses ambient Databricks auth (WorkspaceClient default resolution).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_SHIM_VOLUME_PATH = "/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py"
_LOCAL_SHIM = Path(__file__).parent / "ci" / "run_dbt_in_databricks.py"


def main() -> int:
    if not _LOCAL_SHIM.exists():
        logger.error("Local shim missing at %s", _LOCAL_SHIM)
        return 1

    ws = WorkspaceClient()
    logger.info("Uploading %s to %s", _LOCAL_SHIM, _SHIM_VOLUME_PATH)
    with _LOCAL_SHIM.open("rb") as f:
        ws.files.upload(_SHIM_VOLUME_PATH, f, overwrite=True)
    logger.info("Upload complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2:** Run it once (requires user approval — this is a live Databricks write):

```bash
uv run python scripts/upload_ci_shim.py
```

Expected: "Upload complete". Verify via:

```bash
uv run python - <<'PY'
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()
info = ws.files.get_metadata("/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py")
print(f"Exists: {info.path}, size: {info.file_size}")
PY
```

Expected: path + non-zero file_size.

- [ ] **Step 3:** Commit (requires user approval):

```bash
git add scripts/upload_ci_shim.py
git commit -m "feat(ci): upload-script for Databricks CI shim (PR 4a Phase 1)"
```

---

## Phase 2: GH Actions trigger helper

### Task 2.1: Write trigger_dbt_job tests first (red)

**Files:**
- Create: `src/tests/test_trigger_dbt_job.py`.

- [ ] **Step 1:** Create the test file:

```python
"""Unit tests for scripts.trigger_dbt_job (PR 4a GH-side trigger)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from scripts import trigger_dbt_job as trigger


class TestBuildRunsSubmitPayload:
    def test_payload_structure(self) -> None:
        payload = trigger.build_runs_submit_payload(
            pr_number=42,
            commit_sha="abc1234",
            tarball_volume_path="/Volumes/x/y/z/p.tar.gz",
            manifest_volume_path="/Volumes/x/y/z/m.json",
            select_arg="state:modified+",
            output_volume_path="/Volumes/x/y/z/out.json",
        )
        assert payload["run_name"] == "dbt-live-ci (PR #42, abc1234)"
        assert len(payload["tasks"]) == 1
        task = payload["tasks"][0]
        assert task["task_key"] == "dbt_build"
        assert task["spark_python_task"]["python_file"] == (
            "/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py"
        )
        params = task["spark_python_task"]["parameters"]
        assert "--select-arg" in params
        assert "state:modified+" in params
        assert "--tarball-path" in params
        assert "/Volumes/x/y/z/p.tar.gz" in params


class TestSubmitRun:
    @patch("scripts.trigger_dbt_job.requests.post")
    def test_submit_returns_run_id(self, mock_post: MagicMock) -> None:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"run_id": 12345}
        mock_post.return_value.raise_for_status = MagicMock()

        run_id = trigger.submit_run(
            host="https://workspace.databricks.com",
            token="tok",
            payload={"run_name": "t"},
        )
        assert run_id == 12345

    @patch("scripts.trigger_dbt_job.requests.post")
    def test_submit_propagates_http_error(self, mock_post: MagicMock) -> None:
        err = requests.HTTPError("429 Too Many Requests")
        mock_post.return_value.raise_for_status = MagicMock(side_effect=err)
        with pytest.raises(requests.HTTPError):
            trigger.submit_run(
                host="https://workspace.databricks.com",
                token="tok",
                payload={"run_name": "t"},
            )


class TestPollRun:
    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("scripts.trigger_dbt_job.requests.get")
    def test_poll_returns_on_terminal_success(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: {"state": {"life_cycle_state": "RUNNING"}, "run_page_url": "u"}),
            MagicMock(
                status_code=200,
                json=lambda: {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    },
                    "run_page_url": "u",
                },
            ),
        ]
        for r in mock_get.side_effect:
            r.raise_for_status = MagicMock()

        result = trigger.poll_run(host="h", token="t", run_id=1, max_attempts=10)
        assert result.life_cycle_state == "TERMINATED"
        assert result.result_state == "SUCCESS"
        assert result.run_page_url == "u"

    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("scripts.trigger_dbt_job.requests.get")
    def test_poll_returns_on_terminal_failure(self, mock_get: MagicMock) -> None:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
            "run_page_url": "u",
        }
        mock_get.return_value.raise_for_status = MagicMock()
        result = trigger.poll_run(host="h", token="t", run_id=1, max_attempts=10)
        assert result.result_state == "FAILED"

    @patch("scripts.trigger_dbt_job.time.sleep", new=MagicMock())
    @patch("scripts.trigger_dbt_job.requests.get")
    def test_poll_timeout_raises(self, mock_get: MagicMock) -> None:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "state": {"life_cycle_state": "RUNNING"},
            "run_page_url": "u",
        }
        mock_get.return_value.raise_for_status = MagicMock()
        with pytest.raises(TimeoutError):
            trigger.poll_run(host="h", token="t", run_id=1, max_attempts=3)


class TestUploadTarball:
    @patch("scripts.trigger_dbt_job._workspace_client")
    def test_upload_calls_files_upload(self, mock_ws: MagicMock, tmp_path) -> None:
        p = tmp_path / "proj.tar.gz"
        p.write_bytes(b"fake")
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files

        trigger.upload_tarball(p, "/Volumes/x/y/z/proj.tar.gz")

        mock_files.upload.assert_called_once()
        args, kwargs = mock_files.upload.call_args
        assert args[0] == "/Volumes/x/y/z/proj.tar.gz"
        assert kwargs["overwrite"] is True


class TestMainCLI:
    @patch("scripts.trigger_dbt_job.poll_run")
    @patch("scripts.trigger_dbt_job.submit_run")
    @patch("scripts.trigger_dbt_job.upload_tarball")
    def test_main_returns_zero_on_success(
        self,
        mock_up: MagicMock,
        mock_sub: MagicMock,
        mock_poll: MagicMock,
        tmp_path,
        capsys,
    ) -> None:
        tarball = tmp_path / "p.tar.gz"
        tarball.write_bytes(b"x")
        manifest = tmp_path / "m.json"
        manifest.write_text("{}")
        mock_sub.return_value = 123
        mock_poll.return_value = trigger.RunResult(
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
            run_page_url="https://x",
        )
        rc = trigger.main(
            [
                "--pr-number", "42",
                "--commit-sha", "abc1234",
                "--tarball", str(tarball),
                "--manifest", str(manifest),
                "--select-arg", "state:modified+",
                "--host", "https://workspace.databricks.com",
                "--token", "tok",
                "--volume-prefix", "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert '"result_state": "SUCCESS"' in out

    @patch("scripts.trigger_dbt_job.poll_run")
    @patch("scripts.trigger_dbt_job.submit_run")
    @patch("scripts.trigger_dbt_job.upload_tarball")
    def test_main_returns_nonzero_on_failure(
        self,
        mock_up: MagicMock,
        mock_sub: MagicMock,
        mock_poll: MagicMock,
        tmp_path,
    ) -> None:
        tarball = tmp_path / "p.tar.gz"
        tarball.write_bytes(b"x")
        manifest = tmp_path / "m.json"
        manifest.write_text("{}")
        mock_sub.return_value = 123
        mock_poll.return_value = trigger.RunResult(
            life_cycle_state="TERMINATED",
            result_state="FAILED",
            run_page_url="https://x",
        )
        rc = trigger.main(
            [
                "--pr-number", "42", "--commit-sha", "abc1234",
                "--tarball", str(tarball), "--manifest", str(manifest),
                "--select-arg", "state:modified+",
                "--host", "https://workspace.databricks.com", "--token", "tok",
                "--volume-prefix", "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234",
            ]
        )
        assert rc != 0
```

- [ ] **Step 2:** Run tests:

```bash
uv run pytest src/tests/test_trigger_dbt_job.py -v
```

Expected: `ModuleNotFoundError` for `scripts.trigger_dbt_job`. Red.

### Task 2.2: Write trigger_dbt_job.py (green)

**Files:**
- Create: `scripts/trigger_dbt_job.py`.

- [ ] **Step 1:** Create `scripts/trigger_dbt_job.py`:

```python
#!/usr/bin/env python3
"""GH Actions helper: submit + poll a Databricks one-shot dbt run (PR 4a).

Usage:
    python scripts/trigger_dbt_job.py \
        --pr-number 42 --commit-sha abc1234 \
        --tarball /tmp/dbt_project.tar.gz --manifest /tmp/manifest_main.json \
        --select-arg "state:modified+" \
        --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN" \
        --volume-prefix "/Volumes/soccer_analytics/dev_gold/ci_dbt/42-abc1234"

Emits a JSON object on stdout with final result, e.g.:
    {"run_id": 12345, "life_cycle_state": "TERMINATED", "result_state": "SUCCESS",
     "run_page_url": "https://...", "output_volume_path": "/Volumes/..."}

Exit code: 0 on SUCCESS, 1 on FAILED/CANCELED/INTERNAL_ERROR.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_SHIM_VOLUME_PATH = "/Volumes/soccer_analytics/dev_gold/ci_dbt/_shim/run_dbt_in_databricks.py"
_POLL_INTERVAL_S = 15
_MAX_POLL_ATTEMPTS = 120  # 30 min total at 15s cadence
_IN_FLIGHT = frozenset({"PENDING", "RUNNING", "TERMINATING", "QUEUED"})


@dataclasses.dataclass(frozen=True)
class RunResult:
    life_cycle_state: str
    result_state: str | None
    run_page_url: str


def _workspace_client() -> WorkspaceClient:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def build_runs_submit_payload(
    *,
    pr_number: int,
    commit_sha: str,
    tarball_volume_path: str,
    manifest_volume_path: str,
    select_arg: str,
    output_volume_path: str,
) -> dict[str, Any]:
    """Build the /api/2.0/jobs/runs/submit payload."""
    return {
        "run_name": f"dbt-live-ci (PR #{pr_number}, {commit_sha})",
        "timeout_seconds": 1800,
        "tasks": [
            {
                "task_key": "dbt_build",
                "spark_python_task": {
                    "python_file": _SHIM_VOLUME_PATH,
                    "parameters": [
                        "--tarball-path", tarball_volume_path,
                        "--manifest-path", manifest_volume_path,
                        "--select-arg", select_arg,
                        "--output-path", output_volume_path,
                    ],
                },
                # D2 default: use workspace serverless job compute (no cluster spec).
                # If the workspace doesn't have serverless jobs enabled, add:
                #   "new_cluster": {"spark_version": "14.3.x-scala2.12",
                #                   "num_workers": 0, "node_type_id": "i3.xlarge"}
            }
        ],
    }


def upload_tarball(local_path: Path, volume_path: str) -> None:
    ws = _workspace_client()
    logger.info("Uploading %s (%d bytes) to %s", local_path, local_path.stat().st_size, volume_path)
    with local_path.open("rb") as f:
        ws.files.upload(volume_path, f, overwrite=True)


def submit_run(*, host: str, token: str, payload: dict[str, Any]) -> int:
    host = host.rstrip("/").removeprefix("https://").removeprefix("http://")
    resp = requests.post(
        f"https://{host}/api/2.0/jobs/runs/submit",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=(10, 60),
        verify=True,
    )
    resp.raise_for_status()
    run_id = int(resp.json()["run_id"])
    logger.info("Submitted run_id=%d", run_id)
    return run_id


def poll_run(
    *,
    host: str,
    token: str,
    run_id: int,
    max_attempts: int = _MAX_POLL_ATTEMPTS,
    poll_interval_s: int = _POLL_INTERVAL_S,
) -> RunResult:
    host = host.rstrip("/").removeprefix("https://").removeprefix("http://")
    for attempt in range(max_attempts):
        resp = requests.get(
            f"https://{host}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=(10, 30),
            verify=True,
        )
        resp.raise_for_status()
        body = resp.json()
        life = body.get("state", {}).get("life_cycle_state", "")
        result = body.get("state", {}).get("result_state")
        url = body.get("run_page_url", "")
        logger.info("run_id=%d attempt=%d life_cycle_state=%s result_state=%s", run_id, attempt, life, result)
        if life not in _IN_FLIGHT:
            return RunResult(life_cycle_state=life, result_state=result, run_page_url=url)
        time.sleep(poll_interval_s)
    raise TimeoutError(f"run_id={run_id} did not reach terminal state after {max_attempts} polls")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Trigger a Databricks one-shot dbt run via OIDC.")
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--tarball", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--select-arg", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--volume-prefix", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    tarball_vp = f"{args.volume_prefix}/dbt_project.tar.gz"
    manifest_vp = f"{args.volume_prefix}/manifest_main.json"
    output_vp = f"{args.volume_prefix}/run_results.json"

    upload_tarball(args.tarball, tarball_vp)
    upload_tarball(args.manifest, manifest_vp)

    payload = build_runs_submit_payload(
        pr_number=args.pr_number,
        commit_sha=args.commit_sha,
        tarball_volume_path=tarball_vp,
        manifest_volume_path=manifest_vp,
        select_arg=args.select_arg,
        output_volume_path=output_vp,
    )

    run_id = submit_run(host=args.host, token=args.token, payload=payload)
    result = poll_run(host=args.host, token=args.token, run_id=run_id)

    out = {
        "run_id": run_id,
        "life_cycle_state": result.life_cycle_state,
        "result_state": result.result_state,
        "run_page_url": result.run_page_url,
        "output_volume_path": output_vp,
    }
    print(json.dumps(out))

    return 0 if result.result_state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2:** Run tests:

```bash
uv run pytest src/tests/test_trigger_dbt_job.py -v
```

Expected: all tests pass.

- [ ] **Step 3:** Lint + type check:

```bash
uv run ruff check scripts/trigger_dbt_job.py src/tests/test_trigger_dbt_job.py
uv run ruff format --check scripts/trigger_dbt_job.py src/tests/test_trigger_dbt_job.py
uv run pyright scripts/trigger_dbt_job.py src/tests/test_trigger_dbt_job.py
```

Expected: zero violations.

- [ ] **Step 4:** Commit (requires approval):

```bash
git add scripts/trigger_dbt_job.py src/tests/test_trigger_dbt_job.py
git commit -m "feat(ci): GH Actions trigger helper for Databricks dbt job (PR 4a Phase 2)"
```

---

## Phase 3: Failure-comment poster

### Task 3.1: Write post_dbt_failure_comment tests first (red)

**Files:**
- Create: `src/tests/test_post_dbt_failure_comment.py`.
- Create: `src/tests/fixtures/run_results_mixed.json`.
- Create: `src/tests/fixtures/run_results_all_pass.json`.

- [ ] **Step 1:** Create fixture `src/tests/fixtures/run_results_mixed.json`:

```json
{
  "metadata": {
    "invocation_id": "abc",
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json"
  },
  "results": [
    {
      "status": "success",
      "unique_id": "model.luxury_lakehouse.fct_shots",
      "execution_time": 12.3,
      "message": "OK"
    },
    {
      "status": "error",
      "unique_id": "model.luxury_lakehouse.fct_action_values",
      "execution_time": 0.5,
      "message": "Runtime Error in model fct_action_values (models/marts/fct_action_values.sql)\n  [TABLE_OR_VIEW_NOT_FOUND] The table or view `dim_matches` cannot be found.\n  Compiled Code: ...\n  at line 42 column 15"
    },
    {
      "status": "fail",
      "unique_id": "test.luxury_lakehouse.not_null_fct_action_values_match_key.abcd1234",
      "execution_time": 1.2,
      "failures": 15,
      "message": "Got 15 results, configured to fail if != 0"
    }
  ]
}
```

- [ ] **Step 2:** Create fixture `src/tests/fixtures/run_results_all_pass.json`:

```json
{
  "metadata": {
    "invocation_id": "def",
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v5.json"
  },
  "results": [
    {"status": "success", "unique_id": "model.luxury_lakehouse.fct_shots", "execution_time": 10.0, "message": "OK"}
  ]
}
```

- [ ] **Step 3:** Create `src/tests/test_post_dbt_failure_comment.py`:

```python
"""Unit tests for scripts.post_dbt_failure_comment (PR 4a)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts import post_dbt_failure_comment as post


_FIXTURE_DIR = Path(__file__).parent / "fixtures"


class TestParseRunResults:
    def test_extracts_failing_models_and_tests(self) -> None:
        data = json.loads((_FIXTURE_DIR / "run_results_mixed.json").read_text())
        failures = post.parse_failures(data)
        assert len(failures) == 2
        names = {f.unique_id for f in failures}
        assert "model.luxury_lakehouse.fct_action_values" in names
        assert "test.luxury_lakehouse.not_null_fct_action_values_match_key.abcd1234" in names

    def test_all_pass_returns_empty(self) -> None:
        data = json.loads((_FIXTURE_DIR / "run_results_all_pass.json").read_text())
        assert post.parse_failures(data) == []

    def test_truncates_long_error_message(self) -> None:
        data = {"results": [{"status": "error", "unique_id": "m.x", "message": "\n".join(f"line {i}" for i in range(50))}]}
        failures = post.parse_failures(data)
        assert failures[0].error_excerpt.count("\n") <= 14  # 15 lines max
        assert "... (truncated" in failures[0].error_excerpt


class TestFormatComment:
    def test_happy_path_formatting(self) -> None:
        failures = [
            post.Failure(
                unique_id="model.luxury_lakehouse.fct_foo",
                status="error",
                error_excerpt="TABLE_OR_VIEW_NOT_FOUND\n  line 2",
                failures_count=None,
            ),
            post.Failure(
                unique_id="test.luxury_lakehouse.not_null_bar.123",
                status="fail",
                error_excerpt="Got 15 results",
                failures_count=15,
            ),
        ]
        comment = post.format_comment(
            failures=failures,
            run_page_url="https://workspace.databricks.com/#job/run/1",
        )
        assert "❌ dbt-live-ci failed" in comment
        assert "fct_foo" in comment
        assert "not_null_bar" in comment
        assert "15 failing rows" in comment
        assert "https://workspace.databricks.com/#job/run/1" in comment


class TestFetchRunResultsFromVolume:
    @patch("scripts.post_dbt_failure_comment._workspace_client")
    def test_fetches_and_parses(self, mock_ws: MagicMock) -> None:
        mock_files = MagicMock()
        mock_ws.return_value.files = mock_files
        mock_files.download.return_value = MagicMock(
            contents=b'{"results": [{"status": "success", "unique_id": "m.x", "message": "OK"}]}'
        )
        data = post.fetch_run_results("/Volumes/a/b/c/rr.json")
        assert "results" in data


class TestPostComment:
    @patch("scripts.post_dbt_failure_comment.requests.post")
    def test_posts_with_github_token(self, mock_post: MagicMock) -> None:
        mock_post.return_value.status_code = 201
        mock_post.return_value.raise_for_status = MagicMock()
        post.post_comment_to_pr(
            repo="owner/repo",
            pr_number=42,
            comment_body="body",
            github_token="gh_tok",
        )
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert "repos/owner/repo/issues/42/comments" in args[0]
        assert kwargs["headers"]["Authorization"] == "token gh_tok"

    @patch("scripts.post_dbt_failure_comment.requests.post")
    def test_fork_pr_scope_failure_skips_silently(self, mock_post: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        mock_post.return_value.status_code = 403
        mock_post.return_value.text = "Resource not accessible by integration"

        import requests as req
        err = req.HTTPError("403")
        mock_post.return_value.raise_for_status = MagicMock(side_effect=err)

        # Should NOT raise — fork-PR scope limitation is soft.
        post.post_comment_to_pr(
            repo="owner/repo", pr_number=42, comment_body="b", github_token="gh",
        )
        assert any("fork" in rec.getMessage().lower() or "403" in rec.getMessage() for rec in caplog.records)


class TestMainCLI:
    @patch("scripts.post_dbt_failure_comment.post_comment_to_pr")
    @patch("scripts.post_dbt_failure_comment.fetch_run_results")
    def test_main_returns_zero_after_post(
        self,
        mock_fetch: MagicMock,
        mock_post_fn: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_fetch.return_value = json.loads((_FIXTURE_DIR / "run_results_mixed.json").read_text())
        rc = post.main(
            [
                "--repo", "owner/repo",
                "--pr-number", "42",
                "--run-page-url", "https://x",
                "--run-output-volume-path", "/Volumes/a/b/c/rr.json",
                "--github-token", "gh",
            ]
        )
        assert rc == 0
        mock_post_fn.assert_called_once()

    @patch("scripts.post_dbt_failure_comment.post_comment_to_pr")
    @patch("scripts.post_dbt_failure_comment.fetch_run_results")
    def test_main_skips_comment_on_all_pass(
        self,
        mock_fetch: MagicMock,
        mock_post_fn: MagicMock,
    ) -> None:
        mock_fetch.return_value = json.loads((_FIXTURE_DIR / "run_results_all_pass.json").read_text())
        rc = post.main(
            [
                "--repo", "owner/repo",
                "--pr-number", "42",
                "--run-page-url", "https://x",
                "--run-output-volume-path", "/Volumes/a/b/c/rr.json",
                "--github-token", "gh",
            ]
        )
        assert rc == 0
        mock_post_fn.assert_not_called()
```

- [ ] **Step 4:** Run tests:

```bash
uv run pytest src/tests/test_post_dbt_failure_comment.py -v
```

Expected: `ModuleNotFoundError`. Red.

### Task 3.2: Write post_dbt_failure_comment.py (green)

**Files:**
- Create: `scripts/post_dbt_failure_comment.py`.

- [ ] **Step 1:** Create `scripts/post_dbt_failure_comment.py`:

```python
#!/usr/bin/env python3
"""GH Actions helper: post a PR comment summarizing dbt failures (PR 4a).

Reads dbt's target/run_results.json (uploaded to UC Volume by the shim),
parses failing models + tests, posts a summary comment to the PR.

Exit code: 0 on success (regardless of whether failures were found —
this helper posts OR skips; the trigger helper owns the merge-block signal).
1 only on an unexpected error that prevents any attempt to post.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_MAX_ERROR_LINES = 15


@dataclasses.dataclass(frozen=True)
class Failure:
    unique_id: str
    status: str
    error_excerpt: str
    failures_count: int | None


def _workspace_client() -> WorkspaceClient:
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def fetch_run_results(volume_path: str) -> dict[str, Any]:
    """Download run_results.json from UC Volume and parse as JSON."""
    ws = _workspace_client()
    logger.info("Fetching run_results from %s", volume_path)
    resp = ws.files.download(volume_path)
    contents = resp.contents.read() if hasattr(resp.contents, "read") else resp.contents
    return json.loads(contents)


def parse_failures(run_results: dict[str, Any]) -> list[Failure]:
    """Extract failing models and tests from a run_results.json dict."""
    failures: list[Failure] = []
    for r in run_results.get("results", []):
        status = r.get("status", "")
        if status not in ("error", "fail"):
            continue
        message = r.get("message", "")
        lines = message.splitlines()
        if len(lines) > _MAX_ERROR_LINES:
            excerpt = "\n".join(lines[:_MAX_ERROR_LINES]) + f"\n... (truncated — {len(lines) - _MAX_ERROR_LINES} more lines)"
        else:
            excerpt = message
        failures.append(
            Failure(
                unique_id=r.get("unique_id", ""),
                status=status,
                error_excerpt=excerpt,
                failures_count=r.get("failures"),
            )
        )
    return failures


def format_comment(*, failures: list[Failure], run_page_url: str) -> str:
    """Build the PR comment body."""
    lines = ["### ❌ dbt-live-ci failed", ""]
    lines.append("**Failing models/tests:**")
    for f in failures:
        name = f.unique_id.split(".")[-1] if "." in f.unique_id else f.unique_id
        if f.failures_count is not None:
            lines.append(f"- `{name}` — {f.failures_count} failing rows")
        else:
            lines.append(f"- `{name}` — {f.status}")
    lines.append("")
    lines.append("**Error excerpt (first failure):**")
    lines.append("")
    lines.append("```")
    if failures:
        lines.append(failures[0].error_excerpt)
    lines.append("```")
    lines.append("")
    lines.append(f"[Databricks run log →]({run_page_url})")
    return "\n".join(lines)


def post_comment_to_pr(
    *,
    repo: str,
    pr_number: int,
    comment_body: str,
    github_token: str,
) -> None:
    """POST a comment via GH API. Fork-scope 403 is logged and swallowed."""
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github+json",
            },
            json={"body": comment_body},
            timeout=(10, 30),
            verify=True,
        )
        resp.raise_for_status()
        logger.info("Posted PR comment (status=%d)", resp.status_code)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        body = exc.response.text if exc.response is not None else ""
        if status == 403 and "not accessible" in body.lower():
            logger.warning("PR comment 403 — fork-PR scope limitation. Skipping comment.")
            return
        logger.warning("PR comment POST failed: status=%s body=%s", status, body[:200])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Post dbt-failure PR comment.")
    parser.add_argument("--repo", required=True, help='GH repo, e.g. "owner/repo"')
    parser.add_argument("--pr-number", type=int, required=True)
    parser.add_argument("--run-page-url", required=True)
    parser.add_argument("--run-output-volume-path", required=True)
    parser.add_argument("--github-token", required=True)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    try:
        results = fetch_run_results(args.run_output_volume_path)
    except Exception as exc:
        logger.warning("Could not fetch run_results.json: %s. Posting generic failure comment.", exc)
        generic = (
            "### ❌ dbt-live-ci failed\n\n"
            "dbt failed before producing `run_results.json`. "
            f"[Databricks run log →]({args.run_page_url})"
        )
        post_comment_to_pr(
            repo=args.repo,
            pr_number=args.pr_number,
            comment_body=generic,
            github_token=args.github_token,
        )
        return 0

    failures = parse_failures(results)
    if not failures:
        logger.info("No failures in run_results — skipping comment.")
        return 0

    body = format_comment(failures=failures, run_page_url=args.run_page_url)
    post_comment_to_pr(
        repo=args.repo,
        pr_number=args.pr_number,
        comment_body=body,
        github_token=args.github_token,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2:** Run tests:

```bash
uv run pytest src/tests/test_post_dbt_failure_comment.py -v
```

Expected: all tests pass.

- [ ] **Step 3:** Lint + type check:

```bash
uv run ruff check scripts/post_dbt_failure_comment.py src/tests/test_post_dbt_failure_comment.py
uv run ruff format --check scripts/post_dbt_failure_comment.py src/tests/test_post_dbt_failure_comment.py
uv run pyright scripts/post_dbt_failure_comment.py src/tests/test_post_dbt_failure_comment.py
```

Expected: zero violations.

- [ ] **Step 4:** Commit (requires approval):

```bash
git add scripts/post_dbt_failure_comment.py src/tests/test_post_dbt_failure_comment.py \
        src/tests/fixtures/run_results_mixed.json src/tests/fixtures/run_results_all_pass.json
git commit -m "feat(ci): PR-comment poster for dbt failures (PR 4a Phase 3)"
```

---

## Phase 4: GH Actions workflow

### Task 4.1: Write the workflow

**Files:**
- Create: `.github/workflows/dbt-live-ci.yml`.

- [ ] **Step 1:** Create `.github/workflows/dbt-live-ci.yml`:

```yaml
name: dbt live CI

# Live dbt build via Databricks Job. Complements dbt-ci.yml (parse-only).
# Closes the gap where runtime SQL errors merged to main undetected in PR 3.
# See docs/superpowers/plans/2026-04-23-kimball-pr4-live-ci.md for design.

on:
  pull_request:
    paths:
      - "dbt_project/**"
      - "src/ingestion/**"   # catch Python-producer changes that affect dbt sources
      - ".github/workflows/dbt-live-ci.yml"

permissions:
  contents: read
  id-token: write          # OIDC to Databricks
  pull-requests: write     # Post comment on failure

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  live-build:
    runs-on: ubuntu-latest
    timeout-minutes: 40    # workflow ceiling; trigger_dbt_job has its own 30-min poll budget
    env:
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
      DATABRICKS_AUTH_TYPE: github-oidc
    steps:
      - uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd  # v6.0.2
        with:
          fetch-depth: 0   # full history for state diff

      - name: Fetch main for state comparison
        run: git fetch origin main:refs/remotes/origin/main

      - name: Install uv
        uses: astral-sh/setup-uv@08807647e7069bb48b6ef5acd8ec9567f424441b  # v8.1.0

      - name: Set up Python 3.12
        run: uv python install 3.12

      - name: Install dependencies
        run: uv sync --frozen --extra dbt --no-install-project

      - name: Generate PR-branch manifest
        working-directory: dbt_project
        run: |
          uv run --no-sync dbt deps --profiles-dir .
          uv run --no-sync dbt parse --profiles-dir .

      - name: Generate main-branch manifest
        working-directory: dbt_project
        run: |
          git stash --include-untracked
          git checkout origin/main -- .
          uv run --no-sync dbt deps --profiles-dir .
          uv run --no-sync dbt parse --profiles-dir .
          mkdir -p ../target-main
          cp target/manifest.json ../target-main/manifest.json
          git checkout ${{ github.sha }} -- .
          git stash pop || true

      - name: Compute --select argument
        id: select
        run: |
          set -e
          config_diff=0
          for f in dbt_project/dbt_project.yml dbt_project/packages.yml dbt_project/profiles.yml; do
            if ! git diff --quiet origin/main -- "$f"; then
              config_diff=1
              break
            fi
          done
          if [ "$config_diff" = "1" ]; then
            echo "select_arg=+all" >> "$GITHUB_OUTPUT"
            echo "Config diff detected — running full build"
          else
            echo "select_arg=state:modified+" >> "$GITHUB_OUTPUT"
            echo "No config diff — running state:modified+"
          fi

      - name: Package dbt_project
        run: |
          tar -czf /tmp/dbt_project.tar.gz --exclude='dbt_project/target' --exclude='dbt_project/target-main' --exclude='dbt_project/dbt_packages' dbt_project/
          ls -la /tmp/dbt_project.tar.gz

      - name: Acquire Databricks token via OIDC
        id: databricks_token
        run: |
          # GitHub OIDC → Databricks SP token exchange.
          # Same pattern used by terraform-apply.yml via databricks CLI.
          uv run pip install --quiet databricks-sdk
          TOKEN=$(uv run python -c "
          import os
          from databricks.sdk import WorkspaceClient
          ws = WorkspaceClient(
              host=os.environ['DATABRICKS_HOST'],
              client_id=os.environ['DATABRICKS_CLIENT_ID'],
              auth_type='github-oidc',
          )
          auth = ws.config.authenticate()
          print(auth['Authorization'].replace('Bearer ', ''))
          ")
          echo "::add-mask::$TOKEN"
          echo "token=$TOKEN" >> "$GITHUB_OUTPUT"

      - name: Trigger Databricks dbt run
        id: trigger
        env:
          SHORT_SHA: ${{ github.event.pull_request.head.sha }}
        run: |
          uv run python scripts/trigger_dbt_job.py \
            --pr-number "${{ github.event.pull_request.number }}" \
            --commit-sha "${SHORT_SHA:0:7}" \
            --tarball /tmp/dbt_project.tar.gz \
            --manifest target-main/manifest.json \
            --select-arg "${{ steps.select.outputs.select_arg }}" \
            --host "${{ vars.DATABRICKS_HOST }}" \
            --token "${{ steps.databricks_token.outputs.token }}" \
            --volume-prefix "/Volumes/soccer_analytics/dev_gold/ci_dbt/${{ github.event.pull_request.number }}-${SHORT_SHA:0:7}" \
            > /tmp/trigger_result.json
          cat /tmp/trigger_result.json
          echo "result=$(cat /tmp/trigger_result.json)" >> "$GITHUB_OUTPUT"

      - name: Post PR comment on failure
        if: failure() && steps.trigger.outcome == 'failure'
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          result='${{ steps.trigger.outputs.result }}'
          run_page_url=$(echo "$result" | uv run python -c "import json,sys; print(json.load(sys.stdin).get('run_page_url',''))")
          output_path=$(echo "$result" | uv run python -c "import json,sys; print(json.load(sys.stdin).get('output_volume_path',''))")
          uv run python scripts/post_dbt_failure_comment.py \
            --repo "${{ github.repository }}" \
            --pr-number "${{ github.event.pull_request.number }}" \
            --run-page-url "$run_page_url" \
            --run-output-volume-path "$output_path" \
            --github-token "$GITHUB_TOKEN"
```

- [ ] **Step 2:** Validate YAML syntax:

```bash
uv run --with PyYAML python -c "import yaml; yaml.safe_load(open('.github/workflows/dbt-live-ci.yml'))"
```

Expected: no output (successful parse). If yaml errors: fix before committing.

- [ ] **Step 3:** Validate action-lint (optional but recommended):

```bash
# If actionlint is installed locally:
actionlint .github/workflows/dbt-live-ci.yml 2>/dev/null || echo "actionlint not installed — skipping"
```

Expected: no issues reported, or actionlint missing (non-fatal).

- [ ] **Step 4:** Commit (requires approval):

```bash
git add .github/workflows/dbt-live-ci.yml
git commit -m "feat(ci): live dbt build workflow via Databricks Job (PR 4a Phase 4)"
```

---

## Phase 5: End-to-end verification on a scratch PR

### Task 5.1: Create scratch branch with an intentionally broken model

**Files:** Throwaway branch for verification only.

- [ ] **Step 1:** Check out a new branch from `kimball-pr4-live-ci`:

```bash
git checkout -b scratch-dbt-live-ci-smoke
```

- [ ] **Step 2:** Introduce an intentional break in a dbt model. Pick a low-risk mart to corrupt temporarily:

```bash
# Example: break fct_action_values by referencing a non-existent table.
cat >> dbt_project/models/marts/fct_action_values.sql <<'SQL'
-- temporary smoke test; revert before merge
-- select * from {{ ref('dim_does_not_exist') }}
SQL
```

(Uncomment the line so it actually breaks, then comment again to re-verify green state later.)

- [ ] **Step 3:** Push the scratch branch and open a PR from it:

```bash
git add dbt_project/models/marts/fct_action_values.sql
git commit -m "test: intentional dbt-live-ci smoke failure (DO NOT MERGE)"
git push -u origin scratch-dbt-live-ci-smoke
gh pr create --draft --base kimball-pr4-live-ci --title "SMOKE: dbt-live-ci break (DO NOT MERGE)" --body "Smoke test for PR 4a live CI. Will be closed."
```

### Task 5.2: Verify red path

**Files:** None — observation only.

- [ ] **Step 1:** Watch the workflow run:

```bash
gh run watch --workflow=dbt-live-ci.yml
```

Expected:
- Run reaches the "Trigger Databricks dbt run" step.
- `trigger_dbt_job.py` submits and polls; Databricks job FAILS when dbt hits the missing ref.
- Workflow step exits 1; "Post PR comment on failure" step runs.
- `post_dbt_failure_comment.py` fetches run_results.json, parses, posts comment.
- Workflow marked red.

- [ ] **Step 2:** Inspect the PR comment. Confirm:
  - Title line: "❌ dbt-live-ci failed"
  - `fct_action_values` listed as failing model
  - Error excerpt mentions `dim_does_not_exist`
  - Databricks run log link is present and opens the run

- [ ] **Step 3:** Inspect the required-check status on the PR:

```bash
gh pr checks
```

Expected: `dbt-live-ci / live-build` shows red ❌. Merge button in GitHub UI is blocked (check the PR's web view).

### Task 5.3: Verify green path

- [ ] **Step 1:** Revert the break:

```bash
# Remove the broken line
sed -i '/dim_does_not_exist/d' dbt_project/models/marts/fct_action_values.sql
git add dbt_project/models/marts/fct_action_values.sql
git commit -m "test: revert smoke break (DO NOT MERGE)"
git push
```

- [ ] **Step 2:** Watch the workflow:

```bash
gh run watch --workflow=dbt-live-ci.yml
```

Expected: all steps green; no PR comment posted; check shows green ✅.

### Task 5.4: Cleanup

- [ ] **Step 1:** Close the scratch PR + delete the branch:

```bash
gh pr close --delete-branch
```

---

## Phase 6: Ship PR 4a

### Task 6.1: Include the spec file in PR 4a's first commit

**Files:**
- Add: `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md` (already on disk; not yet staged).

- [ ] **Step 1:** Per user guidance 2026-04-23, the spec doc rides with PR 4a's first commit rather than a separate spec-only commit.

```bash
git add docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md \
        docs/superpowers/plans/2026-04-23-kimball-pr4-live-ci.md
# If this file already exists and is staged in an earlier task's commit, skip.
# Otherwise amend into the first PR 4a commit at rebase time, OR include here
# as a fresh commit just before opening the PR.
git commit -m "docs: PR 4 spec + PR 4a plan"
```

Ask the user: is this a separate commit, or amend into Phase 1 Task 1.2's commit? Default to a separate commit because amend rewrites history.

### Task 6.2: Open PR 4a

- [ ] **Step 1:** Open the PR (requires user approval):

```bash
gh pr create \
  --base main \
  --title "feat(ci): live dbt CI via Databricks Job (Kimball PR 4a)" \
  --body "$(cat <<'EOF'
## Summary
- Adds `.github/workflows/dbt-live-ci.yml` (pull_request) that runs live `dbt build --select state:modified+` against dev_gold via a Databricks Job.
- New shim `scripts/ci/run_dbt_in_databricks.py` runs inside the Databricks cluster; GH Actions side uses `scripts/trigger_dbt_job.py` + `scripts/post_dbt_failure_comment.py`.
- Closes the gap where PR 3's try_cast bugs merged undetected — the existing `dbt-ci.yml` runs parse-only because Thrift is unreachable from public runners.

## Test plan
- [x] Unit tests pass (`src/tests/test_trigger_dbt_job.py`, `src/tests/test_post_dbt_failure_comment.py`, `src/tests/test_run_dbt_in_databricks.py`).
- [x] E2E smoke on scratch branch — red path posts PR comment + blocks merge; green path is silent + unblocks.
- [x] Existing `dbt-ci.yml` (parse-only) unchanged and still green on baseline.

Part of Kimball PR 4 series (spec: `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`). PR 4b (Action Values migration) depends on this merging first.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2:** After user approves merge:

```bash
# User merges via GH UI (or, with approval, gh pr merge --squash --delete-branch).
```

### Task 6.3: Post-merge monitoring

- [ ] **Step 1:** Watch the first 3 PRs after PR 4a merges for live-CI behavior. If any unexpected failure pattern: note in a follow-up issue.

- [ ] **Step 2:** Open a follow-up issue for the UC Volume retention cron (D3 TBD-item): `ci_dbt/` entries older than 14 days should be GC'd. Not in PR 4a scope; tracked separately.

---

## Self-review findings (plan author notes)

**Spec coverage:** PR 4a scope in spec §2 maps to Phases 1–5. Spec §5.1 flow maps to the workflow yaml. Spec §5.2 Job spec maps to Phase 2's `build_runs_submit_payload`. Spec §5.4 comment format maps to Phase 3's `format_comment`. Spec §5.5 failure-surface robustness maps to the fork-scope handling in Phase 3 Task 3.1 tests.

**Placeholders scan:** None. D1–D4 are acknowledged decisions with defaults, not TBDs. D3 retention cron is explicitly out of scope and logged for follow-up.

**Type consistency:** `RunResult` dataclass in Phase 2 matches test expectations. `Failure` dataclass in Phase 3 matches test expectations. Entry-point signatures stable across tests and implementation.

**Known ambiguities for execution time:**
- **OIDC token acquisition step in workflow (`databricks_token`).** The exact Python snippet to exchange GH OIDC → Databricks SP token is tentative; validate against the Databricks SDK's `github-oidc` auth mode. If the inline snippet doesn't work, replace with a `databricks auth` CLI invocation following `terraform-apply.yml`'s pattern.
- **Serverless compute availability.** If the Databricks workspace doesn't have serverless jobs enabled, Phase 2 payload needs a `new_cluster` block. Phase 0 Task 0.1 Step 2 probes this indirectly (list jobs); direct confirmation happens on the first real `runs_submit` call.
- **UC Volume path structure.** Plan uses `/Volumes/soccer_analytics/dev_gold/ci_dbt/` (Phase 0 Task 0.1 Step 3 confirmed schema=`dev_gold` is the right home — existing `model_weights` / `training_data` volumes are already there; `observability` schema has no volumes; no `ops` schema exists). Volume does NOT exist at plan-start; created as Task 0.1 Step 3.bis via SDK + tracked in Terraform going forward.

These are honest uncertainties surfaced during plan-writing — not placeholders. Execution time confirms or redirects.
