# Kimball PR 4c — HF README helper + dataset cards + org-card refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-repo source of truth for HuggingFace dataset READMEs under `docs/huggingface/dataset-cards/`, a shared `src/ingestion/hf_publish.py` helper that uploads any README (dataset OR org Space) to HF Hub, and wire the helper into all four existing publish scripts (`publish_spadl_vaep_hf.py`, `publish_xg_shots_hf.py`, `publish_freeze_frame_hf.py`, `publish_hf_org_card.py`). Closes the PR 3 deferral where per-dataset READMEs drift between in-repo content and what's live on HF Hub.

**Architecture:** A single `upload_hf_readme(repo_id, readme_path, hf_token, repo_type)` function in `src/ingestion/hf_publish.py` handles both `repo_type="dataset"` and `repo_type="space"`. Each `publish_*_hf.py` script calls it at the end of `main()` after the data upload completes — so if data upload fails, README doesn't land either (atomic publish discipline). `scripts/publish_hf_org_card.py` refactors from its current ad-hoc `HfApi.upload_file` call to use the shared helper, unifying the four HF-publish code paths. Dataset cards live under `docs/huggingface/dataset-cards/<repo-name>.md` with the same YAML frontmatter convention used by `docs/huggingface/model-cards/` (already established in the repo).

**Tech Stack:** huggingface_hub Python SDK, pytest, markdown with YAML frontmatter.

**Source spec:** `docs/superpowers/specs/2026-04-23-kimball-pr4-action-values-plus-deferrals-design.md`.

**Depends on:** Kimball PR 4a (live dbt CI) must be merged first. PR 4b (Action Values migration) does NOT block PR 4c — they're orthogonal. But PR 4c landing AFTER PR 4b means the `spadl-vaep-action-values` dataset card (including dual-column sunset) is in place on HF when a post-PR-4b publish run happens. Exact merge ordering: 4a first (gate for 4b's CI); 4b next (ships the data-layer changes); 4c last (README helper auto-uploads updated cards on next publish run).

---

## Decisions required — resolve before/during execution

| # | Decision | Default (this plan assumes) | Alternative |
|---|---|---|---|
| **D1** | `hf_publish.py` error handling on upload failure | **Raise `HfHubHTTPError` loudly; let the calling publish script fail.** Each `publish_*_hf.py` does NOT catch — data already landed on HF, but if the README upload fails, the run fails and the operator re-runs. HfApi uploads are idempotent. | Catch + log-warning + continue — leaves the dataset with stale README; invisible failure. Rejected per CLAUDE.md ADR-002 (no silent-swallow). |
| **D2** | Order of operations in `publish_*_hf.py::main()` | **Data upload first, README upload last.** If README upload fails, the data is already up-to-date on HF — next run succeeds and catches up. If data upload fails, README upload is skipped (earlier raise). | README first — if data upload later fails, README is stale (claims new columns that aren't in the data). Rejected. |
| **D3** | Commit granularity within PR 4c | **Two commits:** (1) helper + tests + 4 dataset-card files; (2) publish-script integrations + org-card refactor + org-card markdown refresh. Squash on merge. | One commit — less reviewable. Four commits — extra bookkeeping; squashed-away. |
| **D4** | Source of existing-on-HF dataset-card content | **Fetch current HF dataset cards during implementation**, copy content into in-repo `.md` files, augment as needed. For `xg-shot-data` and `statsbomb-shots-on-target`, the existing dual-column warnings on HF (pushed manually during PR 3) become the baseline. For `spadl-vaep-action-values`, author fresh content including the 2026-07-22 sunset block. For `xg-freeze-frame-data`, author fresh baseline content. | Author fresh content for all four — risks dropping PR-3-era content that HF consumers already read. Rejected. |
| **D5** | `publish_hf_org_card.py` behavior change scope | **Pure refactor — external signature + side effects unchanged.** The script still pushes `docs/huggingface/org-card.md` to the `luxury-lakehouse/README` Space's `README.md`. Only internal implementation swaps from ad-hoc `HfApi.upload_file` to `upload_hf_readme(repo_type="space")`. | Expand scope to also push `luxury-lakehouse.jpg` via the helper — out of scope; that binary is handled separately per memory `reference_hf_org_card_manual.md`. |
| **D6** | How `export_shots_on_target.py` (wheel-installed Databricks workflow task) accesses the dataset card | **Bundle `docs/huggingface/dataset-cards/` in the wheel via `pyproject.toml` force-include** (same mechanism already used for `dbt_project/dbt_packages/`). Bump wheel version; run `scripts/bump_wheel.py` to sync 19 consumers. Add `get_dataset_card_path(name)` helper in `hf_publish.py` that resolves wheel-first, falls back to repo root. All four publishers use the helper. Reasoning: dbt_packages precedent establishes the force-include pattern; 20KB payload; wheel bumps happen routinely; avoids drift risk of a manual-push alternative (path D) and the package-relocation cost of importlib.resources (path B). | Path D (separate one-shot script, manual cadence) — rejected due to drift risk on dual-column sunset updates. Path B (move cards into a Python package) — rejected because it violates the Q7 decision that cards live under `docs/`. Path C (UC Volume upload + runtime read) — rejected as over-engineering for this one workflow-bound publisher. |

---

## File structure map

### Created

| Path | Responsibility |
|---|---|
| `src/ingestion/hf_publish.py` | The shared helper. Single public function `upload_hf_readme(repo_id, readme_path, hf_token, *, repo_type)`. Validates inputs (file exists + non-empty + `repo_id` matches HF's pattern), LF-normalizes line endings, uploads via `HfApi.upload_file`, returns `{"commit_url": ..., "sha256": ...}` dict. Module docstring frames it as the peer to `artifact_deploy.py` (ML-weights delivery). |
| `src/tests/test_hf_publish.py` | Unit tests for the helper. HfApi mocked; validates argument threading, error cases, LF normalization. |
| `docs/huggingface/dataset-cards/spadl-vaep-action-values.md` | Dataset card. Includes 90-day dual-column sunset warning block (sunset 2026-07-22). |
| `docs/huggingface/dataset-cards/xg-shot-data.md` | Dataset card. Mirrors existing on-HF content (PR 3 dual-column warning). |
| `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md` | Dataset card. Mirrors existing on-HF content (PR 3 dual-column warning, sunset 2026-07-22). |
| `docs/huggingface/dataset-cards/xg-freeze-frame-data.md` | Dataset card. Description only (no dual-column — dataset untouched by Kimball). |

### Modified

| Path | Reason |
|---|---|
| `scripts/publish_spadl_vaep_hf.py` | Import `upload_hf_readme` + `get_dataset_card_path` from `ingestion.hf_publish`; call at end of `main()` after data upload. |
| `scripts/publish_xg_shots_hf.py` | Same — add `upload_hf_readme` call using `get_dataset_card_path`. |
| `scripts/publish_freeze_frame_hf.py` | Same — add `upload_hf_readme` call using `get_dataset_card_path`. |
| `scripts/publish_hf_org_card.py` | Refactor: replace existing `HfApi.upload_file(...)` call with `upload_hf_readme("luxury-lakehouse/README", ..., repo_type="space")`. Behavior unchanged externally. |
| `src/ingestion/export_shots_on_target.py` | Add `upload_hf_readme` call after `_upload_to_hf_hub()` returns in `run_pipeline()`. Uses `get_dataset_card_path("statsbomb-shots-on-target.md")` for wheel-aware resolution. |
| `docs/huggingface/org-card.md` | Refresh: any stale dataset-schema mentions; add references to the two 2026-07-22 sunsets (spadl-vaep-action-values, statsbomb-shots-on-target); verify dataset link list matches the 4 currently published datasets. |
| `pyproject.toml` | Bump wheel version (e.g. 0.3.13 → 0.3.14); add `docs/huggingface/dataset-cards/` to `[tool.hatch.build.targets.wheel.force-include]` (or whatever the existing force-include key is — verify in Phase 0 Task 0.6). |
| Wheel consumers (19 places) | Synced by `scripts/bump_wheel.py`. One command invocation updates `pyproject.toml` references + PEP 723 script headers + Terraform wheel URLs + `deploy.sh` + `hf_taipy_app/requirements.txt` + `src/shared/wheel.py`. |

### Explicitly NOT modified (Chesterton's Fence)

- `src/ingestion/artifact_deploy.py` — ML-weights delivery module; different concern (ADR-012 producer-side weight delivery). `hf_publish.py` is a peer, not an extension.
- `docs/huggingface/model-cards/` — existing directory for governance model cards per CLAUDE.md `AI_GOVERNANCE.md` rule. Untouched.
- `scripts/manage_space.py` — HF Space deployment for the Taipy app; separate concern from dataset-card publishing.
- Wheel and `pyproject.toml` — `hf_publish.py` ships in the wheel via the existing `src/ingestion/` package. No new entry points or `[project.scripts]` bumps.

---

## Phase 0: Pre-flight verification (read-only)

### Task 0.1: Baseline — PR 4a + PR 4b merged and green

**Files:** None.

- [ ] **Step 1:**

```bash
gh pr list --state closed --limit 10 --json title,mergedAt,url | grep -E "PR 4[ab]|PR #"
```

Expected: Kimball PR 4a and PR 4b appear as merged. If either is still open, stop — PR 4c should land after both.

- [ ] **Step 2:** Confirm `dbt-live-ci.yml` is green on recent PRs:

```bash
gh run list --workflow=dbt-live-ci.yml --limit=5 --json conclusion,headBranch
```

Expected: recent `conclusion: success`.

### Task 0.2: Verify existing HF content baseline

**Files:** None — live HF API + browser check.

- [ ] **Step 1:** Fetch current HF dataset-card content for the four datasets:

```bash
uv run --no-project --with huggingface-hub python - <<'PY'
from huggingface_hub import hf_hub_download
import os
for name in ["spadl-vaep-action-values", "xg-shot-data", "statsbomb-shots-on-target", "xg-freeze-frame-data"]:
    try:
        p = hf_hub_download(
            repo_id=f"luxury-lakehouse/{name}",
            filename="README.md",
            repo_type="dataset",
            token=os.environ.get("HF_TOKEN"),
        )
        print(f"=== {name} ===")
        print(open(p).read()[:3000])
        print()
    except Exception as e:
        print(f"{name}: no README (or fetch error: {e})")
        print()
PY
```

Expected: README content (or 404 if never published). Capture the content — it's the baseline for D4's decision.

- [ ] **Step 2:** Fetch current `luxury-lakehouse/README` Space content:

```bash
uv run --no-project --with huggingface-hub python - <<'PY'
from huggingface_hub import hf_hub_download
import os
p = hf_hub_download(
    repo_id="luxury-lakehouse/README",
    filename="README.md",
    repo_type="space",
    token=os.environ.get("HF_TOKEN"),
)
print(open(p).read())
PY
```

Expected: current org-card content. Diff against `docs/huggingface/org-card.md` locally:

```bash
diff <(uv run --no-project --with huggingface-hub python -c "
from huggingface_hub import hf_hub_download
import os
print(open(hf_hub_download(repo_id='luxury-lakehouse/README', filename='README.md', repo_type='space', token=os.environ.get('HF_TOKEN'))).read())
") docs/huggingface/org-card.md
```

Record whether they match. If they don't, note the divergence — Phase 5 may need to reconcile.

### Task 0.3: Verify `HfApi.upload_file` signature

**Files:** None.

- [ ] **Step 1:**

```bash
uv run --no-project --with huggingface-hub python -c "
from huggingface_hub import HfApi
import inspect
print(inspect.signature(HfApi().upload_file))
"
```

Expected signature includes `path_or_fileobj`, `path_in_repo`, `repo_id`, `repo_type`, `token`, `commit_message` — confirms our helper calls the right parameters.

### Task 0.4: Verify HF_TOKEN available in local env

**Files:** None.

- [ ] **Step 1:**

```bash
uv run python -c "import os; assert os.environ.get('HF_TOKEN'), 'HF_TOKEN not set'; print('HF_TOKEN present, length:', len(os.environ['HF_TOKEN']))"
```

Expected: non-empty token. If missing: `export HF_TOKEN=$(cat ~/.hf_token)` (or wherever the user stores it) before continuing.

### Task 0.5: Check whether dataset-cards directory already exists

**Files:** None.

- [ ] **Step 1:**

```bash
ls docs/huggingface/ 2>/dev/null
```

Expected: `model-cards/`, `org-card.md`, possibly `luxury-lakehouse.jpg`. No `dataset-cards/` yet — this PR creates it.

If `dataset-cards/` already exists (unexpected — no memory evidence of it): stop and investigate what's in it before clobbering.

### Task 0.6: Inspect wheel-bundling conventions (drives D6 edits)

**Files:** None — read-only.

- [ ] **Step 1:** Check current wheel version + hatch build configuration:

```bash
grep -A 20 '^\[tool.hatch' pyproject.toml
grep -E '^version|^name' pyproject.toml | head -5
```

Expected: find the existing `[tool.hatch.build.targets.wheel]` (or similar) block with a force-include or artifacts key that already bundles `dbt_project/dbt_packages/`. Record the exact key name + syntax — we'll match it. Record the current wheel version (memory says 0.3.13; confirm).

- [ ] **Step 2:** Inspect `scripts/bump_wheel.py` to understand how it syncs 19 consumers:

```bash
head -80 scripts/bump_wheel.py
```

Expected: a CLI with a `--new-version` argument (or it reads pyproject.toml). Note the exact invocation the task will use later.

- [ ] **Step 3:** Verify `src/shared/wheel.py` (memory `reference_wheel_consumers` calls this the source of truth):

```bash
cat src/shared/wheel.py
```

Expected: a module that holds the canonical wheel URL / hash references consumed by other scripts. `bump_wheel.py` updates this in-place.

### Task 0.7: Verify how `export_shots_on_target.py` is installed at runtime

**Files:** None — read-only verification.

- [ ] **Step 1:** Confirm that the `wf-export-shots` workflow task references the wheel (and which version):

```bash
grep -rn "export_shots_on_target\|wf-export-shots\|export-shots" terraform/modules/workflows/ workflow-cards/ 2>/dev/null
```

Expected: workflow-card YAML or Terraform defines the task with a wheel-install step or entry-point reference. Record.

- [ ] **Step 2:** Verify site-packages layout assumption. Build the wheel locally and inspect:

```bash
uv build --wheel --out-dir /tmp/wheel_inspect 2>&1 | tail -5
ls /tmp/wheel_inspect/
uv run --no-project --with /tmp/wheel_inspect/luxury_lakehouse-*.whl python - <<'PY'
import ingestion
from pathlib import Path
print("ingestion package:", Path(ingestion.__file__).resolve())
# Check that force-included docs/ are a sibling of ingestion/ in site-packages.
# After PR 4c this should show docs/ alongside ingestion/.
parent = Path(ingestion.__file__).resolve().parent.parent
print("parent contents:", sorted(p.name for p in parent.iterdir())[:20])
PY
```

**Note:** this verification happens AFTER Phase 2 Task 2.7 adds the force-include. Before that edit, docs/ will NOT appear in the wheel — expected. Use this step post-Phase-2.7 to confirm the force-include worked.

---

## Phase 1: `hf_publish.py` helper + tests

### Task 1.1: Write tests first (red)

**Files:**
- Create: `src/tests/test_hf_publish.py`.

- [ ] **Step 1:** Create `src/tests/test_hf_publish.py`:

```python
"""Unit tests for src.ingestion.hf_publish (PR 4c).

Covers:
  - upload_hf_readme happy path (dataset + space).
  - Input validation: missing file, empty file, malformed repo_id.
  - LF normalization: CRLF input is converted.
  - HfApi failures propagate (no silent swallow).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion import hf_publish


@pytest.fixture()
def readme_file(tmp_path: Path) -> Path:
    p = tmp_path / "README.md"
    p.write_text("# hello\n\nsome content\n", encoding="utf-8")
    return p


class TestUploadHfReadmeDatasetHappyPath:
    @patch("ingestion.hf_publish.HfApi")
    def test_uploads_dataset_readme(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/datasets/org/name/commit/abc123"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=readme_file,
            hf_token="fake_token",
        )

        mock_hfapi_cls.assert_called_once_with(token="fake_token")
        mock_api.upload_file.assert_called_once()
        call_kwargs = mock_api.upload_file.call_args.kwargs
        assert call_kwargs["path_in_repo"] == "README.md"
        assert call_kwargs["repo_id"] == "org/name"
        assert call_kwargs["repo_type"] == "dataset"
        assert call_kwargs["token"] == "fake_token"
        assert result["commit_url"] == "https://huggingface.co/datasets/org/name/commit/abc123"
        assert "sha256" in result


class TestUploadHfReadmeSpace:
    @patch("ingestion.hf_publish.HfApi")
    def test_uploads_space_readme(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://huggingface.co/spaces/org/name/commit/abc"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=readme_file,
            hf_token="fake_token",
            repo_type="space",
        )

        call_kwargs = mock_api.upload_file.call_args.kwargs
        assert call_kwargs["repo_type"] == "space"
        assert call_kwargs["path_in_repo"] == "README.md"
        assert result["commit_url"] == "https://huggingface.co/spaces/org/name/commit/abc"


class TestValidation:
    def test_missing_file_raises_value_error(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist.md"
        with pytest.raises(ValueError, match="README not found"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=missing,
                hf_token="t",
            )

    def test_empty_file_raises_value_error(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.md"
        empty.write_text("")
        with pytest.raises(ValueError, match="README is empty"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=empty,
                hf_token="t",
            )

    def test_whitespace_only_file_raises_value_error(self, tmp_path: Path) -> None:
        ws = tmp_path / "ws.md"
        ws.write_text("   \n\n   ")
        with pytest.raises(ValueError, match="README is empty"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=ws,
                hf_token="t",
            )

    def test_invalid_repo_id_raises_value_error(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_id"):
            hf_publish.upload_hf_readme(
                repo_id="no-slash-id",
                readme_path=readme_file,
                hf_token="t",
            )

    def test_repo_id_with_dangerous_chars_rejected(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_id"):
            hf_publish.upload_hf_readme(
                repo_id="org/../etc",
                readme_path=readme_file,
                hf_token="t",
            )

    def test_invalid_repo_type_raises(self, readme_file: Path) -> None:
        with pytest.raises(ValueError, match="Invalid repo_type"):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=readme_file,
                hf_token="t",
                repo_type="model",  # not supported by this helper
            )


class TestLineEndingNormalization:
    @patch("ingestion.hf_publish.HfApi")
    def test_crlf_input_uploaded_as_lf(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        p = tmp_path / "README.md"
        p.write_bytes(b"# hello\r\nworld\r\n")
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://x"
        mock_hfapi_cls.return_value = mock_api

        hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=p,
            hf_token="t",
        )

        uploaded_bytes = mock_api.upload_file.call_args.kwargs["path_or_fileobj"]
        assert b"\r" not in uploaded_bytes
        assert uploaded_bytes == b"# hello\nworld\n"


class TestHfApiFailurePropagation:
    @patch("ingestion.hf_publish.HfApi")
    def test_api_error_propagates(self, mock_hfapi_cls: MagicMock, readme_file: Path) -> None:
        from huggingface_hub.errors import HfHubHTTPError

        mock_api = MagicMock()
        mock_api.upload_file.side_effect = HfHubHTTPError("401 Unauthorized")
        mock_hfapi_cls.return_value = mock_api

        with pytest.raises(HfHubHTTPError):
            hf_publish.upload_hf_readme(
                repo_id="org/name",
                readme_path=readme_file,
                hf_token="t",
            )


class TestSha256InReturn:
    @patch("ingestion.hf_publish.HfApi")
    def test_sha256_matches_uploaded_bytes(self, mock_hfapi_cls: MagicMock, tmp_path: Path) -> None:
        import hashlib

        p = tmp_path / "README.md"
        content = b"# consistent content\n"
        p.write_bytes(content)
        mock_api = MagicMock()
        mock_api.upload_file.return_value = "https://x"
        mock_hfapi_cls.return_value = mock_api

        result = hf_publish.upload_hf_readme(
            repo_id="org/name",
            readme_path=p,
            hf_token="t",
        )
        assert result["sha256"] == hashlib.sha256(content).hexdigest()


class TestGetDatasetCardPath:
    """Tests for the wheel-aware path resolver (PR 4c D6)."""

    def test_resolves_repo_path_in_dev(self) -> None:
        # In dev (editable install or source-tree run), the helper should return
        # a path under docs/huggingface/dataset-cards/ in the repo root.
        path = hf_publish.get_dataset_card_path("xg-freeze-frame-data.md")
        assert path.name == "xg-freeze-frame-data.md"
        assert "dataset-cards" in str(path)

    def test_returns_path_even_if_file_not_yet_created(self, tmp_path: Path) -> None:
        # Path object should be returned regardless of existence; caller validates.
        path = hf_publish.get_dataset_card_path("does-not-exist.md")
        assert isinstance(path, Path)

    def test_dangerous_name_rejected(self) -> None:
        # Basic path-traversal guard.
        with pytest.raises(ValueError, match="Invalid dataset-card name"):
            hf_publish.get_dataset_card_path("../../../etc/passwd")
        with pytest.raises(ValueError, match="Invalid dataset-card name"):
            hf_publish.get_dataset_card_path("sub/dir/card.md")

    def test_wheel_path_preferred_when_present(self, tmp_path: Path, monkeypatch) -> None:
        # Simulate wheel layout: create a fake site-packages with ingestion/ and
        # docs/huggingface/dataset-cards/<card>.md as siblings. Point the helper
        # at that layout.
        site_pkgs = tmp_path / "site-packages"
        (site_pkgs / "ingestion").mkdir(parents=True)
        (site_pkgs / "ingestion" / "__init__.py").write_text("")
        cards_dir = site_pkgs / "docs" / "huggingface" / "dataset-cards"
        cards_dir.mkdir(parents=True)
        card = cards_dir / "wheel-test.md"
        card.write_text("# from wheel")

        # Monkeypatch the module's __file__ resolver to point into site-packages.
        monkeypatch.setattr(
            hf_publish, "_WHEEL_INGESTION_FILE",
            site_pkgs / "ingestion" / "__init__.py",
        )
        resolved = hf_publish.get_dataset_card_path("wheel-test.md")
        assert resolved == card
```

- [ ] **Step 2:** Run:

```bash
uv run pytest src/tests/test_hf_publish.py -v
```

Expected: `ModuleNotFoundError` for `ingestion.hf_publish`. Red.

### Task 1.2: Write `hf_publish.py` (green)

**Files:**
- Create: `src/ingestion/hf_publish.py`.

- [ ] **Step 1:** Create `src/ingestion/hf_publish.py`:

```python
"""Shared helper for uploading README.md files to HuggingFace Hub.

Peer module to ``artifact_deploy.py``. The distinction:

- ``artifact_deploy.py`` handles the producer-side *weight* delivery chain
  (MLflow @Champion + UC Volume + HF Hub model weights) per ADR-012.
- ``hf_publish.py`` handles the producer-side *data* delivery chain —
  specifically the README.md documentation that rides with each published
  HF dataset or organization Space.

The helper is called from ``scripts/publish_*_hf.py`` at the end of each
``main()``, after the data upload completes. Because HF uploads are
idempotent, re-running a publish script re-uploads both data and README
without harm.

Validation posture: fail loud on bad inputs (missing file, empty file,
malformed repo_id) so operators see the error immediately. Propagate
``HfHubHTTPError`` from the SDK without catching — silent-swallow is
forbidden per ADR-002.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Literal

from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

# HF repo_id shape: 'owner/name', where owner and name follow HF's
# identifier rules (alphanumerics + -/._; no slashes inside either segment).
_REPO_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*/[a-zA-Z0-9._-]+$")
_SUPPORTED_REPO_TYPES: frozenset[str] = frozenset({"dataset", "space"})

# Dataset-card name validation: basename only, no path separators or traversal.
# Card files live flat under docs/huggingface/dataset-cards/.
_CARD_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*\.md$")

# Module-level reference used for wheel-path resolution. Exposed as a private
# attribute so tests can monkeypatch it to simulate the site-packages layout.
import ingestion as _ingestion  # noqa: E402  — import placement justified by usage

_WHEEL_INGESTION_FILE = Path(_ingestion.__file__).resolve()


def get_dataset_card_path(name: str) -> Path:
    """Resolve the on-disk path to a dataset card markdown file.

    Dual-mode resolution:
      1. **Wheel install (e.g. Databricks workflow task):** the wheel force-includes
         ``docs/huggingface/dataset-cards/`` as a sibling of the ``ingestion``
         package (see ``pyproject.toml`` force-include). Resolve via:
         ``Path(ingestion.__file__).parent.parent / "docs" / "huggingface" / "dataset-cards" / name``.
         Used at runtime by ``src/ingestion/export_shots_on_target.py``.
      2. **Dev / PEP 723 script (e.g. ``hf jobs uv run scripts/publish_*_hf.py``):**
         fall back to walking up from this module to the repo root, then descend
         into ``docs/huggingface/dataset-cards/``.

    Args:
        name: basename of the card file (e.g. ``"spadl-vaep-action-values.md"``).
            Must match ``^[a-zA-Z0-9][a-zA-Z0-9._-]*\\.md$`` — no subdirectories
            or path-traversal patterns.

    Returns:
        ``Path`` to the card. Not guaranteed to exist — caller validates via
        ``upload_hf_readme``'s file-existence check.

    Raises:
        ValueError: if ``name`` contains path separators, traversal patterns, or
            doesn't end in ``.md``.
    """
    if not _CARD_NAME_RE.match(name):
        raise ValueError(
            f"Invalid dataset-card name {name!r}. Expected a basename ending in .md, "
            "no path separators or traversal patterns."
        )

    # Wheel-first: site-packages layout where docs/ is a sibling of ingestion/.
    wheel_candidate = _WHEEL_INGESTION_FILE.parent.parent / "docs" / "huggingface" / "dataset-cards" / name
    if wheel_candidate.is_file():
        return wheel_candidate

    # Dev fallback: walk up from this module to repo root.
    # This file lives at src/ingestion/hf_publish.py → parents[2] = repo root.
    repo_candidate = Path(__file__).resolve().parents[2] / "docs" / "huggingface" / "dataset-cards" / name
    return repo_candidate


def upload_hf_readme(
    repo_id: str,
    readme_path: Path,
    hf_token: str,
    *,
    repo_type: Literal["dataset", "space"] = "dataset",
) -> dict[str, str]:
    """Upload a README.md to an HF dataset or Space repo.

    Validates file + repo_id, LF-normalizes the content, uploads via
    ``HfApi.upload_file``. Returns a dict with the commit URL and the
    SHA-256 of the uploaded bytes.

    Args:
        repo_id: Full HF repo id (e.g. ``"luxury-lakehouse/spadl-vaep-action-values"``
            for a dataset, or ``"luxury-lakehouse/README"`` for the org Space).
        readme_path: Path to the in-repo source markdown file.
        hf_token: HF API token.
        repo_type: ``"dataset"`` (default) or ``"space"``.

    Returns:
        ``{"commit_url": <commit url string>, "sha256": <hex digest of bytes>}``.

    Raises:
        ValueError: if the file is missing, empty/whitespace-only, repo_id
            doesn't match HF's identifier pattern, or repo_type is not one of
            the supported values.
        huggingface_hub.errors.HfHubHTTPError: propagated from the SDK on
            auth, network, or API failures. Callers do NOT catch — they fail
            loud so the operator re-runs after fixing the underlying cause.
    """
    if repo_type not in _SUPPORTED_REPO_TYPES:
        raise ValueError(
            f"Invalid repo_type {repo_type!r}. Supported: {sorted(_SUPPORTED_REPO_TYPES)}"
        )
    if not _REPO_ID_RE.match(repo_id):
        raise ValueError(f"Invalid repo_id {repo_id!r}. Expected 'owner/name' pattern.")
    if not readme_path.exists():
        raise ValueError(f"README not found: {readme_path}")

    raw = readme_path.read_bytes()
    if not raw.strip():
        raise ValueError(f"README is empty: {readme_path}")

    # LF normalize: CRLF → LF, bare CR → LF.
    normalized = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    sha256 = hashlib.sha256(normalized).hexdigest()

    api = HfApi(token=hf_token)
    commit_url = api.upload_file(
        path_or_fileobj=normalized,
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type=repo_type,
        token=hf_token,
        commit_message=f"Update README.md (generated from {readme_path.name})",
    )

    logger.info(
        "Uploaded README for %s (repo_type=%s, bytes=%d, sha256=%s)",
        repo_id,
        repo_type,
        len(normalized),
        sha256[:8],
    )
    return {"commit_url": str(commit_url), "sha256": sha256}
```

- [ ] **Step 2:** Run tests:

```bash
uv run pytest src/tests/test_hf_publish.py -v
```

Expected: all tests pass.

- [ ] **Step 3:** Lint + type check:

```bash
uv run ruff check src/ingestion/hf_publish.py src/tests/test_hf_publish.py
uv run ruff format --check src/ingestion/hf_publish.py src/tests/test_hf_publish.py
uv run pyright src/ingestion/hf_publish.py src/tests/test_hf_publish.py
```

Expected: zero violations.

---

## Phase 2: Dataset card markdown files

### Task 2.1: Create `docs/huggingface/dataset-cards/` directory

**Files:**
- Create: `docs/huggingface/dataset-cards/` (implicit via writing the first file into it).

- [ ] **Step 1:** Create the directory:

```bash
mkdir -p docs/huggingface/dataset-cards
```

### Task 2.2: Author `spadl-vaep-action-values.md`

**Files:**
- Create: `docs/huggingface/dataset-cards/spadl-vaep-action-values.md`.

- [ ] **Step 1:** Create the file. Content:

```markdown
---
license: cc-by-4.0
tags:
  - soccer
  - vaep
  - spadl
  - action-values
  - football-analytics
size_categories:
  - 1M<n<10M
task_categories:
  - tabular-regression
pretty_name: SPADL/VAEP Action Values
---

# SPADL/VAEP Action Values

Valuing Actions by Estimating Probabilities (VAEP) scores for every on-ball action
across StatsBomb + Wyscout open data. Implemented via
[silly-kicks](https://github.com/karsten-s-nielsen/silly-kicks).

Unified to the SPADL format (105×68m pitch; 23 action types).

Published from the `luxury-lakehouse` gold-layer `fct_action_values` table;
re-published when upstream data refreshes. See the `lastModified` timestamp
on the repo page for the most recent push.

## ⚠️ Schema change (cut-over 2026-07-22)

This dataset now emits both legacy and canonical key columns. Legacy columns
will be removed on **2026-07-22** (90 days from the initial dual-emit on
2026-04-23):

| Legacy column | Canonical replacement | Notes |
|---|---|---|
| `match_id` | `match_key` | BIGINT Kimball surrogate; collision-free across providers |
| `competition_id` | `competition_key` | BIGINT Kimball surrogate |

Update consumer code before the cut-over date. After cut-over, legacy columns
will be removed without further notice.

## Schema

| Column | Type | Description |
|---|---|---|
| `action_value_id` | STRING | dbt surrogate key: `md5(match_id, period, time_seconds, player_id, type_id, data_source)`. Unique. |
| `match_key` | BIGINT | Kimball match FK (new canonical; non-null). |
| `competition_key` | BIGINT | Kimball competition FK (new canonical; nullable). |
| `match_id` | BIGINT | LEGACY native match identifier (sunset 2026-07-22). |
| `competition_id` | BIGINT | LEGACY native competition identifier (sunset 2026-07-22). |
| `player_id` | BIGINT | Provider-native player ID. |
| `team_id` | BIGINT | Provider-native team ID. |
| `season_id` | BIGINT | Provider-native season ID. |
| `period` | INT | Half / extra time: 1, 2, 3 (ET1), 4 (ET2). |
| `time_seconds` | DOUBLE | Seconds since period start. |
| `minute` | INT | Match minute (period-relative). |
| `second` | INT | Second within minute. |
| `start_x`, `start_y` | DOUBLE | Action start location (105×68m pitch). |
| `end_x`, `end_y` | DOUBLE | Action end location. |
| `action_type` | STRING | One of 23 SPADL action types (pass, dribble, shot, etc.). |
| `action_result` | STRING | One of: success, fail, offside, owngoal, yellow_card, red_card. |
| `bodypart` | STRING | One of: foot, head, other, foot_right, foot_left, head/other. |
| `offensive_value` | DOUBLE | VAEP offensive component — change in P(score next) attributable to this action. |
| `defensive_value` | DOUBLE | VAEP defensive component — change in P(concede next). |
| `vaep_value` | DOUBLE | `offensive_value - defensive_value`. Positive = net offensive contribution. |
| `original_event_id` | STRING | Provider-native event ID for back-references. |
| `data_source` | STRING | Partition key: `"statsbomb"` or `"wyscout"`. |

## Partitioning

Parquet files partitioned by `data_source` (`data_source=statsbomb/data.parquet`,
`data_source=wyscout/data.parquet`). Load subsets efficiently by source.

## Reference

Decroos, T., Bransen, L., Van Haaren, J., & Davis, J. (2019). *Actions Speak
Louder Than Goals: Valuing Player Actions in Soccer.* KDD '19.
<https://doi.org/10.1145/3292500.3330758>

silly-kicks VAEP implementation: <https://github.com/karsten-s-nielsen/silly-kicks>

## License

CC-BY-4.0. Underlying data is StatsBomb open data + Wyscout open data, each under
their respective terms. Attribution: StatsBomb and Wyscout where used.
```

- [ ] **Step 2:** Validate markdown is well-formed:

```bash
uv run --no-project --with "pyyaml" python -c "
import yaml, pathlib
content = pathlib.Path('docs/huggingface/dataset-cards/spadl-vaep-action-values.md').read_text()
assert content.startswith('---\n'), 'missing frontmatter'
end = content.index('---\n', 4)
frontmatter = yaml.safe_load(content[4:end])
assert 'license' in frontmatter
print('OK:', frontmatter)
"
```

Expected: `OK: {...}` with the license + tags printed.

### Task 2.3: Author `xg-shot-data.md`

**Files:**
- Create: `docs/huggingface/dataset-cards/xg-shot-data.md`.

- [ ] **Step 1:** Fetch what's currently on HF (per Phase 0 Task 0.2 Step 1 output) and use it as the starting template. Typical content — adjust to match the live content verbatim:

```markdown
---
license: cc-by-4.0
tags:
  - soccer
  - xg
  - shots
  - football-analytics
size_categories:
  - 10K<n<100K
task_categories:
  - tabular-classification
  - tabular-regression
pretty_name: xG Shot Data (StatsBomb + Wyscout open)
---

# xG Shot Data

Shot-level features for expected goals (xG) modeling, extracted from StatsBomb +
Wyscout open data via the `luxury-lakehouse` gold-layer `fct_shots` mart.

Primary training input for xG v1 (XGBoost) and xG v2 (Deep Sets) models.

## Schema change (PR 3, 2026-04-22) — uniform rename

`match_id` column was replaced by `match_key` (BIGINT Kimball surrogate)
on 2026-04-22 per ADR-011. The change is a uniform rename — consumers
using `match_id` should switch to `match_key`. No dual-column window on
this dataset (low-usage; 17 monthly downloads pre-rename).

## Schema

| Column | Type | Description |
|---|---|---|
| `shot_id` | STRING | dbt surrogate PK. |
| `match_key` | BIGINT | Kimball match FK (replaces legacy `match_id`). |
| `match_id` | BIGINT | LEGACY; retained as a dual-column transition aid post-PR-3. Will be removed in PR 8 (2026Q3). |
| `competition_id` | BIGINT | Native competition ID (NULL for Wyscout). |
| `season_id` | BIGINT | Native season ID (NULL for Wyscout). |
| `player_id` | BIGINT | Shooter ID. |
| `team_id` | BIGINT | Shooter's team. |
| `period`, `minute`, `second` | INT | Match time. |
| `location_x`, `location_y` | DOUBLE | Shot location (StatsBomb 120×80 pitch). |
| `end_location_x`, `end_location_y` | DOUBLE | Shot destination. |
| `shot_outcome` | STRING | Goal, Saved, Blocked, Off T, Post, Saved Off T, Wayward. |
| `shot_body_part` | STRING | Right Foot, Left Foot, Head, Other. |
| `shot_technique` | STRING | Normal, Volley, Half Volley, Lob, Diving Header, etc. |
| `shot_type` | STRING | Open Play, Free Kick, Corner, Penalty, Kick Off. |
| `is_goal` | BOOLEAN | Derived from shot_outcome = 'Goal'. Target variable. |
| `distance_to_goal` | DOUBLE | Euclidean distance to goal centre (yards). |
| `shot_angle` | DOUBLE | Angle subtended by goal posts from shot location (radians). |
| `is_first_time` | BOOLEAN | Shot taken without prior control. |
| `play_pattern` | STRING | Regular Play, From Counter, From Free Kick, etc. |
| `statsbomb_xg` | DOUBLE | StatsBomb's proprietary xG (benchmark label; NULL for Wyscout). |
| `data_source` | STRING | Partition key: `"statsbomb"` or `"wyscout"`. |

## Partitioning

Parquet files partitioned by `data_source`.

## License

CC-BY-4.0 for this derived dataset. Underlying data from StatsBomb open data and
Wyscout open data; see respective licenses.
```

**Note on D4:** the content above is a working template. During execution, verify against what HF currently shows (Phase 0 Task 0.2 Step 1) and reconcile any schema or language differences. The goal is zero user-visible change on first push — the in-repo markdown matches the live HF README.

- [ ] **Step 2:** Validate YAML frontmatter parses cleanly:

```bash
uv run --no-project --with "pyyaml" python -c "
import yaml, pathlib
content = pathlib.Path('docs/huggingface/dataset-cards/xg-shot-data.md').read_text()
end = content.index('---\n', 4)
yaml.safe_load(content[4:end])
print('OK')
"
```

### Task 2.4: Author `statsbomb-shots-on-target.md`

**Files:**
- Create: `docs/huggingface/dataset-cards/statsbomb-shots-on-target.md`.

- [ ] **Step 1:** Start from the live HF content (Phase 0 Task 0.2). Typical shape:

```markdown
---
license: cc-by-4.0
tags:
  - soccer
  - psxg
  - shots
  - goalkeeper
  - football-analytics
size_categories:
  - 10K<n<100K
task_categories:
  - tabular-classification
pretty_name: StatsBomb Shots on Target (PSxG)
---

# StatsBomb Shots on Target

Shots-on-target dataset for post-shot expected goals (PSxG) modeling, filtered
from the full shot dataset to include only shots that required a save or resulted
in a goal. Derived from StatsBomb open data via the `luxury-lakehouse` gold layer.

Primary training input for PSxG models (goalkeeper quality assessment).

## ⚠️ Schema change (cut-over 2026-07-22)

This dataset emits both legacy and canonical key columns. Legacy `match_id`
will be removed on **2026-07-22** (90 days from the initial dual-emit on
2026-04-22):

| Legacy column | Canonical replacement | Notes |
|---|---|---|
| `match_id` | `match_key` | BIGINT Kimball surrogate; collision-free across providers |

Update consumer code before the cut-over date.

## Schema

See the `xg-shot-data` dataset card for the full shot column set. This dataset
filters to `shot_outcome IN ('Goal', 'Saved', 'Saved Off T', 'Saved To Post')`
and adds ball-end-location columns relevant for PSxG modeling.

## License

CC-BY-4.0 for this derived dataset. Underlying data from StatsBomb open data.
```

- [ ] **Step 2:** Validate frontmatter parses.

### Task 2.5: Author `xg-freeze-frame-data.md`

**Files:**
- Create: `docs/huggingface/dataset-cards/xg-freeze-frame-data.md`.

- [ ] **Step 1:** Create (fresh content — no dual-column):

```markdown
---
license: cc-by-4.0
tags:
  - soccer
  - xg
  - freeze-frame
  - spatial
  - football-analytics
size_categories:
  - 100K<n<1M
task_categories:
  - tabular-classification
pretty_name: xG Shot Freeze Frames
---

# xG Shot Freeze Frames

Per-shot player positions at the moment of each shot, from StatsBomb open data.
One row per player per shot; each shot has ~10–22 rows.

Primary input for xG v2 (Deep Sets) and defender-pressure models.

## Schema

| Column | Type | Description |
|---|---|---|
| `event_id` | STRING | StatsBomb-native shot event ID. Links back to the shot in `fct_shots`. |
| `match_id` | BIGINT | Native match ID. |
| `competition_id` | BIGINT | Native competition ID. |
| `season_id` | BIGINT | Native season ID. |
| `player_x_norm` | DOUBLE | Player x coordinate, normalized to [0, 1] over the 120-yard pitch length. |
| `player_y_norm` | DOUBLE | Player y coordinate, normalized to [0, 1] over the 80-yard pitch width. |
| `is_keeper` | BOOLEAN | True if this player is the goalkeeper. |
| `is_teammate` | BOOLEAN | True if this player is on the shooter's team. |

## Partitioning

Parquet files partitioned by `competition_id`.

## Reference

StatsBomb open data freeze-frame format: <https://github.com/statsbomb/open-data>.

## License

CC-BY-4.0 for this derived dataset. Underlying data from StatsBomb open data.
```

- [ ] **Step 2:** Validate frontmatter parses.

### Task 2.6: Add a content-validation test

**Files:**
- Modify: `src/tests/test_hf_publish.py` (add a test class).

- [ ] **Step 1:** Add to `test_hf_publish.py`:

```python
class TestDatasetCardContent:
    """Invariants on the four dataset card files (guards against accidental empties / broken frontmatter)."""

    _CARDS_DIR = Path(__file__).parent.parent.parent / "docs" / "huggingface" / "dataset-cards"
    _EXPECTED_CARDS = {
        "spadl-vaep-action-values.md",
        "xg-shot-data.md",
        "statsbomb-shots-on-target.md",
        "xg-freeze-frame-data.md",
    }

    def test_all_expected_cards_exist(self) -> None:
        present = {p.name for p in self._CARDS_DIR.iterdir() if p.is_file()}
        missing = self._EXPECTED_CARDS - present
        assert not missing, f"Missing dataset cards: {missing}"

    def test_cards_are_non_empty(self) -> None:
        for name in self._EXPECTED_CARDS:
            p = self._CARDS_DIR / name
            assert p.read_bytes().strip(), f"{name} is empty"

    def test_cards_have_yaml_frontmatter(self) -> None:
        import yaml

        for name in self._EXPECTED_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert content.startswith("---\n"), f"{name} missing frontmatter"
            end = content.index("---\n", 4)
            fm = yaml.safe_load(content[4:end])
            assert isinstance(fm, dict), f"{name} frontmatter is not a mapping"
            assert "license" in fm, f"{name} frontmatter missing license"

    def test_dual_column_cards_include_sunset_date(self) -> None:
        dual_column_cards = ["spadl-vaep-action-values.md", "statsbomb-shots-on-target.md"]
        for name in dual_column_cards:
            content = (self._CARDS_DIR / name).read_text()
            assert "2026-07-22" in content, (
                f"{name} must document the 2026-07-22 sunset date for dual-column removal"
            )

    def test_cards_end_with_newline(self) -> None:
        for name in self._EXPECTED_CARDS:
            content = (self._CARDS_DIR / name).read_text(encoding="utf-8")
            assert content.endswith("\n"), f"{name} must end with a newline"
```

- [ ] **Step 2:** Run:

```bash
uv run pytest src/tests/test_hf_publish.py::TestDatasetCardContent -v
```

Expected: all five tests pass.

### Task 2.7: Update `pyproject.toml` to force-include dataset cards in the wheel

**Files:**
- Modify: `pyproject.toml`.

- [ ] **Step 1:** Open `pyproject.toml`. Locate the existing `[tool.hatch.build.targets.wheel]` (or equivalent) block — Phase 0 Task 0.6 recorded the exact key name. The existing force-include that bundles `dbt_project/dbt_packages/` is the pattern to extend.

- [ ] **Step 2:** Add `docs/huggingface/dataset-cards/*.md` to the force-include list. Example (syntax depends on the existing style — hatch supports both dict and list-of-tuples forms):

```toml
[tool.hatch.build.targets.wheel.force-include]
"dbt_project/dbt_packages" = "dbt_project/dbt_packages"
"docs/huggingface/dataset-cards" = "docs/huggingface/dataset-cards"
```

Preserve the existing `dbt_packages` entry — do NOT replace, only add.

- [ ] **Step 3:** Bump the wheel version. In `pyproject.toml`:

```toml
# [project]
version = "0.3.14"   # was 0.3.13
```

(Exact version depends on Phase 0 Task 0.6's reading of the current version.)

### Task 2.8: Run `bump_wheel.py` to sync 19 consumers

**Files:** All files updated by `bump_wheel.py` (see `src/shared/wheel.py` + ~18 others).

- [ ] **Step 1:** Run the sync script per its CLI (Phase 0 Task 0.6 recorded the exact invocation):

```bash
uv run python scripts/bump_wheel.py --new-version 0.3.14
```

OR if it reads the version from pyproject.toml directly:

```bash
uv run python scripts/bump_wheel.py
```

Expected: script reports N consumers updated (memory says 19). Git status after:

```bash
git status -s | head -25
```

Expected: changes across PEP 723 script headers (`scripts/*_hf.py`), Terraform wheel URLs (`terraform/modules/**/*.tf`), `src/shared/wheel.py`, `hf_taipy_app/requirements.txt`, `deploy.sh`, and any other consumers the script knows about. Each file should show a 0.3.13 → 0.3.14 diff (plus any `#sha256=` fragment update if memory `reference_wheel_consumers` mentions that).

### Task 2.9: Verify the wheel build includes the dataset cards

**Files:** None — live build + inspection.

- [ ] **Step 1:** Build the wheel and inspect contents:

```bash
rm -rf /tmp/wheel_inspect
uv build --wheel --out-dir /tmp/wheel_inspect
ls /tmp/wheel_inspect/

# Inspect the wheel archive contents
uv run --no-project python -c "
import zipfile, sys
whl = next(iter(__import__('pathlib').Path('/tmp/wheel_inspect').glob('luxury_lakehouse-*.whl')))
with zipfile.ZipFile(whl) as z:
    cards = [n for n in z.namelist() if 'dataset-cards' in n and n.endswith('.md')]
    print(f'Found {len(cards)} dataset cards in wheel:')
    for c in sorted(cards): print(f'  {c}')
    assert len(cards) >= 4, f'Expected >= 4 cards, got {len(cards)}'
"
```

Expected: output lists 4 card files inside the wheel under `docs/huggingface/dataset-cards/`.

- [ ] **Step 2:** Verify the runtime path resolution in a wheel install (simulates Databricks workflow runtime):

```bash
uv run --no-project --with "$(ls /tmp/wheel_inspect/luxury_lakehouse-*.whl)" python - <<'PY'
from ingestion.hf_publish import get_dataset_card_path
path = get_dataset_card_path("spadl-vaep-action-values.md")
print(f"Resolved: {path}")
assert path.is_file(), f"Card not found at {path}"
print("OK — wheel-path resolution works")
PY
```

Expected: "OK — wheel-path resolution works". Path should be under a site-packages location, not the repo's `docs/`.

### Task 2.10: Commit Phase 1 + Phase 2 (requires user approval)

Per D3, one commit covers helper + cards + wheel bundling.

- [ ] **Step 1:**

```bash
git add src/ingestion/hf_publish.py \
        src/tests/test_hf_publish.py \
        docs/huggingface/dataset-cards/spadl-vaep-action-values.md \
        docs/huggingface/dataset-cards/xg-shot-data.md \
        docs/huggingface/dataset-cards/statsbomb-shots-on-target.md \
        docs/huggingface/dataset-cards/xg-freeze-frame-data.md \
        pyproject.toml \
        src/shared/wheel.py
# Plus the 17-ish other files bump_wheel.py touched — use git status to enumerate.
git add scripts/*_hf.py hf_taipy_app/requirements.txt deploy.sh terraform/  # adapt to actual touched set
git commit -m "feat(hf): hf_publish helper + 4 dataset cards + wheel 0.3.14 (PR 4c Phase 1+2)"
```

Expected commit touches helper + tests + 4 cards + pyproject.toml + wheel consumers (~20 files total).

---

## Phase 3: Integrate helper into the three dataset publish scripts

### Task 3.1: `publish_spadl_vaep_hf.py`

**Files:**
- Modify: `scripts/publish_spadl_vaep_hf.py`.

- [ ] **Step 1:** Add the import near the top (after other project imports, respecting isort):

```python
from ingestion.hf_publish import get_dataset_card_path, upload_hf_readme
```

- [ ] **Step 2:** Add the README-upload call at the end of `main()`, after the `logger.info("Pipeline complete. Dataset: %s", dataset_url)` line:

```python
    # Upload the README alongside the data publish (PR 4c).
    upload_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_dataset_card_path("spadl-vaep-action-values.md"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        upload_result["commit_url"],
        upload_result["sha256"][:8],
    )
```

- [ ] **Step 3:** Lint:

```bash
uv run ruff check scripts/publish_spadl_vaep_hf.py
uv run pyright scripts/publish_spadl_vaep_hf.py
```

Expected: zero new violations.

### Task 3.2: `publish_xg_shots_hf.py`

**Files:**
- Modify: `scripts/publish_xg_shots_hf.py`.

- [ ] **Step 1:** Add the import and README call analogously. At the end of `main()`:

```python
    upload_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_dataset_card_path("xg-shot-data.md"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        upload_result["commit_url"],
        upload_result["sha256"][:8],
    )
```

- [ ] **Step 2:** Lint.

### Task 3.3: `publish_freeze_frame_hf.py`

**Files:**
- Modify: `scripts/publish_freeze_frame_hf.py`.

- [ ] **Step 1:** Same pattern, with `xg-freeze-frame-data.md`:

```python
    upload_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_dataset_card_path("xg-freeze-frame-data.md"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        upload_result["commit_url"],
        upload_result["sha256"][:8],
    )
```

- [ ] **Step 2:** Lint.

### Task 3.4: Integrate README upload into `export_shots_on_target.py`

**Files:**
- Modify: `src/ingestion/export_shots_on_target.py`.

**Path committed (D6):** the wheel bundles `docs/huggingface/dataset-cards/` via `pyproject.toml` force-include (Task 2.7). At runtime inside a Databricks workflow, `get_dataset_card_path("statsbomb-shots-on-target.md")` resolves to the wheel-install-tree sibling of the `ingestion` package. Same helper, same pattern as the three PEP 723 scripts above.

- [ ] **Step 1:** Open `src/ingestion/export_shots_on_target.py`. Find the `run_pipeline` function (around line 136, with `@workflow("wf-export-shots", phase="export")`).

- [ ] **Step 2:** Add the import near the top (respecting isort):

```python
import os

from ingestion.hf_publish import get_dataset_card_path, upload_hf_readme
```

- [ ] **Step 3:** At the end of `run_pipeline`, after the existing `_upload_to_hf_hub(volume_path)` call returns (around line 128+ caller), add the README upload:

```python
    dataset_url = _upload_to_hf_hub(volume_path)
    logger.info("Uploaded dataset to HF Hub: %s", dataset_url)

    # Upload the README alongside the data publish (PR 4c).
    # Card markdown is force-included in the wheel via pyproject.toml; the
    # resolver handles both wheel (Databricks runtime) and repo (local dev).
    hf_token = os.environ.get("HF_TOKEN", "")
    if not hf_token:
        from huggingface_hub import get_token

        hf_token = get_token() or ""
    if not hf_token:
        raise RuntimeError(
            "HF_TOKEN required for README upload. Set via the workflow's "
            "secret bindings or local env."
        )

    upload_result = upload_hf_readme(
        repo_id=DATASET_REPO,
        readme_path=get_dataset_card_path("statsbomb-shots-on-target.md"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded README: %s (sha256=%s)",
        upload_result["commit_url"],
        upload_result["sha256"][:8],
    )
```

Exact insertion point and indentation depend on how `run_pipeline` currently structures the phases — read the file during execution and match the style.

- [ ] **Step 4:** If `run_pipeline` does NOT currently have access to `HF_TOKEN` via env (e.g., the existing `_upload_to_hf_hub` uses `huggingface_hub.get_token()` exclusively or the workflow task uses a service-principal token): verify the workflow's secret bindings. The Databricks workflow task for `wf-export-shots` must pass `HF_TOKEN` as a task-level environment variable — grep Terraform:

```bash
grep -A 10 "wf-export-shots\|export_shots_on_target" terraform/modules/workflows/main.tf | head -30
```

If HF_TOKEN isn't already bound, add it to the workflow task's env in the Terraform file. This is a one-line Terraform change that rides with PR 4c.

- [ ] **Step 5:** Lint:

```bash
uv run ruff check src/ingestion/export_shots_on_target.py
uv run pyright src/ingestion/export_shots_on_target.py
```

Expected: zero new violations.

- [ ] **Step 6:** Run existing tests for this file to catch any regression:

```bash
ls src/tests/test_export_shots_on_target.py 2>/dev/null && uv run pytest src/tests/test_export_shots_on_target.py -v
```

If the file has tests: expected all pass. If new test for the README integration is warranted, add one that mocks `upload_hf_readme` and verifies it's called with the right args after `_upload_to_hf_hub`.

---

## Phase 4: Refactor `publish_hf_org_card.py` to use the helper

### Task 4.1: Refactor

**Files:**
- Modify: `scripts/publish_hf_org_card.py`.

- [ ] **Step 1:** Read the current file:

```bash
cat scripts/publish_hf_org_card.py
```

Identify the existing `HfApi.upload_file(...)` call (likely inside the `main()` or a helper function).

- [ ] **Step 2:** Replace the upload logic with a call to `upload_hf_readme`. Pre-refactor (approximate; exact shape depends on current file):

```python
# BEFORE:
api = HfApi(token=hf_token)
api.upload_file(
    path_or_fileobj=content_bytes,
    path_in_repo="README.md",
    repo_id="luxury-lakehouse/README",
    repo_type="space",
    token=hf_token,
)
```

Post-refactor:

```python
# AFTER:
from pathlib import Path

from ingestion.hf_publish import upload_hf_readme

result = upload_hf_readme(
    repo_id="luxury-lakehouse/README",
    readme_path=Path(__file__).parent.parent / "docs" / "huggingface" / "org-card.md",
    hf_token=hf_token,
    repo_type="space",
)
logger.info("Uploaded org-card: %s", result["commit_url"])
```

Remove any now-unused imports (`HfApi` might still be needed elsewhere in the file; check before removing). Keep the `luxury-lakehouse.jpg` upload logic unchanged — that's out of scope per D5.

- [ ] **Step 3:** If the script has unit tests (`src/tests/test_publish_hf_org_card.py`), run them:

```bash
ls src/tests/test_publish_hf_org_card.py 2>/dev/null && uv run pytest src/tests/test_publish_hf_org_card.py -v
```

Expected: all existing tests pass. Adapt any that referenced the old `HfApi.upload_file` patch target to point at `ingestion.hf_publish.upload_hf_readme` instead.

- [ ] **Step 4:** Lint.

---

## Phase 5: Refresh `docs/huggingface/org-card.md` content

### Task 5.1: Review and update org-card content

**Files:**
- Modify: `docs/huggingface/org-card.md`.

- [ ] **Step 1:** Read current org-card content:

```bash
cat docs/huggingface/org-card.md
```

Compare against Phase 0 Task 0.2 Step 2 output (live HF content). Any divergence → reconcile (the live HF content is what external users see today; `docs/huggingface/org-card.md` should match or strictly improve on it).

- [ ] **Step 2:** Update org-card to:
- Reference all four published datasets with their current schemas.
- Mention the 2026-07-22 sunset for the two dual-column datasets (spadl-vaep-action-values, statsbomb-shots-on-target).
- Remove any references to datasets that no longer exist or are deprecated.
- Update any "recently published" or "coming soon" sections to reflect reality.

Illustrative snippet to add or update (verify against existing structure during implementation):

```markdown
## Datasets

We maintain four open-access datasets on HF Hub, all derived from StatsBomb +
Wyscout open data and republished with schema conformance + documentation:

- **[spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values)** — 9.5M on-ball actions with VAEP scores (SPADL unified format). Dual-column schema through 2026-07-22; migrate consumers to `match_key` / `competition_key`.
- **[xg-shot-data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data)** — shot features for xG modeling (StatsBomb + Wyscout). Kimball-conformed 2026-04-22.
- **[statsbomb-shots-on-target](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target)** — PSxG training set (shots that required a save or resulted in a goal). Dual-column schema through 2026-07-22.
- **[xg-freeze-frame-data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data)** — per-player positions at moment of shot (Deep Sets input).
```

- [ ] **Step 3:** Confirm no trailing-CRLF or other encoding issues:

```bash
file docs/huggingface/org-card.md
uv run python -c "
import pathlib
content = pathlib.Path('docs/huggingface/org-card.md').read_bytes()
assert b'\\r\\n' not in content, 'CRLF present; LF-normalize before committing'
assert content.endswith(b'\\n'), 'file must end with LF'
print('OK')
"
```

---

## Phase 6: E2E verification (gated on user approval)

### Task 6.1: E2E on the safest dataset (`xg-freeze-frame-data`)

**Files:** None — live HF operation.

- [ ] **Step 1 (gated):** With explicit user approval, run the full publish for `xg-freeze-frame-data` (safest candidate — no dual-column change, so content update is minimal):

```bash
hf jobs uv run scripts/publish_freeze_frame_hf.py \
    --flavor cpu-basic --timeout 30m \
    --secrets HF_TOKEN \
    --env DATABRICKS_HOST="$DATABRICKS_HOST" \
    --env DATABRICKS_TOKEN="$DATABRICKS_TOKEN" \
    --env DATABRICKS_SQL_WAREHOUSE_ID="$DATABRICKS_SQL_WAREHOUSE_ID"
```

Expected: HF job runs to completion. Output logs show data upload + README upload both succeeding.

- [ ] **Step 2:** Verify on HF Hub:

```bash
uv run --no-project --with "huggingface-hub,requests" python - <<'PY'
from huggingface_hub import hf_hub_download
import os
p = hf_hub_download(
    repo_id="luxury-lakehouse/xg-freeze-frame-data",
    filename="README.md",
    repo_type="dataset",
    token=os.environ.get("HF_TOKEN"),
    force_download=True,   # bust the local cache
)
content = open(p).read()
assert "xG Shot Freeze Frames" in content, "README not updated"
print("OK — new README present on HF")
PY
```

Expected: "OK — new README present on HF".

### Task 6.2: E2E on org-card (manual step, gated)

**Files:** None.

- [ ] **Step 1 (gated):** With user approval, run the refactored org-card push:

```bash
uv run python scripts/publish_hf_org_card.py
```

Expected: `Uploaded org-card: <commit_url>` log line. Content on HF (`https://huggingface.co/luxury-lakehouse`) reflects the updated org-card.

- [ ] **Step 2:** Verify:

```bash
curl -s https://huggingface.co/luxury-lakehouse/raw/main/README.md | head -50
```

Expected: the updated content.

---

## Phase 7: Ship PR 4c

### Task 7.1: Commit Phase 3 + Phase 4 + Phase 5 (requires user approval)

- [ ] **Step 1:**

```bash
git add scripts/publish_spadl_vaep_hf.py \
        scripts/publish_xg_shots_hf.py \
        scripts/publish_freeze_frame_hf.py \
        scripts/publish_hf_org_card.py \
        src/ingestion/export_shots_on_target.py \
        docs/huggingface/org-card.md
# Include any Terraform changes from Phase 3 Task 3.4 Step 4 (HF_TOKEN env binding on wf-export-shots)
# Check git status and add matched files:
git status -s
git add terraform/   # if Terraform touched
git commit -m "feat(hf): wire hf_publish into 4 publishers + refresh org-card (PR 4c Phase 3+4+5)"
```

### Task 7.2: Open PR

- [ ] **Step 1:** Open the PR (requires user approval):

```bash
gh pr create \
  --base main \
  --title "feat(hf): README helper + dataset cards + org-card refactor (Kimball PR 4c)" \
  --body "$(cat <<'EOF'
## Summary
- Adds `src/ingestion/hf_publish.py` — shared helper for uploading README.md to HF dataset or Space repos, plus a wheel-aware `get_dataset_card_path` resolver.
- Adds in-repo source of truth for dataset READMEs under `docs/huggingface/dataset-cards/` (4 cards).
- Force-includes `docs/huggingface/dataset-cards/` in the wheel (`pyproject.toml`) so `src/ingestion/export_shots_on_target.py` can read its card at Databricks-workflow runtime. Bumps wheel 0.3.13 → 0.3.14; syncs 19 consumers via `scripts/bump_wheel.py`.
- Wires the helper into all four HF publishers: the three PEP 723 scripts (`publish_spadl_vaep_hf.py`, `publish_xg_shots_hf.py`, `publish_freeze_frame_hf.py`) plus `src/ingestion/export_shots_on_target.py` (statsbomb-shots-on-target writer).
- Refactors `scripts/publish_hf_org_card.py` to use the same helper (repo_type="space"), unifying HF-publish code paths.
- Refreshes `docs/huggingface/org-card.md` to reference current dataset schemas + 2026-07-22 sunset dates.

## Test plan
- [x] Unit tests (`src/tests/test_hf_publish.py`) green: 20+ cases including validation, LF normalization, error propagation, SHA-256, wheel-vs-repo path resolution.
- [x] Dataset-card content invariants pass (`TestDatasetCardContent` class).
- [x] Wheel build contains the 4 dataset cards (`Phase 2.9` verification).
- [ ] E2E on `xg-freeze-frame-data` (safest — no dual-column change) — gated on explicit approval.
- [ ] E2E on `wf-export-shots` workflow run — gated on explicit approval.
- [ ] E2E on org-card push via refactored script — gated on explicit approval.

Closes the PR 3 deferral on per-dataset README drift. Depends on PR 4a merged.
Lands after PR 4b so the spadl-vaep-action-values sunset warning aligns with
the post-PR-4b publish cycle.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 2:** After user approves merge:

```bash
# User merges via GH UI.
```

### Task 7.3: Post-merge

- [ ] **Step 1:** On next publish runs of each `publish_*_hf.py` script (triggered manually or via scheduled workflow), the README auto-uploads. No drift between in-repo markdown and HF Hub content.

- [ ] **Step 2:** Document the new publish pattern in `CLAUDE.md` if not already covered. The "HF artifact link completeness" rule already exists per spec §2 — verify it stays accurate post-PR-4c.

---

## Self-review findings (plan author notes)

**Spec coverage:** Spec §7.1 helper signature maps to Phase 1 Task 1.2. Spec §7.2 dataset-card structure maps to Phase 2 Tasks 2.2–2.5. Spec §7.3 org-card refactor maps to Phase 4. Spec §6 publish-script integration is split across Phase 3 (three PEP 723 scripts + `export_shots_on_target.py`) + Phase 4 (org-card script). Spec §3 row 12 β decision (extend helper to cover org Space) realized in Phase 1 Task 1.2 (the `repo_type` parameter) + Phase 4 (refactor).

**Spec drift:** the spec's §2 "Explicitly NOT modified" section originally said "Wheel + pyproject.toml — no new entry points or `[project.scripts]` bumps." Path A (D6 decision — wheel bundling of dataset cards) supersedes this. The spec was updated in the same session to reflect the scope change; the core architecture (docs/huggingface/dataset-cards/ as user-visible location, hf_publish.py as shared helper) is unchanged.

**Placeholders scan:** None. D4 (source of existing dataset-card content) is resolved in Phase 0 Task 0.2 with a concrete fetch command; the content templates in Phase 2 are starting points explicitly marked "verify against live". D6 (wheel bundling) is resolved in Phase 0 Task 0.6 + committed to path A.

**Type consistency:** `upload_hf_readme` signature is consistent between tests (Phase 1 Task 1.1) and implementation (Phase 1 Task 1.2). `get_dataset_card_path` signature + return type (`Path`) is consistent across tests and all four publisher call sites. Return type dict keys (`commit_url`, `sha256`) match test assertions.

**Known ambiguities for execution time:**
- **Phase 0 Task 0.6.** Exact `[tool.hatch.build.targets.wheel.force-include]` key name + syntax in `pyproject.toml` — read first, replicate style.
- **Phase 0 Task 0.7.** Wheel install layout — confirm docs/ is a sibling of ingestion/ in site-packages post-force-include.
- **Phase 3 Task 3.4 Step 4.** Whether `wf-export-shots` Terraform already binds HF_TOKEN as a task env var; one-line TF addition if not.
- **Phase 4 Task 4.1.** Current shape of `scripts/publish_hf_org_card.py` determines exact refactor mechanics (e.g., imports to drop).
- **Phase 5 Task 5.1.** Org-card content reconciliation with live HF — depends on Phase 0 Task 0.2 Step 2's diff.
- **License fields on dataset cards.** Assumed CC-BY-4.0 for all four. If the current on-HF cards use different license strings (e.g., "mit", "other"), replicate.
